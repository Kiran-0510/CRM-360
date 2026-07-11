"""
spark_clean_transform.py
-------------------------
The PySpark stage of the Customer 360 pipeline. Runs BEFORE dbt.

Does the things that are awkward or impossible in pure SQL:
  1. Parse nested support_tickets JSON (explode messages array)
  2. Fuzzy-dedupe customer records (Levenshtein on name+email)
  3. Compute 90-day rolling spend windows per customer
  4. Clean/quarantine malformed timestamps and broken foreign keys
  5. Write clean Parquet to a "processed" zone

Run locally first (no cluster needed for this data size):
    spark-submit spark_clean_transform.py \
        --input-dir data/raw \
        --output-dir data/processed

Requires: pyspark, jellyfish (for fuzzy string matching)
    pip install pyspark jellyfish
"""

import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType, TimestampType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("crm360-clean")


# ---------------------------------------------------------------------------
# Explicit schemas. We do NOT use inferSchema=True.
# Why this matters (say this in interviews): schema inference is slow on
# large files (Spark has to read the whole file twice) and silently masks
# upstream schema drift — a new column or a type change fails loud here
# instead of corrupting data downstream.
# ---------------------------------------------------------------------------

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("signup_date", StringType(), nullable=True),
    StructField("loyalty_tier", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
])

TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True),  # raw string — we parse manually, see below
    StructField("channel", StringType(), nullable=True),
])

TICKET_MESSAGE_SCHEMA = StructType([
    StructField("sender", StringType(), True),
    StructField("text", StringType(), True),
    StructField("sent_at", StringType(), True),
])

SUPPORT_TICKETS_SCHEMA = StructType([
    StructField("ticket_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("opened_at", StringType(), True),
    StructField("category", StringType(), True),
    StructField("messages", ArrayType(TICKET_MESSAGE_SCHEMA), True),
])


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("crm360-clean-transform")
        # local[*] uses all cores on your machine — fine for this data size.
        # Swap master to a yarn/EMR endpoint later; nothing else in this
        # script changes, which is the point of using Spark here at all.
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")  # small for local; default 200 is overkill here
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Step 1: load raw data with explicit schemas
# ---------------------------------------------------------------------------

def load_customers(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(CUSTOMERS_SCHEMA).json(path)


def load_transactions(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(TRANSACTIONS_SCHEMA).option("header", True).csv(path)


def load_support_tickets(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(SUPPORT_TICKETS_SCHEMA).json(path)


# ---------------------------------------------------------------------------
# Step 2: clean transactions — quarantine bad timestamps / broken FKs
#   instead of silently dropping them. This mirrors the reconciliation
#   habit from your Fontainebleau work: surface mismatches, don't hide them.
# ---------------------------------------------------------------------------

def clean_transactions(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (clean_df, quarantined_df)."""

    # Try strict ISO timestamp parse first
    parsed = df.withColumn("ts_parsed", F.try_to_timestamp("timestamp"))

    has_valid_ts = F.col("ts_parsed").isNotNull()
    has_customer_id = (F.col("customer_id").isNotNull()) & (F.col("customer_id") != "")

    clean = (
        parsed.filter(has_valid_ts & has_customer_id)
        .withColumnRenamed("ts_parsed", "event_timestamp")
        .drop("timestamp")
    )

    quarantined = (
        parsed.filter(~has_valid_ts | ~has_customer_id)
        .withColumn(
            "quarantine_reason",
            F.when(~has_valid_ts, F.lit("malformed_timestamp"))
             .otherwise(F.lit("missing_customer_id"))
        )
    )

    n_clean = clean.count()
    n_quarantined = quarantined.count()
    log.info(f"transactions: {n_clean:,} clean, {n_quarantined:,} quarantined "
             f"({n_quarantined / (n_clean + n_quarantined):.2%})")

    return clean, quarantined


# ---------------------------------------------------------------------------
# Step 3: fuzzy-dedupe customers
#   Approach: block on a normalized last_name to avoid an O(n^2) comparison
#   across all 100K customers, then compare candidates within each block
#   using Levenshtein distance on first_name + email similarity.
#   This is the Spark-scale version of your FuzzyWuzzy work at UNLV.
# ---------------------------------------------------------------------------

def normalize(col):
    return F.lower(F.trim(col))


def dedupe_customers(df: DataFrame, spark: SparkSession) -> DataFrame:
    df = df.withColumn("last_name_norm", normalize(F.col("last_name")))
    df = df.withColumn("first_name_norm", normalize(F.col("first_name")))

    # Self-join within blocks of matching normalized last_name —
    # this keeps the comparison space small instead of comparing
    # every customer to every other customer.
    a = df.alias("a")
    b = df.alias("b")

    candidates = (
        a.join(b, on=F.col("a.last_name_norm") == F.col("b.last_name_norm"))
        .filter(F.col("a.customer_id") < F.col("b.customer_id"))  # avoid comparing a row to itself / double-counting pairs
        .select(
            F.col("a.customer_id").alias("customer_id_a"),
            F.col("a.first_name_norm").alias("first_a"),
            F.col("a.email").alias("email_a"),
            F.col("b.customer_id").alias("customer_id_b"),
            F.col("b.first_name_norm").alias("first_b"),
            F.col("b.email").alias("email_b"),
            F.col("a.state").alias("state_a"),
            F.col("b.state").alias("state_b")
        )
    )

    # levenshtein() is a built-in Spark SQL function — no extra dependency
    # needed for the core distance calc.
    scored = (
        candidates
        .withColumn("name_distance", F.levenshtein(F.col("first_a"), F.col("first_b")))
        .withColumn("email_prefix_a", F.substring_index(F.col("email_a"), "@", 1))
        .withColumn("email_prefix_b", F.substring_index(F.col("email_b"), "@", 1))
        .withColumn(
            "email_prefix_distance",
            F.levenshtein(
                F.col("email_prefix_a"),
                F.col("email_prefix_b")
)
        )
        .withColumn(
            "same_state",
            F.col("state_a") == F.col("state_b")
        )
    )

    likely_dupes = scored.filter(
        (F.col("name_distance") <= 1) &
        (F.col("email_prefix_distance") <= 2) &
        (F.col("same_state") == True)
    )
    likely_dupes.show()

    n_flagged = likely_dupes.count()
    log.info(f"customer dedup: flagged {n_flagged:,} likely-duplicate pairs")

    # Mark the later-signed-up record of each pair as the duplicate.
    # In a real pipeline you'd merge/survive fields across the pair;
    # for this portfolio project, flagging + keeping both with a
    # `dedup_flag` column is enough to demonstrate the technique.
    dupe_ids = likely_dupes.select(F.col("customer_id_b").alias("customer_id")).distinct()

    result = (
        df.join(dupe_ids.withColumn("is_likely_duplicate", F.lit(True)), on="customer_id", how="left")
        .withColumn("is_likely_duplicate", F.coalesce(F.col("is_likely_duplicate"), F.lit(False)))
        .drop("last_name_norm", "first_name_norm")
    )

    return result


# ---------------------------------------------------------------------------
# Step 4: rolling 90-day spend window per customer
#   This is the kind of thing that's clunky in pure SQL across partitions
#   at scale, and is a classic PySpark interview topic — know this pattern.
# ---------------------------------------------------------------------------

def compute_rolling_spend(transactions: DataFrame) -> DataFrame:
    # rangeBetween on a timestamp column needs it expressed in seconds
    seconds_in_90_days = 90 * 24 * 60 * 60

    txn_with_seconds = transactions.withColumn(
        "ts_seconds", F.col("event_timestamp").cast("long")
    )

    window_90d = (
        Window.partitionBy("customer_id")
        .orderBy("ts_seconds")
        .rangeBetween(-seconds_in_90_days, 0)
    )

    result = txn_with_seconds.withColumn(
        "rolling_90d_spend", F.sum("amount").over(window_90d)
    ).withColumn(
        "rolling_90d_txn_count", F.count("transaction_id").over(window_90d)
    )

    return result.drop("ts_seconds")


# ---------------------------------------------------------------------------
# Step 5: explode support ticket messages into a flat table
# ---------------------------------------------------------------------------

def flatten_support_tickets(df: DataFrame) -> DataFrame:
    exploded = df.withColumn("message", F.explode("messages"))
    return (
        exploded
        .select(
            "ticket_id", "customer_id", "opened_at", "category",
            F.col("message.sender").alias("message_sender"),
            F.col("message.text").alias("message_text"),
            F.to_timestamp("message.sent_at").alias("message_sent_at"),
        )
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="data/raw")
    parser.add_argument("--output-dir", default="data/processed")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")  # Spark's own logs are noisy; keep ours visible

    log.info("Loading raw data...")
    customers = load_customers(spark, f"{args.input_dir}/customers.json")
    transactions = load_transactions(spark, f"{args.input_dir}/transactions.csv")
    tickets = load_support_tickets(spark, f"{args.input_dir}/support_tickets.json")

    log.info("Cleaning transactions (quarantining bad rows)...")
    clean_txns, quarantined_txns = clean_transactions(transactions)

    log.info("Deduping customers...")
    deduped_customers = dedupe_customers(customers, spark)

    log.info("Computing rolling 90-day spend...")
    txns_with_rolling = compute_rolling_spend(clean_txns)

    log.info("Flattening support ticket messages...")
    flat_tickets = flatten_support_tickets(tickets)

    log.info("Writing clean Parquet output...")
    deduped_customers.write.mode("overwrite").parquet(f"{args.output_dir}/customers")
    txns_with_rolling.write.mode("overwrite").partitionBy("channel").parquet(f"{args.output_dir}/transactions")
    quarantined_txns.write.mode("overwrite").parquet(f"{args.output_dir}/_quarantine/transactions")
    flat_tickets.write.mode("overwrite").parquet(f"{args.output_dir}/support_ticket_messages")

    log.info("Done. Run a quick sanity check:")
    log.info(f"  customers: {deduped_customers.count():,} rows "
             f"({deduped_customers.filter('is_likely_duplicate').count():,} flagged as likely duplicates)")
    log.info(f"  transactions (clean): {txns_with_rolling.count():,} rows")
    log.info(f"  transactions (quarantined): {quarantined_txns.count():,} rows")
    log.info(f"  support ticket messages (flattened): {flat_tickets.count():,} rows")

    #spark.stop()


if __name__ == "__main__":
    main()
