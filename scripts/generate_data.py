"""
generate_data.py
-----------------
Generates synthetic CRM data for the Customer 360 portfolio project.

Output (local first, upload to S3 separately — see upload_to_s3.py):
    data/raw/customers.json
    data/raw/transactions.csv
    data/raw/support_tickets.json
    data/raw/loyalty_events.csv

The messiness is INTENTIONAL — it's what gives your PySpark cleaning
step real work to do. Don't "fix" it here.

Usage:
    python generate_data.py
"""

import json
import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

N_CUSTOMERS = 100_000
N_TRANSACTIONS = 500_000
N_SUPPORT_TICKETS = 15_000
DUPLICATE_RATE = 0.03          # ~3% of customers get a "duplicate" record
NULL_CUSTOMER_ID_RATE = 0.005  # ~0.5% of transactions have a null/broken FK
MALFORMED_TIMESTAMP_RATE = 0.01

LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
CHANNELS = ["web", "mobile_app", "in_store", "call_center"]

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2026, 6, 1)


def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def maybe_malform_timestamp(ts: datetime) -> str:
    """Return a clean ISO timestamp most of the time, garbage sometimes."""
    if random.random() < MALFORMED_TIMESTAMP_RATE:
        # a few realistic ways timestamps get mangled upstream
        bad_formats = [
            ts.strftime("%d/%m/%Y"),          # ambiguous date format, no time
            ts.strftime("%m-%d-%y %H:%M"),    # 2-digit year
            "",                                # empty string
            "NaT",
            str(int(ts.timestamp())),          # raw unix epoch as string
        ]
        return random.choice(bad_formats)
    return ts.isoformat()


def slightly_mutate(s: str) -> str:
    """Introduce a small typo/variation — used to create fuzzy-duplicate customers."""
    if len(s) < 3:
        return s
    choice = random.random()
    idx = random.randint(1, len(s) - 2)
    if choice < 0.33:
        # swap two adjacent characters
        chars = list(s)
        chars[idx], chars[idx + 1] = chars[idx + 1], chars[idx]
        return "".join(chars)
    elif choice < 0.66:
        # drop a character
        return s[:idx] + s[idx + 1:]
    else:
        # duplicate a character
        return s[:idx] + s[idx] + s[idx:]


def gen_customers():
    print(f"Generating {N_CUSTOMERS:,} customers (+{DUPLICATE_RATE:.0%} fuzzy duplicates)...")
    customers = []
    base_records = []

    for _ in range(N_CUSTOMERS):
        customer_id = str(uuid.uuid4())
        first = fake.first_name()
        last = fake.last_name()
        email = f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{fake.free_email_domain()}"
        signup_date = random_date(START_DATE, END_DATE)

        record = {
            "customer_id": customer_id,
            "first_name": first,
            "last_name": last,
            "email": email,
            "signup_date": signup_date.date().isoformat(),
            "loyalty_tier": random.choices(
                LOYALTY_TIERS, weights=[0.45, 0.30, 0.18, 0.07]
            )[0],
            "state": fake.state_abbr(),
            "is_duplicate_of": None,
        }
        customers.append(record)
        base_records.append((customer_id, first, last, email, signup_date))

    # Inject fuzzy-duplicate customer records (same person, slightly different
    # name/email — this is exactly what your PySpark fuzzy-dedup step will catch)
    n_duplicates = int(N_CUSTOMERS * DUPLICATE_RATE)
    dup_sample = random.sample(base_records, n_duplicates)

    for customer_id, first, last, email, signup_date in dup_sample:
        dup_first = slightly_mutate(first)
        dup_last = last if random.random() < 0.5 else slightly_mutate(last)
        dup_email = f"{dup_first.lower()}.{dup_last.lower()}{random.randint(1,999)}@{fake.free_email_domain()}"
        dup_signup = signup_date + timedelta(days=random.randint(0, 30))

        customers.append({
            "customer_id": str(uuid.uuid4()),
            "first_name": dup_first,
            "last_name": dup_last,
            "email": dup_email,
            "signup_date": dup_signup.date().isoformat(),
            "loyalty_tier": "Bronze",  # duplicates usually look "new"
            "state": fake.state_abbr(),
            "is_duplicate_of": customer_id,  # ground truth, NOT available to your Spark job — drop this column before writing the "raw" file your pipeline reads
        })

    random.shuffle(customers)
    return customers


def gen_transactions(customer_ids):
    print(f"Generating {N_TRANSACTIONS:,} transactions...")
    rows = []
    for _ in range(N_TRANSACTIONS):
        cust_id = random.choice(customer_ids)
        if random.random() < NULL_CUSTOMER_ID_RATE:
            cust_id = ""  # broken FK, intentional

        ts = random_date(START_DATE, END_DATE)
        rows.append({
            "transaction_id": str(uuid.uuid4()),
            "customer_id": cust_id,
            "amount": round(random.lognormvariate(3.5, 1.0), 2),  # realistic skewed spend distribution
            "timestamp": maybe_malform_timestamp(ts),
            "channel": random.choice(CHANNELS),
        })
    return rows


def gen_support_tickets(customer_ids):
    print(f"Generating {N_SUPPORT_TICKETS:,} support tickets (nested JSON)...")
    tickets = []
    for _ in range(N_SUPPORT_TICKETS):
        cust_id = random.choice(customer_ids)
        n_messages = random.randint(1, 5)
        opened = random_date(START_DATE, END_DATE)

        messages = []
        msg_time = opened
        for i in range(n_messages):
            msg_time += timedelta(hours=random.randint(1, 48))
            messages.append({
                "sender": "customer" if i % 2 == 0 else "agent",
                "text": fake.sentence(nb_words=12),
                "sent_at": msg_time.isoformat(),
            })

        tickets.append({
            "ticket_id": str(uuid.uuid4()),
            "customer_id": cust_id,
            "opened_at": opened.isoformat(),
            "category": random.choice(
                ["billing", "loyalty_question", "product_issue", "shipping", "account_access"]
            ),
            "messages": messages,  # nested array — forces real JSON parsing in PySpark
        })
    return tickets


def gen_loyalty_events(customer_ids):
    print("Generating loyalty tier events...")
    rows = []
    for cust_id in random.sample(customer_ids, k=int(len(customer_ids) * 0.4)):
        n_events = random.randint(1, 3)
        event_time = random_date(START_DATE, END_DATE)
        for _ in range(n_events):
            event_time += timedelta(days=random.randint(30, 200))
            if event_time > END_DATE:
                break
            rows.append({
                "customer_id": cust_id,
                "event_type": random.choice(["tier_upgrade", "tier_downgrade"]),
                "new_tier": random.choice(LOYALTY_TIERS),
                "event_date": event_time.date().isoformat(),
            })
    return rows


def write_json(records, path, drop_fields=None):
    drop_fields = drop_fields or []
    with open(path, "w") as f:
        for r in records:
            clean = {k: v for k, v in r.items() if k not in drop_fields}
            f.write(json.dumps(clean) + "\n")
    print(f"  wrote {len(records):,} records -> {path}")


def write_csv(records, path, fieldnames):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"  wrote {len(records):,} records -> {path}")


def main():
    customers = gen_customers()
    customer_ids = [c["customer_id"] for c in customers]

    transactions = gen_transactions(customer_ids)
    tickets = gen_support_tickets(customer_ids)
    loyalty_events = gen_loyalty_events(customer_ids)

    # Customers written as newline-delimited JSON (typical raw-zone format).
    # is_duplicate_of is dropped here on purpose — your PySpark dedup job
    # should NOT have access to ground truth. Keep a separate answer-key
    # file for grading your own dedup accuracy later.
    write_json(customers, OUTPUT_DIR / "customers.json", drop_fields=["is_duplicate_of"])
    write_json(
        [{"customer_id": c["customer_id"], "is_duplicate_of": c["is_duplicate_of"]}
         for c in customers if c["is_duplicate_of"]],
        OUTPUT_DIR / "_answer_key_duplicates.json",
    )

    write_csv(
        transactions, OUTPUT_DIR / "transactions.csv",
        fieldnames=["transaction_id", "customer_id", "amount", "timestamp", "channel"],
    )
    write_json(tickets, OUTPUT_DIR / "support_tickets.json")
    write_csv(
        loyalty_events, OUTPUT_DIR / "loyalty_events.csv",
        fieldnames=["customer_id", "event_type", "new_tier", "event_date"],
    )

    print("\nDone. Files in data/raw/:")
    for f in sorted(OUTPUT_DIR.glob("*")):
        size_mb = f.stat().st_size / 1_000_000
        print(f"  {f.name:35s} {size_mb:6.1f} MB")


if __name__ == "__main__":
    main()
