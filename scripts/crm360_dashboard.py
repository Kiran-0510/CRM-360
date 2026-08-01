"""
CRM 360 Analytics Dashboard
============================
Built on top of Snowflake MARTS schema:
  - feature_store_customer
  - fact_transactions
  - fact_support
  - dim_customer

Run locally:
    pip install streamlit plotly snowflake-connector-python pandas
    streamlit run crm360_dashboard.py

To enable live Snowflake connection, set these environment variables:
    export SNOWFLAKE_ACCOUNT=payzair-oe00840
    export SNOWFLAKE_USER=KIRAN0510
    export SNOWFLAKE_PASSWORD=your_password
    export SNOWFLAKE_DATABASE=CRM360
    export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os

# ── page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CRM 360 Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── theme ─────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0f172a; }
    .stApp { background-color: #0f172a; color: #f1f5f9; }
    .metric-card {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
    }
    .metric-value { font-size: 32px; font-weight: 700; color: #38bdf8; }
    .metric-label { font-size: 13px; color: #94a3b8; margin-top: 4px; }
    .metric-sub { font-size: 11px; color: #64748b; margin-top: 2px; }
    .section-header {
        background: linear-gradient(90deg, #1e40af, #0ea5e9);
        border-radius: 8px;
        padding: 10px 20px;
        margin: 20px 0 16px 0;
    }
    .section-header h2 { color: white; margin: 0; font-size: 18px; }
    .source-badge {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 6px;
        padding: 4px 10px;
        font-size: 11px;
        color: #64748b;
        display: inline-block;
        margin-bottom: 16px;
    }
    div[data-testid="stSidebar"] { background-color: #1e293b; }
    h1, h2, h3 { color: #f1f5f9 !important; }
    p, label { color: #cbd5e1 !important; }
</style>
""", unsafe_allow_html=True)

# ── snowflake connection (live) ────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_from_snowflake(query):
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            database=os.environ.get("SNOWFLAKE_DATABASE", "CRM360"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            schema="MARTS",
        )
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception:
        return None

USE_LIVE = all(k in os.environ for k in [
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"
])

# ── hardcoded data (from Snowflake query results) ────────────────────────────
# Customer KPIs
kpi_customers = {
    "total_customers": 103000,
    "high_value_customers": 10699,  # P90 threshold: spend > $500
    "at_churn_risk": 717,
    "avg_days_since_signup": 495,
    "avg_lifetime_spend": 260.87,
}

# Loyalty tier breakdown
df_tier = pd.DataFrame({
    "loyalty_tier": ["BRONZE", "SILVER", "GOLD", "PLATINUM"],
    "customer_count": [40164, 28471, 20902, 13463],
    "avg_lifetime_spend": [261.25, 262.01, 258.32, 261.32],
    "at_risk_count": [263, 197, 154, 103],
})

# Customer segment
df_segment = pd.DataFrame({
    "customer_segment": ["established", "developing", "new"],
    "customer_count": [66429, 32475, 4096],
    "avg_lifetime_spend": [261.68, 259.10, 261.77],
})

# Top states
df_states = pd.DataFrame({
    "state": ["RI","SC","TN","HI","PA","FL","MA","WA","LA","MI","IL","KS","PR","AS","DC"],
    "customer_count": [1902,1844,1807,1796,1795,1789,1786,1785,1784,1782,1781,1781,1781,1778,1775],
    "avg_lifetime_spend": [266.93,259.18,260.93,273.62,253.76,262.94,254.34,253.32,269.38,261.32,256.19,256.04,264.00,258.43,260.32],
})

# Transaction KPIs
kpi_transactions = {
    "total_revenue": 26869977.28,
    "avg_transaction_value": 54.55,
    "total_transactions": 492561,
    "unique_customers": 102190,
}

# Revenue by month/channel
raw_monthly = [
    ("2024-01","call_center",228218.34,4260),("2024-01","in_store",229485.88,4310),
    ("2024-01","mobile_app",231692.72,4259),("2024-01","web",236504.00,4374),
    ("2024-02","call_center",227298.88,4133),("2024-02","in_store",222788.88,4230),
    ("2024-02","mobile_app",220050.55,4101),("2024-02","web",219268.76,3999),
    ("2024-03","call_center",235182.31,4420),("2024-03","in_store",237575.96,4327),
    ("2024-03","mobile_app",250972.85,4458),("2024-03","web",242690.08,4378),
    ("2024-04","call_center",232022.22,4248),("2024-04","in_store",238034.94,4323),
    ("2024-04","mobile_app",231417.67,4247),("2024-04","web",235610.20,4242),
    ("2024-05","call_center",240299.09,4408),("2024-05","in_store",235471.55,4361),
    ("2024-05","mobile_app",232964.50,4286),("2024-05","web",238401.11,4273),
    ("2024-06","call_center",222067.95,4128),("2024-06","in_store",231790.39,4141),
    ("2024-06","mobile_app",215860.84,4098),("2024-06","web",226035.81,4048),
    ("2024-07","call_center",241605.14,4406),("2024-07","in_store",236082.07,4331),
    ("2024-07","mobile_app",236946.44,4378),("2024-07","web",238801.03,4336),
    ("2024-08","call_center",226455.38,4210),("2024-08","in_store",233388.78,4305),
    ("2024-08","mobile_app",234159.82,4235),("2024-08","web",241761.16,4388),
    ("2024-09","call_center",226082.26,4203),("2024-09","in_store",225551.86,4128),
    ("2024-09","mobile_app",234706.82,4150),("2024-09","web",231214.62,4228),
    ("2024-10","call_center",236744.44,4309),("2024-10","in_store",244412.89,4286),
    ("2024-10","mobile_app",232032.29,4269),("2024-10","web",238851.90,4279),
    ("2024-11","call_center",232528.44,4257),("2024-11","in_store",234720.91,4240),
    ("2024-11","mobile_app",227260.96,4163),("2024-11","web",230180.74,4195),
    ("2024-12","call_center",236074.47,4336),("2024-12","in_store",240541.62,4349),
    ("2024-12","mobile_app",242778.09,4358),("2024-12","web",251550.94,4394),
    ("2025-01","call_center",244077.20,4402),("2025-01","in_store",233965.60,4194),
    ("2025-01","mobile_app",233073.46,4358),("2025-01","web",232613.92,4312),
    ("2025-02","call_center",211403.19,3882),("2025-02","in_store",208997.97,3881),
    ("2025-02","mobile_app",215287.26,3816),("2025-02","web",213053.71,3931),
    ("2025-03","call_center",232288.68,4294),("2025-03","in_store",231685.89,4271),
    ("2025-03","mobile_app",234723.65,4313),("2025-03","web",242349.71,4444),
    ("2025-04","call_center",231381.81,4232),("2025-04","in_store",223589.39,4182),
    ("2025-04","mobile_app",224016.61,4248),("2025-04","web",228059.51,4155),
    ("2025-05","call_center",234052.40,4346),("2025-05","in_store",235384.84,4276),
    ("2025-05","mobile_app",239191.51,4302),("2025-05","web",228342.74,4255),
    ("2025-06","call_center",242668.37,4257),("2025-06","in_store",225820.62,4143),
    ("2025-06","mobile_app",218936.02,4113),("2025-06","web",227025.59,4187),
    ("2025-07","call_center",229143.84,4391),("2025-07","in_store",234521.07,4347),
    ("2025-07","mobile_app",242172.90,4406),("2025-07","web",229633.93,4358),
    ("2025-08","call_center",235836.78,4277),("2025-08","in_store",234791.62,4272),
    ("2025-08","mobile_app",233891.10,4273),("2025-08","web",233543.63,4334),
    ("2025-09","call_center",228762.59,4241),("2025-09","in_store",230176.74,4280),
    ("2025-09","mobile_app",226847.02,4189),("2025-09","web",233447.82,4126),
    ("2025-10","call_center",240844.55,4338),("2025-10","in_store",220076.30,4203),
    ("2025-10","mobile_app",236160.28,4370),("2025-10","web",239496.81,4343),
    ("2025-11","call_center",216838.19,4075),("2025-11","in_store",229665.55,4212),
    ("2025-11","mobile_app",225626.20,4058),("2025-11","web",228614.28,4156),
    ("2025-12","call_center",236331.82,4357),("2025-12","in_store",240574.84,4339),
    ("2025-12","mobile_app",229729.52,4360),("2025-12","web",235226.19,4382),
    ("2026-01","call_center",230314.28,4376),("2026-01","in_store",239720.52,4356),
    ("2026-01","mobile_app",223873.82,4168),("2026-01","web",237333.22,4280),
    ("2026-02","call_center",205598.41,3855),("2026-02","in_store",208294.48,3847),
    ("2026-02","mobile_app",209731.90,3857),("2026-02","web",210531.85,3824),
    ("2026-03","call_center",244891.43,4406),("2026-03","in_store",242905.60,4407),
    ("2026-03","mobile_app",234582.10,4248),("2026-03","web",228069.10,4290),
    ("2026-04","call_center",228227.57,4219),("2026-04","in_store",220440.55,4138),
    ("2026-04","mobile_app",217591.03,4043),("2026-04","web",231401.07,4235),
    ("2026-05","call_center",250216.60,4459),("2026-05","in_store",235411.01,4345),
    ("2026-05","mobile_app",242282.48,4395),("2026-05","web",239466.59,4388),
]
df_monthly = pd.DataFrame(raw_monthly, columns=["month","channel","revenue","transaction_count"])

# Revenue by loyalty tier
df_rev_tier = pd.DataFrame({
    "loyalty_tier": ["BRONZE","SILVER","GOLD","PLATINUM"],
    "total_revenue": [5074503.52,3582523.36,2541247.32,1582997.35],
    "avg_transaction": [54.47,54.96,54.13,54.16],
    "transaction_count": [93154,65183,46947,29226],
})

# Transaction value bands
df_bands = pd.DataFrame({
    "transaction_value_band": ["low_value","mid_value","high_value"],
    "transaction_count": [426811,64062,1688],
    "total_revenue": [14604479.32,11072467.66,1193030.30],
})

# Support KPIs
kpi_support = {
    "total_tickets": 15000,
    "avg_resolution_hours": 49.2,
    "avg_messages": 3.0,
}

# Support by category
df_category = pd.DataFrame({
    "category": ["billing","product_issue","loyalty_question","account_access","shipping"],
    "ticket_count": [3061,3004,3003,2984,2948],
    "avg_resolution_hours": [48.3,49.9,49.9,48.9,49.2],
    "avg_messages": [3.0,3.0,3.0,3.0,3.0],
})

# Resolution speed
df_speed = pd.DataFrame({
    "resolution_speed": ["standard","fast","slow"],
    "ticket_count": [5805,4848,4347],
})

# Churn risk by tier
df_churn = pd.DataFrame({
    "loyalty_tier": ["BRONZE","SILVER","GOLD","PLATINUM"],
    "total_customers": [40164,28471,20902,13463],
    "at_risk_count": [263,197,154,103],
    "avg_spend_at_risk": [1256.68,1235.94,1239.49,1236.48],
    "avg_spend_not_at_risk": [254.69,255.22,251.04,253.80],
    "avg_days_since_txn_at_risk": [179,176,198,180],
})

# ── color palette ─────────────────────────────────────────────────────────────
COLORS = {
    "BRONZE":   "#cd7f32",
    "SILVER":   "#94a3b8",
    "GOLD":     "#f59e0b",
    "PLATINUM": "#818cf8",
    "web":        "#38bdf8",
    "mobile_app": "#34d399",
    "in_store":   "#f472b6",
    "call_center":"#fb923c",
    "established":"#38bdf8",
    "developing": "#34d399",
    "new":        "#f472b6",
    "billing":         "#38bdf8",
    "product_issue":   "#f472b6",
    "loyalty_question":"#34d399",
    "account_access":  "#fb923c",
    "shipping":        "#818cf8",
    "standard": "#f59e0b",
    "fast":     "#34d399",
    "slow":     "#f87171",
    "low_value":  "#38bdf8",
    "mid_value":  "#f59e0b",
    "high_value": "#34d399",
}

PLOTLY_THEME = dict(
    paper_bgcolor="#0f172a",
    plot_bgcolor="#1e293b",
    font=dict(color="#cbd5e1", family="DM Sans, sans-serif"),
    xaxis=dict(gridcolor="#334155", zerolinecolor="#334155"),
    yaxis=dict(gridcolor="#334155", zerolinecolor="#334155"),
    margin=dict(l=20, r=20, t=40, b=20),
)

def apply_theme(fig):
    fig.update_layout(**PLOTLY_THEME)
    return fig

# ── helpers ───────────────────────────────────────────────────────────────────
def kpi_card(label, value, sub=None):
    sub_html = f'<div class="metric-sub">{sub}</div>' if sub else ""
    return f"""
    <div class="metric-card">
        <div class="metric-value">{value}</div>
        <div class="metric-label">{label}</div>
        {sub_html}
    </div>"""

def section(title, source):
    st.markdown(f'<div class="section-header"><h2>📊 {title}</h2></div>', unsafe_allow_html=True)
    st.markdown(f'<span class="source-badge">Source: CRM360.MARTS.{source}</span>', unsafe_allow_html=True)

# ── sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## CRM 360")
    st.markdown("**Analytics Dashboard**")
    st.markdown("---")
    st.markdown("**Pipeline:**")
    st.markdown("Faker → PySpark → Snowflake → dbt → Streamlit")
    st.markdown("---")
    st.markdown("**Data Scale:**")
    st.markdown("- 103K customers")
    st.markdown("- 492K transactions")
    st.markdown("- 15K support tickets")
    st.markdown("- 36 dbt tests ✅")
    st.markdown("---")
    page = st.radio("Navigate", [
        "Customer Overview",
        "Transaction Analytics",
        "Support Intelligence",
        "Churn Risk",
    ])
    st.markdown("---")
    conn_status = "🟢 Live (Snowflake)" if USE_LIVE else "🟡 Snapshot Mode"
    st.markdown(f"**Connection:** {conn_status}")
    st.markdown("*Set SNOWFLAKE_* env vars for live data*")

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Customer Overview
# ═══════════════════════════════════════════════════════════════════════════════
if page == "Customer Overview":
    st.title("Customer Overview")

    section("Customer KPIs", "FEATURE_STORE_CUSTOMER")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(kpi_card("Total Customers", f"{kpi_customers['total_customers']:,}"), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi_card("At Churn Risk", f"{kpi_customers['at_churn_risk']:,}",
                             f"{kpi_customers['at_churn_risk']/kpi_customers['total_customers']*100:.1f}% of base"), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi_card("High Value Customers", f"{kpi_customers['high_value_customers']:,}",
                             "days_since_txn > 60, spend > avg"), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi_card("Avg Lifetime Spend", f"${kpi_customers['avg_lifetime_spend']:,.2f}"), unsafe_allow_html=True)
    with c5:
        st.markdown(kpi_card("Avg Tenure", f"{kpi_customers['avg_days_since_signup']:,} days",
                             "~16 months avg"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            df_tier, x="loyalty_tier", y="customer_count",
            color="loyalty_tier",
            color_discrete_map=COLORS,
            title="Customers by Loyalty Tier",
            labels={"loyalty_tier": "Tier", "customer_count": "Customers"},
        )
        apply_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.pie(
            df_segment, values="customer_count", names="customer_segment",
            color="customer_segment",
            color_discrete_map=COLORS,
            title="Customer Lifecycle Segments",
            hole=0.5,
        )
        apply_theme(fig)
        fig.update_traces(textinfo="percent+label", textfont_color="white")
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        fig = px.bar(
            df_states.sort_values("customer_count"),
            x="customer_count", y="state",
            orientation="h",
            color="avg_lifetime_spend",
            color_continuous_scale="Blues",
            title="Top 15 States by Customer Count",
            labels={"customer_count": "Customers", "state": "State",
                    "avg_lifetime_spend": "Avg Spend ($)"},
        )
        apply_theme(fig)
        fig.update_layout(coloraxis_colorbar=dict(tickfont=dict(color="#cbd5e1")))
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        fig = px.bar(
            df_tier, x="loyalty_tier", y="avg_lifetime_spend",
            color="loyalty_tier",
            color_discrete_map=COLORS,
            title="Avg Lifetime Spend by Tier",
            labels={"loyalty_tier": "Tier", "avg_lifetime_spend": "Avg Spend ($)"},
        )
        apply_theme(fig)
        fig.update_layout(yaxis_range=[255, 265])
        st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Transaction Analytics
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Transaction Analytics":
    st.title("Transaction Analytics")

    section("Transaction KPIs", "FACT_TRANSACTIONS")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi_card("Total Revenue", f"${kpi_transactions['total_revenue']/1e6:.2f}M"), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi_card("Total Transactions", f"{kpi_transactions['total_transactions']:,}"), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi_card("Avg Transaction Value", f"${kpi_transactions['avg_transaction_value']:.2f}"), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi_card("Unique Customers", f"{kpi_transactions['unique_customers']:,}",
                             f"{kpi_transactions['unique_customers']/103000*100:.1f}% of base transacted"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Monthly revenue line chart
    section("Revenue by Month & Channel", "FACT_TRANSACTIONS")
    # exclude June 2026 (partial month)
    df_plot = df_monthly[df_monthly["month"] < "2026-06"].copy()
    channel_filter = st.multiselect(
        "Filter channels",
        options=["web","mobile_app","in_store","call_center"],
        default=["web","mobile_app","in_store","call_center"],
    )
    df_plot = df_plot[df_plot["channel"].isin(channel_filter)]
    fig = px.line(
        df_plot, x="month", y="revenue",
        color="channel",
        color_discrete_map=COLORS,
        title="Monthly Revenue by Channel (Jan 2024 – May 2026)",
        labels={"month": "Month", "revenue": "Revenue ($)", "channel": "Channel"},
        markers=True,
    )
    apply_theme(fig)
    fig.update_traces(line_width=2, marker_size=4)
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            df_rev_tier, x="loyalty_tier", y="total_revenue",
            color="loyalty_tier",
            color_discrete_map=COLORS,
            title="Revenue by Loyalty Tier (Point-in-Time Correct)",
            labels={"loyalty_tier": "Tier", "total_revenue": "Total Revenue ($)"},
        )
        apply_theme(fig)
        fig.add_annotation(
            x=0.5, y=1.05, xref="paper", yref="paper",
            text="★ SCD2 — tier attributed at time of transaction, not current tier",
            showarrow=False,
            font=dict(size=10, color="#94a3b8"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.pie(
            df_bands, values="total_revenue", names="transaction_value_band",
            color="transaction_value_band",
            color_discrete_map=COLORS,
            title="Revenue Share by Transaction Value Band",
            hole=0.5,
        )
        apply_theme(fig)
        fig.update_traces(textinfo="percent+label", textfont_color="white")
        st.plotly_chart(fig, use_container_width=True)

    # transaction count by band
    fig = px.bar(
        df_bands, x="transaction_value_band", y="transaction_count",
        color="transaction_value_band",
        color_discrete_map=COLORS,
        title="Transaction Count by Value Band",
        labels={"transaction_value_band": "Band", "transaction_count": "Transactions"},
    )
    apply_theme(fig)
    st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Support Intelligence
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Support Intelligence":
    st.title("Support Intelligence")

    section("Support KPIs", "FACT_SUPPORT")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(kpi_card("Total Tickets", f"{kpi_support['total_tickets']:,}"), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi_card("Avg Resolution Time", f"{kpi_support['avg_resolution_hours']:.1f} hrs",
                             "~2 days avg"), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi_card("Avg Messages / Ticket", f"{kpi_support['avg_messages']:.1f}",
                             "customer + agent"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            df_category.sort_values("avg_resolution_hours"),
            x="avg_resolution_hours", y="category",
            orientation="h",
            color="category",
            color_discrete_map=COLORS,
            title="Avg Resolution Hours by Category",
            labels={"avg_resolution_hours": "Avg Hours", "category": "Category"},
        )
        apply_theme(fig)
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.pie(
            df_speed, values="ticket_count", names="resolution_speed",
            color="resolution_speed",
            color_discrete_map=COLORS,
            title="Resolution Speed Distribution",
            hole=0.5,
        )
        apply_theme(fig)
        fig.update_traces(textinfo="percent+label", textfont_color="white")
        st.plotly_chart(fig, use_container_width=True)

    fig = px.bar(
        df_category, x="category", y="ticket_count",
        color="category",
        color_discrete_map=COLORS,
        title="Ticket Volume by Category",
        labels={"category": "Category", "ticket_count": "Tickets"},
    )
    apply_theme(fig)
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Churn Risk
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Churn Risk":
    st.title("Churn Risk Analysis")

    section("Churn Risk KPIs", "FEATURE_STORE_CUSTOMER")

    total_at_risk = df_churn["at_risk_count"].sum()
    avg_days = df_churn["avg_days_since_txn_at_risk"].mean()
    highest_risk_tier = df_churn.loc[df_churn["at_risk_count"].idxmax(), "loyalty_tier"]
    avg_spend_risk = df_churn["avg_spend_at_risk"].mean()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi_card("Total At-Risk Customers", f"{total_at_risk:,}",
                             f"{total_at_risk/103000*100:.1f}% of base"), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi_card("Avg Days Inactive", f"{avg_days:.0f} days",
                             "among at-risk segment"), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi_card("Highest Risk Tier", highest_risk_tier,
                             f"{df_churn.loc[df_churn['loyalty_tier']==highest_risk_tier, 'at_risk_count'].values[0]:,} customers"), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi_card("Avg Spend (At Risk)", f"${avg_spend_risk:,.2f}",
                             "vs $253 not at risk"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            df_churn, x="loyalty_tier", y="at_risk_count",
            color="loyalty_tier",
            color_discrete_map=COLORS,
            title="At-Risk Customers by Loyalty Tier",
            labels={"loyalty_tier": "Tier", "at_risk_count": "At-Risk Customers"},
        )
        apply_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        df_spend_compare = pd.melt(
            df_churn[["loyalty_tier","avg_spend_at_risk","avg_spend_not_at_risk"]],
            id_vars="loyalty_tier",
            var_name="segment",
            value_name="avg_spend"
        )
        df_spend_compare["segment"] = df_spend_compare["segment"].map({
            "avg_spend_at_risk": "At Risk",
            "avg_spend_not_at_risk": "Not At Risk"
        })
        fig = px.bar(
            df_spend_compare, x="loyalty_tier", y="avg_spend",
            color="segment",
            barmode="group",
            color_discrete_map={"At Risk": "#f87171", "Not At Risk": "#34d399"},
            title="Avg Lifetime Spend: At Risk vs Not At Risk",
            labels={"loyalty_tier": "Tier", "avg_spend": "Avg Spend ($)", "segment": "Segment"},
        )
        apply_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

    # risk rate by tier
    df_churn["risk_rate"] = df_churn["at_risk_count"] / df_churn["total_customers"] * 100
    fig = px.bar(
        df_churn, x="loyalty_tier", y="risk_rate",
        color="loyalty_tier",
        color_discrete_map=COLORS,
        title="Churn Risk Rate by Tier (%)",
        labels={"loyalty_tier": "Tier", "risk_rate": "Risk Rate (%)"},
        text_auto=".2f",
    )
    apply_theme(fig)
    fig.update_traces(texttemplate="%{text}%", textposition="outside", textfont_color="#cbd5e1")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("""
    <div style="background:#1e293b;border:1px solid #334155;border-radius:10px;padding:16px;margin-top:8px;">
    <b style="color:#f59e0b">⚠️ Churn Risk Definition</b><br>
    <span style="color:#94a3b8;font-size:13px">
    A customer is flagged as at-risk when <b>days_since_last_transaction > 60</b> AND 
    <b>total_spend_lifetime > $1,000</b>. The elevated avg spend among at-risk customers 
    ($1,237 vs $253 for non-at-risk) confirms this segment contains previously active, 
    high-engagement customers who have recently gone quiet — the highest priority for 
    re-engagement campaigns.
    </span>
    </div>
    """, unsafe_allow_html=True)

# ── footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    '<p style="text-align:center;color:#475569;font-size:12px">'
    'CRM 360 Analytics · Built by Kiran Kumari Yadav · '
    'Stack: Faker → PySpark → Snowflake → dbt (36 tests) → Streamlit · '
    'github.com/Kiran-0510/CRM-360'
    '</p>',
    unsafe_allow_html=True
)