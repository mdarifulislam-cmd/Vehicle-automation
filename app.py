import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
import plotly.express as px
import time
import random
from streamlit_gsheets import GSheetsConnection

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(page_title="Truck Sequencing Live", layout="wide")

# ============================================================
# CONNECTION
# ============================================================
conn = st.connection("gsheets", type=GSheetsConnection)

# ============================================================
# HELPERS
# ============================================================
def safe_dt(x):
    if pd.isna(x):
        return pd.NaT
    if isinstance(x, (pd.Timestamp, datetime)):
        return pd.to_datetime(x)
    return pd.to_datetime(str(x), errors="coerce")

def to_num(s):
    return pd.to_numeric(s, errors="coerce").fillna(0)

def table_search(df: pd.DataFrame, q: str) -> pd.DataFrame:
    if df.empty:
        return df
    q = (q or "").strip().lower()
    if not q:
        return df
    mask = pd.Series(False, index=df.index)
    for c in df.columns:
        mask |= df[c].astype(str).str.lower().str.contains(q, na=False)
    return df[mask]

@st.cache_data(show_spinner=False, ttl=15)
def _cached_read(worksheet: str, header: int | None):
    if header is None:
        return conn.read(worksheet=worksheet)
    return conn.read(worksheet=worksheet, header=header)

def read_ws(worksheet: str, header: int | None = None) -> pd.DataFrame:
    """
    Safe reader:
    - Caches results to avoid repeated API calls during reruns
    - Retries transient API failures (rate limit / 500 / 503 / timeouts)
    """
    max_tries = 5
    base_sleep = 0.7

    for i in range(max_tries):
        try:
            df = _cached_read(worksheet, header)
            return df if df is not None else pd.DataFrame()
        except Exception as e:
            msg = str(e).lower()
            transient = any(x in msg for x in ["429", "rate", "quota", "500", "503", "timeout", "temporarily"])
            if (not transient) or (i == max_tries - 1):
                st.error(f"Google Sheets API error while reading '{worksheet}'.\n\n{e}")
                return pd.DataFrame()
            time.sleep((base_sleep * (2 ** i)) + random.uniform(0, 0.3))

def filter_by_edd(df: pd.DataFrame, from_dt: pd.Timestamp, to_dt_excl: pd.Timestamp) -> pd.DataFrame:
    if df.empty or "Earliest Delivery Date" not in df.columns:
        return df
    return df[(df["Earliest Delivery Date"] >= from_dt) & (df["Earliest Delivery Date"] < to_dt_excl)]

# ============================================================
# SIDEBAR
# ============================================================
st.sidebar.title("Truck Sequencing Live")

page = st.sidebar.radio(
    "Menu",
    ["Dashboard", "Truck_LoadPlan", "Truck_Priority", "Sequencing (Row Rank)", "SKU MASTER", "Data Main Sheet"]
)

st.sidebar.markdown("### Date Range (Earliest Delivery Date)")
from_date = st.sidebar.date_input("From", value=date(2025, 12, 12))
to_date = st.sidebar.date_input("To", value=date(2025, 12, 18))

from_dt = pd.to_datetime(from_date)
to_dt_excl = pd.to_datetime(to_date) + pd.Timedelta(days=1)

if st.sidebar.button("🔄 Refresh data"):
    st.cache_data.clear()
    st.rerun()

# ============================================================
# LOAD DATA (LIVE)
# ============================================================
data_main = read_ws("Data Main Sheet")
sku_master = read_ws("SKU MASTER")

# Truck_LoadPlan headers on row 7 => header index 6
truck_lp = read_ws("Truck_LoadPlan", header=6)

# Truck_Priority headers on row 9 => header index 8
truck_priority = read_ws("Truck_Priority", header=8)

# Normalize Data Main Sheet
if not data_main.empty and "Earliest Delivery Date" in data_main.columns:
    data_main["Earliest Delivery Date"] = data_main["Earliest Delivery Date"].apply(safe_dt)

if "Qnt(Bag)" in data_main.columns:
    data_main["Qnt(Bag)"] = to_num(data_main["Qnt(Bag)"])

# ============================================================
# PAGE: DASHBOARD
# ============================================================
if page == "Dashboard":
    st.title("🚚 Dashboard")

    df = filter_by_edd(data_main, from_dt, to_dt_excl)
    if df.empty:
        st.info("No data found in selected date range.")
        st.stop()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Rows", f"{len(df):,}")
    if "Truck ID/Name" in df.columns:
        c2.metric("Trucks", df["Truck ID/Name"].nunique())
    if "SKU ID" in df.columns:
        c3.metric("SKUs", df["SKU ID"].nunique())
    if "Qnt(Bag)" in df.columns:
        c4.metric("Total Bags", f"{df['Qnt(Bag)'].sum():,.0f}")

    st.divider()

    left, right = st.columns(2)

    with left:
        if "Earliest Delivery Date" in df.columns and "Qnt(Bag)" in df.columns:
            tmp = df.copy()
            tmp["EDD_Date"] = tmp["Earliest Delivery Date"].dt.date
            daily = tmp.groupby("EDD_Date", as_index=False)["Qnt(Bag)"].sum()
            fig = px.line(daily, x="EDD_Date", y="Qnt(Bag)", markers=True, title="Total Bags by EDD Date")
            st.plotly_chart(fig, use_container_width=True)

    with right:
        if "Truck ID/Name" in df.columns and "Qnt(Bag)" in df.columns:
            top = df.groupby("Truck ID/Name", as_index=False)["Qnt(Bag)"].sum().sort_values("Qnt(Bag)", ascending=False).head(15)
            fig2 = px.bar(top, x="Truck ID/Name", y="Qnt(Bag)", title="Top 15 Trucks by Bags")
            st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Filtered Data")
    q = st.text_input("Search")
    st.dataframe(table_search(df, q), use_container_width=True)

# ============================================================
# PAGE: TRUCK LOADPLAN
# ============================================================
elif page == "Truck_LoadPlan":
    st.title("🧾 Truck_LoadPlan (Live)")
    st.caption("Headers row = 7 (header=6).")

    st.subheader("Current Truck_LoadPlan")
    st.dataframe(truck_lp, use_container_width=True)

    st.divider()
    st.subheader("Add new row")

    with st.form("add_lp"):
        truck = st.text_input("Truck ID/Name")
        sku_id = st.text_input("SKU_ID")
        qty = st.number_input("Qty", min_value=0, step=1)
        sku_name = st.text_input("SKU NAME (optional)", value="")
        truck_rank = st.text_input("Truck Rank (optional)", value="")
        line_score = st.text_input("Line Score (optional)", value="")
        submitted = st.form_submit_button("✅ Save")

    # ✅ INDENTATION IS CORRECT HERE
    if submitted:
        if (not truck.strip()) or (not sku_id.strip()) or (qty <= 0):
            st.error("Truck ID/Name, SKU_ID and Qty are required.")
        else:
            new_row = pd.DataFrame([{
                "Truck ID/Name": truck.strip(),
                "SKU_ID": sku_id.strip(),
                "Qty": int(qty),
                "SKU NAME": sku_name.strip(),
                "Truck Rank": truck_rank.strip(),
                "Line Score": line_score.strip(),
                "SavedAt": datetime.now().isoformat(timespec="seconds"),
            }])

            conn.append(worksheet="Truck_LoadPlan", data=new_row)

            st.success("Saved! Refreshing…")
            st.cache_data.clear()
            st.rerun()

# ============================================================
# PAGE: TRUCK PRIORITY
# ============================================================
elif page == "Truck_Priority":
    st.title("⭐ Truck_Priority (Real Sequencing)")
    st.caption("Headers row = 9 (header=8).")

    if truck_priority.empty:
        st.info("Truck_Priority sheet is empty or not found.")
        st.stop()

    q = st.text_input("Search Truck_Priority")
    view = table_search(truck_priority, q)
    st.dataframe(view, use_container_width=True)

    st.download_button(
        "⬇️ Download Truck_Priority CSV",
        data=view.to_csv(index=False).encode("utf-8"),
        file_name="truck_priority.csv",
        mime="text/csv",
    )

# ============================================================
# PAGE: SEQUENCING (ROW RANK)
# ============================================================
elif page == "Sequencing (Row Rank)":
    st.title("🔢 Sequencing (Row Rank by EDD)")

    df = filter_by_edd(data_main, from_dt, to_dt_excl)
    if df.empty:
        st.info("No rows in selected range.")
        st.stop()

    if "Truck ID/Name" not in df.columns or "Earliest Delivery Date" not in df.columns:
        st.error("Data Main Sheet must include 'Truck ID/Name' and 'Earliest Delivery Date'.")
        st.stop()

    ranked = df.sort_values(
        ["Earliest Delivery Date", "Truck ID/Name"],
        ascending=[True, True]
    ).reset_index(drop=True)

    ranked["Row Rank"] = np.arange(1, len(ranked) + 1)

    q = st.text_input("Search ranked data")
    st.dataframe(table_search(ranked, q), use_container_width=True)

# ============================================================
# PAGE: SKU MASTER
# ============================================================
elif page == "SKU MASTER":
    st.title("📦 SKU MASTER")

    if sku_master.empty:
        st.info("SKU MASTER is empty or not found.")
        st.stop()

    q = st.text_input("Search SKU MASTER")
    st.dataframe(table_search(sku_master, q), use_container_width=True)

# ============================================================
# PAGE: DATA MAIN SHEET
# ============================================================
else:
    st.title("📄 Data Main Sheet")

    df = filter_by_edd(data_main, from_dt, to_dt_excl)
    q = st.text_input("Search Data Main Sheet")
    st.dataframe(table_search(df, q), use_container_width=True)
