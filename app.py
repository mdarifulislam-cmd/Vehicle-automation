import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
import plotly.express as px
from streamlit_gsheets import GSheetsConnection

st.set_page_config(page_title="Truck Sequencing Live", layout="wide")
conn = st.connection("gsheets", type=GSheetsConnection)

# ---------------------------
# Helpers
# ---------------------------
def safe_dt(x):
    if pd.isna(x):
        return pd.NaT
    if isinstance(x, (pd.Timestamp, datetime)):
        return pd.to_datetime(x)
    return pd.to_datetime(str(x), errors="coerce")

def read_ws(ws: str, ttl: int = 10) -> pd.DataFrame:
    df = conn.read(worksheet=ws, ttl=ttl)
    return df if df is not None else pd.DataFrame()

def read_ws_header_row7(ws: str, ttl: int = 10) -> pd.DataFrame:
    """
    Truck_LoadPlan has headers on row 7.
    We read the sheet, then rebuild a dataframe with row 7 as column names.
    """
    raw = read_ws(ws, ttl=ttl)
    if raw.empty:
        return raw

    # Turn current columns into generic, so we can treat all rows as data
    tmp = raw.copy()
    tmp.columns = [f"col_{i}" for i in range(tmp.shape[1])]

    # In conn.read(), row 1 is header, and data starts at original row 2.
    # So original row 7 is index 5 in tmp (7 - 2).
    header_idx = 5
    if header_idx >= len(tmp):
        return pd.DataFrame()

    new_cols = tmp.iloc[header_idx].astype(str).tolist()
    df = tmp.iloc[header_idx + 1 :].copy()
    df.columns = new_cols
    df = df.reset_index(drop=True)
    df = df.dropna(how="all")
    return df

def append_loadplan_row(row: dict):
    """
    Append a row to Truck_LoadPlan while matching the exact columns.
    Missing fields stay blank.
    """
    cols = ["Truck ID/Name", "SKU_ID", "Qty", "SKU NAME", "Truck Rank", "Line Score"]
    out = {c: row.get(c, "") for c in cols}
    out["SavedAt"] = datetime.now().isoformat(timespec="seconds")
    conn.append(worksheet="Truck_LoadPlan", data=pd.DataFrame([out]))

# ---------------------------
# Load sheets
# ---------------------------
data_main = read_ws("Data Main Sheet", ttl=10)
sku_master = read_ws("SKU MASTER", ttl=60)
truck_lp = read_ws_header_row7("Truck_LoadPlan", ttl=10)

# Normalize Data Main Sheet EDD
if not data_main.empty and "Earliest Delivery Date" in data_main.columns:
    data_main["Earliest Delivery Date"] = data_main["Earliest Delivery Date"].apply(safe_dt)

# ---------------------------
# Sidebar
# ---------------------------
st.sidebar.title("Truck Sequencing Live")
page = st.sidebar.radio("Page", ["Dashboard", "Truck_LoadPlan", "Sequencing (Row Rank)", "SKU MASTER", "Data Main Sheet"])

st.sidebar.markdown("### Date Range (EDD)")
from_date = st.sidebar.date_input("From", value=date(2025, 12, 12))
to_date = st.sidebar.date_input("To", value=date(2025, 12, 18))

from_dt = pd.to_datetime(from_date)
to_dt = pd.to_datetime(to_date) + pd.Timedelta(days=1)

def filter_by_edd(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Earliest Delivery Date" not in df.columns:
        return df
    return df[(df["Earliest Delivery Date"] >= from_dt) & (df["Earliest Delivery Date"] < to_dt)]

# =========================================================
# Dashboard
# =========================================================
if page == "Dashboard":
    st.title("🚚 Live Dashboard")

    df = filter_by_edd(data_main)
    if df.empty:
        st.info("No data found for selected EDD date range.")
        st.stop()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Rows (range)", f"{len(df):,}")

    if "Truck ID/Name" in df.columns:
        c2.metric("Unique Trucks", f"{df['Truck ID/Name'].nunique():,}")
    if "SKU ID" in df.columns:
        c3.metric("Unique SKUs", f"{df['SKU ID'].nunique():,}")
    if "Qnt(Bag)" in df.columns:
        df["Qnt(Bag)"] = pd.to_numeric(df["Qnt(Bag)"], errors="coerce").fillna(0)
        c4.metric("Total Bags", f"{df['Qnt(Bag)'].sum():,.0f}")

    st.divider()

    if "Earliest Delivery Date" in df.columns and "Qnt(Bag)" in df.columns:
        tmp = df.copy()
        tmp["EDD_Date"] = tmp["Earliest Delivery Date"].dt.date
        daily = tmp.groupby("EDD_Date", as_index=False)["Qnt(Bag)"].sum()
        fig = px.line(daily, x="EDD_Date", y="Qnt(Bag)", markers=True, title="Total Bags by EDD Date")
        st.plotly_chart(fig, use_container_width=True)

    if "Truck ID/Name" in df.columns and "Qnt(Bag)" in df.columns:
        top = df.groupby("Truck ID/Name", as_index=False)["Qnt(Bag)"].sum().sort_values("Qnt(Bag)", ascending=False).head(15)
        fig2 = px.bar(top, x="Truck ID/Name", y="Qnt(Bag)", title="Top 15 Trucks by Bags (Range)")
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Filtered Data (Live)")
    st.dataframe(df, use_container_width=True)

# =========================================================
# Truck_LoadPlan (header row 7)
# =========================================================
elif page == "Truck_LoadPlan":
    st.title("🧾 Truck_LoadPlan (Live)")

    st.caption("This tab has headers on row 7. The app reads it correctly and appends new rows.")

    left, right = st.columns([1, 1])

    with left:
        st.subheader("Add a new row")
        with st.form("add_lp"):
            truck = st.text_input("Truck ID/Name")
            sku_id = st.text_input("SKU_ID (e.g., A8)")
            qty = st.number_input("Qty", min_value=0, step=1)
            sku_name = st.text_input("SKU NAME (optional)")
            truck_rank = st.text_input("Truck Rank (optional)")
            line_score = st.text_input("Line Score (optional)")
            submit = st.form_submit_button("✅ Save to Google Sheet")

        if submit:
            if not truck or not sku_id or qty <= 0:
                st.error("Truck ID/Name, SKU_ID, and Qty are required.")
            else:
                append_loadplan_row({
                    "Truck ID/Name": truck.strip(),
                    "SKU_ID": sku_id.strip(),
                    "Qty": int(qty),
                    "SKU NAME": sku_name.strip(),
                    "Truck Rank": truck_rank.strip(),
                    "Line Score": line_score.strip(),
                })
                st.success("Saved! Refreshing…")
                st.rerun()

    with right:
        st.subheader("Current Truck_LoadPlan data")
        st.dataframe(truck_lp, use_container_width=True)

# =========================================================
# Sequencing (Row Rank) - per row/truck by EDD in range
# =========================================================
elif page == "Sequencing (Row Rank)":
    st.title("🔢 Sequencing (Row Rank per row by EDD)")

    df = filter_by_edd(data_main).copy()
    if df.empty:
        st.info("No main data in selected range.")
        st.stop()

    if "Truck ID/Name" not in df.columns or "Earliest Delivery Date" not in df.columns:
        st.error("Data Main Sheet must include Truck ID/Name and Earliest Delivery Date.")
        st.stop()

    # Unique row rank: sort by EDD then Truck ID
    df = df.sort_values(["Earliest Delivery Date", "Truck ID/Name"], ascending=[True, True]).reset_index(drop=True)
    df["Row Rank (EDD)"] = np.arange(1, len(df) + 1)

    st.dataframe(df, use_container_width=True)

# =========================================================
# SKU MASTER
# =========================================================
elif page == "SKU MASTER":
    st.title("📦 SKU MASTER")
    st.dataframe(sku_master, use_container_width=True)

# =========================================================
# Data Main Sheet
# =========================================================
else:
    st.title("📄 Data Main Sheet (Live)")
    st.dataframe(filter_by_edd(data_main), use_container_width=True)
