import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
import plotly.express as px
import time
import random

from streamlit_gsheets import GSheetsConnection

import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(page_title="Truck Sequencing Live", layout="wide")

# ============================================================
# CONNECTION (READ)
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
# WRITE HELPERS (gspread) - insert into first blank row
# ============================================================
def _get_gspread_client():
    cfg = st.secrets["connections"]["gsheets"]

    # Supports both nested and flat secrets
    if "service_account" in cfg:
        sa = cfg["service_account"]
    else:
        sa = cfg

    creds_dict = {
        "type": "service_account",
        "project_id": sa["project_id"],
        "private_key_id": sa["private_key_id"],
        "private_key": sa["private_key"],
        "client_email": sa["client_email"],
        "client_id": sa["client_id"],
        "token_uri": sa.get("token_uri", "https://oauth2.googleapis.com/token"),
    }

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def _open_spreadsheet():
    cfg = st.secrets["connections"]["gsheets"]
    spreadsheet = cfg["spreadsheet"]
    gc = _get_gspread_client()

    if "docs.google.com" in spreadsheet:
        return gc.open_by_url(spreadsheet)
    return gc.open_by_key(spreadsheet)

def _first_blank_row_in_colA(ws, start_row=2, scan_rows=5000):
    rng = f"A{start_row}:A{scan_rows}"
    vals = ws.get(rng)

    for i, row in enumerate(vals, start=start_row):
        if not row or str(row[0]).strip() == "":
            return i

    return scan_rows + 1

def _colnum_to_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def push_input_rows_to_data_main(input_df: pd.DataFrame, selected_idx: list[int]):
    """
    Writes selected input_df rows into Data Main Sheet,
    placing them at the first blank row (based on column A).
    Mapping is done by matching column headers.
    """
    if not selected_idx:
        return 0

    sh = _open_spreadsheet()
    ws_main = sh.worksheet("Data Main Sheet")

    main_headers = ws_main.row_values(1)
    if not main_headers:
        raise ValueError("Data Main Sheet row 1 has no headers.")

    main_col_count = len(main_headers)
    last_col_letter = _colnum_to_letter(main_col_count)

    start_row = _first_blank_row_in_colA(ws_main, start_row=2, scan_rows=5000)

    values_to_write = []
    for ridx in selected_idx:
        row_series = input_df.iloc[ridx]
        row_dict = row_series.to_dict()

        aligned = []
        for h in main_headers:
            aligned.append(row_dict.get(h, ""))

        values_to_write.append(aligned)

    end_row = start_row + len(values_to_write) - 1
    target_range = f"A{start_row}:{last_col_letter}{end_row}"

    ws_main.update(target_range, values_to_write)
    return len(values_to_write)

# ============================================================
# SIDEBAR
# ============================================================
st.sidebar.title("Truck Sequencing Live")

page = st.sidebar.radio(
    "Menu",
    [
        "Dashboard",
        "Input (Push to Data Main)",
        "Truck_Priority",
        "SKU MASTER",
        "Truck_LoadPlan",
        "Data Main Sheet",
        "Sequencing (Row Rank)",
    ],
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
# LOAD SHEETS (READ)
# ============================================================
data_main = read_ws("Data Main Sheet")
sku_master = read_ws("SKU MASTER")
truck_lp = read_ws("Truck_LoadPlan", header=6)        # headers row 7
truck_priority = read_ws("Truck_Priority", header=8)  # headers row 9

# Normalize main
if not data_main.empty and "Earliest Delivery Date" in data_main.columns:
    data_main["Earliest Delivery Date"] = data_main["Earliest Delivery Date"].apply(safe_dt)
if "Qnt(Bag)" in data_main.columns:
    data_main["Qnt(Bag)"] = to_num(data_main["Qnt(Bag)"])

# Detect FIRST worksheet name via gspread
try:
    sh = _open_spreadsheet()
    worksheet_titles = [w.title for w in sh.worksheets()]
    FIRST_SHEET_NAME = worksheet_titles[0] if worksheet_titles else None
except Exception:
    FIRST_SHEET_NAME = None

# Read FIRST sheet with headers row 5 => header index = 4
input_sheet_df = read_ws(FIRST_SHEET_NAME, header=4) if FIRST_SHEET_NAME else pd.DataFrame()

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
            st.plotly_chart(px.line(daily, x="EDD_Date", y="Qnt(Bag)", markers=True), use_container_width=True)

    with right:
        if "Truck ID/Name" in df.columns and "Qnt(Bag)" in df.columns:
            top = df.groupby("Truck ID/Name", as_index=False)["Qnt(Bag)"].sum().sort_values("Qnt(Bag)", ascending=False).head(15)
            st.plotly_chart(px.bar(top, x="Truck ID/Name", y="Qnt(Bag)"), use_container_width=True)

    st.subheader("Filtered Data")
    q = st.text_input("Search")
    st.dataframe(table_search(df, q), use_container_width=True)

# ============================================================
# PAGE: INPUT
# ============================================================
elif page == "Input (Push to Data Main)":
    st.title("Input Sheet")  # BIG TITLE AS REQUESTED

    if not FIRST_SHEET_NAME:
        st.error("Could not detect the first worksheet name.")
        st.stop()

    st.caption(f"Detected 1st tab: **{FIRST_SHEET_NAME}** (Headers row = 5)")

    if input_sheet_df.empty:
        st.info("Input sheet is empty or could not be read.")
        st.stop()

    # Only show columns A..C
    input_subset = input_sheet_df.iloc[:, 0:3] if input_sheet_df.shape[1] >= 3 else input_sheet_df

    st.subheader("Input Data (A–C)")
    q = st.text_input("Search Input Sheet (A–C)")
    view = table_search(input_subset, q).reset_index(drop=True)

    view2 = view.copy()
    view2.insert(0, "✅ Push?", False)

    edited = st.data_editor(
        view2,
        use_container_width=True,
        num_rows="dynamic",
        key="input_editor"
    )

    st.markdown("### Push selected rows into **Data Main Sheet**")
    if st.button("🚀 Push Now"):
        selected_mask = edited["✅ Push?"] == True
        if selected_mask.sum() == 0:
            st.warning("No rows selected.")
        else:
            selected_idx = edited.index[selected_mask].tolist()

            try:
                pushed_count = push_input_rows_to_data_main(view, selected_idx)
                st.success(f"Pushed {pushed_count} row(s) into Data Main Sheet (first blank row in column A).")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Failed to push rows: {e}")

# ============================================================
# PAGE: TRUCK PRIORITY (G-K)
# ============================================================
elif page == "Truck_Priority":
    st.title("⭐ Truck_Priority (Real Sequencing)")

    if truck_priority.empty:
        st.info("Truck_Priority sheet is empty or not found.")
        st.stop()

    subset = truck_priority.iloc[:, 6:11] if truck_priority.shape[1] >= 11 else truck_priority

    q = st.text_input("Search Truck_Priority (G–K)")
    st.dataframe(table_search(subset, q), use_container_width=True)

# ============================================================
# PAGE: SKU MASTER (A-E)
# ============================================================
elif page == "SKU MASTER":
    st.title("📦 SKU MASTER")

    if sku_master.empty:
        st.info("SKU MASTER is empty or not found.")
        st.stop()

    subset = sku_master.iloc[:, 0:5] if sku_master.shape[1] >= 5 else sku_master

    q = st.text_input("Search SKU MASTER (A–E)")
    st.dataframe(table_search(subset, q), use_container_width=True)

# ============================================================
# PAGE: TRUCK LOADPLAN (VIEW ONLY)
# ============================================================
elif page == "Truck_LoadPlan":
    st.title("🧾 Truck_LoadPlan (View only)")
    st.dataframe(truck_lp, use_container_width=True)

# ============================================================
# PAGE: DATA MAIN SHEET
# ============================================================
elif page == "Data Main Sheet":
    st.title("📄 Data Main Sheet")
    df = filter_by_edd(data_main, from_dt, to_dt_excl)
    q = st.text_input("Search Data Main Sheet")
    st.dataframe(table_search(df, q), use_container_width=True)

# ============================================================
# PAGE: SEQUENCING (ROW RANK)
# ============================================================
else:
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
