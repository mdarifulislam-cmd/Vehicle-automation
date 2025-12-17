import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
import plotly.express as px
import time
import random
import re

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

def fmt_time_12h(dt: datetime) -> str:
    return dt.strftime("%I:%M %p")  # 09:23 PM

def parse_time_to_hms(t: str):
    if t is None:
        return None
    s = str(t).strip().upper()
    fmts = ["%I:%M %p", "%I:%M:%S %p", "%H:%M:%S", "%H:%M"]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            return dt.hour, dt.minute, dt.second
        except Exception:
            pass
    m = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?$", s)
    if not m:
        return None
    h = int(m.group(1))
    mi = int(m.group(2))
    se = int(m.group(3) or 0)
    ap = m.group(4)
    if ap:
        if ap == "PM" and h != 12:
            h += 12
        if ap == "AM" and h == 12:
            h = 0
    return h, mi, se

def merge_delivery_datetime(delivery_date_val, delivery_time_val) -> str:
    """
    Output exactly: 12/1/2025 3:17:00
    """
    d = pd.to_datetime(delivery_date_val, errors="coerce")
    if pd.isna(d):
        return ""
    hms = parse_time_to_hms(delivery_time_val)
    if not hms:
        return ""
    h, mi, se = hms
    dt = datetime(d.year, d.month, d.day, h, mi, se)
    return f"{dt.month}/{dt.day}/{dt.year} {dt.hour}:{dt.minute:02d}:{dt.second:02d}"

# ============================================================
# WRITE HELPERS (gspread)
# ============================================================
def _get_gspread_client():
    cfg = st.secrets["connections"]["gsheets"]
    sa = cfg["service_account"] if "service_account" in cfg else cfg

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

def _first_blank_row_in_colA(ws, start_row=2):
    """
    Finds first blank row in Column A without touching formulas.
    """
    max_row = ws.row_count
    colA = ws.col_values(1)
    if len(colA) < max_row:
        colA = colA + [""] * (max_row - len(colA))

    for r in range(start_row, max_row + 1):
        if str(colA[r - 1]).strip() == "":
            return r
    return max_row + 1

def _ensure_rows(ws, needed_last_row: int):
    if needed_last_row <= ws.row_count:
        return
    ws.add_rows(needed_last_row - ws.row_count)

def _safe_cell_updates(ws, updates: dict):
    """
    updates = {"A362": "value", "B362": "value", ...}
    Writes ONLY these cells. Does not clear any others.
    """
    cells = []
    for addr, val in updates.items():
        c = ws.acell(addr)  # fetch cell object
        c.value = val
        cells.append(c)
    ws.update_cells(cells, value_input_option="USER_ENTERED")

def push_rows_SAFE_no_formula_damage(input_full_df: pd.DataFrame, selected_idx: list[int]):
    """
    ✅ Writes only specific cells in Data Main Sheet row (NO range writes),
       so ARRAYFORMULA and other formulas stay untouched.
    ✅ Only writes within columns A..I, and only non-empty values.
    ✅ Column A is merged Delivery Date + Delivery Time into datetime string.
    """
    if not selected_idx:
        return 0

    sh = _open_spreadsheet()
    ws_main = sh.worksheet("Data Main Sheet")

    start_row = _first_blank_row_in_colA(ws_main, start_row=2)
    end_row = start_row + len(selected_idx) - 1
    _ensure_rows(ws_main, end_row)

    # Build name map from input df columns (case-insensitive)
    colmap = {str(c).strip().lower(): c for c in input_full_df.columns}

    # Input columns we *try* to use (best effort)
    # If your input sheet has different names, it still won’t clear anything —
    # it just won’t fill those fields.
    key_delivery_date = colmap.get("delivery date")
    key_delivery_time = colmap.get("delivery time")
    key_sku = colmap.get("sku")
    key_sku_id = colmap.get("sku id")
    key_qty = colmap.get("enter quantity") or colmap.get("qty") or colmap.get("quantity")
    key_truck = colmap.get("truck id/name") or colmap.get("truck") or colmap.get("truck id")

    # We only ever write A..I (and only if we have values)
    written = 0
    for offset, ridx in enumerate(selected_idx):
        target_r = start_row + offset
        row = input_full_df.iloc[ridx].to_dict()

        updates = {}

        # A = merged datetime
        if key_delivery_date and key_delivery_time:
            merged = merge_delivery_datetime(row.get(key_delivery_date), row.get(key_delivery_time))
            if merged:
                updates[f"A{target_r}"] = merged

        # Optional mappings to B..I (ONLY if value exists)
        # You can adjust the target columns later if your Data Main layout differs.
        if key_sku:
            v = str(row.get(key_sku, "")).strip()
            if v:
                updates[f"B{target_r}"] = v

        if key_sku_id:
            v = str(row.get(key_sku_id, "")).strip()
            if v:
                updates[f"C{target_r}"] = v

        if key_qty:
            v = row.get(key_qty, "")
            if v != "" and v is not None:
                updates[f"D{target_r}"] = v

        if key_truck:
            v = str(row.get(key_truck, "")).strip()
            if v:
                updates[f"E{target_r}"] = v

        # ✅ Only write if we actually have something to write
        if updates:
            _safe_cell_updates(ws_main, updates)
            written += 1

    return written

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

# Detect FIRST worksheet name via gspread
try:
    sh = _open_spreadsheet()
    worksheet_titles = [w.title for w in sh.worksheets()]
    FIRST_SHEET_NAME = worksheet_titles[0] if worksheet_titles else None
except Exception:
    FIRST_SHEET_NAME = None

# Read FIRST sheet with headers row 5 => header index = 4
input_sheet_df_full = read_ws(FIRST_SHEET_NAME, header=4) if FIRST_SHEET_NAME else pd.DataFrame()

# ============================================================
# PAGES
# ============================================================
if page == "Dashboard":
    st.title("🚚 Dashboard")
    st.info("Dashboard unchanged. (Optional: I can add your KPIs here.)")

elif page == "Input (Push to Data Main)":
    st.title("Input Sheet")

    if not FIRST_SHEET_NAME:
        st.error("Could not detect the first worksheet name.")
        st.stop()

    st.caption(f"Detected 1st tab: **{FIRST_SHEET_NAME}** (Headers row = 5)")

    # Show only A..C as you wanted earlier
    if input_sheet_df_full.empty:
        st.info("Input sheet is empty or could not be read.")
        st.stop()

    input_subset = input_sheet_df_full.iloc[:, 0:3] if input_sheet_df_full.shape[1] >= 3 else input_sheet_df_full

    st.subheader("Part 2: Input Table (A–C) → Push to Data Main Sheet")
    q = st.text_input("Search Input Table (A–C)")
    view = table_search(input_subset, q).reset_index(drop=True)

    view2 = view.copy()
    view2.insert(0, "✅ Push?", False)

    edited = st.data_editor(view2, use_container_width=True, num_rows="dynamic", key="input_editor")

    if st.button("🚀 Push Selected Rows to Data Main"):
        selected_mask = edited["✅ Push?"] == True
        if selected_mask.sum() == 0:
            st.warning("No rows selected.")
        else:
            selected_idx = edited.index[selected_mask].tolist()
            try:
                pushed = push_rows_SAFE_no_formula_damage(input_sheet_df_full, selected_idx)
                st.success(
                    f"Pushed {pushed} row(s) safely. "
                    f"✅ Only writes specific cells (no range writes), so formulas/ARRAYFORMULA stay untouched."
                )
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Failed to push rows: {e}")

elif page == "Truck_Priority":
    st.title("⭐ Truck_Priority (G–K only)")
    if truck_priority.empty:
        st.info("Truck_Priority sheet is empty or not found.")
        st.stop()
    subset = truck_priority.iloc[:, 6:11] if truck_priority.shape[1] >= 11 else truck_priority
    st.dataframe(subset, use_container_width=True)

elif page == "SKU MASTER":
    st.title("📦 SKU MASTER (A–E only)")
    if sku_master.empty:
        st.info("SKU MASTER is empty or not found.")
        st.stop()
    subset = sku_master.iloc[:, 0:5] if sku_master.shape[1] >= 5 else sku_master
    st.dataframe(subset, use_container_width=True)

elif page == "Truck_LoadPlan":
    st.title("🧾 Truck_LoadPlan (View only)")
    st.dataframe(truck_lp, use_container_width=True)

elif page == "Data Main Sheet":
    st.title("📄 Data Main Sheet")
    st.dataframe(data_main, use_container_width=True)

else:
    st.title("🔢 Sequencing (Row Rank)")
    st.info("Sequencing unchanged. (We can rebuild it once your input/write is stable.)")
