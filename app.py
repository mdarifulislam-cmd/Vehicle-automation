import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
import time
import random
import re

from streamlit_gsheets import GSheetsConnection
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(page_title="Truck Sequencing Live", layout="wide")
conn = st.connection("gsheets", type=GSheetsConnection)

# ============================================================
# READ HELPERS
# ============================================================
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

@st.cache_data(show_spinner=False, ttl=20)
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

# ============================================================
# FORMATTING / VALIDATION
# ============================================================
def fmt_time_12h(now: datetime) -> str:
    return now.strftime("%I:%M %p")  # 09:23 PM

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
    Output: M/D/YYYY H:MM:SS  (example: 12/1/2025 3:17:00)
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

TIME_12H_PATTERN = r"^(0[1-9]|1[0-2]):[0-5][0-9]\s?(AM|PM)$"
TRUCK_PATTERN = r"^[A-Z]{2}-[A-Z]{2}-\d{6}$"

# ============================================================
# GSHEET WRITE (gspread)
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

def first_blank_row_colA(ws, start_row=2) -> int:
    """
    Finds first blank row in column A (trimmed).
    If data ends at 361, returns 362.
    """
    max_row = ws.row_count
    colA = ws.col_values(1)
    if len(colA) < max_row:
        colA = colA + [""] * (max_row - len(colA))
    for r in range(start_row, max_row + 1):
        if str(colA[r - 1]).strip() == "":
            return r
    return max_row + 1

def ensure_rows(ws, needed_last_row: int):
    if needed_last_row > ws.row_count:
        ws.add_rows(needed_last_row - ws.row_count)

def batch_update_cells(ws, cell_to_value: dict):
    """
    Writes ONLY given cells (no rectangle writes).
    This protects ARRAYFORMULA (J onward).
    """
    body = {"valueInputOption": "USER_ENTERED", "data": []}
    for a1, val in cell_to_value.items():
        body["data"].append({"range": a1, "values": [[val]]})
    ws.batch_update(body)

# ============================================================
# INPUT SHEET: READ FORM (A labels, B values)
# ============================================================
INPUT_FORM_CELLS = {
    "Delivery Date": "B6",
    "Delivery Time": "B7",
    "SKU": "B8",
    "SKU ID": "B9",  # formula cell
    "Enter Quantity": "B10",
    "Truck ID/Name": "B11",
    "Vehicle Factory In Date": "B12",
    "Vehicle Factory In Time": "B13",
    "Vehicle Factory Out Date": "B14",
    "Vehicle Factory Out Time": "B15",
}

def read_input_form(ws_input) -> dict:
    vals = {}
    for k, addr in INPUT_FORM_CELLS.items():
        vals[k] = ws_input.acell(addr).value
    return vals

# ============================================================
# PUSH: INPUT FORM -> DATA MAIN (A..I ONLY)
# ============================================================
def push_current_input_to_data_main(input_tab_name: str):
    sh = _open_spreadsheet()
    ws_input = sh.worksheet(input_tab_name)
    ws_main = sh.worksheet("Data Main Sheet")

    form = read_input_form(ws_input)

    # Validate required
    if not form["Delivery Date"] or not form["Delivery Time"]:
        raise ValueError("Delivery Date/Time is blank.")
    if not form["SKU"] or not form["SKU ID"]:
        raise ValueError("SKU / SKU ID is blank (check dropdown + formula).")
    if not form["Enter Quantity"]:
        raise ValueError("Enter Quantity is blank.")
    if not form["Truck ID/Name"]:
        raise ValueError("Truck ID/Name is blank.")

    # Merge Delivery Date+Time into Data Main A
    merged_A = merge_delivery_datetime(form["Delivery Date"], form["Delivery Time"])
    if not merged_A:
        raise ValueError("Could not merge Delivery Date+Time. Check time format.")

    # Find next row
    r = first_blank_row_colA(ws_main, start_row=2)
    ensure_rows(ws_main, r)

    # Write ONLY A..I (J onward untouched)
    updates = {
        f"Data Main Sheet!A{r}": merged_A,
        f"Data Main Sheet!B{r}": form["SKU"],
        f"Data Main Sheet!C{r}": form["SKU ID"],
        f"Data Main Sheet!D{r}": form["Enter Quantity"],
        f"Data Main Sheet!E{r}": form["Truck ID/Name"],
        f"Data Main Sheet!F{r}": form["Vehicle Factory In Date"],
        f"Data Main Sheet!G{r}": str(form["Vehicle Factory In Time"]).strip().upper(),
        f"Data Main Sheet!H{r}": form["Vehicle Factory Out Date"],
        f"Data Main Sheet!I{r}": str(form["Vehicle Factory Out Time"]).strip().upper(),
    }

    batch_update_cells(ws_main, updates)
    return r, form

# ============================================================
# SIDEBAR / NAV + DATE FILTER
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

# Detect all worksheet names (so you can choose input tab if needed)
try:
    sh = _open_spreadsheet()
    worksheet_titles = [w.title for w in sh.worksheets()]
except Exception:
    worksheet_titles = []

# Default input tab = first tab (your Input form)
default_input_tab = worksheet_titles[0] if worksheet_titles else ""
input_tab = st.sidebar.selectbox("Input Tab", worksheet_titles, index=0 if worksheet_titles else None)

# ============================================================
# DASHBOARD
# ============================================================
if page == "Dashboard":
    st.title("🚚 Dashboard")

    # Best effort: use "Earliest Delivery Date" if present
    if not data_main.empty and "Earliest Delivery Date" in data_main.columns:
        dm = data_main.copy()
        dm["Earliest Delivery Date"] = pd.to_datetime(dm["Earliest Delivery Date"], errors="coerce")
        dm = dm[(dm["Earliest Delivery Date"] >= from_dt) & (dm["Earliest Delivery Date"] < to_dt_excl)]
    else:
        dm = data_main.copy()

    if dm.empty:
        st.info("No data found for selected date range.")
        st.stop()

    c1, c2, c3 = st.columns(3)
    c1.metric("Rows", f"{len(dm):,}")

    if "Truck ID/Name" in dm.columns:
        c2.metric("Trucks", f"{dm['Truck ID/Name'].nunique():,}")
    if "Qnt(Bag)" in dm.columns:
        qsum = pd.to_numeric(dm["Qnt(Bag)"], errors="coerce").fillna(0).sum()
        c3.metric("Total Qty(Bag)", f"{qsum:,.0f}")

    st.divider()

    if "Truck ID/Name" in dm.columns and "Qnt(Bag)" in dm.columns:
        tmp = dm.copy()
        tmp["Qnt(Bag)"] = pd.to_numeric(tmp["Qnt(Bag)"], errors="coerce").fillna(0)
        top = tmp.groupby("Truck ID/Name", as_index=False)["Qnt(Bag)"].sum().sort_values("Qnt(Bag)", ascending=False).head(15)
        st.plotly_chart(px.bar(top, x="Truck ID/Name", y="Qnt(Bag)"), use_container_width=True)

    st.subheader("Filtered Data Preview")
    st.dataframe(dm.head(200), use_container_width=True)

# ============================================================
# INPUT PAGE
# ============================================================
elif page == "Input (Push to Data Main)":
    st.title("Input Sheet")

    if not input_tab:
        st.error("No input tab detected.")
        st.stop()

    st.caption(f"Input tab selected: **{input_tab}** (Form layout: labels in A, values in B)")

    # -------------------------
    # PART 1: FORM (writes B6..B15)
    # -------------------------
    st.subheader("Part 1: Input Form (writes to cells B6–B15)")

    now = datetime.now()

    with st.form("input_cells_form"):
        c1, c2 = st.columns(2)

        with c1:
            delivery_date = st.date_input("Delivery Date (B6)", value=date.today())
            delivery_time = st.text_input("Delivery Time (B7) - 12 hour (e.g. 09:23 PM)", value=fmt_time_12h(now))
            sku_name = st.text_input("SKU (B8) - dropdown in sheet", value="")  # dropdown exists in sheet UI
            st.caption("Note: In Google Sheet, B8 is dropdown. Here you can type the exact SKU text if needed.")
            qty = st.number_input("Enter Quantity (B10)", min_value=0, step=1)

        with c2:
            truck_id = st.text_input("Truck ID/Name (B11) e.g. DM-TA-224564", value="")
            vin_date = st.date_input("Vehicle Factory In Date (B12)", value=date.today())
            vin_time = st.text_input("Vehicle Factory In Time (B13) - 12 hour", value=fmt_time_12h(now))
            vout_date = st.date_input("Vehicle Factory Out Date (B14)", value=date.today())
            vout_time = st.text_input("Vehicle Factory Out Time (B15) - 12 hour", value=fmt_time_12h(now))

        save_form = st.form_submit_button("✅ Save to Input Sheet")

    if save_form:
        for label, t in [
            ("Delivery Time", delivery_time),
            ("Vehicle Factory In Time", vin_time),
            ("Vehicle Factory Out Time", vout_time),
        ]:
            if re.match(TIME_12H_PATTERN, str(t).strip().upper()) is None:
                st.error(f"{label} must be like 09:23 PM")
                st.stop()

        if truck_id and re.match(TRUCK_PATTERN, truck_id.strip().upper()) is None:
            st.error("Truck ID/Name must be like DM-TA-224564")
            st.stop()

        try:
            sh2 = _open_spreadsheet()
            ws_input = sh2.worksheet(input_tab)

            # IMPORTANT: do NOT touch B9 (formula)
            updates = {
                f"{input_tab}!B6": delivery_date.strftime("%Y-%m-%d"),
                f"{input_tab}!B7": delivery_time.strip().upper(),
                f"{input_tab}!B8": sku_name,  # if sheet has dropdown, it will accept matching option
                f"{input_tab}!B10": int(qty),
                f"{input_tab}!B11": truck_id.strip().upper(),
                f"{input_tab}!B12": vin_date.strftime("%Y-%m-%d"),
                f"{input_tab}!B13": vin_time.strip().upper(),
                f"{input_tab}!B14": vout_date.strftime("%Y-%m-%d"),
                f"{input_tab}!B15": vout_time.strip().upper(),
            }

            body = {"valueInputOption": "USER_ENTERED", "data": []}
            for rng, val in updates.items():
                body["data"].append({"range": rng, "values": [[val]]})

            ws_input.batch_update(body)
            st.success("Saved to Input sheet cells B6–B15 (B9 remains formula).")
            st.cache_data.clear()
            st.rerun()

        except Exception as e:
            st.error(f"Failed to write input cells: {e}")

    st.divider()

    # -------------------------
    # PART 2: LIVE VIEW + PUSH CURRENT INPUT
    # -------------------------
    st.subheader("Part 2: Push current input line to Data Main Sheet")

    try:
        sh_live = _open_spreadsheet()
        ws_input_live = sh_live.worksheet(input_tab)
        form_vals = read_input_form(ws_input_live)

        preview = pd.DataFrame(
            [{"Field": k, "Value": v} for k, v in form_vals.items()]
        )
        st.dataframe(preview, use_container_width=True, hide_index=True)

        if st.button("🚀 Push NOW to Data Main Sheet (next blank row in column A)"):
            row_written, pushed_vals = push_current_input_to_data_main(input_tab)
            st.success(f"✅ Pushed to Data Main Sheet row {row_written}. (Should be 362 if data ends at 361)")
            st.cache_data.clear()
            st.rerun()

    except Exception as e:
        st.error(f"Could not read/push input form: {e}")

# ============================================================
# TRUCK PRIORITY
# ============================================================
elif page == "Truck_Priority":
    st.title("⭐ Truck_Priority (G–K only)")
    if truck_priority.empty:
        st.info("Truck_Priority sheet is empty or not found.")
        st.stop()
    subset = truck_priority.iloc[:, 6:11] if truck_priority.shape[1] >= 11 else truck_priority
    st.dataframe(subset, use_container_width=True)

# ============================================================
# SKU MASTER
# ============================================================
elif page == "SKU MASTER":
    st.title("📦 SKU MASTER (A–E only)")
    if sku_master.empty:
        st.info("SKU MASTER sheet is empty or not found.")
        st.stop()
    subset = sku_master.iloc[:, 0:5] if sku_master.shape[1] >= 5 else sku_master
    st.dataframe(subset, use_container_width=True)

# ============================================================
# TRUCK LOADPLAN
# ============================================================
elif page == "Truck_LoadPlan":
    st.title("🧾 Truck_LoadPlan (View only)")
    st.dataframe(truck_lp, use_container_width=True)

# ============================================================
# DATA MAIN SHEET (FILTERED)
# ============================================================
elif page == "Data Main Sheet":
    st.title("📄 Data Main Sheet (Filtered)")

    dm = data_main.copy()
    if not dm.empty and "Earliest Delivery Date" in dm.columns:
        dm["Earliest Delivery Date"] = pd.to_datetime(dm["Earliest Delivery Date"], errors="coerce")
        dm = dm[(dm["Earliest Delivery Date"] >= from_dt) & (dm["Earliest Delivery Date"] < to_dt_excl)]

    q = st.text_input("Search")
    st.dataframe(table_search(dm, q), use_container_width=True)

# ============================================================
# SEQUENCING (SIMPLE)
# ============================================================
else:
    st.title("🔢 Sequencing (Row Rank)")

    dm = data_main.copy()
    if not dm.empty and "Earliest Delivery Date" in dm.columns:
        dm["Earliest Delivery Date"] = pd.to_datetime(dm["Earliest Delivery Date"], errors="coerce")
        dm = dm[(dm["Earliest Delivery Date"] >= from_dt) & (dm["Earliest Delivery Date"] < to_dt_excl)]

    if dm.empty:
        st.info("No rows in selected date range.")
        st.stop()

    # Sort by EDD then truck if present
    sort_cols = []
    if "Earliest Delivery Date" in dm.columns:
        sort_cols.append("Earliest Delivery Date")
    if "Truck ID/Name" in dm.columns:
        sort_cols.append("Truck ID/Name")

    if sort_cols:
        dm = dm.sort_values(sort_cols).reset_index(drop=True)

    dm.insert(0, "Row Rank", np.arange(1, len(dm) + 1))
    st.dataframe(dm, use_container_width=True)
