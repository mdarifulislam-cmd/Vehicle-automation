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
    Writes ONLY given cells (A..I only). Does not affect J+ formulas.
    """
    body = {"valueInputOption": "USER_ENTERED", "data": []}
    for a1, val in cell_to_value.items():
        body["data"].append({"range": a1, "values": [[val]]})
    ws.batch_update(body)

# ============================================================
# INPUT FORM CELLS (your layout)
# ============================================================
INPUT_FORM_CELLS = {
    "Delivery Date": "B6",
    "Delivery Time": "B7",
    "SKU": "B8",
    "SKU ID": "B9",  # formula - NEVER write it
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

def push_current_input_to_data_main(input_tab_name: str):
    sh = _open_spreadsheet()
    ws_input = sh.worksheet(input_tab_name)
    ws_main = sh.worksheet("Data Main Sheet")

    form = read_input_form(ws_input)

    # basic required checks
    if not form["Delivery Date"] or not form["Delivery Time"]:
        raise ValueError("Delivery Date / Delivery Time is blank.")
    if not form["SKU"]:
        raise ValueError("SKU is blank.")
    if not form["SKU ID"]:
        raise ValueError("SKU ID (B9 formula) is blank. Select SKU first.")
    if not form["Enter Quantity"]:
        raise ValueError("Enter Quantity is blank.")
    if not form["Truck ID/Name"]:
        raise ValueError("Truck ID/Name is blank.")

    merged_A = merge_delivery_datetime(form["Delivery Date"], form["Delivery Time"])
    if not merged_A:
        raise ValueError("Could not merge Delivery Date+Time. Ensure Delivery Time is like 03:42 PM.")

    r = first_blank_row_colA(ws_main, start_row=2)
    ensure_rows(ws_main, r)

    # Write ONLY A..I (J+ untouched)
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
# SIDEBAR / NAV + DATE FILTER (kept)
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
# LOAD SHEETS
# ============================================================
data_main = read_ws("Data Main Sheet")
sku_master = read_ws("SKU MASTER")
truck_lp = read_ws("Truck_LoadPlan", header=6)        # headers row 7
truck_priority = read_ws("Truck_Priority", header=8)  # headers row 9

# Detect input tab: FIRST worksheet (no sidebar option)
try:
    sh = _open_spreadsheet()
    worksheet_titles = [w.title for w in sh.worksheets()]
    INPUT_TAB_NAME = worksheet_titles[0] if worksheet_titles else ""
except Exception:
    INPUT_TAB_NAME = ""

# Build SKU dropdown from SKU MASTER (SKU Name + ID)
sku_name_options = []
sku_id_lookup = {}
if not sku_master.empty and sku_master.shape[1] >= 2:
    names = sku_master.iloc[:, 0].astype(str).fillna("")
    ids = sku_master.iloc[:, 1].astype(str).fillna("")
    for n, sid in zip(names, ids):
        n2 = n.strip()
        if n2:
            sku_name_options.append(n2)
            sku_id_lookup[n2] = sid.strip()

# ============================================================
# DASHBOARD (restore line chart + bar chart)
# ============================================================
if page == "Dashboard":
    st.title("🚚 Dashboard")

    dm = data_main.copy()
    if dm.empty:
        st.info("Data Main Sheet is empty.")
        st.stop()

    # Filter by EDD if available
    if "Earliest Delivery Date" in dm.columns:
        dm["Earliest Delivery Date"] = pd.to_datetime(dm["Earliest Delivery Date"], errors="coerce")
        dm = dm[(dm["Earliest Delivery Date"] >= from_dt) & (dm["Earliest Delivery Date"] < to_dt_excl)]

    if dm.empty:
        st.info("No data found for selected date range.")
        st.stop()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Rows", f"{len(dm):,}")
    if "Truck ID/Name" in dm.columns:
        c2.metric("Trucks", f"{dm['Truck ID/Name'].nunique():,}")
    if "SKU ID" in dm.columns:
        c3.metric("SKUs", f"{dm['SKU ID'].nunique():,}")
    if "Qnt(Bag)" in dm.columns:
        dm["Qnt(Bag)"] = pd.to_numeric(dm["Qnt(Bag)"], errors="coerce").fillna(0)
        c4.metric("Total Bags", f"{dm['Qnt(Bag)'].sum():,.0f}")

    st.divider()

    left, right = st.columns(2)

    # ✅ Line chart (as before): daily bags vs EDD
    with left:
        if "Earliest Delivery Date" in dm.columns and "Qnt(Bag)" in dm.columns:
            tmp = dm.copy()
            tmp["EDD_Date"] = tmp["Earliest Delivery Date"].dt.date
            daily = tmp.groupby("EDD_Date", as_index=False)["Qnt(Bag)"].sum()
            st.plotly_chart(px.line(daily, x="EDD_Date", y="Qnt(Bag)", markers=True), use_container_width=True)
        else:
            st.info("Line chart needs 'Earliest Delivery Date' and 'Qnt(Bag)' columns.")

    # Bar chart: top trucks
    with right:
        if "Truck ID/Name" in dm.columns and "Qnt(Bag)" in dm.columns:
            top = dm.groupby("Truck ID/Name", as_index=False)["Qnt(Bag)"].sum().sort_values("Qnt(Bag)", ascending=False).head(15)
            st.plotly_chart(px.bar(top, x="Truck ID/Name", y="Qnt(Bag)"), use_container_width=True)
        else:
            st.info("Bar chart needs 'Truck ID/Name' and 'Qnt(Bag)' columns.")

    st.subheader("Filtered Data Preview")
    st.dataframe(dm.head(300), use_container_width=True)

# ============================================================
# INPUT PAGE (dropdown + B9 untouched + Part 2 visible)
# ============================================================
elif page == "Input (Push to Data Main)":
    st.title("Input Sheet")

    if not INPUT_TAB_NAME:
        st.error("Could not detect Input sheet (first tab).")
        st.stop()

    st.caption(f"Input tab: **{INPUT_TAB_NAME}** (Your form layout A-labels / B-values)")

    sh_live = _open_spreadsheet()
    ws_input = sh_live.worksheet(INPUT_TAB_NAME)

    # --------------- PART 1 ---------------
    st.subheader("Part 1: Input Form")

    now = datetime.now()

    # Read current values (so UI mirrors sheet)
    current = read_input_form(ws_input)

    with st.form("input_form"):
        col1, col2 = st.columns(2)

        with col1:
            delivery_date = st.date_input("Delivery Date (B6)", value=pd.to_datetime(current["Delivery Date"], errors="coerce").date() if current["Delivery Date"] else date.today())
            delivery_time = st.text_input("Delivery Time (B7) - 12 hour", value=(current["Delivery Time"] or fmt_time_12h(now)))

            # ✅ REAL dropdown in Streamlit (from SKU MASTER)
            sku_selected = st.selectbox("SKU (B8)", options=sku_name_options, index=sku_name_options.index(current["SKU"]) if current.get("SKU") in sku_name_options else 0)

            # ✅ B9 is formula - show live value, do not edit
            st.text_input("SKU ID (B9) - formula (read-only)", value=(current["SKU ID"] or ""), disabled=True)

            qty = st.number_input("Enter Quantity (B10)", min_value=0, step=1, value=int(float(current["Enter Quantity"])) if str(current.get("Enter Quantity", "")).strip() not in ("", "None") else 0)

        with col2:
            truck_id = st.text_input("Truck ID/Name (B11)", value=(current["Truck ID/Name"] or ""))
            vin_date = st.date_input("Vehicle Factory In Date (B12)", value=pd.to_datetime(current["Vehicle Factory In Date"], errors="coerce").date() if current["Vehicle Factory In Date"] else date.today())
            vin_time = st.text_input("Vehicle Factory In Time (B13) - 12 hour", value=(current["Vehicle Factory In Time"] or fmt_time_12h(now)))
            vout_date = st.date_input("Vehicle Factory Out Date (B14)", value=pd.to_datetime(current["Vehicle Factory Out Date"], errors="coerce").date() if current["Vehicle Factory Out Date"] else date.today())
            vout_time = st.text_input("Vehicle Factory Out Time (B15) - 12 hour", value=(current["Vehicle Factory Out Time"] or fmt_time_12h(now)))

        save_btn = st.form_submit_button("✅ Save to Input Sheet")

    if save_btn:
        # validate times
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
            # ✅ write only B6,B7,B8,B10,B11,B12,B13,B14,B15 (never touch B9)
            updates = {
                f"{INPUT_TAB_NAME}!B6": delivery_date.strftime("%Y-%m-%d"),
                f"{INPUT_TAB_NAME}!B7": str(delivery_time).strip().upper(),
                f"{INPUT_TAB_NAME}!B8": sku_selected,  # dropdown equivalent
                f"{INPUT_TAB_NAME}!B10": int(qty),
                f"{INPUT_TAB_NAME}!B11": str(truck_id).strip().upper(),
                f"{INPUT_TAB_NAME}!B12": vin_date.strftime("%Y-%m-%d"),
                f"{INPUT_TAB_NAME}!B13": str(vin_time).strip().upper(),
                f"{INPUT_TAB_NAME}!B14": vout_date.strftime("%Y-%m-%d"),
                f"{INPUT_TAB_NAME}!B15": str(vout_time).strip().upper(),
            }

            body = {"valueInputOption": "USER_ENTERED", "data": []}
            for rng, val in updates.items():
                body["data"].append({"range": rng, "values": [[val]]})
            ws_input.batch_update(body)

            # IMPORTANT: no st.rerun() (so page doesn't "jump" and hide Part 2)
            st.success("Saved. SKU ID (B9) will update automatically from the sheet formula.")

            # refresh current snapshot for Part 2 immediately
            current = read_input_form(ws_input)

        except Exception as e:
            st.error(f"Failed to save: {e}")

    st.divider()

    # --------------- PART 2 ---------------
    st.subheader("Part 2: Push to Data Main Sheet")

    # Always show live preview (so after Save you see updated values here)
    try:
        current = read_input_form(ws_input)
        preview = pd.DataFrame([{"Field": k, "Value": v} for k, v in current.items()])
        st.dataframe(preview, use_container_width=True, hide_index=True)

        if st.button("🚀 Push NOW (to next blank row in Data Main column A)"):
            row_written, pushed_vals = push_current_input_to_data_main(INPUT_TAB_NAME)
            st.success(f"✅ Pushed to Data Main Sheet row {row_written} (should be 362 if data ends at 361).")

    except Exception as e:
        st.error(f"Could not read/push: {e}")

# ============================================================
# TRUCK PRIORITY / SKU MASTER / LOADPLAN / DATA MAIN / SEQUENCING
# ============================================================
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
        st.info("SKU MASTER sheet is empty or not found.")
        st.stop()
    subset = sku_master.iloc[:, 0:5] if sku_master.shape[1] >= 5 else sku_master
    st.dataframe(subset, use_container_width=True)

elif page == "Truck_LoadPlan":
    st.title("🧾 Truck_LoadPlan (View only)")
    st.dataframe(truck_lp, use_container_width=True)

elif page == "Data Main Sheet":
    st.title("📄 Data Main Sheet (Filtered by sidebar date range)")

    dm = data_main.copy()
    if not dm.empty and "Earliest Delivery Date" in dm.columns:
        dm["Earliest Delivery Date"] = pd.to_datetime(dm["Earliest Delivery Date"], errors="coerce")
        dm = dm[(dm["Earliest Delivery Date"] >= from_dt) & (dm["Earliest Delivery Date"] < to_dt_excl)]

    q = st.text_input("Search")
    st.dataframe(table_search(dm, q), use_container_width=True)

else:
    st.title("🔢 Sequencing (Row Rank)")

    dm = data_main.copy()
    if not dm.empty and "Earliest Delivery Date" in dm.columns:
        dm["Earliest Delivery Date"] = pd.to_datetime(dm["Earliest Delivery Date"], errors="coerce")
        dm = dm[(dm["Earliest Delivery Date"] >= from_dt) & (dm["Earliest Delivery Date"] < to_dt_excl)]

    if dm.empty:
        st.info("No rows in selected date range.")
        st.stop()

    sort_cols = []
    if "Earliest Delivery Date" in dm.columns:
        sort_cols.append("Earliest Delivery Date")
    if "Truck ID/Name" in dm.columns:
        sort_cols.append("Truck ID/Name")

    if sort_cols:
        dm = dm.sort_values(sort_cols).reset_index(drop=True)

    dm.insert(0, "Row Rank", np.arange(1, len(dm) + 1))
    st.dataframe(dm, use_container_width=True)
