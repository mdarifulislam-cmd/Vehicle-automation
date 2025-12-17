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

def ws_batch_update(ws, updates: dict, user_entered=True):
    """
    ✅ Correct gspread Worksheet.batch_update usage.
    updates: {"B6": "2025-12-17", "B7": "03:42 PM", ...}
    """
    data = [{"range": a1, "values": [[val]]} for a1, val in updates.items()]
    ws.batch_update(
        data,
        value_input_option="USER_ENTERED" if user_entered else "RAW"
    )

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

    # ✅ ONLY A..I (J+ formulas untouched)
    updates = {
        f"A{r}": merged_A,
        f"B{r}": form["SKU"],
        f"C{r}": form["SKU ID"],
        f"D{r}": form["Enter Quantity"],
        f"E{r}": form["Truck ID/Name"],
        f"F{r}": form["Vehicle Factory In Date"],
        f"G{r}": str(form["Vehicle Factory In Time"]).strip().upper(),
        f"H{r}": form["Vehicle Factory Out Date"],
        f"I{r}": str(form["Vehicle Factory Out Time"]).strip().upper(),
    }
    ws_batch_update(ws_main, updates, user_entered=True)
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
# LOAD SHEETS
# ============================================================
data_main = read_ws("Data Main Sheet")
sku_master = read_ws("SKU MASTER")
truck_lp = read_ws("Truck_LoadPlan", header=6)
truck_priority = read_ws("Truck_Priority", header=8)

# detect input tab (first sheet) - no sidebar option
try:
    sh = _open_spreadsheet()
    worksheet_titles = [w.title for w in sh.worksheets()]
    INPUT_TAB_NAME = worksheet_titles[0] if worksheet_titles else ""
except Exception:
    INPUT_TAB_NAME = ""

# SKU dropdown from SKU MASTER
sku_name_options = []
if not sku_master.empty and sku_master.shape[1] >= 1:
    sku_name_options = [x.strip() for x in sku_master.iloc[:, 0].astype(str).fillna("") if x.strip()]

# ============================================================
# DASHBOARD (line + bar)
# ============================================================
if page == "Dashboard":
    st.title("🚚 Dashboard")

    dm = data_main.copy()
    if dm.empty:
        st.info("Data Main Sheet is empty.")
        st.stop()

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

    with left:
        if "Earliest Delivery Date" in dm.columns and "Qnt(Bag)" in dm.columns:
            tmp = dm.copy()
            tmp["EDD_Date"] = tmp["Earliest Delivery Date"].dt.date
            daily = tmp.groupby("EDD_Date", as_index=False)["Qnt(Bag)"].sum()
            st.plotly_chart(px.line(daily, x="EDD_Date", y="Qnt(Bag)", markers=True), use_container_width=True)
        else:
            st.info("Line chart needs 'Earliest Delivery Date' + 'Qnt(Bag)'.")

    with right:
        if "Truck ID/Name" in dm.columns and "Qnt(Bag)" in dm.columns:
            top = dm.groupby("Truck ID/Name", as_index=False)["Qnt(Bag)"].sum().sort_values("Qnt(Bag)", ascending=False).head(15)
            st.plotly_chart(px.bar(top, x="Truck ID/Name", y="Qnt(Bag)"), use_container_width=True)
        else:
            st.info("Bar chart needs 'Truck ID/Name' + 'Qnt(Bag)'.")

    st.subheader("Filtered Data Preview")
    st.dataframe(dm.head(300), use_container_width=True)

# ============================================================
# INPUT PAGE (dropdown + B9 untouched + Part2 reads live)
# ============================================================
elif page == "Input (Push to Data Main)":
    st.title("Input Sheet")

    if not INPUT_TAB_NAME:
        st.error("Could not detect Input sheet (first tab).")
        st.stop()

    st.caption(f"Input tab: **{INPUT_TAB_NAME}**")

    sh_live = _open_spreadsheet()
    ws_input = sh_live.worksheet(INPUT_TAB_NAME)

    # always read current
    current = read_input_form(ws_input)

    st.subheader("Part 1: Input Form")
    now = datetime.now()

    with st.form("input_form"):
        col1, col2 = st.columns(2)

        with col1:
            delivery_date = st.date_input(
                "Delivery Date (B6)",
                value=pd.to_datetime(current["Delivery Date"], errors="coerce").date() if current["Delivery Date"] else date.today()
            )
            delivery_time = st.text_input("Delivery Time (B7) - 12 hour", value=(current["Delivery Time"] or fmt_time_12h(now)))

            if not sku_name_options:
                st.error("SKU MASTER is empty (cannot build dropdown).")
                st.stop()

            sku_selected = st.selectbox(
                "SKU (B8)",
                options=sku_name_options,
                index=sku_name_options.index(current["SKU"]) if current.get("SKU") in sku_name_options else 0
            )

            # B9 is formula
            st.text_input("SKU ID (B9) - formula (read-only)", value=(current["SKU ID"] or ""), disabled=True)

            qty_default = 0
            try:
                qty_default = int(float(current["Enter Quantity"])) if str(current.get("Enter Quantity", "")).strip() not in ("", "None") else 0
            except Exception:
                qty_default = 0
            qty = st.number_input("Enter Quantity (B10)", min_value=0, step=1, value=qty_default)

        with col2:
            truck_id = st.text_input("Truck ID/Name (B11)", value=(current["Truck ID/Name"] or ""))
            vin_date = st.date_input(
                "Vehicle Factory In Date (B12)",
                value=pd.to_datetime(current["Vehicle Factory In Date"], errors="coerce").date() if current["Vehicle Factory In Date"] else date.today()
            )
            vin_time = st.text_input("Vehicle Factory In Time (B13) - 12 hour", value=(current["Vehicle Factory In Time"] or fmt_time_12h(now)))
            vout_date = st.date_input(
                "Vehicle Factory Out Date (B14)",
                value=pd.to_datetime(current["Vehicle Factory Out Date"], errors="coerce").date() if current["Vehicle Factory Out Date"] else date.today()
            )
            vout_time = st.text_input("Vehicle Factory Out Time (B15) - 12 hour", value=(current["Vehicle Factory Out Time"] or fmt_time_12h(now)))

        save_btn = st.form_submit_button("✅ Save to Input Sheet")

    if save_btn:
        for label, t in [
            ("Delivery Time", delivery_time),
            ("Vehicle Factory In Time", vin_time),
            ("Vehicle Factory Out Time", vout_time),
        ]:
            if re.match(TIME_12H_PATTERN, str(t).strip().upper()) is None:
                st.error(f"{label} must be like 09:23 PM")
                st.stop()

        if truck_id and re.match(TRUCK_PATTERN, str(truck_id).strip().upper()) is None:
            st.error("Truck ID/Name must be like DM-TA-224564")
            st.stop()

        try:
            # ✅ DO NOT touch B9
            updates = {
                "B6": delivery_date.strftime("%Y-%m-%d"),
                "B7": str(delivery_time).strip().upper(),
                "B8": sku_selected,
                "B10": int(qty),
                "B11": str(truck_id).strip().upper(),
                "B12": vin_date.strftime("%Y-%m-%d"),
                "B13": str(vin_time).strip().upper(),
                "B14": vout_date.strftime("%Y-%m-%d"),
                "B15": str(vout_time).strip().upper(),
            }
            ws_batch_update(ws_input, updates, user_entered=True)

            # let sheet formula calculate B9 then re-read
            time.sleep(0.4)
            current = read_input_form(ws_input)

            st.success("Saved. SKU ID (B9) updated automatically by sheet formula.")

        except Exception as e:
            st.error(f"Failed to save: {e}")

    st.divider()
    st.subheader("Part 2: Push to Data Main Sheet")

    # always show live preview
    current = read_input_form(ws_input)
    preview = pd.DataFrame([{"Field": k, "Value": v} for k, v in current.items()])
    st.dataframe(preview, use_container_width=True, hide_index=True)

    if st.button("🚀 Push NOW (to next blank row in Data Main column A)"):
        try:
            row_written, _ = push_current_input_to_data_main(INPUT_TAB_NAME)
            st.success(f"✅ Pushed to Data Main Sheet row {row_written}.")
        except Exception as e:
            st.error(f"Failed to push: {e}")

# ============================================================
# OTHER PAGES (unchanged)
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
