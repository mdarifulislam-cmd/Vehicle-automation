import streamlit as st
import pandas as pd
from datetime import datetime, date
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
    Writes ONLY given cells. Does not overwrite formulas in other columns.
    """
    body = {"valueInputOption": "USER_ENTERED", "data": []}
    for a1, val in cell_to_value.items():
        body["data"].append({"range": a1, "values": [[val]]})
    ws.batch_update(body)

# ============================================================
# PUSH LOGIC (A..I only; J onwards untouched)
# ============================================================
INPUT_HEADERS = [
    "Delivery Date",
    "Delivery Time",
    "SKU",
    "SKU ID",
    "Enter Quantity",
    "Truck ID/Name",
    "Vehicle Factory In Date",
    "Vehicle Factory In Time",
    "Vehicle Factory Out Date",
    "Vehicle Factory Out Time",
]

def push_selected_rows_to_data_main(input_df_full: pd.DataFrame, selected_rowids: list[int]):
    """
    Data Main columns:
      A = Delivery Date+Time (merged)
      B = SKU
      C = SKU ID
      D = Enter Quantity
      E = Truck ID/Name
      F = Vehicle Factory In Date
      G = Vehicle Factory In Time
      H = Vehicle Factory Out Date
      I = Vehicle Factory Out Time
    Only updates these cells. J+ is untouched.
    """
    if not selected_rowids:
        return 0, []

    sh = _open_spreadsheet()
    ws_main = sh.worksheet("Data Main Sheet")

    start_row = first_blank_row_colA(ws_main, start_row=2)
    end_row = start_row + len(selected_rowids) - 1
    ensure_rows(ws_main, end_row)

    # Ensure required columns exist in input df
    missing = [h for h in INPUT_HEADERS if h not in input_df_full.columns]
    if missing:
        raise ValueError(f"Input sheet missing columns: {missing}")

    written = 0
    wrote_rows = []

    for i, rid in enumerate(selected_rowids):
        r_target = start_row + i
        row = input_df_full.loc[rid]

        # Merge delivery datetime into A
        A_val = merge_delivery_datetime(row["Delivery Date"], row["Delivery Time"])

        updates = {
            f"Data Main Sheet!A{r_target}": A_val,
            f"Data Main Sheet!B{r_target}": str(row["SKU"]).strip(),
            f"Data Main Sheet!C{r_target}": str(row["SKU ID"]).strip(),
            f"Data Main Sheet!D{r_target}": row["Enter Quantity"],
            f"Data Main Sheet!E{r_target}": str(row["Truck ID/Name"]).strip(),
            f"Data Main Sheet!F{r_target}": str(row["Vehicle Factory In Date"]).strip(),
            f"Data Main Sheet!G{r_target}": str(row["Vehicle Factory In Time"]).strip().upper(),
            f"Data Main Sheet!H{r_target}": str(row["Vehicle Factory Out Date"]).strip(),
            f"Data Main Sheet!I{r_target}": str(row["Vehicle Factory Out Time"]).strip().upper(),
        }

        # IMPORTANT: do not write empty strings into critical cells that might trigger sheet behaviors.
        # But for A..I you want the row filled, so keep as-is. If you prefer skip blanks, tell me.

        batch_update_cells(ws_main, updates)

        written += 1
        wrote_rows.append(r_target)

    return written, wrote_rows

# ============================================================
# SIDEBAR / NAV
# ============================================================
st.sidebar.title("Truck Sequencing Live")
page = st.sidebar.radio(
    "Menu",
    [
        "Input (Push to Data Main)",
        "Truck_Priority",
        "SKU MASTER",
        "Truck_LoadPlan",
        "Data Main Sheet",
    ],
)

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

# Detect FIRST worksheet name via gspread
try:
    sh = _open_spreadsheet()
    worksheet_titles = [w.title for w in sh.worksheets()]
    FIRST_SHEET_NAME = worksheet_titles[0] if worksheet_titles else None
except Exception:
    FIRST_SHEET_NAME = None

# Input sheet headers row = 5 => header=4
input_df_full = read_ws(FIRST_SHEET_NAME, header=4) if FIRST_SHEET_NAME else pd.DataFrame()

# SKU dropdown options
sku_name_options = []
sku_id_lookup = {}
if not sku_master.empty and sku_master.shape[1] >= 2:
    for n, sid in zip(sku_master.iloc[:, 0].astype(str), sku_master.iloc[:, 1].astype(str)):
        n2 = n.strip()
        if n2:
            sku_name_options.append(n2)
            sku_id_lookup[n2] = sid.strip()

# ============================================================
# PAGES
# ============================================================
if page == "Input (Push to Data Main)":
    st.title("Input Sheet")

    if not FIRST_SHEET_NAME:
        st.error("Could not detect your first worksheet (Input sheet).")
        st.stop()

    st.caption(f"Connected Input Sheet tab: **{FIRST_SHEET_NAME}** (headers row = 5)")

    if input_df_full.empty:
        st.error("Input sheet loaded empty. Check tab / permissions / header row.")
        st.stop()

    # -------------------------
    # PART 1: INPUT FORM (writes B6..B15)
    # -------------------------
    st.subheader("Part 1: Input Form (writes to cells B6–B15)")

    now = datetime.now()

    with st.form("input_cells_form"):
        c1, c2 = st.columns(2)

        with c1:
            delivery_date = st.date_input("Delivery Date (B6)", value=date.today())
            delivery_time = st.text_input("Delivery Time (B7) - 12 hour (e.g. 09:23 PM)", value=fmt_time_12h(now))

            sku_name = st.selectbox("SKU (B8)", options=sku_name_options) if sku_name_options else st.text_input("SKU (B8)", "")
            sku_id_auto = sku_id_lookup.get(sku_name, "")
            st.text_input("SKU ID (B9) - Auto (formula)", value=sku_id_auto, disabled=True)

            qty = st.number_input("Enter Quantity (B10)", min_value=0, step=1)

        with c2:
            truck_id = st.text_input("Truck ID/Name (B11) e.g. DM-TA-224564", value="")
            vin_date = st.date_input("Vehicle Factory In Date (B12)", value=date.today())
            vin_time = st.text_input("Vehicle Factory In Time (B13) - 12 hour", value=fmt_time_12h(now))
            vout_date = st.date_input("Vehicle Factory Out Date (B14)", value=date.today())
            vout_time = st.text_input("Vehicle Factory Out Time (B15) - 12 hour", value=fmt_time_12h(now))

        save_form = st.form_submit_button("✅ Save to Input Sheet")

    if save_form:
        # Validate time format (12h)
        for label, t in [
            ("Delivery Time", delivery_time),
            ("Vehicle Factory In Time", vin_time),
            ("Vehicle Factory Out Time", vout_time),
        ]:
            if re.match(TIME_12H_PATTERN, str(t).strip().upper()) is None:
                st.error(f"{label} must be like 09:23 PM")
                st.stop()

        # Validate truck id
        if truck_id and re.match(TRUCK_PATTERN, truck_id.strip().upper()) is None:
            st.error("Truck ID/Name must be like DM-TA-224564")
            st.stop()

        try:
            sh2 = _open_spreadsheet()
            ws_input = sh2.worksheet(FIRST_SHEET_NAME)

            # Do NOT touch B9
            updates = {
                f"{FIRST_SHEET_NAME}!B6": delivery_date.strftime("%Y-%m-%d"),
                f"{FIRST_SHEET_NAME}!B7": delivery_time.strip().upper(),
                f"{FIRST_SHEET_NAME}!B8": sku_name,
                f"{FIRST_SHEET_NAME}!B10": int(qty),
                f"{FIRST_SHEET_NAME}!B11": truck_id.strip().upper(),
                f"{FIRST_SHEET_NAME}!B12": vin_date.strftime("%Y-%m-%d"),
                f"{FIRST_SHEET_NAME}!B13": vin_time.strip().upper(),
                f"{FIRST_SHEET_NAME}!B14": vout_date.strftime("%Y-%m-%d"),
                f"{FIRST_SHEET_NAME}!B15": vout_time.strip().upper(),
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
    # PART 2: TABLE (A–C view) + PUSH
    # -------------------------
    st.subheader("Part 2: Select rows (A–C view) → Push into Data Main Sheet")

    # Preserve original index so selection maps correctly
    view_df = input_df_full.copy()
    view_df["_ROWID_"] = view_df.index

    cols_to_show = list(input_df_full.columns[:3])  # A..C
    view_small = view_df[["_ROWID_"] + cols_to_show]

    q = st.text_input("Search Input Table (A–C)")
    view_small = table_search(view_small, q).reset_index(drop=True)

    view_small.insert(0, "✅ Push?", False)

    edited = st.data_editor(
        view_small,
        use_container_width=True,
        num_rows="dynamic",
        key="input_table_editor",
        column_config={"_ROWID_": st.column_config.NumberColumn("_ROWID_", disabled=True)},
    )

    if st.button("🚀 Push Selected Rows to Data Main"):
        selected = edited[edited["✅ Push?"] == True]
        if selected.empty:
            st.warning("No rows selected.")
        else:
            selected_rowids = selected["_ROWID_"].astype(int).tolist()
            try:
                pushed, wrote_rows = push_selected_rows_to_data_main(input_df_full, selected_rowids)
                st.success(f"Pushed {pushed} row(s) into Data Main. Rows written: {wrote_rows} (should start at 362).")
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
        st.info("SKU MASTER sheet is empty or not found.")
        st.stop()
    subset = sku_master.iloc[:, 0:5] if sku_master.shape[1] >= 5 else sku_master
    st.dataframe(subset, use_container_width=True)

elif page == "Truck_LoadPlan":
    st.title("🧾 Truck_LoadPlan (View only)")
    st.dataframe(truck_lp, use_container_width=True)

else:
    st.title("📄 Data Main Sheet")
    st.dataframe(data_main, use_container_width=True)
