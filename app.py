import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
import plotly.express as px
from streamlit_gsheets import GSheetsConnection

st.set_page_config(page_title="Truck Sequencing Live", layout="wide")

# -----------------------------
# Connection
# -----------------------------
conn = st.connection("gsheets", type=GSheetsConnection)

# -----------------------------
# Helpers
# -----------------------------
def safe_dt(x):
    if pd.isna(x):
        return pd.NaT
    if isinstance(x, (pd.Timestamp, datetime)):
        return pd.to_datetime(x)
    return pd.to_datetime(str(x), errors="coerce")

def to_num(s):
    return pd.to_numeric(s, errors="coerce").fillna(0)

def read_ws(ws: str, ttl: int = 10, header: int | None = None) -> pd.DataFrame:
    # header is 0-indexed row number for header (library supports header=)
    if header is None:
        df = conn.read(worksheet=ws, ttl=ttl)
    else:
        df = conn.read(worksheet=ws, ttl=ttl, header=header)
    return df if df is not None else pd.DataFrame()

def append_rows(ws: str, df_new: pd.DataFrame):
    conn.append(worksheet=ws, data=df_new)

def filter_by_edd(df: pd.DataFrame, from_dt: pd.Timestamp, to_dt_excl: pd.Timestamp) -> pd.DataFrame:
    if df.empty or "Earliest Delivery Date" not in df.columns:
        return df
    return df[(df["Earliest Delivery Date"] >= from_dt) & (df["Earliest Delivery Date"] < to_dt_excl)]

def normalize_main(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "Earliest Delivery Date" in df.columns:
        df["Earliest Delivery Date"] = df["Earliest Delivery Date"].apply(safe_dt)
    # Common numeric columns
    for col in ["Qnt(Bag)", "Qty", "Quantity", "Qnt"]:
        if col in df.columns:
            df[col] = to_num(df[col])
    return df

# -----------------------------
# Sidebar controls
# -----------------------------
st.sidebar.title("Truck Sequencing Live")

page = st.sidebar.radio(
    "Menu",
    ["Dashboard", "Truck_LoadPlan", "Sequencing (Row Rank)", "SKU MASTER", "Data Main Sheet"]
)

st.sidebar.markdown("### Date Range (Earliest Delivery Date)")
default_from = date(2025, 12, 12)
default_to = date(2025, 12, 18)
from_date = st.sidebar.date_input("From", value=default_from)
to_date = st.sidebar.date_input("To", value=default_to)

from_dt = pd.to_datetime(from_date)
to_dt_excl = pd.to_datetime(to_date) + pd.Timedelta(days=1)  # include whole To day

refresh = st.sidebar.button("🔄 Refresh Now")

# -----------------------------
# Load sheets (Live)
# -----------------------------
ttl_main = 0 if refresh else 10
ttl_lp = 0 if refresh else 10

data_main = normalize_main(read_ws("Data Main Sheet", ttl=ttl_main))
sku_master = read_ws("SKU MASTER", ttl=60)
# Truck_LoadPlan headers are on row 7 => header index = 6
truck_lp = read_ws("Truck_LoadPlan", ttl=ttl_lp, header=6)

# -----------------------------
# Global search filter helper
# -----------------------------
def table_search(df: pd.DataFrame, q: str) -> pd.DataFrame:
    if df.empty or not q.strip():
        return df
    q = q.strip().lower()
    mask = pd.Series(False, index=df.index)
    for c in df.columns:
        mask = mask | df[c].astype(str).str.lower().str.contains(q, na=False)
    return df[mask]

# ============================================================
# PAGE: DASHBOARD
# ============================================================
if page == "Dashboard":
    st.title("🚚 Dashboard")

    df = filter_by_edd(data_main, from_dt, to_dt_excl).copy()
    if df.empty:
        st.info("No rows found in Data Main Sheet for the selected date range.")
        st.stop()

    # KPIs
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Rows (range)", f"{len(df):,}")

    if "Truck ID/Name" in df.columns:
        c2.metric("Unique Trucks", f"{df['Truck ID/Name'].nunique():,}")
    if "SKU ID" in df.columns:
        c3.metric("Unique SKU IDs", f"{df['SKU ID'].nunique():,}")
    if "Qnt(Bag)" in df.columns:
        c4.metric("Total Bags", f"{df['Qnt(Bag)'].sum():,.0f}")

    st.divider()

    # Charts
    left, right = st.columns([1, 1])

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
            fig2 = px.bar(top, x="Truck ID/Name", y="Qnt(Bag)", title="Top 15 Trucks by Bags (Range)")
            st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # Searchable table
    st.subheader("Live Data (Filtered)")
    q = st.text_input("Search in table (truck / sku / anything)")
    st.dataframe(table_search(df, q), use_container_width=True)

    # Download filtered
    st.download_button(
        "⬇️ Download filtered CSV",
        data=table_search(df, q).to_csv(index=False).encode("utf-8"),
        file_name="data_main_filtered.csv",
        mime="text/csv",
    )

# ============================================================
# PAGE: TRUCK LOADPLAN
# ============================================================
elif page == "Truck_LoadPlan":
    st.title("🧾 Truck_LoadPlan (Live)")

    st.caption("Headers are on row 7 in Google Sheets (we read using header=6).")

    # Show current table
    st.subheader("Current Truck_LoadPlan")
    st.dataframe(truck_lp, use_container_width=True)

    st.divider()

    # Add new row
    st.subheader("Add new LoadPlan row")

    col1, col2 = st.columns([1, 1])

    with col1:
        with st.form("add_row"):
            truck = st.text_input("Truck ID/Name (Plate)")
            sku_id = st.text_input("SKU_ID (e.g., A8)")
            qty = st.number_input("Qty", min_value=0, step=1)
            sku_name = st.text_input("SKU NAME (optional)", value="")
            truck_rank = st.text_input("Truck Rank (optional)", value="")
            line_score = st.text_input("Line Score (optional)", value="")
            submitted = st.form_submit_button("✅ Save to Google Sheet")

        if submitted:
            if not truck.strip() or not sku_id.strip() or qty <= 0:
                st.error("Truck ID/Name, SKU_ID and Qty are required.")
            else:
                df_new = pd.DataFrame([{
                    "Truck ID/Name": truck.strip(),
                    "SKU_ID": sku_id.strip(),
                    "Qty": int(qty),
                    "SKU NAME": sku_name.strip(),
                    "Truck Rank": truck_rank.strip(),
                    "Line Score": line_score.strip(),
                    "SavedAt": datetime.now().isoformat(timespec="seconds"),
                }])
                append_rows("Truck_LoadPlan", df_new)
                st.success("Saved! Refreshing…")
                st.rerun()

    with col2:
        st.markdown("#### Bulk upload (CSV)")
        up = st.file_uploader("Upload CSV with columns: Truck ID/Name, SKU_ID, Qty (others optional)", type=["csv"])
        if up is not None:
            try:
                dfu = pd.read_csv(up)
                st.dataframe(dfu.head(), use_container_width=True)

                required = {"Truck ID/Name", "SKU_ID", "Qty"}
                if not required.issubset(set(dfu.columns)):
                    st.error(f"CSV must contain columns: {', '.join(sorted(required))}")
                else:
                    if st.button("⬆️ Append CSV rows to Truck_LoadPlan"):
                        # Ensure all expected columns exist
                        for c in ["SKU NAME", "Truck Rank", "Line Score"]:
                            if c not in dfu.columns:
                                dfu[c] = ""
                        dfu["SavedAt"] = datetime.now().isoformat(timespec="seconds")
                        append_rows("Truck_LoadPlan", dfu[["Truck ID/Name","SKU_ID","Qty","SKU NAME","Truck Rank","Line Score","SavedAt"]])
                        st.success("Uploaded and appended!")
                        st.rerun()
            except Exception as e:
                st.error(f"Failed to read CSV: {e}")

# ============================================================
# PAGE: SEQUENCING (ROW RANK PER TRUCK/ROW)
# ============================================================
elif page == "Sequencing (Row Rank)":
    st.title("🔢 Sequencing (Row Rank per row)")

    df = filter_by_edd(data_main, from_dt, to_dt_excl).copy()
    if df.empty:
        st.info("No rows found in selected date range.")
        st.stop()

    if "Truck ID/Name" not in df.columns or "Earliest Delivery Date" not in df.columns:
        st.error("Data Main Sheet must include 'Truck ID/Name' and 'Earliest Delivery Date'.")
        st.stop()

    # Rank per row: earliest EDD first; tie-break by Truck ID/Name
    ranked = df.sort_values(["Earliest Delivery Date", "Truck ID/Name"], ascending=[True, True]).reset_index(drop=True)
    ranked["Row Rank (EDD)"] = np.arange(1, len(ranked) + 1)

    st.subheader("Ranked rows (Rank 1 = earliest EDD)")
    q = st.text_input("Search ranked table")
    st.dataframe(table_search(ranked, q), use_container_width=True)

    st.download_button(
        "⬇️ Download ranked CSV",
        data=table_search(ranked, q).to_csv(index=False).encode("utf-8"),
        file_name="sequencing_ranked.csv",
        mime="text/csv",
    )

# ============================================================
# PAGE: SKU MASTER
# ============================================================
elif page == "SKU MASTER":
    st.title("📦 SKU MASTER")

    if sku_master.empty:
        st.info("SKU MASTER is empty or not found.")
        st.stop()

    st.subheader("Master Table")
    q = st.text_input("Search SKU master")
    st.dataframe(table_search(sku_master, q), use_container_width=True)

    st.divider()
    st.subheader("Lookup SKU Name by SKU_ID")

    lookup_id = st.text_input("Enter SKU_ID (e.g., A8)", key="sku_lookup")
    if lookup_id:
        # Try to detect ID and Name columns
        cols_lower = {c.lower(): c for c in sku_master.columns}
        id_col = cols_lower.get("sku_id") or cols_lower.get("sku id") or list(sku_master.columns)[1]
        name_col = cols_lower.get("sku name") or cols_lower.get("sku") or list(sku_master.columns)[0]

        hit = sku_master[sku_master[id_col].astype(str).str.strip() == lookup_id.strip()]
        if hit.empty:
            st.warning("Not found.")
        else:
            st.success(f"SKU NAME: {hit.iloc[0][name_col]}")

# ============================================================
# PAGE: DATA MAIN SHEET
# ============================================================
else:
    st.title("📄 Data Main Sheet (Live View)")
    df = filter_by_edd(data_main, from_dt, to_dt_excl)
    q = st.text_input("Search Data Main Sheet")
    st.dataframe(table_search(df, q), use_container_width=True)
