import streamlit as st
from streamlit_gsheets import GSheetsConnection

st.title("GSheets Connection Test")

conn = st.connection("gsheets", type=GSheetsConnection)

st.subheader("Data Main Sheet (first rows)")
df_main = conn.read(worksheet="Data Main Sheet")
st.dataframe(df_main.head(20))

st.subheader("Truck_LoadPlan (headers on row 7)")
df_lp = conn.read(worksheet="Truck_LoadPlan", header=6)
st.dataframe(df_lp.head(20))
