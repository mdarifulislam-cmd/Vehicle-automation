from streamlit_gsheets import GSheetsConnection
import streamlit as st

conn = st.connection("gsheets", type=GSheetsConnection)

df_main = conn.read(worksheet="Data Main Sheet")
st.dataframe(df_main.head(20))

df_lp = conn.read(worksheet="Truck_LoadPlan", header=6)  # headers on row 7
st.dataframe(df_lp.head(20))
