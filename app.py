from streamlit_gsheets import GSheetsConnection
import streamlit as st

conn = st.connection("gsheets", type=GSheetsConnection)
df_main = conn.read(worksheet="Data Main Sheet")
st.dataframe(df_main.head())
