import streamlit as st

s = st.secrets["connections"]["gsheets"]["spreadsheet"]
st.write("Spreadsheet length:", len(s))
st.write("Has newline:", ("\n" in s) or ("\r" in s))
