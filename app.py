import streamlit as st
st.write("gsheets secrets keys:", list(st.secrets["connections"]["gsheets"].keys()))
st.write("gsheets type:", st.secrets["connections"]["gsheets"].get("type"))
st.write("spreadsheet:", st.secrets["connections"]["gsheets"].get("spreadsheet"))
