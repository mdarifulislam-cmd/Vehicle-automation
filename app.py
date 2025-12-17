import streamlit as st
st.write("keys:", list(st.secrets["connections"]["gsheets"].keys()))
st.write("service_account keys:", list(st.secrets["connections"]["gsheets"]["service_account"].keys()))
