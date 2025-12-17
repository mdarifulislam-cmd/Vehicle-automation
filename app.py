import streamlit as st

cfg = st.secrets["connections"]["gsheets"]
st.write("Top-level keys:", list(cfg.keys()))
st.write("Top-level type:", cfg.get("type"))
st.write("Has nested service_account:", "service_account" in cfg)
if "service_account" in cfg:
    st.write("Nested keys:", list(cfg["service_account"].keys()))
