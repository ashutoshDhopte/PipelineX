import streamlit as st

params_to_check = ["metadata"]
missing_params = [param for param in params_to_check if param not in st.session_state]

st.title("Metadata")

if missing_params:
    st.error(f"Please Upload files and check back after processing is done!")
else:
    st.json(st.session_state["metadata"])
