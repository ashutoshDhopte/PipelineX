import streamlit as st
from Utils import file_upload
import json

# Title for the app
st.title("Multiple File Upload and Processing Application")

# Description or instructions
st.write("Upload your files here, and we will process them!")

# File uploader (allow multiple file uploads)
uploaded_files = st.file_uploader(
    "Choose files", type=["csv", "xlsx", "json"], accept_multiple_files=True
)

# Handle uploaded files
if uploaded_files:
    # Process the files to generate JSON
    result_json = file_upload.process_files_to_json(uploaded_files)

    # Display the JSON result in the app
    st.write("Generated JSON:")
    st.json(result_json)

    # # Optionally, download the JSON
    # json_string = json.dumps(result_json, indent=4)
    # st.download_button(
    #     label="Download JSON",
    #     data=json_string,
    #     file_name="result.json",
    #     mime="application/json",
    # )
