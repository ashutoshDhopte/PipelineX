import streamlit as st
from Utils import file_upload, api
import json
import pandas as pd

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

    files_dict = {}

    for file in uploaded_files:
        # Read the file into a Pandas DataFrame based on file type
        if file.type == "text/csv":
            df = pd.read_csv(file)
        elif (
            file.type
            == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ):
            df = pd.read_excel(file)
        elif file.type == "application/json":
            df = pd.read_json(file)
        else:
            continue  # Skip unsupported file types

        # Extract table name from the file name (without extension)
        table_name = file.name.split(".")[0]

        files_dict[table_name] = df

    # Process the files to generate JSON
    result_json = file_upload.get_datatype_input_json(files_dict)

    # Display the JSON result in the app
    st.write("Datatypes JSON:")

    response = api.getDataTypes(result_json)

    dataTypeJson = json.loads(response.strip("```json").strip("```"))

    joinsInputJson = file_upload.get_joins_input_json(files_dict, dataTypeJson)

    st.write(joinsInputJson)
