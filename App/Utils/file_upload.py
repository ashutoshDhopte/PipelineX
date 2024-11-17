import pandas as pd


def process_files_to_json(uploaded_files):
    """
    Process uploaded files and return the required JSON structure.
    """
    output_json = {}

    for file in uploaded_files:
        # Read the file into a Pandas DataFrame based on the file type
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

        # Generate the JSON for this table
        table_json = {
            "number_of_rows": len(df),
        }

        for column in df.columns:
            column_data = df[column].dropna()  # Drop NaN values to focus on valid data
            if not column_data.empty:
                table_json[column] = {
                    "unique_count": column_data.nunique(),
                    "value": column_data.iloc[0],  # Take any one row with a value
                }
            else:
                table_json[column] = {
                    "unique_count": 0,
                    "value": None,
                }

        # Add this table's JSON to the final output
        output_json[table_name] = [table_json]

    return output_json
