import json
import pandas as pd  # type: ignore
from dateutil import parser


def expandObjectValuesToColumns(files_dict, dataTypeJson):

    newColumnAdded = False

    for tableName, tableDict in dataTypeJson["tables"].items():
        if tableName in files_dict:
            df = files_dict[tableName]
            for column, obj in tableDict["columns"].items():
                if obj["datatype"] == "string" and obj["isJson"]:
                    df[column] = df[column].apply(
                        lambda x: x.replace("'", '"') if isinstance(x, str) else x
                    )

                    # ---- replace spaced columns with _
                    jsonSeries = df[column].apply(json.loads)
                    spacedColArr = set()
                    for seriesObj in jsonSeries:
                        for newCol, value in seriesObj.items():
                            if len(newCol.split(" ")) > 1:
                                spacedColArr.add(newCol)
                    for spacedCol in spacedColArr:
                        splitArr = spacedCol.split(" ")
                        df[column] = df[column].apply(
                            lambda x: (
                                x.replace(spacedCol, splitArr[0] + "_" + splitArr[1])
                                if isinstance(x, str)
                                else x
                            )
                        )
                    # ----

                    df[column] = df[column].apply(json.loads)
                    json_expanded_df = pd.json_normalize(df[column])
                    df = pd.concat([df, json_expanded_df], axis=1)
                    df = df.drop(columns=[column])
                    files_dict[tableName] = df
                    newColumnAdded = True

    return newColumnAdded


def createMetadata(dataTypeJson, joinJson, files_dict, uploaded_files):

    metadata = {
        "TABLE_NAME": [],
        "ROW_COUNT": {},
        "DESCRIPTION": {},
        "RELATIONSHIP": {},
        "FILE_INFO": {},
        "MISSING_OR_NULL_COUNT": {},
        "OUTLIERS": {},
        "STATISTICS": {},
        "PURPOSE": "",
        "TARGET_AUDIENCE": "",
        "LINK": "",
    }

    # ATTRIBUTE                    VALUE

    # TABLE_NAME                  [portfolio, profile, transcript]
    # ROW_COUNT                    {portfolio:30, profile:1000, transcript:99899}
    # DESCRIPTION                 {portfolio:Description, profile:Description, transcript:Description}
    # RELATIONSHIP               [output from joins API]
    # FILE_INFO                  {portfolio:{format:csv, size:0.2mb}, ...}
    # MISSING_OR_NULL_COUNT          {portfolio:{column1:10, column3:5}, profile:{...}, ...}
    # OUTLIERS                    [output from joins API]
    # STATISTICS                  {portfolio:{column1:{mean:345, median:3445, mode:334, ...}, column3:{...}, ...}, profile:{...}, ...} //means, medians, modes, Standard deviations, ranges
    # PURPOSE                     ______
    # TARGET_AUDIENCE              ______
    # LINK                        link of the RDS on AWS

    metadata["RELATIONSHIP"] = joinJson["joins"]
    metadata["PURPOSE"] = dataTypeJson["purpose"]
    metadata["TARGET_AUDIENCE"] = dataTypeJson["targetAudience"]

    # for file format and size
    for file in uploaded_files:
        # Get the file size in bytes
        file_size_bytes = file.size
        # Convert the file size to a readable format
        if file_size_bytes < 1024:
            file_size_str = f"{file_size_bytes} B"
        elif file_size_bytes < 1024 * 1024:
            file_size_kb = file_size_bytes / 1024
            file_size_str = f"{file_size_kb:.2f} KB"
        else:
            file_size_mb = file_size_bytes / (1024 * 1024)
            file_size_str = f"{file_size_mb:.2f} MB"
        # Add file size to metadata
        tableName = file.name.split(".")[0]
        metadata["FILE_INFO"][tableName] = {"format": file.type, "size": file_size_str}

    # for table level info
    for tableName, tableObj in dataTypeJson["tables"].items():
        metadata["TABLE_NAME"].append(tableName)
        metadata["DESCRIPTION"][tableName] = tableObj["description"]
        if tableName in files_dict:
            metadata["ROW_COUNT"][tableName] = files_dict[tableName].shape[0]

    # for data point level info
    for tableName, df in files_dict.items():
        null_or_blank_counts = df.apply(lambda x: x.isnull().sum() + (x == "").sum())
        for columnName, count in null_or_blank_counts.items():
            if count > 0:
                if not tableName in metadata["MISSING_OR_NULL_COUNT"]:
                    metadata["MISSING_OR_NULL_COUNT"][tableName] = {}
                metadata["MISSING_OR_NULL_COUNT"][tableName][columnName] = count

    for tableName, colObj in joinJson["outliers"].items():
        for colName, colProp in colObj.items():
            if colProp["isOutlier"]:
                if not tableName in metadata["OUTLIERS"]:
                    metadata["OUTLIERS"][tableName] = {}
                metadata["OUTLIERS"][tableName][colName] = {}
                metadata["OUTLIERS"][tableName][colName]["outlier_reason"] = colProp[
                    "outlier_reason"
                ]
                metadata["OUTLIERS"][tableName][colName]["outlier_values"] = colProp[
                    "outlier_values"
                ]
                metadata["OUTLIERS"][tableName][colName]["valid_min_value"] = colProp[
                    "valid_min_value"
                ]
                metadata["OUTLIERS"][tableName][colName]["valid_max_value"] = colProp[
                    "valid_max_value"
                ]

    return metadata


def cleanData(dataTypeJson, files_dict):

    for tableName, df in files_dict.items():
        files_dict[tableName] = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    for tableName, df in files_dict.items():
        if tableName in dataTypeJson["tables"]:
            for index, row in df.iterrows():
                for column in df.columns:
                    cell_value = row[column]
                    colProp = dataTypeJson["tables"][tableName]["columns"][column]
                    columnType = colProp["datatype"]

                    if columnType == "int" or columnType == "integer":
                        if type(cell_value) == "string":
                            try:
                                row[column] = int(cell_value)
                            except Exception as e:
                                print("Parsing error " + e)
                    elif columnType == "float" or columnType == "double":
                        if (
                            type(cell_value) == "string"
                            or type(cell_value) == "int"
                            or type(cell_value) == "integer"
                        ):
                            try:
                                row[column] = round(float(cell_value), 2)
                            except Exception as e:
                                print("Parsing error " + e)
                    elif colProp["isArray"]:
                        if type(cell_value) == "string":
                            try:
                                row[column] = json.loads(cell_value)
                            except Exception as e:
                                print("Parsing error " + e)
                    elif colProp["isJson"]:
                        if type(cell_value) == "string":
                            try:
                                row[column] = json.loads(cell_value)
                            except Exception as e:
                                print("Parsing error " + e)
                    elif colProp["isDate"]:
                        if type(cell_value) == "string":
                            try:
                                row[column] = parser.parse(cell_value)
                            except Exception as e:
                                print("Parsing error " + e)
                    elif colProp["isCategorical"]:
                        if not type(cell_value) == "string":
                            try:
                                row[column] = str(cell_value)
                            except Exception as e:
                                print("Parsing error " + e)
