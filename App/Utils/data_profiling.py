import json
import pandas as pd # type: ignore

def expandObjectValuesToColumns(files_dict, dataTypeJson):

    newColumnAdded = False

    for tableName, tableDict in dataTypeJson.items():
        if tableName in files_dict:
            df = files_dict[tableName]
            for column, obj in tableDict['columns'].items():
                if obj['datatype'] == 'string' and obj['isJson']:
                    df[column] = df[column].apply(lambda x: x.replace("'", '"') if isinstance(x, str) else x)

                    # ---- replace spaced columns with _
                    jsonSeries = df[column].apply(json.loads)
                    spacedColArr = set()
                    for seriesObj in jsonSeries:
                        for newCol, value in seriesObj.items():
                            if len(newCol.split(" ")) > 1:
                                spacedColArr.add(newCol)
                    for spacedCol in spacedColArr:
                        splitArr = spacedCol.split(" ")
                        df[column] = df[column].apply(lambda x: x.replace(spacedCol, splitArr[0]+"_"+splitArr[1]) if isinstance(x, str) else x)
                    # ----

                    df[column] = df[column].apply(json.loads)
                    json_expanded_df = pd.json_normalize(df[column])
                    df = pd.concat([df, json_expanded_df], axis=1)
                    df = df.drop(columns=[column])
                    files_dict[tableName] = df
                    newColumnAdded = True
                
    return newColumnAdded


def createMetadata(dataTypeJson, joinJson, files_dict):

    metadata = {
        'TABLE_NAME': [],
        'ROW_COUNT': {},
        'DESCRIPTION': {},
        'RELATIONSHIP': {},
        'FILE_FORMAT': {},
        'FILE_SIZE': {},
        'MISSING_OR_NULL_COUNT': {},
        'OUTLIERS': {},
        'STATISTICS': {},
        'PURPOSE': "",
        'TARGET_AUDIENCE': "",
        'LINK': ""
    }

    # ATTRIBUTE                    VALUE
    # TABLE_NAME                  [portfolio, profile, transcript]
    # ROW_COUNT                    {portfolio:30, profile:1000, transcript:99899}
    # DESCRIPTION                 {portfolio:Description, profile:Description, transcript:Description}
    # RELATIONSHIP               [output from joins API]
    # FILE_FORMAT                  {portfolio:csv, profile:csv, transcript:csv}
    # FILE_SIZE                    {portfolio:0.2 mb, profile:3 mb, transcript:25 mb}
    # MISSING_OR_NULL_COUNT          {portfolio:{column1:10, column3:5}, profile:{...}, ...}
    # OUTLIERS                    [output from joins API]
    # STATISTICS                  {portfolio:{column1:{mean:345, median:3445, mode:334, ...}, column3:{...}, ...}, profile:{...}, ...} //means, medians, modes, Standard deviations, ranges
    # PURPOSE                     ______
    # TARGET_AUDIENCE              ______
    # LINK                        link of the RDS on AWS

    # for tableName, colArr in dataTypeJson.items():
        
    #     metadata['TABLE_NAME'].append(tableName)
    #     if tableName in files_dict:
    #         metadata['ROW_COUNT'][tableName] = files_dict[tableName].shape[0]
            
    #     for colObj in colArr:
    #         for columnName, obj in colObj.items():
                
