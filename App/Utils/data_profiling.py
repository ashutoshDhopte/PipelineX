import json
import pandas as pd # type: ignore

def expandObjectValuesToColumns(files_dict, dataTypeJson):

    newColumnAdded = False

    for tableName, arr in dataTypeJson.items():
        if tableName in files_dict:
            df = files_dict[tableName]
            for colDict in arr:
                for column, obj in colDict.items():
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

