import synapseclient
import numpy as np
import pandas as pd
from synapsebridgehelpers import tableWithFileIds
from synapsebridgehelpers import groupTableActivity

def transferTables(syn,sourceProjId, uploadProjId, extId_Str = ''):
    """ This function transfers tables from a source project to the upload project (target project) 
    sorted by external Ids which contain extId_Str """

    # List of tables sorted by activity as defined in groupTableActivity, which is based on get_tables from
    # synapsebridgehelper.tableHelpers
    tables_list = groupTableActivity(syn,sourceProjId,extId_Str)
    all_tables = groupTableActivity(syn,sourceProjId)

    # Iterate over each activity in tables_list
    for activity_ in tables_list:
        print(activity_)
        
        # list of all table ids corresponding to that activity 
        activityTableIds = tables_list[activity_]
        result = tableWithFileIds(syn,table_id = activityTableIds[0], extIdStr = extId_Str)
        df_main = result['df']
        cols = result['cols']
        
        # appending the rest of the sorted tables corresponding to that activity if they exist
        for table_index in range(1, len(activityTableIds)):
            result = tableWithFileIds(syn,table_id = activityTableIds[table_index], extIdStr = extId_Str)
            df = result['df']
            cols = result['cols']
            df_main = df_main.append(df)
        
        # Correcting the order of the columns while uploading
        df_main = df_main[df_main.columns]
        
        # Updaing schema and uploading
        schema = synapseclient.Schema(name=activity_ +' extIdStr_' + extId_Str, columns=cols, parent=uploadProjId)
        table = synapseclient.Table(schema, df_main)
        table = syn.store(table)
        table = syn.setProvenance(table.schema.id , activity = synapseclient.activity.Activity(used = all_tables[activity_]))
