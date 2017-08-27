import pandas as pd

def delAllTables(syn,projectId):
    """Deletes All tables in a given project
     Arguments:
    - syn: a Synapse client object
    - projectID: synapse ID of the project we want to delete the files in"""
    all_tables = syn.getChildren(projectId,includeTypes=[u'table'],sortBy=u'NAME', sortDirection=u'ASC')
    df_table = pd.DataFrame(all_tables)
    
    if df_table.shape[0]*df_table.shape[1] == 0:
        print('No tables in the given project : ' + str(projectId))
    else:
        for table_id in df_table['id']:
            syn.delete(table_id)
        print('Done deleting all tables in the given project : ' + str(projectId))


