import synapseutils as su

def copyFileIdsInBatch(syn, table_id, fileIds, content_type = "application/json"):
    """Copy file handles from a pandas.Series object.

    Parameters
    ----------
    syn : synapseclient.Synapse
    table_id : str
        Synapse ID of the table the original file handles belong to.
    fileIds : pandas.Series
        The column containing file handles
    content_type : str

    Returns
    -------
    A dict mapping original file handles to newly created file handles.
    """
    fhids_to_copy = fileIds.dropna().drop_duplicates().astype(int).tolist()
    new_fhids = []
    for i in range(0, len(fhids_to_copy), 100):
        fhids_to_copy_i = fhids_to_copy[i:i+100]
        new_fhids_i = su.copyFileHandles(
                syn = syn,
                fileHandles = fhids_to_copy_i,
                associateObjectTypes = ["TableEntity"] * len(fhids_to_copy_i),
                associateObjectIds = [table_id] * len(fhids_to_copy_i),
                newContentTypes = [content_type] * len(fhids_to_copy_i),
                newFileNames = [None] * len(fhids_to_copy_i))
        for j in [int(i['newFileHandle']['id']) for i in new_fhids_i]:
            new_fhids.append(j)
    fhid_map = {k: v for k, v in zip(fhids_to_copy, new_fhids)}
    return fhid_map


def tableWithFileIds(syn,table_id, healthcodes=None):
    """ Returns a dict like {'df': dataFrame, 'cols': names of columns of type FILEHANDLEID} with actual fileHandleIds,
    also has an option to filter table given a list of healthcodes """

    # Getting cols from current table id
    cols = syn.getTableColumns(table_id) # Generator object

    # Finding column names in the current table that have FILEHANDLEIDs as their type
    cols_filehandleids = [col.name for col in cols if col.columnType == 'FILEHANDLEID']

    # Grabbing results
    if healthcodes == None:
        results = syn.tableQuery('select * from %s' %(table_id))
    else:
        healthcodes = '(\''+'\',\''.join(healthcodes)+'\')'
        results = syn.tableQuery('select * from %s where healthCode in %s' %(table_id, healthcodes))

    # Store the results as a dataframe
    df = results.asDataFrame()

    # Iterate for each element(column) that has columntype FILEHANDLEID
    for element in cols_filehandleids:
        df[element] = df[element].map(copyFileIdsInBatch(syn,table_id,df[element]))
        df[element] = [int(x) if x==x else '' for x in df[element]]

    return {'df' : df, 'cols' : cols_filehandleids}
