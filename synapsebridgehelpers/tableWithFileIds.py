import synapseclient
import numpy as np
import pandas as pd
from synapseutils.copy import copyFileHandles

def tableWithFileIds(syn,table_id, extIdStr=''):
    """ Returns a dict like {'df': dataFrame, 'cols': correspoding columns with tags} with actual fileHandleIds, 
    also has an option to filter tables according to externalIDs which contain the string extIdStr """
    
    # Remove all duplicates and NaNs
    def removeDuplicatesAndNans(num_seq):
        if len(num_seq) == 0:
            return num_seq
        else:
            num_seq_unique = pd.unique(num_seq)
            num_seq_unique = num_seq_unique[~np.isnan(num_seq_unique)]
            num_seq_unique = [int(x) for x in num_seq_unique]    
            return num_seq_unique
    
    # Return corresponding newIds for a given fileIds_ according to a map(dict) idMap_ 
    def fillIdsMap(fileIds_, idMap_):
        returnIds = []
        for index in range(0,len(fileIds_)):
            if ~np.isnan(fileIds_[index]):
                returnIds.append(int(idMap_[str(int(fileIds_[index]))]))
            else:
                returnIds.append('')
        return returnIds      
    
    # Getting schema and cols from current table id
    current_project = syn.get(table_id)
    cols = syn.getTableColumns(current_project)

    # Finding column names in the current table that have FILEHANDLEIDs as their type
    cols_filehandleids = []
    for col in cols:
        if col.columnType == 'FILEHANDLEID':
            cols_filehandleids.append({'name':col.name, 'id':col.id})
    

    # To see if externalId was given as an input or not
    if extIdStr !='':
        # Given an external ID like, we will select those rows from the current table given its synapse ID
        results = syn.tableQuery('select * from '+ table_id +' where externalId like "' + extIdStr + '%"')
    else:
        # Given no extenal ID, we just tranfer all columns
        results = syn.tableQuery('select * from '+ table_id )


    # Store the results as a dataframe
    df = results.asDataFrame()
    cols = synapseclient.as_table_columns(df)

    # Change the type of columns that are FILEHANDLEIDs as calculated before
    for col in cols:
        for element in cols_filehandleids:
            if col.name == element['name']:
                col.columnType = 'FILEHANDLEID'
    
    # Iterate for each element(column) that has columntype FILEHANDLEID 
    for element in cols_filehandleids:
        fileIds = (df[element['name']])
        fileIds = removeDuplicatesAndNans(fileIds)
        len_fIds = len(fileIds)
        newIds = []
    
        # The check for 100 is because copyFileHandles cannot handle more than 100 requests at a time
        # The check for 0 is for empty colunms
        if len_fIds <= 100 and len_fIds>0:
            tempIds = copyFileHandles(syn, fileIds, ['TableEntity']*len_fIds,[table_id]*len_fIds, [None]*len_fIds, [None]*len_fIds)
            tempIds = [x['newFileHandle']['id'] for x in tempIds['copyResults']]
            newIds = newIds + tempIds
        else:
            start_pos = 0
            while start_pos < len_fIds:
                curr_len = min(100, len_fIds-start_pos)
                tempIds = copyFileHandles(syn, fileIds[start_pos:min(len_fIds, start_pos+100)], 
                                      ['TableEntity']*curr_len,[table_id]*curr_len, 
                                      [None]*curr_len,[None]*curr_len)
                tempIds = [x['newFileHandle']['id'] for x in tempIds['copyResults']]
                newIds = newIds + tempIds
                start_pos = start_pos+100
        
        # Map the fileIds to corresponding newIds
        idMap = dict(zip([str(x) for x in fileIds],newIds))
        newIds = fillIdsMap(df[element['name']], idMap)
        df[element['name']] = newIds
   
    
    return {'df' : df, 'cols' : cols}

