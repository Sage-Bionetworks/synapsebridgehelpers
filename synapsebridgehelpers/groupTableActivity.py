import synapsebridgehelpers
       
    
def groupTableActivity(syn,sourceProjId, extIdStr = '', activityNameFilters=[]):
    """ Groups all table versions according to corresponding activities, 
    has option to find tables with certain externalIds. activityNameFilters is used 
    to remove the unneccesary strings in table names before grouping them together 
    
    Arguments:
    - syn: a Synapse client object
    - sourceprojectId: Synapse Id of the project we want to group the tables in
    - extIdStr: the string the externalIds we are looking for should contain, is optional
    - activityNameFilters: list of strings to remove in the table names before grouping them
      together"""
    
    all_tables = synapsebridgehelpers.get_tables(syn,sourceProjId,activityNameFilters)
    table_activities = dict(all_tables.groupby(by='simpleName')['table.id'].apply(list))
    
    if extIdStr == '':
        return table_activities
    else:
        res = synapsebridgehelpers.externalIds2healthCodes(syn,list(all_tables['table.id']))
        res = res[res['externalId'].str.contains(extIdStr)]
        res = synapsebridgehelpers.find_tables_with_data(syn=syn, healthCodes=res['healthCode'], tables=all_tables)
        res = res[res['healthCodeCounts']>0]
        table_activities = dict(res.groupby(by='simpleName')['table.id'].apply(list))
        return table_activities
