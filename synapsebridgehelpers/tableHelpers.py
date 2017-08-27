import pandas as pd

def get_tables(syn, projectId, simpleNameFilters=[]):
    """Returns all the tables in a projects as a dataFrame with 
    columns for synapseId, table names, Version and Simplified Name
    
     Arguments:
    - syn: a Synapse client object
    - projectId: Synapse ID of the project we want tables from
    - simpleNameFilters: the strings that are to be 
    filtered out from the table names to create a simple name"""
    
    tables = syn.chunkedQuery('select name,id from table where projectId=="%s"' %projectId)
    tables = pd.DataFrame(list(tables))
    # removing tables named 'parkinson-status' and 'parkinson-appVersion'
    tables = tables[(tables['table.name']!='parkinson-status') &(tables['table.name']!='parkinson-appVersion')]
    tables['version'] = tables['table.name'].str.extract('(.)(-v\d+)', expand=True)[1]
    names = tables['table.name'].str.extract('([ -_a-z-A-Z\d]+)(-v\d+)',expand=True)[0]
    for word in simpleNameFilters:
        names = [name.replace(word,'') for name in names]
    tables['simpleName'] = names     
    return tables 

def find_tables_with_data(syn, tables, healthCodes):
    """Go through a list of tables and find those where there is data given a
    data frame with sought healthCodes.

    Returns the tables data frame with an additional column containing the 
    number of unique healthCodes found in each table."""
    query = ("select count(distinct healthCode) from %s where healthCode in ('"+
            "','".join(healthCodes)+"')")
    counts = [syn.tableQuery(query %synId, resultsAs='rowset').asInteger() for
              synId in tables['table.id']]
    tables['healthCodeCounts'] = counts
    return tables
