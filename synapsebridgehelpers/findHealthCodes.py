import pandas as pd
from synapseclient.core.exceptions import SynapseHTTPError

def externalIds2healthCodes(syn,tables, continueOnMissingColumn=True):
    """Given a list of tables determines
    the healthCodes that map to externalIds.

    This function is used to find healthCodes in case that data is
    contributed both before and after externalIds have been assigned.

    Arguments:
    - `syn`: a Synapse client object
    - `tables`: list of table Ids
    """
    QUERY = 'SELECT distinct externalId, healthCode FROM %s'
    dfs = []
    for table in tables:
        try:
            idMap = syn.tableQuery(QUERY %table).asDataFrame()
        except SynapseHTTPError as err:
            if err.response.status_code == 400 and continueOnMissingColumn:
                pass
            else:
                raise err
        dfs.append(idMap)
    idMap = pd.concat(dfs)
    return idMap.drop_duplicates().dropna()


