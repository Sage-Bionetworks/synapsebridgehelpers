import synapseclient as sc
import pandas as pd
import multiprocessing
import warnings

def get_tables(syn, projectId, simpleNameFilters=[]):
    """Returns all the tables in a projects as a dataFrame with
    columns for synapseId, table names, Version and Simplified Name

     Arguments:
    - syn: a Synapse client object
    - projectId: Synapse ID of the project we want tables from
    - simpleNameFilters: the strings that are to be
    filtered out from the table names to create a simple name"""

    tables = syn.getChildren(projectId, includeTypes=['table'])
    tables = pd.DataFrame(list(tables))
    # removing tables named 'parkinson-status' and 'parkinson-appVersion'
    tables = tables[(tables['name'] != 'parkinson-status') &
                    (tables['name'] != 'parkinson-appVersion')]
    tables['version'] = tables['name'].str.extract('(.)(-v\d+)', expand=True)[1]
    names = tables['name'].str.extract('([ -_a-z-A-Z\d]+)(-v\d+)', expand=True)[0]
    print(names)
    for word in simpleNameFilters:
        names = [name.replace(word, '') for name in names]
    tables['simpleName'] = names
    return tables


def find_tables_with_data(syn, tables, healthCodes):
    """Go through a list of tables and find those where there is data given a
    data frame with sought healthCodes.

    Returns the tables data frame with an additional column containing the
    number of unique healthCodes found in each table."""
    query = ("select count(distinct healthCode) from %s where healthCode in ('" +
             "','".join(healthCodes) + "')")
    counts = [syn.tableQuery(query % synId, resultsAs='rowset').asInteger() for
              synId in tables['id']]
    tables['healthCodeCounts'] = counts
    return tables


def safe_query(query_str, syn, continueOnMissingColumn):
    try:
        return syn.tableQuery(query_str)
    except sc.exceptions.SynapseHTTPError as e:
        if e.response.status_code == 400 and continueOnMissingColumn:
            return
        else:
            raise Exception("Invalid query:\n\n{}".format(query_str)) from e

def query_across_tables(syn, tables, query=None,
                        substudy=None, external_id=None,
                        substudy_col="substudyMemberships",
                        external_id_col="externalId", as_data_frame = True,
                        continueOnMissingColumn=True):
    """Retrieve all records that match a filtering criteria. Two convenience
    parameters (substudy and external_id) are provided to filter by one or more
    values of that respective parameter. The filtering criteria use logical
    conjunction (AND), meaning rows will only be returned if they match all
    the filtering criteria.

    Parameters
    ----------
    syn : synapseclient.Synapse
    tables : str or array-like
        Synapse IDs of the table(s) you are filtering from.
    query : str or array-like
        If a string, a raw query with an unassigned string in the from clause.
        E.g. "select foo from %s". If a list, a list of SQL WHERE statements
        compatible with Synapse Table querying. If passing query as a string,
        the query will not be modified even when setting other function
        parameters.
    substudy : str or array-like
        Matches if `substudy_col` contains one or more of these arguments.
        (Uses SQL LIKE).
    external_id : str or array-like
        Matches if `external_id_col` is equal to (str) or contained in (list)
        this argument. (Uses SQL IN).
    substudy_col : str
        The column to reference for the `substudy` parameter.
    external_id_col : str
        The column to reference for the `external_id` parameter.
    as_data_frame : boolean, default True
        Return the results as pandas DataFrames, rather than
        synapseclient.tableQuery objects.

    Returns
    -------
    If `tables` is a str, returns a pandas DataFrame.
    If `tables` is a list, returns a dict where the keys are Synapse IDs and
    the values are pandas DataFrames.
    """
    filtered_tables = {}
    if isinstance(query, str):
        warnings.warn("The passed `query` is a string and will be passed "
                      "directly to the client. If you want to enable other "
                      "parameters of this function, do not use the `query` "
                      "parameter or pass a list of SQL logical WHERE criteria "
                      "to the `query` parameter.")
    if isinstance(tables, str):
        tables = [tables]
    if substudy is not None:
        if isinstance(substudy, str):
            substudy = [substudy]
        substudy_list = ["{} LIKE '%{}%'".format(substudy_col, s) for s in substudy]
        substudy_str = "({})".format(" OR ".join(substudy_list))
    if external_id is not None:
        if isinstance(external_id, str):
            external_id = [external_id]
        external_id_str = "('{}')".format("', '".join(external_id))
    queries = []
    for t in tables:
        if isinstance(query, str):
            queries.append(query % t)
        else:
            query_str = "SELECT * FROM {}".format(t)
            if (query is not None or substudy is not None
                    or external_id is not None):
                query_str = "{} WHERE".format(query_str)
            if substudy is not None:
                query_str = "{} {}".format(query_str, substudy_str)
            if external_id is not None:
                if substudy is not None:
                    query_str = "{} AND".format(query_str)
                query_str = "{} {} IN {}".format(
                        query_str, external_id_col, external_id_str)
            if query is not None:
                if substudy is not None or external_id is not None:
                    query_str = "{} AND".format(query_str)
                query_str = "{} {}".format(query_str, " AND ".join(query))
            queries.append(query_str)
    mp = multiprocessing.dummy.Pool(8)
    filtered_tables = mp.map(
            lambda q : safe_query(q, syn, continueOnMissingColumn), queries)
    if as_data_frame:
        filtered_tables = [q.asDataFrame()
                           for q in filtered_tables]
    return filtered_tables

