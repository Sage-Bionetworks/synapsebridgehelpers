import synapseclient as sc
import pandas as pd

def filter_tables(syn, tables, filter_criteria=None, substudy=None,
                  external_id=None, substudy_col="substudyMemberships",
                  external_id_col="externalId", as_data_frame = True):
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
    filter_criteria : str or array-like
        A list of SQL statements compatible with Synapse Table querying.
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
    if isinstance(tables, str):
        tables = [tables]
    if substudy is not None:
        if isinstance(substudy, str):
            substudy = [substudy]
        substudy_list = ["{} LIKE '%{}%'".format(substudy_col, s) for s in substudy]
        substudy_str = "({})".format(" OR ".join(substudy_list))
    if isinstance(filter_criteria, str):
        filter_criteria = [filter_criteria]
    if external_id is not None:
        if isinstance(external_id, str):
            external_id = [external_id]
        external_id_str = "('{}')".format("', '".join(external_id))
    for t in tables:
        query = "SELECT * FROM {}".format(t)
        if (filter_criteria is not None or substudy is not None
                or external_id is not None):
            query = "{} WHERE".format(query)
        if substudy is not None:
            query = "{} {}".format(query, substudy_str)
        if external_id is not None:
            if substudy is not None:
                query = "{} AND".format(query)
            query = "{} {} IN {}".format(query, external_id_col, external_id_str)
        if filter_criteria is not None:
            if substudy is not None or external_id is not None:
                query = "{} AND".format(query)
            query = "{} {}".format(query, " AND ".join(filter_criteria))
        try:
            filtered_tables[t] = syn.tableQuery(query)
        except sc.exceptions.SynapseHTTPError as e:
            raise Exception("Invalid query:\n\n{}".format(query)) from e
    if len(filtered_tables) == 1:
        filtered_table = filtered_tables.pop(tables[0])
        if as_data_frame:
            return filtered_table.asDataFrame()
        else:
            return filterd_table
    elif as_data_frame:
            filtered_tables = {k: filtered_tables[k].asDataFrame()
                               for k in filtered_tables}
    return filtered_tables

