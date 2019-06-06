import os
import synapsebridgehelpers
import synapseclient as sc
import numpy as np


def replace_file_handles(syn, df, source_table_id, source_table_cols=None,
                         content_type = "application/json"):
    """Replace the file handles in columns of type 'FILEHANDLEID'

    Parameters
    ----------
    syn : synapseclient.Synapse
    df : pandas.DataFrame
    source_table_id : str
        Synapse ID of the table the original file handles belong to.
    source_table_cols : iterable of synapseclient.Column objects
    content_type : str

    Returns
    -------
    The pandas.DataFrame `df` but with new file handle values in columns
    of type 'FILEHANDLEID' within the source table.
    """
    if source_table_cols is None:
        source_table_cols = syn.getTableColumns(source_table_id)
    for c in source_table_cols:
        if c["columnType"] == "FILEHANDLEID":
            fhid_map = synapsebridgehelpers.copyFileIdsInBatch(
                    syn,
                    table_id = source_table_id,
                    fileIds = df[c["name"]],
                    content_type = content_type)
            fhid_map = {str(k): str(v) for k, v in fhid_map.items()}
            df[c["name"]] = df[c["name"]].apply(
                    parse_float_to_int).map(fhid_map)
    return df


def parse_float_to_int(i):
    str_i = str(i)
    if str_i == "nan":
        str_i = None
    elif str_i.endswith(".0"):
        str_i = str_i[:-2]
    return str_i


def _sanitize_table(syn, records, target=None, cols=None):
    """Format the values and dtypes of a pandas DataFrame so that it may be
    uploaded to Synapse as a Table.

    Parameters
    ----------
    syn : synapseclient.Synapse
    records : pandas.DataFrame
    target : str
        Synapse ID where `records` will be stored to.
    cols : list of synapseclient.Column
        The Column objects of the Synapse Table where `records` will be
        stored to.

    Returns
    -------
    A pandas DataFrame with values and their dtypes modified in a way that
    allows them to be stored to Synapse as a Table.
    """
    if cols is None and target is None:
        raise TypeError("Either target or cols must be set.")
    if cols is None:
        cols = syn.getTableColumns(target)
    for c in cols:
        if (c["columnType"] in ["INTEGER", "DATE", "FILEHANDLEID", "USER"] and
            isinstance(records[c["name"]].iloc[0], np.float64)):
            records[c["name"]] = list(map(parse_float_to_int, records[c["name"]]))
    return records


def dump_on_error(df, e, syn, source_table, target_table):
    """Write `df` to the current directory, send an email to the current
    Synapse user, and raise an exception.

    Parameters
    ----------
    df : pandas.DataFrame
    e : Exception
    syn : synapseclient.Synapse
    source_table : str
    target_table : str

    Returns
    -------
    None
    """
    dump_name = "target_table_dump.csv"
    df.to_csv(dump_name)
    this_user = syn.getUserProfile()
    syn.sendMessage(userIds=[this_user["ownerId"]],
                    messageSubject="Failed Table Export",
                    messageBody="There was a failed attempt to export table {0} "
                                "to table {1} after an attempted schema change "
                                "to {1}. The contents of {1} have been written "
                                "to {2}.".format(source_table, target_table,
                                    os.path.join(os.getcwd(), dump_name)))
    raise Exception(
            "There was a problem synchronizing the source and target schemas. "
            "The target table has been saved to {} in the current directory "
            "as a precautionary measure".format(
                "target_table_dump.csv")) from e

def compare_schemas(source_cols, target_cols, source_table=None,
                    target_table=None, rename_threshold=0.9):
    """Compare two Table schemas to find the differences between them.
    A difference is either classified as "added" (when the source schema contains
    a column that the target schema lacks), "removed" (when the target schema
    contains a column that the source table lacks) "modified", or "renamed".
    A column is classified as modified if one of the column properties other
    than `name` has been changed. A column will
    only be classified as renamed if the table data is provided for each schema.
    If there is a column in the target table of the same type as a potentially
    renamed column in the source table, and that column contains a data overlap
    greater than `rename_threshold`, the column will be classified as renamed.

    Parameters
    ----------
    source_cols : list of synapseclient.Column objects
    target_cols : list of synapseclient.Column objects
    source_table : pandas.DataFrame
    target_table : pandas.DataFrame
    rename_threshold : float
        When comparing column values between `source_table` and `target_table`,
        if a proportion greater than the `rename_threshold` is shared between
        the columns, these column are considered to be the same column, even
        if they have different names. This results in the columns being labled
        as "renamed". Values are compared both on actual value and index, up to
        the length of the shorter column.

    Returns
    -------
    A dictionary containing keys:

    added
    removed
    modified
    renamed

    The value of renamed is a key mapping the column name in the target table
    to the new column name in the source table. The rest of the values are sets.
    """
    comparison = {}
    source_cols_dic = {c["name"]: c for c in source_cols}
    target_cols_dic = {c["name"]: c for c in target_cols}
    added_cols = {c["name"] for c in source_cols if c not in target_cols}
    removed_cols = {c["name"] for c in target_cols if c not in source_cols}
    modified_cols = {c for c in added_cols if c in removed_cols}
    for c in modified_cols:
        added_cols.discard(c)
        removed_cols.discard(c)
    renamed_cols = {}
    if (source_table is not None and target_table is not None and
        len(added_cols) and len(removed_cols)):
        for source_col in added_cols:
            for target_col in removed_cols:
                if (source_cols_dic[source_col]["columnType"] ==
                    target_cols_dic[target_col]["columnType"]):
                    if source_cols_dic[source_col]["columnType"] == "FILEHANDLEID":
                        raise sc.exceptions.SynapseMalformedEntityError(
                                "A column containing file handles "
                                "was potentially renamed.")
                    overlap = [i == j for i, j in
                               zip(source_table[source_col],
                                   target_table[target_col])]
                    if sum(overlap) / len(overlap) >= rename_threshold:
                        renamed_cols[target_col] = source_col
        for target_col, source_col in renamed_cols.items():
            removed_cols.discard(target_col)
            added_cols.discard(source_col)
    comparison["renamed"] = renamed_cols
    comparison["modified"] = modified_cols
    comparison["removed"] = removed_cols
    comparison["added"] = added_cols
    return comparison


def synchronize_schemas(syn, schema_comparison, source, target,
                        source_cols=None, target_cols=None):
    """Update (on Synapse) a target Schema to match a source Schema.

    Parameters
    ----------
    syn : synapseclient.Synapse
    schema_comparison : dict
        Normally the returned object from a call to `compare_schemas`.

        A dictionary containing keys:

        added
        removed
        modified
        renamed

        If any of the keys are not relavant (e.g., no columns were
        modified, thus `modified` is just an empty set), you may omit that key.
        The value of renamed is a key mapping the column name in the target table
        to the new column name in the source table. The rest of the values are sets.
    source : str
        Synapse ID of a source table on Synapse
    target : str
        Synapse ID of a target table on Synapse. The schema of this table will
        be modified to match that of `source`.
    source_cols : list of synapseclient.Column objects
    target_cols : list of synapseclient.Column objects

    Returns
    -------
    The synchronized Schema of the target Table.
    """
    target_schema = syn.get(target)
    if source_cols is None:
        source_cols = syn.getTableColumns(source)
    if target_cols is None:
        target_cols = syn.getTableColumns(target)
    for action, cols in schema_comparison.items():
        if action == "added":
            added_columns = list(filter(lambda c : c["name"] in cols,
                                      source_cols))
            target_schema.addColumns(added_columns)
        if action == "removed":
            removed_columns = filter(lambda c : c["name"] in cols, target_cols)
            for c in removed_columns:
                target_schema.removeColumn(c)
        if action == "modified":
            modified_source_columns = list(filter(lambda c : c["name"] in cols,
                                                  source_cols))
            modified_target_columns = list(filter(lambda c : c["name"] in cols,
                                                  target_cols))
            for c in modified_target_columns:
                target_schema.removeColumn(c)
            target_schema.addColumns(modified_source_columns)
        if action == "renamed":
            for target_name, source_name in cols.items():
                renamed_source_column = next(
                        filter(lambda c : c["name"] == source_name, source_cols))
                renamed_target_column = next(
                        filter(lambda c : c["name"] == target_name, target_cols))
                target_schema.removeColumn(renamed_target_column)
                target_schema.addColumn(renamed_source_column)
    target_schema = syn.store(target_schema)
    return target_schema


def export_tables(syn, table_mapping, target_project=None, update=True,
                  reference_col="recordId", copy_file_handles=True, **kwargs):
    """Copy rows from one Synapse table to another. Or copy tables
    to a new table in a separate project.

    Parameters
    ----------
    syn : synapseclient.Synapse
    table_mapping : dict, list, or str
        If exporting records of one or more tables to other, preexisting tables,
        table_mapping is a dictionary containing Synapse ID key/value mappings
        from source to target tables. If exporting table records to not yet
        created tables in a seperate project, table_mapping can be a list or
        string.
    target_project : str, default None
        If exporting table records to not yet created tables in a seperate
        project, specify the target project's Synapse ID here.
    update : bool, default True
        When exporting records of one or more tables to other, preexisting
        tables, whether to append new records to these tables or completely
        overwrite the table records.
    referenceCol : str
        If `update` is True, use this column as the table index to determine
        which records are already present in the target table.
    copy_file_handles : bool, default True
        Whether to copy the file handles from the source table to the target
        table. If you are not the creator of these file handles, this must
        be set to True if you want to a column containing file handles in your
        target table.
    **kwargs
        Additional named arguments to pass to synapsebridgehelpers.query_across_tables

    Returns
    -------
    """
    results = {}
    if isinstance(table_mapping, (list, str)): # export to brand new tables
        if target_project is None:
            raise TypeError("If passing a list to table_mapping, "
                            "target_project must be set.")
        source_tables = synapsebridgehelpers.query_across_tables(
                syn, tables=table_mapping, as_data_frame=True, **kwargs)
        if isinstance(table_mapping, str):
            source_table_iter = [(table_mapping, source_tables[0])]
        else:
            source_table_iter = zip(table_mapping, source_tables)
        for source_id, source_table in source_table_iter:
            source_table_info = syn.get(source_id)
            source_table_cols = list(syn.getTableColumns(source_id))
            if copy_file_handles:
                source_table = replace_file_handles(
                        syn,
                        df = source_table,
                        source_table_id = source_id,
                        source_table_cols = source_table_cols)
            sanitized_source_table = _sanitize_table(
                    syn,
                    records = source_table,
                    cols = source_table_cols)
            target_table_schema = sc.Schema(
                    name = source_table_info["name"],
                    parent = target_project,
                    columns = source_table_cols)
            target_table = sc.Table(
                    schema = target_table_schema,
                    values = sanitized_source_table)
            target_table = syn.store(target_table)
            results[source_id] = (target_table.tableId, source_table)
    elif isinstance(table_mapping, dict): # export to preexisting tables
        tables = list(table_mapping)
        source_tables = synapsebridgehelpers.query_across_tables(
                syn, tables, **kwargs)
        source_tables = {t: df for t, df in zip(tables, source_tables)}
        for source, target in table_mapping.items():
            source_table = source_tables[source]
            target_table = syn.tableQuery("select * from {}".format(target))
            target_table = target_table.asDataFrame()
            # has the schema changed?
            source_cols = list(syn.getTableColumns(source))
            target_cols = list(syn.getTableColumns(target))
            schema_comparison = compare_schemas(
                    source_cols = source_cols,
                    target_cols = target_cols,
                    source_table = source_table,
                    target_table = target_table)
            try: # error after updating schema -> data may be lost on Synapse
                if len(schema_comparison.values()):
                    synchronize_schemas(
                            syn,
                            schema_comparison = schema_comparison,
                            source = source,
                            target = target,
                            source_cols = source_cols,
                            target_cols = target_cols)
                    # synchronize schema of pandas DataFrame with Synapse
                    for col in schema_comparison["removed"]:
                        target_table = target_table.drop(col, axis = 1)
                    target_table = target_table.rename(
                            schema_comparison["renamed"], axis = 1)
                    target_table = _sanitize_table(syn, target_table, target)
                    syn.store(sc.Table(target, target_table))
            except Exception as e:
                dump_on_error(target_table, e, syn, source, target)
            if update:
                if reference_col is not None:
                    source_table = source_table.set_index(reference_col, drop=False)
                    target_table = target_table.set_index(reference_col, drop=False)
                else:
                    raise TypeError("If updating target tables with new records "
                                    "from a source table, you must specify a "
                                    "reference column as a basis for comparison.")
                new_records = source_table.loc[
                        source_table.index.difference(target_table.index)]
                if len(new_records):
                    if (copy_file_handles):
                        new_records = replace_file_handles(
                                syn, df = new_records, source_table_id = source)
                    new_records = _sanitize_table(
                            syn, records = new_records, target = target)
                    new_target_table = sc.Table(
                            target, new_records.values.tolist())
                    syn.store(new_target_table, used = source)
                    results[source] = (target, new_records)
            else:
                target_table = syn.tableQuery("select * from {}".format(target))
                syn.delete(target_table.asRowSet())
                table_to_store = source_table.reset_index(drop=True)
                syn.store(sc.Table(target, table_to_store))
                results[source] = (target, table_to_store)
    else:
        raise TypeError("table_mapping must be either a list (if exporting "
                        "tables to a target_project), str (if exporting a single "
                        "table to a project), or a dict (if exporting "
                        "tables to preexisting tables).")
    return results

