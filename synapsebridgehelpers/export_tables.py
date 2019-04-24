import synapseclient as sc
import synapseutils as su
import pandas as pd
import numpy as np
import synapsebridgehelpers


def replace_file_handles(syn, new_records, source, cols=None,
                         content_type = "application/json"):
    if cols is None:
        cols = syn.getColumns(source)
    for c in cols:
        if c["columnType"] == "FILEHANDLEID":
            fhids_to_copy = new_records[c["name"]].dropna().astype(int).tolist()
            new_fhids = []
            for i in range(0, len(fhids_to_copy), 100):
                fhids_to_copy_i = fhids_to_copy[i:i+100]
                new_fhids_i = su.copyFileHandles(
                        syn = syn,
                        fileHandles = fhids_to_copy_i,
                        associateObjectTypes = ["TableEntity"] * len(fhids_to_copy_i),
                        associateObjectIds = [source] * len(fhids_to_copy_i),
                        contentTypes = [content_type] * len(fhids_to_copy_i),
                        fileNames = [None] * len(fhids_to_copy_i))
                for j in [int(i["newFileHandle"]["id"]) for i in new_fhids_i["copyResults"]]:
                    new_fhids.append(j)
            new_fhids = pd.DataFrame(
                    {c["name"]: fhids_to_copy,
                     "new_fhids": new_fhids})
            new_records = new_records.merge(new_fhids, how="left", on=c["name"])
            new_records[c["name"]] = new_records["new_fhids"]
            new_records = new_records.drop("new_fhids", axis = 1)
            new_records = new_records.drop_duplicates(subset = "recordId")
        elif c["columnType"] not in ["INTEGER", "DOUBLE"]:
            new_records[c["name"]] = [
                    None if pd.isnull(i) else i for i in new_records[c["name"]]]
    return new_records


def parse_float_to_int(i):
    str_i = str(i)
    if "nan" == str_i:
        str_i = None
    elif str_i.endswith(".0"):
        str_i = str_i[:-2]
    return str_i


def sanitize_table(syn, records, target=None, cols=None):
    if cols is None and target is None:
        raise TypeError("Either target or cols must be set.")
    if cols is None:
        cols = syn.getTableColumns(target)
    for c in cols:
        #if c['columnType'] == 'STRING':
        #    if ('timezone' in c['name'] and
        #        type(records[c['name']].iloc[0]) is np.float64):
        #        records[c['name']] = list(map(parse_float_to_int, records[c['name']]))
        if (c["columnType"] in ["INTEGER", "DATE", "FILEHANDLEID", "USER"] and
            isinstance(records[c["name"]].iloc[0], np.float64)):
            records[c["name"]] = list(map(parse_float_to_int, records[c["name"]]))
    return records


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
    added_cols = set([c["name"] for c in source_cols if c not in target_cols])
    removed_cols = set([c["name"] for c in target_cols if c not in source_cols])
    modified_cols = set([c for c in added_cols if c in removed_cols])
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
                        raise Exception("A column containing file handles "
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
    source_table : pandas.DataFrame
        A dictionary containing (potential) keys:

        added
        removed
        modified
        renamed

        The value of renamed is a key mapping the column name in the target
        table to the new column name in the source table. The rest of the values
        are sets. If any of the keys are not relavant (e.g., no columns were
        modified, thus `modified` is just an empty set), you may omit that key.
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
    results = {}
    if isinstance(table_mapping, (list, str)): # export to brand new tables
        if target_project is None:
            raise TypeError("If passing a list to table_mapping, "
                            "target_project must be set.")
        source_tables = synapsebridgehelpers.filter_tables(
                syn, tables=table_mapping, as_data_frame=True, **kwargs)
        if isinstance(source_tables, dict):
            source_table_iter = source_tables.items()
        else: # table_mapping is only a single table
            if isinstance(table_mapping, str):
                source_table_iter = [(table_mapping, source_tables)]
            else:
                source_table_iter = [(table_mapping[0], source_tables)]
        for source_id, source_table in source_table_iter:
            source_table = source_table.drop(["ROW_ID", "ROW_VERSION"], axis=1)
            source_table_info = syn.get(source_id)
            source_table_cols = list(syn.getColumns(source_id))
            sanitized_source_table = sanitize_table(
                    syn,
                    records = source_table,
                    cols = source_table_cols)
            if copy_file_handles:
                sanitized_source_table = replace_file_handles(
                        syn,
                        new_records = sanitized_source_table,
                        source = source_id,
                        cols = source_table_cols)
            target_table_schema = sc.Schema(
                    name = source_table_info["name"],
                    parent = target_project,
                    columns = source_table_cols)
            target_table = sc.Table(
                    schema = target_table_schema,
                    values = sanitized_source_table)
            target_table = syn.store(target_table)
    elif isinstance(table_mapping, dict): # export to preexisting tables
        source_tables = synapsebridgehelpers.filter_tables(
                syn, table_mapping.keys(), **kwargs)
        for source, target in table_mapping.items():
            source_table = source_tables[source]
            target_table = syn.tableQuery("select * from {}".format(target))
            target_table = target_table.asDataFrame()
            if update:
                if reference_col is not None:
                    source_table = source_table.set_index(reference_col, drop=False)
                    target_table = target_table.set_index(reference_col, drop=False)
                else:
                    raise TypeError("If updating target tables with new records "
                                    "from a source table, you must specify a "
                                    "reference column as a basis for comparison.")
                # has the schema changed?
                source_cols = list(syn.getTableColumns(source))
                target_cols = list(syn.getTableColumns(target))
                schema_comparison = compare_schemas(source_cols, target_cols)
                new_records = source_table.loc[
                        source_table.index.difference(target_table.index)]
        #        if len(new_records):
        #            new_records = replace_file_handles(syn, new_records, source)
        #            new_records = sanitize_table(syn, target, new_records)
        #            new_target_table = sc.Table(target, new_records.values.tolist())
        #        try:
        #            syn.store(new_target_table, used = source)
        #        except:
        #            print(source)
        #            print(new_records)
    else:
        raise TypeError("table_mapping must be either a list (if exporting "
                        "tables to a target_project), str (if exporting a single "
                        "table to a project), or a dict (if exporting "
                        "tables to preexisting tables).")

