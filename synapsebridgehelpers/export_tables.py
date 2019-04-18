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
        pass
        # TODO
        #source_tables = synapsebridgehelpers.filter_tables(
        #        syn, table_mapping.keys(), **kwargs)
        #for source, target in table_mapping.items():
        #    source_table = source_table.asDataFrame().set_index(
        #            reference_col, drop=False)
        #    target_table = syn.tableQuery("select * from {}".format(target))
        #    target_table = target_table.asDataFrame().set_index("recordId", drop=False)
        #    new_records = source_table.loc[
        #            source_table.index.difference(target_table.index)]
        #    if len(new_records): # new records found from the relevant external ids
        #        new_records = replace_file_handles(syn, new_records, source)
        #        new_records = sanitize_table(syn, target, new_records)
        #        new_target_table = sc.Table(target, new_records.values.tolist())
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

