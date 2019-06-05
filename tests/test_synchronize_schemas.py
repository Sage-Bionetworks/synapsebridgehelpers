import synapseclient as sc
from synapsebridgehelpers import synchronize_schemas
from copy import deepcopy
from conftest import table_schema

#TESTING_TABLE = "syn18503475"
#TESTING_TABLE_2 = "syn18503678"
#TEMP_PROJECT = "syn11657334"
#TESTING_TABLE_ORIGINAL = syn.tableQuery(
#        "select * from {}".format(TESTING_TABLE)).asDataFrame()
#TESTING_TABLE_2_ORIGINAL = syn.tableQuery(
#        "select * from {}".format(TESTING_TABLE_2)).asDataFrame()
#TESTING_TABLE_ORIGINAL_COLS = list(syn.getTableColumns(TESTING_TABLE))
#TESTING_TABLE_2_ORIGINAL_COLS = list(syn.getTableColumns(TESTING_TABLE_2))

def test_add_column(syn, new_tables):
    source_table = new_tables["schema"][0]
    target_table = new_tables["schema"][1]
    new_column = sc.Column(columnType="STRING", maximumSize=5, name="new_col")
    source_table.addColumn(new_column)
    source_table = syn.store(source_table)
    target_table = synchronize_schemas(
            syn,
            schema_comparison = {"added": ["new_col"]},
            source = source_table["id"],
            target = target_table["id"])
    source_cols = [c["name"] for c in syn.getTableColumns(source_table["id"])]
    target_cols = [c["name"] for c in syn.getTableColumns(target_table["id"])]
    assert all([c in source_cols for c in target_cols])

def test_remove_column(syn, new_tables):
    source_table = new_tables["schema"][0]
    target_table = new_tables["schema"][1]
    source_cols = new_tables["columns"][0]
    removed_col = source_cols[0]
    source_table.removeColumn(removed_col)
    source_table = syn.store(source_table)
    target_table = synchronize_schemas(
            syn,
            schema_comparison = {"removed": [removed_col["name"]]},
            source = source_table["id"],
            target = target_table["id"])
    source_cols = [c["name"] for c in syn.getTableColumns(source_table["id"])]
    target_cols = [c["name"] for c in syn.getTableColumns(target_table["id"])]
    assert all([c in source_cols for c in target_cols])

def test_modify_column(syn, new_tables):
    source_table = new_tables["schema"][0]
    target_table = new_tables["schema"][1]
    source_cols = new_tables["columns"][0]
    modified_col = source_cols[0]
    source_table.removeColumn(modified_col)
    modified_col["maximumSize"] = 100
    source_table.addColumn(modified_col)
    source_table = syn.store(source_table)
    target_table = synchronize_schemas(
            syn,
            schema_comparison = {"modified": [modified_col["name"]]},
            source = source_table["id"],
            target = target_table["id"])
    source_cols = [c["name"] for c in syn.getTableColumns(source_table["id"])]
    target_cols = [c["name"] for c in syn.getTableColumns(target_table["id"])]
    assert all([c in target_cols for c in source_cols])

def test_rename_column(syn, new_tables):
    source_table = new_tables["schema"][0]
    target_table = new_tables["schema"][1]
    source_cols = new_tables["columns"][0]
    source_table_vals = syn.tableQuery(
            "select * from {}".format(source_table["id"])).asDataFrame()
    renamed_col = source_cols[0]
    original_col_name = renamed_col["name"]
    new_col_name = "table_index"
    source_table.removeColumn(renamed_col)
    renamed_col["name"] = new_col_name
    renamed_col.pop("id")
    source_table.addColumn(renamed_col)
    source_table = syn.store(source_table)
    source_table_vals = source_table_vals.rename(
        {original_col_name: new_col_name}, axis=1)
    syn.store(sc.Table(source_table, source_table_vals))
    target_table = synchronize_schemas(
            syn,
            schema_comparison = {"renamed": {original_col_name: new_col_name}},
            source = source_table["id"],
            target = target_table["id"])
    source_cols = [c["name"] for c in syn.getTableColumns(source_table["id"])]
    target_cols = [c["name"] for c in syn.getTableColumns(target_table["id"])]
    assert all([c in target_cols for c in source_cols])
