import synapseclient as sc
import uuid
from synapsebridgehelpers import synchronize_schemas
from copy import deepcopy

syn = sc.login()

TESTING_TABLE = "syn18503475"
TESTING_TABLE_2 = "syn18503678"
TEMP_PROJECT = "syn11657334"
TESTING_TABLE_ORIGINAL = syn.tableQuery(
        "select * from {}".format(TESTING_TABLE)).asDataFrame()
TESTING_TABLE_2_ORIGINAL = syn.tableQuery(
        "select * from {}".format(TESTING_TABLE_2)).asDataFrame()
TESTING_TABLE_ORIGINAL_COLS = list(syn.getTableColumns(TESTING_TABLE))
TESTING_TABLE_2_ORIGINAL_COLS = list(syn.getTableColumns(TESTING_TABLE_2))

def create_testing_table(syn, cols, values):
     testing_schema = sc.Schema(name = str(uuid.uuid4()),
                                columns = cols,
                                parent = TEMP_PROJECT)
     testing_table = sc.Table(testing_schema, values)
     testing_table = syn.store(testing_table)
     return testing_table

def delete_testing_tables(syn, testing_tables):
   for t in testing_tables:
       syn.delete(t)

def test_add_column():
    source_table = create_testing_table(
            syn, TESTING_TABLE_ORIGINAL_COLS, TESTING_TABLE_ORIGINAL)
    target_table = create_testing_table(
            syn, TESTING_TABLE_ORIGINAL_COLS, TESTING_TABLE_ORIGINAL)
    try:
        source_table = syn.get(source_table.tableId)
        target_table = syn.get(target_table.tableId)
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
    finally:
        delete_testing_tables(syn, [source_table, target_table])

def test_remove_column():
    source_table = create_testing_table(
            syn, TESTING_TABLE_ORIGINAL_COLS, TESTING_TABLE_ORIGINAL)
    target_table = create_testing_table(
            syn, TESTING_TABLE_ORIGINAL_COLS, TESTING_TABLE_ORIGINAL)
    try:
        source_table = syn.get(source_table.tableId)
        target_table = syn.get(target_table.tableId)
        source_cols = syn.getTableColumns(source_table["id"])
        removed_col = next(source_cols)
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
    finally:
        delete_testing_tables(syn, [source_table, target_table])

def test_modify_column():
    source_table = create_testing_table(
            syn, TESTING_TABLE_ORIGINAL_COLS, TESTING_TABLE_ORIGINAL)
    target_table = create_testing_table(
            syn, TESTING_TABLE_ORIGINAL_COLS, TESTING_TABLE_ORIGINAL)
    try:
        source_table = syn.get(source_table.tableId)
        target_table = syn.get(target_table.tableId)
        source_cols = syn.getTableColumns(source_table["id"])
        modified_col = next(source_cols)
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
    finally:
        delete_testing_tables(syn, [source_table, target_table])

def test_rename_column():
    source_table = create_testing_table(
            syn, TESTING_TABLE_ORIGINAL_COLS, TESTING_TABLE_ORIGINAL)
    target_table = create_testing_table(
            syn, TESTING_TABLE_ORIGINAL_COLS, TESTING_TABLE_ORIGINAL)
    try:
        source_table = syn.get(source_table.tableId)
        target_table = syn.get(target_table.tableId)
        source_table_vals = syn.tableQuery(
                "select * from {}".format(source_table["id"])).asDataFrame()
        source_cols = syn.getTableColumns(source_table["id"])
        renamed_col = next(source_cols)
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
    finally:
        delete_testing_tables(syn, [source_table, target_table])
