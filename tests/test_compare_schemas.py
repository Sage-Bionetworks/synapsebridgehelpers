import synapseclient as sc
import pytest
from synapsebridgehelpers import compare_schemas
from copy import deepcopy

syn = sc.login()

TESTING_TABLE = "syn18503475"
TESTING_TABLE_2 = "syn18503678"
TESTING_TABLE_ORIGINAL = syn.tableQuery(
        "select * from {}".format(TESTING_TABLE)).asDataFrame()
TESTING_TABLE_2_ORIGINAL = syn.tableQuery(
        "select * from {}".format(TESTING_TABLE_2)).asDataFrame()
TESTING_TABLE_ORIGINAL_COLS = list(syn.getTableColumns(TESTING_TABLE))
TESTING_TABLE_2_ORIGINAL_COLS = list(syn.getTableColumns(TESTING_TABLE_2))

def test_new_column():
    target_cols = deepcopy(TESTING_TABLE_2_ORIGINAL_COLS)
    source_cols = TESTING_TABLE_ORIGINAL_COLS
    target_cols.pop(-1) # raw_data
    result = compare_schemas(source_cols, target_cols)
    assert result["new"] == set(["raw_data"])

def test_removed_column():
    target_cols = TESTING_TABLE_2_ORIGINAL_COLS
    source_cols = deepcopy(TESTING_TABLE_ORIGINAL_COLS)
    source_cols.pop(-1) # raw_data
    result = compare_schemas(source_cols, target_cols)
    assert result["removed"] == set(["raw_data"])

def test_modified_column():
    target_cols = TESTING_TABLE_2_ORIGINAL_COLS
    source_cols = deepcopy(TESTING_TABLE_ORIGINAL_COLS)
    source_cols[1]['maximumSize'] = 100 # externalId
    result = compare_schemas(source_cols, target_cols)
    assert result["modified"] == set(["externalId"])

def test_renamed_column():
    target_cols = deepcopy(TESTING_TABLE_2_ORIGINAL_COLS)
    source_cols = TESTING_TABLE_ORIGINAL_COLS
    source_table = TESTING_TABLE_ORIGINAL
    target_table = deepcopy(TESTING_TABLE_2_ORIGINAL)
    target_table = target_table.rename({"externalId": "ext"}, axis = 1)
    target_cols[1]["name"] = "ext" # externalId
    result = compare_schemas(source_cols, target_cols,
                             source_table, target_table)
    assert result["renamed"] == {"ext": "externalId"}

def test_new_removed_column():
    target_cols = deepcopy(TESTING_TABLE_2_ORIGINAL_COLS)
    source_cols = deepcopy(TESTING_TABLE_ORIGINAL_COLS)
    source_table = deepcopy(TESTING_TABLE_ORIGINAL)
    target_table = deepcopy(TESTING_TABLE_2_ORIGINAL)
    target_cols.pop(-1) # raw_data
    target_table = target_table.drop("raw_data", axis = 1)
    source_cols.pop(1) # externalId
    source_table.drop("externalId", axis = 1)
    # should be interpreted as added column `raw_data`
    # and dropped column `externalId`
    result = compare_schemas(source_cols, target_cols, source_table, target_table)
    assert result["new"] == set(["raw_data"])
    assert result["removed"] == set(["externalId"])

def test_error_on_renamed_file_column():
    target_cols = deepcopy(TESTING_TABLE_2_ORIGINAL_COLS)
    source_cols = deepcopy(TESTING_TABLE_ORIGINAL_COLS)
    source_table = deepcopy(TESTING_TABLE_ORIGINAL)
    target_table = deepcopy(TESTING_TABLE_2_ORIGINAL)
    source_cols[-1]["name"] = "rawData"
    source_table = source_table.rename({"raw_data": "rawData"}, axis = 1)
    with(pytest.raises(Exception)):
        compare_schemas(source_cols, target_cols, source_table, target_table)
