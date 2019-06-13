import pytest
from synapsebridgehelpers import compare_schemas
from copy import deepcopy

def test_new_column(tables):
    target_cols = deepcopy(tables["columns"][1])
    source_cols = tables["columns"][0]
    target_cols.pop(-1) # raw_data
    result = compare_schemas(source_cols, target_cols)
    result["added"] == set(["raw_data"])

def test_removed_column(tables):
    target_cols = tables["columns"][1]
    source_cols = deepcopy(tables["columns"][0])
    source_cols.pop(-1) # raw_data
    result = compare_schemas(source_cols, target_cols)
    assert result["removed"] == set(["raw_data"])

def test_modified_column(tables):
    target_cols = tables["columns"][1]
    source_cols = deepcopy(tables["columns"][0])
    source_cols[1]['maximumSize'] = 100 # externalId
    result = compare_schemas(source_cols, target_cols)
    assert result["modified"] == set(["externalId"])

def test_renamed_column(tables, sample_table):
    target_cols = deepcopy(tables["columns"][1])
    source_cols = tables["columns"][0]
    source_table = sample_table
    target_table = deepcopy(sample_table)
    target_table = target_table.rename({"externalId": "ext"}, axis = 1)
    target_cols[1]["name"] = "ext" # externalId
    result = compare_schemas(source_cols, target_cols,
                             source_table, target_table)
    assert result["renamed"] == {"ext": "externalId"}

def test_new_removed_column(tables, sample_table):
    target_cols = deepcopy(tables["columns"][1])
    source_cols = deepcopy(tables["columns"][0])
    source_table = deepcopy(sample_table)
    target_table = deepcopy(sample_table)
    target_cols.pop(-1) # raw_data
    target_table = target_table.drop("raw_data", axis = 1)
    source_cols.pop(1) # externalId
    source_table.drop("externalId", axis = 1)
    # should be interpreted as added column `raw_data`
    # and dropped column `externalId`
    result = compare_schemas(source_cols, target_cols, source_table, target_table)
    assert result["added"] == set(["raw_data"])
    assert result["removed"] == set(["externalId"])

def test_error_on_renamed_file_column(tables, sample_table):
    target_cols = deepcopy(tables["columns"][1])
    source_cols = deepcopy(tables["columns"][0])
    source_table = deepcopy(sample_table)
    target_table = deepcopy(sample_table)
    source_cols[-1]["name"] = "rawData"
    source_table = source_table.rename({"raw_data": "rawData"}, axis = 1)
    print(source_cols)
    print(target_cols)
    print(source_table)
    print(target_table)
    with(pytest.raises(Exception)):
        compare_schemas(source_cols, target_cols, source_table, target_table)
