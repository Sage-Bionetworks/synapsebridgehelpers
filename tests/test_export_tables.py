import pytest
import uuid
import synapseclient as sc
import pandas as pd
from synapsebridgehelpers import export_tables
from copy import deepcopy


def test_export_one_table_to_new(syn, new_project, tables, sample_table):
    source_table = tables["schema"][0]["id"]
    exported_table = export_tables(
            syn,
            table_mapping = source_table,
            target_project = new_project["id"])
    exported_table_no_fh = exported_table[source_table][1].drop(
            "raw_data", axis = 1).reset_index(drop = True)
    testing_table_no_fh = sample_table.drop(
            "raw_data", axis = 1).reset_index(drop = True)
    pd.testing.assert_frame_equal(exported_table_no_fh, testing_table_no_fh)

def test_export_one_table_to_new_no_filehandles(syn, new_project, tables, sample_table):
    source_table = tables["schema"][0]["id"]
    exported_table = export_tables(
            syn,
            table_mapping = source_table,
            target_project = new_project["id"],
            copy_file_handles = False)
    exported_table[source_table][1].reset_index(drop=True, inplace=True)
    pd.testing.assert_frame_equal(exported_table[source_table][1], sample_table)

def test_export_one_table_to_preexisting_update(syn, new_project, tables, sample_table):
    source_table = tables["schema"][0]["id"]
    schema = sc.Schema(
            name = tables["schema"][0]["name"],
            columns = tables["columns"][0],
            parent = new_project["id"])
    incomplete_table = deepcopy(
             sample_table.iloc[:len(sample_table)//2])
    table = syn.store(sc.Table(schema, incomplete_table))
    exported_table = export_tables(
            syn,
            table_mapping = {source_table: table.tableId},
            update = True)
    updated_table = syn.tableQuery("select * from {}".format(table.tableId))
    updated_table = updated_table.asDataFrame().reset_index(drop = True)
    updated_table_no_fh = updated_table.drop("raw_data", axis = 1)
    update = exported_table[source_table][1]
    correct_table_no_fh = incomplete_table.append(
            update, ignore_index = True, sort = False)
    correct_table_no_fh = correct_table_no_fh.drop(
            "raw_data", axis = 1).reset_index(drop = True)
    print("returned results \n", updated_table_no_fh)
    print("correct result \n", correct_table_no_fh)
    pd.testing.assert_frame_equal(updated_table_no_fh, correct_table_no_fh)

def test_export_one_table_to_preexisting_no_update(syn, new_project, tables, sample_table):
    source_table = tables["schema"][0]["id"]
    schema = sc.Schema(
            name = tables["schema"][0]["name"],
            columns = tables["columns"][0],
            parent = new_project["id"])
    incomplete_table = deepcopy(
             sample_table.iloc[:len(sample_table)//2])
    table = syn.store(sc.Table(schema, incomplete_table))
    exported_table = export_tables(
            syn,
            table_mapping = {source_table: table.tableId},
            update = False)
    updated_table = syn.tableQuery("select * from {}".format(table.tableId))
    updated_table = updated_table.asDataFrame().reset_index(drop = True)
    updated_table_no_fh = updated_table.drop("raw_data", axis = 1)
    comparison_table = sample_table.drop(
            "raw_data", axis = 1).reset_index(drop = True)
    print(updated_table_no_fh)
    print(comparison_table)
    pd.testing.assert_frame_equal(updated_table_no_fh, comparison_table)

def test_export_multiple_tables_to_new(syn, new_project, tables, sample_table):
    source_table = tables["schema"][0]["id"]
    source_table_2 = tables["schema"][1]["id"]
    exported_table = export_tables(
            syn,
            table_mapping = [s["id"] for s in tables["schema"]],
            target_project = new_project["id"])
    exported_table_no_fh = exported_table[source_table][1].drop(
            "raw_data", axis = 1).reset_index(drop = True)
    exported_table_2_no_fh = exported_table[source_table_2][1].drop(
            "raw_data", axis = 1).reset_index(drop = True)
    testing_table_no_fh = sample_table.drop(
            "raw_data", axis = 1).reset_index(drop = True)
    assert (exported_table_no_fh.equals(testing_table_no_fh) and
            exported_table_2_no_fh.equals(testing_table_no_fh))

def test_export_multiple_tables_to_preexisting_update(syn, new_project,
                                                      tables, sample_table):
    source_table = tables["schema"][0]["id"]
    source_table_2 = tables["schema"][1]["id"]
    schema = sc.Schema(
            name = tables["schema"][0]["name"],
            columns = tables["columns"][0],
            parent = new_project["id"])
    incomplete_table = deepcopy(
             sample_table.iloc[:len(sample_table)//2])
    table = syn.store(sc.Table(schema, incomplete_table))
    schema_2 = sc.Schema(
            name = tables["schema"][1]["name"],
            columns = tables["columns"][1],
            parent = new_project["id"])
    incomplete_table_2 = deepcopy(
             sample_table.iloc[:len(sample_table)//3])
    table_2 = syn.store(sc.Table(schema_2, incomplete_table_2))
    exported_table = export_tables(
            syn,
            table_mapping = {
                source_table: table.tableId,
                source_table_2: table_2.tableId},
            update = True)
    updated_table = syn.tableQuery("select * from {}".format(table.tableId))
    updated_table = updated_table.asDataFrame().reset_index(drop = True)
    updated_table_no_fh = updated_table.drop("raw_data", axis = 1)
    update = exported_table[source_table][1]
    correct_table_no_fh = incomplete_table.append(
            update, ignore_index = True, sort = False)
    correct_table_no_fh = correct_table_no_fh.drop(
            "raw_data", axis = 1).reset_index(drop = True)
    updated_table_2 = syn.tableQuery("select * from {}".format(table_2.tableId))
    updated_table_2 = updated_table_2.asDataFrame().reset_index(drop = True)
    updated_table_2_no_fh = updated_table_2.drop("raw_data", axis = 1)
    update_2 = exported_table[source_table_2][1]
    correct_table_no_fh_2 = incomplete_table_2.append(
            update_2, ignore_index = True, sort = False)
    correct_table_no_fh_2 = correct_table_no_fh_2.drop(
            "raw_data", axis = 1).reset_index(drop = True)
    print("returned results \n", updated_table_no_fh)
    print("correct result \n", correct_table_no_fh)
    assert (updated_table_no_fh.equals(correct_table_no_fh) and
            updated_table_2_no_fh.equals(correct_table_no_fh_2))

def test_table_mapping_exception(syn):
    with pytest.raises(TypeError):
        export_tables(syn, table_mapping = 42, update = True)

def test_kwargs(syn, tables, new_project, sample_table):
    source_table = tables["schema"][0]["id"]
    exported_table = export_tables(
            syn,
            table_mapping = source_table,
            target_project = new_project["id"],
            substudy = "my-study",
            substudy_col = "substudyMemberships")
    exported_table_no_fh = exported_table[source_table][1].drop(
            "raw_data", axis = 1).reset_index(drop = True)
    testing_table_no_fh = sample_table.drop(
            "raw_data", axis = 1).reset_index(drop = True)
    to_keep = ["my-study" in s for s in
               testing_table_no_fh.substudyMemberships.values]
    testing_table_no_fh = testing_table_no_fh.loc[to_keep]
    pd.testing.assert_frame_equal(exported_table_no_fh, testing_table_no_fh)

def test_schema_change(syn, tables, new_project, sample_table):
    source_table = tables["schema"][0]["id"]
    target_table_cols = deepcopy(tables["columns"][0])
    added_col = target_table_cols.pop(2)
    renamed_original_name = target_table_cols[2]["name"]
    target_table_cols[2]["name"] = "renamed_col"
    target_table_cols[3]["maximumSize"] = 100
    schema = sc.Schema(
            name = tables["schema"][0]["name"],
            columns = target_table_cols,
            parent = new_project["id"])
    incomplete_table = deepcopy(
             sample_table.iloc[:len(sample_table)//2])
    incomplete_table = incomplete_table.drop(added_col["name"], axis=1)
    incomplete_table = incomplete_table.rename(
            {renamed_original_name: "renamed_col"}, axis=1)
    table = syn.store(sc.Table(schema, incomplete_table))
    exported_table = export_tables(
            syn,
            table_mapping = {source_table: table.tableId},
            update = False)
    updated_table = syn.tableQuery("select * from {}".format(table.tableId))
    updated_table = updated_table.asDataFrame().reset_index(drop = True)
    updated_table_no_fh = updated_table.drop("raw_data", axis = 1)
    comparison_table = sample_table.drop(
            "raw_data", axis = 1).reset_index(drop = True)
    updated_table_no_fh = updated_table_no_fh[comparison_table.columns]
    print(updated_table_no_fh)
    print(comparison_table)
    pd.testing.assert_frame_equal(updated_table_no_fh, comparison_table)
