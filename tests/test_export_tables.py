import synapseclient as sc
import pytest
import uuid
from synapsebridgehelpers import export_tables
from copy import deepcopy

syn = sc.login()

TESTING_TABLE = "syn18503475"
TESTING_TABLE_2 = "syn18503678"
TESTING_TABLE_NAME = "testing_filter_table"
TESTING_TABLE_2_NAME = "testing_filter_table_2"
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

def create_testing_project(syn):
    testing_project = sc.Project(name = str(uuid.uuid4()))
    testing_project = syn.store(testing_project)
    return testing_project

def delete_testing_project(syn, project):
    syn.delete(project)

def test_export_one_table_to_new():
    project = create_testing_project(syn)
    try:
        exported_table = export_tables(
                syn,
                table_mapping = TESTING_TABLE,
                target_project = project["id"])
        exported_table_no_fh = exported_table[TESTING_TABLE][1].drop(
                "raw_data", axis = 1).reset_index(drop = True)
        testing_table_no_fh = TESTING_TABLE_ORIGINAL.drop(
                "raw_data", axis = 1).reset_index(drop = True)
        assert exported_table_no_fh.equals(testing_table_no_fh)
    finally:
        delete_testing_project(syn, project)

def test_export_one_table_to_new_no_filehandles():
    project = create_testing_project(syn)
    try:
        exported_table = export_tables(
                syn,
                table_mapping = TESTING_TABLE,
                target_project = project["id"],
                copy_file_handles = False)
        assert exported_table[TESTING_TABLE][1].equals(TESTING_TABLE_ORIGINAL)
    finally:
        delete_testing_project(syn, project)

def test_export_one_table_to_preexisting_update():
    project = create_testing_project(syn)
    try:
        schema = sc.Schema(
                name = TESTING_TABLE_NAME,
                columns = TESTING_TABLE_ORIGINAL_COLS,
                parent = project["id"])
        incomplete_table = deepcopy(
                 TESTING_TABLE_ORIGINAL.iloc[:len(TESTING_TABLE_ORIGINAL)//2])
        table = syn.store(sc.Table(schema, incomplete_table))
        exported_table = export_tables(
                syn,
                table_mapping = {TESTING_TABLE: table.tableId},
                update = True)
        updated_table = syn.tableQuery("select * from {}".format(table.tableId))
        updated_table = updated_table.asDataFrame().reset_index(drop = True)
        updated_table_no_fh = updated_table.drop("raw_data", axis = 1)
        update = exported_table[TESTING_TABLE][1]
        correct_table_no_fh = incomplete_table.append(
                update, ignore_index = True, sort = False)
        correct_table_no_fh = correct_table_no_fh.drop(
                "raw_data", axis = 1).reset_index(drop = True)
        print("returned results \n", updated_table_no_fh)
        print("correct result \n", correct_table_no_fh)
        assert updated_table_no_fh.equals(correct_table_no_fh)
    finally:
        delete_testing_project(syn, project)

def test_export_one_table_to_preexisting_no_update():
    project = create_testing_project(syn)
    try:
        schema = sc.Schema(
                name = TESTING_TABLE_NAME,
                columns = TESTING_TABLE_ORIGINAL_COLS,
                parent = project["id"])
        incomplete_table = deepcopy(
                 TESTING_TABLE_ORIGINAL.iloc[:len(TESTING_TABLE_ORIGINAL)//2])
        table = syn.store(sc.Table(schema, incomplete_table))
        exported_table = export_tables(
                syn,
                table_mapping = {TESTING_TABLE: table.tableId},
                update = False)
        updated_table = syn.tableQuery("select * from {}".format(table.tableId))
        updated_table = updated_table.asDataFrame().reset_index(drop = True)
        updated_table_no_fh = updated_table.drop("raw_data", axis = 1)
        comparison_table = TESTING_TABLE_ORIGINAL.drop(
                "raw_data", axis = 1).reset_index(drop = True)
        print(updated_table_no_fh)
        print(comparison_table)
        assert updated_table_no_fh.equals(comparison_table)
    finally:
        delete_testing_project(syn, project)

def test_export_multiple_tables_to_new():
    project = create_testing_project(syn)
    try:
        exported_table = export_tables(
                syn,
                table_mapping = [TESTING_TABLE, TESTING_TABLE_2],
                target_project = project["id"])
        exported_table_no_fh = exported_table[TESTING_TABLE][1].drop(
                "raw_data", axis = 1).reset_index(drop = True)
        exported_table_2_no_fh = exported_table[TESTING_TABLE_2][1].drop(
                "raw_data", axis = 1).reset_index(drop = True)
        testing_table_no_fh = TESTING_TABLE_ORIGINAL.drop(
                "raw_data", axis = 1).reset_index(drop = True)
        assert (exported_table_no_fh.equals(testing_table_no_fh) and
                exported_table_2_no_fh.equals(testing_table_no_fh))
    finally:
        delete_testing_project(syn, project)

def test_export_multiple_tables_to_preexisting_update():
    project = create_testing_project(syn)
    try:
        schema = sc.Schema(
                name = TESTING_TABLE_NAME,
                columns = TESTING_TABLE_ORIGINAL_COLS,
                parent = project["id"])
        incomplete_table = deepcopy(
                 TESTING_TABLE_ORIGINAL.iloc[:len(TESTING_TABLE_ORIGINAL)//2])
        table = syn.store(sc.Table(schema, incomplete_table))
        schema_2 = sc.Schema(
                name = TESTING_TABLE_2_NAME,
                columns = TESTING_TABLE_2_ORIGINAL_COLS,
                parent = project["id"])
        incomplete_table_2 = deepcopy(
                 TESTING_TABLE_2_ORIGINAL.iloc[:len(TESTING_TABLE_2_ORIGINAL)//3])
        table_2 = syn.store(sc.Table(schema_2, incomplete_table_2))
        exported_table = export_tables(
                syn,
                table_mapping = {
                    TESTING_TABLE: table.tableId,
                    TESTING_TABLE_2: table_2.tableId},
                update = True)
        updated_table = syn.tableQuery("select * from {}".format(table.tableId))
        updated_table = updated_table.asDataFrame().reset_index(drop = True)
        updated_table_no_fh = updated_table.drop("raw_data", axis = 1)
        update = exported_table[TESTING_TABLE][1]
        correct_table_no_fh = incomplete_table.append(
                update, ignore_index = True, sort = False)
        correct_table_no_fh = correct_table_no_fh.drop(
                "raw_data", axis = 1).reset_index(drop = True)
        updated_table_2 = syn.tableQuery("select * from {}".format(table_2.tableId))
        updated_table_2 = updated_table_2.asDataFrame().reset_index(drop = True)
        updated_table_2_no_fh = updated_table_2.drop("raw_data", axis = 1)
        update_2 = exported_table[TESTING_TABLE_2][1]
        correct_table_no_fh_2 = incomplete_table_2.append(
                update_2, ignore_index = True, sort = False)
        correct_table_no_fh_2 = correct_table_no_fh_2.drop(
                "raw_data", axis = 1).reset_index(drop = True)
        print("returned results \n", updated_table_no_fh)
        print("correct result \n", correct_table_no_fh)
        assert (updated_table_no_fh.equals(correct_table_no_fh) and
                updated_table_2_no_fh.equals(correct_table_no_fh_2))
    finally:
        delete_testing_project(syn, project)

def test_table_mapping_exception():
    with pytest.raises(TypeError):
        export_tables(syn, table_mapping = 42, update = True)

def test_kwargs():
    project = create_testing_project(syn)
    try:
        exported_table = export_tables(
                syn,
                table_mapping = TESTING_TABLE,
                target_project = project["id"],
                substudy = "my-study",
                substudy_col = "substudyMemberships")
        exported_table_no_fh = exported_table[TESTING_TABLE][1].drop(
                "raw_data", axis = 1).reset_index(drop = True)
        testing_table_no_fh = TESTING_TABLE_ORIGINAL.drop(
                "raw_data", axis = 1).reset_index(drop = True)
        to_keep = ["my-study" in s for s in
                   testing_table_no_fh.substudyMemberships.values]
        testing_table_no_fh = testing_table_no_fh.loc[to_keep]
        assert exported_table_no_fh.equals(testing_table_no_fh)
    finally:
        delete_testing_project(syn, project)
