import synapseclient as sc
from synapsebridgehelpers import filter_tables

syn = sc.login()

TESTING_TABLE = "syn18503475"
TESTING_TABLE_2 = "syn18503678"

def test_one_table():
    filter_tables(syn, tables = TESTING_TABLE)

def test_one_table_one_filter_criteria():
    filter_tables(syn, tables = TESTING_TABLE, filter_criteria = "bool_property = true")

def test_one_table_multiple_filter_criteria():
    filter_tables(syn, tables = TESTING_TABLE,
                  filter_criteria = ["bool_property = true", "str_property = 'red'"])

def test_one_table_one_substudy():
    filter_tables(syn, tables = TESTING_TABLE, substudy = "my-study")

def test_one_table_multiple_substudy():
    filter_tables(syn, tables = TESTING_TABLE, substudy = ["my-study", "other-study"])

def test_one_table_one_external_id():
    filter_tables(syn, tables = TESTING_TABLE, external_id = "ABC")

def test_one_table_multiple_external_id():
    filter_tables(syn, tables = TESTING_TABLE, external_id = ["ABC", "FGH"])

def test_change_substudy_col():
    filter_tables(syn, tables = TESTING_TABLE, substudy = "blue",
                  substudy_col = "str_property")

def test_change_external_id_col():
    filter_tables(syn, tables = TESTING_TABLE, external_id = "red",
                  external_id_col = "str_property")

def test_two_tables():
    filter_tables(syn, tables = [TESTING_TABLE, TESTING_TABLE_2])

def test_one_everything():
    filter_tables(
            syn, tables = TESTING_TABLE, filter_criteria = "str_property = 'blue'",
            substudy = "other-study", external_id = "DEF")

def test_multiple_everything():
    filter_tables(
            syn,
            tables = [TESTING_TABLE, TESTING_TABLE_2],
            filter_criteria = ["bool_property = true", "str_property = 'red'"],
            substudy = ["my-study", "other-study"],
            external_id = ["DEF", "ABC", "FGH"])

def test_mix_everything():
    filter_tables(
            syn,
            tables = [TESTING_TABLE, TESTING_TABLE_2],
            filter_criteria = "bool_property = true",
            substudy = "other-study",
            external_id = ["DEF", "ABC", "FGH"])
