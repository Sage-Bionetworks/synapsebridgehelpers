import synapseclient as sc
import pytest
from synapsebridgehelpers import query_across_tables

syn = sc.login()

TESTING_TABLE = "syn18503475"
TESTING_TABLE_2 = "syn18503678"

def test_str_query():
    query_across_tables(syn, tables = [TESTING_TABLE, TESTING_TABLE_2],
                        query = "select * from %s")

def test_str_query_exception():
    with pytest.raises(Exception):
        query_across_tables(syn, tables = [TESTING_TABLE, TESTING_TABLE_2],
                            query = "select * from %s", substudy="my-study")

def test_one_table():
    query_across_tables(syn, tables = TESTING_TABLE)

def test_one_table_one_query():
    query_across_tables(syn, tables = TESTING_TABLE, query = ["bool_property = true"])

def test_one_table_multiple_query():
    query_across_tables(syn, tables = TESTING_TABLE,
                  query = ["bool_property = true", "str_property = 'red'"])

def test_one_table_one_substudy():
    query_across_tables(syn, tables = TESTING_TABLE, substudy = "my-study")

def test_one_table_multiple_substudy():
    query_across_tables(syn, tables = TESTING_TABLE, substudy = ["my-study", "other-study"])

def test_one_table_one_identifier():
    query_across_tables(syn, tables = TESTING_TABLE, identifier = "ABC")

def test_one_table_multiple_identifier():
    query_across_tables(syn, tables = TESTING_TABLE, identifier = ["ABC", "FGH"])

def test_one_table_function_identifier():
    query_across_tables(syn, tables = "syn18503475",
                        identifier = lambda s : s.startswith("A"))

def test_change_substudy_col():
    query_across_tables(syn, tables = TESTING_TABLE, substudy = "blue",
                  substudy_col = "str_property")

def test_change_identifier_col():
    query_across_tables(syn, tables = TESTING_TABLE, identifier = "red",
                  identifier_col = "str_property")

def test_two_tables():
    query_across_tables(syn, tables = [TESTING_TABLE, TESTING_TABLE_2])

def test_one_everything():
    query_across_tables(
            syn, tables = TESTING_TABLE, query = ["str_property = 'blue'"],
            substudy = "other-study", identifier = "DEF")

def test_multiple_everything():
    query_across_tables(
            syn,
            tables = [TESTING_TABLE, TESTING_TABLE_2],
            query = ["bool_property = true", "str_property = 'red'"],
            substudy = ["my-study", "other-study"],
            identifier = ["DEF", "ABC", "FGH"])

def test_mix_everything():
    query_across_tables(
            syn,
            tables = [TESTING_TABLE, TESTING_TABLE_2],
            query = ["bool_property = true"],
            substudy = "other-study",
            identifier = ["DEF", "ABC", "FGH"])
