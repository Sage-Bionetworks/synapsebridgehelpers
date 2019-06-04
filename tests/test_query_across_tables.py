import pytest
from synapsebridgehelpers import query_across_tables

def test_str_query(syn, tables):
    query_across_tables(syn, tables = [s["id"] for s in tables["schema"]],
                        query = "select * from {}")

def test_str_query_exception(syn, tables):
    with pytest.raises(TypeError):
        query_across_tables(syn, [s["id"] for s in tables["schema"]],
                            query = "select * from {}", substudy="my-study")

def test_one_table(syn, tables):
    query_across_tables(syn, tables = tables["schema"][0]["id"])

def test_one_table_one_query(syn, tables):
    query_across_tables(syn, tables = tables["schema"][0]["id"],
                        query = ["bool_property = true"])

def test_one_table_multiple_query(syn, tables):
    query_across_tables(syn, tables = tables["schema"][0]["id"],
                  query = ["bool_property = true", "str_property = 'red'"])

def test_one_table_one_substudy(syn, tables):
    query_across_tables(syn, tables = tables["schema"][0]["id"],
                        substudy = "my-study")

def test_one_table_multiple_substudy(syn, tables):
    query_across_tables(syn, tables = tables["schema"][0]["id"],
                        substudy = ["my-study", "other-study"])

def test_one_table_one_identifier(syn, tables):
    query_across_tables(syn, tables = tables["schema"][0]["id"],
                        identifier = "ABC")

def test_one_table_multiple_identifier(syn, tables):
    query_across_tables(syn, tables = tables["schema"][0]["id"],
                        identifier = ["ABC", "FGH"])

def test_one_table_function_identifier(syn, tables):
    query_across_tables(syn, tables = tables["schema"][0]["id"],
                        identifier = lambda s : s.startswith("A"))

def test_change_substudy_col(syn, tables):
    query_across_tables(syn, tables = tables["schema"][0]["id"],
                        substudy = "blue", substudy_col = "str_property")

def test_change_identifier_col(syn, tables):
    query_across_tables(syn, tables = tables["schema"][0]["id"],
                        identifier = "red", identifier_col = "str_property")

def test_two_tables(syn, tables):
    query_across_tables(syn, tables = [s["id"] for s in tables["schema"]])

def test_one_everything(syn, tables):
    query_across_tables(
            syn,
            tables = tables["schema"][0]["id"],
            query = ["str_property = 'blue'"],
            substudy = "other-study", identifier = "DEF")

def test_multiple_everything(syn, tables):
    query_across_tables(
            syn,
            tables = [s["id"] for s in tables["schema"]],
            query = ["bool_property = true", "str_property = 'red'"],
            substudy = ["my-study", "other-study"],
            identifier = ["DEF", "ABC", "FGH"])

def test_mix_everything(syn, tables):
    query_across_tables(
            syn,
            tables = [s["id"] for s in tables["schema"]],
            query = ["bool_property = true"],
            substudy = "other-study",
            identifier = ["DEF", "ABC", "FGH"])
