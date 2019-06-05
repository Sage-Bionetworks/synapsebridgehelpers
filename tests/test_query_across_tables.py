import pytest
import pandas as pd
from synapsebridgehelpers import query_across_tables

def test_str_query(syn, tables, sample_table):
    result = query_across_tables(
            syn,
            tables = [s["id"] for s in tables["schema"]],
            query = "select * from {}")
    reference = sample_table.reset_index(drop=True)
    pd.testing.assert_frame_equal(result[0].reset_index(drop=True), reference)

def test_str_query_exception(syn, tables):
    with pytest.raises(TypeError):
        query_across_tables(syn, [s["id"] for s in tables["schema"]],
                            query = "select * from {}", substudy="my-study")

def test_one_table(syn, tables, sample_table):
    result = query_across_tables(syn, tables = tables["schema"][0]["id"])
    reference = sample_table.reset_index(drop=True)
    pd.testing.assert_frame_equal(result[0].reset_index(drop=True), sample_table)

def test_one_table_one_query(syn, tables, sample_table):
    result = query_across_tables(syn, tables = tables["schema"][0]["id"],
                        query = ["bool_property = true"])
    reference = sample_table.query("bool_property == True").reset_index(drop=True)
    pd.testing.assert_frame_equal(result[0].reset_index(drop=True), reference)

def test_one_table_multiple_query(syn, tables, sample_table):
    result = query_across_tables(syn, tables = tables["schema"][0]["id"],
                  query = ["bool_property = true", "str_property = 'red'"])
    reference = sample_table.query("bool_property == True and "
                                   "str_property == 'red'").reset_index(drop=True)
    pd.testing.assert_frame_equal(result[0].reset_index(drop=True), reference)

def test_one_table_one_substudy(syn, tables, sample_table):
    result = query_across_tables(syn, tables = tables["schema"][0]["id"],
                                 substudy = "my-study")
    reference = sample_table[['my-study' in s for s in sample_table.substudyMemberships]]
    reference = reference.reset_index(drop=True)
    pd.testing.assert_frame_equal(result[0].reset_index(drop=True), reference)

def test_one_table_multiple_substudy(syn, tables, sample_table):
    result = query_across_tables(syn, tables = tables["schema"][0]["id"],
                        substudy = ["my-study", "other-study"])
    reference = sample_table.query(
            "'my_study' in substudyMemberships or "
            "'other-study' in substudyMemberships").reset_index(drop=True)
    reference = sample_table[
        pd.np.array(['my-study' in s for s in sample_table.substudyMemberships]) |
        pd.np.array(['other-study' in s for s in sample_table.substudyMemberships])]
    reference = reference.reset_index(drop=True)
    pd.testing.assert_frame_equal(result[0].reset_index(drop=True), reference)

def test_one_table_one_identifier(syn, tables, sample_table):
    result = query_across_tables(syn, tables = tables["schema"][0]["id"],
                        identifier = "ABC")
    reference = sample_table.query("externalId == 'ABC'").reset_index(drop=True)
    result[0].reset_index(drop=True).equals(reference)

def test_one_table_multiple_identifier(syn, tables, sample_table):
    result = query_across_tables(syn, tables = tables["schema"][0]["id"],
                        identifier = ["ABC", "FGH"])
    reference = sample_table.query("externalId == 'ABC' or "
                                   "externalId == 'FGH'").reset_index(drop=True)
    pd.testing.assert_frame_equal(result[0].reset_index(drop=True), reference)

def test_one_table_function_identifier(syn, tables, sample_table):
    result = query_across_tables(syn, tables = tables["schema"][0]["id"],
                        identifier = lambda s : s.startswith("A"))
    reference = sample_table[[s.startswith("A") for s in sample_table.externalId]]
    reference = reference.reset_index(drop=True)
    pd.testing.assert_frame_equal(result[0].reset_index(drop=True), reference)


def test_change_substudy_col(syn, tables, sample_table):
    result = query_across_tables(syn, tables = tables["schema"][0]["id"],
                        substudy = "blue", substudy_col = "str_property")
    reference = sample_table[['blue' in s for s in sample_table.str_property]]
    reference = reference.reset_index(drop=True)
    pd.testing.assert_frame_equal(result[0].reset_index(drop=True), reference)

def test_change_identifier_col(syn, tables, sample_table):
    result = query_across_tables(syn, tables = tables["schema"][0]["id"],
                        identifier = "red", identifier_col = "str_property")
    reference = sample_table.query("str_property == 'red'").reset_index(drop=True)
    pd.testing.assert_frame_equal(result[0].reset_index(drop=True), reference)

def test_two_tables(syn, tables, sample_table):
    result = query_across_tables(syn, tables = [s["id"] for s in tables["schema"]])
    reference = sample_table.reset_index(drop=True)
    assert (result[0].reset_index(drop=True).equals(reference) and
            result[1].reset_index(drop=True).equals(reference))

def test_one_everything(syn, tables, sample_table):
    result = query_across_tables(
            syn,
            tables = tables["schema"][0]["id"],
            query = ["str_property = 'blue'"],
            substudy = "other-study", identifier = "DEF")
    reference = sample_table.query("str_property == 'blue' and externalId == 'DEF'")
    reference = reference[['other-study' in s for s in reference.substudyMemberships]]
    reference = reference.reset_index(drop=True)
    pd.testing.assert_frame_equal(result[0].reset_index(drop=True), reference)

def test_multiple_everything(syn, tables, sample_table):
    result = query_across_tables(
            syn,
            tables = [s["id"] for s in tables["schema"]],
            query = ["bool_property = true", "str_property = 'red'"],
            substudy = ["my-study", "other-study"],
            identifier = ["DEF", "ABC", "FGH"])
    reference = sample_table.query("str_property == 'red' and bool_property == True")
    reference = reference[[('other-study' in s or 'my-study' in s)
                           for s in reference.substudyMemberships]]
    reference = reference[[s in ["DEF", "ABC", "FGH"]
                           for s in reference.externalId]]
    reference = reference.reset_index(drop=True)
    assert (result[0].reset_index(drop=True).equals(reference) and
            result[1].reset_index(drop=True).equals(reference))


def test_mix_everything(syn, tables, sample_table):
    result = query_across_tables(
            syn,
            tables = [s["id"] for s in tables["schema"]],
            query = ["bool_property = true"],
            substudy = "other-study",
            identifier = ["DEF", "ABC", "FGH"])
    reference = sample_table.query("bool_property == True")
    reference = reference[['other-study' in s
                           for s in reference.substudyMemberships]]
    reference = reference[[s in ["DEF", "ABC", "FGH"]
                           for s in reference.externalId]]
    reference = reference.reset_index(drop=True)
    assert (result[0].reset_index(drop=True).equals(reference) and
            result[1].reset_index(drop=True).equals(reference))
