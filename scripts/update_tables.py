"""
Get all healthcodes enrolled in a study by filtering any healthcode which
has a `substudyMemberships` value which overlaps with the specified --study.
Then export all records for those healthcodes not already existing in each
target table to the target tables as specified in --table-mapping.
"""
import json
import os
import argparse
import synapseclient as sc
import synapsebridgehelpers

def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--synapse-access-token")
    parser.add_argument("--study", help="The Bridge study name to filter upon.")
    parser.add_argument("--reference-table",
            help=("The table to reference for users who belong to this study. "
                  "This is usually the Health Data Summary Table."))
    parser.add_argument("--table-mapping",
            type=json.loads,
            help=("A JSON object (dict) mapping source tables to target tables."))
    args = parser.parse_args()
    return args


def get_relevant_healthcodes(syn, reference_table, study):
    relevant_healthcodes = syn.tableQuery(
            f"SELECT distinct healthCode FROM {reference_table} "
            f"where substudyMemberships like '%{study}%'").asDataFrame()
    relevant_healthcodes = list(relevant_healthcodes.healthCode)
    return relevant_healthcodes


def verify_no_new_table_versions(syn):
    """
    This is only used by the Cirrhosis_pilot study, and raises an error
    if a new table is discovered.
    """
    new_table_names_and_versions = [
            "birth-gender-v5", "Diagnosis-v4", "biaffect-keyboard-v2",
            "biaffect-appVersion-v2", "biaffect-MDQ-v2", "biaffect-KeyboardSession-v3"]
    source_tables = syn.getChildren("syn7838471", includeTypes=['table'])
    source_table_names = [t['name'] for t in source_tables]
    prohibited_tables = [table_name in source_table_names
            for table_name in new_table_names_and_versions]
    if any(prohibited_tables):
        prohibited_table_names = [
                n[0] for n in zip(new_table_names_and_versions, prohibited_tables)
                if n[1]]
        error_message = "Found an unexpected table(s): {}".format(
                    ", ".join(prohibited_table_names))
        syn.sendMessage([syn.getUserProfile()['ownerId']],
                        "New BiAffect Table Detected",
                        error_message)
        raise sc.exceptions.SynapseHTTPError(error_message)

def main():
    args = read_args()
    syn = sc.login(authToken=args.synapse_access_token)
    if args.study == "Cirrhosis_pilot":
        verify_no_new_table_versions(syn)
    relevant_healthcodes = get_relevant_healthcodes(
            syn=syn,
            reference_table=args.reference_table,
            study=args.study
    )
    synapsebridgehelpers.export_tables(
            syn = syn,
            table_mapping = args.table_mapping,
            identifier_col = "healthCode",
            identifier = relevant_healthcodes,
            copy_file_handles = True
    )


if __name__ == "__main__":
    main()

