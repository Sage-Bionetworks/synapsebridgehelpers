"""
Get all healthcodes enrolled in a study by filtering any healthcode which
has a `substudyMemberships` value which overlaps with the specified --study.
Then export all records for those healthcodes not already existing in each
target table to the target tables as specified in --table-mapping.
"""

import json
import os
import argparse
import logging
import synapseclient as sc
import synapsebridgehelpers


def configure_logging(level=logging.INFO):
    """Force logging configuration so module logs are visible in all environments."""
    if isinstance(level, str):
        level = getattr(logging, level.upper())
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    logging.getLogger("synapsebridgehelpers").setLevel(level)


def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--synapse-access-token")
    parser.add_argument(
        "--scheduled-job",
        help=(
            "Set to 'enabled' to use Scheduled Job secrets for auth. "
            "Reads SCHEDULED_JOB_SECRETS JSON from environment and uses key "
            "SYNAPSE_ACCESS_TOKEN."
        ),
    )
    parser.add_argument("--study", help="The Bridge study name to filter upon.")
    parser.add_argument(
        "--reference-table",
        help=(
            "The table to reference for users who belong to this study. "
            "This is usually the Health Data Summary Table."
        ),
    )
    parser.add_argument(
        "--table-mapping",
        help=(
            "Table mapping definition. Accepts JSON dict/list/string or a "
            "comma-separated list of source table IDs."
        ),
    )
    parser.add_argument(
        "--target-project",
        help=(
            "Target project Synapse ID for exporting source table(s) "
            "to newly created table(s). Only considered when --table-mapping "
            "is a string or list."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging level for script and synapsebridgehelpers logs.",
    )
    args = parser.parse_args()
    return args


def get_synapse_access_token(args):
    if args.scheduled_job is not None and args.scheduled_job != "enabled":
        raise ValueError("--scheduled-job must be set to 'enabled' when provided.")

    if args.scheduled_job == "enabled":
        secrets_blob = os.getenv("SCHEDULED_JOB_SECRETS")
        if not secrets_blob:
            raise ValueError(
                "--scheduled-job was set but SCHEDULED_JOB_SECRETS is not set."
            )
        try:
            secrets = json.loads(secrets_blob)
        except json.JSONDecodeError as e:
            raise ValueError("SCHEDULED_JOB_SECRETS is not valid JSON.") from e
        token = secrets.get("SYNAPSE_ACCESS_TOKEN")
        if not token:
            raise ValueError("SCHEDULED_JOB_SECRETS is missing SYNAPSE_ACCESS_TOKEN.")
        return token

    if not args.synapse_access_token:
        raise ValueError(
            "--synapse-access-token must be set unless --scheduled-job is used."
        )
    return args.synapse_access_token


def log_args(args):
    logger = logging.getLogger(__name__)
    args_to_log = vars(args).copy()
    args_to_log.pop("synapse_access_token", None)
    logger.info("Parsed arguments: %s", args_to_log)


def parse_table_mapping(table_mapping):
    logger = logging.getLogger(__name__)
    if table_mapping is None:
        raise TypeError("--table-mapping must be set.")
    try:
        parsed_table_mapping = json.loads(table_mapping)
    except json.JSONDecodeError:
        parsed_table_mapping = None
    if isinstance(parsed_table_mapping, (dict, list, str)):
        logger.info("Parsed table mapping: %s", parsed_table_mapping)
        return parsed_table_mapping
    # If JSON parses to a scalar (e.g. number/bool/null), treat input as raw text
    # so plain, non-JSON usage remains supported.
    if parsed_table_mapping is not None:
        parsed_table_mapping = None

    split_mapping = [table_id.strip() for table_id in table_mapping.split(",")]
    split_mapping = [table_id for table_id in split_mapping if table_id]
    if len(split_mapping) > 1:
        logger.info("Parsed table mapping: %s", split_mapping)
        return split_mapping
    logger.info("Parsed table mapping: %s", table_mapping)
    return table_mapping


def get_relevant_healthcodes(syn, reference_table, study):
    relevant_healthcodes = syn.tableQuery(
        f"SELECT distinct healthCode FROM {reference_table} "
        f"where substudyMemberships like '%{study}%'"
    ).asDataFrame()
    relevant_healthcodes = list(relevant_healthcodes.healthCode)
    return relevant_healthcodes


def verify_no_new_table_versions(syn):
    """
    This is only used by the Cirrhosis_pilot study, and raises an error
    if a new table is discovered.
    """
    new_table_names_and_versions = [
        "birth-gender-v5",
        "Diagnosis-v4",
        "biaffect-keyboard-v2",
        "biaffect-appVersion-v2",
        "biaffect-MDQ-v2",
        "biaffect-KeyboardSession-v3",
    ]
    source_tables = syn.getChildren("syn7838471", includeTypes=["table"])
    source_table_names = [t["name"] for t in source_tables]
    prohibited_tables = [
        table_name in source_table_names for table_name in new_table_names_and_versions
    ]
    if any(prohibited_tables):
        prohibited_table_names = [
            n[0] for n in zip(new_table_names_and_versions, prohibited_tables) if n[1]
        ]
        error_message = "Found an unexpected table(s): {}".format(
            ", ".join(prohibited_table_names)
        )
        syn.sendMessage(
            [syn.getUserProfile()["ownerId"]],
            "New BiAffect Table Detected",
            error_message,
        )
        raise sc.exceptions.SynapseHTTPError(error_message)


def main():
    args = read_args()
    configure_logging(args.log_level)
    log_args(args)
    synapse_access_token = get_synapse_access_token(args)
    syn = sc.login(authToken=synapse_access_token)
    table_mapping = parse_table_mapping(args.table_mapping)
    if args.target_project and isinstance(table_mapping, dict):
        table_mapping = list(table_mapping.keys())
    if args.study == "Cirrhosis_pilot":
        verify_no_new_table_versions(syn)
    relevant_healthcodes = get_relevant_healthcodes(
        syn=syn, reference_table=args.reference_table, study=args.study
    )
    synapsebridgehelpers.export_tables(
        syn=syn,
        table_mapping=table_mapping,
        target_project=args.target_project,
        identifier_col="healthCode",
        identifier=relevant_healthcodes,
    )


if __name__ == "__main__":
    main()
