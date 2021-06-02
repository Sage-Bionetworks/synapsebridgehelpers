library(bridgeclient)
library(synapser)
library(optparse)
library(dplyr)
library(glue)

read_args <- function() {
  option_list <- list(
      make_option("--inputTable",
          help=glue("Required. Synapse table where participant information ",
                    "is initially stored.")),
      make_option("--outputTable",
          help=glue("Required. Synapse table where result of this script ",
                    "will be stored.")),
      make_option("--app",
          help="Required. The identifier of the app to enroll users in."),
      make_option("--bridgeEmail",
          help="Required. The email address associated with your Bridge account."),
      make_option("--bridgePassword", help="Required. Your Bridge password."),
      make_option("--synapseEmail",
          help="Required. The email address associated with your Synapse account."),
      make_option("--synapsePassword", help="Required. Your Synapse password."),
      make_option("--study",
          help="The identifier of the study to enroll users in."),
      make_option("--participantIdentifier",
          help=glue("Required. The field in the input and output tables containing ",
                    "the participant identifier. This could be the same field ",
                    "as --phone or --email.")),
      make_option("--phone",
          help=glue("The field in the input table containing phone numbers. ",
                    "Either this parameter or --email must be provided.")),
      make_option("--email",
          help=glue("The field in the input table containing email addresses. ",
                    "Either this parameter or --phone must be provided.")),
      make_option("--dataGroups",
          help="A comma-delimited string of data groups to assign."),
      make_option("--statusField",
          help=glue("Required. Defaults to `status`. The field name in the ",
                    "output table to write the status message to."),
          default="status"),
      make_option("--logField",
          help=glue("Required. Defaults to `logs`. The field name in the ",
                    "output table to log errors to."),
          default="logs"),
      make_option("--supportEmail",
          help=glue("The email address to include in the status messages when ",
                    "something goes wrong.")))
  parser <- OptionParser(
      option_list=option_list,
      prog="Rscript",
      description=glue("This script serves as a backend service for registering ",
                       "new study participants with Bridge. Its original ",
                       "intended use was to allow external study coordinators ",
                       "to enter participant data into Synapse tables via ",
                       "Synapse wiki forms. The data is added as a row to an ",
                       "input table. The input table is checked against an ",
                       "output table of already processed participants. ",
                       "Participants found in the input table but not in the ",
                       "output table will have a Bridge account created ",
                       "using the provided information. The status (success ",
                       "or failure) of the account creation and any ",
                       "relevant logs will be stored to the output table. ",
                       "Input and output tables are compared using a common ",
                       "field, specified by the --participantIdentifier flag. ",
                       "The input table must have a field for the participant ",
                       "identifier as well as a field(s) for either ",
                       "phone numbers, email addresses, or both. The output ",
                       "table must have the participant identifier field ",
                       "as well as fields for status messages and logs. ",
                       "See the full list of available options below."))
  args <- parse_args(parser)
  return(args)
}


validate_args <- function(args) {
  required_arguments <- c("inputTable", "outputTable", "app", "bridgeEmail",
                          "bridgePassword", "synapseEmail", "synapsePassword",
                          "participantIdentifier", "statusField", "logField")
  lapply(required_arguments, function(arg) {
    if (!hasName(args, arg)) {
      stop(glue("--{arg} is required."))
    }
  })
  if (!any(hasName(args, c("phone", "email")))) {
    stop("Either --phone or --email must be provided")
  }
  if (any(hasName(args, c("participantIdentifier", "study"))) &&
      !all(hasName(args, c("participantIdentifier", "study")))) {
    stop(glue("If either --study or --participantIdentifier is supplied, ",
              "then both parameters must be supplied."))
  }
}

#' Retrieve new entries in the input table
#'
#' New entries are determined by taking the difference between the input and
#' output tables. It is assumed that the columns share the same field name for
#' the participant identifier.
#'
#' @param input_table The Synapse ID of the input table.
#' @param output_table The Synapse ID of the output table.
#' @param phone The phone number of this participant.
#' @param email The email address of this participant.
#' @param participant_identifier The participant identifier.
#' @return The anti_join (see dplyr::anti_join) of the input and output tables.
get_new_participants <- function(
    input_table,
    output_table,
    participant_identifier) {
  input_q <- synTableQuery(glue("SELECT * FROM {input_table}"))
  input_df <- dplyr::as_tibble(input_q$asDataFrame())
  output_q <- synTableQuery(glue("SELECT * FROM {output_table}"))
  output_df <- dplyr::as_tibble(output_q$asDataFrame())
  diff_df <- input_df %>%
    dplyr::anti_join(output_df, by = {{ participant_identifier }}) %>%
    arrange(desc(ROW_ID)) %>%
    distinct(.data[[participant_identifier]],
             .keep_all=TRUE) # only consider most recent
  return(diff_df)
}

#' @return List with names `valid` and `number`
validate_and_format_phone <- function(phone_number) {
  phone_digits <- stringr::str_replace_all(phone_number, "\\D", "")
  if (nchar(phone_digits) < 7) {
    result <- list(valid=FALSE, number=phone_digits)
  } else {
    result <- list(valid=TRUE, number=phone_digits)
  }
  return(result)
}

#' Create a single account on Bridge for this record
#'
#' @param output_table The Synapse ID of the table to output the resulting
#' status to.
#' @param study The identifier of the study to enroll this user in.
#' @param participant_identifier The participant identifier.
#' @param phone The phone number of this participant.
#' @param email The email address of this participant.
#' @param support_email An email address to include in the status message
#' stored back to the output_table in case account creation fails.
create_participant_account <- function(
    output_table,
    study=NULL,
    participant_identifier=NULL,
    phone=NULL,
    email=NULL,
    support_email=NULL,
    data_groups=NULL) {
  if (is.null(phone) && is.null(email)) {
    stop("Either phone or email must be provided to create an account.")
  }
  phone <- validate_and_format_phone(phone)
  if (!phone[["valid"]]) {
    status <- list(success = FALSE,
                   content = glue("Account creation failed. ",
                                  "Check phone number formatting."))
    return(status)
  }
  status <- tryCatch({
    content <- bridgeclient::create_participant(
      phone_number = phone[["number"]],
      email = email,
      study = study,
      external_id = participant_identifier,
      data_groups = data_groups)
    status <- list(success = TRUE,
                   content = "Account creation successful.",
                   log = NA_character_)
    return(status)
  }, error = function(e) {
    if (!is.null(support_email)) {
      status <- list(success = FALSE,
                     content = glue("Account creation failed. Please contact ",
                                    "{support_email}"),
                     log = e$message)
    } else {
      status <- list(success = FALSE,
                     content = "Account creation failed.",
                     log = e$message)
    }
    return(status)
  })
  return(status)
}

#' Store the status of account creation back to Synapse
#'
#' @param output_table The Synapse ID of the table to output the resulting
#' status to.
#' @param participant_identifier_field The field in the output table for
#' participant identifiers.
#' @param participant_identifier The participant identifier.
#' @param status_field The field in the output table for status messages.
#' @param status_message The status message.
#' @param log_field The field in the output table for logging errors.
#' @param log_message The log message.
store_result <- function(
    output_table,
    participant_identifier_field,
    participant_identifier,
    status_field,
    status_message,
    log_field,
    log_message) {
  result <- list()
  result[[participant_identifier_field]] <- participant_identifier
  result[[status_field]] <- status_message
  result[[log_field]] <- log_message
  result_df <- dplyr::as_tibble(result)
  t <- synapser::Table(output_table, result_df)
  synStore(t)
}

main <- function() {
  args <- read_args()
  validate_args(args)
  if (hasName(args, "dataGroups")) {
    data_groups <- stringr::str_split(args[["dataGroups"]], ",")[[1]]
  } else {
    data_groups <- NULL
  }
  synLogin(email = args$synapseEmail, password = args$synapsePassword)
  bridge_login(args$app, args$bridgeEmail, args$bridgePassword)
  new_participants <- get_new_participants(
    input_table = args[["inputTable"]],
    output_table = args[["outputTable"]],
    participant_identifier = args[["participantIdentifier"]])
  purrr::pmap(new_participants, function(...) {
    record <- list(...)
    if (hasName(args, "phone")) {
      phone <- record[[args[["phone"]]]]
    } else {
      phone <- NULL
    }
    if (hasName(args, "email")) {
      email <- record[[args[["email"]]]]
    } else {
      email <- NULL
    }
    if (hasName(args, "participantIdentifier")) {
      participant_identifier <- record[[args[["participantIdentifier"]]]]
    } else {
      participant_identifier <- NULL
    }
    status <- create_participant_account(
        output_table = args[["outputTable"]],
        study = args[["study"]],
        participant_identifier = participant_identifier,
        phone = phone,
        email = email,
        support_email = args[["supportEmail"]],
        data_groups = data_groups)
    store_result(
      output_table = args[["outputTable"]],
      participant_identifier_field = args[["participantIdentifier"]],
      participant_identifier = participant_identifier,
      status_field = args[["statusField"]],
      status_message = status[["content"]],
      log_field = args[["logField"]],
      log_message = status[["log"]])
  })
}

main()

