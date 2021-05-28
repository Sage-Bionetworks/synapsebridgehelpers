library(bridgeclient)
library(synapser)
library(optparse)
library(glue)

read_args <- function() {
  option_list <- list(
      make_option("--inputTable",
          help="Synapse table where participant information is initially stored."),
      make_option("--outputTable",
          help="Synapse table where result of this script will be stored."),
      make_option("--app",
          help="The identifier of the app to enroll users in."),
      make_option("--bridgeEmail",
          help="The email address associated with your Bridge account."),
      make_option("--bridgePassword", help="Your Bridge password."),
      make_option("--synapseEmail",
          help="The email address associated with your Synapse account."),
      make_option("--synapsePassword", help="Your Synapse password."),
      make_option("--study",
          help="The identifier of the study to enroll users in."),
      make_option("--participantIdentifier",
          help=glue("The field in the input and output tables containing ",
                    "anonymized participant identifiers.")),
      make_option("--phone",
          help=glue("The field in the input table containing phone numbers. ",
                    "Either this parameter or --email must be provided.")),
      make_option("--email",
          help=glue("The field in the input table containing email addresses. ",
                    "Either this parameter or --phone must be provided.")),
      make_option("--supportEmail",
          help=glue("The email address to include in status messages when ",
                    "something goes wrong.")))
  parser <- OptionParser(option_list=option_list)
  args <- parse_args(parser)
  return(args)
}

validate_args <- function(args) {
  required_arguments <- c("inputTable", "outputTable", "app", "bridgeEmail",
                          "bridgePassword", "synapseEmail", "synapsePassword")
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

get_new_participants <- function(
    input_table,
    output_table) {
  #if (is.null(phone_field) && is.null(email_field) && is.null(identifier_field)) {
  #  stop("At least one field name must be provided to compare tables.")
  #}
  input_q <- synTableQuery(glue("SELECT * FROM {input_table}"))
  input_df <- dplyr::as_tibble(input_q$asDataFrame())
  output_q <- synTableQuery(glue("SELECT * FROM {output_table}"))
  output_df <- dplyr::as_tibble(output_q$asDataFrame())
  diff_df <- dplyr::anti_join(input_table, output_table)
  return(diff_df)
}

main <- function() {
  #synLogin()
  args <- read_args()
  print(args)
  validate_args(args)
  new_participants <- get_new_participants(
    input_table = args[["inputTable"]],
    output_table = args[["outputTable"]])
}

main()
