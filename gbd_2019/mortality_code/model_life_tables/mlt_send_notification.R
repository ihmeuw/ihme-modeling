rm(list=ls())

if (Sys.info()[1]=="Windows") {
    root <- "FILEPATH"
    user <- Sys.getenv("USERNAME")
} else {
    root <- "FILEPATH"
    user <- Sys.getenv("USER")
}

library(slackr)
library(argparse)

parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                  help='The version_id for this run')
parser$add_argument('--slack_username', type="character", required=TRUE,
                  help='Slack username to ping')

args <- parser$parse_args()
mlt_lt_run_id <- args$version_id
slack_username <- args$slack_username


slackrSetup(config_file = "FILEPATH") # Setup slack bot config options
text_slackr(channel=paste0("@", slack_username), paste0("Cluster jobs for MLT run ", mlt_lt_run_id, " are done"))
