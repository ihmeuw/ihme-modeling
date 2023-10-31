
# Load libraries ----------------------------------------------------------

library(data.table)
library(parallel)
library(glue)
library(argparse)
library(mortdb, lib.loc = "FILEPATH")


# Set parameters ----------------------------------------------------------

rm(list = ls())
user <- Sys.getenv("USER")

parser <- ArgumentParser()
parser$add_argument('--version_id', type = "integer", required = TRUE,
                    help = 'The version_id for this run')
parser$add_argument('--gbd_year', type = "integer", required = TRUE,
                    help = 'GBD year')
parser$add_argument('--code_dir', type="character", required = TRUE,
                    help = "Directory where ELT code is cloned")

args <- parser$parse_args()

if (interactive()) {
  args <- list(
    version_id = "VERSION",
    gbd_year = "YEAR"
  )
}

empir_run_id <- args$version_id
gbd_year <- args$gbd_year
gbd_year_previous <- get_gbd_year(get_gbd_round(gbd_year) - 1)
code_dir <- args$code_dir

# Get lineage to determine input DDM run
parent_version_dt <- get_proc_lineage("life table empirical", "data", run_id = empir_run_id)
parent_ddm_run <- parent_version_dt[parent_process_name == "ddm estimate" & exclude == 0, parent_run_id]


# Load diagnostic code ----------------------------------------------------

vetting_dir <- paste(code_dir, "/vetting", sep = "/")

source(paste(vetting_dir, "compare_elt_source_years.R", sep = "/"))

# Run diagnostics ---------------------------------------------------------

run_list <- list(
  previous_round_best = get_proc_version("life table empirical", "data", gbd_year = gbd_year_previous),
  current_round_best = get_proc_version("life table empirical", "data", gbd_year = gbd_year),
  current_round_recent = empir_run_id
)

results <- compare_elt_source_years(run_list = run_list, raw_elt_run = parent_ddm_run)

total_current_sy <- nrow(results$source_years[run == "current_round_recent"])
total_current_rsy <- nrow(results$raw_source_years)


# Post diagnostics --------------------------------------------------------

print_sy_count_tbl <- paste0(capture.output(results$source_year_count_tbl), collapse = "\n")
print_sy_change_tbl <- paste0(capture.output(results$source_year_change_tbl), collapse = "\n")


diagnostic_message <- glue("
Diagnostics for ELT data run v{empir_run_id}:

Total source-years:           {total_current_sy}
Total raw input source-years: {total_current_rsy}

Source-year category counts:
```
{print_sy_count_tbl}
```

Source-year category changes across runs:
```
{print_sy_change_tbl}
```
")

send_slack_message(
  message = diagnostic_message,
  channel = "#mortality",
  botname = "ELTbot"
)
