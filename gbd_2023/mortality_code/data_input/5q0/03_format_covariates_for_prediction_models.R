################################################################################
## Description: Formats 5q0 data and covariates for the first and second
##              stage models
################################################################################

rm(list=ls())
Sys.unsetenv("PYTHONPATH")

# Import installed libraries
library(foreign)
library(reshape)
library(plyr)
library(data.table)
library(haven)
library(devtools)
library(argparse)
library(methods)
library(mortdb)

# Get arguments
if (!interactive()) {
  parser <- ArgumentParser()
  parser$add_argument('--version_id', type="integer", required=TRUE,
                      help='The version_id for this run of 5q0')
  parser$add_argument('--gbd_round_id', type="integer", required=TRUE,
                      help='The gbd_round_id for this run of 5q0')
  parser$add_argument('--start_year', type="integer", required=TRUE,
                      help='The starting year for this run of 5q0')
  parser$add_argument('--end_year', type="integer", required=TRUE,
                      help='The ending year for this run of 5q0')
  parser$add_argument('--population_estimate_version', type="character", required=TRUE,
                      help='Population estimate version id')
  parser$add_argument('--code_dir', type="character", required=TRUE,
                      help='Directory where child-mortality code is cloned')
  args <- parser$parse_args()
  list2env(args, .GlobalEnv)
} else {

  version_id <-
  gbd_round_id <-
  start_year <-
  end_year <-
  code_dir <- "FILEPATH"

}

# Get release_id
yml_dir <- gsub("FILEPATH")
yml <- readLines(paste0("FILEPATH"))
release_id <- yml[grepl("release_id", yml)]
release_id <- as.numeric(gsub("\\D", "", release_id))
population_estimate_version <- yml[grepl("pop_est_version", yml)]
population_estimate_version <- as.numeric(gsub("\\D", "", population_estimate_version))

# Set core directories
output_dir <- paste0("FILEPATH")

# Load the GBD specific libraries
source("FILEPATH")
source("FILEPATH")
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))

# Get external_inputs
external_inputs <- get_external_input_map(process_name = "5q0 estimate", run_id = version_id,external_input_name = "")
ldi_pc <- external_inputs[external_input_name == 'ldi_pc', external_input_version]
hiv_child_cdr <- external_inputs[external_input_name == 'hiv_child_cdr', external_input_version]
maternal_edu <- external_inputs[external_input_name == 'maternal_edu', external_input_version]
covid_inf_pc <- external_inputs[external_input_name == 'covid_inf_pc', external_input_version]

external_inputs <- list(
  ldi_pc = as.numeric(ldi_pc),
  hiv_child_cdr = as.numeric(hiv_child_cdr),
  maternal_edu = as.numeric(maternal_edu),
  covid_inf_pc = as.numeric(covid_inf_pc)
)

# Get data
dc = DataContainer$new(release_id = release_id, start_year = start_year,
                       version_id = version_id,
                       population_estimate_version = population_estimate_version,
                       end_year = end_year, output_dir = output_dir,
                       external_inputs = external_inputs)
ldi_data <- dc$get('ldi')
hiv_data <- dc$get('hiv')
maternal_edu_data <- dc$get('maternal_education')
population_data <- dc$get('population')
covid_inf_data <- dc$get('covid_inf_pc')

# Create old Andhra Pradesh location
ldi_data <- make_old_ap("ldi", ldi_data, population_data)
hiv_data <- make_old_ap("hiv", hiv_data, population_data)
maternal_edu_data <- make_old_ap("maternal_education", maternal_edu_data,
                                 population_data)
covid_inf_data <- make_old_ap("covid_inf_pc", covid_inf_data,
                              population_data)

# Merge together
covariate_data <- merge(ldi_data, maternal_edu_data,
                        by = c('location_id', 'year_id'))
covariate_data <- merge(covariate_data, hiv_data,
                        by = c('location_id', 'year_id'),
                        all.x = TRUE)
covariate_data <- merge(covariate_data, covid_inf_data,
                        by = c('location_id', 'year_id'),
                        all.x = TRUE)

# covid_inf_pc ony goes back to 1980, so fill other years with 0
covariate_data[is.na(covid_inf_pc), `:=` (
  covid_inf_pc_model_version_id = external_inputs$covid_inf_pc,
  covid_inf_pc = 0
)
]
covariate_data[is.na(hiv), `:=` (
  covid_inf_pc_model_version_id = external_inputs$hiv_child_cdr,
  hiv = 0
)
]

# Check that the data is filled in
location_data <- dc$get('location')
location_ids <- location_data[level >= 3 | location_id == 44849, location_id]
year_ids <- c(start_year:end_year)
print(location_ids)
index_ids <- list(location_id = location_ids, year_id = year_ids)
covariate_data <- covariate_data[location_id %in% location_ids, ]
assertable::assert_ids(covariate_data, index_ids)
print("Checking HIV")
assertthat::assert_that((!is.null(covariate_data$hiv)))
print("Checking LDI")
assertthat::assert_that((!is.null(covariate_data$ldi)))
print("Checking maternal")
assertthat::assert_that((!is.null(covariate_data$maternal_edu)))
print("Checking COVID infections")
assertthat::assert_that((!is.null(covariate_data$covid_inf_pc)))



# Load 5q0 data from VR bias adjustment step
data_5q0 <- dc$get('raw_adjusted_5q0')

# Merge covariate data with 5q0 data
data_5q0$year <- floor(data_5q0$year) + 0.5
covariate_data$year <- covariate_data$year_id + 0.5
data <- merge(covariate_data, data_5q0, by = c("location_id", "year"),
              all.x = TRUE)

# Merge on ihme_loc_id
data <- merge(data, location_data[, c("location_id", "ihme_loc_id")],
              by = "location_id")

# Make indicator for input data
data$data <- as.numeric(!is.na(data$mort))

# Set the method
data$method[grepl("indirect", data$type)] <- "SBH"
data$method[data$type == "direct"] <- "CBH"
data$method[grepl("vr|srs|dsp", data$source)] <- "VR/SRS/DSP"
data$method[data$type == "hh" | (is.na(data$method) & grepl("census|survey", data$source))] <- "HH"
data$method[data$source == "hsrc" & data$ihme_loc_id == "ZAF"] <- "CBH"
data$method[data$source == "icsi" & data$ihme_loc_id == "ZWE"] <- "SBH"
data$method[data$source == "indirect" & data$ihme_loc_id == "DZA"] <- "SBH"
data$method[is.na(data$method) & data$data == 1] <- "HH"




############################################################
## EXCEPTIONS
############################################################

## create variable for survey series indicator for survey random effects
data$source1 <- rep(0, length(data$source))

#sbh ind vector
sbh_ind <- grepl("indirect", data$type, ignore.case = T)
data$source1[sbh_ind] <- paste(data$source[sbh_ind], data$source_year[sbh_ind], data$type[sbh_ind])

#cbh indicator vector
cbh_ind <- grepl("direct", data$type, ignore.case = T) & !grepl("indirect", data$type, ignore.case = T)
data$source1[cbh_ind] <- paste(data$source[cbh_ind], data$type[cbh_ind])

#hh indicator vector
hh_ind <- grepl("hh", data$type, ignore.case = T)
data$source1[hh_ind] <- paste(data$source[hh_ind], data$type[hh_ind])

#everything else, only source
data$source1[data$source1 == 0] <- data$source[data$source1 == 0]

#fix for in-depth DHS's so that they have the same RE as normal DHS's
data$source1[data$source1 == "dhs in direct"] <- "dhs direct"
data$source1[grepl("dhs sp", data$source1) & grepl("BGD|GHA|UZB",data$ihme_loc_id)] <- gsub("dhs sp", "dhs", data$source1[grepl("dhs sp", data$source1) & grepl("BGD|GHA|UZB",data$ihme_loc_id)])

#classify DHS completes from reports same as our DHS completes
data$source1[grepl("dhs", data$source1, ignore.case = T) & grepl("report", data$source1, ignore.case = T) & grepl("direct", data$source1, ignore.case = T) & !grepl("indirect", data$source1, ignore.case = T)] <- "dhs direct"

#classify province (and country) level DSP into before and after 2004
dsp.ind <- data$source1 %in% c("dsp", "china dsp hh")
data$source1[dsp.ind] <- paste(data$source1[dsp.ind], as.numeric(data$year[dsp.ind] > 2004), sep = "_")

# Set the graphing source
data$graphing.source[grepl("vr|vital registration", tolower(data$source))] <- "VR"
data$graphing.source[grepl("srs", tolower(data$source))] <- "SRS"
data$graphing.source[grepl("dsp", tolower(data$source))] <- "DSP"
data$graphing.source[grepl("census", tolower(data$source)) & !grepl("intra-census survey", data$source)] <- "Census"
data$graphing.source[grepl("_ipums_", data$source) & !grepl("Survey",data$source)] <- "Census"
data$graphing.source[data$source == "DHS" | data$source == "dhs" | data$source == "dhs in" | grepl("_dhs", data$source)] <- "Standard_DHS"
data$graphing.source[grepl("^dhs .*direct", data$source1) & !grepl("sp", data$source1)] <- "Standard_DHS"
data$graphing.source[tolower(data$source) %in% c("dhs itr", "dhs sp", "dhs statcompiler") | grepl("dhs sp", data$source)] <- "Other_DHS"
data$graphing.source[grepl("mics|multiple indicator cluster", tolower(data$source))] <- "MICS"
data$graphing.source[tolower(data$source) %in% c("cdc", "cdc-rhs", "cdc rhs", "rhs-cdc", "reproductive health survey") | grepl("cdc-rhs|cdc rhs", data$source)] <- "RHS"
data$graphing.source[grepl("world fertility survey|wfs|world fertitlity survey", tolower(data$source))] <- "WFS"
data$graphing.source[tolower(data$source) == "papfam" | grepl("papfam", data$source)] <- "PAPFAM"
data$graphing.source[tolower(data$source) == "papchild" | grepl("papchild", data$source)] <- "PAPCHILD"
data$graphing.source[tolower(data$source) == "lsms" | grepl("lsms", data$source)] <- "LSMS"
data$graphing.source[tolower(data$source) == "mis" | tolower(data$source) == "mis final report" | grepl("mis", data$source)] <- "MIS"
data$graphing.source[tolower(data$source) == "ais" | grepl("ais", data$source)] <- "AIS"
data$graphing.source[is.na(data$graphing.source) & data$data == 1] <- "Other"

# Combining certain types and graphing sources for survey fixed effects
data$graphing.source[data$graphing.source %in% c("Other_DHS", "PAPCHILD", "PAPFAM", "LSMS", "RHS")] <- "Other"
data$graphing.source[data$graphing.source == "CENSUS" & data$type == "CBH"] <- "Other"
data$graphing.source[data$graphing.source %in% c("AIS", "MIS")] <- "AIS_MIS"

# VR is split into biased and unbiased. Use that here for the source and source.type
data$graphing.source[data$vr == 1 & data$data == 1] <- data$category[data$vr == 1 & data$data == 1]

# Get combo source/type
data$source.type <- paste(data$graphing.source, data$method, sep="_")
data$source.type[data$data == 0] <- NA



# Format and save
keep_cols <- c("location_id", "ihme_loc_id", "year", "ldi", "maternal_edu",
               "hiv", "covid_inf_pc", "mort", "category", "corr_code_bias", "to_correct",
               "source", "source_year", "source1", "vr", "data", "ptid",
               "log10.sd.q5", "type", "method", "source.type", "graphing.source")
data <- data[order(ihme_loc_id, year), keep_cols, with = FALSE]

# Write data
dc$save(data, 'model_input')

# Format and write out HIV file for MLT
keep_cols <- c("ihme_loc_id", "year", "child_hiv_cdr")
data$child_hiv_cdr <- data$hiv
data <- data[order(ihme_loc_id, year), keep_cols, with = FALSE]
dc$save(data, 'mlt_hiv')
