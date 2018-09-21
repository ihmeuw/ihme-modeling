#!/usr/local/bin/R
#########################################
## Description: loads functions used to format prepped mi model data for the epi uploader
## Input(s)/Output(s): see individual functions
## How To Use: must be sourced by the mi_prep script
#########################################
## load libraries

source(file.path(h, 'cancer_estimation/utilities.R'))  # enables generation of common filepaths
source(get_path('ihme_upload_epi_function', process="cancer_model"))

## #####
## Set Global Variables
## #####
mi_epi.uwnetID_dUSERt <<- "[your_uwnetID]" # arbitrary value

mi_epi.required_columns <<- c(
    ## list of columns reqiured by the epi uploader, in order
    "location_id",
    "year_id",
    "sex",
    "age_group_id",
    "measure",
    "field_citation_value",
    "smaller_site_unit",
    "site_memo",
    "sample_size",
    "measure_issue",
    "measure_adjustment",
    "note_modeler",
    "note_SR",
    "is_outlier",
    "bundle_id",
    "bundle_name",
    "model_input",
    "minimum_cases_allowed",
    "incident_cases",
    "deaths",
    "mi_ratio",
    "upper_cap",
    "lower_cap",
    "variance",
    "val"

)

## #####
## Pipeline Functions
## #####

mi_epi.addEpiUploaderInfo <- function(df, extractor=mi_epi.uwnetID_dUSERt){
    ## adds data for columns reqired by the IHME-shared epi uploader function ##

    age_groups <- read.csv(get_path("age_groups"), stringsAsFactors=FALSE)
    outputDf <- merge(df, age_groups[,c('age_group_id', 'age_start', 'age_end')], all.x=TRUE, all.y=FALSE)

    setnames(outputDf, old = c('data'), new = c('val') )
    outputDf[outputDf$sex_id == 1, 'sex'] = "Male"
    outputDf[outputDf$sex_id == 2, 'sex'] = "Female"
    outputDf$sample_size <- outputDf$incident_cases
    outputDf$cases <- NA
    outputDf$mean <- outputDf$val
    outputDf$extractor <- extractor
    outputDf$year_start <- outputDf$year_id
    outputDf$year_end <- outputDf$year_id
    outputDf$is_outlier <- 0
    outputDf <- outputDf[, !(names(outputDf) %in% c(
        'minimum_cases', 'sdi', 'sdi_quintile', 'test_variance',
        'lg_var', 'obs_data_variance', 'country_id'))]

    outputDf$source_type = "Registry - cancer"
    outputDf$smaller_site_unit = 1
    outputDf$sex_issue = 0
    outputDf$year_issue = 0
    outputDf$age_issue = 0
    outputDf$age_demographer = 1
    outputDf$measure = "proportion"
    outputDf$unit_type = "Person"
    outputDf$unit_value_as_published = 1
    outputDf$measure_issue = 0
    outputDf$measure_adjustment = 1
    outputDf$recall_type = "point"
    outputDf$note_SR == "cleaned mortality/incidence ratio based on prepped cancer data"

    for (c in mi_epi.required_columns) {
        if (!(c %in% colnames(outputDf))) outputDf[,c] <- ""
    }

    mi_epi.test_addEpiUploaderInfo(outputDf, df, extractor)
    return(outputDf[,c(mi_epi.required_columns)])
}

mi_epi.uploadToEpi <- function(cause="", model_input_number=NA, extractor=mi_epi.uwnetID_dUSERt) {
    ## loads relevant information and upload data for the requested cause and model number ##

    mi_epi.test_uploadToEpi(cause, model_input_number, extractor) # tests inputs

    ## loop through bundle information to upload each entity
    bundle_folder = get_path("epi_bundle_staging",process="cancer_model")
    bundle_info <- read.csv(get_path("stgpr_bundles"), stringsAsFactors=FALSE)

    ## upload to epi
    bundle_id_number <- bundle_info[bundle_info$acause == cause, 'bundle_id']
    output_file <- paste0(bundle_folder, '/', cause, '/', bundle_id_number, '/01_input_data/01_nonlit/model_input_', model_input_number, '.xlsx')
    upload_epi_data(bundle_id = bundle_id_number, filepath=output_file)
}

## #####
## Test Functions
## #####
mi_epi.test_addEpiUploaderInfo <- function(output_df, input_df, extractor_name){
    if (extractor_name == mi_epi.uwnetID_dUSERt) stop ("ERROR: valid uwnetID must be sent to addEpiUploaderInfo")
    if (nrow(output_df) != nrow(input_df)) stop("ERROR: rows lost during addEpiUploaderInfo")
}

mi_epi.test_uploadToEpi <- function(cause, model_input_number, extractor_name){
    if (cause=="" | is.na(model_input_number) | extractor_name== mi_epi.uwnetID_dUSERt){
        stop("ERROR: incorrect arguments sent to function mi_epi.uploadToEpi")
    }
}

## #####
## End
## #####