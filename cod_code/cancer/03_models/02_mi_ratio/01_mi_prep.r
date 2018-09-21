#!/usr/local/bin/R
################################################################################
## Description: Preps staged, single-source MI ratio data for modeling,
##     then upload to epi if upload_to_epi is True
## Input(s): Prompts the user for a model_input number and uwnetID
## Outputs(s): Saves formatted files in the MI model storage folder
## How to Use: source script, then enter information when prompted
################################################################################
## Clear workspace and load libraries
rm(list=ls())

source(file.path(h, 'cancer_estimation/utilities.R'))  # enables generation of common filepaths
for (pkg in c('data.table', 'WriteXLS')){
    if (!require(pkg,character.only=TRUE)) install.packages(pkg)
    library(pkg,character.only=TRUE)
}
source(get_path('common_model_functions', process="cancer_model"))
source(get_path('mi_epi_upload_functions', process="cancer_model")) # mi_epi.[functions]
source(get_path("mi_prep_functions", process="cancer_model"))

################################################################################
## Set Paths, Load Libraries and Prediction Frame
################################################################################
## Set model input of interest from the input_config
refresh_locations = FALSE  # Set this value to TRUE if you want to refresh location or SDI information
upload_to_epi = FALSE # Set this value to TRUE if you want to upload all data to Epi at the end of the script
model_input_number <- mi_prep.set_modelInputNumber()
uwnetID <- mi_prep.set_userInfo()

## Respond to user inputs
print(paste('## Preparing data for model input', model_input_number, "for user", uwnetID))
if (upload_to_epi) print("upload_to_epi toggled on")
if (refresh_locations) mi_prep.refreshLocationInfo()

################################################################################
## Get/Set Model Specifications and Create Maps
################################################################################
## Set UID columns
mi_prep.set_uidColumns(columnList=c('NID', 'location_id', 'year', 'sex', 'acause', 'age_group_id'))

## Get model specifications. Set Input Data and M/I Value Restrictions
input_config <- mi_prep.get_inputConfig()
mi_input_version = input_config$model_input_version
run_logit_models = eval(parse(text=input_config$logit_input))
offset = input_config$offset

## Set major filepaths per the config
raw_mi_input = paste0(get_path("mi_database"), '/03_MI_ratio_model_input_', mi_input_version ,'.csv')
stgpr_output_folder = paste0(get_path("mi_model_inputs", process="cancer_model"), '/model_input_', model_input_number)
epi_output_folder = get_path("epi_bundle_staging",process="cancer_model")

cause_config <- mi_prep.get_causeConfig()
minCases <- mi_prep.generateMinimumCasesMaps()

################################################################################
## Load Data, Enforce Required Data Types, and Apply Initial Restrictions
################################################################################
print(paste0('MIR input version ', mi_input_version))
raw_input <- read.csv(raw_mi_input, stringsAsFactors = FALSE)
mi_prep.test_inputData(raw_input)
input <- raw_input[, c('NID', 'location_id', 'year', 'sex', 'age_group_id', 'acause', 'cases', 'deaths')]

## Enforce data types and apply initial restrictions
input$acause <- as.character(input$acause)
input$sex <- as.numeric(input$sex)

## Prevent undefined mi_ratios by dropping data with 0 cases
reformatted_input <- input[input$cases > 0, ]

################################################################################
## Aggregate and restrict data:
##    1) aggregate by year, 3) add sdi and location info to enable 3, 3) apply restrictions, 4) aggregate by age
################################################################################
#  use cause_config to create a map of the causes for which data should be aggregated to 5-year bins
year_aggregate_map <- subset(cause_config, cause_config$aggregate_yearBins > 1, c('acause', 'aggregate_yearBins'))
setnames(year_aggregate_map, old='aggregate_yearBins', new='bins')
years_aggregated <- mi_prep.aggregatePerCauseConfig(reformatted_input, year_aggregate_map, "year")

## Merge with location_info dataframe to drop data for location-years that cannot be modeled
location_info <- read.csv(get_path("mi_location_info", process="cancer_model"))
ready_for_restrictions <- merge(years_aggregated, location_info, by = c('location_id', 'year'), all.x=TRUE, all.y=FALSE)

## apply restrictions
restricted_data <- mi_prep.applyRestrictions(ready_for_restrictions, min_cases_map = minCases$data)

## aggregate by age
age_aggregate_map  <- cause_config[, c('acause', 'aggregate_youngestAge')]
age_aggregate_map$aggregate_youngestAge[is.na(age_aggregate_map$aggregate_youngestAge)] <- 1
setnames(age_aggregate_map, old='aggregate_youngestAge', new='bins')
age_aggregated <- mi_prep.aggregatePerCauseConfig(restricted_data, age_aggregate_map, "age_group_id")

################################################################################
## Restrict data by upper cap if running logit models
################################################################################
if (run_logit_models == TRUE) {
    ## Generate caps based on restriction-applied data
    upper_cap_data <- mi_prep.applyRestrictions(ready_for_restrictions, min_cases_map = minCases$upperCap)
    upper_cap_map <- mi_prep.generateCaps(type = "upper", storage_file=get_path("mi_ratio_upperCaps", process="cancer_model"), input_data=upper_cap_data, pctile = input_config$upper_cap_percentile, model_input=model_input_number)
    lower_cap_data <- mi_prep.applyRestrictions(ready_for_restrictions, min_cases_map = minCases$lowerCap)
    lower_cap_map <- mi_prep.generateCaps(type = "lower", storage_file=get_path("mi_ratio_lowerCaps", process="cancer_model"), input_data=lower_cap_data, pctile = input_config$lower_cap_percentile, model_input=model_input_number)

    ## apply caps to the age-aggregated data
    processed_data <- age_aggregated
    processed_data <- merge(processed_data, upper_cap_map, all.x=TRUE)
    processed_data <- merge(processed_data, lower_cap_map, all.x=TRUE)
    processed_data[processed_data$mi_ratio > processed_data$upper_cap, 'mi_ratio'] <- processed_data[processed_data$mi_ratio > processed_data$upper_cap, 'upper_cap']
    processed_data[processed_data$mi_ratio < processed_data$lower_cap, 'mi_ratio'] <- processed_data[processed_data$mi_ratio < processed_data$lower_cap, 'lower_cap']

} else {
    processed_data <- age_aggregated
}

################################################################################
## Finalize
################################################################################
## Add variance
variance_added <- mi_prep.addVariance(processed_data, run_logit_models, offset)
variance_added$model_input <- model_input_number

## Add final ST-GPR requirements and test the output before saving
prepped_data <- mi_prep.addSTGPR_requirements(variance_added)
mi_prep.test_outputData(prepped_data, raw_input)

## save compiled version of data
##  then save cause-specific files for the shared st-gpr function
write.csv(prepped_data, paste0(stgpr_output_folder, '/compiled_input.csv'), row.names=FALSE)
mi_prep.printDataAvailability(prepped_data)
for (cause in unique(prepped_data$acause)) {
    ## format, subset, and save
    variance_added$age_group_id[variance_added$age_group_id == 21] <- 30 # change '80+' age category to '80-85' to accomodate st_gpr
    output_data <- prepped_data[prepped_data$acause == cause,]

    ## Save data for stgpr tool
    this_stgpr_output_file <- paste0(stgpr_output_folder,'/', cause, '.csv')
    write.csv(output_data, this_stgpr_output_file, row.names=FALSE)
}

if (upload_to_epi) {
    ## finalize and export for Epi uploader
    mi_input <- mi_epi.addEpiUploaderInfo(prepped_data, uwnetID)
    bundle_info <- read.csv(get_path("stgpr_bundles"), stringsAsFactors=FALSE)

    for (cause in sort(unique(bundle_info$acause))) {
        print(paste("Saving and Uploading Epi Data for", cause))
        bundle_id_number <- bundle_info[bundle_info$acause == cause, 'bundle_id']
        output_data$bundle_name <- bundle_info[bundle_info$acause == cause, 'bundle_name']
        this_epi_output_folder = paste0(epi_output_folder, '/', cause, '/', bundle_id_number, '/01_input_data/01_nonlit')
        this_epi_output_file <- paste0(this_epi_output_folder, '/model_input_', model_input_number, '.xlsx')
        WriteXLS(output_data, ExcelFileName=this_epi_output_file, SheetNames = "extraction", row.names =FALSE)
        mi_epi.uploadToEpi(cause, model_input_number, uwnetID)
    }
}

## #######
## END
## #######