#!/usr/local/bin/R
################################################################################
## Description: Preps staged, single-source MI ratio data for modeling
## Outputs(s): Saves formatted files in the MI model storage folder
################################################################################

library(here)
if (!exists("code_repo"))  {
    code_repo <-  sub("*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
source(file.path(code_repo, 'r_utils/utilities.r')) 
library(data.table)
library(WriteXLS)
source(get_path('mir_functions', process="mir_model"))
source(get_path('ihme_upload_epi_function', process="cancer_model")) # mi_epi.[functions]
source(get_path("mir_prep_functions", process="mir_model"))



load_formatted_input <-function() {
    ## loads and formats data types input data for the speciifed mir_version
    ##
    # Import data
    input_config <- mir.get_inputConfig()
    mir_model_version = input_config$update_suffix
    print(paste0('Loading and testing MIR input created on ', mir_model_version))
    raw_input_file = paste0(get_path("mi_staging"), 
                            '/MI_ratio_model_input_', mir_model_version ,'.csv')
    raw_input <- read.csv(raw_input_file, stringsAsFactors = FALSE)
    raw_input[!is.na(raw_input$location_id),]
    ## Enforce data types
    raw_input$acause <- as.character(raw_input$acause)
    raw_input$location_id <- as.numeric(as.character(raw_input$location_id))
    raw_input$year_id <- as.numeric(as.character(raw_input$year_id))
    raw_input$sex_id <- as.numeric(as.character(raw_input$sex_id))
    raw_input$age_group_id <- as.numeric(as.character(raw_input$age_group_id))
    # Test and return
    mir_prep.test_inputData(raw_input)
    input_df <- raw_input[, c('NID', 'location_id', 'year_id', 'sex_id', 'age_group_id', 
                            'acause', 'cases', 'deaths')]
    return(input_df)
}


aggregate_and_restrict <- function(input_df){
    ## Aggregate and restrict data:
    ##  1) apply general data restrictions
    ##  2) aggregate by year, 
    ##  3) add haq and location info to enable step 3,  
    ##  4) apply value-specific restrictions (haq requirements, min_cases, etc.) 
    ##  5) aggregate by age
    ##
    # Apply general restrictions
    min_year <- get_gbd_parameter("min_year_cod")
    df <- input_df[input_df$year >= min_year, ]
    df <- df[df$acause %in% mir.get_acauseList(), ]
    # Aggregate and apply value-specific restrictions
    year_aggregated <- mir_prep.aggData(df,var_to_aggregate="year_id")
    restricted_data <- mir_prep.applyRestrictions(mi_df=year_aggregated, 
                                                restriction_type='data')
    age_aggregated <- mir_prep.aggData(restricted_data, 
                                                var_to_aggregate="age_group_id")
    return(list('age_aggregated'=age_aggregated, 'year_aggregated'=year_aggregated))
}


apply_logit_transform <- function(mir_model_version_id, 
                                    fully_aggregated_data, year_aggregated_data) {
    ## Generate and apply caps to the data
    ##
    input_config <- mir.get_inputConfig()
    print("Creating upper caps...")
    upper_cap_data <- mir_prep.applyRestrictions(mi_df=year_aggregated_data, 
                                                restriction_type = "upper_cap")
    upper_cap_map <- mir_prep.generateCaps(type = "upper", 
                                        mir_model_version_id=mir_model_version_id,
                                        input_data=upper_cap_data, 
                                        pctile = input_config$upper_cap_percentile) 
    print("Creating lower caps...")
    lower_cap_data <- mir_prep.applyRestrictions(mi_df=year_aggregated_data, 
                                                restriction_type = "lower_cap")
    lower_cap_map <- mir_prep.generateCaps(type = "lower", 
                                        mir_model_version_id=mir_model_version_id,
                                        input_data=lower_cap_data, 
                                        pctile = input_config$lower_cap_percentile)

    # apply caps to the data
    processed_data <- fully_aggregated_data
    processed_data <- merge(processed_data, upper_cap_map, all.x=TRUE)
    processed_data <- merge(processed_data, lower_cap_map, all.x=TRUE)
    unique(processed_data[is.na(processed_data$lower_cap), c('age_group_id', 'acause')])
    processed_data[is.na(processed_data$lower_cap), 'lower_cap'] <- 0
    above_upper_caps = processed_data$mi_ratio > processed_data$upper_cap
    processed_data[above_upper_caps, 'mi_ratio'] <- processed_data[above_upper_caps, 
                                                                'upper_cap']
    below_lower_caps = processed_data$mi_ratio < processed_data$lower_cap
    processed_data[below_lower_caps, 'mi_ratio'] <- processed_data[below_lower_caps,
                                                                'lower_cap']
    return(processed_data)
}


save_output <- function(prepped_data){
    # save compiled version of data
    #  then save cause-specific files for the shared st-gpr functiong
    print("Saving...")
    output_cols <- c('nid', 'location_id', 'year_id', 'sex_id', 'age_group_id', 'acause',
                    'incident_cases', 'data', 'variance', 'upper_cap', 'lower_cap',
                    'me_name', 'sample_size', 'mir_model_version_id', 'mi_ratio')
    # Determine output folder
    input_config <- mir.get_inputConfig()
    mir_model_version = input_config$mir_model_version_id
    stgpr_inputs_dir = paste0(get_path("mir_inputs", process="mir_model"), 
                            '/model_version_', mir_model_version)
    epi_output_folder = get_path("epi_bundle_staging",process="cancer_model")
    compiled_file = paste0(stgpr_inputs_dir, '/compiled_input.csv')
    ensure_dir(compiled_file)
    #
    write.csv(prepped_data, compiled_file, row.names=FALSE)
    for (cause in mir.get_acauseList()) {
        # format, subset, and save
        cause_output <- prepped_data[prepped_data$acause == cause, output_cols]
        # Save data for stgpr tool
        this_stgpr_output_file <- paste0(stgpr_inputs_dir,'/', cause, '.csv')
        ensure_dir(this_stgpr_output_file)
        write.csv(cause_output, this_stgpr_output_file, row.names=FALSE)
        me_name <- paste0(cause, "_mi_ratio")
        mir_prep.updateOutliers(cause_output, me_name)
    }
}


prep_mir <- function(mir_version_id) {
    ## Preps data to for launch in STGPR tool 
    ##
    mir_model_version_id <- mir.set_mir_version_id(mir_version_id)
    # Load Data
    input_df <- load_formatted_input()   
    ar <- aggregate_and_restrict(input_df)
    fully_aggregated_data <- ar$age_aggregated
    year_aggregated_data <-ar$year_aggregated
    # Apply logit transformation
    input_config <- mir.get_inputConfig()
    run_logit_models <- as.logical(input_config$is_logit_model)
    if (!run_logit_models) {
        processed_data <- fully_aggregated_data
    } else {
        processed_data <- apply_logit_transform(mir_model_version_id,
                                                fully_aggregated_data, 
                                                year_aggregated_data)
    } 
    # Add variance
    variance_added <- mir_prep.addVariance(processed_data, run_logit_models)
    # Add final ST-GPR requirements and test the output before saving
    prepped_data <- mir_prep.addSTGPR_requirements(variance_added, 
                                                is_logit_model=run_logit_models)
    uid_cols <- mir_prep.get_uidColumns()
    mir_prep.test_outputData(prepped_data, input_df)
    save_output(prepped_data)
    print("Model inputs are ready!")
}



if (!interactive()){
    ## Run Prep
    ##
    mir_model_version_id <- as.numeric(commandArgs(trailingOnly=TRUE)[1])
    prep_mir(mir_model_version_id)
}

