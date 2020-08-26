
################################################################################
## Description: Preps staged, single-source MI ratio data for modeling
## Input(s): Prompts the user for a mir_model_version_id number
## Outputs(s): Saves formatted files in the MI model storage folder
## How to Use: source script, then enter information when prompted
################################################################################
## Clear workspace and load libraries
library(here)
if (!exists("code_repo"))  {
    code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
source(file.path(code_repo, 'r_utils/utilities.r')) # Loads utilities functions, eg. get_path
source(file.path(code_repo, '_database/cdb_utils.r'))
library(data.table)
library(writexl)
library(readxl)
source(get_path('mir_functions', process="mir_model"))
source(get_path("mir_prep_functions", process="mir_model"))
source(get_path("upload_bundle", process="cancer_model"))
source(get_path("get_bundle", process="cancer_model"))
source(get_path("save_bundle_version", process="cancer_model"))
source(get_path("get_bundle_version", process="cancer_model"))
source(get_path("save_crosswalk_version", process="cancer_model"))


prompt_current_decomp_step <- function() {
    d_step <- get_gbd_parameter('current_decomp_step')
    print(paste0("The current decomp step in gbd_parameters is ... ", d_step))
}

upload_bundle <- function(df, cause, f_path) { 
    ## Replaces new bundle data for a given cause
    ##
    d_step <- get_gbd_parameter('current_decomp_step')
    b_id <- df[1,'mir_bundle_id']
    result <- upload_bundle_data(bundle_id=b_id,
                                decomp_step=d_step,
                                filepath=f_path) 
    print(sprintf('Request status: %s', result$request_status))
    print(sprintf('Request ID: %s', result$request_id))
    return()
}


format_for_bundle_upload <- function(data){
  ## This script handles formatting for epi uploader by adding required columns
  ##
    print('formatting for epi input...')
    d_step <- get_gbd_parameter('current_decomp_step')

    # convert to data table and rename columns
    df <- as.data.table(data)
    df[, data := deaths / cases]
    df <- df[!is.infinite(data),]
    df[deaths == 0 & cases == 0, data := 0]
    df[, sample_size := cases]
    setnames(df, 'data', 'val')

    #add measure
    df[,measure:='continuous']
    df[,paste0(d_step, "_location_year"):= paste0('new bundle data for Decomp ', d_step)]

    # add given variance
    df[, variance:= 0.000001]

    #convert sex_id to sex
    df[sex_id == 1, sex:='Male']
    df[sex_id == 2, sex:='Female']
    df[sex_id == 3, sex:='Both']

    final <- copy(df)

    final[, seq:=seq(.N)]
    final[, underlying_nid:="NA"]
    final[, nid:=get_gbd_parameter('generic_cancer_nid')]

    return(final)
  
}


load_formatted_input <-function(mor_type) {
    ## loads and formats data types input data for the speciifed mir_version
    ##
    # assign filepaths 
    input_config <- mir.get_inputConfig()
    mir_date = input_config$date_generated
    mir_model_version <- input_config$mir_model_version_id
    stgpr_inputs_dir = paste0(get_path("mir_inputs", process="mir_model"), 
                            '/model_version_', mir_model_version)
    stgpr_bundle_dir <- paste0(stgpr_inputs_dir, '/bundle_version')
    ensure_dir(stgpr_bundle_dir)
    stgpr_crosswalk_dir <- paste0(stgpr_inputs_dir, '/crosswalk_version')
    ensure_dir(stgpr_crosswalk_dir)

    # Load input 
    print(paste0('Loading and testing MIR input created on ', mir_date))
    if (mor_type == "CR") {
        raw_input_file = paste0(get_path("mi_staging"), 
                            '/MI_ratio_model_input_', mir_date ,'.csv')
    } else {
        raw_input_file = paste0(get_path("mi_staging"),
                            '/MI_ratio_model_input_VR_', mir_date , '.csv')
    }
    raw_input <- read.csv(raw_input_file, stringsAsFactors = FALSE)
    raw_input[!is.na(raw_input$location_id),]

    ## Enforce data types
    raw_input$acause <- as.character(raw_input$acause)
    raw_input$location_id <- as.numeric(as.character(raw_input$location_id))
    raw_input$year_id <- as.numeric(as.character(raw_input$year_id)) 
    raw_input$sex_id <- as.numeric(as.character(raw_input$sex_id)) 
    raw_input$age_group_id <- as.numeric(as.character(raw_input$age_group_id))

    # test input and save as bundle input
    mir_prep.test_inputData(raw_input)
    input_df <- raw_input[, c('NID', 'location_id', 'year_id', 'sex_id', 'age_group_id', 
                            'acause', 'cases', 'deaths')]
    cause_list <- unique(input_df$acause)
    input_df <- outlier_data(input_df)
    if (mor_type == "VR") {
        new_mir_causes <- mir_prep.get_new_causes()
        input_df <- input_df[input_df$acause %in% new_mir_causes$acause,]
    }
    compiled_df <- data.frame()
    if (mor_type == "CR") {
        # format and save each cause as a bundle 
        for (cause in cause_list) {
            print(cause)
            bundle_cause <- subset(input_df, input_df$acause == cause)
            bundle_cause <- format_for_bundle_upload(bundle_cause)
            this_stgpr_bundle_dir <- paste0(stgpr_bundle_dir, '/', cause, '.xlsx')
            write_xlsx(list('extraction'=bundle_cause), 
                this_stgpr_bundle_dir,
                col_names=TRUE)
        #    upload_bundle(bundle_cause, cause, this_stgpr_bundle_dir)
            compiled_df <- rbind(compiled_df, bundle_cause)
        } 
    } 
    if (mor_type == "CR") {
        input_df <- compiled_df[, c('NID', 'location_id', 'year_id', 'sex_id', 'age_group_id',
                                'acause', 'cases', 'deaths', 'is_outlier', 'seq')]
    }
    return(input_df)
}


compile_bundle_data <- function() { 
    ## Returns a compiled dataframe, and a list of bundle version ids 
    ##
    g_id <- get_gbd_parameter('current_gbd_round')
    d_step <- get_gbd_parameter('current_decomp_step')

    # get list of bundle_ids 
    mir_df <- as.data.table(cdb.get_table(table_name='mir_model_entity'))
    mir_df <- mir_df[gbd_round_id == g_id & is_active==1,]
    bundle_list <- unique(mir_df[,mir_bundle_id])

    # load bundles and save using shared functions, preserving returned version_id
    bundle_version_list <- c() 
    comp_df <- data.table() 
    for (b_id in bundle_list) { 
        print(paste0('saving bundle version...',b_id))
        result <- save_bundle_version(bundle_id=b_id,
                                    decomp_step=d_step)
        bundle_vers_df <- get_bundle_version(result$bundle_version_id)
        
        # save list of bundle version ids  and compile MIRS
        bundle_version_list <- c(bundle_version_list, result$bundle_version_id)
        comp_df <- rbind(comp_df,bundle_vers_df, fill=TRUE)
    }
    return(list(comp_df, bundle_version_list))
}


aggregate_and_restrict <- function(input_df,mor_type){
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
    year_aggregated <- mir_prep.aggData(df,var_to_aggregate="year_id", mor_type)
    restricted_data <- mir_prep.applyRestrictions(mi_df=year_aggregated, 
                                                restriction_type='data')
    age_aggregated <- mir_prep.aggData(restricted_data, 
                                                var_to_aggregate="age_group_id", mor_type)
    return(list('age_aggregated'=age_aggregated, 'year_aggregated'=year_aggregated))
}


apply_logit_transform <- function(mir_model_version_id, 
                                    fully_aggregated_data, year_aggregated_data, mor_type) {
    ## Generate and apply caps to the data
    ##
    input_config <- mir.get_inputConfig()
    print("Creating upper caps...")
    upper_cap_data <- mir_prep.applyRestrictions(mi_df=year_aggregated_data, 
                                                restriction_type = "upper_cap")
    upper_cap_map <- mir_prep.generateCaps(type = "upper", 
                                        mir_model_version_id=mir_model_version_id,
                                        input_data=upper_cap_data, 
                                        pctile = input_config$upper_cap_percentile, mor_type) 
    print("Creating lower caps...")
    lower_cap_data <- mir_prep.applyRestrictions(mi_df=year_aggregated_data, 
                                                restriction_type = "lower_cap")
    lower_cap_map <- mir_prep.generateCaps(type = "lower", 
                                        mir_model_version_id=mir_model_version_id,
                                        input_data=lower_cap_data, 
                                        pctile = input_config$lower_cap_percentile, mor_type)

    # apply caps to the data
    processed_data <- fully_aggregated_data
    processed_data <- merge(processed_data, upper_cap_map)
    processed_data <- merge(processed_data, lower_cap_map)
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



save_output <- function(prepped_data, mor_type){
    # save compiled version of data
    #  then save cause-specific files for the shared st-gpr functiong
    print("Saving...")
    if (mor_type == "CR") {
        output_cols <- c('nid', 'location_id', 'year_id', 'sex_id', 'age_group_id', 'acause',
                        'incident_cases', 'data', 'variance', 'upper_cap', 'lower_cap',
                        'me_name', 'sample_size', 'mir_model_version_id', 'mi_ratio',
                        'mir_bundle_id', 'bundle_version_id', 'is_outlier', 'seq')
    } else {
         output_cols <- c('nid', 'location_id', 'year_id', 'sex_id', 'age_group_id', 'acause',
                        'incident_cases', 'data', 'variance', 'upper_cap', 'lower_cap',
                        'me_name', 'sample_size', 'mir_model_version_id', 'mi_ratio',
                        'mir_bundle_id', 'is_outlier')
    }
    # Determine output folder
    input_config <- mir.get_inputConfig()
    mir_model_version = input_config$mir_model_version_id
    stgpr_inputs_dir = paste0(get_path("mir_inputs", process="mir_model"), 
                            '/model_version_', mir_model_version)
    epi_output_folder = get_path("epi_bundle_staging",process="cancer_model")
    if (mor_type == "CR") {
        compiled_file = paste0(stgpr_inputs_dir, '/compiled_input.csv')
    } else {
        compiled_file = paste0(stgpr_inputs_dir, '/compiled_input_VR.csv')
    }
    ensure_dir(compiled_file)
    #
    write.csv(prepped_data, compiled_file, row.names=FALSE)
    for (cause in mir.get_acauseList()) {
        print(paste0('saving...',cause))
        # format, subset, and save
        cause_output <- prepped_data[prepped_data$acause == cause,]
        if (nrow(cause_output) == 0) { 
            next
        }
        # Save data for stgpr tool
        if (mor_type == "CR") {
            this_stgpr_output_file <- paste0(stgpr_inputs_dir,'/', cause, '.csv')
        } else {
            this_stgpr_output_file <- paste0(stgpr_inputs_dir,'/', cause, '_VR.csv')
        }
        ensure_dir(this_stgpr_output_file)
        write.csv(cause_output, this_stgpr_output_file, row.names=FALSE)
        me_name <- paste0(cause, "_mi_ratio")
        #old_causes <- c('neo_bladder', 'neo_brain', 'neo_breast', 'cervical', 'neo_colorectal', 'neo_esophageal',
        #                'neo_gallbladder', 'neo_hodgkins', 'kidney', 'neo_larynx', 'neo_leukemia', 'neo_leukemia_ll_acute',
        #                'neo_leukemia_ll_chronic', 'neo_leukemia_ml_acute', 'neo_leukemia_ml_chronic', 'neo_leukemia_other',
        #                'neo_liver', 'neo_lymphoma', 'neo_lung', 'neo_melanoma', 'neo_meso', 'neo_mouth', 'neo_myeloma', 
        #                'neo_nasopharynx', 'neo_other_cancer', 'neo_otherpharynx', 'neo_ovarian', 'neo_pancreas', 
        #                'neo_prostate', 'neo_stomach', 'neo_testicular','neo_thyroid', 'neo_uterine')
        #if (cause %in% old_causes) {
        #    mir_prep.updateOutliers(cause_output, me_name)
        #}
    }
}


save_bundles <- function(prepped_data){
    # save compiled version of data
    #  then save cause-specific files for the shared st-gpr functiong
    print("Saving...")
    output_cols <- c('nid', 'location_id', 'year_id', 'sex_id', 'age_group_id', 'acause',
                    'incident_cases', 'data', 'variance', 'upper_cap', 'lower_cap',
                    'me_name', 'sample_size', 'mir_model_version_id', 'mi_ratio',
                    'mir_bundle_id', 'bundle_version_id', 'is_outlier', 'seq')
    # Determine output folder
    input_config <- mir.get_inputConfig()
    mir_model_version = input_config$mir_model_version_id
    stgpr_inputs_dir = paste0(get_path("mir_inputs", process="mir_model"), 
                            '/model_version_', mir_model_version)
    stgpr_bundle_dir <- paste0(stgpr_inputs_dir, '/bundle_version')
    ensure_dir(stgpr_bundle_dir)
    compiled_file = paste0(stgpr_inputs_dir, '/compiled_input.csv')
    d_step <- get_gbd_parameter('current_decomp_step')
    
    write.csv(prepped_data, compiled_file, row.names=FALSE)
    for (cause in mir.get_acauseList()) {
        print(paste0('saving...',cause))
        # format, subset, and save
        bundle_input <- prepped_data[prepped_data$acause == cause,]
        if (nrow(bundle_input) == 0) { 
            next
        }
        # Save data for stgpr tool
        this_stgpr_input_file_xlsx <- paste0(stgpr_bundle_dir, '/', cause, '.xlsx')
        bundle_input <- format_for_epi_uploader(bundle_input)
        write_xlsx(list('extraction'=bundle_input), 
                this_stgpr_input_file_xlsx,
                col_names=TRUE)
        upload_bundle(bundle_input, cause, this_stgpr_input_file_xlsx)
    }
}  


outlier_data <- function(df) { 
    ## Loads uid outliers found in mir-outliers, merged mir_bundle_id 
    ## and drops any uids that merge with main dataframe 
    ##
    print('outliering data...')
    gbd_id <- get_gbd_parameter('current_gbd_round')

    # retrieve outlier uids and mir_model_entity 
    mir_outliers <- cdb.get_table('mir_outliers')
    mir_outliers <- mir_outliers[c('is_active','mir_bundle_id','location_id',
                                'sex_id','year_id','age_group_id')]

    # subset to correct version of bundles
    bundle_info <- cdb.get_table('mir_model_entity')
    bundle_info <- subset(bundle_info, bundle_info$gbd_round_id == gbd_id & bundle_info$is_active == 1)
    bundle_info <- bundle_info[c('mir_bundle_id','acause')]

    # merge bundle info to main dataframe 
    df_bundle <- merge(df, bundle_info, by=c('acause'), all.x=TRUE)
    acause_mir_outliers <- merge(mir_outliers, bundle_info, by=c('mir_bundle_id'))
    df_outlier <- merge(df_bundle, acause_mir_outliers,
                        by=c('location_id','sex_id','year_id','age_group_id','mir_bundle_id','acause'), 
                        all.x=TRUE)

    # remove NA values from un-merged entries 
    df_outlier$is_active[is.na(df_outlier$is_active)] <- 0 

    # drop outliers
    df_final <- as.data.table(df_outlier) 
    df_final <- df_final[,'is_outlier':=df_final[,'is_active']]
    return(df_final)
}


format_for_crosswalk_upload <- function(df) {
    print('formatting for crosswalk upload...')
    d_step <- get_gbd_parameter('current_decomp_step')
    df <- outlier_data(df)
    # convert to data table and rename columns
    df <- as.data.table(data)
    setnames(df, c('data', 'seq'), c('val', 'crosswalk_parent_seq'))

    #add measure
    df[,measure:='proportion']
    df[,paste0(d_step, "_location_year"):= paste0('new crosswalk data for Decomp ', d_step)]

    #convert sex_id to sex
    df[sex_id == 1, sex:='Male']
    df[sex_id == 2, sex:='Female']
    df[sex_id == 3, sex:='Both']

    final <- copy(df)

    final[, seq:=seq(.N)]
    final[, underlying_nid:="NA"]

    return(final)
}


prep_mir <- function(mir_version_id) {
    ## Preps data to for launch in STGPR tool 
    ##
    mir_model_version_id <- mir.set_mir_version_id(mir_version_id)
    # Load Data, upload bundle data 
    for (mor_type in c("VR")) {
        print(paste0("prepping...", mor_type))
        input_df <- load_formatted_input(mor_type)  

        #if (mor_type == "CR") {
        #    # save bundle versions and compile bundles to be used as input
        #    df_bundle_list <- compile_bundle_data()
        #    input_df <- df_bundle_list[[1]]
        #    bundle_version_list <- df_bundle_list[[2]]
        #    print(bundle_version_list)
        #}
        input_df <- as.data.frame(input_df)
        ar <- aggregate_and_restrict(input_df,mor_type)
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
                                                    year_aggregated_data,
                                                    mor_type)
        } 
        # Add variance
        variance_added <- mir_prep.addVariance(processed_data, run_logit_models,
                                                    is_logit_model=run_logit_models)

        # Add final ST-GPR requirements and test the output before saving
        prepped_data <- mir_prep.addSTGPR_requirements(variance_added)
        mir_prep.test_outputData(prepped_data, input_df)
        save_output(prepped_data, mor_type)
    }
    print("Model inputs are ready!")
}

## Run Prep
##
if (!interactive()){
    prompt_current_decomp_step()
    mir_model_version_id <- as.numeric(commandArgs(trailingOnly=TRUE)[1])
    prep_mir(mir_model_version_id)
}
