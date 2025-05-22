#!/usr/local/bin/R
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


prompt_current_release_id <- function() {
    release <- get_gbd_parameter('current_release_id')
    print(paste0("The current release id in gbd_parameters is ... ", release))
}

upload_bundle <- function(df, cause, f_path) { 
    ## Replaces new bundle data for a given cause
    ##
    # print(paste0('uploading bundle for...', cause))
    b_id <- df[1,'mir_bundle_id']
    result <- upload_bundle_data(bundle_id=b_id,
                                release_id=release,
                                filepath=f_path) 
    print(sprintf('Request status: %s', result$request_status))
    print(sprintf('Request ID: %s', result$request_id))
    return()
}



format_for_bundle_upload <- function(data){
  
  ##
    # print('formatting for epi input...')
    release <- get_gbd_parameter('current_release_id')

    # convert to data table and rename columns
    df <- as.data.table(data)
    df[, val := deaths / cases]
    df <- df[!is.infinite(val),]
    df[deaths == 0 & cases == 0, val := 0]
    df[, sample_size := cases]

    #add measure
    df[,measure:='continuous']
    df[,paste0(release, "_location_year"):= paste0('new bundle data for release id ', release)]

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


load_formatted_input <-function(mor_type, process_type_id, age_type) {
    
    ##
    # assign filepaths 
    input_config <- mir.get_inputConfig()
    mir_date = input_config$date_generated
    staged_inc_vers = input_config$staged_incidence_version_id
    mir_model_version <- input_config$mir_model_version_id
    stgpr_inputs_dir = paste0(get_path("mir_inputs", process="mir_model"), 
                            '/model_version_', mir_model_version)
    stgpr_bundle_dir <- paste0(stgpr_inputs_dir, '/bundle_version')
    ensure_dir(stgpr_bundle_dir)
    stgpr_crosswalk_dir <- paste0(stgpr_inputs_dir, '/crosswalk_version')
    ensure_dir(stgpr_crosswalk_dir)

    # Load input 
    print(paste0('Loading and testing MIR input created on ', mir_date))
    if (mor_type == "CR" | mor_type == "CRVR") {
      if (age_type=="both"){
        raw_input_file_all_ages = paste0(get_path("mi_staging"), 
                                '/MI_ratio_model_input_all_ages_CR_v', staged_inc_vers, '_', mir_date ,'.csv')
        raw_input_all_ages <- read.csv(raw_input_file_all_ages, stringsAsFactors = FALSE)
        raw_input_all_ages$age_type <- "all_ages"
        raw_input_file_peds = paste0(get_path("mi_staging"), 
                                '/MI_ratio_model_input_pediatric_CR_v', staged_inc_vers, '_', mir_date ,'.csv')
        raw_input_peds <- read.csv(raw_input_file_peds, stringsAsFactors = FALSE)
        raw_input_peds$age_type <- "pediatric"
        raw_input <- rbind(raw_input_all_ages, raw_input_peds)
      }else{
        raw_input_file = paste0(get_path("mi_staging"), 
                                '/MI_ratio_model_input_', age_type, '_CR_v', staged_inc_vers, '_', mir_date ,'.csv')
        raw_input <- read.csv(raw_input_file, stringsAsFactors = FALSE) 
        raw_input$age_type <- age_type
      }
        
    } else {
      if (age_type=="both"){
        raw_input_file_all_ages = paste0(get_path("mi_staging"), 
                                         '/MI_ratio_model_input_all_ages_VR_v', staged_inc_vers, '_', mir_date ,'.csv')
        raw_input_all_ages <- read.csv(raw_input_file_all_ages, stringsAsFactors = FALSE)
        raw_input_all_ages$age_type <- "all_ages"
        raw_input_file_peds = paste0(get_path("mi_staging"), 
                                     '/MI_ratio_model_input_pediatric_VR_v', staged_inc_vers, '_', mir_date ,'.csv')
        raw_input_peds <- read.csv(raw_input_file_peds, stringsAsFactors = FALSE)
        raw_input_peds$age_type <- "pediatric"
        raw_input <- rbind(raw_input_all_ages, raw_input_peds)
      }else{
        raw_input_file = paste0(get_path("mi_staging"),
                                '/MI_ratio_model_input_', age_type, '_VR_v', staged_inc_vers, '_', mir_date ,'.csv')
        raw_input <- read.csv(raw_input_file, stringsAsFactors = FALSE)     
        raw_input$age_type <- age_type
      }

    }
    raw_input[!is.na(raw_input$location_id),]

    
    raw_input$acause <- as.character(raw_input$acause)
    raw_input$location_id <- as.numeric(as.character(raw_input$location_id))
    raw_input$year_id <- as.numeric(as.character(raw_input$year_id)) 
    raw_input$sex_id <- as.numeric(as.character(raw_input$sex_id)) 
    raw_input$age_group_id <- as.numeric(as.character(raw_input$age_group_id))

    # test input and save as bundle input
    mir_prep.test_inputData(raw_input)
    input_df <- raw_input[, c('NID', 'location_id', 'year_id', 'sex_id', 'age_group_id', 
                            'acause', 'cases', 'deaths', 'age_type')]
    cause_list <- unique(input_df$acause)
    input_df <- outlier_data(input_df, process_type_id)
    if (mor_type == "CRVR" | mor_type == "VR") {
        VR_causes <- mir_prep.get_VR_causes()
        input_df <- input_df[input_df$acause %in% VR_causes$acause,]
    }
    compiled_df <- data.frame()
    if (mor_type == "CR" | mor_type == "CRVR") {
        # format and save each cause as a bundle 
        for (cause in cause_list) {
            # print(cause)
            bundle_cause <- subset(input_df, input_df$acause == cause)
            bundle_cause <- format_for_bundle_upload(bundle_cause)
            this_stgpr_bundle_dir <- paste0(stgpr_bundle_dir, '/', cause, '.xlsx')
            if(!file.exists(this_stgpr_bundle_dir)){
              write_xlsx(list('extraction'=bundle_cause),
                  this_stgpr_bundle_dir,
                  col_names=TRUE)
            }
            compiled_df <- rbind(compiled_df, bundle_cause)
        } 
    } 
    if (mor_type == "CR" | mor_type == "CRVR") {
        input_df <- compiled_df[, c('NID', 'location_id', 'year_id', 'sex_id', 'age_group_id',
                                'acause', 'cases', 'deaths', 'is_outlier', 'seq', 'age_type')]
    }
    return(input_df)
}


aggregate_and_restrict <- function(input_df,mor_type,process_type_id, age_type){
    ## Aggregate and restrict data:
    
    
    ##  3) add haq and location info to enable step 3,  
    
    
    ##
    
    min_year <- get_gbd_parameter("min_year_cod")
    df <- input_df[input_df$year >= min_year, ]
    df <- df[df$acause %in% mir.get_acauseList(process_type_id), ]
    
    year_aggregated <- mir_prep.aggData(df,var_to_aggregate="year_id", mor_type)
    
    restricted_data <- mir_prep.applyRestrictions(mi_df=year_aggregated, 
                                                restriction_type='data',
                                                mor_type, 
                                                age_type)
    age_aggregated <- mir_prep.aggData(restricted_data,
                                                var_to_aggregate="age_group_id", mor_type)
    return(list('age_aggregated'=age_aggregated, 'year_aggregated'=year_aggregated))
}



generate_caps_from_all_data <- function(mir_model_version_id, all_year_aggregated_data, age_type){

  
  input_config <- mir.get_inputConfig()
  
  print("Adding type specific restrictions to create upper and lower cap data...")
  
  upper_cap_data = data.frame()
  lower_cap_data = data.frame()
  for(mor_type in unique(all_year_aggregated_data$mor_type)){
    mor_type_data = all_year_aggregated_data[all_year_aggregated_data$mor_type==mor_type,]
    add_u <- mir_prep.applyRestrictions(mi_df=mor_type_data,
                                               restriction_type = "upper_cap",
                                               mor_type,
                                               age_type)
    print(paste0("saving out FILEPATH", mir_model_version_id,"_", mor_type, "_", Sys.Date(),".csv")) 
    fwrite(add_u, paste0("FILEPATH", mir_model_version_id,"_", mor_type, "_", Sys.Date(),".csv"))
    upper_cap_data <- rbind(upper_cap_data, add_u)
    fwrite(upper_cap_data, paste0("FILEPATH", mir_model_version_id,"_ALL_", Sys.Date(),".csv"))
    
    add_l <- mir_prep.applyRestrictions(mi_df=mor_type_data,
                                        restriction_type = "lower_cap",
                                        mor_type, 
                                        age_type)
    print(paste0("saving out FILEPATH", mir_model_version_id,"_", mor_type, "_", Sys.Date(),".csv")) 
    fwrite(add_l, paste0("FILEPATH", mir_model_version_id,"_", mor_type, "_", Sys.Date(),".csv"))
    lower_cap_data <- rbind(lower_cap_data, add_l)
    fwrite(lower_cap_data, paste0("FILEPATH", mir_model_version_id,"_ALL_", Sys.Date(),".csv"))
    
  }
  
  print("Creating upper caps from all data...")
  upper_cap_map <- mir_prep.generateCaps(type = "upper", 
                                         mir_model_version_id=mir_model_version_id,
                                         input_data=upper_cap_data, 
                                         pctile = input_config$upper_cap_percentile) 

  print("Creating lower caps from all data...")
  lower_cap_map <- mir_prep.generateCaps(type = "lower", 
                                         mir_model_version_id=mir_model_version_id,
                                         input_data=lower_cap_data, 
                                         pctile = input_config$lower_cap_percentile)
  
  return(list(upper_cap_map = upper_cap_map, lower_cap_map = lower_cap_map))
}
  


apply_logit_transform <- function(mir_model_version_id, 
                                    fully_aggregated_data, year_aggregated_data, mor_type, caps) {
    
    ##
    upper_cap_map = caps$upper_cap_map
    lower_cap_map = caps$lower_cap_map
    
    
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



save_output <- function(prepped_data, mor_type, process_type_id){
    # save compiled version of data
    #  then save cause-specific files for the shared st-gpr functiong
    print("Saving...")
    if (mor_type == "CR" | mor_type == "CRVR") {
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
    stgpr_crosswalk_dir <- paste0(stgpr_inputs_dir, '/crosswalk_version')
    ensure_dir(stgpr_crosswalk_dir)
    if (mor_type == "CR" | mor_type == "CRVR") {
        compiled_file = paste0(stgpr_inputs_dir, '/compiled_input.csv')
    } else {
        compiled_file = paste0(stgpr_inputs_dir, '/compiled_input_VR.csv')
    }
    ensure_dir(compiled_file)

    # change age groups where age_start is at 1 (should be removed for GBD2020 step3, refresh2) 
    prepped_data$age_group_id[prepped_data$acause == 'neo_lymphoma' & prepped_data$age_group_id == 1] <- 5
    prepped_data$age_group_id[prepped_data$acause == 'neo_lymphoma_burkitt' & prepped_data$age_group_id == 1] <- 5
    prepped_data$age_group_id[prepped_data$acause == 'neo_lymphoma_other' & prepped_data$age_group_id == 1] <- 5
    prepped_data$age_group_id[prepped_data$acause == 'neo_bone' & prepped_data$age_group_id == 1] <- 5
    prepped_data$age_group_id[prepped_data$acause == 'neo_hodgkins' & prepped_data$age_group_id == 1] <- 34
    write.csv(prepped_data, compiled_file, row.names=FALSE)
    for (cause in mir.get_acauseList(process_type_id)) {
        print(paste0('saving...',cause))
        # format, subset, and save
        cause_output <- prepped_data[prepped_data$acause == cause,]
        save
        if (nrow(cause_output) == 0) { 
            next
        }
        
        # Save data for stgpr tool
        if (mor_type == "CR" | mor_type == "CRVR") {
          print(paste("CR/CRVR", cause))
            this_stgpr_output_file <- paste0(stgpr_inputs_dir,'/', cause, '.csv')
            this_crosswalk_output_file <- paste0(stgpr_crosswalk_dir, '/crosswalk_', cause, '.csv')
            crosswalk_df <- format_for_crosswalk_upload(cause_output, process_type_id)
            write.csv(crosswalk_df, this_crosswalk_output_file, row.names=FALSE)
        } else {
          print(paste("Other", cause))
            this_stgpr_output_file <- paste0(stgpr_inputs_dir,'/', cause, '_VR.csv')
        }
        ensure_dir(this_stgpr_output_file)

        write.csv(cause_output, this_stgpr_output_file, row.names=FALSE)
        me_name <- paste0(cause, "_mi_ratio")
    }
}


outlier_data <- function(df, process_type_id) {
    ## Loads uid outliers found in mir-outliers, merged mir_bundle_id 
    
    ##
    print('outliering data...')
    release <- get_gbd_parameter('current_release_id')

    
    mir_outliers <- cdb.get_table('mir_outliers')
    mir_outliers <- mir_outliers[c('is_active','mir_bundle_id','location_id',
                                'sex_id','year_id','age_group_id')]

    # subset to correct version of bundles
    bundle_info <- cdb.get_table('mir_model_entity')
    bundle_info <- subset(bundle_info, bundle_info$release_id == release & 
                          bundle_info$is_active == 1 & 
                          bundle_info$mir_process_type_id == process_type_id)
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


format_for_crosswalk_upload <- function(df, process_type_id) {
    print('formatting for crosswalk upload...')
    release <- get_gbd_parameter('current_release_id')
    df <- outlier_data(df, process_type_id)
    # convert to data table and rename columns
    df <- as.data.table(df)
    setnames(df, c('data'), c('val'))

    #add measure
    df[,measure:='proportion']
    df[,paste0(release, "_location_year"):= paste0('new crosswalk data for release id ', release)]

    #convert sex_id to sex
    df[sex_id == 1, sex:='Male']
    df[sex_id == 2, sex:='Female']
    df[sex_id == 3, sex:='Both']

    final <- copy(df)

    final[, seq:=seq(.N)]
    final[, underlying_nid:="NA"]

    return(final)
}


prep_mir <- function(mir_model_version_id, mir_process_type_id, age_type, calculate_caps = F) {
    ## Preps data to for launch in STGPR tool 
    ##
    mir_mvid <- mir.set_mir_version_id(mir_model_version_id)
    
    if(calculate_caps){
      all_year_aggregated_data = data.frame()
      for (mor_type in c("CR","CRVR","VR")) {
        print(paste0("prepping...", mor_type, " for calculating caps"))
        input_df <- load_formatted_input(mor_type, mir_process_type_id, age_type)
        if(nrow(input_df) == 0){
          print(paste0("Input MIR data for ", mor_type, " is empty! Skipping for now ..."))
          break
        }
        input_df <- as.data.frame(input_df)
        ar <- aggregate_and_restrict(input_df,mor_type, mir_process_type_id, age_type)
        year_aggregated_data <-ar$year_aggregated
        year_aggregated_data$mor_type = mor_type
        year_aggregated_data$seq = 0
        all_year_aggregated_data <- rbind(all_year_aggregated_data, year_aggregated_data)
        print(paste0("saving out FILEPATH", mor_type, "_", Sys.Date(),".csv")) 
        fwrite(all_year_aggregated_data, paste0("FILEPATH", mor_type, "_", Sys.Date(),".csv")) # added this to test min case requirements 
      }
      # Calculate caps
      caps <- generate_caps_from_all_data(mir_mvid, all_year_aggregated_data, age_type)
    } else{
      upper_cap_map <- cdb.get_table("mir_upper_cap")
      upper_cap_map <- upper_cap_map[upper_cap_map$mir_model_version_id == (mir_mvid),]
      lower_cap_map <- cdb.get_table("mir_lower_cap")
      lower_cap_map <- lower_cap_map[lower_cap_map$mir_model_version_id == (mir_mvid),]
      req_cap_cols <- c("mir_model_version_id", "age_group_id", "acause")
      caps <- list(upper_cap_map = upper_cap_map[c(req_cap_cols, "upper_cap")], 
                   lower_cap_map = lower_cap_map[c(req_cap_cols, "lower_cap")])
    }

    # Input processing 
    for (mor_type in c("CR","CRVR","VR")) { 
        print(paste0("prepping...", mor_type))
        input_df <- load_formatted_input(mor_type, mir_process_type_id, age_type)
        if(nrow(input_df) == 0){
          print(paste0("Input MIR data for ", mor_type, " is empty! Skipping for now ..."))
          break
        }
        input_df <- as.data.frame(input_df)
      
        ar <- aggregate_and_restrict(input_df,mor_type, mir_process_type_id, age_type)
      
        fully_aggregated_data <- ar$age_aggregated
        year_aggregated_data <-ar$year_aggregated
        
        
        input_config <- mir.get_inputConfig()
        run_logit_models <- as.logical(input_config$is_logit_model)
        if (!run_logit_models) {
            processed_data <- fully_aggregated_data
        } else {
            processed_data <- apply_logit_transform(mir_mvid,
                                                    fully_aggregated_data,
                                                    year_aggregated_data,
                                                    mor_type, caps)
        }
        # # Add variance
        variance_added <- mir_prep.addVariance(processed_data, run_logit_models,
                                                    is_logit_model=run_logit_models)

        # Add final ST-GPR requirements and test the output before saving
        prepped_data <- mir_prep.addSTGPR_requirements(variance_added)
        mir_prep.test_outputData(prepped_data, input_df)
        save_output(prepped_data, mor_type, mir_process_type_id)
    }
    
    print("Model inputs are ready!")
}

## Run Prep
##
if (!interactive()){
    prompt_current_release_id()
    args <- commandArgs(trailingOnly=TRUE)
    mir_model_version_id <- as.numeric(args[1])
    mir_process_type_id <- as.numeric(args[2])
    calculate_caps <- as.logical(args[3])
    prep_mir(mir_model_version_id, mir_process_type_id, calculate_caps)
} else{
}

