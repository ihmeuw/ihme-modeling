################################################################################
## Description: Loads function that retrieves mi_ratio model draws from a 
##                  centralized location, then formats them for the cancer 
##                  nonfatal pipeline
## Input(s): gbd cause, location_id
## Output(s): saves formatted draws for the cause and location id in the 
##              nonfatal workspace
## How To Use: 
##       intended to be called by calculate_incidence.r, however for testing you
##          can source the script then run 
##          format_mir_draws(format_acause, location_of_interest)
## Notes: 
##  - Currently assumes that mi_ratio model was run in logit space
##  - An S4 object error on loading the HDF5 files often results when the 
##          connection is out of memory. If this happens, try running 
##          rm(list=ls()) first, or restart R
## Contributors: 
################################################################################
## load Libraries
library(here)  
if (!exists("code_repo"))  {
  code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
  if (!grepl("cancer_estimation", code_repo)) {
    code_repo <- file.path(code_repo, 'cancer_estimation')
  }
}
source(file.path(code_repo, '/r_utils/utilities.r')) 
source(get_path('nonfatal_functions', process="nonfatal_model"))

source(get_path('mir_functions', process="mir_model"))
library(dplyr)
for (pkg in c('data.table')){ 
  if (!require(pkg,character.only=TRUE)) install.packages(pkg)
  library(pkg,character.only=TRUE)
}
# set the nf rids for the follow cancers
rb_custom_rids <- get_gbd_parameter("neo_eye_rb_winsorize", 
                                    parameter_type = "nonfatal_parameters")$neo_eye_rb_winsorize
nb_custom_rids <- get_gbd_parameter("new_causes_custom_mir", 
                                    parameter_type = "nonfatal_parameters")$new_causes_custom_mir


################################################################################
## Define Functions
################################################################################
RB_MIR_hotfix <- function(location_id) { 
  ## Returns the correct filepath for MIRs depending on location 
  ## If location is in high-income super region, it will pull MIR draws from model_version_id=90.
  ## Otherwise, will pull from model_version_id=89
  
  # get list of high-income countries 
  location_metadata <- fread(paste0(get_path(process='nonfatal_model', key='database_cache'), '/location_metadata.csv'))
  HIC_locs <- unique(location_metadata[super_region_name == "High-income", location_id])
  
  if (location_id %in% HIC_locs) { 
    message('Pulling from CODEm replaced MIRs')
    input_folder <- paste0(get_path(process='mir_model', key='new_mir_tempFolder', 
                                    base_folder = "workspace"), '/90/neo_eye_rb') 
    input_file <- paste0(input_folder, '/neo_eye_rb_mir_v90_draws', location_id, '_CODEM_HIC_replacement.csv') 
  }
  else { 
    input_folder <- get_path(process='mir_model', key='mir_new_causes', 
                             base_folder = "workspace")
    input_file <- paste0(input_folder, '/89/neo_eye_rb/neo_eye_rb_mir_v89_draws',location_id,'.csv')
  }
  
  return(input_file)
}


load_data <- function(this_acause, this_location_id, cnf_model_version_id) {
  ##
  ##
  stgpr_run_id_map <- get_run_id_map(this_acause, cnf_model_version_id)
  mir_run_id <- unique(stgpr_run_id_map$run_id)
  print(paste("    loading data for run_id", mir_run_id))
  input_folder = get_path("STGPR_outputs", process="cancer_model")
  input_file <- paste0(input_folder, '/', mir_run_id, '/draws_temp_0/', 
                       this_location_id, '.csv')
  
  if ((cnf_model_version_id==17) & (this_acause %in% c('neo_bone','neo_eye','neo_eye_other','neo_eye_rb','neo_liver_hbl','neo_lymphoma_burkitt','neo_lymphoma_other','neo_neuro','neo_tissue_sarcoma'))) { 
    input_folder <- get_path(process='nonfatal_model', key='mir_draws_output')
    input_file <- paste0(input_folder, '/', this_acause, '/', this_location_id, '.csv')
  }
  else if ((cnf_model_version_id %in% nb_custom_rids) & (this_acause %in% get_gbd_parameter('NB_causes')$NB_causes)) { 
    if ((cnf_model_version_id %in% rb_custom_rids) &(this_acause == "neo_eye_rb")) {      
      print("Applying rb modeling hotfix")      
      input_file <- RB_MIR_hotfix(this_location_id)
    }
    else {
      message('pulling old mirs')
      input_folder <- get_path(process='mir_model', key='new_mir_tempFolder')
      input_file <- paste0(input_folder, '/89/',this_acause, '/', this_acause,'_mir_v89_draws',this_location_id,'.csv')
    }
  }
  input_dt <- fread(input_file)
  input_dt$age_group_id <- as.integer(as.character(input_dt$age_group_id))
  return(input_dt)
}


get_mir_model_version <- function(cnf_model_id) {
  ##
  ##
  run_table <- fread(paste0(get_path(process='nonfatal_model', key='database_cache'), '/cnf_model_version.csv')) #cdb.get_table('cnf_model_version')
  mvid <- run_table[cnf_model_version_id == cnf_model_id, ]
  return(as.integer(mvid[,mir_model_version_id]))
}


load_bestMIR_runs <- function(mir_model_id=NULL) {
  ## Creates a map linking each cause with the run information specified
  ##
  if (is.null(mir_model_id)) stop("must supply valid mir_model_version_id")
  # load best runs for the model version
  run_record <- fread(paste0(get_path(process='nonfatal_model', key='database_cache'), '/mir_model_run.csv')) #mir.download_mir_run_table()
  run_record <- run_record[mir_model_version_id == mir_model_id,]
  run_record <- run_record[is_best==1,] 
  # Add acause value
  run_record[,acause:= gsub("_mi_ratio", "", run_record[,me_name])]
  # validate before returning
  if (nrow(run_record) == 0) {
    stop(paste(
      "No runs marked best for mir_model_version_id", mir_model_id))
  } else if (nrow(run_record) != length(unique(run_record[,me_name]))){
    stop(paste("One and only one run_id from this mir_model_version_id must",
               "be marked best. This is not currently the case"))
  } else if (nrow(run_record[duplicated(run_record[,me_name]),]) > 0){
    print("Multiple run_ids marked best for the following causes")
    stop(run_record[duplicated(run_record[,acause]),])
  }
  return(run_record)
}

get_run_id_map <- function(this_acause, cnf_model_version_id) {
  ##
  ##
  mvid <- get_mir_model_version(cnf_model_version_id)
  run_id_map <- load_bestMIR_runs(mir_model_id=mvid)
  run_id_map <- subset(run_id_map, 
                       acause == this_acause, 
                       c('run_id','mir_model_version_id','my_model_id','acause'))
  return(run_id_map)
}


revertAndAdjust <- function(col, draws) {
  ## adjusts the draws in the indicated column by the upper and lower caps, then
  ##     returns the correctd data frame
  ##
  draws <- as.data.table(subset(draws,,c(col, 'upper_cap', 'lower_cap')))
  setnames(draws, col, "fixcol")
  draws[fixcol < 0, fixcol:=0] 
  draws[fixcol > 1, fixcol:=1] 
  draws[ ,fixcol:=(fixcol * upper_cap)]
  draws[fixcol < lower_cap, fixcol:=lower_cap]
  setnames(draws, "fixcol", col)
  return(draws[[col]])
}


load_mi_caps <- function(type, mir_model_version_id=NULL, 
                         add_missing_ages=FALSE) {
  ## Returns a named list containing formatted maps of upper and lower caps 
  ##      for the provided model_input numbers
  ## -- Arguments
  ##         type : one of c("upper", "lower") 
  ##         mir_model_version_id
  ##      
  ##
  if (!is.numeric(mir_model_version_id)) stop("Must sent numeric mir_model_version_id")
  if (type == "upper") {
    caps_table = "mir_upper_cap"
    required_columns <- c('upper_cap', 'age_group_id',
                          'mir_model_version_id', 'acause')
  } else if (type == "lower") {
    caps_table = "mir_lower_cap"
    required_columns <- c('lower_cap', 'age_group_id', 
                          'mir_model_version_id', 'acause')
  }
  #cap_map <- cdb.get_table(caps_table)
  cap_map <- fread(paste0(get_path(process='nonfatal_model', key='database_cache'), '/', caps_table, '.csv'))
  version_id = mir_model_version_id
  cap_map <- subset(cap_map, 
                    mir_model_version_id==version_id,
                    required_columns)
  mir.test_load_mi_caps(cap_map, required_columns)
  if (add_missing_ages) cap_map <- mir.replace_missing_ages(cap_map)
  cap_map <- as.data.frame(cap_map)
  for (col in colnames(cap_map)){
    if (col == "acause") next
    cap_map[,col] <- as.numeric(as.character(cap_map[, col]))
  }    
  mir.test_load_mi_caps(cap_map, required_columns)
  # Result
  return(as.data.table(subset(cap_map,,required_columns)))
}


add_caps <- function(full_mir_data, this_acause, cnf_model_version_id) {
  ## Attaches values at which the model is capped relative to the 
  ##      mir_model_version being run
  ## IMPORTANT NOTE: 
  ## FOR mir_model_version_id 28 ONLY, USE THE MODEL_VERSION 17 CAPS AND
  ##       APPLY THE v.17 NEO_OTHER CAPS TO NEO_OTHER_CANCER
  ##
  # Attach run information
  if (cnf_model_version_id == 17 & this_acause %in% c('neo_bone','neo_eye','neo_eye_other','neo_eye_rb','neo_liver_hbl','neo_lymphoma_burkitt','neo_lymphoma_other','neo_neuro','neo_tissue_sarcoma')) { 
    model_version <- 80
  } else { 
    run_id_map <-  get_run_id_map(this_acause, cnf_model_version_id)
    full_mir_data$run_id <- run_id_map[['run_id']]
    full_mir_data$mir_model_version_id <- run_id_map[['mir_model_version_id']]
  }
  # Attach run information and caps
  print("Adding caps...")
  model_version <- run_id_map[['mir_model_version_id']]
  # Use alternative caps where applicable (see docstring)
  if (model_version == 28) {
    caps_version <- 17
  } else { 
    caps_version <- model_version
  }
  # Load upper caps
  upper_caps <- load_mi_caps(type="upper", 
                             mir_model_version_id=caps_version,
                             add_missing_ages=TRUE) 
  upper_caps[,'mir_model_version_id'] = model_version
  # Load lower caps             
  lower_caps <- load_mi_caps(type="lower", 
                             mir_model_version_id=caps_version,
                             add_missing_ages=TRUE) 
  lower_caps[,'mir_model_version_id'] = model_version
  if (model_version == 28 & this_acause == "neo_other_cancer") {
    lower_caps[lower_caps$acause == "neo_other", 'acause'] = this_acause
  }
  # Attach lower caps, then test to validate that all values are associated with 
  #   non-null lower-cap and upper-cap values
  full_mir_data$acause <- this_acause
  merge_dt <- merge(as.data.frame(full_mir_data), upper_caps, 
                    by= c('age_group_id', 'mir_model_version_id','acause'), 
                    all.x=T)
  has_caps <- merge(merge_dt, lower_caps, 
                    by=c('age_group_id', 'acause', 'mir_model_version_id'), 
                    all.x=T)
  if (nrow(has_caps[is.null(has_caps$lower_cap) | 
                    is.null(has_caps$upper_cap),])) {
    stop("Not all estimates could be matched to caps")
  }
  return(has_caps)
}

format_mir <- function(this_acause, this_location_id, cnf_model_version_id) {
  ## Retrieves and formats mi_ratio model results for the indicated gbd cause 
  ##      and location_id, then finalizes (thus saving) the results
  print(paste("formatting mi for", this_acause, this_location_id))
  uid_cols = c('location_id', 'year_id', 'sex_id', 'age_group_id')
  output_folder <- file.path(get_path("mir_draws_output", process="nonfatal_model"), 
                             this_acause)
  output_file = paste0(output_folder,"/", this_location_id, ".csv")
  ensure_dir(output_file)
  
  # GBD2020: NB MIR modeling decided eye parent should just be neo_eye_rb and neo_eye_other MIR draws appended together
  if(this_acause == "neo_eye"){
    input_dt_eye_other <- load_data("neo_eye_other", this_location_id, cnf_model_version_id)
    input_dt_eye_rb <- load_data("neo_eye_rb", this_location_id, cnf_model_version_id)
    input_dt <- rbindlist(list(input_dt_eye_other, input_dt_eye_rb), fill = TRUE)
    input_dt$acause <- "neo_eye"
    
    # check location_id
    if(length(setdiff(input_dt$location_id %>% unique, this_location_id) > 0)){
      stop("Error: Mismatched location_id in Eye Parent when combining RB and Other")
    }
    
    # check length 
    if(nrow(input_dt[duplicated(input_dt[, list(location_id, age_group_id, acause, sex, year)])]) > 0){
      stop("Error: Duplicated rows in Eye Parent by loc, age, cause, sex, year when combining RB and Other")
    }
    
  } else{
    input_dt <- load_data(this_acause, this_location_id, cnf_model_version_id)
  }
  
  # GBD2020: MIR modeling for lymphoma other utilized STGPR and generated sex-aggregated data
  # apply sex-aggregated MIRs to both males and females
  if (this_acause == "neo_lymphoma_other" & cnf_model_version_id >= 19) { 
    tmp_copy <- copy(input_dt) 
    input_dt[, sex_id := 1]
    tmp_copy[, sex_id := 2]
    input_dt <- rbind(tmp_copy, input_dt)
  }
  
  full_dt <- mir.replace_missing_ages(input_dt)
  if (this_acause %in% get_gbd_parameter('NB_causes')$NB_causes) { # current list of datasets going through negative binomial regression
    has_caps <- copy(full_dt)
    names(has_caps)[names(has_caps) == 'year'] <- 'year_id'
    names(has_caps)[names(has_caps) == 'sex'] <- 'sex_id'
    if ((this_acause == "neo_lymphoma_burkitt") & (cnf_model_version_id==17)) { 
      has_caps <- has_caps[!duplicated(has_caps[, c('location_id','year_id','sex_id','age_group_id')]), ]
    }
  }
  else { 
    has_caps <- add_caps(full_dt, this_acause, cnf_model_version_id)
  } 
  # revert to cartesian space and adjust outputs by the caps, first ensuring
  #       that data are within the correct boundaries
  print("Calculating final mi and saving...")
  draw_cols = names(has_caps)[grepl('draw', names(has_caps))]
  mir_cols = gsub("draw", "mir", draw_cols)
  if (this_acause %ni% get_gbd_parameter('NB_causes')$NB_causes) { 
    mir_draws <- as.data.table(sapply(draw_cols, revertAndAdjust, draws = has_caps))
  }
  else { 
    mir_draws <- subset(has_caps,, draw_cols) 
  }
  # save and output as csv
  setnames(mir_draws, colnames(mir_draws), mir_cols)
  final_data <- cbind(subset(has_caps,,uid_cols), mir_draws)
  final_data$age_group_id <- as.integer(as.character(final_data$age_group_id))
  final_data <- subset(final_data, age_group_id %ni% c(21, 27, 28), )
  #
  nonfatal_model.finalize_draws(final_data, output_file, mir_cols)
  return(final_data)
}

################################################################################
## END
################################################################################