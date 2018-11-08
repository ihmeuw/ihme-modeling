#########################################
## Description: Loads function that retrieves mi_ratio model draws from a centralized location,
##                  then formats them for the cancer nonfatal pipeline
## Input(s): gbd cause, location_id
## Output(s): saves formatted draws for the cause and location id in the nonfatal workspace
## Notes: Currently assumes that mi_ratio model was run in logit space
#########################################
## load Libraries
library(here)  
if (!exists("code_repo")) code_repo <- sub("*", 'cancer_estimation', here())
source(file.path(code_repo, '/r_utils/utilities.r')) 
source(get_path('nonfatal_functions', process="nonfatal_model"))
source(get_path('model_functions', process="cancer_model")) 
source(get_path('mir_functions', process="mir_model"))
for (pkg in c('data.table')){ #c('car', 'foreign', 'plyr', 'reshape2', 'data.table')
  if (!require(pkg,character.only=TRUE)) install.packages(pkg)
  library(pkg,character.only=TRUE)
}

##########################################
## Define Functions
##########################################
load_data <- function(this_acause, this_location_id, cnf_model_run_id) {
    ##
    ##
    run_id_map <- get_run_id_map(this_acause, cnf_model_run_id)
    mir_run_id <- unique(run_id_map$run_id)
    print(paste("    loading data for run_id", mir_run_id))
    input_folder = get_path("STGPR_outputs", process="cancer_model")
    input_file <- paste0(input_folder, '/', mir_run_id, '/draws_temp_1/', 
                            this_location_id, '.csv')
    input_dt <- fread(input_file)
    input_dt$age_group_id <- as.integer(as.character(input_dt$age_group_id))
    return(input_dt)
}


get_mir_model_version <- function(cnf_model_run_id) {
    ##
    ##
    run_table <- cdb.get_table('cnf_model_run')
    mvid <- unique(run_table[run_table$cnf_model_run_id == cnf_model_run_id, 
                        'mir_model_version_id'])
    return(mvid)
}


get_run_id_map <- function(this_acause, cnf_model_run_id) {
    ##
    ##
    mvid <- get_mir_model_version(cnf_model_run_id)
    run_id_map <- mir.load_bestMIR_runs(mir_model_version_id=mvid)
    run_id_map <- subset(run_id_map, acause == this_acause, 
                        c('run_id', 'mir_model_version_id', 'my_model_id', 'me_name'))
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


format_mir <- function(this_acause, this_location_id, cnf_model_run_id) {
    ## Retrieves and formats mi_ratio model results for the indicated gbd cause and location_id, then
    ##       finalizes (thus saving) the results
    ##
    print(paste("formatting mi for", this_acause, this_location_id))
    uid_cols = c('location_id', 'year_id', 'sex_id', 'age_group_id')
    output_folder <- file.path(get_path("mir_draws_output", process="nonfatal_model"), 
                                this_acause)
    output_file = paste0(output_folder,"/", this_location_id, ".csv")
    ensure_dir(output_file)
    input_dt <- load_data(this_acause, this_location_id, cnf_model_run_id)

    # Use data from the smallest existing age group to fill-in any missing data
    full_dt <- mir.replace_missing_ages(input_dt)

    # Attach run information
    run_id_map <-  get_run_id_map(this_acause, cnf_model_run_id)
    full_dt$run_id <- run_id_map[['run_id']]
    full_dt <- merge(full_dt, run_id_map, by='run_id', all=TRUE)

    # Attach run information and caps
    print("Adding caps...")
    upper_caps <- mir.load_mi_caps(type="upper", 
                                    mir_model_version_id=17,
                                    add_missing_ages=TRUE) 
    upper_caps[,mir_model_version_id:=model_version]                       
    lower_caps <- mir.load_mi_caps(type="lower", 
                                    mir_model_version_id=17,
                                    add_missing_ages=TRUE) 
    lower_caps[,mir_model_version_id:=model_version]  
    merge_dt <- merge(as.data.frame(full_dt), upper_caps, 
                        by= c('age_group_id', 'mir_model_version_id'), all.x=T)
    merge_dt$acause <- this_acause
    has_caps <- merge(merge_dt, lower_caps, 
                      by=c('age_group_id', 'acause', 'mir_model_version_id'), 
                      all.x=T)

    # revert to cartesian space and adjust outputs by the caps, first ensuring
    #       that data are within the correct boundaries
    print("Calculating final mi and saving...")
    draw_cols = names(has_caps)[grepl('draw', names(has_caps))]
    mir_cols = gsub("draw", "mir", draw_cols)
    mir_draws <- as.data.table(sapply(draw_cols, revertAndAdjust, draws=has_caps))
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
