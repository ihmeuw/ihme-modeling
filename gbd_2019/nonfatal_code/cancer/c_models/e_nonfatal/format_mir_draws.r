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
##
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
for (pkg in c('data.table')){ 
  if (!require(pkg,character.only=TRUE)) install.packages(pkg)
  library(pkg,character.only=TRUE)
}

################################################################################
## Define Functions
################################################################################
load_data <- function(this_acause, this_location_id, cnf_model_version_id) {
    ##
    ##
    stgpr_run_id_map <- get_run_id_map(this_acause, cnf_model_version_id)
    mir_run_id <- unique(stgpr_run_id_map$run_id)
    #if (this_acause %in% c('neo_liver_hbl', 'neo_lymphoma_burkitt','neo_eye',
    #                    'neo_eye_rb','neo_eye_other','neo_tissue_sarcoma',
    #                    'neo_bone','neo_neuro')) { # use neo_leukemia_ll_acute 
    #    mir_run_id <- 71741  
    #}
    #if (this_acause == "neo_lymphoma_other") {
    #    mir_run_id <- 70778
   # }
    print(paste("    loading data for run_id", mir_run_id))
    input_folder = get_path("STGPR_outputs", process="cancer_model")
    input_file <- paste0(input_folder, '/', mir_run_id, '/draws_temp_0/', 
                            this_location_id, '.csv')
    input_dt <- fread(input_file)
    input_dt$age_group_id <- as.integer(as.character(input_dt$age_group_id))
    return(input_dt)
}


get_mir_model_version <- function(cnf_model_version_id) {
    ##
    ##
    run_table <- cdb.get_table('cnf_model_version')
    mvid <- run_table[run_table$cnf_model_version_id == cnf_model_version_id, 
                        'mir_model_version_id']
    return(as.integer(unique(mvid)))
}


get_run_id_map <- function(this_acause, cnf_model_version_id) {
    ##
    ##
    mvid <- get_mir_model_version(cnf_model_version_id)
    run_id_map <- mir.load_bestMIR_runs(mir_model_version_id=mvid)
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

add_caps <- function(full_mir_data, this_acause, cnf_model_version_id) {
    ## Attaches values at which the model is capped relative to the 
    ##      mir_model_version being run
    ##
    # Attach run information
    run_id_map <-  get_run_id_map(this_acause, cnf_model_version_id)
    full_mir_data$run_id <- run_id_map[['run_id']]
    full_mir_data$mir_model_version_id <- run_id_map[['mir_model_version_id']]
    # Attach run information and caps
    print("Adding caps...")
    model_version <- run_id_map[['mir_model_version_id']]
    # Use alternative caps where applicable
    if (model_version == 28) {
        caps_version <- 17
    } else caps_version <- model_version
    # Load upper caps
    upper_caps <- mir.load_mi_caps(type="upper", 
                                    mir_model_version_id=caps_version,
                                    add_missing_ages=TRUE) 
    upper_caps[,'mir_model_version_id'] = model_version
    # Load lower caps             
    lower_caps <- mir.load_mi_caps(type="lower", 
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
                        by= c('age_group_id', 'mir_model_version_id'), 
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
    input_dt <- load_data(this_acause, this_location_id, cnf_model_version_id)

    # Use data from the smallest existing age group to fill-in any missing data
    full_dt <- mir.replace_missing_ages(input_dt)
    has_caps <- add_caps(full_dt, this_acause, cnf_model_version_id)
    # revert to cartesian space and adjust outputs by the caps, first ensuring
    #       that data are within the correct boundaries
    print("Calculating final mi and saving...")
    draw_cols = names(has_caps)[grepl('draw', names(has_caps))]
    mir_cols = gsub("draw", "mir", draw_cols)
    mir_draws <- as.data.table(sapply(draw_cols, revertAndAdjust, draws = has_caps))
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
