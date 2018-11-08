#########################################
## Description: Loads function that downloads mortality model draws from a centralized location,
##                  then formats them for the cancer nonfatal pipeline
## Input(s): gbd cause, location_id
## Output(s): saves formatted draws for the cause and location id in the nonfatal workspace
#########################################
## load Libraries
library(here)  
if (!exists("code_repo")) code_repo <- sub("*", 'cancer_estimation', here())
source(file.path(code_repo, '/r_utils/utilities.r'))
source(get_path('nonfatal_functions', process="nonfatal_model"))
source(get_path("cdb_utils_r"))
source(file.path(get_path("shared_r_libraries"), "/get_draws.R"))
source(get_path('mir_functions', process="mir_model"))

##########################################
## Define Functions
##########################################
get_cause_id <- function(acause){
    ## returns the cause_id for the requested acause
    ##
    cause_info <- cdb.get_table("cod_model_entity")
    cause_id <- unique(cause_info[cause_info$acause == acause, "cause_id"])[1]
    if (!is.numeric(cause_id)) Sys.stop(paste("Error determining cause_id for", acause))
    return(cause_id)
}

get_mortality_model_version <- function(cnf_model_run_id) {
    ##
    ##
    run_table <- cdb.get_table('cnf_model_run')
    mvid <- run_table[run_table$cnf_model_run_id == cnf_model_run_id, 
                'codcorrect_model_version_id']
    return(mvid)
}


format_mortality <-function(this_acause, this_location_id, cnf_model_run_id) {
    ## Retrieves and formats mortality draws for the specified gbd cause and 
    ##      location_id, then finalizes (thus saving) the results
    ##
    print(paste("loading mortality for", this_acause, this_location_id))
    output_folder <- paste0(get_path("mortality_draws_output", 
                            process="nonfatal_model"), "/", this_acause)
    ensure_dir(output_folder)
    output_file <- paste0(output_folder, "/", this_location_id, ".csv")
    asr_output <- paste0(output_folder, "/", this_location_id, "_asr.csv")
    # Attempt to get data for most granular ages
    #    (some causes will not have values for the youngest age groups)
    age_list <- c(seq(2,20), seq(30,32), 235) 
    # Set expected sexes
    sex_id_list <- c(1,2)
    if (this_acause %in% c('neo_prostate', 'neo_testicular')) sex_id_list <- 1
    if (this_acause %in% c('neo_cervical', 'neo_ovarian', 'neo_uterine')) {
        sex_id_list <- 2
    }
    # Get codcorrect mortality draws (only use codem if codcorrect is unavailable)
    df <- get_draws(gbd_id_type="cause_id", gbd_id=get_cause_id(this_acause),
                    source="codcorrect", measure_id=1, 
                    location_id=this_location_id, sex_id=sex_id_list, 
                    age_group_id = age_list,
                    version_id=get_mortality_model_version(cnf_model_run_id))
    if (length(df) == 0) stop("Error loading data with get_draws")
    draw_cols <- colnames(df)[grepl('draw', colnames(df))]
    mort_cols <- gsub("draw", "deaths", draw_cols)
    final_df <- subset(df, , c('cause_id', 'location_id', 'year_id', 'sex_id', 
                            'age_group_id', draw_cols))
    setnames(final_df, draw_cols, mort_cols)
    nonfatal_model.finalize_draws(final_df, output_file, mort_cols)
    return(final_df)
}


################################################################################
## END
################################################################################
