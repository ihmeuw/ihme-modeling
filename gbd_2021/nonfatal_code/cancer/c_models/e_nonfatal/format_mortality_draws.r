################################################################################
## Description: Loads function that downloads mortality model draws from the
##                  central database, then formats them for the cancer nonfatal 
##                  pipeline
## Input(s): gbd cause, location_id
## Output(s): saves formatted draws for the cause and location id in the 
##              nonfatal workspace
## How To Use: 
##       intended to be called by calculate_incidence.r, however for testing you
##          can source the script and run 
##          format_mortality(acause, location_of_interest)
## Contributors: Christine Allen, James Harvey 
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
source(get_path("cdb_utils_r"))
source(file.path(get_path("shared_r_libraries"), "/get_draws.R"))
source(get_path('mir_functions', process="mir_model"))

################################################################################
## Define Functions
################################################################################
get_cause_id <- function(cancer){
  ## returns the cause_id for the requested acause
  ##
  cause_info <- fread(paste0(get_path(process='nonfatal_model', key='database_cache'), '/cod_model_entity.csv')) 
  #cdb.get_table("cod_model_entity")
  cause_id <- cause_info[acause == cancer, cause_id]
  if (!is.numeric(cause_id)) stop(paste("Error determining cause_id for", cancer))
  return(cause_id)
}

get_mortality_model_version <- function(cnf_model_id) {
  ##
  ##
  run_table <- fread(paste0(get_path(process='nonfatal_model', key='database_cache'), '/cnf_model_version.csv'))
  #cdb.get_table('cnf_model_version')
  mvid <- run_table[cnf_model_version_id == cnf_model_id, 
                    codcorrect_model_version_id]
  return(mvid)
}


determine_source <- function(faux_correct) { 
  if (faux_correct == TRUE) { 
    src <- 'fauxcorrect'
  }
  else { 
    src <- 'codcorrect'
  }
  return(src)
}


format_mortality <-function(this_acause, this_location_id, cnf_model_version_id) {
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
  age_list <- get_gbd_parameter('current_age_groups') #c(1,seq(2,20), seq(30,32), 235) 
  # Set expected sexes
  sex_id_list <- c(1,2)
  if (this_acause %in% c('neo_prostate', 'neo_testicular')) sex_id_list <- 1
  if (this_acause %in% c('neo_cervical', 'neo_ovarian', 'neo_uterine')) {
    sex_id_list <- 2
  }
  # Get codcorrect mortality draws (only use codem if codcorrect is unavailable)
  d_step <- get_gbd_parameter('current_decomp_step')
  gbd_id <- get_gbd_parameter('current_gbd_round')
  mortality_version_id = get_mortality_model_version(cnf_model_version_id)
  #src <- determine_source(faux_correct)
  df <- get_draws(gbd_id_type="cause_id", 
                  gbd_id=get_cause_id(this_acause),
                  source='codcorrect', 
                  measure_id=1,
                  version_id=mortality_version_id, 
                  location_id=this_location_id, 
                  sex_id=sex_id_list, 
                  gbd_round_id = gbd_id,
                  decomp_step = d_step)
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