################################################################################
## Description: Loads draws from the stgpr folder, if present, or the predicted 
##                  model mean (if draws not present) then and saves a data 
##                  frame with the mean values.
## Input(s)/Output(s): see individual functions
## How To Use: intended for submission as a cluster job in mi result retrieval 
##                  (see "Run Functions" below),
##                  Can also be run alone to retrieve results for a single run_id
## Contributors: USERNAME
################################################################################
## Load libraries
library(here)
source(file.path('FILEPATH/utilities.r'))
source(get_path('mir_functions', process="mir_model"))
source(file.path(get_path('shared_r_libraries', process='common'), '/get_location_metadata.R')) 
source(file.path(get_path('central_root', process='cancer_model'), '/r_functions/utilities/utility.r'))
for (pkg in c('plyr', 'data.table', 'rhdf5', 'parallel')){
  if (!require(pkg,character.only=TRUE)) install.packages(pkg)
  library(pkg,character.only=TRUE)
}

################################################################################
## Define Functions
################################################################################
get_output_file <- function(run_id_number) {
    temp_folder <- get_path("compile_mir_tempFolder", process="mir_model")
    ensure_dir(temp_folder)
    output_file <- paste0(temp_folder, "/mean_draws_", run_id_number, ".csv")
    return(output_file)
}

get_expected_locations <- function(runID) {
    ## Retrieves a guess of the location_set_version_id used for the runID, 
    ##      then returns a list of the locations within that location_set_version
    ## Assumes that all runs of the same model input use the same location_set_version_id
    print("loading list of expected locations")
    run_map <- mir.load_run_records(run_id_list=c(runID))
    mir_model_version_id <- unique(run_map$mir_model_version_id)[1]
    my_model_id <- unique(run_map$my_model_id)[1]
    gpr_config <- mir.load_stgprConfig(mir_model_version_id)
    gpr_config <- gpr_config[gpr_config$model_index_id==my_model_id,]
    lsvid <- as.integer(max(gpr_config$location_set_id, na.rm=TRUE))
    current_gbd <- get_gbd_parameter("current_gbd_round")
    d_step <- get_gbd_parameter('current_decomp_step')
    location_meta <- as.data.frame(get_location_metadata(location_set_id=lsvid, 
                                    gbd_round_id=current_gbd), decomp_step = d_step)
    expected_locations <- unique(location_meta[location_meta$is_estimate == 1, 'location_id'])
    return(expected_locations)
}

load_output_with_mean <- function(runID) {
    ## Loads the output of the runID, calling processes to ensure that the mean estimate is present
    # Determine output location
    h5_folder <- get_path("STGPR_outputs", process="cancer_model")
    draws_folder <- paste0(h5_folder, '/', runID, '/draws_temp_0')
    draw_files <-list.files(draws_folder, pattern = "*.csv", full.names=TRUE)
    # Determine whether draws were used by determining the number of output files for expected locations
    expected_locations <- get_expected_locations(runID)
    expected_files <- paste0(draws_folder, "/", expected_locations, ".csv")
    if (!any(expected_files %in% draw_files)) {
        print("no draws. using raked file.")
        this_data <- model_load(runID, 'raked')
        this_data$has_draws <-0
    } else if (!all(expected_files %in% draw_files)) {
        print("Examples of missing files: ")
        print(head(expected_files[!(expected_files %in% draw_files)]))
        stop("ERROR: not all expected draws files were present. Canceling script.")
    } else {
        this_data <- load_draws(draw_files)
        this_data$has_draws <- 1
    }
    # Format the data and return
    this_data <- subset(this_data, , colnames(this_data) %in%
        c('location_id', 'year_id', 'sex_id', 'age_group_id', 'gpr_mean', 'has_draws'))
    this_data$run_id <- as.integer(runID)
    return(as.data.table(this_data))
}

load_draws <- function(list_of_draw_files) {
    ## applies meanDraws to load the means of the draws 
    time_estimate = length(list_of_draw_files)*.5/60
    time_estimate = round(time_estimate)
    time_alert = paste("Calculating the mean of the draws. This should take between", 
                        time_estimate, "and", time_estimate*2, "minutes...")
    print(time_alert)
    #all_draws <- do.call(rbind, lapply(list_of_draw_files, meanDraws))
    all_draws <- mclapply(list_of_draw_files, FUN = meanDraws, mc.cores = detectCores()-3)
    all_draws <- rbindlist(all_draws)
    return(all_draws)
}

meanDraws <- function(filepath_for_draws) {
    ## Load data from the draws file and calculate the mean value, dropping any "draw" columns
    d_in <- fread(filepath_for_draws, showProgress = FALSE)
    draw_cols <- colnames(d_in)[]
    d_in$gpr_mean <- rowMeans(subset(d_in, ,grepl( "draw" , colnames(d_in))))
    d_out <- subset(d_in, ,c('location_id', 'year_id', 'sex_id', 'age_group_id', 'gpr_mean'))
    return(d_out)
}

manage_mean_draws <- function(run_id_number) {
    ## Manages code to load, format, and save the means of the STGPR draws
    print(paste("formatting draws for run_id", run_id_number))
    output_data <- load_output_with_mean(run_id_number)
    fwrite(output_data, get_output_file(run_id_number) , row.names=FALSE)
    print("Done.")
}

################################################################################
## Run Functions
################################################################################
# Note: compile_mir.__is_master__ variable is defined only by the master script. 
#         It prevents this worker script from running when sourced by the master
if (!interactive() & !exists("compile_mir.__is_master__")) {
    run_id <- commandArgs(trailingOnly=TRUE)[1]
    if (!is.na(run_id)) manage_mean_draws(run_id)
}

