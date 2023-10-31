################################################################################
## Description: Compiles all MI model results for the most recent run of MIR
## Input(s): mir_model_version_id number of interest
## Output(s): compiled data frame in the mi_ratio model storage folder
## How To Use: After sourcing script...
##                Run generate_compiled_file() function to compile mean results
##                      for the most recent 'best' run_ids
##                    Toggle is_resubmission to preserve any worker outputs that
##                         may have been completed by a previous run
## Contributors: USERNAME
################################################################################
# Set global variable that will prevent the worker script from running when
#   sourced
compile_mir.__is_master__ <<- TRUE

# Load libraries
library(here)
source(file.path('FILEPATH/utilities.r'))
source(file.path('FILEPATH/cluster_tools.r'))
source(get_path('mir_functions', process="mir_model"))
source(get_path("compile_mir_worker", process="mir_model"))
library(data.table)
library(rhdf5)
library(parallel)

################################################################################
## Get and set functions
get_temp_folder <- function() {
    ##
    ##
    return(get_path("compile_mir_tempFolder", process="mir_model"))
}

get_output_files <- function(mir_model_version_id) {
    ## Loads a list of the final output files required by this process
    ##
    output_file <- get_path("compiled_mir_results", process="mir_model")
    today <- Sys.Date()
    output_file <- gsub("<mir_model_version_id>", mir_model_version_id, output_file)
    archive_file <- gsub(".csv", paste0("_", today, ".csv"), output_file)
    archive_file <- gsub(get_root("storage"), get_root("workspace"), archive_file)
    temp_folder <- get_temp_folder()
    archive_folder <- paste0(temp_folder, "/_archive")
    for (d in c(output_file, temp_folder, archive_file)) {
        ensure_dir(d)
    }
    return(c(output_file=output_file, archive_file=archive_file))
}

################################################################################
## Get and set functions
################################################################################

format_compiled_data <- function(raw_data, run_id_map, mir_model_version_id) {
    ## Formats data for use with the mortality model
    ##
    #
    print("Formatting...")
    uid_cols = c('location_id', 'year', 'sex', 'age_group_id', 'run_id')
    output_cols = c(uid_cols, 'acause', 'mi_ratio')
    # Subset data and update references
    raw_data$age_group_id <- as.numeric(as.character(raw_data$age_group_id))
    setnames(raw_data, old = c('sex_id','year_id'), new = c('sex', 'year'))
    raw_data <- subset(raw_data, ,c(uid_cols, 'gpr_mean'))
    marked_data <- merge(raw_data, run_id_map, by='run_id', all.x=TRUE)
    marked_data$acause <- gsub("_mi_ratio", "", marked_data$me_name)
    marked_data$acause <- gsub("mi_ratio_", "neo_",  marked_data$acause)
    expanded_data <- mir.replace_missing_ages(marked_data)
    ## Load and format maps for upper- and lower- caps
    upper_caps <- mir.load_mi_caps(type="upper",
                                            mir_model_version_id=mir_model_version_id,
                                            add_missing_ages=TRUE)
    lower_caps <- mir.load_mi_caps(type="lower",
                                            mir_model_version_id=mir_model_version_id,
                                            add_missing_ages=TRUE)

    print("Adjusting results. This may take several minutes...")
    adjusted_mi <- merge(expanded_data, upper_caps,
                         by= c('age_group_id','acause'),
                         all.x=TRUE)
    adjusted_mi <- merge(adjusted_mi, lower_caps,
                         by=c('age_group_id', 'acause'),
                         all.x=TRUE)
    adjusted_mi[gpr_mean < 0, gpr_mean := 0]
    adjusted_mi[gpr_mean > 1, gpr_mean := 1]
    adjusted_mi[, mi_ratio := adjusted_mi$gpr_mean * adjusted_mi$upper_cap]
    adjusted_mi[mi_ratio < lower_cap, mi_ratio := lower_cap]
    # Subset and return
    output <- subset(adjusted_mi, ,output_cols)
    return(output)
}


launch <- function(runID) {
    mean_file = get_output_file(runID) # see worker script for this function
    # Launch job if file doesn't exist
    if (!file.exists(mean_file)) {
        worker_script <- get_path("compile_mir_worker", process="mir_model")
        job_name <- paste0('mirCompile_', runID)
        launch_jobs(worker_script, 
                    memory_request=50,
                    num_threads=10, 
                    output_path=get_path(process='common', key='cancer_logs'),
                    error_path= get_path(process='common', key='cancer_logs'),
                    script_arguments=c(runID), job_header=job_name)
    }
}


loadMean <- function(runID) {
    ## Waits to load the ouptut of the mean function for the passed runID
    ##
    mean_file = get_output_file(runID) # see worker script for this function
    # Launch job if file doesn't exist
    if (!file.exists(mean_file)) {
        count = 0
        while (!file.exists(mean_file)) {
            if (count > 60) {
                print(paste0("Mean output for run_id ", runID,
                            " not found. Running function..."))
                manage_mean_draws(runID)
            }
            count = count + 1
            Sys.sleep(1)
        }
    }
    Sys.sleep(3) # wait to ensure that the file is fully saved
    mean_data <- fread(mean_file, stringsAsFactors=FALSE)
    return(mean_data)
}


compile_raw_data <- function(run_ids) {
    ## Concatenates the output files of the worker script
    ##
    print("Generating results and concatenating. This may take up to 15 minutes...")
    # Launch jobs
    mclapply(run_ids, FUN = launch, mc.cores = detectCores() - 3)
    #lapply(run_ids, launch)
    # Load means with the loadMean functon for the list of run_ids
    #raw_data <- do.call(rbind, lapply(run_ids, loadMean))
    raw_data <- mclapply(run_ids, FUN = loadMean, mc.cores = detectCores() - 3)
    raw_data <- rbindlist(raw_data)
    # Alert user if only some of the compiled data came from draws
    if (sum(raw_data$has_draws) != nrow(raw_data) ) {
        print("ALERT! Not all outputs contained draws!")
    }
    return(raw_data)
}

################################################################################
## Main Functions
################################################################################
get_compiled_data <- function(mir_model_version_id, is_resubmission) {
    ## Returns a dataframe of formatted, compiled, post ST-GPR mi_ratio results
    ##      for the runs in the mir_model_version
    ##
    run_map <- mir.load_bestMIR_runs(mir_model_version_id)
    run_map <- subset(run_map, ,
                    c('run_id', 'mir_model_version_id', 'me_name', 'my_model_id'))
    if (!is_resubmission) {
        temp_folder <- get_temp_folder()
        unlink(temp_folder, recursive=TRUE)
    } else print("NOTICE: process will use existing outputs")
    print(paste("Compiling data for model input", mir_model_version_id))
    raw_data <- compile_raw_data(run_map$run_id)
    output <- format_compiled_data(raw_data=raw_data,
                                    run_id_map = run_map,
                                    mir_model_version_id =mir_model_version_id)
    return(output)
}


save_compiled_data <- function(compiled_df, mir_model_version_id) {
    ## Saves a dataframe in the currently specified location for compiled MIR results
    ##
    # Set output files and folders
    files <- get_output_files(mir_model_version_id)
    output_file <- files['output_file']
    archive_file <- files['archive_file']
    print(output_file)
    fwrite(compiled_df, file=output_file, row.names=FALSE)
    fwrite(compiled_df, file=archive_file, row.names=FALSE)
    print(paste("Data saved to", output_file))
}


generate_compiled_file <- function(mir_model_version_id, is_resubmission){
    ## Runs the pipeline to compile data and save together in one output file
    ##
    compiled_data <- get_compiled_data(mir_model_version_id, is_resubmission)
    save_compiled_data(compiled_data, mir_model_version_id)
}

################################################################################
## Run master
################################################################################
print(paste("Alert! get_compiled_data() requires significant memory. Ensure that",
            "you have at least 30G of memory before proceeding"))
print("All functions loaded")
if (!interactive()) {
    mir_model_version_id <- as.integer(commandArgs(trailingOnly=TRUE)[1])
    is_resubmission <- as.logical(commandArgs(trailingOnly=TRUE)[2])
                    
    generate_compiled_file(mir_model_version_id, is_resubmission)
}
