##
##

rm(list=ls())


library(here)
library(data.table)
repo_name <- "cancer_estimation"
code_repo <- here()
if (!grepl(repo_name, code_repo)) {
  code_repo <- paste0(code_repo, "/", repo_name)
}

# import libraries
source(file.path(code_repo, 'r_utils/utilities.r'))
source(file.path(code_repo, 'r_utils/cluster_tools.r'))
source(file.path(code_repo, '_database/cdb_utils.r'))
source(paste0(get_path('common', key='shared_r_libraries'), 'get_location_metadata.R'))

# set up common directories
worker_script <- get_path(process='cod_mortality', key='noise_reduction_prior')
nr_scratch_space <- get_path(process='cod_mortality', key='noise_reduction_workspace')
work_dir = get_path(process='cod_mortality', key='noise_reduction_workspace')
unlink(paste0(work_dir,'/*'), recursive=FALSE) # this line deletes all previous outputs


compile_nr_priors <- function() {
    file_list <- list.files(work_dir)
    final_df <- data.table()
    for (f in file_list) {
        print(paste0('appending... ',f))
        df <- fread(paste0(work_dir,'/',f))
        if (length(df) == 0 ) {
            next
        }
        final_df <- rbind(final_df,df,fill=TRUE)
    }
    return(final_df)
}


launch_NR_priors <- function(data) {
    print('LAUNCHING NOISE REDUCTION')
    causes <- unique(data[,acause])
    causes <- causes[causes != 'cc_code']
    output_list <- c()
    for (this_cause in causes) {
        sexes <- unique(data[,sex_id])
        for (this_sex in sexes) {
            cat("\n\n")
            print(paste0('running prior for cause ',  this_cause, ', sex ', this_sex))
            this_output <- paste0(this_cause, '_',
                            this_sex,
                            '.csv')
            this_job_name = paste0('cancer_nr', '_',
                                this_cause, '_',
                                this_sex)
            output_list <- c(output_list,this_output)

            
            
            if (!file.exists(paste0(work_dir,'/',this_output))) {
                launch_jobs(worker_script, script_arguments=c(this_cause,
                                                        this_sex),
                                                        memory_request=15,
                                                        job_header = this_job_name,
                                                        output_path=file.path(get_path("cancer_logs"), "noise_reduction_jobs"),
                                                        error_path=file.path(get_path("cancer_logs"), "noise_reduction_jobs"))
            }
        }
    }
    cat("\n\n")
    print(paste0('length of output_list is...', length(output_list)))
    # checks running NR jobs on cluster
    while (cluster_tools.get_job_count("cancer_nr") != 0) {
        cat("\n\n")
        print(paste0("Number of NR jobs left is ...", cluster_tools.get_job_count("cancer_nr")))
        print('waiting for current NR jobs to finish...')
        print('waiting 1 min and checking again...')
        Sys.sleep(60)
    }
    ## check for all outputs and return all missing files
    missing_list <- NULL
    for (j in output_list) {
        if (!file.exists(paste0(work_dir,'/',j))) {
            missing_list <- rbind(missing_list, j)
        }
    }
    print(paste0('number of remaining missing outputs...', length(missing_list)))
    Sys.sleep(60)
    if (length(missing_list) == 0) {
        return(TRUE)
    }
    else {
        print(paste0('outputs missing...', missing_list))
        print('rerunning priors for missing subsets...')
        return(FALSE)
    }
    return(FALSE)
}


if (!interactive()){
    input_dir <- get_path(process='cod_mortality', key='mortality_split_cc')
    output_dir <- get_path(process='cod_mortality', key='NR_prior')
    data <- fread(input_dir)
    success= FALSE
    while (success == FALSE) {
        success = launch_NR_priors(data)
    }
    if (success) {
        cat("\n\n")
        print('ready to compile!!!')
        final_df <- compile_nr_priors()
        print(paste0('compiling done! writing data to ', output_dir))
        write.csv(final_df, output_dir)
        return(TRUE)
    }
    else {
        print('need to rerun launch script for missing outputs')
        return(FALSE)
    }
}
