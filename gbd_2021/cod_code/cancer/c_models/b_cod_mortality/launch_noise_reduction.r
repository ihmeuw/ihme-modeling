##
## Description: Launches jobs to noise reduce cancer registry data 
## Contributors: USERNAME
##

# set working directories by operating system
library(here)
library(data.table)

# import libraries 
source(file.path('FILEPATH/utilities.r'))
source(file.path('FILEPATH/cluster_tools.r'))
source(file.path('FILEPATH/cdb_utils.r'))
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
    ## Launch Noise Reduction that creates priors for every cause-sex-country GBD
    ## models for mortality
    ##
    print('LAUCHING NOISE REDUCTION')

    # NOTE should use active causes from database 
    causes <- unique(data[,acause])
    causes <- causes[causes != 'cc_code'] # don't model cc_code, but used to calculate sample_size
    output_list <- c()
    for (this_cause in causes) {
        sexes <- unique(data[,sex_id])
        for (this_sex in sexes) {
            print(paste0('running prior for cause... ',  this_cause, ', ', this_sex))
            this_output <- paste0(this_cause, '_',
                            this_sex,
                            '.csv')
            this_job_name = paste0('cancer_nr', '_',
                                this_cause, '_', 
                                this_sex)
            output_list <- c(output_list,this_output)
            
            # launch jobs for every cause-sex-country combination that doesn't 
            # already have an output 
            if (!file.exists(paste0(work_dir,'/',this_output))) { 
                launch_jobs(worker_script, script_arguments=c(this_cause,
                                                        this_sex), 
                                                        memory_request=15, 
                                                        job_header = this_job_name,
                                                        output_path=this_output,
                                                        error_path='FILEPATH')
            }
        }
    }
    print(paste0('length of output_list is...', length(output_list)))
    # checks running NR jobs on cluster
    while (system(paste0("qstat | grep cancer_nr | wc -l "), intern=T) != 0) { 
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
        print('ready to compile!!!')
        final_df <- compile_nr_priors()
        print('compiling done!')   
        write.csv(final_df, output_dir)
        return(TRUE)
    }
    else {
        print('need to rerun launch script for missing outputs')
        return(FALSE)
    }
}
