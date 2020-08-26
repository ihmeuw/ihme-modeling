##
## Description: Launches jobs to noise reduce cancer registry data 
## Contributors: NAME
##

# set working directories by operating system
library(here)
library(data.table)
if (!exists("code_repo")) { 
    code_repo <- sub("cancer_estimation.*", 'cancer_estimation', here())
}

# import libraries 
source(file.path(code_repo, 'r_utils/utilities.r'))
source(file.path(code_repo, 'r_utils/cluster_tools.r'))
source(file.path(code_repo, '_database/cdb_utils.r'))

# set up common directories
worker_script <- get_path(process='cod_mortality', key='noise_reduction_prior')
nr_scratch_space <- get_path(process='cod_mortality', key='noise_reduction_workspace')
work_dir = get_path(process='cod_mortality', key='noise_reduction_workspace')
#unlink(paste0(work_dir,'/*'), recursive=FALSE) # this line deletes all previous outputs 

add_loc_agg <- function(df) { 
    ## Accepts a dataframe and marks entries that are location aggregates
    ## is_loc_agg == 1 iff country level AND country isn't modeled subnationally 
    subnat_countries <- get_gbd_parameter('subnationally_modeled_countries')
    df$is_loc_agg <- 0 
    df$is_loc_agg[(df$country_id %in% subnat_countries$subnationally_modeled_countries) & 
                    (df$country_id == df$location_id)] <- 1 

    # subnat id to help determine which countries are subnationally modeled
    df$subnat_id <- copy(df$location_id) 
    df$subnat_id[(df$location_id == df$country_id)] <- 0
    return(df) 
}


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
    causes <- unique(data$acause)
    causes <- causes[causes != 'cc_code'] # don't model cc_code, but used to calculate sample_size
    output_list <- c()
    for (this_cause in causes) {
        sexes <- unique(data$sex_id)
        for (this_sex in sexes) {
            is_loc_agg <- unique(data$is_loc_agg)
            for (loc_agg in is_loc_agg) {
                print(paste0('running prior for... ', this_cause, ', ', this_sex, ', ', loc_agg))
                this_output <- paste0(this_cause, '_',
                                this_sex, '_',
                                loc_agg,
                                '.csv')
                this_job_name = paste0('cancer_nr', '_',
                                    this_cause, '_', 
                                    this_sex, '_', 
                                    loc_agg)
                output_list <- c(output_list,this_output)
                
                # launch jobs for every cause-sex-country combination
                if (!file.exists(paste0(work_dir,'/',this_output))) { 
                    launch_jobs(worker_script, script_arguments=c(this_cause,
                                                            this_sex, loc_agg), 
                                                            memory_request=15, 
                                                            job_header = this_job_name)
                }
            }
        }
    }
    while (length(system(paste0("qstat -r | grep cancer_nr"), intern=T)) > 0) { 
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
    if (length(missing_list) == 0) { 
        return(TRUE)     
    }
    else { 
        print(paste0('outputs missing...', missing_list))
        return(FALSE)
    }        
}


if (!interactive()){
    input_dir <- get_path(process='cod_mortality', key='neonatal_split')
    output_dir <- get_path(process='cod_mortality', key='NR_prior')
    data <- read.csv(input_dir) 
    data <- add_loc_agg(data)
    success = launch_NR_priors(data)
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
