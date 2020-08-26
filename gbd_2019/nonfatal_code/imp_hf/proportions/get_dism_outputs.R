
####### Steps the script perform:
####### 1. Launch qsub parallelized by locs to generate corrected prevalence 
####### for HF etiologies (23 total - 21 CVD-processed causes & 3 Chagas severity causes) 
####### 2. Save_results 

#######################################
## PREP ENVIRONMENT & GLOBAL VAR
#######################################

# source the central functions
source("get_draws.R")
source("get_location_metadata.R")
source("get_demographics.R")
source("save_results_epi.R")
source("get_age_metadata.R")

source("cluster_tools.r")
source("ubcov_tools.r")
source("hdf_utils.R")

# add libs
library(data.table)
library(foreign)
library(parallel)
library(rhdf5)

gbd_round_id <- 6

year_ids <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)
sex_ids  <- c(1,2)
age_group_ids = c(2:21,30,31,32,235)

age_groups <- get_age_metadata(gbd_round_id = gbd_round_id, age_group_set_id = 12)[, .(age_group_years_start, age_group_id)]
setnames(age_groups, "age_group_years_start", "age_start")
#age_groups[age_group_id %in% c(2, 3, 4), age_start := 0]

locations <- get_location_metadata(location_set_id=35, gbd_round_id=gbd_round_id)
dismod_locs <- unique(metadata$location_id)
loc_choices <- locations[location_type_id==2 | location_id %in%dismod_locs]$location_id

# cvd output folder
cvd_path = "FILEPATH"

# hf etiology map
composite <- fread(paste0(cvd_path, '/composite_cause_list.csv'))

# qsub diagnostic file paths
qsub_output = paste0("FILEPATH") 
qsub_errors = paste0("FILEPATH") 
do.call(file.remove, list(list.files(qsub_output, full.names = TRUE))) 
do.call(file.remove, list(list.files(qsub_errors, full.names = TRUE))) 

#######################################
## RUNS CHILD SCRIPT IN PARALLELIZATION
#######################################
file_list <- NULL
outputDir <- paste0(cvd_path, '/post_dismod')
datetime <- format(Sys.time(), '%Y_%m_%d')
dir.create(outputDir, showWarnings = FALSE)

locs <- expand.grid(loc_choices)
loc_path <- "FILEPATH"
write.csv(locs, loc_path, row.names=F)

rscript <- "dis_outputs.R"
n_jobs <- nrow(locs)

shell <- 'FILEPATH'

code_command <- paste0(shell, " ", rscript, " ", loc_path)
full_command <- paste0("qsub -l m_mem_free=VALUE -l fthread=VALUE ",
                       "-N hf_post ",
                       "-t ", paste0("1:", n_jobs), " ",
                       "-o FILEPATH ",
                       "-e FILEPATH ",
                                              code_command)
print(full_command)
system(full_command)

folder_list <- unique(na.omit(composite[!cause_name%like%'Chagas']$prev_folder))
for (loc in loc_choices){
  for (folder in folder_list) {
    if (folder !=""){
      output_path <- "FILEPATH"
      file_list <- c(file_list, output_path)
    }
  }
}
## Wait until all jobs are done
job_hold("hfpost_", file_list = file_list)

# Stop if file missing
for (file in file_list){
  if (!file.exists(file)) stop((paste0('Missing ', file)))
}


#######################################
## UPLOAD PREV TO APPRORIATE MES
#######################################
save_results <- T

me_ids <- unique(composite[!(is.na(prev_me_id)), prev_me_id])
me_ids <- expand.grid(me_ids)
m_path <- "FILEPATH"
write.csv(me_ids, m_path)

rscript <- "hf_upload_draws.R"
n_jobs <- nrow(me_ids)

shell <- 'FILEPATH'

code_command <- paste0(shell, " ", rscript, " ", m_path, " ", datetime)
full_command <- paste0("qsub -l m_mem_free=VALUE-l fthread=VALUE ",
                       "-N upload_draws ",
                       "-t ", paste0("1:", n_jobs), " ",
                       "-o FILEPATH ",
                       "-e FILEPATH ",
                       code_command)

print(full_command)
if (save_results) system(full_command)


