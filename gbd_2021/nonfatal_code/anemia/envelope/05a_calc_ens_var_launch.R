
Sys.umask(mode = 002)

os <- .Platform$OS.type

library(data.table)
library(magrittr)
library(ggplot2)

# source all central functions
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# ----- SET PARAMS 

decomp_step = "iterative"
gbd_round_id = 7
var = "anemia"
version = "gbd2020_prelim"
mean_meid = 10487
prev_meids = "25321,25322,25318" 
thresholds1 = "90,130,150"
thresholds2 = "70,100,110"
thresholds3 = "80,110,115"
thresholds4 = "80,110,130"
thresholds5 = "80,110,120"
threshold_weights = ".149,.052,.004"
outdir = "FILEPATH"
weights_filepath <- paste0("FILEPATH/weights.csv")
num_draws = 1000

## Demographics
source(paste0("FILEPATH/get_location_metadata.R"))
loc_meta <- get_location_metadata(location_set_id=9,gbd_round_id=7)
loc_meta <- loc_meta[is_estimate == 1 & most_detailed == 1]
location_ids <- unique(loc_meta$location_id)

year_ids = c(1990:2022)
sex_ids1 = c(1,2)
sex_ids2 = c(1,2)
sex_ids3 = c(1,2)
sex_ids4 = c(1)
sex_ids5 = c(2)
age_group_ids1 = c(2,3)
age_group_ids2 = c(388,389,238,34)
age_group_ids3 = c(6,7)
age_group_ids4 = c(8:20,30:32,235)
age_group_ids5 = c(8:20,30:32,235)

# ----- Launch job

dir.create(file.path(outdir, var, version), recursive = T)

weights <- fread(weights_filepath)
names(weights) <- tolower(names(weights))
weights <- weights[, c("exp","gamma","gumbel","invgamma","llogis", "lnorm","mgamma","mgumbel","norm","weibull")]

write.csv(weights, file.path(outdir, var, version, "weights.csv"), na = "", row.names = F)

XMIN = 20
XMAX = 220

min_allowed_sd = 0.5
max_allowed_sd = 55

param_map1 <- data.table(expand.grid(
  
  decomp_step=decomp_step,
  gbd_round_id=gbd_round_id,
  version = version,
  var=var,
  mean_meid = mean_meid,
  prev_meids = prev_meids,
  min_allowed_sd = min_allowed_sd,
  max_allowed_sd = max_allowed_sd,
  XMIN = XMIN,
  XMAX = XMAX,
  thresholds = thresholds1,
  threshold_weights = threshold_weights,
  outdir = outdir,
  num_draws = num_draws,
  location_ids=location_ids,
  year_ids = year_ids,
  sex_ids = sex_ids1,
  age_group_ids = age_group_ids1
  
))

param_map2 <- data.table(expand.grid(
  
  decomp_step=decomp_step,
  gbd_round_id=gbd_round_id,
  version = version,
  var=var,
  mean_meid = mean_meid,
  prev_meids = prev_meids,
  min_allowed_sd = min_allowed_sd,
  max_allowed_sd = max_allowed_sd,
  XMIN = XMIN,
  XMAX = XMAX,
  thresholds = thresholds2,
  threshold_weights = threshold_weights,
  outdir = outdir,
  num_draws = num_draws,
  location_ids=location_ids,
  year_ids = year_ids,
  sex_ids = sex_ids2,
  age_group_ids = age_group_ids2
  
))

param_map3 <- data.table(expand.grid(
  
  decomp_step=decomp_step,
  gbd_round_id=gbd_round_id,
  version = version,
  var=var,
  mean_meid = mean_meid,
  prev_meids = prev_meids,
  min_allowed_sd = min_allowed_sd,
  max_allowed_sd = max_allowed_sd,
  XMIN = XMIN,
  XMAX = XMAX,
  thresholds = thresholds3,
  threshold_weights = threshold_weights,
  outdir = outdir,
  num_draws = num_draws,
  location_ids=location_ids,
  year_ids = year_ids,
  sex_ids = sex_ids3,
  age_group_ids = age_group_ids3
  
))

param_map4 <- data.table(expand.grid(
  
  decomp_step=decomp_step,
  gbd_round_id=gbd_round_id,
  version = version,
  var=var,
  mean_meid = mean_meid,
  prev_meids = prev_meids,
  min_allowed_sd = min_allowed_sd,
  max_allowed_sd = max_allowed_sd,
  XMIN = XMIN,
  XMAX = XMAX,
  thresholds = thresholds4,
  threshold_weights = threshold_weights,
  outdir = outdir,
  num_draws = num_draws,
  location_ids=location_ids,
  year_ids = year_ids,
  sex_ids = sex_ids4,
  age_group_ids = age_group_ids4
  
))

param_map5 <- data.table(expand.grid(
  
  decomp_step=decomp_step,
  gbd_round_id=gbd_round_id,
  version = version,
  var=var,
  mean_meid = mean_meid,
  prev_meids = prev_meids,
  min_allowed_sd = min_allowed_sd,
  max_allowed_sd = max_allowed_sd,
  XMIN = XMIN,
  XMAX = XMAX,
  thresholds = thresholds5,
  threshold_weights = threshold_weights,
  outdir = outdir,
  num_draws = num_draws,
  location_ids=location_ids,
  year_ids = year_ids,
  sex_ids = sex_ids5,
  age_group_ids = age_group_ids5
  
))

param_map <- rbind(param_map1, param_map2, param_map3, param_map4, param_map5)


## Run below if need to re-launch failed jobs
#setwd("FILEPATH")
#param_map[,filename := paste0("meanSD_",location_ids,"_",age_group_ids,"_",sex_ids,"_",year_ids,".rds")]
#files <- list.files()
#files_want <- param_map$filename
#files_want <- files_want[!(files_want %in% files)]

#param_map <- param_map[filename %in% files_want]

## Return here
param_map_filepath <- file.path(outdir, var, version, "param_map.csv")
write.csv(param_map, param_map_filepath, row.names = F, na = "")


## QSUB Command
job_name <- paste0(var, "_ens_var")
thread_flag <- "-l fthread=2"
mem_flag <- "-l m_mem_free=10G"
runtime_flag <- "-l h_rt=05:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q all.q"
throttle_flag <- "-tc 200"
n_jobs <- paste0("1:", nrow(param_map))
prev_job <- "nojobholds"
next_script <- paste0("FILEPATH/model_ens_variance.R")
error_filepath <- paste0("FILEPATH/")
output_filepath <- paste0("FILEPATH/")
project_flag<- "-P PROJECT"

qsub_command <- paste( "qsub", thread_flag, "-N", job_name, project_flag, mem_flag, runtime_flag, throttle_flag, queue_flag, "-t", n_jobs, "-e", error_filepath, "-o", output_filepath, "-hold_jid", prev_job,  "/ihme/singularity-images/rstudio/shells/execRscript.sh -i /ihme/singularity-images/rstudio/ihme_rstudio_3601.img -s", next_script, param_map_filepath )

system(qsub_command)
