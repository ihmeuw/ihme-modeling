################################################################################
## DESCRIPTION: Register and launch ST-GPR Model from R ##
## INPUTS: Finalized processed anthropometric data ##
## OUTPUTS: Launched ST-GPR Model ##
## AUTHOR:  ##
## DATE: 31 January 2019 ##

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

# Base filepaths
work_dir <- paste0(j, 'FILEPATH')
share_dir <- 'FILEPATH' # only accessible from the cluster
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
stgpr_dir <- 'FILEPATH'


## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH/primer.R'))
source(paste0(code_dir, 'FILEPATH/version_tools.R'))
source(paste0(code_dir, 'FILEPATH/anthro_utilities.R'))
library(rhdf5, lib.loc = "FILEPATH")
source("FILEPATH/plot_gpr.R")
source('FILEPATH/register.R')
source('FILEPATH/sendoff.R')
source('FILEPATH/utility.r')

## PARSE ARGS
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "Site where data will be stored",
                    default = 'FILEPATH/', type = "character")
parser$add_argument("--anthro_version_id", help = "version number for whole run",
                    default = 1, type = "integer")
parser$add_argument("--model_type", help = "which anthropometrics model to run, currently only height or bmi (adult vs child)",
                    default = 'prev_ob', type = "character")
parser$add_argument("--decomp_step", help = "decomp step we're uploading for",
                    default = "iterative", type = "character")
parser$add_argument("--cycle_year", help = "current GBD cycle",
                    default = "gbd_2020", type = "character")
parser$add_argument("--run_type", help = "test or production. 0 draws if test",
                    default = 'production', type = "character")
parser$add_argument("--stgpr_model_id", help = "Model id to use from config",
                    default = 10, type = "integer")
parser$add_argument("--ow_cw_version_id", help = "Crosswalk version to use for overweight model",
                    default = 30458, type = "integer")
parser$add_argument("--ob_cw_version_id", help = "Crosswalk version to use for obesity model",
                    default = 30461, type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)


# Specify cluster arguments for launch
cluster_project = 'proj_stgpr'

# GBD 2020 - use config
ow_config <- fread(sprintf('%s/FILEPATH/%s_config_2020.csv', code_dir, "prev_ow"))
ob_config <- fread(sprintf('%s/FILEPATH/%s_config_2020.csv', code_dir, "prev_ob"))

prev_ow_stgpr_id <- register_stgpr_model(sprintf('%s/FILEPATH/%s_config_2020.csv', code_dir, "prev_ow"),
                                         stgpr_model_id)

prev_ob_stgpr_id <- register_stgpr_model(sprintf('%s/FILEPATH/%s_config_2020.csv', code_dir, "prev_ob"),
                                         stgpr_model_id)

  



# Submit ST-GPR model for your newly-created run_id!
stgpr_sendoff(prev_ow_stgpr_id, cluster_project)
stgpr_sendoff(prev_ob_stgpr_id, cluster_project)

# Link st_gpr run id with internal model version id (generated in model launch run all)
version_map_path <- paste0(code_dir, '/FILEPATH/data_versions.csv')
tracking <- fread(version_map_path)
temp_ow <- tracking[crosswalk_version_id == ow_cw_version_id] %>% .[ ,stgpr_run_id := prev_ow_stgpr_id]
temp_ob <- tracking[crosswalk_version_id == ob_cw_version_id] %>% .[, stgpr_run_id := prev_ob_stgpr_id]
temp_ow[, note := "Outliers and fixed variance, logit raking on"]
temp_ob[, note := "Outliers and fixed variance, logit raking on"]
tracking <- rbind(tracking[!crosswalk_version_id %in% c(ow_cw_version_id, ob_cw_version_id)], unique(temp_ow), unique(temp_ob))
fwrite(tracking, version_map_path)
link.version(version_map_path, anthro_version_id, 'prev_ow_stgpr_id', prev_ow_stgpr_id)
link.version(version_map_path, anthro_version_id, 'prev_ob_stgpr_id', prev_ob_stgpr_id)

# Plot the results
plot_gpr(prev_ow_stgpr_id, output.path = sprintf("%s/FILEPATH/%d_plots.pdf", work_dir, cycle_year, "prev_ow", prev_ow_stgpr_id))
plot_gpr(prev_ob_stgpr_id, output.path = sprintf("%s/FILEPATH/%d_plots.pdf", work_dir, cycle_year, "prev_ob", prev_ob_stgpr_id))
