##################################################################################
# Name of Script: 01_pull_VR_gold_standard.R
# Date: 4/4/2024
# Description: Pulls VR gold standard data from the CoD database
## Then can use the saved file starting at step 02_prep_cw_inputs
#
# Output: One file of gold standard VR data for crosswalk prep: 
#             1) cod_cancer_data_23.csv
#
# Contributors: INDIVIDUAL_NAME
###################################################################################

# Load required libraries
pacman::p_load(data.table, dplyr, tidyr, tidyverse, haven, readxl, reshape2,
               crayon)

library(ggplot2)
library(Cairo)
# define drive paths
if (Sys.info()["sysname"] == 'Linux'){
  j <- "FILEPATH"
  h <- paste0("FILEPATH",Sys.info()[7],"/")
} else if (Sys.info()["sysname"] == 'Darwin'){
  j <- "FILEPATH"
  h <- paste0("FILEPATH",Sys.info()[7],"/")
} else {
  j <- "J:/"
  h <- "H:/"
}

# Source required shared functions
source("FILEPATH/get_cod_data.R")


# Set CoD version variables
COD_VERSION <- data.table(version_name = c("gbd_2023_best", "gbd_2021_best"),
                          release = c(16, 9),

# Get cancer causes
gbd_causes <- fread("FILEPATH")

# Pull most recent CoD data for all cancer causes
cod_dt <- c()

# Pull COD data for causes
for (cause in unique(gbd_causes$cause_id)){
  
  cat(green(paste0('>>> pulling for cause_id ', cause, ' ')))
  
  cod_dt_temp <- get_cod_data(cause_id = cause, ###
                              release_id = COD_VERSION[version_name=="gbd_2023_best"]$release,
                              refresh_id= COD_VERSION[version_name=="gbd_2023_best"]$refresh,
                              is_outlier = 'all')
  cod_dt_temp <- cod_dt_temp[,.(location_id, year_id, age_group_id, sex_id, cause_id, cf_raw, cf, 
                                is_outlier, is_representative, data_type_name, cod_source_label,
                                sample_size)]
  cod_dt <- rbind(cod_dt, cod_dt_temp)
}

gbd_2023_best <- copy(cod_dt)

fwrite(gbd_2023_best, "FILEPATH")
