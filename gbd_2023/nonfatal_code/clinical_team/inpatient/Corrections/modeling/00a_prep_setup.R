#############################################################
# Settings for CF prep
#############################################################

# Set Controls / Constants ####
## CF Run Parameters ####
USER = Sys.info()[7]
PREP_VERS = 38
CURRENT_MAP_VERSION = 33 
## GBD Parameters ####
LOCATION_SET_ID = 35 
GBD_RELEASE_ID = 16

## Data source switches #### 
prep_ms = TRUE
prep_pol = TRUE
prep_hosp = TRUE # includes HCUP, PHL, and NZL data
prep_twn = TRUE
prep_cms = TRUE
agg_over_year = TRUE
agg_over_subnational = TRUE

# Libraries and source functions ####
library(reticulate)
library(data.table)
library(readr)
library(readstata13)
library(tidyverse)
library(arrow)
source('FILEPATH/db_utilities.R')
source('FILEPATH/get_covariate_estimates.R')
source('FILEPATH/get_age_metadata.R')
source('FILEPATH/get_location_metadata.R')
source('FILEPATH/get_population.R')

delta_transform <- function(mean, sd, transformation) {
  
  if (transformation == "linear_to_log") f <- xwalk$utils$linear_to_log
  if (transformation == "log_to_linear") f <- xwalk$utils$log_to_linear
  if (transformation == "linear_to_logit") f <- xwalk$utils$linear_to_logit
  if (transformation == "logit_to_linear") f <- xwalk$utils$logit_to_linear
  
  out <- do.call("cbind", f(mean = array(mean), sd = array(sd)))
  colnames(out) <- paste0(c("mean", "sd"), "_", strsplit(transformation, "_")[[1]][3])
  return(out)
}

# Set up write folder ####
write_folder = paste0('FILEPATH')
if (!dir.exists(write_folder)) dir.create(write_folder)

# Get GBD locs/ages ####
ages = get_age_metadata(release_id = GBD_RELEASE_ID) %>%
  setnames(c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
age_1 = data.table(age_group_id = 28, age_start = 0, age_end = 1)
ages = ages %>% rbind(age_1, fill = TRUE)

bundle_map = bundle_icg()

locs = get_location_metadata(location_set_id = LOCATION_SET_ID, release_id = GBD_RELEASE_ID)

# DNO Mapping ####
dno_mapping = data.table(estimate_type = c('inp_pri_claims_cases','inp_pri_indv_cases', 'inp_any_indv_cases', 'inp_otp_any_adjusted_otp_only_indv_cases', 'inp_pri_claims_5_percent', 'inp_pri_indv_5_percent_cases', 'inp_any_indv_5_percent_cases'),
                         dno_name = c('dno0', 'dno1', 'dno12', 'dno123', 'dno0', 'dno1', 'dno12'),
                         estimate_id = c('estimate_14', 'estimate_15', 'estimate_17', 'estimate_21', 'estimate_26', 'estimate_28', 'estimate_29'))


# Aggregate neonatal bundles function ####
neonatal_collapser = function(df){
  df_chromo = df[bundle_id %in% c(3029)]
  cols = names(df_chromo)
  cols = str_subset(cols,"count|population", negate=TRUE)
  df_chromo = df_chromo[, .(count = sum(count), population = sum(population))
                         ,by=cols]
  df_chromo[, index := 1]
  df_chromo[, bundle_id := NULL]
  
  buns = data.table(bundle_id = c(436,437,438,439,638,3029), index = c(1,1,1,1,1,1))
  df_chromo = merge(df_chromo, buns, by='index', allow.cartesian = TRUE)
  df_chromo[, index := NULL]
  
  df = df[!(bundle_id %in% c(436,437,438,439,638,3029))]
  df = rbind(df, df_chromo)
  
  return(df)
}
