#############################################################

##Date: 3/4/2019
##Purpose:Prep MR-BRT at bundle-level
##Notes: 
##Updates: Aggregate ICG, prep and split by bundle into workable datasets
#
###########################################################

# Master function -----
prep_cfs <- function(run){
  
user <- Sys.info()[7]
run <- commandArgs(trailingOnly = TRUE)[1]

library(data.table)
library(readr)
library(readstata13)
source(paste0('db_utilities.R'))
source("utility.r") # For envelope
source('get_covariate_estimates.R')
source('get_age_metadata.R')
source('get_location_metadata.R')
source('get_population.R')

write_folder <- paste0(FILEPATH)

ages <- get_age_metadata(12, gbd_round_id = 5) %>% 
  setnames(c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
age_1 <- data.table(age_group_id = 28, age_group_years_start = 0, age_group_years_end = 1)%>% 
  setnames(c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
ages <- ages[age_group_id > 4] %>% rbind(age_1, fill = TRUE)

bundle_map <- bundle_icg()
locs <- get_location_metadata(35)

# Load hospital files -----
# Download and append files. Need to figure out where these files write to
hosp_files <- Sys.glob(paste0(FILEPATH))
filereader <- lapply(hosp_files, function(filepath){
  a <- tryCatch({
    df <- fread(filepath)
    outcome <- paste0(filepath, ' worked!')
    print(outcome)
    print(ncol(df))
    print(nrow(df))
    output <- list(data = df, logs = outcome)
    return(output)
  }, 
  error = function(cond){
    outcome <- paste0(filepath, ' broke')
    message(outcome)
    output <- list(data = NULL, logs = outcome)
    return(output)
  })
} )

hosp_dt <- rbindlist(lapply(filereader, function(list) list$data))
logs <- unlist(lapply(filereader, function(list) list$logs), use.names = FALSE)
logs <- data.frame(logs)
write_csv(logs, paste0(write_folder, 'fileloading.csv'))

# Hospital formatting -----
hosp_dt[is.na(hosp_dt)] <- 0
hosp_dt <- age_binner(hosp_dt)

# Calculate CFs
hosp_dt[, cf1 := inp_pri_indv_cases/inp_pri_claims_cases]
hosp_dt[, cf2 := inp_any_indv_cases/inp_pri_claims_cases]


# Format ages
hosp_dt <- merge(hosp_dt, ages[, c('age_start', 'age_group_id', 'age_end')], by = c('age_start', 'age_end'))
# Make mid-point of ages, keeping age_start as the variable
hosp_dt[, age_start := (age_start+age_end)/2]
hosp_dt <- merge(hosp_dt, locs[, c('location_id', 'parent_id')], by = 'location_id') %>%
  .[parent_id == 9, source := 'PHL'] %>%
  .[parent_id == 72, source := 'NZL'] %>%
  .[parent_id == 102, source := 'HCUP']

# Need to download the envelope and population to get an estimate of inpatient sample size
pops <- get_population(year_id = unique(hosp_dt$year_start), sex_id = c(1,2), location_id = unique(hosp_dt$location_id), age_group_id = unique(hosp_dt$age_group_id),
                       gbd_round_id = 6, decomp_step = 'step1') %>% setnames('year_id', 'year_start')
ip_envelope <- model_load(53771, 'raked') %>% setnames('year_id', 'year_start') %>% merge(ages[, c('age_group_id', 'age_start')], by = 'age_group_id')

samp_size <- merge(pops[, c('location_id', 'year_start', 'age_group_id', 'sex_id', 'population')], 
                   ip_envelope[, c('age_group_id', 'year_start','sex_id', 'location_id', 'gpr_mean')], by = c('sex_id', 'age_group_id', 'year_start', 'location_id'))
samp_size[, sample_size := gpr_mean*population]
hosp_dt <- merge(hosp_dt, samp_size[, c('sex_id', 'age_group_id', 'year_start', 'location_id', 'sample_size')], by = c('sex_id', 'age_group_id', 'year_start', 'location_id'))

# Calculate sample size
# std_error = sqrt(1 / sample_size * cf * (1 - cf) + 1 / (4 * sample_size^2) * 1.96^2)
# Just need to calculate the total claims to get a cause fraction
hosp_dt[, total_inp_pri_claims_cases := sum(inp_pri_claims_cases), by = c('age_start', 'location_id', 'sex_id', 'year_start')]
hosp_dt[, cf := inp_pri_claims_cases/total_inp_pri_claims_cases]
hosp_dt[sample_size < 1, sample_size := 1]
hosp_dt[, se := sqrt(1/sample_size*cf*(1-cf) + 1/(4*sample_size^2)*1.96^2)] # Should check these order of operations, but this is straight from Stata
str

# Prep Marketscan inputs -----
# Download and format data
df <- fread(paste0(FILEPATH))
df <- dcast(df, age_end + age_start + bundle_id + location_id + sex_id + year_end + year_start ~ estimate_type, value.var = 'val')
df[is.na(df)] <- 0
df[, cf3 := inp_otp_any_adjusted_otp_only_indv_cases/inp_pri_claims_cases]
df[, cf2 := inp_any_indv_cases/inp_pri_claims_cases]
df[, cf1 := inp_pri_indv_cases/inp_pri_claims_cases]

# Drop US national data, not representative like the states for Marketscan (should all be coded by the state)
df <- df[location_id != 102]

# Calculate variance on each point, using wilson's method 
# std_error = sqrt(1 / sample_size * cf * (1 - cf) + 1 / (4 * sample_size^2) * 1.96^2)
# Just need to calculate the total claims to get a cause fraction
df[, total_inp_pri_claims_cases := sum(inp_pri_claims_cases), by = c('age_start', 'location_id', 'sex_id', 'year_start')]
df[, cf := inp_pri_claims_cases/total_inp_pri_claims_cases]

# Get sample size, need Marketscan sample size files and an egeoloc map to get to location id
egeoloc_map <- fread(FILEPATH) # For merging
enrolee_files <- Sys.glob(FILEPATH)
enrolees <- rbindlist(lapply(enrolee_files, read.dta13)) %>% 
  merge(egeoloc_map, by = 'egeoloc') %>% 
  setnames('age_start', 'age') %>% age_binner()
enrolees[, sample_size := sum(sample_size), by = c('age_start', 'sex', 'year', 'location_id')] # Aggregate over 5-year age groups
enrolees <- unique(enrolees[, c('age_start', 'sex', 'year', 'location_id', 'sample_size', 'state')]) %>% setnames(c('sex', 'year'),                                                                                                                  c('sex_id', 'year_start'))
enrolees[, sex_id := as.numeric(sex_id)]

# merge enrolees
df <- merge(df, enrolees, by = c('age_start', 'sex_id', 'year_start', 'location_id'))
df[sample_size < 1, sample_size := 1]
df[, se := sqrt(1/sample_size*cf*(1-cf) + 1/(4*sample_size^2)*1.96^2)] # Should check these order of operations, but this is straigth from Stata

# Merge HAQ and age names, format ages
haqi <- get_covariate_estimates(1099, gbd_round_id = 6, decomp_step = 'step1') %>% setnames(c('year_id', 'mean_value'), c('year_start', 'haqi'))
df <- merge(df, haqi[, c('location_id', 'year_start', 'haqi')], by = c('location_id', 'year_start'))
df <- merge(df, ages[, c('age_start', 'age_group_id')], by = 'age_start')
df[, age_start := (age_start + age_end)/2]
df[age_start == 110, age_start := 97.5]
df[, source := 'Marketscan']
df[, parent_id := 102]

# Aggregate neonatal bundles -----
neonatal_collapser <- function(df){
  anence_bundles <- c(610, 612, 614)
  chromo_bundles <- c(436, 437, 438, 439, 638)
  poly_synd_bundles <- c(602, 604, 606, 799)
  cong_bundles <- c(622, 624, 626, 803)
  cong2_bundles <- c(616, 618)
  neonate_bundles <- c(80, 81, 82, 500)
  
  # Create dummy variable to sum over. Go through each set
  df[bundle_id %in% anence_bundles, neonate := 'anence'][bundle_id %in% chromo_bundles, neonate := 'chromo']
  df[bundle_id %in% poly_synd_bundles, neonate := 'poly_synd'][bundle_id %in% cong_bundles, neonate := 'cong']
  df[bundle_id %in% cong2_bundles, neonate := 'cong2'][bundle_id %in% neonate_bundles, neonate := 'neonate']
  
  # Otherwise just assign it as their bundle ID
  df[is.na(neonate), neonate := as.character(bundle_id)]
  
  # And collapse over neonate and demographics
  case_cols <- names(df)[grep("_cases$", names(df))]
  
  df[, inp_pri_claims_cases := sum(inp_pri_claims_cases), by = c('age_start', 'location_id', 'year_start', 'sex_id', 
                                                                 'neonate')]
  df[, inp_any_indv_cases := sum(inp_any_indv_cases), by = c('age_start', 'location_id', 'year_start', 'sex_id', 
                                                             'neonate')]
  df[, inp_pri_indv_cases := sum(inp_pri_indv_cases), by = c('age_start', 'location_id', 'year_start', 'sex_id', 
                                                             'neonate')]
  if('inp_otp_any_adjusted_otp_only_indv_cases' %in% colnames(df)){
    df[, inp_otp_any_adjusted_otp_only_indv_cases := sum(inp_otp_any_adjusted_otp_only_indv_cases), 
       by = c('age_start', 'location_id', 'year_start', 'sex_id', 'neonate')]
    
    df[, cf1 := inp_pri_indv_cases/inp_pri_claims_cases][, cf2 := inp_any_indv_cases/inp_pri_claims_cases][, cf3 := inp_otp_any_adjusted_otp_only_indv_cases/inp_pri_claims_cases]
  } else {
    df[, cf1 := inp_pri_indv_cases/inp_pri_claims_cases][, cf2 := inp_any_indv_cases/inp_pri_claims_cases]
  }
  
  return(df)
  
}

df <- neonatal_collapser(df)
hosp_dt <- neonatal_collapser(hosp_dt)

# Output formatting ----
# Drop infinite values and NAs, split by bundle and CF
mod_cf1 <- hosp_dt[!is.na(cf1) & cf1 != Inf]
mod_cf2 <- hosp_dt[!is.na(cf2) & cf2 != Inf]

mod_cf1[, age := NULL]
mod_cf2[, age := NULL]
cf1_cols <- names(mod_cf1)
cf2_cols <- names(mod_cf2)
ms_cf1 <- df[, ..cf1_cols] %>% .[!is.na(cf1) & cf1 != Inf & cf1 != 0]
ms_cf2 <- df[, ..cf2_cols] %>% .[!is.na(cf2) & cf2 != Inf & cf2 != 0]

# Append hospital and marketscan
mod_cf1 <- rbind(mod_cf1, ms_cf1, fill = TRUE)
mod_cf2 <- rbind(mod_cf2, ms_cf2, fill = TRUE)

mod_cf1 <- mod_cf1[cf1 != 0]
mod_cf1[cf1 == 1, cf1 := 0.99]
mod_cf1[, logit_mean := logit(cf1)]
mod_cf1[, logit_se := logit(se)]

# Getting NAs in tiny sample size NZL age group 235. Just going to drop because such small numbers anyways.
mod_cf1 <- mod_cf1[!is.na(logit_se)]

# Split
bundles <- unique(mod_cf1$bundle_id)
invisible(lapply(bundles, function(x){
  print(x)
  subset <- mod_cf1[bundle_id == x & cf1 != 0]
  subset[cf1 == 1, cf1 := 0.99] # Need to set an offset. 
  subset[, logit_mean := logit(cf1)]
  subset[, logit_se := logit(se)]
  subset[, intercept := 1]
  #subset[, log_haqi := log(haqi)]
  
  write_csv(subset, paste0(write_folder, 'prep_data/cf1/', x, '.csv'))
}))

bundles <- unique(mod_cf2$bundle_id)
invisible(lapply(bundles, function(x){
  print(x)
  subset <- mod_cf2[bundle_id == x & cf2 != 0 & cf2 != Inf]
  subset[, log_mean := log(cf2)]
  subset[, log_se := log(se)]
  subset[, intercept := 1]
  #subset[, log_haqi := log(haqi)]
  
  write_csv(subset, paste0(write_folder, 'prep_data/cf2/', x, '.csv'))
}))

bundles <- unique(df$bundle_id)
invisible(lapply(bundles, function(x){
  print(x)
  subset <- df[bundle_id == x & cf3 != 0 & !is.na(cf3) & cf3 != Inf]
  # Outlier polycystic ovarian syndrome data point over 2000
  if(x == 201){
    subset <- subset[cf3 < 2000]
  }
  # Outlier downs data point over 200
  if(x == 436){
    subset <- subset[cf3 < 200]
  }
  subset[, log_mean := log(cf3)]
  subset[, log_se := log(se)]
  subset[, intercept := 1]
  subset[, log_haqi := log(haqi)]
  subset <- subset[, c('age_start', 'location_id', 'sex_id', 'bundle_id', 'log_mean', 'log_se', 'intercept')]
  
  write_csv(subset, paste0(write_folder, 'prep_data/cf3/', x, '.csv'))
}))
  
}

prep_cfs(run=commandArgs(trailingOnly = TRUE)[[1]][1])

