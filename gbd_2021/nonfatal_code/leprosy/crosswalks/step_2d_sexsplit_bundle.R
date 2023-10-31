# SEX SPLIT Bundle ADDRESS - NTDS: Leprosy
# Purpose: Apply sex crosswalk to bundle ADDRESS -- India only, age-specific
# Description: ADDRESS - India, age-specific  data sources: ICMR incidence (INDIA LEPROSY SURVEY), Sci Lit incidence

#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

code_root <- paste0("/homes/", Sys.info()[7], "/")
data_root <- 'FILEPATH'

# packages
library(msm)
library(data.table)
library(ggplot2)
library(openxlsx)
library(dplyr)
library(crosswalk, lib.loc = "FILEPATH")

# central functions
source("FILEPATH/get_ids.R")
source("FILEPATH//get_location_metadata.R")
source("FILEPATH/r/get_age_metadata.R")
source("FILEPATH/get_population.R")
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_bundle_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_crosswalk_version.R')
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_demographics.R")

run_file <- fread(paste0(data_root, "FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")

decomp_step <- 'ADDRESS'
gbd_round_id <- 'ADDRESS'

#############################################################################################
###'                         [Create Bundle Version (if needed)]                          ###
#############################################################################################

bundle_version <- save_bundle_version(bundle_id = ADDRESS, decomp_step = decomp_step, gbd_round_id = gbd_round_id) 

#############################################################################################
###'                          [Upload Sex CW Object]                                      ###
#############################################################################################

fit1 <- py_load_object(filename = "FILEPATH", pickle = "dill")

#############################################################################################
###'                      [Sex Split Age-SPECIFIC Bundles ADDRESS]                           ###
#############################################################################################

locs <- get_location_metadata(ADDRESS)
locs <- locs[, .(location_id, ihme_loc_id)]

# cleaning
bADDRESS <- get_bundle_version(bundle_version_id=ADDRESS, fetch = "all")
bADDRESS <- merge(bADDRESS, locs, by = "location_id")
bADDRESS[, unit_value_as_published := 1]
bADDRESS <- bADDRESS[!(cases >= sample_size)]

# exclude prevalence data
bADDRESS <- bADDRESS[measure == "incidence"]

# we don't model data where mean = 0
bADDRESS<-bADDRESS[mean>0,]

# pull subset to male and female data points only 
bADDRESS_ss <- bADDRESS[!(sex=="Both")]

# pull subset to 'both' sex data points only 
bADDRESS <- bADDRESS[(sex=="Both")]

####  apply sex split  ####

# convert both to female
bADDRESS[, c("inc2", "inc2_se", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
  fit_object = fit1,       # result of CWModel()
  df = bADDRESS,            # original data with obs to be adjusted
  orig_dorms = "sex", # name of column with (all) def/method levels
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "standard_error"  # standard error of original mean
)

# create male estimates via subtraction by subtracting mean female estimate from both

#generate new case estimates from the adjusted mean

#need female population
#need male population

# list of years in dataset 
years <- unique(bADDRESS$year_start)
# list of locations in dataset
locs <- unique(bADDRESS$location_id)

# pull populations
malepop <- get_population(gbd_round_id=ADDRESS, decomp_step="ADDRESS", location_id=locs, year_id=years, sex_id = 1, age_group_id=22)
fempop <- get_population(gbd_round_id=ADDRESS, decomp_step="ADDRESS", location_id=locs, year_id=years, sex_id = 2, age_group_id=22)

# merge populations
malepop$year_start  <-malepop$year_id
fempop$year_start <- fempop$year_id

bADDRESS_b <- merge(bADDRESS, malepop, by=c("location_id","year_start")) 

# rename population variable
bADDRESS_b <- bADDRESS_b %>% 
  rename(male_pop = population)

bADDRESS_b <- merge(bADDRESS_b, fempop, by=c("location_id","year_start")) 

bADDRESS_b<-bADDRESS_b %>% 
  rename(fem_pop = population)

# drop variables for clean up
bADDRESS_b <- select(bADDRESS_b,-c(sex_id.x,sex_id.y,year_id.x,year_id.y, run_id.y,run_id.x, age_group_id.y,age_group_id.x))

## NEW - take mean, SEs into real-space so we can combine and make adjustments
bADDRESS_b[, se_val  := unique(sqrt(diff_se^2 + as.vector(fit1$gamma)))]
bADDRESS_b[, c("diff2", "diff2_se") := data.table(delta_transform(mean = diff, sd = se_val, transform = "log_to_linear"))]
# convert diff because we want to tte ratio of female/male in linear space to apply to populations

# calculate %female
bADDRESS_b[,tot_pop := fem_pop+male_pop]
bADDRESS_b[,fem_per := fem_pop/tot_pop]

# calculate male and female denominator
bADDRESS_b[,fem_ss := (fem_per*effective_sample_size)]
bADDRESS_b[,male_ss := effective_sample_size-(fem_per*effective_sample_size)]

####take adjusted incidence x female population
#here assumption around 1 year for person-time
bADDRESS_b[,fem_cases := inc2*fem_ss]
bADDRESS_b[,male_cases := cases-fem_cases]
bADDRESS_b[,mean_male := male_cases/male_ss]

##recalcuate standard error of adjusted incidence
bADDRESS_b[, inc3_se := sqrt((diff2_se^2 * standard_error^2) + (diff2_se^2 * mean^2) + (standard_error^2 * diff2^2))]

##recalculate SE of males/females - multiply by the recalculated SE from the MR-BRT model
bADDRESS_b[measure == "incidence", se_male := sqrt((1/male_ss)*(male_cases/male_ss)*(1-(male_cases/male_ss)))*inc3_se]
bADDRESS_b[measure == "incidence", se_fem := sqrt((1/fem_ss)*(fem_cases/fem_ss)*(1-(fem_cases/fem_ss)))*inc3_se]

##split data and append male and female records
males<-bADDRESS_b %>% 
  select(nid, location_id, year_start, year_end, age_start, unit_value_as_published, measure, recall_type, representative_name, source_type, unit_type, urbanicity_type, uncertainty_type_value, upper, lower, age_end, seq, is_outlier, case_name, bundle_id, origin_seq, cause_id, male_cases, mean_male, se_male, male_ss) %>%
  rename(mean = mean_male, cases = male_cases, standard_error=se_male, effective_sample_size=male_ss)

# merge back on sex specific dataset portion of bundle
males_ss <- bADDRESS_ss[bADDRESS_ss$sex=="Male", ]
males_ss<-males_ss %>%
  select(nid, location_id, underlying_nid, sampling_type, recall_type_value, uncertainty_type, input_type, design_effect, year_start, year_end, age_start, unit_value_as_published, measure, age_end, recall_type, representative_name, source_type, unit_type, urbanicity_type, uncertainty_type_value, upper, lower, seq, is_outlier, case_name, bundle_id, origin_seq, cause_id, cases, mean, standard_error, effective_sample_size)
males_ss <- rbind(males,males_ss, fill=TRUE)
males_ss$sex<-"Male"

# for cw upload need sample size, same as effective sample size
males_ss[, sample_size := effective_sample_size]

## NEW - changed standard_error to adjusted SE for females
females<-bADDRESS_b %>% 
  select(nid, location_id, year_start, year_end, age_start, unit_value_as_published, measure, age_end, recall_type, representative_name, source_type, unit_type, urbanicity_type, uncertainty_type_value, upper, lower, seq, is_outlier, case_name, bundle_id, origin_seq, cause_id, fem_cases, inc2, se_fem, fem_ss) %>%
  rename(mean = inc2, cases = fem_cases, standard_error=se_fem, effective_sample_size=fem_ss)

females_ss <- bADDRESS_ss[bADDRESS_ss$sex=="Female", ]
females_ss<-females_ss %>%
  select(nid, location_id, underlying_nid, sampling_type, recall_type_value, uncertainty_type, input_type, design_effect, year_start, year_end, age_start, unit_value_as_published, measure, age_end, recall_type, representative_name, source_type, unit_type, urbanicity_type, uncertainty_type_value, upper, lower, seq, is_outlier, case_name, bundle_id, origin_seq, cause_id, cases, mean, standard_error, effective_sample_size)
females_ss <- rbind(females,females_ss,fill=TRUE)
females_ss$sex<-"Female"

# for cw upload need sample size, same as effective sample size
females_ss[, sample_size := effective_sample_size]

#append together
split_ADDRESS_b <- rbind(males_ss,females_ss)
split_ADDRESS_b$note_modeler<-"sexsplit"

### add seq
split_ADDRESS_b[, crosswalk_parent_seq := seq]
split_ADDRESS_b[, seq := ""]

###write csv here
write.csv(split_ADDRESS_b,"FILEPATH")

## saving
bADDRESS_outfile <- paste0(crosswalks_dir, "FILEPATH")

openxlsx::write.xlsx(split_ADDRESS_b, bADDRESS_outfile,  sheetName = "extraction")
bADDRESS_description <- "ADDRESS"

bADDRESS_cw_md  <- save_crosswalk_version(bundle_version_id = ADDRESS,
                                       data_filepath =  bADDRESS_outfile,
                                       description = bADDRESS_description)