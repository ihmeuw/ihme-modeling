# SEX SPLIT Bundle ADDRESS - NTDS: Leprosy
# Purpose: Apply sex crosswalk to bundle ADDRESS -- reported cases <grade 2 
# Description: ADDRESS - grade <2   data sources: NLEP (INDIA NATIONAL PROGRAM DATA), WER disaggregated incidence-WHO

#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

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

decomp_step <- 'ADDRESS'
gbd_round_id <- 'ADDRESS'

#############################################################################################
###'                         [Create Bundle Version (if needed)]                          ###
#############################################################################################

bundle_version <- save_bundle_version(bundle_id = ADDRESS, decomp_step = decomp_step, gbd_round_id = gbd_round_id) 

#############################################################################################
###'                                  [Upload Sex CW Object]                              ###
#############################################################################################

fit1 <- py_load_object(filename = "FILEPATH", pickle = "dill")

#############################################################################################
###'                     [Sex Split Input data Bundle ADDRESS]                               ###
#############################################################################################

#[Sex crosswalk - get bundle data] #needs to be updated, pulling in separate file here and reattaching to bundle uploaded
bADDRESS <- get_bundle_version(ADDRESS, fetch="all")

# exclude sinan data -- these data are the proportion of cases < grade2 among leprosy cases (not the total population)
bADDRESS <- bADDRESS[nid != ADDRESS]

# exclude prevalence data
bADDRESS <- bADDRESS[measure == "incidence"]

# pull subset to male and female data points only
bADDRESS_ss <- bADDRESS[!(sex=="Both")]

# pull subset to 'both' sex data points only 
bADDRESS <- bADDRESS[(sex=="Both")]

# we don't model data where mean = 0
bADDRESS<-bADDRESS[mean>0,]

#viewing data in a scatterplot
ggplot(bADDRESS, aes(x=year_start, y=mean)) +
  geom_point() +
  xlab("Year") + ylab("Both Sex") +
  geom_abline(intercept = 0, slope = 1) 

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
# generate new case estimates from the adjusted mean

#need female population
#need male population

# list of years in dataset 
years <- unique(bADDRESS$year_start)
# list of locations in dataset
locs <- unique(bADDRESS$location_id)

# pull populations
malepop <- get_population(gbd_round_id=7\ADDRESS, decomp_step="ADDRESS", location_id=locs, year_id=years, sex_id = 1, age_group_id=22)
fempop <- get_population(gbd_round_id=ADDRESS, decomp_step="ADDRESS", location_id=locs, year_id=years, sex_id = 2, age_group_id=22)

# merge populations
malepop$year_start  <- malepop$year_id
fempop$year_start <- fempop$year_id

bADDRESS_b <- merge(bADDRESS, malepop, by=c("location_id","year_start")) 

# rename population variable
bADDRESS_b <- bADDRESS_b %>% 
  rename(male_pop = population)

bADDRESS_b <- merge(bADDRESS_b, fempop, by=c("location_id","year_start")) 

bADDRESS_b <- bADDRESS_b %>% 
  rename(fem_pop = population)

# drop variables for clean up
bADDRESS_b <- select(bADDRESS_b,-c(sex_id.x,sex_id.y,year_id.x,year_id.y, run_id.y,run_id.x, age_group_id.y,age_group_id.x))

## NEW - take mean, SEs into real-space so we can combine and make adjustments
bADDRESS_b[, se_val  := unique(sqrt(diff_se^2 + as.vector(fit1$gamma)))]
bADDRESS_b[, c("diff2", "diff2_se") := data.table(delta_transform(mean = diff, sd = se_val, transform = "log_to_linear"))]
# convert diff because we want to the ratio of female/male in linear space to apply to populations

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

#need to update the standard error for males and females
#recalcuate standard error of adjusted incidence
bADDRESS_b[, inc3_se := sqrt((diff2_se^2 * standard_error^2) + (diff2_se^2 * mean^2) + (standard_error^2 * diff2^2))]

#recalculate SE of males/females - multiply by the recalculated SE from the MR-BRT model
bADDRESS_b[measure == "incidence", se_male := sqrt((1/male_ss)*(male_cases/male_ss)*(1-(male_cases/male_ss)))*inc3_se]
bADDRESS_b[measure == "incidence", se_fem := sqrt((1/fem_ss)*(fem_cases/fem_ss)*(1-(fem_cases/fem_ss)))*inc3_se]

##split data and append male and female records, merge back on sex split records from original bundle if necessary
males<-bADDRESS_b %>% 
  select(nid, location_id, year_start, year_end, age_start, group_review, case_definition, ihme_loc_id, group, unit_value_as_published, measure, recall_type, representative_name, source_type, unit_type, urbanicity_type, uncertainty_type_value, upper, lower, age_end, seq, is_outlier, case_name, bundle_id, origin_seq,cause_id,male_cases, mean_male, se_male, male_ss) %>%
  rename(mean = mean_male, cases = male_cases, standard_error=se_male, effective_sample_size=male_ss)

#need sex_id for age split code
males_ss <- males
males_ss$sex <- 'Male'
males_ss$sex_id <- 1

##changed standard_error to adjusted SE for females
# repeat for females
females<-bADDRESS_b %>% 
  select(nid, location_id, year_start, year_end, age_start, group_review, case_definition, ihme_loc_id, group, unit_value_as_published, measure, age_end, recall_type, representative_name, source_type, unit_type, urbanicity_type, uncertainty_type_value, upper, lower, seq, is_outlier, case_name, bundle_id, origin_seq,cause_id,fem_cases, inc2, se_fem, fem_ss) %>%
  rename(mean = inc2, cases = fem_cases, standard_error = se_fem, effective_sample_size=fem_ss)


females_ss <- females
females_ss$sex <- 'Female'
females_ss$sex_id <- 2

#append together
split_ADDRESS_b <- rbind(males_ss,females_ss)
split_ADDRESS_b$note_modeler<-"sexsplit"

write.csv(split_ADDRESS_b, "FILEPATH")

### add seq
split_ADDRESS_b[, crosswalk_parent_seq := seq]
#change seq to NA not missing as character string
split_ADDRESS_b[,seq:=NA]

###write csv here
write.csv(split_ADDRESS_b,"FILEPATH")