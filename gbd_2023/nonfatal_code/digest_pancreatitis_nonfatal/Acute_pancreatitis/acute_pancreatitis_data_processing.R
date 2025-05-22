######################################################################################################
######################################################################################################
## Purpose: this script retrieves new data, applies GBD 2019 crosswalk coefficients, and systematically 
##          outlier data based on median absoluate deviation values
######################################################################################################
######################################################################################################


## Source central functions
pacman::p_load(data.table, openxlsx, ggplot2, readr, RMySQL, openxlsx, readxl, stringr, tidyr, plyr, dplyr, gtools)
library(msm, lib.loc = FILEPATH)
library(Hmisc, lib.loc = FILEPATH)
library(metafor, lib.loc=FILEPATH)

base_dir <- FILEPATH
temp_dir <- FILEPATH
functions <- c( "get_bundle_version", "save_bundle_version", "get_crosswalk_version", "save_crosswalk_version",
               "get_age_metadata","get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))
lapply(paste0(base_dir, functions, ".R"), source)

date <- Sys.Date()
date <- gsub("-", "_", date)


#########################################################################################
##STEP 1: Retrieve all step 2 data
#########################################################################################
bundle_version_id= BUNDLE_VERSION_ID	 
df_all = get_bundle_version(bundle_version_id, fetch='all', export = FALSE)


#########################################################################################
##STEP 2: Set objects 
#########################################################################################
  #For xwalk
  df <- subset(df_all, is.na(cv_literature))
  df = within(df, {cv_marketscan = ifelse((nid==433114| nid== 244370 | nid== 336850
                                          | nid==244371| nid==336849| nid==336848
                                          | nid==336847| nid==408680), 1, 0)})
  df = within(df, {cv_ms2000 = ifelse(nid==244369, 1, 0)})
  df = within(df, {cv_hospital = ifelse(cv_marketscan==0 & cv_ms2000==0, 1, 0)})
  dt <- copy(as.data.table(df))
  xwalk_coef <- read.csv(FILEPATH)
  xwalk_clin_output_filepath <- FILEPATH
  xwalk_lit_output_filepath <- FILEPATH
  
  #For MAD outlier
  output_filepath <- FILEPATH
  MAD <- 3
  age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235 ) 
  byvars <- c("location_id", "sex", "year_start", "year_end", "nid") 
  
  
#########################################################################################
##STEP 3: Apply xwalk to all clinical admin data
#########################################################################################
dt_xwalk <- subset(dt, cv_marketscan ==1 | cv_ms2000==1)
dt_no_xwalk <- subset(dt, cv_hospital ==1)
cov_names <- c("cv_marketscan", "cv_ms2000") 

predicted <- as.data.table(subset(xwalk_coef, (X_cv_marketscan == 1 & X_cv_ms2000==0 & X_cv_chart_review==0) | (X_cv_marketscan == 0 & X_cv_ms2000==1 & X_cv_chart_review==0)))
setnames(predicted, c("X_cv_marketscan", "X_cv_ms2000"), c("cv_marketscan", "cv_ms2000"))
predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]


dt_xwalk <- merge(dt_xwalk, predicted, by=cov_names)
dt_xwalk[, `:=` (log_mean = log(mean), log_se = deltamethod(~log(x1), mean, standard_error^2)), by = c("mean", "standard_error")]
setnames(dt_xwalk, c("mean", "standard_error"), c("mean_orig", "standard_error_orig"))

dt_xwalk[, `:=` (log_mean = log_mean - Y_mean, log_se = sqrt(log_se^2 + Y_se^2))]
dt_xwalk[, `:=` (mean = exp(log_mean), standard_error = deltamethod(~exp(x1), log_mean, log_se^2)), by = c("log_mean", "log_se")]
dt_xwalk[, `:=` (cases = NA, lower = NA, upper = NA)]

dt_xwalk[, (c( "X_cv_ms2000", "Y_mean", "Y_se", "mean_logit", "se_logit", "Y_se_norm", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe", "Y_negp", "Z_intercept")) := NULL]
dt_xwalk[, (c("cv_marketscan_all_2015", "cv_marketscan_inp_2015",
              "cv_marketscan_inp_2000", "cv_marketscan_all_2000",
              "cv_marketscan_inp_2012", "cv_marketscan_all_2012", 
              "super_region_abbrev","region_name","super_region_name","cv_survey",
              "cv_marketscan_all_2013", "cv_marketscan_inp_2013",  
              "cv_marketscan_all_2014", "cv_marketscan_inp_2014",
                "cv_marketscan_inp_2011", "cv_marketscan_all_2011", 
               "cv_marketscan_data", "cv_inpatient",   "cv_taiwan_claims_data")) := NULL]

dt_xwalk[is.na(lower), uncertainty_type_value := NA]

dt_total_admin <- rbind.fill(dt_xwalk, dt_no_xwalk)
write.xlsx(dt_total_admin, xwalk_clin_output_filepath, sheetName = "extraction", col.names=TRUE)

#########################################################################################
##STEP 4: Apply xwalk to GBD 2019 lit data
#########################################################################################
dt_lit <- as.data.table(read.csv(FILEPATH))
dt_xwalk <- subset(dt_lit, cv_chart_review ==1)
dt_no_xwalk <- subset(dt_lit, cv_dx_admin_data ==1)

#fill in SE for OMAL, which had more than 5 cases
dt_xwalk[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
dt_xwalk[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]

cov_names <- c( "cv_chart_review") 
predicted <- as.data.table(subset(xwalk_coef, X_cv_marketscan == 0 & X_cv_ms2000==0 & X_cv_chart_review==1))
setnames(predicted, c("X_cv_chart_review"), c("cv_chart_review"))
predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]

dt_xwalk <- as.data.table(merge(dt_xwalk, predicted, by=cov_names))
dt_xwalk[, `:=` (log_mean = log(mean), log_se = deltamethod(~log(x1), mean, standard_error^2)), by = c("mean", "standard_error")]
setnames(dt_xwalk, c("mean", "standard_error"), c("mean_orig", "standard_error_orig"))

dt_xwalk[, `:=` (log_mean = log_mean - Y_mean, log_se = sqrt(log_se^2 + Y_se^2))]
dt_xwalk[, `:=` (mean = exp(log_mean), standard_error = deltamethod(~exp(x1), log_mean, log_se^2)), by = c("log_mean", "log_se")]
dt_xwalk[, `:=` (cases = NA, lower = NA, upper = NA)]

dt_xwalk[, (c( "X_cv_ms2000", "Y_mean", "Y_se", "mean_logit", "se_logit", "Y_se_norm", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe", "Y_negp", "Z_intercept")) := NULL]
dt_xwalk[, (c("cv_marketscan_all_2015", "cv_marketscan_inp_2015",
              "cv_marketscan_inp_2000", "cv_marketscan_all_2000",
              "cv_marketscan_inp_2012", "cv_marketscan_all_2012", 
              "super_region_abbrev","region_name","super_region_name","cv_survey",
              "cv_marketscan_all_2013", "cv_marketscan_inp_2013",  
              "cv_marketscan_all_2014", "cv_marketscan_inp_2014",
              "cv_marketscan_inp_2011", "cv_marketscan_all_2011", 
              "cv_marketscan_data", "cv_inpatient",   "cv_taiwan_claims_data")) := NULL]

dt_xwalk[is.na(lower), uncertainty_type_value := NA]
dt_total_lit <- rbind.fill(dt_xwalk, dt_no_xwalk)

write.xlsx(dt_total_lit, xwalk_lit_output_filepath, sheetName = "extraction", col.names=TRUE)


#########################################################################################
##STEP 5: age split
#########################################################################################
  dt <- as.data.table(copy(dt_total_lit))
  
  # GET OBJECTS -------------------------------------------------------------
  b_id <- BUNDLE_ID
  a_cause <- CAUSE_NAME
  name <- "Acute_pancreatitis"
  gbd_id <- MEID
  region_pattern <- F
  ages <- get_age_metadata(12, gbd_round_id=6)
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  location_pattern_id <- 1
  age_groups <- ages[age_start >= 5, age_group_id]
  age <- age_groups
  draws <- paste0("draw_", 0:999)
  

  ## FILL OUT MEAN/CASES/SAMPLE SIZE
  get_cases_sample_size <- function(raw_dt){
    dt <- copy(raw_dt)
    dt[is.na(mean), mean := cases/sample_size]
    dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
    dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
    return(dt)
  }
  
  ## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
  get_se <- function(raw_dt){
    dt <- copy(raw_dt)
    dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
    z <- qnorm(0.975)
    dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
    return(dt)
  }
  
  ## GET CASES IF THEY ARE MISSING
  calculate_cases_fromse <- function(raw_dt){
    dt <- copy(raw_dt)
    dt[is.na(cases) & is.na(sample_size) & measure == "proportion", sample_size := (mean*(1-mean)/standard_error^2)]
    dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
    dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
    dt[is.na(cases), cases := mean * sample_size]
    return(dt)
  }
  
  ## MAKE SURE DATA IS FORMATTED CORRECTLY
  format_data <- function(unformatted_dt, sex_dt){
    dt <- copy(unformatted_dt)
    dt[, `:=` (mean = as.numeric(mean), sample_size = as.numeric(sample_size), cases = as.numeric(cases),
               age_start = as.numeric(age_start), age_end = as.numeric(age_end), year_start = as.numeric(year_start))]
    dt <- dt[measure %in% c("proportion", "prevalence", "incidence"),] 
    dt <- dt[!group_review==0 | is.na(group_review),] ##don't use group_review 0
    dt <- dt[is_outlier==0,] ##don't age split outliered data
    dt <- dt[(age_end-age_start)>25 & cv_literature==1 ,] #for prevelance, incidence, proportion
    #dt1 <- dt[!mean == 0 & !cases == 0, ] ##don't split points with zero prevalence
    dt <- dt[(!mean == 0 & !cases == 0) |(!mean == 0 & is.na(cases))  , ] 
    dt <- merge(dt, sex_dt, by = "sex")
    dt[measure == "proportion", measure_id := 18]
    dt[measure == "prevalence", measure_id := 5]
    dt[measure == "incidence", measure_id := 6]
    
    dt[, year_id := round((year_start + year_end)/2, 0)] ##so that can merge on year later
    return(dt)
  }
  
  ## CREATE NEW AGE ROWS
  expand_age <- function(small_dt, age_dt = ages){
    dt <- copy(small_dt)
    
    ## ROUND AGE GROUPS
    dt[, age_start := age_start - age_start %%5]
    dt[, age_end := age_end - age_end %%5 + 4]
    dt <- dt[age_end > 99, age_end := 99]
    
    ## EXPAND FOR AGE
    dt[, n.age:=(age_end+1 - age_start)/5]
    dt[, age_start_floor:=age_start]
    dt[, drop := cases/n.age] 
    expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
    split <- merge(expanded, dt, by="id", all=T)
    split[, age.rep := 1:.N - 1, by =.(id)]
    split[, age_start:= age_start+age.rep*5]
    split[, age_end :=  age_start + 4]
    split <- merge(split, age_dt, by = c("age_start", "age_end"), all.x = T)
    split[age_start == 0 & age_end == 4, age_group_id := 1]
    split <- split[age_group_id %in% age | age_group_id == 1] ##don't keep where age group id isn't estimated for cause
    return(split)
  }
  
  ## GET DISMOD AGE PATTERN
  get_age_pattern <- function(locs, id, age_groups){
    age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, ## USING 2010 AGE PATTERN BECAUSE LIKELY HAVE MORE DATA FOR 2010
                             measure_id = c(6), location_id = locs, source = "epi", ##Measure ID 5= prev, 6=incidence, 18=proportion
                             version_id = 392678,  sex_id = c(1,2), gbd_round_id = 6, decomp_step = "iterative", #can replace version_id with status = "best" or "latest"
                             age_group_id = age_groups, year_id = 2010) ##imposing age pattern
    us_population <- get_population(location_id = locs, year_id = 2010, sex_id = c(1, 2),
                                    age_group_id = age_groups, decomp_step = "step2")
    us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]
    age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
    age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
    age_pattern[, (draws) := NULL]
    age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]
    
    ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
    age_1 <- copy(age_pattern)
    age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5), ]
    se <- copy(age_1)
    se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)]
    age_1 <- merge(age_1, us_population, by = c("age_group_id", "sex_id", "location_id"))
    age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
    age_1[, frac_pop := population / total_pop]
    age_1[, weight_rate := rate_dis * frac_pop]
    age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
    age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
    age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
    age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
    age_1[, age_group_id := 1]
    age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5)]
    age_pattern <- rbind(age_pattern, age_1)
    
    ## CASES AND SAMPLE SIZE
    age_pattern[measure_id == 18, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
    age_pattern[, cases_us := sample_size_us * rate_dis]
    age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
    age_pattern[is.nan(cases_us), cases_us := 0]
    
    ## GET SEX ID 3
    sex_3 <- copy(age_pattern)
    sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
    sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
    sex_3[, rate_dis := cases_us/sample_size_us]
    sex_3[measure_id == 18, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
    sex_3[is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0 can't calculate rate and standard error, but should both be 0
    sex_3[is.nan(se_dismod), se_dismod := 0]
    sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
    sex_3[, sex_id := 3]
    age_pattern <- rbind(age_pattern, sex_3)
    
    age_pattern[, super_region_id := location_id]
    age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
    return(age_pattern)
  }
  
  ## GET POPULATION STRUCTURE
  get_pop_structure <- function(locs, years, age_groups){
    populations <- get_population(location_id = locs, year_id = years,decomp_step = "step2",
                                  sex_id = c(1, 2, 3), age_group_id = age_groups)
    age_1 <- copy(populations) ##create age group id 1 by collapsing lower age groups
    age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5)]
    age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
    age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
    age_1[, age_group_id := 1]
    populations <- populations[!age_group_id %in% c(2, 3, 4, 5)]
    populations <- rbind(populations, age_1)  ##add age group id 1 back on
    return(populations)
  }
  
  ## SPLIT THE DATA
  split_data <- function(raw_dt){
    dt <- copy(raw_dt)
    dt[, total_pop := sum(population), by = "id"]
    dt[, sample_size := (population / total_pop) * sample_size]
    dt[, cases_dis := sample_size * rate_dis]
    dt[, total_cases_dis := sum(cases_dis), by = "id"]
    dt[, total_sample_size := sum(sample_size), by = "id"]
    dt[, all_age_rate := total_cases_dis/total_sample_size]
    dt[, ratio := mean / all_age_rate]
    dt[, mean := ratio * rate_dis ]
    dt <- dt[mean < 1, ]
    dt[, cases := mean * sample_size]
    return(dt)
  }
  
  ## FORMAT DATA TO FINISH
  format_data_forfinal <- function(unformatted_dt, location_split_id, region, original_dt){
    dt <- copy(unformatted_dt)
    dt[, group := 1]
    dt[, specificity := "age,sex"]
    dt[, group_review := 1]
   # dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
    blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
    dt[, (blank_vars) := NA]
    dt <- get_se(dt)
    # dt <- col_order(dt)
    if (region == T) {
      dt[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern", date)]
    } else {
      dt[, note_modeler := paste0(note_modeler, "| age split using the age pattern from location id ", location_split_id, " ", date)]
    }
    split_ids <- dt[, unique(id)]
    dt <- rbind(original_dt[!id %in% split_ids], dt, fill = T)
    dt <- dt[, c(names(df)), with = F]
    return(dt)
  }
  
  # AGE SPLIT FUNCTION -----------------------------------------------------------------------
  age_split <- function(gbd_id, df, age, region_pattern, location_pattern_id){
    
    ## GET TABLES
    sex_names <- get_ids(table = "sex")
    ages[, age_group_weight_value := NULL]
    ages[age_start >= 1, age_end := age_end - 1]
    ages[age_end == 124, age_end := 99]
    super_region_dt <- get_location_metadata(location_set_id = 22, gbd_round_id=6)
    super_region_dt <- super_region_dt[, .(location_id, super_region_id)]
    
    
    ## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQS)
    original <- copy(dt)
    original[, id := 1:.N]
    
    ## FORMAT DATA
    dt <- format_data(original, sex_dt = sex_names)
    dt <- get_cases_sample_size(dt)
    dt <- get_se(dt)
    dt <- calculate_cases_fromse(dt)
    
    ## EXPAND AGE
    split_dt <- expand_age(dt, age_dt = ages)
    
    ## GET PULL LOCATIONS
    if (region_pattern == T){
      split_dt <- merge(split_dt, super_region_dt, by = "location_id")
      super_regions <- unique(split_dt$super_region_id) ##get super regions for dismod results
      locations <- super_regions
    } else {
      locations <- location_pattern_id
    }
    
    ##GET LOCS AND POPS
    pop_locs <- unique(split_dt$location_id)
    pop_years <- unique(split_dt$year_id)
    
    ## GET AGE PATTERN
    print("getting age pattern")
    age_pattern <- get_age_pattern(locs = locations, id = gbd_id, age_groups = age) #set desired super region here if you want to specify
    
    if (region_pattern == T) {
      age_pattern1 <- copy(age_pattern)
      split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
    } else {
      age_pattern1 <- copy(age_pattern)
      split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
    }
    
    ## GET POPULATION INFO
    print("getting pop structure")
    
    pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
    split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))
    
    #####CALCULATE AGE SPLIT POINTS#######################################################################
    ## CREATE NEW POINTS
    print("splitting data")
    split_dt <- split_data(split_dt)
    
    
  }
  
  
  
  final_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                   original_dt = original)
  
  write.csv(final_dt,  FILEPATH)


#########################################################################################
##STEP 6: final appended dataset
#########################################################################################
dt_total <- as.data.table(rbind.fill(final_dt, dt_total_admin))
dt_total[, (c("cv_marketscan_all_2015", "cv_marketscan_inp_2015",
              "cv_marketscan_inp_2000", "cv_marketscan_all_2000",
              "cv_marketscan_inp_2012", "cv_marketscan_all_2012", 
              "super_region_abbrev","region_name","super_region_name","cv_survey",
              "cv_marketscan_all_2013", "cv_marketscan_inp_2013",  
              "cv_marketscan_all_2014", "cv_marketscan_inp_2014",
              "cv_marketscan_inp_2011", "cv_marketscan_all_2011", 
              "cv_marketscan_data", "cv_inpatient",   "cv_taiwan_claims_data")) := NULL]

#epi uploader validation
dt_total[is.na(unit_value_as_published), unit_value_as_published:=1]
dt_total[is.na(recall_type), recall_type:="Not Set"]
dt_total[recall_type=="", recall_type:="Not Set"]
dt_total[is.na(unit_type), unit_type:="Person"]
dt_total[unit_type=="", unit_type:="Person"]

dt_total[is.na(urbanicity_type), urbanicity_type:="Unknown"]
dt_total[urbanicity_type=="", urbanicity_type:="Unknown"]

dt_total[is.na(lower), uncertainty_type_value:=NA]
dt_total[!is.na(lower), uncertainty_type_value:=95]

#########################################################################################
##STEP 7: MAD outlier
#########################################################################################
  ## GET AGE WEIGHTS
  all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19)) 
  
  all_fine_ages[, age_start := age_group_years_start]
  
  ## Delete rows with emtpy means
  dt_total<- dt_total[!is.na(mean)]

  
  ##merge age table map and merge on to dataset
  dt_total <- as.data.table(merge(dt_total, all_fine_ages, by = c("age_start")))
  
  #calculate age-standardized prevalence/incidence
  
  ##create new age-weights for each data source
  dt_total <- dt_total[, sum1 := sum(age_group_weight_value), by = byvars] #sum of standard age-weights for all the ages we are using, by location-age-sex-nid, sum will be same for all age-groups and possibly less than one 
  dt_total <- dt_total[, new_weight1 := age_group_weight_value/sum1, by = byvars] #divide each age-group's standard weight by the sum of all the weights in their locaiton-age-sex-nid group 
  
  ##age standardizing per location-year by sex
  #add a column titled "age_std_mean" with the age-standardized mean for the location-year-sex-nid
  dt_total[, as_mean := mean * new_weight1] #initially just the weighted mean for that AGE-location-year-sex-nid
  dt_total[, as_mean := sum(as_mean), by = byvars] #sum across ages within the location-year-sex-nid group, you now have age-standardized mean for that series
  
  ##mark as outlier if age standardized mean is 0 (either biased data or rare disease in small ppln)
  dt_total[as_mean == 0, is_outlier := 1] 
  dt_total$note_modeler <- as.character(dt_total$note_modeler)
  dt_total[as_mean == 0, note_modeler := paste0(note_modeler, " | GBD 2021, outliered this location-year-sex-NID age-series because age standardized mean is 0")]
  
  ## log-transform to pick up low outliers
  dt_total[as_mean != 0, as_mean := log(as_mean)]
  
  # calculate median absolute deviation
  dt_total[as_mean == 0, as_mean := NA] ## don't count zeros in median calculations
  dt_total[,mad:=mad(as_mean,na.rm = T),by=c("sex")]
  dt_total[,median:=median(as_mean,na.rm = T),by=c("sex")]
  
  
  #***can change number of MAD to mark here
  dt_total[as_mean>((MAD*mad)+median), is_outlier := 1]
  dt_total[as_mean>((MAD*mad)+median), note_modeler := paste0(note_modeler, " | GBD 2021, outliered because log age-standardized mean for location-year-sex-NID is higher than ", MAD, " MAD above median")]
  dt_total[as_mean<(median-(MAD*mad)), is_outlier := 1]
  dt_total[as_mean<(median-(MAD*mad)), note_modeler := paste0(note_modeler, " | GBD 2021, outliered because log age-standardized mean for location-year-sex-NID is lower than ", MAD, " MAD below median")]
  dt_total[, c("age_group_name",	"age_group_years_start",	"age_group_years_end",	"most_detailed",	"sum1",	"new_weight1","sum", "new_weight", "as_mean", "median", "mad", "age_group_weight_value", "age_group_id") := NULL]
  
  
  
  
  ## create a new variable
  dt_total$crosswalk_parent_seq[dt_total$cv_marketscan==1 |dt_total$cv_chart_review==1 | dt_total$cv_ms2000==1] <- dt_total$seq #only when adjustment was made to data
  dt_total$crosswalk_parent_seq[dt_total$cv_hospital==1] <-  NA #when no adjustment was made to data
  
  ## fix SE that are greater than 1
  dt_total$standard_error[dt_total$standard_error>1] <-1
  dt_total$standard_error[dt_total$standard_error<0] <-0
  
  write.xlsx(dt_total, output_filepath, sheetName = "extraction", col.names=TRUE)
  
###################################################################################################33
##STEP 8: Upload the xwalk version
###################################################################################################33
  
  path_to_data <- output_filepath
  description <- DESCRIPTION
  result <- save_crosswalk_version( bundle_version_id, path_to_data, description=description)
  