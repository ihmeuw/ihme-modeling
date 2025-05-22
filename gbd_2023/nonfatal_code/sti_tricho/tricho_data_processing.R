################################################################################################
## DATA PROCESSING
################################################################################################

date <- gsub("-", "_", Sys.Date())
j <- "FILEPATH"
h_root <- "FILEPATH"
j_work <- "FILEPATH"
xwalk_temp <- "FILEPATH"
out_dir <- "FILEPATH"
out_dir

#SET UP-------------------------------------------------------------------------------------------------------------------
source(paste0(h_root,"FILEPATH"))
source("FILEPATH")
source("FILEPATH")
source_shared_functions("get_bundle_version")
require(data.table)
require(stringr)
require(magrittr)
require(assertthat)
library(dplyr)
library(data.table)
library(metafor, lib.loc = "FILEPATH")
library(msm, lib.loc = "FILEPATH")
library(readxl)
library(openxlsx)
library(metafor)
pacman::p_load(data.table, openxlsx, ggplot2)
library(mortdb, lib = "FILEPATH")
library(msm, lib.loc = "FILEPATH")
library(Hmisc, lib.loc = paste0(j, "FILEPATH"))
library(ggplot2)
library(ggridges, lib = "FILEPATH")

draws <- paste0("draw_", 0:999)


#PREP INPUT DATA -------------------------------------------------------------------------------------------------------------------
get_data <- function(fpath){
  dat_original <- data.table(read_excel(path = fpath, guess_max = 1048576))
  dt <- dat_original[!(is.na(nid)), ]
  dt_prev <- dt[measure == "prevalence",]
  dt_prev <- dt_prev[sex != "Both", ]
  return(list(dt = dt, dt_prev = dt_prev))
}

get_db_data <- function(bvid){
  data <- get_bundle_version(bvid, export = FALSE, transform = TRUE)
  dt_prev <- data[sex != "Both" & measure == "prevalence"]
  return(list(dt = data, dt_prev = dt_prev))
}

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
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

#GET SEX RATIOS
find_sex_match <- function(dt){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence")]
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end", cv_keep) #, cv_keep
  sex_dt[, match_n := .N, by = match_vars]
  sex_dt <- sex_dt[match_n == 2]
  keep_vars <- c(match_vars, "sex", "mean","standard_error")
  sex_dt[, id := .GRP, by = match_vars]
  sex_dt <- dplyr::select(sex_dt, keep_vars)
  sex_dt <- dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate = mean, na.rm = TRUE) 
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0]
  sex_dt[, id := .GRP, by = c("nid", "location_id")]
  return(sex_dt)
}
calc_sex_ratios <- function(dt){
  ratio_dt <- copy(dt)
  ratio_dt[, midage := (age_start + age_end)/2]
  ratio_dt[, `:=` (ratio = mean_Female/mean_Male,
                   ratio_se = sqrt((mean_Female^2 / mean_Male^2) * (standard_error_Female^2/mean_Female^2 + standard_error_Male^2/mean_Male^2)))]
  ratio_dt[, log_ratio := log(ratio)]
  ratio_dt$log_se <- sapply(1:nrow(ratio_dt), function(i) {
    mean_i <- ratio_dt[i, "ratio"]
    se_i <- ratio_dt[i, "ratio_se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  ratio_dt <- ratio_dt[!is.nan(ratio_se), ]
  return(ratio_dt)
}



###########################################################
## INTRASTUDY AGE SEX SPLITTING
###########################################################


age_sex_split <- function(dt){
  notsplit <- as.data.table(subset(dt, age_sex_split == "" | is.na(age_sex_split)))
  dt_split <- as.data.table(subset(dt, age_sex_split!=""))
  dt_split[, note_modeler := as.character(note_modeler)]
  dt_split[, specificity := as.character(specificity)]
  print(paste0(nrow(dt_split), " rows in the dt_split"))
  print("Sanity check: The sum of rows in all subsets should be equal to the rows in dt_split.")
  
  #fill in cases and sample size for age-sex splitting
  dt_split[is.na(sample_size), note_modeler := paste0(note_modeler, " | sample size back calculated to age sex split")]
  dt_split[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  dt_split[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt_split[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  dt_split[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  dt_split[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  dt_split[is.na(cases), cases := sample_size * mean]
  dt_split <- dt_split[!is.na(cases),]
  
  #SEX SPLITTING --------------------------------------------------
  #subset to data needing age-sex splitting
  tosplit_sex <- dt_split[age_sex_split =="tosplit" & sex == "Both"] 
  # print the number of rows in this subset
  print(paste0(nrow(tosplit_sex), " rows in the tosplit_sex"))
  
  #subset the rows with sex-specific data
  sex <- dt_split[age_sex_split=="sexvalue" & year_specific==""]
  sex_yr <- dt_split[age_sex_split=="sexvalue" & year_specific=="yes"]
  setnames(sex_yr, c("cases", "sample_size"),  c("cases1", "sample_size1"))
  sex_yr <- sex_yr[,  c("nid", "sex", "measure", "age_sex_split", "location_id", "cases1", "sample_size1", "year_specific", "year_start", "year_end")]
  print(paste0(nrow(sex), " rows in the sex df"))
  print(paste0(nrow(sex_yr), " rows in the sex_yr df"))
  
  #sum female and male-specific data points from the same study but across multiple years to calculate pooled sex ratio
  sex[, cases1:= sum(cases), by = list(nid, sex, measure, location_id)]
  sex[, sample_size1:= sum(sample_size), by = list(nid, sex, measure, location_id)]
  sex <- sex[,  c("nid", "sex", "measure", "age_sex_split", "location_id", "cases1", "sample_size1", "year_specific")]
  sex <- unique(sex)
  
  #calculate proportion of cases male and female
  sex <- as.data.table(sex)
  sex[, cases_total:= sum(cases1), by = list(nid,  measure)]
  sex[, prop_cases := round(cases1 / cases_total, digits = 3)]
  
  #calculate proportion of sample male and female
  sex[, ss_total:= sum(sample_size1), by = list(nid, measure)]
  sex[, prop_ss := round(sample_size1 / ss_total, digits = 3)]
  
  #calculate standard error of % cases & sample_size M and F
  sex[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  sex[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  #estimate ratio & standard error of ratio % cases : % sample
  sex[, ratio := round(prop_cases / prop_ss, digits = 3)]
  sex[, se_ratio:= round(sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) ), digits = 3)]
  
  #calculate proportion of cases male and female
  sex_yr <- as.data.table(sex_yr)
  sex_yr[, cases_total:= sum(cases1), by = list(nid,  measure, year_start, year_end)]
  sex_yr[, prop_cases := round(cases1 / cases_total, digits = 3)]
  
  #calculate proportion of sample male and female
  sex_yr[, ss_total:= sum(sample_size1), by = list(nid, measure, year_start, year_end)]
  sex_yr[, prop_ss := round(sample_size1 / ss_total, digits = 3)]
  
  #calculate standard error of % cases & sample_size M and F
  sex_yr[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  sex_yr[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  #estimate ratio & standard error of ratio % cases : % sample
  sex_yr[, ratio := round(prop_cases / prop_ss, digits = 3)]
  sex_yr[, se_ratio:= round(sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) ), digits = 3)]
  
  
  #Create age,sex observations 
  tosplit_sex[,specificity := paste(specificity, ",sex")]
  tosplit_sex[, seq := NA]
  male <- copy(tosplit_sex[, sex := "Male"]) #create a copy of data table with sex specified as male
  female <- copy(tosplit_sex[, sex := "Female"]) #create a copy of data table with sex specified as female
  tosplit_sex <- rbind(male, female)
  
  #Merge sex ratios to age,sex observations
  tosplit_sex1 <- merge(tosplit_sex, sex, by = c("nid", "sex", "measure", "location_id"))
  tosplit_sex2 <- merge(tosplit_sex, sex_yr, by = c("nid", "sex", "measure", "location_id", "year_start", "year_end"))
  tosplit_sex <- as.data.table(rbind.fill(tosplit_sex1,tosplit_sex2))
  
  #calculate age-sex specific mean, standard_error, cases, sample_size
  tosplit_sex[, standard_error1 := (sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2))]
  tosplit_sex[, mean1 := mean * ratio]
  tosplit_sex[, cases1 := round(cases * prop_cases, digits = 0)]
  tosplit_sex[, sample_size1 := round(sample_size * prop_ss, digits = 0)]
  tosplit_sex[, `:=` (upper = NA, lower = NA, uncertainty_type_value= NA)]
  
  tosplit_sex[,note_modeler := paste(note_modeler, "| sex split using sex ratio", round(ratio, digits = 2))]
  tosplit_sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss","age_sex_split.x", "age_sex_split.y", "ss_total", "se_cases", "se_ss", "cases1", "sample_size1", "cases_total") := NULL]
  
  #clean sex-specific data points that we want to append to final dataset
  sex_append <- dt_split[age_sex_split=="sexvalue"]
  sex_append <- subset(sex_append, group_review==1)
  
  final_sex <- rbind.fill(tosplit_sex, sex_append)
  
  #AGE SPLITTING --------------------------------------------------
  #subset to data needing age splitting
  tosplit_age <- dt_split[age_sex_split =="tosplit" & sex != "Both"] 
  print(paste0(nrow(tosplit_age), " rows in the tosplit_age"))
  
  #subset the rows with age-specific data
  age <- as.data.table(dt_split[(age_sex_split=="agevalue") & year_specific==""])
  print(paste0(nrow(age), " rows in the age df"))
  
  #sum age-specific data points from the same study but across multiple years to calculate pooled sex ratio
  age[, cases1:= sum(cases), by = list(nid, age_start, age_end, sex, measure, location_id)]
  age[, sample_size1:= sum(sample_size), by = list(nid, age_start, age_end, sex, measure, location_id)]
  age <- age[,  c("nid", "age_start", "age_end", "sex", "measure", "age_sex_split", "location_id", "cases1", "sample_size1", "year_specific")]
  age <- unique(age)
  
  #calculate proportion of cases for each age group
  age <- as.data.table(age)
  age[, cases_total:= sum(cases1), by = list(nid,  measure, sex)]
  age[, prop_cases := round(cases1 / cases_total, digits = 3)]
  
  #calculate proportion of sample male and female
  age[, ss_total:= sum(sample_size1), by = list(nid, measure, sex)]
  age[, prop_ss := round(sample_size1 / ss_total, digits = 3)]
  
  #calculate standard error of % cases & sample_size M and F
  age[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  age[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  #estimate ratio & standard error of ratio % cases : % sample
  age[, ratio := round(prop_cases / prop_ss, digits = 3)]
  age[, se_ratio:= round(sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) ), digits = 3)]
  
  #Create age,sex observations 
  age_nid <- unique(age$nid)
  tosplit_age <- tosplit_age[nid %in% age_nid]
  tosplit_age[,specificity := paste(specificity, ",age")]
  tosplit_age[, seq := NA]
  tosplit_age[, c("age_start", "age_end")] <- NULL
  
  #Merge sex ratios to age,sex observations
  age_spec <- subset(age)
  age<- subset(age)
  age$sex <- NULL
  split_age <- merge(tosplit_age, age, by = c("nid", "location_id", "measure"),  allow.cartesian = T)
  split_age_spec <- merge(tosplit_age, age_spec, by = c("nid", "location_id", "measure", "sex"),   allow.cartesian = T)
  
  split_age <- as.data.table(rbind.fill(split_age,split_age_spec))
  
  #calculate age-sex specific mean, standard_error, cases, sample_size
  split_age[, standard_error := (sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2))]
  split_age[, mean := mean * ratio]
  split_age[, cases := round(cases * prop_cases, digits = 0)]
  split_age[, sample_size := round(sample_size * prop_ss, digits = 0)]
  split_age[, `:=` (upper = NA, lower = NA)]
  
  split_age[,note_modeler := paste(note_modeler, "| age split using age ratio", round(ratio, digits = 2))]
  split_age[,c("ratio", "se_ratio", "prop_cases", "prop_ss","age_sex_split.x", "age_sex_split.y", "ss_total", "se_cases", "se_ss", "cases1", 
              "sample_size1", "cases_total", "year_specific.y","year_start.x", "year_start.y", "year_end.x", "year_end.y", "age_sex_split", "year_specific", "year_specific.x") := NULL]
  
  #clean sex-specific data points that we want to append to final dataset
  age_append <- dt_split[age_sex_split=="agevalue" | age_sex_split == "agesexvalue"]
  age_append <- subset(age_append, group_review==1)
  
  within_split <- as.data.table(rbind.fill(final_sex, split_age, age_append))
  
  within_split[,c("ratio", "se_ratio", "prop_cases", "prop_ss","age_sex_split.x", "age_sex_split.y", "ss_total", "se_cases", "se_ss", "cases1", "
                  sample_size1", "cases_total", "year_specific.y","year_start.x", "year_start.y", "year_end.x", "year_end.y", "age_sex_split", "year_specific", "year_specific.x") := NULL]
  
  within_split  <- subset(within_split, standard_error != "NaN")
  notsplit  <- subset(notsplit, standard_error != "NaN")
  
  ##return the prepped data
  return(list(within_split = within_split, notsplit_original_data = notsplit))
}

split <- age_sex_split(dt)
within_split <- split$within_split
notsplit_original_data <- split$notsplit_original_data

dt_prev <- as.data.table(rbind.fill(within_split, notsplit_original_data))


###########################################################
## SEX SPLITTING
###########################################################

cv_drop <- ""
sex_dt <- as.data.table(copy(dt))
sex_dt <- get_cases_sample_size(sex_dt)
sex_dt <- get_se(sex_dt)
sex_dt <- calculate_cases_fromse(sex_dt)

message("There are ", nrow(sex_dt[sample_size == 0,]), " rows of data with sample size zero")
sex_dt <- sex_dt[sample_size != 0, ]

## find sex matches
find_sex_match <- function(dt){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence", "incidence")]
  match_vars <- c( "location_name", "nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end",
                   names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
  sex_dt[, match_n := .N, by = match_vars] #finding female-male pairs within each study, matched on measure, year, age, location_name
  sex_dt <- sex_dt[match_n >1] 
  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt[, id := .GRP, by = match_vars] 
  sex_dt <- dplyr::select(sex_dt, keep_vars) 
  sex_dt<-data.table::dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate = mean) #reshape long to wide, to match male to female, rename mean and se with "x_Female" and "x_Male"
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0] #drop where sex-specific mean is zero -> not informative 
  sex_dt[, id := .GRP, by = c("nid", "location_id")] #re-number based on nid and location id
  sex_dt$dorm_alt <- "Female"
  sex_dt$dorm_ref <- "Male"
  return(sex_dt)
}

# sex_matches <- find_sex_match(sex_dt)
sex_matches <- find_sex_match(sex_dt %>% filter(is_outlier != 1))
length(unique(sex_matches$nid))


#calculate logit difference between sex-specific mean estimates: female alternative; male reference-----
# the functions
#1. logit transform mean
sex_matches[, c("mean_alt_Female", "mean_se_alt_Female")] <- cw$utils$linear_to_logit(
  mean = array(sex_matches$mean_Female), 
  sd = array(sex_matches$standard_error_Female))

sex_matches[, c("mean_ref_Male", "mean_se_ref_Male")] <- cw$utils$linear_to_logit(
  mean = array(sex_matches$mean_Male), 
  sd = array(sex_matches$standard_error_Male))

#2. calculate logit(prev_alt) - logit(prev_ref)
sex_matches <- sex_matches %>%
  mutate(
    logit_diff = mean_alt_Female - mean_ref_Male,
    logit_diff_se = sqrt(mean_se_alt_Female^2 + mean_se_ref_Male^2))

#3. create study_id 
sex_matches <- as.data.table(sex_matches)
sex_matches[, id := .GRP, by = c("nid")]

# FORMAT DATA FOR MRBRT -----------------------------------------------------------------------------
formatted_data <- cw$CWData(df = sex_matches,
                            obs = "logit_diff",   # matched differences in logit space
                            obs_se = "logit_diff_se",  # SE of matched differences in logit space
                            alt_dorms = "dorm_alt",   # var for the alternative def/method
                            ref_dorms = "dorm_ref",   # var for the reference def/method
                            covs = list(),            # list of (potential) covariate column names
                            study_id = "id"  )     # var for random intercepts; i.e. (1|study_id)
# RUN MR-BRT -----------------------------------------------------------------------------
#cw$CWModel() runs mrbrt model
## LAUNCH MR-BRT SEX-MODEL ------------------------------------
sex_results <- cw$CWModel(cwdata = formatted_data,           # result of CWData() function call
                          obs_type = "diff_logit",                     # must be "diff_logit" or "diff_log"
                          cov_models = list(                         # specify covariate details
                            cw$CovModel("intercept")),
                          gold_dorm = "Male")                         # level of 'ref_dorms' that's the gold standard

sex_results$fit(inlier_pct = 0.9
) #trimming

## VIEW MR-BRT MODEL OUTPUTS -------------------------------------------------------------------
results_sex <- sex_results$create_result_df() %>%
  select(dorms, cov_names, beta, beta_sd, gamma)

## SEX SPLIT -----------------------------------------------------------------------
full_dt <- as.data.table(copy(dt_prev))
full_dt$crosswalk_parent_seq <- ""
full_dt <- full_dt[!(sex == "Both" & group_review == 0),]

# call the function
split_both_sex <- function(full_dt, sex_results) {
  
  offset <- min(full_dt[full_dt$mean>0,mean])/2
  
  full_dt <- get_cases_sample_size(full_dt)
  full_dt <- get_se(full_dt)
  full_dt <- calculate_cases_fromse(full_dt)
  n <- names(full_dt)
  
  both_sex <- full_dt[sex == "Both" & measure == measure]
  both_sex[, sex_dummy := "Female"]
  both_zero_rows <- nrow(both_sex[mean == 0, ]) #Both sex, zero mean data 
  message(paste0("There are ", both_zero_rows, " data points that will be offset by ", offset))
  both_sex[mean == 0, mean := offset]  # apply offset to mean zero data points 
  sex_specific <- copy(full_dt[sex != "Both"]) # sex specific data before sex splitting 
  
  #Adjust mean and standard error in both_sex using beta coeff and sd from sex_results. 
  # Ref_vals_mean and ref_vals_sd are already post-adjustment and exponentiated to linear space
  both_sex[, c("ref_vals_mean", "ref_vals_sd", "pred_diff_mean", "pred_diff_se", "data_id")] <- sex_results$adjust_orig_vals(
    df = both_sex,            # original data with obs to be adjusted
    orig_dorms = "sex_dummy", # name of column with (all) def/method levels
    orig_vals_mean = "mean",  # original mean
    orig_vals_se = "standard_error"  # standard error of original mean
  ) 
  
  logit_ratio_mean <- both_sex$pred_diff_mean
  logit_ratio_se <- sqrt(both_sex$pred_diff_se^2 + as.numeric(sex_results$gamma))
  
  adjust_both <- copy(both_sex)
  
  ## To return later
  adjust_both[, year_mid := (year_start + year_end)/2]
  adjust_both[, year_floor := floor(year_mid)]
  adjust_both[, age_mid := (age_start + age_end)/2]
  adjust_both[, age_floor:= floor(age_mid)]
  
  ## Pull out population data. We need age- sex- location-specific population.
  message("Getting population")
  pop <- get_population(age_group_id = "all", sex_id = "all", year_id = unique(floor(adjust_both$year_mid)),
                        location_id=unique(adjust_both$location_id), single_year_age = T, release_id = release_id)
  ids <- get_ids("age_group") ## age group IDs
  pop <- merge(pop, ids, by="age_group_id", all.x=T, all.y=F)
  pop$age_group_name <- as.numeric(pop$age_group_name)
  pop <- pop[!(is.na(age_group_name))]
  pop$age_group_id <- NULL
  
  ## Merge in populations for both-sex and each sex.
  adjust_both <- merge(adjust_both, pop[sex_id==3,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
  setnames(adjust_both, "population", "population_both")
  adjust_both <- merge(adjust_both, pop[sex_id==1,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
  setnames(adjust_both, "population", "population_male")
  adjust_both <- merge(adjust_both, pop[sex_id==2,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
  setnames(adjust_both, "population", "population_female")
  
  ## Take mean, SEs into real-space to combine and make adjustments
  adjust_both[, se_val  := unique(sqrt(both_sex$pred_diff_se^2 + as.vector(sex_results$gamma)))]
  adjust_both[, c("real_pred_mean", "real_pred_se")] <-cw$utils$logit_to_linear(mean = array(adjust_both$pred_diff_mean), 
                                                                              sd = array(adjust_both$se_val))
  
  # Subtract the offset before making any adjustments
  adjust_both[mean == paste0(offset), mean := 0 ] 
  
  ## Make adjustments. See documentation for rationale behind algebra.
  adjust_both[, m_mean := mean * (population_both/(population_male + real_pred_mean * population_female))]
  adjust_both[, f_mean := real_pred_mean * m_mean]
  
  ## Get combined standard errors
  adjust_both[, m_standard_error := sqrt((real_pred_se^2 * standard_error^2) + (real_pred_se^2 * mean^2) + (standard_error^2 * real_pred_mean^2))]
  adjust_both[, f_standard_error := sqrt((real_pred_se^2 * standard_error^2) + (real_pred_se^2 * mean^2) + (standard_error^2 * real_pred_mean^2))]
  
  ## Adjust the standard error of mean 0 data points 
  adjust_both[mean == 0, `:=` (m_standard_error = sqrt(2)*standard_error, f_standard_error = sqrt(2)*standard_error)]
  
  ## Make male- and female-specific dts
  message("Getting male and female specific data tables")
  male_dt <- copy(adjust_both)
  male_dt[, `:=` (mean = m_mean, standard_error = m_standard_error, upper = NA, lower = NA,
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", real_pred_mean, " (",
                                        real_pred_se, ")"))]
  male_dt <- dplyr::select(male_dt, all_of(n))
  female_dt <- copy(adjust_both)
  female_dt[, `:=` (mean = f_mean, standard_error = f_standard_error, upper = NA, lower = NA,
                    cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", real_pred_mean, " (",
                                          real_pred_se, ")"))]
  female_dt <- dplyr::select(female_dt, all_of(n))
  dt_all <- rbindlist(list(sex_specific, female_dt, male_dt))
  
  sex_specific_data <- copy(dt_all)
  drop_nids <- setdiff(full_dt[sex == "Both" & measure == measure, unique(nid)], adjust_both$nid)
  message("Dropped nids:", list(drop_nids))
  
  message("Finished sex splitting.")
  return(list(final = sex_specific_data, graph = adjust_both, missing_nids = drop_nids))
}

# Run sex-split function
prev_sex_split <- split_both_sex(full_dt, sex_results)
final_sex_prev <- copy(prev_sex_split$final)
graph_sex_prev <- copy(prev_sex_split$graph)


###########################################################
#DIAGNOSTIC CROSSWALK 
###########################################################

## EXTRACTED VALIDATION STUDIES
orig_dt <- data.table(read.xlsx("FILEPATH"))

# MATCH-FINDING ----------------------------------------------------------

#fill in missing variables
orig_dt <- get_cases_sample_size(orig_dt)
orig_dt <- get_se(orig_dt)
orig_dt <- calculate_cases_fromse(orig_dt)

##create "obs_method" variable for reference and alternatives
orig_dt[(cv_diag_wet_mount==1), `:=` (obs_method = "wetmount")]
orig_dt[(cv_diag_culture==1), `:=` (obs_method = "culture")]
orig_dt[(cv_naat==1), `:=` (obs_method = "naat")]

## create a column "is_reference" for pcr: 1 for pcr but 0 for other
orig_dt$is_reference <- ifelse(orig_dt$obs_method=="naat", 1, 0)

orig_dt <- orig_dt[, orig_mean:=mean]

## Create subsets for data
orig_dt <- orig_dt[, mean:=orig_mean]
#naat
orig_dt_ref <- subset(orig_dt, cv_naat==1)
orig_dt_ref <- orig_dt_ref[, c("id", "field_citation_value", "sex", "nid", "input_type", "source_type", "location_id", "location_name", "year_start", "year_end", "age_start", "age_end",  "measure",
                       "cv_naat", "cv_diag_culture", "mean","group_review", "standard_error", "extractor", "is_outlier","obs_method")]
orig_dt_ref <- plyr::rename(orig_dt_ref, c("mean" = "mean_ref", "standard_error"="std_error_ref"))
#alt 1: culture
orig_dt_cult <- subset(orig_dt, cv_diag_culture==1)
orig_dt_cult <- orig_dt_cult[, c("id", "field_citation_value", "sex", "nid","input_type", "source_type", "location_id", "location_name", "year_start", "year_end", "age_start", "age_end",  "measure",
                       "cv_naat", "cv_diag_culture", "mean","group_review", "standard_error", "extractor", "is_outlier", "obs_method")]
orig_dt_cult <- plyr::rename(orig_dt_cult, c("mean"="mean_alt", "standard_error"="std_error_alt"))
#alt 2: wetmount
orig_dt_wm <- subset(orig_dt, cv_diag_wet_mount==1)
orig_dt_wm <- orig_dt_wm[, c("id", "field_citation_value", "sex", "nid","input_type", "source_type", "location_id", "location_name", "year_start", "year_end", "age_start", "age_end",  "measure",
                                 "cv_naat", "cv_diag_culture", "mean","group_review", "standard_error", "extractor", "is_outlier", "obs_method")]
orig_dt_wm <- plyr::rename(orig_dt_wm, c("mean"="mean_wm", "standard_error"="std_error_wm"))

#####################################################################################################################################################
# within-study matches. Will transform into logit space in the second half of this code rather than here
match_orig_dt <- merge(orig_dt_ref, orig_dt_cult, by=c("id", "field_citation_value", "sex", "nid", "measure", "location_id", "year_start", "year_end", "age_start", "age_end"))
match_orig_dt <- subset(match_orig_dt, measure=="prevalence") 
match_orig_dt <- match_orig_dt[!is.na(match_orig_dt$mean_ref)]
setnames(match_orig_dt, "obs_method.x", "ref_dorms")
setnames(match_orig_dt, "obs_method.y", "alt_dorms")

# ref and wet mount
match_orig_wm <- merge(orig_dt_ref, orig_dt_wm, by=c("id", "field_citation_value", "sex", "nid", "measure", "location_id", "year_start", "year_end", "age_start", "age_end"))
match_orig_wm <- subset(match_orig_wm, measure=="prevalence") 
match_orig_wm <- match_orig_wm[!is.na(match_orig_wm$mean_ref)]
setnames(match_orig_wm, "obs_method.x", "ref_dorms")
setnames(match_orig_wm, "obs_method.y", "alt_dorms")
setnames(match_orig_wm, "mean_wm", "mean_alt")
setnames(match_orig_wm, "std_error_wm", "std_error_alt")

# culture vs  wetmount
match_orig_cult_wm <- merge(orig_dt_cult, orig_dt_wm, by=c("id", "field_citation_value", "sex", "nid", "measure", "location_id", "year_start", "year_end", "age_start", "age_end"))
match_orig_cult_wm <- subset(match_orig_cult_wm, measure=="prevalence") 
match_orig_cult_wm <- match_orig_cult_wm[!is.na(match_orig_cult_wm$mean_alt)]
setnames(match_orig_cult_wm, "obs_method.x", "ref_dorms")
setnames(match_orig_cult_wm, "obs_method.y", "alt_dorms")
setnames(match_orig_cult_wm, "mean_alt", "mean_ref")
setnames(match_orig_cult_wm, "std_error_alt", "std_error_ref")
setnames(match_orig_cult_wm, "mean_wm", "mean_alt")
setnames(match_orig_cult_wm, "std_error_wm", "std_error_alt")

#merge data set
match_orig_fin <- bind_rows(match_orig_dt, match_orig_wm, match_orig_cult_wm)

write.csv(match_orig_fin)

#####################################################################################################################################################
################################################################################################################################################
# convert ratios to logit space
# using within-study matches for test
# Construct the file path
file_path <- file.path("FILEPATH")
# Read the CSV file into a data table format data frame
df <- as.data.table(read.csv(file_path))

cv_orig_dt <- copy(df)

# calculate logit-transformed means and sds for reference and alternative using cw$utils$linear_to_logit from "crosswalk" package
# [1] represents first returned parameter, which is mean; [2] represents second returned parameter, which is sd
cv_orig_dt$mean_logit_ref <-cw$utils$linear_to_logit(mean = array(cv_orig_dt$mean_ref), sd = array(cv_orig_dt$std_error_ref))[[1]]
cv_orig_dt$sd_logit_ref <- cw$utils$linear_to_logit(mean = array(cv_orig_dt$mean_ref), sd = array(cv_orig_dt$std_error_ref))[[2]]

cv_orig_dt$mean_logit_alt <-cw$utils$linear_to_logit(mean = array(cv_orig_dt$mean_alt), sd = array(cv_orig_dt$std_error_alt))[[1]]
cv_orig_dt$sd_logit_alt <- cw$utils$linear_to_logit(mean = array(cv_orig_dt$mean_alt), sd = array(cv_orig_dt$std_error_alt))[[2]]

# take difference of logit-transformed means (same mathematically as logit ratio), and calculate sd of difference in logit space
calculate_diff <- function(cv_orig_dt, alt_mean, alt_sd, ref_mean, ref_sd) {
  dat_diff <- data.table(mean_alt = cv_orig_dt[, mean_logit_alt], mean_se_alt = cv_orig_dt[, sd_logit_alt], 
                         mean_ref = cv_orig_dt[, mean_logit_ref], mean_se_ref = cv_orig_dt[, sd_logit_ref])
  dat_diff[, `:=` (diff = mean_alt-mean_ref, diff_se = sqrt(mean_se_alt^2 + mean_se_ref^2))] 
  return(dat_diff[,c("diff", "diff_se")])
}

diff <- calculate_diff(
  cv_orig_dt, alt_mean = "mean_logit_alt", alt_sd = "sd_logit_alt",
  ref_mean = "mean_logit_ref", ref_sd = "sd_logit_ref"
)

names(diff) <- c("logit_diff_mean", "logit_diff_se")
cv_orig_dt <- cbind(cv_orig_dt, diff)

################################################################################################################################################
# now we have df with all needed columns. run mr-brt using logit difference

# create crosswalk dataframe
dat1 <- cw$CWData(
  df = cv_orig_dt,
  obs = "logit_diff_mean",
  obs_se = "logit_diff_se",
  alt_dorms = "alt_dorms",
  ref_dorms = "ref_dorms",
  covs = list(),
  study_id = "nid")

# create crosswalk model
fit1 <- cw$CWModel(
  cwdata = dat1, 
  obs_type = "diff_logit",
  cov_models = list(cw$CovModel("intercept")),
  gold_dorm = "naat")

# fit model
fit1$fit( #inlier_pct=0.9 # optionally can include trimming here
  )

# create table of results, betas/dorms/cov_names/gammas
results <- fit1$create_result_df() %>%
  select(dorms, cov_names, beta, beta_sd, gamma)

################################################################################################################################################
# apply adjustment to original vals

df_orig <- as.data.table(final_sex_prev)

#fill in missing variables
df_orig <- get_cases_sample_size(df_orig)
df_orig <- get_se(df_orig)
df_orig <- calculate_cases_fromse(df_orig)

##create "obs_method" variable for reference and alternatives
table(df_orig$cv_diag_wet_mount)
table(df_orig$cv_diag_culture)
table(df_orig$cv_diag_pcr)

df_orig[cv_diag_wet_mount == 1, obs_method := "wetmount"]
df_orig[cv_diag_culture == 1, obs_method := "culture"]
df_orig[cv_diag_pcr == 1, obs_method := "naat"]

df_orig$age <- (df_orig$age_start + df_orig$age_end)/2

#find the lowest non zero mean
non_zero_means <- df_orig[df_orig$mean != 0, ]
lowest_non_zero_mean_row <- non_zero_means[which.min(non_zero_means$mean), ]

# replace mean=0 with half of the lowest observed value 
df_orig <- df_orig[measure=="prevalence" & mean==0, adj_zero_mean:=1]
df_orig <- df_orig[, orig_mean:=mean]
df_orig <- df_orig[measure=="prevalence" & mean==0, mean:=(0.5*(lowest_non_zero_mean_row$mean))]

# remove rows not in reference or alt obs
df_orig <- df_orig[obs_method=="naat" | obs_method=="culture"| obs_method=="wetmount" ]
df_orig <- df_orig[!is.na(df_orig$mean)]
df_orig <- df_orig[!is.na(df_orig$standard_error)]
df_orig <- df_orig[measure=="prevalence"]

# adjusting rows
preds1 <- fit1$adjust_orig_vals(
  df = df_orig,
  orig_dorms = "obs_method",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)

# add adjustments back to original dataset
df_orig[, c("meanvar_adjusted", "sdvar_adjusted", "pred_logit", "pred_se_logit", "data_id")] <- preds1

# revert adjusted zeroes back to zero
df_orig <- df_orig[adj_zero_mean==1, mean:=0]
df_orig <- df_orig[adj_zero_mean==1, meanvar_adjusted:=0]

## update data variables post-crosswalk
df_orig[, mean := NULL] #remove column called mean, values are saved in orig_mean
df_orig <- df_orig[, orig_standard_error:= standard_error] 
df_orig <- df_orig[, mean:=meanvar_adjusted]
df_orig <- df_orig[, standard_error:=sdvar_adjusted]
df_orig <- df_orig[obs_method!="naat", upper:=NA]
df_orig <- df_orig[obs_method!="naat", lower:=NA]



###########################################################
#AGE SPLITTING
###########################################################

#PREP CROSSWALKED AND/OR SEX_SPLIT DT TO BE AGE_SPLIT--------------------------------------------------------------------------------------------------
database <- F
filepath <- T
memory <- F

prep_for_split <- function(bvid, to_as_fpath, dt_in_memory){
  if (database == T){
    dt = get_bundle_version(bundle_version_id = bvid, fetch = "all", export = FALSE, transform = TRUE)
  } else if (filepath == TRUE) {
    dt <-  data.table(read.xlsx(to_as_fpath))
  } else if (memory == TRUE){
    dt <- copy(dt_in_memory)
  }

  print(paste0("There are ",nrow(dt[measure == "prevalence"]), " prevalence rows in the dt."))
  print(paste0("# of 'Both' sex rows: ", nrow(dt[sex == "Both" & measure == "prevalence"])))
  print(paste0("# of rows that need to be age-split: ", nrow(dt[(age_end - age_start > 5) & measure == "prevalence"])))

  dt[measure %in% c("prevalence", "proportion") & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  dt[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := as.numeric(sample_size) * as.numeric(mean)]
  #set it up so that seqs will line up correctly
  dt[ ,age_seq := seq]

  dt[is.na(age_seq), age_seq := crosswalk_parent_seq]
  dt[, id_seq := 1:nrow(dt)]
  return(dt)
}

for_AS_fpath <- paste0("FILEPATH")
print(for_AS_fpath)
prepped_dt <- prep_for_split(bvid = bundle_version, to_as_fpath = for_AS_fpath, dt_in_memory = full_bv_xwalked)


#SPECIFY LAST COUPLE OF PARAMETERS------------------------------------------------------------------------------------------------------------------------
dt_to_agesplit  <- copy(prepped_dt)
gbd_round_id        <- c(6)
age                 <- c(2:20, 30:32, 235)

#BEGIN TO RUN THE FUNCTION---------------------------------------------------------------------------------------------------------------------------------
age_split <- function(split_meid, year_id = 2010, age,
                          location_pattern_id, measures = c("prevalence", "incidence"), measure_ids = c(5,6)){

  print(paste0("getting data"))
  all_age <- copy(dt_to_agesplit)

  ## FORMAT DATA
  print(paste0("formatting data"))
  all_age <- all_age[measure %in% measures,]

  tri_cols <- c("group", "specificity", "group_review")
  if (tri_cols[1] %in% names(all_age)){
    all_age <- all_age[group_review %in% c(1,NA),] 
  } else {
    all_age[ ,`:=` (group = as.numeric(), specificity = "", group_review = as.numeric())]
  }

  all_age <- all_age[(age_end-age_start)>5,]
  all_age <- all_age[!mean ==0, ] ##don't split points with zero prevalence
  all_age[sex=="Female", sex_id := 2]
  all_age[sex=="Male", sex_id :=1]
  all_age[, sex_id := as.integer(sex_id)]
  all_age[measure == "proportion", measure_id := 18]
  all_age[measure == "prevalence", measure_id := 5]
  all_age[measure == "incidence", measure_id := 6]
  all_age[, year_id := year_start] 

  ## CALC CASES AND SAMPLE SIZE
  print(paste0("CALC CASES AND SAMPLE SIZE"))
  all_age <- all_age[cases!=0,] ##don't want to split points with zero cases
  all_age_original <- copy(all_age)

  ## ROUND AGE GROUPS
  print(paste0("ROUND AGE GROUPS"))
  all_age_round <- copy(all_age)
  all_age_round[, age_start := age_start - age_start %%5]
  all_age_round[, age_end := age_end - age_end %%5 + 4]
  all_age_round <- all_age_round[age_end > 99, age_end := 99]

  ## EXPAND FOR AGE
  print(paste0("EXPAND FOR AGE"))
  all_age_round[, n_age:=(age_end+1 - age_start)/5]
  all_age_round[, age_start_floor:=age_start]
  all_age_round[ ,cases := as.numeric(cases)]
  all_age_round[, drop := cases/n_age] ##drop the data points if cases/n_age is less than 1 
  all_age_round <- all_age_round[!drop<1,]
  seqs_to_split <- all_age_round[, unique(id_seq)] #changed from seq to id seq
  all_age_parents <- all_age_original[id_seq %in% seqs_to_split] 
  expanded <- rep(all_age_round$id_seq, all_age_round$n_age) %>% data.table("id_seq" = .) 
  split <- merge(expanded, all_age_round, by="id_seq", all=T)
  split[,age.rep:= (1:.N - 1), by =.(id_seq)]
  split[,age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4 ]

  ## GET SUPER REGION INFO
  print("getting super regions")
  super_region <- get_location_metadata() 
  super_region <- super_region[, .(location_id, super_region_id)]
  split <- merge(split, super_region, by = "location_id")
  super_regions <- unique(split$super_region_id) ##get super regions

  ## GET AGE GROUPS #THIS IS WHERE YOU REMERGE
  all_age_total <- merge(split, gbd19_ages, by = c("age_start", "age_end"), all.x = T)
  all_age_total <- data.table(all_age_total)

  ## CREATE AGE GROUP ID 1
  all_age_total <- all_age_total[age_group_id %in% age] ##don't keep where age group id isn't estimated for cause

  ##GET LOCS AND POPS
  pop_locs <- unique(all_age_total$location_id)
  pop_years <- unique(all_age_total$year_id)

  ## GET AGE PATTERN
  print("GET AGE PATTERN")

  locations <- location_pattern_id
  print("getting age pattern")
  draws <- paste0("draw_", 0:999)
  print(split_meid)

  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = split_meid,
                           measure_id = measure_ids, location_id = locations, source = "epi",
                           status = "best", sex_id = unique(all_age$sex_id)
                           age_group_id = age, year_id = 2010) ##imposing age pattern

  global_population <- as.data.table(get_population(location_id = locations, year_id = 2010, sex_id = 2,
                                                age_group_id = age))
  global_population <- global_population[, .(age_group_id, sex_id, population, location_id)]
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]

  print("formatting age pattern")

  ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
  age_1 <- copy(age_pattern)
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5,6)]
  se <- copy(age_1)
  se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] 
  age_1 <- merge(age_1, global_population, by = c("age_group_id", "sex_id", "location_id"))
  age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
  age_1[, frac_pop := population / total_pop]
  age_1[, weight_rate := rate_dis * frac_pop]
  age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
  age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
  age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
  age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
  age_1[, age_group_id := 1]
  age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5,6)]
  age_pattern <- rbind(age_pattern, age_1)

  ## CASES AND SAMPLE SIZE
  age_pattern[measure_id %in% c(5, 18), sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
  age_pattern[is.nan(cases_us), cases_us := 0]

  ## GET SEX ID 3
  sex_3 <- copy(age_pattern)
  sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, rate_dis := cases_us/sample_size_us]
  sex_3[measure_id %in% c(5, 18), se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
  sex_3[measure_id == 6, se_dismod := sqrt(cases_us)/sample_size_us]
  sex_3[is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0 can't calculate rate and standard error, but should both be 0
  sex_3[is.nan(se_dismod), se_dismod := 0]
  sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
  sex_3[, sex_id := 3]
  age_pattern <- rbind(age_pattern, sex_3)

  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]

  ## MERGE AGE PATTERN
  age_pattern1 <- copy(age_pattern)
  all_age_total <- merge(all_age_total, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))

  ## GET POPULATION INFO
  print("getting populations for age structure")
  populations <- as.data.table(get_population(location_id = pop_locs, year_id = pop_years,
                                              sex_id =unique(all_age$sex_id), age_group_id = age, gbd_round_id =  6, decomp_step = "step4"))
  age_1 <- copy(populations) 
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5,6)]
  age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
  age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
  age_1[, age_group_id := 1]
  populations <- populations[!age_group_id %in% c(2, 3, 4, 5,6)]
  populations <- rbind(populations, age_1)  
  total_age <- merge(all_age_total, populations, by = c("location_id", "sex_id", "year_id", "age_group_id"))

  #####CALCULATE AGE SPLIT POINTS#######################################################################
  ## CREATE NEW POINTS
  print("creating new age split points")
  total_age[, total_pop := sum(population), by = "id_seq"]
  total_age[, sample_size := (as.numeric(population) / as.numeric(total_pop)) * as.numeric(sample_size)]
  total_age[, cases_dis := sample_size * rate_dis]
  total_age[, total_cases_dis := sum(cases_dis), by = "id_seq"]
  total_age[, total_sample_size := sum(sample_size), by = "id_seq"]
  total_age[, all_age_rate := total_cases_dis/total_sample_size]
  total_age[, ratio := mean / all_age_rate]
  total_age[, mean := ratio * rate_dis]
  total_age[, cases := sample_size * mean] 

  ######################################################################################################
  ## FORMATTING
  total_age[, specificity := paste0(specificity, ", age-split child")]
  total_age[ ,specificity := "age-split child"]
  total_age[, group := 1]
  total_age[, group_review := 1]

  blank_vars <- c("lower", "upper", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", "standard_error", "cases") 
  total_age[ ,(blank_vars) := NA] #this will not work properly without parenthesis
  total_age[, crosswalk_parent_seq := age_seq]
  total_age[ , seq := NA]

  ## ADD PARENTS
  all_age_parents[, crosswalk_parent_seq := NA]
  all_age_parents[ ,measure_id := NULL]
  all_age_parents[, group_review := 0]
  all_age_parents[, group := 1]
  all_age_parents[, specificity := paste0(specificity, ", age-split parent")]
  all_age_parents[ ,specificity := "age-split parent"]

  #MORE FORMATTING
  total_age[, setdiff(names(total_age), names(all_age_parents)) := NULL] ## make columns the same
  total <- rbind(all_age_parents, total_age)
  total <- total[mean > 1, group_review := 0]
  total$sex_id <- NULL
  total$year_id <- NULL

  #RBIND TO THE NON AGE-SPLIT DATA POINTS
  non_split <- dt_to_agesplit[!(id_seq %in% seqs_to_split)]
  all_points <- rbind(non_split, total, fill = TRUE)
  print("finished.")
  return(all_points)
}
agesplit_data <- age_split(split_meid = split_meid, year_id = 2010, age, location_pattern_id = location_pattern_id, measures = c("prevalence", "incidence"), measure_ids = c(5.6))


## until subnational priors are calculated from random effects instead of ratio 
## between data and fit, truncate all subnational data that is not age split to have a maximum age of 54yrs
agesplit_data$age_size <- as.numeric(agesplit_data$age_end) - as.numeric(agesplit_data$age_start)
# pull and merge location type
location_type <- get_location_metadata()
location_type <- location_type[, .(location_id, location_type)]
agesplit_data <- merge(agesplit_data, location_type, by = "location_id", all.x = TRUE)
# only change if prevalence data, age_end > 54, age_size is greater than 40 yrs, and is_outlier is 0
agesplit_data[measure == "prevalence" & location_type == "admin1" & age_end > 54 & age_size >= 40 & is_outlier == 0, age_end := 54]
# set the age_start to 10 if it's 0
agesplit_data[measure == "prevalence" & location_type == "admin1" & age_size >= 40 & age_start == 0 & is_outlier == 0, age_start := 10]
# double check this has 0 items (nothing should have age_end > 54)
agesplit_data[measure == "prevalence" & location_type == "admin1" & age_end > 54 & age_size >= 40 & is_outlier == 0][, c(age_start, age_end, age_size)]
# save a note in the crosswalk
agesplit_data[measure == "prevalence" & location_type == "admin1" & age_size >= 40 & is_outlier == 0,
                   note_modeler := paste0(note_modeler, "| truncated age_end to 54 years for subnational data that was not age-split")]
## drop columns for location_type and age_size
agesplit_data <- agesplit_data[, c("location_type", "age_size") := NULL]


agesplit_data