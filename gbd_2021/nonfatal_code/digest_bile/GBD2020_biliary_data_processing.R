################################################################################################################
################################################################################################################
## Purpose: to process total biliary disease data for DisMod
################################################################################################################
################################################################################################################

rm(list=ls())

## Source packages and functions
pacman::p_load(data.table, openxlsx, ggplot2, readr, RMySQL, openxlsx, readxl, stringr, tidyr, plyr, dplyr, gtools)
library(metafor, lib.loc=FILEPATH)
library(msm)
library(Hmisc, lib.loc = FILEPATH)
library(mortdb, lib.loc = FILEPATH)

base_dir <- FILEPATH
temp_dir <- FILEPATH
functions <- c("get_bundle_data", "upload_bundle_data","get_bundle_version", "save_bundle_version", "get_crosswalk_version", "save_crosswalk_version",
               "get_age_metadata", "save_bulk_outlier","get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids")
lapply(paste0(base_dir, functions, ".R"), source)

date <- Sys.Date()
date <- gsub("-", "_", date)

mrbrt_helper_dir <- FILEPATH
mrbrt_dir <- FILEPATH
draws <- paste0("draw_", 0:999)
functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
            "load_mr_brt_preds_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}

## Source custom functions to fill out mean/cases/sample size/standard error when missing
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

#########################################################################################
##STEP 1: Set objects 
#########################################################################################
bundle <- BUNDLE_ID
model_name <- "Biliary"
acause <- CAUSE_NAME
gbd_round_id <- GBD_ROUND_ID
decomp_step <- STEP_ID
save_path <- FILEPATH

output_filepath_bundle_data <- FILEPATH
output_filepath_age_split <- FILEPATH

#MAD
MAD <- 2
output_filepath_xwalk_data <- FILEPATH
age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235 ) # new in GBD 2020
byvars <- c("location_id", "sex", "year_start", "year_end", "nid") 

#########################################################################################
##STEP 2: Save a bundle version with refresh 3 clinical informatics data
#########################################################################################
step2_bundle_version <-BUNDLE_VERSION_ID 
df_all = get_bundle_version(step2_bundle_version, fetch='all', export = FALSE)

#########################################################################################
##STEP 3: GBD2020 CROSSWALK WITH UPDATED MRBRT FUNCTIONS
#########################################################################################
xwalk <- as.data.table(copy(df_all))
xwalk[(nid==454458), `:=` (cv_literature=1)]
xwalk$cv_literature[is.na(xwalk$cv_literature)] <-0
xwalk$cv_diagn_admin_data[is.na(xwalk$cv_diagn_admin_data)] <-0

xwalk = within(xwalk, {cv_ms2000 = ifelse(nid==244369, 1, 0)})
xwalk = within(xwalk, {cv_marketscan = ifelse((nid==244370 | nid== 336850| nid== 244371| nid== 336849| nid== 336848| nid== 336847| nid== 408680 | nid==433114), 1, 0)})
xwalk = within(xwalk, {cv_hospital = ifelse((cv_marketscan==0 & cv_ms2000==0 & cv_literature==0 )| cv_diagn_admin_data==1, 1, 0)})
xwalk$cv_diagn_admin_data <- NULL
xwalk = within(xwalk, {stringent = ifelse(cv_literature==1 , 1, 0)})

xwalk_prev <- subset(xwalk, measure=="prevalence") #only prevalence needs crosswalking

#PREVALENCE ------------------------------------------------------------

xwalk_prev <- get_cases_sample_size(xwalk_prev)
xwalk_prev <- calculate_cases_fromse(xwalk_prev)
xwalk_prev <- get_se(xwalk_prev)

##create "obs_method" variable
xwalk_prev[(cv_ms2000==1), `:=` (obs_method = "ms2000")]
xwalk_prev[(cv_marketscan==1), `:=` (obs_method = "marketscan")]
xwalk_prev[(cv_hospital==1), `:=` (obs_method = "inpatient")]
xwalk_prev[(stringent==1), `:=` (obs_method = "stringent")]


##set crosswalk objects
method_var <- "obs_method"
gold_def <- "stringent"
year_range <- 5


##calculate mid-year
xwalk_prev$mid_year <- (xwalk_prev$year_start + xwalk_prev$year_end)/2

#subset matches that are mean = 0, cann't logit transform
xwalk_prev[(standard_error>1), `:=` (standard_error = 0.999)]
xwalk_prev[(standard_error<0), `:=` (standard_error = 0.00000001)]

df_zero_inp <- subset(xwalk_prev, mean ==0 & obs_method !="stringent")
df_zero_ref <- subset(xwalk_prev, mean ==0 & obs_method =="stringent")
unique(df_zero_inp$obs_method) #check if any of the mean = 0 data points are from alternative case definitions
df_nonzero <- subset(xwalk_prev, mean >0 & mean <1 ) #drop all rows of data with mean greater than 1


#match alt to ref
#1. subset datasets based on case definition
ref <- subset(df_nonzero, stringent==1)
alt_inp <- subset(df_nonzero, cv_hospital==1)
alt_ms2000 <- subset(df_nonzero, cv_ms2000==1)
alt_ms <- subset(df_nonzero, cv_marketscan==1)
ref_inp <- subset(df_nonzero, cv_hospital==1) #this is for indirect comparisons with cv_marketscan and cv_ms2000

#2. clean ref and alt datasets
ref[, c("cv_ms2000", "cv_marketscan", "cv_hospital")] <- NULL
alt_inp <- alt_inp[, c("nid", "location_id", "sex", "age_start", "age_end", "mid_year", "measure", "mean", "cases", "sample_size", "standard_error",  "cv_hospital")]
alt_ms2000 <- alt_ms2000[, c("nid", "location_id", "sex", "age_start", "age_end",  "mid_year", "measure", "mean", "standard_error", "cv_ms2000")]
alt_ms <- alt_ms[, c("nid", "location_id", "sex", "age_start", "age_end", "mid_year", "measure", "mean", "standard_error",  "cv_marketscan")]
ref_inp[, c("cv_ms2000", "cv_marketscan", "stringent")] <- NULL #this is for indirect comparisons with cv_marketscan and cv_ms2000


#3. change the mean and standard error variables in both ref and alt datasets for clarification
setnames(alt_inp, c("mean", "standard_error", "mid_year", "nid", "cases", "sample_size"), c("prev_alt", "prev_se_alt", "mid_year_alt", "nid_alt",  "cases_alt", "sample_size_alt"))
setnames(ref, c("mean", "standard_error", "mid_year", "nid"), c("prev_ref", "prev_se_ref", "mid_year_ref", "nid_ref"))
setnames(alt_ms2000, c("mean", "standard_error", "mid_year", "nid"), c("prev_alt", "prev_se_alt", "mid_year_alt", "nid_alt"))
setnames(alt_ms, c("mean", "standard_error", "mid_year", "nid"), c("prev_alt", "prev_se_alt", "mid_year_alt", "nid_alt"))
setnames(ref_inp, c("mean", "standard_error", "mid_year", "nid"), c("prev_ref", "prev_se_ref", "mid_year_ref", "nid_ref"))


#4. Create a variable identifying different case definitions
alt_inp$dorm_alt<- "inpatient"
ref$dorm_ref<- "stringent"
alt_ms2000$dorm_alt<- "ms2000"
alt_ms$dorm_alt<- "marketscan"
ref_inp$dorm_ref<- "inpatient"

#5. between-study mathces based on exact location, sex, age
df_matched_inp     <- merge(ref, alt_inp, by = c( "location_id",  "measure"))
df_matched_inp <- subset(df_matched_inp, location_name!= "Jilin")

df_matched_ms2000 <- merge(ref_inp, alt_ms2000, by = c("location_id", "sex", "age_start", "age_end", "measure")) 
df_matched_ms     <- merge(ref_inp, alt_ms, by = c( "location_id", "sex", "age_start", "age_end", "measure")) 

#6. between-study matches based on 5-year span
#A. First, let's start with ref-hospital data matches
df_matched_inp <- df_matched_inp[abs(df_matched_inp$mid_year_ref - df_matched_inp$mid_year_alt) <= 5, ] 
df_matched_inp[, c("location_name", "age_start.x", "age_end.x", "mid_year_ref", "nid_ref", "sex.x" ,"age_start.y", "age_end.y", "mid_year_alt", "nid_alt", "sex.y")]

#A.a. subset df_matched_inp on age matched; get rid of ages outside the ref age range
df_matched_inp$age_end <- round(df_matched_inp$age_end.x/5)*5 #round up to the nearest 5

df_matched_inp$age_match[(df_matched_inp$age_start.y >= df_matched_inp$age_start.x) & (df_matched_inp$age_end.y <= df_matched_inp$age_end)] <-1 
df_matched_inp <- subset(df_matched_inp, age_match==1)
df_matched_inp[, c("location_name", "age_start.x", "age_end.x", "mid_year_ref", "nid_ref", "sex.x" ,"age_start.y", "age_end.y", "mid_year_alt", "nid_alt", "sex.y")]


#A.b. aggregate sex to both sex 
df_matched_inp_both <- subset(df_matched_inp, sex.x == "Both")
df_matched_inp_both[, c("location_name", "age_start.x", "age_end.x", "mid_year_ref", "nid_ref", "sex.x" ,"age_start.y", "age_end.y", "mid_year_alt", "nid_alt", "sex.y")]

df_matched_inp_both[, id := .GRP, by = c("age_start.x", "age_end.x",  "nid_ref", "nid_alt")]
df_matched_inp_both[, c("location_name", "age_start.x", "age_end.x", "mid_year_ref", "nid_ref", "sex.x", "age_start.y", "age_end.y", "mid_year_alt", "nid_alt", "sex.y", "id", "cases_alt", "sample_size_alt", "id", "prev_ref", "prev_alt") ]

df_matched_inp_both[, `:=` (n_cases = sum(cases_alt), n_sample_size = sum(sample_size_alt)), by = "id"]
df_matched_inp_both[, `:=` (n_mean = n_cases/n_sample_size, lower = NA, upper = NA)]
z <- qnorm(0.975)
df_matched_inp_both[, n_standard_error := sqrt(n_mean*(1-n_mean)/n_sample_size + z^2/(4*n_sample_size^2))]
df_matched_inp_both[, c("location_name", "age_start.x", "age_end.x", "mid_year_ref", "nid_ref", "sex.x", "age_start.y", "age_end.y", "mid_year_alt", "nid_alt", "sex.y", "id", "cases_alt", "sample_size_alt", "id", "prev_ref", "prev_alt", "n_cases", "n_sample_size", "n_mean") ]


df_matched_inp_both <- unique(df_matched_inp_both, by = "id")
df_matched_inp_both[, c("cases_alt", "sample_size_alt",  "prev_alt", "prev_se_alt")] <- NULL
setnames(df_matched_inp_both, c("n_cases", "n_sample_size",  "n_mean", "n_standard_error"),  c("cases_alt", "sample_size_alt",  "prev_alt", "prev_se_alt"))
df_matched_inp_both[, c("cases_alt", "sample_size_alt")] <- NULL
df_matched_inp_both[, c("location_name", "age_start.x", "age_end.x", "mid_year_ref", "nid_ref", "sex.x", "age_start.y", "age_end.y", "mid_year_alt", "nid_alt", "sex.y", "id",  "id", "prev_ref", "prev_alt") ]

#A.b. calculate new mean, cases, sample_size, standard error for aggregated alt data points
df_matched_inp_sex <- subset(df_matched_inp, sex.x != "Both")
df_matched_inp_sex <- subset(df_matched_inp_sex, sex.x == sex.y)
df_matched_inp_sex$sex.y <- NULL
setnames(df_matched_inp_sex, "sex.x", "sex")
df_matched_inp_sex[, id := .GRP, by = c("age_start.x", "age_end.x", "sex", "nid_ref", "nid_alt")]
df_matched_inp_sex[, c("location_name", "age_start.x", "age_end.x", "mid_year_ref", "nid_ref", "age_start.y", "age_end.y", "mid_year_alt", "nid_alt", "sex", "id", "cases_alt", "sample_size_alt", "id", "prev_ref", "prev_alt") ]

df_matched_inp_sex[, `:=` (n_cases = sum(cases_alt), n_sample_size = sum(sample_size_alt)), by = "id"]
df_matched_inp_sex[, `:=` (n_mean = n_cases/n_sample_size, lower = NA, upper = NA)]
z <- qnorm(0.975)
df_matched_inp_sex[, n_standard_error := sqrt(n_mean*(1-n_mean)/n_sample_size + z^2/(4*n_sample_size^2))]
df_matched_inp_sex <- unique(df_matched_inp_sex, by = "id")
df_matched_inp_sex[, c("cases_alt", "sample_size_alt",  "prev_alt", "prev_se_alt")] <- NULL
setnames(df_matched_inp_sex, c("n_cases", "n_sample_size",  "n_mean", "n_standard_error"),  c("cases_alt", "sample_size_alt",  "prev_alt", "prev_se_alt"))
df_matched_inp_sex[, c("cases_alt", "sample_size_alt")] <- NULL
df_matched_inp_sex[, c("location_name", "age_start.x", "age_end.x", "mid_year_ref", "nid_ref", "sex", "age_start.y", "age_end.y", "mid_year_alt", "nid_alt", "id",  "id", "prev_ref", "prev_alt") ]

#A.c. append df_matched_inp_sex and df_matched_inp_both
df_matched_inp <- rbind.fill(df_matched_inp_sex, df_matched_inp_both)

#B. Then, hospital-marketscan data matches
df_matched_ms2000 <- df_matched_ms2000[abs(df_matched_ms2000$mid_year_ref - df_matched_ms2000$mid_year_alt) <= year_range, ] 
df_matched_ms <- df_matched_ms[abs(df_matched_ms$mid_year_ref - df_matched_ms$mid_year_alt) <= 0, ] 


#9. recalculate logit differene of alt in df_matched_inp
df_matched <- rbind.fill(df_matched_inp, df_matched_ms, df_matched_ms2000)
library(crosswalk, lib.loc = FILEPATH)

df_matched[, c("mean_logit_alt", "se_logit_alt", "mean_logit_ref", "se_logit_ref")] <- as.data.frame(cbind(delta_transform(
                                                                                                                              mean = df_matched$prev_alt, 
                                                                                                                              sd = df_matched$prev_se_alt,
                                                                                                                              transformation = "linear_to_logit"),
                                                                                                          delta_transform(
                                                                                                                                mean = df_matched$prev_ref, 
                                                                                                                                sd = df_matched$prev_se_ref,
                                                                                                                                transformation = "linear_to_logit")))


#10. create study_id 
df_matched <- as.data.table(df_matched)
df_matched[, id := .GRP, by = c("nid_ref", "nid_alt")]

#11. calculate logit difference : logit(prev_alt) - logit(prev_ref)
df_matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = df_matched, 
  alt_mean = "mean_logit_alt", alt_sd = "se_logit_alt",
  ref_mean = "mean_logit_ref", ref_sd = "se_logit_ref")



#13. CWData() formats meta-regression data  
dat1_xwalk <- CWData(df = df_matched, 
                     obs = "logit_diff",                         #matched differences in logit space
                     obs_se = "logit_diff_se",                   #SE of matched differences in logit space
                     alt_dorms = "dorm_alt",                     #var for the alternative def/method
                     ref_dorms = "dorm_ref",
                     study_id = "id" )

#14. CWModel() runs mrbrt model
fit1_xwalk1 <- CWModel(
  cwdata = dat1_xwalk,                                #result of CWData() function  call
  obs_type = "diff_logit",                            #must be "diff_logit" or "diff_log"
  cov_models = list(CovModel("intercept")),           #specify covariate details
  inlier_pct = 0.9,
  gold_dorm = "stringent" )                           #level of "dorm_ref" that is the gold standard

fit1_xwalk1$fixed_vars 


#15. apply crosswalk coefficients to original data
setnames(df_nonzero, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))
df_nonzero[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
                                                                                            fit_object = fit1_xwalk1,                # result of CWModel()
                                                                                            df = df_nonzero,                         # original data with obs to be adjusted
                                                                                            orig_dorms = "obs_method",               # name of column with (all) def/method levels
                                                                                            orig_vals_mean = "orig_mean",            # original mean
                                                                                            orig_vals_se = "orig_standard_error"     # standard error of original mean
                                                                                                )


check <- subset(df_nonzero, stringent==1)
check %>% select(location_name, age_start, age_end, nid, orig_mean, orig_standard_error, mean, standard_error, diff, diff_se, obs_method)
df_nonzero[(obs_method!="stringent"), `:=` (crosswalk_parent_seq = seq, note_modeler = paste0(note_modeler, " | crosswalked ", date))]

#17. apply crosswalk variance to mean = zero data points of alternative case definition
preds <- fit1_xwalk1$fixed_vars
preds <- as.data.frame(preds)
preds <- rbind(preds, fit1_xwalk1$beta_sd)
unique(df_zero_inp$obs_method)

cvs <- c("inpatient")    

for (cv in cvs) {
  ladj <- preds[cv][[1]][1]
  ladj_se <- preds[cv][[1]][2]
  df_zero_inp[obs_method == cv , `:=` (ladj = ladj, ladj_se = ladj_se)]
  df_zero_inp$adj_se <- sapply(1:nrow(df_zero_inp), function(i){
    mean_i <- as.numeric(df_zero_inp[i, "ladj"])
    se_i <- as.numeric(df_zero_inp[i, "ladj_se"])
    deltamethod(~exp(x1)/(1+exp(x1)), mean_i, se_i^2)
  })
  df_zero_inp[, `:=` (standard_error = sqrt(standard_error^2 + adj_se^2), 
                      note_modeler = paste0(note_modeler, " | uncertainty from network analysis added"))]
  df_zero_inp[, `:=` (crosswalk_parent_seq = seq, seq = NA)]
} 

#18. combine all datasets
df_final <- as.data.table(rbind.fill(df_zero_ref, df_zero_inp, df_nonzero))
write.csv(df_final, FILEPATH)
df_final <- as.data.table(read.csv(FILEPATH))


#########################################################################################
##STEP 4: Within-study age-sex split
#########################################################################################

#FIRST, WITHIN-STUDY AGE-SEX SPLIT
dt <- subset(df_final, measure!="incidence")
dt <- as.data.table(subset(dt, cv_literature==1 )) #subset literature data only, excluding custom EMR and CSMR NIDs
dt_admin <- as.data.table(subset(df_final, cv_literature==0))
dt$note_modeler <- ""
output_filepath <- paste0(save_path, acause, "_age-sex_split_", date, ".csv")

age_sex_split <- function(dt){
  notsplit <- as.data.table(subset(dt, age_sex_split=="" ))
  dt_split <- as.data.table(subset(dt, age_sex_split!=""))
  dt_split[, note_modeler := as.character(note_modeler)]
  dt_split[, specificity := as.character(specificity)]
  
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
  
  #subset the rows with sex-specific data
  sex <- dt_split[age_sex_split=="sexvalue" ]
  
  #sum female and male-specific data points from the same study but across multiple years to calculate pooled sex ratio
  sex[, cases1:= sum(cases), by = list(nid, sex, measure, location_id)]
  sex[, sample_size1:= sum(sample_size), by = list(nid, sex, measure, location_id)]
  sex <- sex[,  c("nid", "sex", "measure", "age_sex_split", "location_id", "cases1", "sample_size1")]
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
  
  
  #Create age,sex observations 
  tosplit_sex[,specificity := paste(specificity, ",sex")]
  tosplit_sex[, seq := NA]
  male <- copy(tosplit_sex[, sex := "Male"]) #create a copy of data table with sex specified as male
  female <- copy(tosplit_sex[, sex := "Female"]) #create a copy of data table with sex specified as female
  tosplit_sex <- rbind(male, female)
  
  #Merge sex ratios to age,sex observations
  tosplit_sex<- merge(tosplit_sex, sex, by = c("nid", "sex", "measure", "location_id"))
  
  #calculate age-sex specific mean, standard_error, cases, sample_size
  tosplit_sex[, standard_error := (sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2))]
  tosplit_sex[, mean := mean * ratio]
  tosplit_sex[, cases := round(cases * prop_cases, digits = 0)]
  tosplit_sex[, sample_size := round(sample_size * prop_ss, digits = 0)]
  tosplit_sex[,note_modeler := paste(note_modeler, "| sex split using sex ratio", round(ratio, digits = 2))]
  tosplit_sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss","age_sex_split.x", "age_sex_split.y", "ss_total", "se_cases", "se_ss", "cases1", "sample_size1", "cases_total") := NULL]
  
  
  #clean sex-specific data points that we want to append to final dataset
  sex_append <- dt_split[age_sex_split=="sexvalue"]
  sex_append <- subset(sex_append, group_review==1)
  
  ## merge back on any dropped un-split seqs.
  final_sex <- rbind.fill(tosplit_sex,notsplit, sex_append)
  
  
  dt <- as.data.table(final_sex)
  
  ##return the prepped data
  return(dt)
  
}

within_study_split_dt <- age_sex_split(dt)
within_study_split_dt[, c("cv_marketscan_all_2000", "cv_marketscan_all_2010", "cv_marketscan_all_2012","cv_marketscan_inp_2000", "cv_marketscan_inp_2010", "cv_marketscan_inp_2012", "cv_inpatient", "cv_self_report", "cv_meps", "cv_marketscan")] <- NULL
dt_admin_both_sex <- subset(dt_admin, sex=="Both")
dt_admin_sex <- subset(dt_admin, sex!="Both")

dt <- rbind.fill(within_study_split_dt, dt_admin_both_sex)
write.csv(within_study_split_dt, output_filepath,  row.names=F)

########################################################################################################     
#Step 5: Sex-split both sex data points using the pooled ratio from sex-specific lit data points
########################################################################################################     
draws <- paste0("draw_", 0:999)
cv_drop <- c("cv_literature")

sex_prev <-  as.data.table(copy(dt))
sex_prev[, sex_dummy := "Female"]
within_study_split_dt[, sex_dummy := "Female"]

## Prevalence first -------------------------------------------------------------------------------------
#subset to both sex
both_sex <- copy(sex_prev[sex == "Both"]) #we are going to split crosswalked data
both_sex[, id := 1:nrow(both_sex)]

both_zero <- copy(both_sex)
both_zero <- both_zero[mean == 0, ]
nrow(both_zero)

both_sex <- both_sex[mean != 0]
sex_specific <- copy(sex_prev[sex != "Both" ]) #to estimate sex-split ratio, we use raw data that has only been within-study sex-split

#find sex matches--------------------------------------------------------------------------------------
find_sex_match <- function(dt){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence", "incidence")]
  match_vars <- c( "location_name", "nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end",
                   names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
  sex_dt[, match_n := .N, by = match_vars] #finding female-male pairs within each study, matched on measure, year, age, location_name
  sex_dt <- sex_dt[match_n >1] 
  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt[, id := .GRP, by = match_vars] #ID: number of matches
  sex_dt <- dplyr::select(sex_dt, keep_vars) #keep only the variables in "keep_var" vector
  sex_dt<-data.table::dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate = mean) #reshape long to wide, to match male to female, rename mean and se with "x_Female" and "x_<ale"
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0] #drop where sex-specific mean is zero -> not informative 
  sex_dt[, id := .GRP, by = c("nid", "location_id")] #re-number based on nid and location id
  sex_dt$dorm_alt <- "Female"
  sex_dt$dorm_ref <- "Male"
  return(sex_dt)
}

sex_matches <- find_sex_match(sex_specific)
sex_matches

#calculate logit difference between sex-specific mean estimates: female alternative; male refernce-----
#1. logit transform mean
model <- paste0("sex_split_prevalence_", model_name, "_", date)
sex_diff <- as.data.frame(cbind(
  delta_transform(
    mean = sex_matches$mean_Female, 
    sd = sex_matches$standard_error_Female,
    transformation = "linear_to_log" ),
  delta_transform(
    mean = sex_matches$mean_Male, 
    sd = sex_matches$standard_error_Male,
    transformation = "linear_to_log")
))
names(sex_diff) <- c("mean_alt_Female", "mean_se_alt_Female", "mean_ref_Male", "mean_se_ref_Male")


#2. calculate logit(prev_alt) - logit(prev_ref)
sex_matches[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = sex_diff, 
  alt_mean = "mean_alt_Female", alt_sd = "mean_se_alt_Female",
  ref_mean = "mean_ref_Male", ref_sd = "mean_se_ref_Male" )



#3. run MRBRT
sex_model <- run_mr_brt(
  output_dir = FILEPATH,
  model_label = paste0(model_name, "_sex_split_prevalence_", date),
  data = sex_matches,
  overwrite_previous = TRUE,
  remove_x_intercept = FALSE,
  mean_var = "log_diff",
  se_var = "log_diff_se",
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.10
  # method = "remL"
  
)

#4. predict out
sex_model$model_coefs

#5. set up data for sex-splitting
sex_plit_data <- both_sex
sex_plit_data$midyear <- (sex_plit_data$year_end + sex_plit_data$year_start)/2

#6. call sex-split functions
get_row <- function(n, dt){
  row <- copy(dt)[n]
  pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                          age_group_years_start >= row[, age_start] & age_group_years_end <= row[, age_end]])
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex")]
  row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
              both_N = agg[sex == "Both", pop_sum])]
  return(row)
}
split_data <- function(dt, model){
  tosplit_dt <- copy(dt)
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review == 0)]
  tosplit_dt <- tosplit_dt[sex == "Both" & (group_review == 1 | is.na(group_review))]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]
  location_ids <- tosplit_dt[, unique(location_id)]
  location_ids<-location_ids[!location_ids %in% NA]
  preds <- predict_mr_brt(model, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T)
  pred_draws <- as.data.table(preds$model_draws)
  pred_draws[, c("X_intercept", "Z_intercept") := NULL]
  pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
  ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
  ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
  
  pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                           year_ids = tosplit_dt[, unique(midyear)], location_ids =location_ids)
  
  
  pops[age_group_years_end == 125, age_group_years_end := 99]
  
  
  tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt), mc.cores = 9))
  tosplit_dt <- tosplit_dt[!is.na(both_N)] ## GET RID OF DATA THAT COULDN'T FIND POPS - RIGHT NOW THIS IS HAPPENING FOR 100+ DATA POINTS
  
  
  tosplit_dt[, merge := 1]
  pred_draws[, merge := 1]
  split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
  male_dt <- copy(split_dt)
  
  male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = NA, lower = NA, 
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                        ratio_se, ")"))]
  male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  male_dt <- dplyr::select(male_dt, names(sex_plit_data))
  female_dt <- copy(split_dt)
  
  female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA, 
                    cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                          ratio_se, ")"))]
  female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  female_dt <- dplyr::select(female_dt, names(sex_plit_data))
  total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))
  return(list(final = total_dt, graph = split_dt))
}
graph_predictions <- function(dt){
  graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
  graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
  graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"))
  graph_dt[, N := (mean*(1-mean)/error^2)]
  graph_dt$N[graph_dt$N=="NaN"] <-NA
  graph_dt <-subset(graph_dt, N!="")
  wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  graph_dt[, midage := (age_end + age_start)/2]
  ages <- c(60, 70, 80, 90)
  graph_dt[, age_group := cut2(midage, ages)]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) + 
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper)) +
    #  facet_wrap(~age_group) +
    labs(x = "Both Sex Mean", y = " Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex Split Means Compared to Both Sex Mean by Age") +
    scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
    theme_classic()
  return(gg_sex)
}

#7. run sex-split functions
predict_sex <- split_data(sex_plit_data, sex_model)
final_dt_prev <- copy(predict_sex$final)
final_dt_prev <- rbind.fill(final_dt_prev, sex_specific, dt_admin_sex)
graph_dt_prev <- copy(predict_sex$graph)

#8. create visualization 
sex_graph <- graph_predictions(graph_dt_prev)
sex_graph
ggsave(filename = paste0(save_path,  "sex_graph_prevalence_", date,".pdf"), plot = sex_graph, width = 6, height = 6)


final_dt_prev$crosswalk_parent_seq <- final_dt_prev$seq
unique(final_dt_prev$sex)

######################################################################################################  
# Step 6. GETTING AGE PATTERN FROM DISMOD
######################################################################################################  
#subset to age range below 25
age_split_prep <- as.data.table(copy(final_dt_prev))
age_split_prep$age_range <- age_split_prep$age_end - age_split_prep$age_start
age_split_prep <- subset(age_split_prep, age_range <25 | (age_end==124 & age_start==95))
age_split_prep$standard_error[age_split_prep$standard_error>1] <- 1
age_split_prep$crosswalk_parent_seq <- age_split_prep$seq
age_split_prep[(group_review==""), `:=` (specificity=NA, group= NA)]
write.xlsx(age_split_prep, output_filepath_age_split, sheetName = "extraction", col.names=TRUE)


#upload it as a crosswalk version
description <- 'refresh 3 updated crosswalked, subset <25 age range, updated xwalk and sex-split' #description needed for each crosswalk version; this is what is going to show up in Dismod
result <- save_crosswalk_version( step2_bundle_version, output_filepath_age_split, description=description)


#########################################################################################
#STEP 7: age-splitting
#########################################################################################
# GET OBJECTS -------------------------------------------------------------
b_id <- BUNDLE_ID
a_cause <- CAUSE_NAME
name <- "Biliary"
bundle_version_id <- step2_bundle_version 
id <- MEID
region_pattern <- T 

date <- gsub("-", "_", date)
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
  dt <- dt[(age_end-age_start)>25  ,] #for prevelance, incidence, proportion
  dt <- dt[(!mean == 0 & !cases == 0) |(!mean == 0 & is.na(cases))  , ] ##HH modified on July 11 because many rows of data do not have cases so the original code was dropping all of the rows with missing cases
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
  #split <- split[age_group_id %in% age | age_group_id == 1] ##don't keep where age group id isn't estimated for cause
  return(split)
}

## GET DISMOD AGE PATTERN
get_age_pattern <- function(locs, id, age_groups){
  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, ## USING 2010 AGE PATTERN BECAUSE LIKELY HAVE MORE DATA FOR 2010
                           measure_id = c(5), location_id = locs, source = "epi", ##Measure ID 5= prev, 6=incidence, 18=proportion
                           version_id = id,  sex_id = c(1,2), gbd_round_id = 7, decomp_step = "iterative", #can replace version_id with status = "best" or "latest"
                           age_group_id = age_groups, year_id = 2010) ##imposing age pattern
  us_population <- get_population(location_id = locs, year_id = 2010, sex_id = c(1, 2),
                                  age_group_id = age_groups, decomp_step = "iterative")
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

## ACTUALLY SPLIT THE DATA
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
  dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
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

# RUN THESE CALLS ---------------------------------------------------------------------------
ages <- get_age_metadata(19, gbd_round_id = 7)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_groups <- ages[age_start >= 5, age_group_id]

df <- as.data.table(copy(final_dt_prev))
age <- age_groups
gbd_id <- id

location_pattern_id <- 1

# AGE SPLIT FUNCTION -----------------------------------------------------------------------
age_split <- function(gbd_id, df, age, region_pattern, location_pattern_id){
  
  ## GET TABLES
  sex_names <- get_ids(table = "sex")
  ages[, age_group_weight_value := NULL]
  ages[age_start >= 1, age_end := age_end - 1]
  ages[age_end == 124, age_end := 100]
  super_region_dt <- get_location_metadata(location_set_id = 22, gbd_round_id =7)
  super_region_dt <- super_region_dt[, .(location_id, super_region_id)]
  
  
  ## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQS)
  original <- as.data.table(copy(df))
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
  pop_locs<-pop_locs[!pop_locs %in% NA]
  
  pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
  split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))
  
  #####CALCULATE AGE SPLIT POINTS#######################################################################
  ## CREATE NEW POINTS
  print("splitting data")
  split_dt <- split_data(split_dt)
  
  
}


######################################################################################################
final_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                 original_dt = original)


final_dt$dup[duplicated(final_dt$seq)] <-1
final_dt$dup[is.na(final_dt$dup)] <-0

final_dt$seq[final_dt$dup==1] <-NA
final_dt$crosswalk_parent_seq[final_dt$dup==1] <-NA

#########################################################################################
#STEP 8: MAD-OUTLIER
#########################################################################################
new_xwalk <- as.data.table(copy(final_dt))

## GET AGE WEIGHTS
unique(new_xwalk$age_start)

fine_age_groups <- "no" #specify how you want to get your age group weights

if (fine_age_groups=="yes") {
  #---option 1: if all data are at the most detailed age groups
  all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19)) ##For GBD 2020, age group set 19 defines most detailed ages
  setnames(all_fine_ages, c("age_group_years_start"), c("age_start"))
  
  
} else if (fine_age_groups=="no") {
  #---option 2: if <1 years is aggregated, change that age group id to 28 and aggregate age weights
  all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19)) ##For GBD 2020, age group set 19 defines most detailed ages
  not_babies <- all_fine_ages[!age_group_id %in% c(2:3, 388, 389)]
  not_babies[, age_group_years_end := age_group_years_end-1]
  not_babies <- not_babies[, c("age_group_years_start", "age_group_years_end", "age_group_id", "age_group_weight_value")]
  group_babies1 <- all_fine_ages[age_group_id %in% c(2:3, 388, 389)]
  group_babies1$age_group_id <- 28
  group_babies1$age_group_years_end <- 0.999
  group_babies1$age_group_years_start <- 0
  group_babies1[, age_group_weight_value := sum(age_group_weight_value)]
  group_babies1 <- group_babies1[, c("age_group_years_start", "age_group_years_end", "age_group_id", "age_group_weight_value")]
  group_babies1 <- unique(group_babies1)
  all_fine_ages <- rbind(not_babies, group_babies1)  
  all_fine_ages_m <- copy(all_fine_ages)
  all_fine_ages_m$sex <- "Male"
  all_fine_ages_f <- copy(all_fine_ages)
  all_fine_ages_f$sex <- "Female"
  all_fine_ages <- rbind(all_fine_ages_m, all_fine_ages_f)
  all_fine_ages$age_group_years_end[all_fine_ages$age_group_years_end==124] <- 99
  
}

## Delete rows with emtpy means
new_xwalk<- new_xwalk[!is.na(mean)]
new_xwalk[, id := .GRP, (1:nrow(new_xwalk))]
new_xwalk$age_end1 <- round((new_xwalk$age_end+1)/5)*5
new_xwalk$age_start1 <- round((new_xwalk$age_start)/5)*5

##merge age table map and merge on to dataset
new_xwalk <- as.data.table(merge(new_xwalk, all_fine_ages,  by = "sex",  all=T,allow.cartesian = T))
new_xwalk$age_match[(new_xwalk$age_group_years_start >= new_xwalk$age_start1) & (new_xwalk$age_group_years_end <= new_xwalk$age_end1)] <-1 
new_xwalk <- as.data.table(subset(new_xwalk, age_match==1))
new_xwalk <- new_xwalk[, age_group_weight_value := sum(age_group_weight_value), by = "id"]
new_xwalk[, c("age_group_years_start", "age_group_years_end", "age_group_id")] <- NULL
new_xwalk <- unique(new_xwalk)


#calculate age-standardized prevalence/incidence below:
##create new age-weights for each data source
new_xwalk <- new_xwalk[, sum1 := sum(age_group_weight_value), by = byvars] #sum of standard age-weights for all the ages we are using, by location-age-sex-nid, sum will be same for all age-groups and possibly less than one 
new_xwalk <- new_xwalk[, new_weight1 := age_group_weight_value/sum1, by = byvars] #divide each age-group's standard weight by the sum of all the weights in their locaiton-age-sex-nid group 

##age standardizing per location-year by sex
#add a column titled "age_std_mean" with the age-standardized mean for the location-year-sex-nid
new_xwalk[, as_mean := mean * new_weight1] #initially just the weighted mean for that AGE-location-year-sex-nid
new_xwalk[, as_mean := sum(as_mean), by = byvars] #sum across ages within the location-year-sex-nid group, you now have age-standardized mean for that series

##mark as outlier if age standardized mean is 0 (either biased data or rare disease in small ppln)
new_xwalk[as_mean == 0, is_outlier := 1] 
new_xwalk$note_modeler <- as.character(new_xwalk$note_modeler)
new_xwalk[as_mean == 0, note_modeler := paste0(note_modeler, " | GBD 2020, outliered this location-year-sex-NID age-series because age standardized mean is 0")]

## log-transform to pick up low outliers
new_xwalk[as_mean != 0, as_mean := log(as_mean)]

# calculate median absolute deviation
new_xwalk[as_mean == 0, as_mean := NA] ## don't count zeros in median calculations
new_xwalk[,mad:=mad(as_mean,na.rm = T),by=c("sex")]
new_xwalk[,median:=median(as_mean,na.rm = T),by=c("sex")]


# outlier based on MAD
new_xwalk[as_mean>((MAD*mad)+median), is_outlier := 1]
new_xwalk[as_mean>((MAD*mad)+median), note_modeler := paste0(note_modeler, " | GBD 2020, outliered because log age-standardized mean for location-year-sex-NID is higher than ", MAD, " MAD above median")]
new_xwalk[as_mean<(median-(MAD*mad)), is_outlier := 1]
new_xwalk[as_mean<(median-(MAD*mad)), note_modeler := paste0(note_modeler, " | GBD 2020, outliered because log age-standardized mean for location-year-sex-NID is lower than ", MAD, " MAD below median")]
with(new_xwalk, table(sex, mad))
with(new_xwalk, table(sex, median))
with(new_xwalk, table(sex, exp(median)))

new_xwalk$standard_error[new_xwalk$standard_error>1] <- 1
new_xwalk[(is.na(group)), `:=` (specificity="")]
new_xwalk$group_review[new_xwalk$group_review==0.0] <- 0
new_xwalk1 <- subset(new_xwalk, group_review!=0 | is.na(group_review))
new_xwalk1[, `:=` (group_review=NA, group = NA, specificity ="")]
write.xlsx(new_xwalk1, output_filepath_xwalk_data, sheetName = "extraction", col.names=TRUE)

########################################################################################
#STEP 8: Upload MAD-outliered new data 
########################################################################################
description2 <- DESCRIPTION
result <- save_crosswalk_version( step2_bundle_version, output_filepath_xwalk_data, description=description2)

print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
print(sprintf('Crosswalk version ID from decomp 2/3 best model: %s', result$previous_step_crosswalk_version_id))

