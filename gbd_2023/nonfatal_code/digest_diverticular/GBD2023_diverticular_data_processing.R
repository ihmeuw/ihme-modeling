################################################################################################################
################################################################################################################
## Project:      GBD 2023 Nonfatal Estimation
## Purpose:      CROSSWALKING DIVERTICULAR DISEASE IN GBD 2023 USING NEW CLINICAL INFORMATICS DATA
################################################################################################################
################################################################################################################
rm(list=ls())

# SOURCE FUNCTIONS AND LIBRARIES --------------------------------------------------
pacman::p_load(cowplot,data.table, ggplot2, RMySQL, dplyr, readr, tidyverse, openxlsx, tidyr, plyr, stringr, readxl, foreign, maptools, RColorBrewer, grid, gridExtra, ggplot2, sp, reshape2, rgdal, timeDate, scales, lubridate, lattice, viridis, zoo, ggrepel, data.table, labeling, forcats)
invisible(sapply(list.files("FILEPATH/r/", full.names = T), source))
functions <- c("get_age_metadata", "get_outputs", "get_location_metadata", "get_ids", "get_covariate_estimates", "get_envelope", "get_pct_change", "get_draws", "get_population", "get_elmo_ids", "get_bundle_version", "get_crosswalk_version", "get_cod_data")
source("FILEPATH/2019GBD_MAP.R")
library(reticulate)
# SOURCE CUSTOM FUNCTIONS -------------------------------------------------------------------------
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
} #Fill out mean/cases/sample sizes
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
} #Calculate std error based o nuploader formulas
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
} #back-calculate cases adn sample size from SE
aggregate_marketscan <- function(mark_dt){
  dt <- copy(mark_dt)
  if (covariate_name == "cv_marketscan") marketscan_dt <- copy(dt[cv_marketscan==1])
  if (covariate_name == "cv_ms2000") marketscan_dt <- copy(dt[cv_ms2000==1])
  
  by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
  marketscan_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
  marketscan_dt[, `:=` (location_id = 102, location_name = "United States", mean = cases/sample_size,
                        lower = NA, upper = NA)]
  z <- qnorm(0.975)
  marketscan_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  marketscan_dt <- unique(marketscan_dt, by = by_vars)
  full_dt <- rbind(dt, marketscan_dt, fill=TRUE)
  return(full_dt)
} #Aggregate Marketscan data

# SOURCE CROSSWALK PACKAGE (Latest version) --------------------------------------------------------
reticulate::use_python("FILEPATH/python")
mr <- import("mrtool")
cw <- import("crosswalk")
#########################################################################################
## Set objects 
bundle_id <- OBJECT 
acause <- "digest_diverticular"
measures <- "prevalence" #measure for which clinical informatics data are prepped for
save_outdir <- paste0("FILEPATH")

#For Match_finding
match_finding_dir <-("FILEPATH")

#For XWALK
cov_names <- c("cv_marketscan", "cv_ms2000") 
mrbrt_cv <- "intercept_age_sex"
xwalk_type<-"logit"
year_range <- 5

xwalk_output_dir <- ("FILEPATH")
date <- gsub("-", "_", Sys.Date())
model_name_ms2000 <- paste0("/crosswalk_MS2000_", date)
dir.create(paste0(xwalk_output_dir, model_name_ms2000))
model_name_ms <- paste0("/crosswalk_MS_", date)
dir.create(paste0(xwalk_output_dir, model_name_ms))


#For outlier
output_filepath_bundle_data <- ("FILEPATH")
output_filepath <- ("FILEPATH")
age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235 ) 
byvars <- c("location_id", "sex", "year_start", "year_end", "nid") 


#########################################################################################
##STEP 1: Clean bundle data
#check bundle data and see what's in it
active_data = get_bundle_data(bundle_id)
df_info <- get_elmo_ids(bundle_id)

#Refresh  bundle version
#bv <- get_bundle_version(bundle_version_id = bundle_version_id, fetch= 'all', export = FALSE)

#save clinical_version_id = 3
#result <- save_bundle_version(bundle_id, include_clinical=c("claims", "inpatient")) #uncomment this when have new clinical data refresh
#print(sprintf('Bundle version ID: %s', result$bundle_version_id))
#step2_bundle_version <- 43418 #GBD2022 bundle_version_id (clinical_version_id = 3)

#save clinical_version_id = 4
#result <- save_bundle_version(bundle_id, include_clinical=c("claims", "inpatient")) #uncomment this when have new clinical data refresh
#print(sprintf('Bundle version ID: %s', result$bundle_version_id))
step2_bundle_version <- 45701 #GBD2023 bundle_version_id (clinical_version_id = 4)
#########################################################################################
##STEP 2: Find matches and run MR-BRT to crosswalk 
df_all = get_bundle_version(step2_bundle_version, fetch='all', export = FALSE) #GBD2023 bundle_version_id

df_remove_hmdb <- read.csv("FILEPATH")

df_all2 <- df_all[!(df_all$field_citation_value == "World Health Organization Regional Office for Europe (WHO/Europe). European Hospital Morbidity Database, 2008. Copenhagen, Denmark: World Health Organization Regional Office for Europe (WHO/Europe)." & df_all$location_id != 76), ]


df <- subset(df_all2, clinical_data_type!="" | nid==416752 )
#Create new variables for reference/inpatient, Marketscan 2000 data, Marketscan 2010-2019 data
df = within(df, {cv_ms2000 = ifelse(nid==244369, 1, 0)})
df = within(df, {cv_marketscan = ifelse((nid==244370 | nid== 336850| nid== 244371| nid== 336849| nid== 336848| nid== 336847| nid== 408680 | nid==433114| nid== 494351 | nid==494352), 1, 0)})
df = within(df, {cv_hospital = ifelse(cv_marketscan==0 & cv_ms2000==0, 1, 0)})

##create "obs_method" variable
df[(cv_ms2000==1), `:=` (obs_method = "ms2000")]
df[(cv_marketscan==1), `:=` (obs_method = "marketscan")]
df[(cv_hospital==1), `:=` (obs_method = "inpatient")]

##set crosswalk objects
method_var <- "obs_method"
gold_def <- "inpatient"

##calculate mid-year and mid-age
df$mid_year <- (df$year_start + df$year_end)/2
df$age <- (df$age_start + df$age_end)/2

#original data plot
# Create the plot
pdf(paste0("FILEPATH", acause, "_original_data_", date, ".pdf"),  width=20, height=8) # this will be the name of the file
p <- ggplot(df, aes(x = age, y = mean, color = obs_method)) +
  geom_point() +      # To add points at data positions
  labs(title = "Original Data for Diverticular Disease",
       x = "Mid-Age",
       y = "Prevalence Rate") +
  theme_minimal() +   # A clean theme
  scale_color_manual(values = c("ms2000" = "blue", "marketscan" = "red", "inpatient" = "green"))

# Print the plot
print(p)
dev.off()

#subset matches that are mean = 0, can't logit transform
df_zero <- subset(df, mean ==0 | nid == 416752)
df_zero[(sex=="Male"), `:=` (sex_id =1)] 
df_zero[(sex=="Female"), `:=` (sex_id =2)]
unique(df_zero$obs_method) #check if any of the mean = 0 data points are from alternative case definitions
df_nonzero <- subset(df, mean >0 & mean <1 & nid!=416752) #drop all rows of data with mean greater than 1
df_nonzero[(sex=="Male"), `:=` (sex_id =1)]
df_nonzero[(sex=="Female"), `:=` (sex_id =2)]

# drop Marketscan data for those who are 65 and up
df_nonzero_65plus <- subset(df_nonzero, ((cv_ms2000==1 | cv_marketscan==1) & age_start>64))
message(paste0("There are ", nrow(df_nonzero_65plus), " observations that will be dropped from crosswalk version because they are Marketscan cases who are ages 65 years or above"))
df_nonzero_65below <- subset(df_nonzero, !((cv_ms2000==1 | cv_marketscan==1) & age_start>64)) 

pdf(paste0("FILEPATH", acause, "_nonzero_65below_", date, ".pdf"),  width=20, height=8) # this will be the name of the file
p <- ggplot(df_nonzero_65below, aes(x = age, y = mean, color = obs_method)) +
  geom_point() +      # To add points at data positions
  labs(title = "Nonzero 65 below for Diverticular Disease",
       x = "Mid-Age",
       y = "Prevalence Rate") +
  theme_minimal() +   # A clean theme
  scale_color_manual(values = c("ms2000" = "blue", "marketscan" = "red", "inpatient" = "green"))

# Print the plot
print(p)
dev.off()

#original data
df_nonzero_ms2000 <- subset(df_nonzero, cv_ms2000 ==1)
df_nonzero_ms <- subset(df_nonzero, cv_marketscan ==1)
df_nonzero_ref <- subset(df_nonzero, cv_hospital ==1)

#logit transform mean and delta-transform SE, it's to convert standard errors into/out of log/logit space
#Note that the delta_transform()function has been deprecated. Use cw$utils$linear_to_log() and cw$utils$linear_to_logit() instead
df_nonzero_65below[, c("mean_logit", "se_logit")] <- cw$utils$linear_to_logit(
  mean = array(df_nonzero_65below$mean), 
  sd = array(df_nonzero_65below$standard_error))




#match alt to ref
#1. subset datasets based on case definition
ref <- subset(df_nonzero_65below, cv_hospital==1)
alt_ms2000 <- subset(df_nonzero_65below, cv_ms2000==1)
alt_ms <- subset(df_nonzero_65below, cv_marketscan==1)

#2. clean ref and alt datasets
alt_ms2000 <- alt_ms2000[, c("nid", "location_id", "sex", "age_start", "age_end",  "mid_year", "measure", "mean", "standard_error", "cv_ms2000","mean_logit", "se_logit")]
alt_ms <- alt_ms[, c("nid", "location_id", "sex", "age_start", "age_end", "mid_year", "measure", "mean", "standard_error",  "cv_marketscan","mean_logit", "se_logit")]
ref[, c("cv_ms2000", "cv_marketscan")] <- NULL

#3. change the mean and standard error variables in both ref and alt datasets for clarification
setnames(ref, c("mean", "standard_error", "mid_year", "nid", "mean_logit", "se_logit"), c("prev_ref", "prev_se_ref", "mid_year_ref", "nid_ref", "mean_logit_ref", "se_logit_ref"))
setnames(alt_ms2000, c("mean", "standard_error", "mid_year", "nid", "mean_logit", "se_logit"), c("prev_alt", "prev_se_alt", "mid_year_alt", "nid_alt", "mean_logit_alt", "se_logit_alt"))
setnames(alt_ms, c("mean", "standard_error", "mid_year", "nid", "mean_logit", "se_logit"), c("prev_alt", "prev_se_alt", "mid_year_alt", "nid_alt", "mean_logit_alt", "se_logit_alt"))

#4. create a variable identifying different case definitions
alt_ms2000$dorm_alt<- "ms2000"
alt_ms$dorm_alt<- "marketscan"
ref$dorm_ref<- "inpatient"

#5. between-study matches based on exact location, sex, age
df_matched_ms2000 <- merge(ref, alt_ms2000, by = c("location_id", "sex", "age_start", "age_end", "measure"))
df_matched_ms     <- merge(ref, alt_ms, by = c( "location_id", "sex", "age_start", "age_end", "measure"))

#6 between-study matches based on 5-year span for ms2000 and 3-year span for ms
df_matched_ms2000 <- df_matched_ms2000[abs(df_matched_ms2000$mid_year_ref - df_matched_ms2000$mid_year_alt) <= year_range, ] 
df_matched_ms <- df_matched_ms[abs(df_matched_ms$mid_year_ref - df_matched_ms$mid_year_alt) <= 3, ] 

#7. Optional: check the matches
df_matched_ms2000 %>% select(location_name, age_start, age_end, nid_ref, mid_year_ref, prev_ref, dorm_ref, mean_logit_ref, se_logit_ref, nid_alt, mid_year_alt, prev_alt, dorm_alt, mean_logit_alt, se_logit_alt)
unique(df_matched_ms2000$nid_ref)
unique(df_matched_ms2000$nid_alt)

df_matched_ms %>% select(location_name, age_start, age_end, nid_ref, mid_year_ref, prev_ref, dorm_ref, mean_logit_ref, se_logit_ref, nid_alt, mid_year_alt, prev_alt, dorm_alt, mean_logit_alt, se_logit_alt)
unique(df_matched_ms$nid_ref)
unique(df_matched_ms$nid_alt)

match_finding_dir <-("FILEPATH")
file_name_2000 <- "df_matched_ms2000.xlsx"
match_finding_full_path_2000 <- file.path(match_finding_dir, file_name_2000)
write.xlsx(df_matched_ms2000, match_finding_full_path_2000, sheetName = "matched_ms_2000", colNames=TRUE)
file_name_ms <- "df_matched_ms.xlsx"
match_finding_full_path_ms <- file.path(match_finding_dir, file_name_ms)
write.xlsx(df_matched_ms, match_finding_full_path_ms, sheetName = "matched_ms", colNames=TRUE)

#8. calculate logit difference : logit(prev_alt) - logit(prev_ref)
df_matched_ms2000 <- df_matched_ms2000 %>%
  mutate(
    logit_diff = mean_logit_alt - mean_logit_ref,
    logit_diff_se = sqrt(se_logit_alt^2 + se_logit_ref^2)
  )
df_matched_ms <- df_matched_ms %>%
  mutate(
    logit_diff = mean_logit_alt - mean_logit_ref,
    logit_diff_se = sqrt(se_logit_alt^2 + se_logit_ref^2)
  )


# Reshape the data to long format
data_long_ms2000 <- df_matched_ms2000 %>%
  pivot_longer(cols = c(mean_logit_ref, mean_logit_alt),
               names_to = "method",
               values_to = "prevalence")
# Rename the levels in the obs_method column
data_long_ms2000$method <- factor(data_long_ms2000$method, 
                               levels = c("mean_logit_ref", "mean_logit_alt"), 
                               labels = c("inpatient", "ms2000"))
#plot the matching
pdf(paste0("FILEPATH", acause, "_matched_ms2000_", date, ".pdf"),  width=20, height=8) # this will be the name of the file

p <- ggplot(data_long_ms2000, aes(x = age, y = prevalence, color = method)) +
  geom_point() +      # To add points at data positions
  labs(title = "Prevalence by Age, logit scale, matched ms2000",
       x = "Age",
       y = "Prevalence",
       color = "Observation Method") + # Label for the legend
  theme_minimal() +   # A clean theme
  scale_color_manual(values = c("inpatient" = "green", "ms2000" = "blue"))
# Print the plot
print(p)
dev.off()

# Reshape the data to long format
data_long_ms <- df_matched_ms %>%
  pivot_longer(cols = c(mean_logit_ref, mean_logit_alt),
               names_to = "method",
               values_to = "prevalence")
# Rename the levels in the obs_method column
data_long_ms$method <- factor(data_long_ms$method, 
                                  levels = c("mean_logit_ref", "mean_logit_alt"), 
                                  labels = c("inpatient", "marketscan"))
#plot the matching
pdf(paste0("FILEPATH", acause, "_matched_ms2000_", date, ".pdf"),  width=20, height=8) # this will be the name of the file

p <- ggplot(data_long_ms, aes(x = age, y = prevalence, color = method)) +
  geom_point() +      # To add points at data positions
  labs(title = "Prevalence by Age, logit scale, matched ms2000",
       x = "Age",
       y = "Prevalence",
       color = "Observation Method") + # Label for the legend
  theme_minimal() +   # A clean theme
  scale_color_manual(values = c("inpatient" = "green", "marketscan" = "red"))
# Print the plot
print(p)
dev.off()
#9. create study_id 
df_matched_ms2000 <- as.data.table(df_matched_ms2000)
df_matched_ms2000[, id := .GRP, by = c("nid_ref", "nid_alt")]

df_matched_ms <- as.data.table(df_matched_ms)
df_matched_ms[, id := .GRP, by = c("nid_ref", "nid_alt")]

#10. fill in cv_ms2000 and cv_markestcan
df_matched_ms[(is.na(cv_marketscan)), `:=` (cv_marketscan=0)]
df_matched_ms2000[(is.na(cv_ms2000)), `:=` (cv_ms2000=0)]

#11. clean the list of covariate variates
df_matched_ms2000 <- df_matched_ms2000[, c("logit_diff", "logit_diff_se", "dorm_alt", "dorm_ref", "age_start", "sex_id", "id", "location_id", "field_citation_value", "nid_ref", "nid_alt", "obs_method", "age")]
df_matched_ms <- df_matched_ms[, c("logit_diff", "logit_diff_se", "dorm_alt", "dorm_ref", "age_start", "sex_id", "id", "location_id", "field_citation_value", "nid_ref", "nid_alt", "obs_method", "age")]




# FORMAT DATA FOR MRBRT -----------------------------------------------------------------------------
#cw$CWData() formats meta-regression data  
dat1_ms2000 <- cw$CWData(df = df_matched_ms2000, 
                         obs = "logit_diff",                         #matched differences in logit space
                         obs_se = "logit_diff_se",                   #SE of matched differences in logit space
                         alt_dorms = "dorm_alt",                     #var for the alternative def/method
                         ref_dorms = "dorm_ref",                     #var for the reference def/method
                         covs = list("age", "sex_id"),                         #list of (potential) covariate columes 
                         study_id = "id" )

dat1_ms <- cw$CWData(df = df_matched_ms, 
                     obs = "logit_diff",                         
                     obs_se = "logit_diff_se",                 
                     alt_dorms = "dorm_alt",                    
                     ref_dorms = "dorm_ref",                    
                     covs = list("age", "sex_id"), 
                     study_id = "id" )

# RUN MR-BRT -----------------------------------------------------------------------------
#cw$CWModel() runs mrbrt model
fit1_ms2000 <- cw$CWModel(
  cwdata = dat1_ms2000,                              #result of CWData() function call
  obs_type = "diff_logit",                           #must be "diff_logit" or "diff_log"
  cov_models = list(
    cw$CovModel("age"),
    cw$CovModel("sex_id"),
    cw$CovModel("intercept")),            #specify covariate details
  gold_dorm = "inpatient" )                         #level of "dorm_ref" that is the gold standard

fit1_ms <- cw$CWModel(
  cwdata = dat1_ms,                             
  obs_type = "diff_logit",                   
  cov_models = list(
    cw$CovModel("age"),
    cw$CovModel("sex_id"),
    cw$CovModel("intercept")),      
  gold_dorm = "inpatient" )   

fit1_ms2000$fit(inlier_pct = 0.9) #trimming
fit1_ms$fit(inlier_pct = 0.9)

## VIEW MR-BRT MODEL OUTPUTS -------------------------------------------------------------------
results_ms <- fit1_ms$create_result_df() %>%
  select(dorms, cov_names, beta, beta_sd, gamma)

results_ms2000 <- fit1_ms2000$create_result_df() %>%
  select(dorms, cov_names, beta, beta_sd, gamma)

## SAVE MR-BRT MODEL OUTPUTS -------------------------------------------------------------------
save_model_RDS <- function(results, path, model_name){
  names <- c("beta", 
             "beta_sd", 
             "constraint_mat",
             "cov_mat",
             "cov_models",
             "cwdata",
             "design_mat",
             "fixed_vars",
             "gamma",
             "gold_dorm", 
             "lt",
             "num_vars",
             "num_vars_per_dorm",
             "obs_type",
             "order_prior",
             "random_vars",
             "relation_mat",
             "var_idx",
             "vars",
             "w")
  model <- list()
  for (name in names){
    if(is.null(results[[name]])) {
      message(name, " is NULL in original object, will not be included in RDS")
    }
    model[[name]] <- results[[name]]
  }
  saveRDS(model, paste0(path, "/", model_name, "/model_object.RDS"))
  message("RDS object saved to ", paste0(path, "/",model_name, "/model_object.RDS"))
  return(model)
}


save_mrbrt <- function(model, mrbrt_directory, model_name) {
  df_result <- model$create_result_df()
  write.csv(df_result, paste0(mrbrt_directory, model_name, "/df_result_crosswalk.csv"), row.names = FALSE)
  py_save_object(object = model, filename = paste0(mrbrt_directory,model_name, "/results.pkl"), pickle = "dill")
}


save_ms2000 <- save_mrbrt(fit1_ms2000, xwalk_output_dir, model_name_ms2000)
save_ms     <- save_mrbrt(fit1_ms, xwalk_output_dir, model_name_ms)

save_ms2000_rds <- save_model_RDS(fit1_ms2000, xwalk_output_dir, model_name_ms2000)
save_ms_rds     <- save_model_RDS(fit1_ms, xwalk_output_dir, model_name_ms)

# Funnel plots
repl_python()

plots <- import("crosswalk.plots")
obs_method <- "marketscan"
plots$funnel_plot(
  cwmodel = fit1_ms, 
  cwdata = dat1_ms,
  obs_method =obs_method,
  continuous_variables = list("age", "sex_id"),
  plot_note = paste0("Funnel plot_appendicitis_", obs_method, "_prevalence"), 
  plots_dir = paste0("FILEPATH"), 
  file_name = paste0("Funnel_plot_", obs_method,"_prevalence_0.1trim", date),
  write_file = TRUE
)

plots <- import("crosswalk.plots")
obs_method <- "ms2000"
plots$funnel_plot(
  cwmodel = fit1_ms2000, 
  cwdata = dat1_ms2000,
  obs_method =obs_method,
  continuous_variables = list("age", "sex_id"),
  plot_note = paste0("Funnel plot_appendicitis_", obs_method, "_prevalence"), 
  plots_dir = paste0("FILEPATH"), 
  file_name = paste0("Funnel_plot_", obs_method,"_prevalence_0.1trim", date),
  write_file = TRUE
)



# Check summary statistics for logit_diff
summary(df_matched_ms2000$logit_diff)

# Check the range of logit_diff to understand its distribution
print(range(df_matched_ms2000$logit_diff))
print(range(df_matched_ms2000$logit_diff_se))
print(range(df_matched_ms$logit_diff))
print(range(df_matched_ms$logit_diff_se))

# APPLY ADJUSTMENT TO MARKESTCAN DATA IN ORIGINAL DATSET -----------------------------------------------
#calculate offset value for observations with mean zero
offset <- min(df[df$mean>0,mean])/2 
#replace mean=0 in dt_zero to offset
zero_type <- unique(df[mean==0, obs_method])
message(paste0("Zero rows exist in ", list(zero_type), " case definition(s) in the original dataset"))


if (nrow(df_zero[obs_method!="inpatient",]!=0)) {
  zero_rows <- nrow(df[mean == 0 & obs_method!="inpatient", ]) 
  message(paste0("There are ", zero_rows, " alternative data points that will be offset by ", offset))
  df_zero[(mean == 0 & obs_method!="inpatient"), `:=` (mean = offset, lower=NA, upper=NA)]  # apply offset to mean zero data points in alternative case definition
} else {
  message(paste0("All mean=0 observations are within inpatient case definition, thus no offset transformation will happen"))
  
}

#recalculate SE of the mean=0 observations in the alternative case definition with Crosswalk results
df_zero_ref <- subset(df_zero, obs_method=="inpatient")
df_zero_alt <- subset(df_zero, obs_method!="inpatient")
table(df_zero_alt$obs_method)


  df_zero_alt_ms2000_adj <- subset(df_zero_alt, obs_method=="ms2000")
  setnames(df_zero_alt_ms2000_adj, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))
  df_zero_alt_ms2000_adj[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- fit1_ms2000$adjust_orig_vals(
    df = df_zero_alt_ms2000_adj,            # original data with obs to be adjusted
    orig_dorms = "obs_method", # name of column with (all) def/method levels
    orig_vals_mean = "orig_mean",  # original mean
    orig_vals_se = "orig_standard_error"  # standard error of original mean
  )
  df_zero_alt_ms2000_adj[(orig_mean ==offset), `:=` (mean=0, orig_mean = 0)]
  
  

  df_zero_alt_ms_adj <- subset(df_zero_alt, obs_method=="marketscan")
  setnames(df_zero_alt_ms_adj, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))
  df_zero_alt_ms_adj[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- fit1_ms$adjust_orig_vals(
    df = df_zero_alt_ms_adj,            # original data with obs to be adjusted
    orig_dorms = "obs_method", # name of column with (all) def/method levels
    orig_vals_mean = "orig_mean",  # original mean
    orig_vals_se = "orig_standard_error"  # standard error of original mean
  )
  df_zero_alt_ms_adj[(orig_mean ==offset), `:=` (mean=0, orig_mean = 0)]
  


#head(df_zero_alt_ms_adj[, .(mean, standard_error, upper, lower, definition, cases, orig_mean, orig_standard_error)]) check

#########################################################################################
##STEP 3: Apply crosswalk coefficients to original data
setnames(df_nonzero_ms2000, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))
df_nonzero_ms2000[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- fit1_ms2000$adjust_orig_vals(
  df = df_nonzero_ms2000,                      # original data with obs to be adjusted
  orig_dorms = "obs_method",            # name of column with (all) def/method levels
  orig_vals_mean = "orig_mean",         # original mean
  orig_vals_se = "orig_standard_error") # standard error of original mean


setnames(df_nonzero_ms, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))
df_nonzero_ms[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- fit1_ms$adjust_orig_vals(
  df = df_nonzero_ms,          
  orig_dorms = "obs_method", 
  orig_vals_mean = "orig_mean",  
  orig_vals_se = "orig_standard_error")

head(df_nonzero_ms[, .(mean, standard_error, upper, lower, obs_method, cases, orig_mean, orig_standard_error, diff, diff_se)]) #check
head(df_nonzero_ms2000[, .(mean, standard_error, upper, lower, obs_method, cases, orig_mean, orig_standard_error, diff, diff_se)]) #check

df_nonzero_ms2000[, `:=` (upper=NA, lower=NA, uncertainty_type_value = NA, effective_sample_size = NA, note_modeler ="GBD 2023 crosswalked")]
df_nonzero_ms[, `:=` (upper=NA, lower=NA, uncertainty_type_value = NA, effective_sample_size = NA, note_modeler = " GBD 2023 crosswalked")]

# PREPARE FINAL POST-CROSSWALK DATASET ----------------------------------------------------------------------------
df_nonzero_ref[, `:=` (orig_mean=mean, orig_standard_error = standard_error)]
df_zero_ref[, `:=` (orig_mean=mean, orig_standard_error = standard_error)]
head(df_nonzero_ref[, .(mean, orig_mean, standard_error, orig_standard_error)]) #check
head(df_zero_ref[, .(mean, orig_mean, standard_error, orig_standard_error)]) #check


df_final <- as.data.table(rbind.fill(df_nonzero_ms2000, df_nonzero_ms, df_nonzero_ref, df_zero_ref, df_zero_alt_ms_adj))
# df_final2 <- as.data.table(rbind.fill(df_nonzero_ms2000, df_nonzero_ms, df_nonzero_ref, df_zero_ref, df_zero_alt_ms_adj, df_zero_alt_ms2000_adj))
## clean for Epi Uploader validation
df_final <- df_final[(obs_method!="inpatient"), `:=` (lower = NA, upper = NA)]
df_final[is.na(lower), uncertainty_type_value := NA]
df_final[(obs_method!="inpatient"), `:=` (crosswalk_parent_seq = seq)]
df_final[(obs_method=="inpatient"), `:=` (crosswalk_parent_seq = NA)]

df_final$standard_error[df_final$standard_error>1] <-1

write.xlsx(df_final, xwalk_output_dir, sheetName = "extraction", colNames=TRUE)



#########################################################################################

envelope_new<-read.xlsx("FILEPATH")
#table(addNA(envelope_new$nid))
#table(df_final$nid)
envelope_new<-envelope_new%>%mutate(nid=ifelse(merged_nid=="na",nid,merged_nid))
envelope_distinct_new <- envelope_new %>% distinct(nid, uses_env,.keep_all = TRUE)
envelope_distinct_new$nid<-as.numeric(envelope_distinct_new$nid)
df_final<-left_join(df_final,envelope_distinct_new,by="nid")
#table(addNA(df_final$ms_hcup),addNA(df_final$uses_env))
#table(addNA(df_final$is_outlier))
df_final_exclude_envelope<-df_final
df_final_exclude_envelope <- df_final_exclude_envelope %>%
  mutate(is_outlier = ifelse(is.na(uses_env), 0, ifelse(uses_env == 1, 1, 0)))

df_final_exclude_envelope <- df_final_exclude_envelope[df_final_exclude_envelope$location_name != "Botswana", ]
write.xlsx(df_final_exclude_envelope, mad_filepath, rowNames = FALSE, sheetName = "extraction")

#table(addNA(df_final_exclude_envelope$uses_env),addNA(df_final_exclude_envelope$is_outlier))
#########################################################################################

########################################################################################
mad_filepath <- paste0(save_outdir,"description", date, ".xlsx")
write.xlsx(df_final_exclude_envelope, mad_filepath, rowNames = FALSE, sheetName = "extraction")

#STEP 5: Upload MAD-outliered new data 
description <- paste0('description') #description needed for each crosswalk version; this is what is going to show up in Dismod
result <- save_crosswalk_version(step2_bundle_version, mad_filepath, description)

print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))

