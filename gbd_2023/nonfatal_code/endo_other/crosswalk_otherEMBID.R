################################################################################################################################
### Purpose:      CROSSWALKING OTHER EMBID USING UPDATED CLINICAL INFORMATICS DATA
################################################################################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "/home/j/"
  h <- "~/"
  l <- "/ihme/limited_use/"
} else {
  j <- "J:/"
  h <- "H:/"
  l <- "L:/"
}


# SOURCE FUNCTIONS AND LIBRARIES --------------------------------------------------
invisible(sapply(list.files("FILEPATH", full.names = T), source))
pacman::p_load(data.table, ggplot2, dplyr, readr, tidyverse, openxlsx, tidyr, plyr, readxl, boot, gtools, msm, Hmisc, gridExtra)
source("FILEPATH/cw_mrbrt_helper_functions.R")


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
} #back-calculate cases and sample size from SE
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
ave_mrbrt <- function(model, mrbrt_directory, model_name) {
  df_result <- model$create_result_df()
  write.csv(df_result, paste0(mrbrt_directory,"/",model_name, "/df_result_crosswalk.csv"), row.names = FALSE)
  py_save_object(object = model, filename = paste0(mrbrt_directory, "/",model_name, "/results.pkl"), pickle = "dill")
}
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
  saveRDS(model, paste0(path,"/",model_name, "/model_object.RDS"))
  message("RDS object saved to ", paste0(path,"/",model_name, "/model_object.RDS"))
  return(model)
}

# SOURCE CROSSWALK PACKAGE (Latest version) --------------------------------------------------------
library(reticulate)
reticulate::use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")


# SET OBJECTS -----------------------------------------------------------------------------------------
date <- gsub("-", "_", Sys.Date())
release_id = 16   #GBD 2022
acause <-"endo_other"
cause_name <- "Other_EMBID"
b_id <- 7544 #bundle ID used for the cause
measures <- "prevalence" #measure for which clinical informatics data are prepped for
save_outdir <- FILEPATH
outdir <- paste0("FILEPATH")
if (!dir.exists(outdir)) dir.create(outdir)


#crosswalk
cov_names <- c("cv_marketscan", "cv_ms2000") 
mrbrt_cv <- "intercept_age_sex"
xwalk_type<-"logit"
year_range_ms_other <- 3
year_range_ms2000 <- 5
trim <- 0.1

model_name_ms2000 <- paste0("/crosswalk_MS2000_", date)
dir.create(paste0(outdir, model_name_ms2000))
model_name_ms_other <- paste0("crosswalk_MSother_", date)
dir.create(paste0(outdir, model_name_ms_other))


## Sources that utilizes hospital envelope
envelope_new<-read.xlsx("/FILEPATH/clinical_version_id_3_NIDs_uses_env.xlsx")
table(addNA(envelope_new$nid))
envelope_new<-envelope_new%>%mutate(nid=ifelse(merged_nid=="na",nid,merged_nid))
envelope_distinct_new <- envelope_new %>% distinct(nid, uses_env,.keep_all = TRUE)
envelope_distinct_new$nid<-as.numeric(envelope_distinct_new$nid)


# DATA PROCESSING FUNCTIONS ---------------------------------------------------------------------------
save_bv <- save_bundle_version(b_id, include_clinical=c("claims", "inpatient"))
print(sprintf('Bundle version ID: %s', save_bv$bundle_version_id))
bundle_version_id <- OBJECT 
orig_dt <- get_bundle_version(bundle_version_id, export = FALSE)
checkpoint <- copy(orig_dt)
table(orig_dt$measure)
dt <- subset(orig_dt, measure==measures)
dt <- subset(dt, clinical_data_type!="claims - flagged")


# RUN DATA SETUP --------------------------------------------------------------------------------------
#Fill in missing values for sample_size, SE, and cases
dt <- get_cases_sample_size(dt)
dt <- get_se(dt)
dt <- calculate_cases_fromse(dt)


#Calculate mid-year for matching
dt[, mid_year := (year_start + year_end) / 2]


#Create new variables for reference (HCUP, CMS and NHDS-national), Marketscan 2000 data, Marketscan 2010-2019 data
dt = within(dt, {cv_ms2000 = ifelse(nid==244369, 1, 0)})
dt = within(dt, {cv_marketscan = ifelse((nid==244370 | nid== 336850| nid== 244371| nid== 336849| nid== 336848| nid== 336847| nid== 408680 | nid==433114 | nid==494351  | nid==494352), 1, 0)}) #494351 and 4904352 are new NIDs added in GBD 2022
dt = within(dt, {cv_reference = ifelse(cv_marketscan==0 & cv_ms2000==0, 1, 0)})


##create "definition" variable
dt[(cv_ms2000==1), `:=` (definition = "ms2000")]
dt[(cv_marketscan==1), `:=` (definition = "ms_other")]
dt[(cv_reference==1), `:=` (definition = "reference")]


#Subset dataset to observations where mean is not zero to obtain crosswalk coefficients
dt_zero <- subset(dt, mean ==0)
unique(dt_zero$definition) #check if any of the mean = 0 data points are from alternative case definitions
dt_nonzero <- subset(dt, mean !=0) #drop all rows of data with mean greater than 1
unique(dt_nonzero$definition) #check if any of the mean = 0 data points are from alternative case definitions


#Create a binary numeric variable for sex
dt_zero[(sex=="Male"), `:=` (sex_id =1)] 
dt_zero[(sex=="Female"), `:=` (sex_id =2)]

dt_nonzero[(sex=="Male"), `:=` (sex_id =1)] 
dt_nonzero[(sex=="Female"), `:=` (sex_id =2)]


#Subset dataset to remove MS data for people who are older than age 65 years
#------ Rationale: for other EMBID, similar to other prevalence bundles like cirrhosis (both total and decompensated), prevalence estimates from HCUP and CMS differ a lot and prevalence estimates from Marketscan fall in between the two sources.
#------            for neuro/sensory causes that has the same pattern across HCUP, CMS and Marketscan, Theo and CJLM advised they remove MS data for people who are older than 65 years of age and 
#------            only use HCUP and CMS to estimate prevalence for the older age groups; and use Marketscan only for those below age 65 and crosswalk it towards HCUP
dt_nonzero_65plus <- subset(dt_nonzero, ((cv_ms2000==1 | cv_marketscan==1) & age_start>64))
message(paste0("There are ", nrow(dt_nonzero_65plus), " observations that will be dropped from crosswalk version because they are Marketscan cases who are ages 65 years or above"))
dt_nonzero <- subset(dt_nonzero, !((cv_ms2000==1 | cv_marketscan==1) & age_start>64))



#Logit-transform mean and SE using delta-transfomration:
dt_nonzero[, c("mean_logit", "se_logit")] <- cw$utils$linear_to_logit(
                                            mean = array(dt_nonzero$mean), 
                                            sd = array(dt_nonzero$standard_error))
  
  
#match alt to ref
#1. subset datasets based on case definition
ref_orig<- subset(dt_nonzero, cv_reference==1)
alt_ms2000_orig <- subset(dt_nonzero, cv_ms2000==1)
alt_ms_orig <- subset(dt_nonzero, cv_marketscan==1)


#2. clean ref and alt datasets
alt_ms2000 <- alt_ms2000_orig[, c("nid", "location_id", "sex","sex_id", "age_start", "age_end",  "mid_year", "measure", "mean", "standard_error", "cv_ms2000","mean_logit", "se_logit", "definition")]
alt_ms <- alt_ms_orig[, c("nid", "location_id", "sex","sex_id", "age_start", "age_end", "mid_year", "measure", "mean", "standard_error",  "cv_marketscan","mean_logit", "se_logit", "definition")]
ref <- copy(ref_orig)
ref[, c("cv_ms2000", "cv_marketscan")] <- NULL


#3. change the mean and standard error variables in both ref and alt datasets for clarification
setnames(ref, c("mean", "standard_error", "mid_year", "nid", "mean_logit", "se_logit"), c("prev_ref", "prev_se_ref", "mid_year_ref", "nid_ref", "mean_logit_ref", "se_logit_ref"))
setnames(alt_ms2000, c("mean", "standard_error", "mid_year", "nid", "mean_logit", "se_logit"), c("prev_alt", "prev_se_alt", "mid_year_alt", "nid_alt", "mean_logit_alt", "se_logit_alt"))
setnames(alt_ms, c("mean", "standard_error", "mid_year", "nid", "mean_logit", "se_logit"), c("prev_alt", "prev_se_alt", "mid_year_alt", "nid_alt", "mean_logit_alt", "se_logit_alt"))


#4. create a variable identifying different case definitions
alt_ms2000$dorm_alt<- "ms2000"
alt_ms$dorm_alt<- "ms_other"
ref$dorm_ref<- "reference"


#5. between-study mathces based on exact location, sex, age
df_matched_ms2000 <- merge(ref, alt_ms2000, by = c("location_id", "sex", "sex_id", "age_start", "age_end", "measure"))
df_matched_ms     <- merge(ref, alt_ms, by = c( "location_id", "sex","sex_id", "age_start", "age_end", "measure"))


#6 between-study matches based on 5-year span for ms2000 and 3-year span for ms
df_matched_ms2000 <- df_matched_ms2000[abs(df_matched_ms2000$mid_year_ref - df_matched_ms2000$mid_year_alt) <= year_range_ms2000, ] 
df_matched_ms <- df_matched_ms[abs(df_matched_ms$mid_year_ref - df_matched_ms$mid_year_alt) <= year_range_ms_other, ] 


#7. Optional: check the matches
df_matched_ms2000 %>% select(location_name, age_start, age_end, nid_ref, mid_year_ref, prev_ref, dorm_ref, mean_logit_ref, se_logit_ref, nid_alt, mid_year_alt, prev_alt, dorm_alt, mean_logit_alt, se_logit_alt)
unique(df_matched_ms2000$nid_ref)
unique(df_matched_ms2000$nid_alt)

df_matched_ms %>% select(location_name, age_start, age_end, nid_ref, mid_year_ref, prev_ref, dorm_ref, mean_logit_ref, se_logit_ref, nid_alt, mid_year_alt, prev_alt, dorm_alt, mean_logit_alt, se_logit_alt)
unique(df_matched_ms$nid_ref)
unique(df_matched_ms$nid_alt)


#8. calculate logit difference : logit(prev_alt) - logit(prev_ref)
df_matched_ms2000 <- df_matched_ms2000 %>%
                                        mutate(
                                          logit_diff = mean_logit_alt - mean_logit_ref,
                                          logit_diff_se = sqrt(se_logit_alt^2 + se_logit_ref^2))
df_matched_ms <- df_matched_ms %>%
                                        mutate(
                                          logit_diff = mean_logit_alt - mean_logit_ref,
                                          logit_diff_se = sqrt(se_logit_alt^2 + se_logit_ref^2))


#9. create study_id 
df_matched_ms2000 <- as.data.table(df_matched_ms2000)
df_matched_ms2000[, id := .GRP, by = c("nid_ref", "nid_alt")]

df_matched_ms <- as.data.table(df_matched_ms)
df_matched_ms[, id := .GRP, by = c("nid_ref", "nid_alt")]



#10. clean the list of covariate variates
df_matched_ms2000 <- df_matched_ms2000[, c("logit_diff", "logit_diff_se", "dorm_alt", "dorm_ref", "age_start", "sex_id", "id", "location_id", "field_citation_value", "nid_ref", "nid_alt")]
df_matched_ms <- df_matched_ms[, c("logit_diff", "logit_diff_se", "dorm_alt", "dorm_ref", "age_start", "sex_id", "id", "location_id", "field_citation_value", "nid_ref", "nid_alt")]


# FORMAT DATA FOR MRBRT -----------------------------------------------------------------------------
#CWData() formats meta-regression data 
dat1_ms2000 <- cw$CWData(df = df_matched_ms2000, 
                      obs = "logit_diff",                         #matched differences in logit space
                      obs_se = "logit_diff_se",                   #SE of matched differences in logit space
                      alt_dorms = "dorm_alt",                     #var for the alternative def/method
                      ref_dorms = "dorm_ref",                     #var for the reference def/method
                      covs = list("age_start", "sex_id"),         #list of (potential) covariate columes 
                      study_id = "id" )

dat1_ms <- cw$CWData(df = df_matched_ms, 
                  obs = "logit_diff",                         #matched differences in logit space
                  obs_se = "logit_diff_se",                   #SE of matched differences in logit space
                  alt_dorms = "dorm_alt",                     #var for the alternative def/method
                  ref_dorms = "dorm_ref",                     #var for the reference def/method
                  covs = list("age_start", "sex_id"),         #list of (potential) covariate columes 
                  study_id = "id" )


# RUN MR-BRT -----------------------------------------------------------------------------
fit1_ms2000 <- cw$CWModel(cwdata = dat1_ms2000,                              #result of CWData() function call
                          obs_type = "diff_logit",                           #must be "diff_logit" or "diff_log"
                          cov_models = list(
                                            cw$CovModel("age_start"),
                                            cw$CovModel("sex_id"),
                                            cw$CovModel("intercept")),       #specify covariate details
                          gold_dorm = "reference" )                          #level of "dorm_ref" that is the gold standard

fit1_ms     <- cw$CWModel(cwdata = dat1_ms,                                  #result of CWData() function call
                          obs_type = "diff_logit",                           #must be "diff_logit" or "diff_log"
                          cov_models = list(
                                            cw$CovModel("age_start"),
                                            cw$CovModel("sex_id"),
                                            cw$CovModel("intercept")),       #specify covariate details
                          gold_dorm = "reference" )                          #level of "dorm_ref" that is the gold standard


fit1_ms2000$fit(inlier_pct = (1-trim))
fit1_ms$fit(inlier_pct = (1-trim))


## VIEW MR-BRT MODEL OUTPUTS -------------------------------------------------------------------
results_ms2000 <- fit1_ms2000$create_result_df() %>%
  select(dorms, cov_names, beta, beta_sd, gamma)

results_ms <- fit1_ms$create_result_df() %>%
  select(dorms, cov_names, beta, beta_sd, gamma)


## SAVE MR-BRT MODEL OUTPUTS -------------------------------------------------------------------
save_ms2000 <- save_mrbrt(fit1_ms2000, outdir, model_name_ms2000)
save_ms     <- save_mrbrt(fit1_ms, outdir, model_name_ms_other)

save_ms2000_rds <- save_model_RDS(fit1_ms2000, outdir, model_name_ms2000)
save_ms_rds     <- save_model_RDS(fit1_ms, outdir, model_name_ms_other)


# APPLY ADJUSTMENT TO MARKESTCAN DATA IN ORIGINAL DATSET -----------------------------------------------
#calculate offset value for observations with mean zero
offset <- min(dt[dt$mean>0,mean])/2 


#replace mean=0 in dt_zero to offset
zero_type <- unique(dt[mean==0, definition])
message(paste0("Zero rows exist in ", list(zero_type), " case definition(s) in the original dataset"))

#recalculate SE of the mean=0 observations in the alternative case definition with Crosswalk results
dt_zero_ref <- subset(dt_zero, definition=="reference")
dt_zero_alt <- subset(dt_zero, definition!="reference")
table(dt_zero_alt$definition)


#apply crosswalk coefficients to original data
setnames(alt_ms2000_orig, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))
setnames(alt_ms_orig, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))

alt_ms2000_orig[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- fit1_ms2000$adjust_orig_vals(
  df = alt_ms2000_orig,            # original data with obs to be adjusted
  orig_dorms = "definition", # name of column with (all) def/method levels
  orig_vals_mean = "orig_mean",  # original mean
  orig_vals_se = "orig_standard_error"  # standard error of original mean
)


alt_ms_orig[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- fit1_ms$adjust_orig_vals(
  df = alt_ms_orig,            # original data with obs to be adjusted
  orig_dorms = "definition", # name of column with (all) def/method levels
  orig_vals_mean = "orig_mean",  # original mean
  orig_vals_se = "orig_standard_error"  # standard error of original mean
)

head(alt_ms_orig[, .(mean, standard_error, upper, lower, definition, cases, orig_mean, orig_standard_error, diff, diff_se)]) #check
head(alt_ms2000_orig[, .(mean, standard_error, upper, lower, definition, cases, orig_mean, orig_standard_error, diff, diff_se)]) #check

alt_ms2000_orig[, `:=` (upper=NA, lower=NA, uncertainty_type_value = NA, effective_sample_size = NA, note_modeler ="GBD 2022 crosswalked")]
alt_ms_orig[, `:=` (upper=NA, lower=NA, uncertainty_type_value = NA, effective_sample_size = NA, note_modeler = " GBD 2022 crosswalked")]



# PREPARE FINAL POST-CROSSWALK DATASET ----------------------------------------------------------------------------
ref_orig[, `:=` (orig_mean=mean, orig_standard_error = standard_error)]
dt_zero_ref[, `:=` (orig_mean=mean, orig_standard_error = standard_error)]
head(ref_orig[, .(mean, orig_mean, standard_error, orig_standard_error)]) #check
head(dt_zero_ref[, .(mean, orig_mean, standard_error, orig_standard_error)]) #check


#df_cw <- as.data.table(rbind.fill(alt_ms2000_orig, alt_ms_orig, ref_orig, dt_zero_ref, dt_zero_alt_ms_adj, dt_zero_alt_ms2000_adj))
df_cw <- as.data.table(rbind.fill(alt_ms2000_orig, alt_ms_orig, ref_orig, dt_zero_ref))

nrow(df_cw) + nrow(dt_nonzero_65plus) == nrow(dt) #check to see if you got all the observations from the original dataset


# CLEAN FOR EPI UPLOADER VALIDATION -------------------------------------------------------------------------------
df_cw <- df_cw[(definition!="reference"), `:=` (lower = NA, upper = NA)]
df_cw[is.na(lower), uncertainty_type_value := NA]
df_cw[(definition!="reference"), `:=` (crosswalk_parent_seq = seq)]
df_cw[(definition=="reference"), `:=` (crosswalk_parent_seq = NA)]

df_cw$standard_error[df_cw$standard_error>1] <-1

data_filepath <- paste0(save_outdir, cause_name, "_mkscn_xw_", date, ".xlsx")
write.xlsx(df_cw, data_filepath, rowNames = FALSE, sheetName = "extraction")


# CREATE DIAGNOSTIC PLOTS -----------------------------------------------------------------------------------------
df_cw <- as.data.table(df_cw)
dt_plot <- subset(df_cw, (nid== 284421 | nid== 284422| nid== 499633| nid== 499634 | nid==471277 |nid== 471267 |nid== 471258 |nid== 470991 |nid== 470990) | definition == "ms_other" | definition == "ms2000")
dt_plot[(nid== 284421 | nid== 284422| nid== 499633| nid== 499634), `:=` (definition = "HCUP")]
dt_plot[(nid==471277 |nid== 471267 |nid== 471258 |nid== 470991 |nid== 470990), `:=` (definition = "CMS")]
table(dt_plot$definition)

locs <- unique(dt_plot[, location_id])
group.colors <- c("ms_other" = "red3","ms2000"="orange3", "HCUP" = "blue2", "CMS" ="green3")
p <-1

# Unadjusted values
pdf(paste0(outdir, cause_name, "_vetting_plots_pre_crosswalk_", date, ".pdf"),  width=20, height=8) # this will be the name of the file
for (loc in locs) {
  message(p)
  p <- p+1
  data <- copy(dt_plot[location_id == loc, ])
  loc_name <- data$location_name
  
  ymax <- max(data$orig_mean)
  unadj_data_plot <- ggplot() +
    geom_point(data, mapping = aes(x = age_start, y = (orig_mean), color = definition)) +
    facet_wrap(~sex) +
    scale_color_manual( values = group.colors) +
    ggtitle(paste0("Unadjusted ", loc_name)) +
    xlab("Age start") + ylab("Prevalence")  + ylim(0, ymax) +
    theme_bw()  
  
  print(unadj_data_plot)
}
dev.off()

pdf(paste0(outdir, cause_name, "_vetting_plots_post_crosswalk_", date, ".pdf"),  width=20, height=8) # this will be the name of the file
for (loc in locs) {
  message(p)
  p <- p+1
  data <- copy(dt_plot[location_id == loc, ])
  loc_name <- data$location_name
  
  ymax <- max(data$mean)
  adj_data_plot <- ggplot() +
    geom_point(data, mapping = aes(x = age_start, y = (mean), color = definition)) +
    facet_wrap(~sex) +
    scale_color_manual( values = group.colors) +
    ggtitle(paste0("Adjusted ", loc_name)) +
    xlab("Age start") + ylab("Prevalence")  + ylim(0, ymax) + 
    theme_bw() 
  
  print(adj_data_plot)
}
dev.off()


# Outlier based on hospital envelope usuage --------------------------------------------------------------
table(df_cw$nid)
prev <- df_cw %>% filter(measure=="prevalence")

## apply envelope
prev$is_outlier <- 0

prev<-left_join(prev,envelope_distinct_new,by="nid")
table(addNA(prev$is_outlier))

prev_exclude_envelope <- copy(prev)
prev_exclude_envelope <- prev_exclude_envelope %>%
                                  mutate(is_outlier = ifelse(is.na(uses_env), 0, ifelse(uses_env == 1, 1, 0)))
prev_exclude_envelope[(is_outlier==1), note_modeler := paste0(note_modeler, " | GBD 2023 refresh4, outliered because it uses hospital envelope")]

table(addNA(prev_exclude_envelope$uses_env),addNA(prev_exclude_envelope$is_outlier))

hosp_env_filepath <- FILEPATH
write.xlsx(prev_exclude_envelope, hosp_env_filepath, rowNames = FALSE, sheetName = "extraction")


decomp_desc <- DESCRIPTION
result <- save_crosswalk_version(bundle_version_id, hosp_env_filepath, decomp_desc)

print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))

