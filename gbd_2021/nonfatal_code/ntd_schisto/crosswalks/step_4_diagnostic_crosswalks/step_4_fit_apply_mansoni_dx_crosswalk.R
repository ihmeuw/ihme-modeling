# Schisto diagnostic Crosswalk - S mansoni

# - Subseting sex-split data
# - Selecting Diagnostic Criteria
# - MR-BRT Model Fit
# - Adjust Data and Evaluate Plots

### ----------------------- Set-Up ------------------------------

rm(list=ls())
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_population.R")
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_crosswalk_version.R')

library(crosswalk, lib.loc = "FILEPATH/packages/")
library(dplyr)
library(data.table)
library(openxlsx)
library(stringr)
library(tidyr)
library(boot)
library(ggplot2)

### NETWORK Crosswalk- Schistosomiasis

### ----------------------- Consolidate Data ------------------------------

#import data with dx comparisons

dt<-read.csv("FILEPATH")
#subset to variables we need

dt2<-dt %>% select(NID, year, species, cases_1, sample_size_1, cases_2, sample_size_2, dx_1_alt, dx_2_ref, group_id)

#mean 2 is the reference 

#create mean 1 and mean 2 - this is prevalence
dt2$mean_1<-dt2$cases_1/dt2$sample_size_1
dt2$mean_2<-dt2$cases_2/dt2$sample_size_2

#double check proportion SE formula
dt2$se_1<-sqrt(((dt2$mean_1*(1-dt2$mean_1))/dt2$sample_size_1))
dt2$se_2<-sqrt(((dt2$mean_2*(1-dt2$mean_2))/dt2$sample_size_2))

#drop mean_1=0, mean_2 =0
dt2<-dt2[dt2$mean_1>0,]
dt2<-dt2[dt2$mean_2>0,]

head(dt2)


### ----------------------- Model Fit + Params ------------------------------

#note we are using linear_to_logit since we are modeling prev

dat_diff <- as.data.frame(cbind(
  delta_transform(
    mean = dt2$mean_1, 
    sd = dt2$se_1,
    transformation = "linear_to_logit" ),
  delta_transform(
    mean = dt2$mean_2, 
    sd = dt2$se_2,
    transformation = "linear_to_logit")
))

#recode names 
names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")


dt2[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = dat_diff, 
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref" )

##############################
#Model fit
################################


#Mansoni
dt_mansoni<-dt2[dt2$species=="mansoni",]

df2 <- CWData(
  df = dt_mansoni,         # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "dx_1_alt",     # column name of the variable indicating the alternative method
  ref_dorms = "dx_2_ref",     # column name of the variable indicating the reference method
  covs = list("year"),        # names of columns to be used as covariates later - year is a place holder for covariate to test functionality
  study_id = "NID",     # name of the column indicating group membership, usually the matching groups
  add_intercept=TRUE
)



fit2 <- CWModel(
  cwdata = df2,            # object returned by `CWData()`
  obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept")),

  gold_dorm = "kk3",   # the level of `ref_dorms` that indicates it's the gold standard

)

table(dt_mansoni$dx_1_alt)
table(dt_mansoni$dx_2_ref)

fit2$fixed_vars

fit2$gamma

 
#Calculate 95% UI
#formula 95ci = beta_mean +/- 1.96 * sqrt(beta_se^2 + gamma)
df_result <- fit2$create_result_df()
gamma_val <- df_result$gamma[1]
ci_calc2 <- subset(df_result, select = c(dorms, beta, beta_sd))
ci_calc2 <- mutate(ci_calc2, gamma = gamma_val)
ci_calc2 <- mutate(ci_calc2, ci_lower = beta - 1.96*sqrt((beta_sd^2) + gamma)) 
ci_calc2 <- mutate(ci_calc2, ci_upper = beta + 1.96*sqrt((beta_sd^2) + gamma))


#funnel plot

repl_python()

plots <- import("crosswalk.plots")


plots$funnel_plot(
  cwmodel = fit2, 
  cwdata = df2,
  continuous_variables = list("year"),
  obs_method = 'elisa',
  plot_note = 'Funnel plot Mansoni - dx split', 
  plots_dir = 'FILEPATH', 
  file_name = "funnel_plot_mansoni_diag_elisa",
  write_file = TRUE
)

### ----------------------- Apply Fit to Adjust Data ------------------------------

#apply crosswalk to dataset -
orig_dt<-read.xlsx("FILEPATH")


table(orig_dt$case_name)
orig_dt<-orig_dt[orig_dt$case_diagnostics!="not specified",]


orig_dt_mansoni<-orig_dt[orig_dt$case_name=="S mansoni"|orig_dt$case_name=="S intercalatum"|orig_dt$case_name=="S guineensis",]

orig_dt_mansoni$year<-orig_dt_mansoni$year_start

table(orig_dt$case_diagnostics)


#keep : kk3, kk2, kk1, cca, elisa, fec, pcr, sec


orig_dt_mansoni<-orig_dt_mansoni[orig_dt_mansoni$mean>0,]

preds1 <- adjust_orig_vals(
  fit_object = fit2, # object returned by `CWModel()`
  df = orig_dt_mansoni,
  orig_dorms = "case_diagnostics",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)


orig_dt_mansoni[, c("meanvar_adjusted", "sdvar_adjusted", "pred_logit", "pred_se_logit", "a")] <- preds1


##plot adjustments

ggplot(orig_dt_mansoni, aes(x=mean, y=meanvar_adjusted, color=case_diagnostics)) +
  geom_point()

##adding note_modeler
orig_dt_mansoni <- data.table(orig_dt_mansoni)
orig_dt_mansoni[, note_modeler := ifelse(orig_dt_mansoni$case_diagnostics!= "kk3", paste0(note_modeler, "| dx-xwalk"), note_modeler)]
