# Schisto diagnostic Crosswalk - S japonicum

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

### NETWORK Crosswalk - Schistosomiasis


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


###########################################
##### Model fit
#Japonicum

dt_japon<-dt2[dt2$species=="japonicum" | dt2$species=="mekongi",]

df4 <- CWData(
  df = dt_japon,         # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "dx_1_alt",     # column name of the variable indicating the alternative method
  ref_dorms = "dx_2_ref",     # column name of the variable indicating the reference method
  covs = list("year"),        # names of columns to be used as covariates later - year is a place holder for covariate to test functionality
  study_id = "NID",     # name of the column indicating group membership, usually the matching groups
  add_intercept=TRUE
)

fit4 <- CWModel(
  cwdata = df4,            # object returned by `CWData()`
  obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept")),
  
  gold_dorm = "iha",   # the level of `ref_dorms` that indicates it's the gold standard

)

table(dt_japon$dx_1_alt)
table(dt_japon$dx_2_ref)


fit4$fixed_vars


fit4$gamma


#Calculate 95% UI
#formula 95ci = beta_mean +/- 1.96 * sqrt(beta_se^2 + gamma)
df_result <- fit4$create_result_df()
gamma_val <- df_result$gamma[1]
ci_calc4 <- subset(df_result, select = c(dorms, beta, beta_sd))
ci_calc4 <- mutate(ci_calc4, gamma = gamma_val)
ci_calc4 <- mutate(ci_calc4, ci_lower = beta - 1.96*sqrt((beta_sd^2) + gamma)) 
ci_calc4 <- mutate(ci_calc4, ci_upper = beta + 1.96*sqrt((beta_sd^2) + gamma))

plots$funnel_plot(
  cwmodel = fit4, 
  cwdata = df4,
  continuous_variables = list("year"),
  obs_method = 'pcr',
  plot_note = 'Funnel plot japonicum - dx crosswalk', 
  plots_dir = 'FILEPATH', 
  file_name = "FILEPATH",
  write_file = TRUE
)

### ----------------------- Apply Fit to Adjust Data ------------------------------

#apply crosswalk to dataset - 

orig_dt<-read.xlsx("FILEPATH")

orig_dt_japon<-orig_dt[orig_dt$case_name=="S japonicum" | orig_dt$case_name=="S mekongi",]


orig_dt_japon$year<-orig_dt_japon$year_start

table(orig_dt_japon$case_diagnostics)


#drop mean < 0 or =0
orig_dt_japon<-orig_dt_japon[orig_dt_japon$mean>0,]

preds4 <- adjust_orig_vals(
  fit_object = fit4, # object returned by `CWModel()`
  df = orig_dt_japon,
  orig_dorms = "case_diagnostics",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)

orig_dt_japon[, c("meanvar_adjusted", "sdvar_adjusted", "pred_logit", "pred_se_logit", "a")] <- preds4


##plot adjustments

ggplot(orig_dt_japon, aes(x=mean, y=meanvar_adjusted, color=case_diagnostics)) +
  geom_point()

#adding modelers note
orig_dt_japon <- data.table(orig_dt_japon)
orig_dt_japon[, note_modeler := ifelse(orig_dt_japon$case_diagnostics!= "iha", paste0(note_modeler, "| dx-xwalk"), note_modeler)]


####################### END #####################


###############################################################################################
###
###     APPENDING DATA FOR ALL SPECIES
###
######################################################################

# Append species
final_dx_xwalk <- rbind(orig_dt_mansoni, orig_dt_hema, orig_dt_japon)
final_dx_xwalk <- data.table(final_dx_xwalk)

#calculating adjusted cases

final_dx_xwalk [, cases_adj := (meanvar_adjusted) *(effective_sample_size)]

#calculating lower and upper

final_dx_xwalk[, upper_adj := meanvar_adjusted + 1.96*(sdvar_adjusted)]
final_dx_xwalk[, lower_adj := meanvar_adjusted - 1.96*(sdvar_adjusted)]


##### 

final_dx_xwalk1 <- subset(final_dx_xwalk, note_modeler == "sexsplit" | note_modeler =="")
final_dx_xwalk1<-final_dx_xwalk1 %>% 

  select(-meanvar_adjusted, -sdvar_adjusted, -cases_adj, -a, -lower_adj, -upper_adj, -pred_logit, -pred_se_logit) 


final_dx_xwalk2 <- subset(final_dx_xwalk, note_modeler != "sexsplit" & note_modeler !="")


final_dx_xwalk2<-final_dx_xwalk2 %>% 

  select(-mean, -standard_error, -cases, -a, -lower, -upper, -pred_logit, -pred_se_logit) %>%
  rename(mean = meanvar_adjusted, cases = cases_adj, standard_error=sdvar_adjusted, upper = upper_adj, lower = lower_adj)

final_dx_xwalk_a <- rbind(final_dx_xwalk1, final_dx_xwalk2)


#no cases mean >1
test <- subset(final_dx_xwalk_a, mean>1)

#upper limit to 1
final_dx_xwalk_a$upper[final_dx_xwalk_a$upper>1] <- 1

#lower bound 0 to lower
final_dx_xwalk_a$lower[final_dx_xwalk_a$lower<0] <- 0


#saving as a flat file
openxlsx::write.xlsx(final_dx_xwalk_a, sheetName = "extraction", file = paste0("FILEPATH"))

