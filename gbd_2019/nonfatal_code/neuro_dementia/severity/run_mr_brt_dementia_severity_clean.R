## ---------------------------------------------------------------------------------------------------- ##
## RUN MR_BRT LOGIT MODELS FOR DEMENTIA SEVERITY SPLITS, OUTPUT PLOTS AND CVS
##
## Author: USERNAME, sourcing from USERNAME
## 06/20/2019
## ---------------------------------------------------------------------------------------------------- ##


## SET UP ENVIRONMENT --------------------------------------------------------------------------------- ##

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_drive <- "FILEPATH"
  h_drive <- "FILEPATH"
} else {
  j_drive <- "FILEPATH"
  h_drive <- "FILEPATH"
}

library(data.table)
library(ggplot2)
library(msm)
library(readr)
library(readxl)
date <- gsub("-", "_", Sys.Date())

## SET OBJECTS ---------------------------------------------------------------------------------------- ##

dem_dir <- paste0("FILEPATH")
severity_dir <- paste0("FILEPATH")
fn_dir <- "FILEPATH"
helper_dir <- paste0("FILEPATH")
severity_master <- paste0("FILEPATH")
beatrixh_logs <- "FILEPATH"
mr_brt_dir <- paste0("FILEPATH")
saved_models_dir <- paste0("FILEPATH")


draws <- paste0("draw_", 0:999)
all_severities <- c("mild","mod","severe")


## SOURCE FNS ----------------------------------------------------------------------------------------- ##

functs <- c("run_mr_brt_function", "check_for_outputs_function", "check_for_preds_function", "cov_info_function",
            "load_mr_brt_outputs_function", "load_mr_brt_preds_function", "predict_mr_brt_function")
for (funct in functs){
  source(paste0(helper_dir, funct, ".R"))
}

source(paste0(fn_dir, "get_age_metadata.R"))
source(paste0(fn_dir, "get_envelope.R"))
source(paste0(fn_dir, "get_ids.R"))
source(paste0(fn_dir, "get_population.R"))
source(paste0(severity_master,"viz_mr_brt_functions.R"))
mround <- function(x,base){ 
  base*round(x/base) 
} 

## READ IN DATA  -------------------------------------------------------------------------------------- ##

age_dt <- get_age_metadata(12)
sex_dt <- get_ids(table = "sex")

data <- fread(paste0("FILEPATH")) # CHNAGE DATE AS NECESSARY


## OUTLIER NIDS FROM CV_OUTPATIENT EXAMINATION
not_repr_outpatient <- c(ID)
data <- data[!(NID %in% not_repr_outpatient)]

## Pull in estimates from GBD2017 for comparison vizs
lastyr <- as.data.table(read_excel(paste0("FILEPATH")))

## SET UP DATA  --------------------------------------------------------------------------------------- ##

## Create sex_cov
data[sex_id==3,sex_cov:=0]
data[sex_id==2,sex_cov:=0.5]
data[sex_id==1,sex_cov:=-0.5]

## Check for sex_cov errors
sex_cov_issues <- data[is.na(sex_cov),]
if(nrow(sex_cov_issues)>0){
  print("something went wrong with sex_ids.")
  print(sex_cov_issues)
}

## Add logit vals

data[,logit_mean:=log(mean/(1-mean))]

data$logit_se <- sapply(1:nrow(data), function(i){
  mean <- data[i, mean]
  se <- data[i, standard_error]
  deltamethod(~log(x1/(1-x1)), mean, se^2)
})

## outlier cases==0 and mean==0 for logit
logit_data <- data[cases!=0 & mean!=1,]


## SEVERE MR-BRT MODEL LOGIT_MEAN  ---------------------------------------------------------------------- ##

## Subset to severe data
sev_data <- logit_data[severity_rating %in% "3",]

## Set variance
sev_cov_var <- .05

## Set covariates
sev_covs <-  list(cov_info("mean_age", "X", degree = 3,
                           i_knots = paste(data[, quantile(mean_age, prob = seq(0, 1, by = 0.2))][2:5], collapse = ", "),
                           bspline_gprior_mean = "0, 0, 0, 0, 0",
                           bspline_gprior_var = "1e-4, inf, inf, inf, inf"),
                  cov_info("sex_cov", "X", gprior_mean = 0, gprior_var = sev_cov_var),
                  cov_info("cv_filtered_for_dementia", "X", gprior_mean = 0, gprior_var = sev_cov_var),
                  cov_info("cv_filtered_for_dementia", "Z", gprior_mean = 0, gprior_var = sev_cov_var),
                  cov_info("cv_doctor_diagnosis_dementia", "X", gprior_mean = 0, gprior_var = sev_cov_var),
                  cov_info("cv_doctor_diagnosis_dementia", "Z", gprior_mean = 0, gprior_var = sev_cov_var),
                  cov_info("cv_screening_phase", "X", gprior_mean = 0, gprior_var = sev_cov_var),
                  cov_info("cv_screening_phase", "Z", gprior_mean = 0, gprior_var = sev_cov_var),
                  cov_info("cv_ad_type_dementia", "X", gprior_mean = 0, gprior_var = sev_cov_var),
                  cov_info("cv_ad_type_dementia", "Z", gprior_mean = 0, gprior_var = sev_cov_var),
                  cov_info("cv_outpatient", "X", gprior_mean = 0, gprior_var = sev_cov_var),
                  cov_info("cv_outpatient", "Z", gprior_mean = 0, gprior_var = sev_cov_var))



## Get prediction matrices
sev_pred_dt <- expand.grid(mean_age = seq(42.5, 97.5, by = 5),
                           sex_cov = c(-0.5, 0.5),
                           cv_filtered_for_dementia = 1,
                           cv_doctor_diagnosis_dementia= 1,
                           cv_screening_phase = 0,
                           cv_ad_type_dementia = 0,
                           cv_outpatient = 0)


## Run MRRBT (or load MRBRT) ------------------------------------------------------------- ##
severity <- "sev"
sev_model_name <- paste0("severe_model_pre_squeeze_", date)

severe_logit_model <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = sev_model_name,
  data = sev_data,
  mean_var = "logit_mean",
  se_var = "logit_se",
  covs = sev_covs,
  study_id = "NID",
  method = "trim_maxL",
  trim_pct = 0.1,
  overwrite_previous = T
)

severe_logit_model_pred <- predict_mr_brt(severe_logit_model, newdata = sev_pred_dt, write_draws = T)

## Plot MRBRT --------------------------------------------------------------------------------- ##

pdf(paste0("FILEPATH")) 
plot_mr_brt(results = severe_logit_model,
            predicts = severe_logit_model_pred,
            title = "Meta-Analysis Results: severe dementia",
            subtitle = "Logit transformed, pre-squeeze; \nprior on left tail with var=1e-4",
            results_logit = T,
            preds_logit = T)
dev.off()


## plot covariates
pdf(paste0("FILEPATH"))
cov_plot(severe_logit_model, " Severe model")
dev.off()


## MODERATE MR-BRT MODEL LOGIT_MEAN  ---------------------------------------------------------------------- ##

mod_data <- logit_data[severity_rating %in% "2",]

mod_cvs <-mod_data[,grepl("^cv_", names(data)), with = F]

#check out which covs are being used sufficiently
colSums(mod_cvs)

mod_cov_var <- .05
mod_covs <-  list(cov_info("mean_age", "X", degree = 3,
                           i_knots = paste(data[, quantile(mean_age, prob = seq(0, 1, by = 0.2))][2:5], collapse = ", "),
                           bspline_gprior_mean = "0, 0, 0, 0, 0", bspline_mono = "increasing",
                           bspline_gprior_var = "1e-4, inf, inf, inf, inf"),
                  cov_info("sex_cov", "X", gprior_mean = 0, gprior_var = mod_cov_var),
                  cov_info("cv_filtered_for_dementia", "X", gprior_mean = 0, gprior_var = mod_cov_var),
                  cov_info("cv_filtered_for_dementia", "Z", gprior_mean = 0, gprior_var = mod_cov_var),
                  cov_info("cv_doctor_diagnosis_dementia", "X", gprior_mean = 0, gprior_var = mod_cov_var),
                  cov_info("cv_doctor_diagnosis_dementia", "Z", gprior_mean = 0, gprior_var = mod_cov_var),
                  cov_info("cv_screening_phase", "X", gprior_mean = 0, gprior_var = mod_cov_var),
                  cov_info("cv_screening_phase", "Z", gprior_mean = 0, gprior_var = mod_cov_var),
                  cov_info("cv_ad_type_dementia", "X", gprior_mean = 0, gprior_var = mod_cov_var),
                  cov_info("cv_ad_type_dementia", "Z", gprior_mean = 0, gprior_var = mod_cov_var),
                  cov_info("cv_outpatient", "X", gprior_mean = 0, gprior_var = mod_cov_var),
                  cov_info("cv_outpatient", "Z", gprior_mean = 0, gprior_var = mod_cov_var),
                  cov_info("cv_DSMIIIR", "X", gprior_mean = 0, gprior_var = mod_cov_var),
                  cov_info("cv_DSMIIIR", "Z", gprior_mean = 0, gprior_var = mod_cov_var))


## Get prediction matrices
mod_pred_dt <- expand.grid(mean_age = seq(42.5, 97.5, by = 5),
                           sex_cov = c(-0.5, 0.5),
                           cv_filtered_for_dementia = 1,
                           cv_doctor_diagnosis_dementia= 1,
                           cv_screening_phase = 0,
                           cv_ad_type_dementia = 0,
                           cv_outpatient = 0,
                           cv_DSMIIIR = 0)


## Run MRRBT (or load MRBRT) ------------------------------------------------------------- ##
severity <- "mod"
mod_model_name <- paste0("moderate_model_pre_squeeze_", date)


mod_logit_model <-readRDS("FILEPATH"))
mod_logit_model <- run_mr_brt(
  output_dir = mr_brt_dir,
  model_label = mod_model_name,
  data = mod_data,
  mean_var = "logit_mean",
  se_var = "logit_se",
  covs = mod_covs,
  study_id = "NID",
  method = "trim_maxL",
  trim_pct = 0.1,
  overwrite_previous = T
)

mod_logit_model_pred <- predict_mr_brt(mod_logit_model, newdata = mod_pred_dt, write_draws = T)


## MILD MR-BRT MODEL LOGIT_MEAN  ---------------------------------------------------------------------- ##

mild_data <- logit_data[severity_rating %in% c("1","0.5-1.0"),]


cvs <-data[,grepl("^cv_", names(data)), with = F]

mild_cov_var <- .05
mild_covs <-  list(cov_info("mean_age", "X", degree = 3,
                            i_knots = paste(data[, quantile(mean_age, prob = seq(0, 1, by = 0.2))][2:5], collapse = ", "),
                            bspline_gprior_mean = "0, 0, 0, 0, 0",
                            bspline_gprior_var = "5e-5, inf, inf, inf, inf"),
                   cov_info("sex_cov", "X", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_filtered_for_dementia", "X", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_filtered_for_dementia", "Z", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_algorithm", "X", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_algorithm", "Z", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_doctor_diagnosis_dementia", "X", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_doctor_diagnosis_dementia", "Z", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_screening_phase", "X", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_screening_phase", "Z", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_ad_type_dementia", "X", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_ad_type_dementia", "Z", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_outpatient", "X", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_outpatient", "Z", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_CDRSB1", "X", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_CDRSB1", "Z", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_DSMIIIR", "X", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_DSMIIIR", "Z", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_GDS", "X", gprior_mean = 0, gprior_var = mild_cov_var),
                   cov_info("cv_GDS", "Z", gprior_mean = 0, gprior_var = mild_cov_var))


## Get prediction matrices
mild_pred_dt <- expand.grid(mean_age = seq(42.5, 97.5, by = 5),
                            sex_cov = c(-0.5, 0.5),
                            cv_filtered_for_dementia = 1,
                            cv_algorithm = 0,
                            cv_doctor_diagnosis_dementia= 1,
                            cv_screening_phase = 0,
                            cv_ad_type_dementia = 0,
                            cv_outpatient = 0,
                            cv_CDRSB1 = 0,
                            cv_DSMIIIR = 0,
                            cv_GDS = 0)


## Run MRRBT (or load MRBRT) ------------------------------------------------------------- ##
severity <- "mild"
mild_model_name <- paste0("mild_model_pre_squeeze_", date)


mild_logit_model <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = mild_model_name,
  data = mild_data,
  mean_var = "logit_mean",
  se_var = "logit_se",
  covs = mild_covs,
  study_id = "NID",
  method = "trim_maxL",
  trim_pct = 0.1,
  overwrite_previous = T
)

mild_logit_model_pred <- predict_mr_brt(mild_logit_model, newdata = mild_pred_dt, write_draws = T)


## SAVE FINAL ESTIMATES TO CSV ------------------------------------------------------------------- ##

estimates <- squeeze_model(mild_predicts = mild_logit_model_pred,
                           mod_predicts = mod_logit_model_pred,
                           sev_predicts = severe_logit_model_pred,
                           which_sev = "all",
                           logit = T)
estimates <- summaries(estimates, draw_vars = draws)

# add sex_id, sex
estimates[X_sex_cov==0.5,c("sex_id","sex"):=list(2,"Female")]
estimates[X_sex_cov==-0.5,c("sex_id","sex"):=list(1,"Male")]
estimates[,X_sex_cov:=NULL]

# add age_start, age_end, age_group_ids
estimates[,age_start:=X_mean_age-(X_mean_age%%5)]

age_dt_for_merge <- as.data.table(sapply(age_dt, as.numeric))
estimates <- merge(estimates, age_dt_for_merge[,.(age_group_id,age_group_years_start,age_group_years_end)], by.x="age_start", by.y = "age_group_years_start", all.x = T)
setnames(estimates, "age_group_years_end", "age_end")
estimates[,X_mean_age:=NULL]

estimates <- estimates[,c("age_group_id", "age_start", "age_end", "sex_id", "sex", "mean", "lower", "upper", "severity")]

## save estimates
mild_estimates <- estimates[severity=="mild",]
mild_estimates[,severity:=NULL]
dir.create(paste0("FILEPATH"))
write.csv(x = mild_estimates, file = paste0("FILEPATH"), row.names = F)

mod_estimates <- estimates[severity=="moderate",]
mod_estimates[,severity:=NULL]
dir.create(paste0("FILEPATH"))
write.csv(x = mod_estimates, file = paste0("FILEPATH"), row.names = F)

sev_estimates <- estimates[severity=="severe",]
sev_estimates[,severity:=NULL]
dir.create(paste0("FILEPATH"))
write.csv(x = sev_estimates, file = paste0("FILEPATH"), row.names = F)

dir.create(paste0("FILEPATH"))
write.csv(x = estimates, file = paste0("FILEPATH"), row.names = F)
