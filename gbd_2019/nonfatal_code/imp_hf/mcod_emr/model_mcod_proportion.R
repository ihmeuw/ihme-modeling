
##
## Purpose: Model the MCOD proportion by age, sex. 
##          This is the proportion of deaths due to an etiology that have heart failure coded with them.
##

date<-gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, doBy, msm, boot)

###### Paths, args
############################################################################################################

## Central functions
central <- "FILEPATH"

### MR-BRT functions
mrbrt_helper_dir <- "FILEPATH"
mrbrt_functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
                  "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
                  "load_mr_brt_preds_function", "plot_mr_brt_function")

## GBD 2017/2019 etiology list for HF
etiologies <-  c("cvd_ihd", "cvd_rhd", "cvd_htn", "cvd_cmp_myocarditis", "cvd_cmp_other", "cvd_endo", "cvd_other",
                 "resp_copd", "resp_pneum_silico", "resp_pneum_asbest", "resp_pneum_coal", "resp_pneum_other",
                 "resp_interstitial", "hemog_thalass", "hemog_g6pd", "hemog_other", "endo_other", "cong_heart",
                 "cvd_cmp_alcoholic", "cvd_valvu_other")
hund_attribution <- c("cvd_htn", "cvd_cmp_other", "cvd_cmp_alcoholic")

## ICD codes for HF 
hf_codes <- "I50|I11|^428|^402|^425" # HF, HHD, cardiomyopathies

gbd_round_id <- 6
decomp_step <- "step3"

write_path <- 'FILEPATH'


###### Functions
############################################################################################################

## Age metadata
source(paste0(central, "get_age_metadata.R"))
age_groups <- get_age_metadata(age_group_set_id = 12, gbd_round_id = gbd_round_id)

source(paste0(central, "get_covariate_estimates.R"))
source(paste0(central, "get_location_metadata.R"))

locs <- get_location_metadata(location_set_id = 9)

## Helper functions
source('convenient_funcs.R')

## Pull in MR-BRT functions
for (funct in mrbrt_functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}

###### Pull in data
###########################################################################################################


usa <- fread("FILEPATH")
usa[, location_id := 102]
usa <- usa[, .(sum_deaths, sum_hf_deaths, cause_yll_cause, cause_yll_cause_name, sex_id, age_group_id, location_id)]
usa[, country := "USA"]

twn <- fread("FILEPATH")
twn[, location_id := 8]
twn[, country := "TWN"]

bra <- fread("FILEPATH")
bra[, country := "BRA"]

mex <- fread("FILEPATH")
mex[, country := "MEX"]

col <- fread("FILEPATH")
col[, location_id := 125]
col[, country := "COL"]


data <- do.call("rbind", list(usa, twn, bra, col, mex, fill=T))
data[, year_id := NULL]
data[, location_id := NULL]

## Sum deaths by country, age, sex, cause
data[, `:=` (sum_deaths=sum(sum_deaths), sum_hf_deaths=sum(sum_hf_deaths)), by=c("cause_yll_cause", "sex_id", "age_group_id", "country")]
data <- unique(data)

## Collapse causes with small numbers
collapse <- T
if (collapse) {
  data[cause_yll_cause %in% c("hemog_g6pd", "hemog_other", "hemog_thalass"), cause_yll_cause := "hemog_other"]
  data[cause_yll_cause %in% c("resp_pneum_asbest", "resp_pneum_coal", "resp_pneum_other", "resp_pneum_silico"), cause_yll_cause := "resp_pneum_other"]  
  data[, `:=` (sum_deaths=sum(sum_deaths), sum_hf_deaths=sum(sum_hf_deaths)), by=c("cause_yll_cause", "sex_id", "age_group_id", "country")]
  #data[, hazard := deaths/person_years]
  data <- unique(data)
}

## Calculate the proportion and transform into logit space
data[, ratio := sum_hf_deaths/sum_deaths]
data <- data[ratio > 0 & ratio < 1,]
data[, logit_ratio := log(ratio/(1-ratio))]
data[, se := sqrt(ratio/sum_deaths)]
data$logit_se <- sapply(1:nrow(data), function(i){
  mean_i <- data[i, ratio]
  se_i <- data[i, se]
  deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
}
)
data[, id_var := .N, by=names(data)]
data <- merge(data, age_groups[, .(age_group_id, age_group_years_start)], by="age_group_id")
data[, male := ifelse(sex_id==1, 1, 0)]
data <- data[sex_id %in% c(1, 2)]
data[, id_var := .GRP, by="country"]

  ## Make dummy data for prediction purposes
  new_data <- expand.grid(age_group_years_start=unique(data$age_group_years_start), male=c(1, 0))
  predictions <- data.table()
  
  for (etiology in unique(data[!(cause_yll_cause %in% hund_attribution), cause_yll_cause])) {
    
    df <- copy(data[cause_yll_cause==etiology,])
    name <- unique(data[cause_yll_cause==etiology, cause_yll_cause_name])
    
    results <- run_mr_brt(
      output_dir = "FILEPATH",
      model_label = etiology,
      data = df,
      mean_var = "logit_ratio",
      se_var = "logit_se",
      covs = list(cov_info("age_group_years_start", "X", n_i_knots = 4, degree = 3,
                           knot_placement_procedure = "frequency"
      ),
      cov_info("male", "X")),
      study_id = "id_var",
      method = "trim_maxL", trim_pct = 0.20,
      overwrite_previous = T
    )
    
    predicts <- predict_mr_brt(results, new_data)
    pred <- as.data.table(predicts$model_summaries)
    
    pred <- pred[, .(X_male, X_age_group_years_start, Y_mean, Y_mean_lo, Y_mean_hi)]
    pred[, et := etiology]
    
    predictions <- rbind(predictions, pred, fill=T)
    
  }

  ## Pull predictions out of logit-space
  predictions[, mcod_ratio := inv.logit(Y_mean)]
  predictions[, pred_se_logit := (Y_mean_hi - Y_mean_lo)/3.92]
  predictions$pred_se <- sapply(1:nrow(predictions), function(i) {
    ratio_i <- predictions[i, Y_mean]
    ratio_se_i <- predictions[i, pred_se_logit]
    deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i^2)
  })
  
  predictions[, sex_id := ifelse(X_male==1, 1, 2)]
  
  setnames(predictions, "X_age_group_years_start", "age_group_years_start")
  predictions$X_male <- NULL
  
  for (et in hund_attribution) {
    df <- as.data.table(copy(new_data))
    setnames(df, "age_group_years_start", "age_start")
    df[, mcod_ratio := 1]
    df[, et := et]
    df[, sex_id := ifelse(male==1, 1, 2)]
    df[, `:=`(Y_mean=0, Y_mean_lo=0, Y_mean_hi=0, pred_se=0)]
    predictions <- rbind(predictions, df, fill=T)
  }
  
  predictions[is.na(sex_id), sex_id := ifelse(male==1, 1, 2)]
  predictions[is.na(age_group_years_start), age_group_years_start := age_start]
  #
  predictions[et == "endo_other", et := "endo"]
  hemog <- copy(predictions[et == "hemog_other"])[, et := "hemog_g6pd"]
  hemog_thalass <- copy(predictions[et == "hemog_other"])[, et := "hemog_thalass"]
  asbest <- copy(predictions[et == "resp_pneum_other"])[, et := "resp_pneum_asbest"]
  coal <- copy(predictions[et == "resp_pneum_other"])[, et := "resp_pneum_coal"]
  silico <- copy(predictions[et == "resp_pneum_other"])[, et := "resp_pneum_silico"]
  
  predictions <- do.call("rbind", list(predictions, hemog, hemog_thalass, asbest, coal, silico))
  
  setnames(predictions, "et", "etiology")

  predictions <- predictions[, .(age_group_years_start, etiology, mcod_ratio, pred_se, sex_id)]
  
  write.csv(predictions, file="FILEPATH")

  