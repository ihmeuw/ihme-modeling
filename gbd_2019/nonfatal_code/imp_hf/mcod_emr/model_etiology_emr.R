
## 
## Purpose: Model etiology-specific EMR from linked datasets. 
##
##

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, survival, survminer, ggrepel, msm, stats)


###### Paths, args
#################################################################################

central <- "FILEPATH"

emr_path <- 'FILEPATH'

## GBD 2017/2019 etiology list for HF
etiologies <-  c("cvd_ihd", "cvd_rhd", "cvd_htn", "cvd_cmp_myocarditis", "cvd_cmp_other", "cvd_endo", "cvd_other", "resp_copd", "resp_pneum_silico", "resp_pneum_asbest",
                 "resp_pneum_coal", "resp_pneum_other", "resp_interstitial", "hemog_thalass", "hemog_g6pd", "hemog_other", "endo", "cong_heart", "cvd_cmp_alcoholic", "cvd_valvu_other")

code_root<- "FILEPATH"
hf_codes <- c("I50|I11|^428|^402|^425")

save_file <- 'FILEPATH'

### MR-BRT functions
mrbrt_helper_dir <- "FILEPATH"
mrbrt_functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
                  "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
                  "load_mr_brt_preds_function", "plot_mr_brt_function")

## Are we collapsing some small etiologies into larger bins?
collapse <- T

###### Functions
#################################################################################

source("data_tests.R")
source("model_helper_functions.R")

## Functions written to deal with multiple diagnosis data
source('CI_cleaning_funcs.R')

source(paste0(central, "get_age_metadata.R"))

## Pull in MR-BRT functions
for (funct in mrbrt_functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}


###### Read in and prep dataset
#################################################################################

df <- fread(emr_path)

## Pull in ages to predict for
ages <- get_age_metadata(age_group_set_id=12)
new_data <- data.table(age_group_years_start = ages[age_group_years_start >= 40, age_group_years_start])

## Collapse etiologies with small counts into categories. Recalculate hazard. 
if (collapse) {
  df[etiology %in% c("hemog_g6pd", "hemog_other", "hemog_thalass"), etiology := "hemog_other"]
  df[etiology %in% c("resp_pneum_asbest", "resp_pneum_coal", "resp_pneum_other", "resp_pneum_silico"), etiology := "resp_pneum_other"]  
  df[etiology %in% c("cvd_cmp_alcoholic", "cvd_cmp_myocarditis", "cvd_cmp_other"), etiology := "cvd_cmp_other"]
  df[, `:=` (deaths=sum(deaths), person_years=sum(person_years)), by=c("age_group_years_start", "demographics", "etiology")]
  df[, hazard := deaths/person_years]
  df <- unique(df)
}


collapse_pyrs <- function(df) {
  
  collapse_dt <- copy(df[demographics=="heart_failure" & age_group_years_start >= 40])
  no_collapse_dt <- copy(df[demographics=="never_heart_failure" & age_group_years_start >= 40])
  
  collapse_dt[, collapse := ifelse(person_years < 50, 1, 0)]
  setorder(collapse_dt, etiology, age_group_years_start)
  
  #while(length(nrow(collapse_dt[collapse==1]))>0) {
    
    #col <- data.table()
    for (row in 1:nrow(collapse_dt)) {
      
      if (row <= nrow(collapse_dt)) {
        r <- collapse_dt[row,]
        et <- r$etiology
        
        if (r$collapse==1) {
          
          if (row == nrow(collapse_dt)) {
            collapse_with <- data.table(collapse_dt[row-1,])
            n <- row - 1
          } else if (collapse_dt[row + 1,]$etiology == et) {
            collapse_with <- data.table(collapse_dt[row+1,])
            n <- row + 1
          } else if (collapse_dt[row-1, etiology] == et) {
            collapse_with <- data.table(collapse_dt[row-1,])
            n <- row - 1
          } else {
            break
          }
          
          collapsed <- rbind(r, collapse_with)
          collapsed[, `:=` (deaths=sum(deaths), person_years=sum(person_years), age_group_years_start=min(age_group_years_start), age_group_years_end=max(age_group_years_end))]
          collapsed[, hazard := deaths/person_years]
          collapsed[, collapse := ifelse(person_years > 50, 0, 1)]
          collapsed <- unique(collapsed)
          collapsed[, pooled := 1]
          
          collapse_dt <- collapse_dt[-c(row, n),]
          collapse_dt <- rbind(collapse_dt, collapsed, fill=T)
          
          setorder(collapse_dt, etiology, age_group_years_start)
          
        }
        
      }
      
    }
    
 # }
  collapse_dt$collapse <- NULL
  collapse_dt[is.na(pooled), pooled := 0]
  
  dt <- rbind(collapse_dt, no_collapse_dt, fill=T)
  dt[is.na(pooled), pooled := 0]
  
  
  
}



df <- collapse_pyrs(df)
df[pooled==1, age_group_years_start := (age_group_years_start + age_group_years_end)/2]
df[hazard>1, hazard := 1]

df$dem <- NULL

## Calculate standard error
df[, standard_error := sqrt(hazard/person_years)]

spline_covs <- data.table(
  cvd_ihd = c("51, 66, 79", F, T),
  cvd_endo = c("58, 68", T, T),
  cvd_htn = c("51, 71, 86", T, T),
  cvd_rhd = c("51, 66, 79", T, T),
  cvd_cmp_other = c("51, 69, 81", T, T),
  cvd_other = c("51, 69, 82", F, T),
  resp_copd = c("59, 71, 81", T, T),
  resp_interstitial = c("56, 66, 76", F, T),
  endo = c("51, 66, 79", F, T),
  hemog_other = c("64, 81", T, T),
  resp_pneum_other = c("66, 78", T, T),
  cong_heart = c("56, 74", T, F),
  cvd_valvu_other = c("51, 66, 79", T, T)
)


predictions <- data.table()
for (et in unique(df$etiology)) {
  
  print(et)
  
  mod <- copy(df[etiology == et,])
  mod[, c("deaths", "person_years", "small") := NULL]
  mod <- unique(mod)
  mod <- melt(mod, measure.vars = c("hazard", "standard_error"))
  mod <- data.table::dcast(mod, formula = age_group_years_start + age_group_years_end + etiology ~ variable + demographics )
  mod <- mod[!(is.na(hazard_heart_failure)) & !(is.na(hazard_never_heart_failure))]
  mod[, id_var := .GRP, by=names(mod)]
  
  ## EMR = hazard, HF - hazard, background. Add SEs.
  mod[, excess_mortality := hazard_heart_failure - hazard_never_heart_failure]
  mod <- mod[excess_mortality >0,]
  mod[, se_emr := sqrt(standard_error_heart_failure^2 + standard_error_never_heart_failure^2)]
  
  mod[, logit_emr := log(excess_mortality/(1-excess_mortality))]
  mod$logit_se <- sapply(1:nrow(mod), function(i){
    mean_i <- mod[i, excess_mortality]
    se_i <- mod[i, se_emr]
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  }
  )  
  
  results <- run_mr_brt(
    output_dir = "FILEPATH",
    model_label = et,
    data = mod,
    mean_var = "logit_emr",
    se_var = "logit_se",
    covs = list(cov_info("age_group_years_start", "X", degree = 4, 
                         bspline_gprior_mean = ifelse(length(strsplit(spline_covs[1, get(et)], split = ",")[[1]]) == 3, "0, 0, 0, 0", "0, 0, 0"),
                         bspline_gprior_var = ifelse(et == "resp_pneum_other", "1e-6, inf, inf", 
                                                     ifelse(length(strsplit(spline_covs[1, get(et)], split = ",")[[1]]) == 3, "inf, inf, inf, inf", "inf, inf, inf")), 
                         i_knots = spline_covs[1, get(et)], 
                         r_linear=spline_covs[3, get(et)], l_linear=spline_covs[2, get(et)])),
    study_id = "id_var",
    method = "trim_maxL", trim_pct = 0.30,
    overwrite_previous = T
  )
  
  predicts <- predict_mr_brt(model_object = results, newdata = new_data)
  pred <- as.data.table(predicts$model_summaries)
  
  pred[, estimate := inv_logit(Y_mean)]
  pred[, pred_se_logit := (Y_mean_hi - Y_mean_lo)/(2*1.96)]
  pred$pred_se <- sapply(1:nrow(pred), function(i) {
    ratio_i <- pred[i, Y_mean]
    ratio_se_i <- pred[i, pred_se_logit]
    deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i^2)
  })

  pred <- pred[, .(X_age_group_years_start, estimate, pred_se)]
  setnames(pred, "X_age_group_years_start", "age_group_years_start")
  pred[, etiology := et]
    
  predictions <- rbind(predictions, pred, fill=T)
}

## Un-collapse small etiologies
g6pd <- copy(predictions[etiology=="hemog_other"])[, etiology := "hemog_g6pd"]
thalass <- copy(predictions[etiology=="hemog_other"])[, etiology := "hemog_thalass"]
asbest <- copy(predictions[etiology=="hemog_other"])[, etiology := "resp_pneum_asbest"]
coal <- copy(predictions[etiology=="hemog_other"])[, etiology := "resp_pneum_coal"]
silico <- copy(predictions[etiology=="hemog_other"])[, etiology := "resp_pneum_silico"]
alc <- copy(predictions[etiology=="cvd_cmp_other"])[, etiology := "cvd_cmp_alcoholic"]
myo <- copy(predictions[etiology=="cvd_cmp_other"])[, etiology := "cvd_cmp_myocarditis"]


predictions <- do.call("rbind", list(g6pd, thalass, asbest, coal, silico, alc, myo, predictions))
predictions[estimate>1, estimate := 1]

append <- data.table(expand.grid(age_group_years_start = unique(ages[age_group_years_start< 40, age_group_years_start]), etiology = unique(predictions$etiology)))
append[, `:=` (estimate = .01, pred_se = .01)]

predictions <- rbind(predictions, append)

pred_m <- copy(predictions)[, sex_id := 1]
pred_f <- copy(predictions)[, sex_id := 2]

predictions <- rbind(pred_f, pred_m)
write.csv(predictions, "FILEPATH")



