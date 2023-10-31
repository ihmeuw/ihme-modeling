
##
## Author: USER
## Date: DATE
## 
## Purpose: Model etiology-specific EMR from linked datasets. 
##          Linkage and dataset prep done in 'FILEPATH'
##          Calculation of EMR done in 'FILEPATH'
##
## source('FILEPATH')
##

rm(list=ls())

# Date of data to be read in
date <- 'DATE'
# Current Date
datetime <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, survival, ggrepel, msm, stats, boot, RColorBrewer)
user <- Sys.getenv('USER')
library(mrbrt002, lib.loc = "FILEPATH")

###### Paths, args
#################################################################################

central <- "FILEPATH"

emr_path <- paste0('FILEPATH', date ,'all_etiologies_nosubcauses.csv')

## GBD 2020 etiology list for HF
etiologies <-  c("cvd_ihd",
                 "cvd_rhd", "cvd_htn", "cvd_cmp_myocarditis", "cvd_cmp_other", "cvd_endo", "cvd_other",
                 "resp_copd", "resp_pneum_silico", "resp_pneum_asbest", "resp_pneum_coal", "resp_pneum_other",
                 "resp_interstitial", "hemog_thalass", "hemog_g6pd", "hemog_other", "endo_other", "cong_heart",
                 "cvd_cmp_alcoholic", "cvd_valvu_other", "cvd_pah", 
                 "mental_drug_cocaine", "mental_drug_amphet",
                 "cvd_afib",
                 "ckd",
                 "cirrhosis",
                 "cvd_stroke", 
                 "endo_thyroid", "endo_hypothyroid", "endo_hyperthyroid") 

code_root<- "FILEPATH"
hf_codes <- c("I50|I11|^428|^402|^425")

save_file <- 'FILEPATH'
plot_file <- 'FILEPATH'

## Are we collapsing some small etiologies into larger bins?
collapse <- T


###### Functions
#################################################################################

# Functions written to deal with multiple diagnosis data
source('FILEPATH')

## Helper functions written by USER and USER
source('FILEPATH')


source(paste0(central, "get_age_metadata.R"))

## Function to graph the MRBRT fit
graph_mrbrt_spline <- function(model, title_append="") {
  trimmed_data <- cbind(model$data$to_df(), data.frame(w = model$w_soln))
  trimmed_data <- as.data.table(trimmed_data)
  trimmed_data[w == 0, excluded := "Trimmed"][w > 0, excluded := "Not trimmed"]      
  p <- ggplot() +
    geom_point(data = trimmed_data, aes(x = age_group_years_start, y = obs, color = as.factor(excluded))) +
    scale_color_manual(values = c("purple", "red")) +
    labs(x = "Age", y = "EMR, Logit", color = "") +
    ggtitle(paste0("Meta-Analysis Results: ", title_append)) +
    theme_bw() +
    theme(text = element_text(size = 17, color = "black")) +
    geom_smooth(data = new_data, aes(x = age_group_years_start, y = logit_value), color = "black", se = F, fullrange = T) 
  print(p)
}


###### Read in and prep dataset
#################################################################################

df <- fread(emr_path)

## Pull in ages to predict for
ages <- get_age_metadata(age_group_set_id=VALUE, gbd_round_id = VALUE)
new_data <- data.table(age_group_years_start = ages[age_group_years_start >= 40, age_group_years_start])

## Collapse etiologies with small counts into categories. Recalculate hazard. 
if (collapse) {
  df[etiology %in% c("mental_drug_cocaine", "mental_drug_amphet"), etiology := "mental_drug_cocaine"]
  df[etiology %in% c("hemog_g6pd", "hemog_other", "hemog_thalass"), etiology := "hemog_other"]
  df[etiology %in% c("resp_pneum_asbest", "resp_pneum_coal", "resp_pneum_other", "resp_pneum_silico"), etiology := "resp_pneum_other"]  
  df[etiology %in% c("cvd_cmp_alcoholic", "cvd_cmp_myocarditis", "cvd_cmp_other"), etiology := "cvd_cmp_other"]
  df[etiology %in% c("cvd_other", "cvd_pah", "cong_heart"), etiology := "cvd_other"]
  df[etiology %in% c("cvd_ihd_chronic", "cvd_ihd_angina", "cvd_ihd_acute"), etiology := "cvd_ihd"]
  df[etiology %in% c("cvd_stroke_cerhem", "cvd_stroke_isch", "cvd_stroke_subhem"), etiology := "cvd_stroke"]
  df[etiology %in% c("endo_thyroid", "endo_hypothyroid", "endo_hyperthyroid"), etiology := "endo_thyroid"]
  df[, `:=` (deaths=sum(deaths), person_years=sum(person_years)), by=c("age_group_years_start", "demographics", "etiology")]
  df[, hazard := deaths/person_years]
  df <- unique(df)
}

## Collapse person-years under 50 to larger groups
collapse_pyrs <- function(df) {
  
  collapse_dt <- copy(df[demographics=="heart_failure" & age_group_years_start >= 40])
  no_collapse_dt <- copy(df[demographics=="never_heart_failure" & age_group_years_start >= 40])
  
  collapse_dt[, collapse := ifelse(person_years < 50, 1, 0)]
  setorder(collapse_dt, etiology, age_group_years_start)
  
    
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

## Save intermediate
write.csv(df, paste0(save_file, "collapsed_data_", datetime, ".csv"))


### LAUNCH ARRAY ###
df_file <- paste0(save_file, "collapsed_data_", datetime, ".csv")
etiology_file <- paste0(save_file, "etiologies_", datetime, ".csv")
write.csv(expand.grid(unique(df$etiology)), etiology_file)
n_jobs <- length(unique(df$etiology))
rscript <- "FILEPATH"

shell <- 'FILEPATH'


code_command <- paste0(shell,  " -s ", rscript, " ", etiology_file, " ", save_file, " ", df_file)
full_command <- paste0("sbatch -J emr -A proj_cvd --mem=50G -c 2 -t 1:00:00 -C archive -p all.q ",
                       "-a ", paste0("1-", n_jobs), " ",
                       "-o FILEPATH",user,"/output_log/%x.o%j ",
                       "-e FILEPATH",user,"/error_log/%x.e%j ",
                       code_command)

print(full_command)
system(full_command)

#job_hold("emr")

failed <- c()
for (et in unique(df$etiology)) {
  if (!file.exists(paste0(save_file, "predictions_", et, "_", datetime, ".csv"))) failed <- c(failed, et)
}

print(failed)

for (et in failed) {
  
  plot <- T
  predictions <- data.table()
  if (plot) pdf(paste0(save_file, "modeled_emr_", datetime, "_", et, ".pdf"), height=8, width=12)
  
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
  mod[, logit_se := sqrt((1/(excess_mortality - excess_mortality^2))^2 * se_emr^2)]
  
  
  initialized_data <- MRData()
  initialized_data$load_df(
    data = mod,  col_obs = "logit_emr", col_obs_se = "logit_se",
    col_covs = list("age_group_years_start"), col_study_id = "id_var" )
  
  model <- MRBRT(
    data = initialized_data,
    cov_models = list(
      LinearCovModel("intercept", use_re = TRUE),
      LinearCovModel(alt_cov = "age_group_years_start",
                     use_spline = TRUE,
                     spline_knots_type = "domain",
                     spline_degree = 4L, 
                     spline_r_linear = TRUE, 
                     spline_l_linear = TRUE)),
    inlier_pct = 0.9)
  
  model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
  
  ## Predict
  ages <- get_age_metadata(age_group_set_id=VALUE, gbd_round_id = VALUE)
  new_data <- data.table(age_group_years_start = ages[age_group_years_start >= 40, age_group_years_start])
  
  prediction_df_initalized <- MRData()
  
  prediction_df_initalized$load_df(
    data = new_data, 
    col_covs=list('age_group_years_start')
  )
  
  n_samples <- 1000L
  samples <- model$sample_soln(sample_size = n_samples)
  
  draws <- model$create_draws(
    data = prediction_df_initalized,
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study = TRUE)
  
  new_data$logit_value <- model$predict(prediction_df_initalized)
  new_data$logit_value_low <- apply(draws, 1, function(x) quantile(x, 0.025))    
  new_data$logit_value_high <- apply(draws, 1, function(x) quantile(x, 0.975))
  new_data[, etiology := et]
  
  if (plot) graph_mrbrt_spline(model = model, title_append = et)
  if (plot) dev.off()
  
  new_data[, estimate := inv.logit(logit_value)]
  new_data[, pred_se_logit := (logit_value_high - logit_value_low)/(2*1.96)]
  new_data$pred_se <- sapply(1:nrow(new_data), function(i) {
    ratio_i <- new_data[i, logit_value]
    ratio_se_i <- new_data[i, pred_se_logit]
    sqrt((exp(ratio_i)/ (1 + exp(ratio_i))^2)^2 * ratio_se_i^2)
  })
  
  
  predictions <- rbind(predictions, new_data, fill=T)
  
  write.csv(new_data, paste0(save_file, "predictions_", et, "_", datetime, ".csv"))
  
}

for (et in c("hemog_other", "resp_pneum_other")) {
  plot <- T
  predictions <- data.table()
  if (plot) pdf(paste0(save_file, "modeled_emr_", datetime, "_", et, ".pdf"), height=8, width=12)
  
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
  mod[, logit_se := sqrt((1/(excess_mortality - excess_mortality^2))^2 * se_emr^2)]
  
  
  initialized_data <- MRData()
  initialized_data$load_df(
    data = mod,  col_obs = "logit_emr", col_obs_se = "logit_se",
    col_covs = list("age_group_years_start"), col_study_id = "id_var" )
  
  if (et == "hemog_other") {
    model <- MRBRT(
      data = initialized_data,
      cov_models = list(
        LinearCovModel("intercept", use_re = TRUE),
        LinearCovModel(alt_cov = "age_group_years_start",
                       use_spline = TRUE,
                       spline_knots_type = "frequency",
                       spline_degree = 4L, 
                       spline_r_linear = TRUE, 
                       spline_l_linear = TRUE)),
      inlier_pct = 0.9)
  } else {
    model <- MRBRT(
      data = initialized_data,
      cov_models = list(
        LinearCovModel("intercept", use_re = TRUE),
        LinearCovModel(alt_cov = "age_group_years_start",
                       use_spline = FALSE)),
  #                     spline_knots = c(71, 76, 81, 86),
   #                    spline_degree = 3L, 
    #                   spline_r_linear = TRUE, 
     #                  spline_l_linear = TRUE)),
      inlier_pct = 0.9)
  }

  model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
  
  ## Predict
  ages <- get_age_metadata(age_group_set_id=VALUE, gbd_round_id = VALUE)
  new_data <- data.table(age_group_years_start = ages[age_group_years_start >= 40, age_group_years_start])
  
  prediction_df_initalized <- MRData()
  
  prediction_df_initalized$load_df(
    data = new_data, 
    col_covs=list('age_group_years_start')
  )
  
  n_samples <- 1000L
  samples <- model$sample_soln(sample_size = n_samples)
  
  draws <- model$create_draws(
    data = prediction_df_initalized,
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study = TRUE)
  
  new_data$logit_value <- model$predict(prediction_df_initalized)
  new_data$logit_value_low <- apply(draws, 1, function(x) quantile(x, 0.025))    
  new_data$logit_value_high <- apply(draws, 1, function(x) quantile(x, 0.975))
  new_data[, etiology := et]
  
  if (plot) graph_mrbrt_spline(model = model, title_append = et)
  if (plot) dev.off()
  
  new_data[, estimate := inv.logit(logit_value)]
  new_data[, pred_se_logit := (logit_value_high - logit_value_low)/(2*1.96)]
  new_data$pred_se <- sapply(1:nrow(new_data), function(i) {
    ratio_i <- new_data[i, logit_value]
    ratio_se_i <- new_data[i, pred_se_logit]
    sqrt((exp(ratio_i)/ (1 + exp(ratio_i))^2)^2 * ratio_se_i^2)
  })
  
  
  predictions <- rbind(predictions, new_data, fill=T)
  
  write.csv(new_data, paste0(save_file, "predictions_", et, "_", datetime, ".csv"))
  
}

## Bring back together
predictions <- rbindlist(lapply(X = unique(df$etiology), FUN = function(x) {
  df <- fread(paste0(save_file, "predictions_", x, "_", datetime, ".csv"))
  df
}))

pdf(paste0('FILEPATH', datetime, '.pdf'), width=12, height=8)
for (et in unique(predictions$etiology)) {
  p <- ggplot(predictions[etiology==et,], aes(x=age_group_years_start, y=logit_value, color=pred_se_logit)) + geom_point() +
    theme_classic() + labs(x="Age",y="Logit EMR", color="SE, logit EMR", title=et)
  print(p)
}
p <- ggplot(predictions, aes(x=age_group_years_start, y=logit_value, color=etiology)) + geom_line() +
  theme_classic() + labs(x="Age", y='Logit EMR', color="Cause Name, short", title="") +
  scale_color_manual(values = colorRampPalette(brewer.pal(8, "Paired"))(18))
print(p)
dev.off()

preds <- copy(predictions)
## Un-collapse small etiologies
g6pd <- copy(predictions[etiology=="hemog_other"])[, etiology := "hemog_g6pd"]
thalass <- copy(predictions[etiology=="hemog_other"])[, etiology := "hemog_thalass"]
asbest <- copy(predictions[etiology=="resp_pneum_other"])[, etiology := "resp_pneum_asbest"]
coal <- copy(predictions[etiology=="resp_pneum_other"])[, etiology := "resp_pneum_coal"]
silico <- copy(predictions[etiology=="resp_pneum_other"])[, etiology := "resp_pneum_silico"]
alc <- copy(predictions[etiology=="cvd_cmp_other"])[, etiology := "cvd_cmp_alcoholic"]
myo <- copy(predictions[etiology=="cvd_cmp_other"])[, etiology := "cvd_cmp_myocarditis"]
pah <- copy(predictions[etiology=="cvd_other"])[, etiology := "cvd_pah"]
cong <- copy(predictions[etiology=="cvd_other"])[, etiology := "cong_heart"]
amphet <- copy(predictions[etiology=="cvd_other"])[, etiology := "mental_drug_amphet"]
cocaine <- copy(predictions[etiology=="cvd_other"])[, etiology := "mental_drug_cocaine"]
s1 <- copy(predictions[etiology=="cvd_stroke"])[, etiology := "cvd_stroke_subhem"]
s2 <- copy(predictions[etiology=="cvd_stroke"])[, etiology := "cvd_stroke_cerhem"]
s3 <- copy(predictions[etiology=="cvd_stroke"])[, etiology := "cvd_stroke_isch"]

predictions <- do.call("rbind", list(g6pd, thalass, asbest, coal, silico, alc, myo, pah, cong, amphet, cocaine, predictions, s1, s2, s3))
predictions[estimate>1, estimate := 1]

append <- data.table(expand.grid(age_group_years_start = unique(ages[age_group_years_start< 40, age_group_years_start]), etiology = unique(predictions$etiology)))
append[, `:=` (estimate = .01, pred_se = .01)]

predictions <- rbind(predictions[, .(age_group_years_start, etiology, estimate, pred_se)], append)

pred_m <- copy(predictions)[, sex_id := 1]
pred_f <- copy(predictions)[, sex_id := 2]

predictions <- rbind(pred_f, pred_m)

fwrite(predictions, paste0(save_file, "emr_modeled_", datetime, ".csv"))

