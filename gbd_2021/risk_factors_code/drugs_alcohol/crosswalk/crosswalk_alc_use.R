##########################################################################
### Purpose: CROSSWALKING GBD 2019
##########################################################################
library(dplyr)
library(data.table)

rm(list=ls())

#######################
sub_topics <- c("current_drinkers_1month", "current_drinkers_1week")

# Set Preferences
sub_topic      <- sub_topics[6]
x_walk_measure <- "logit_difference"

if (topic =="alcohol_use" & sub_topic == "current_drinkers_1month"){
  
  crosswalk      <- "cv_recall_1m"
  covariates     <- c("age_50_plus", 
                      "male")
  keep_more_cols <- NULL
  a <- 0
  y <- 0
                             
  
} else if (topic =="alcohol_use" & sub_topic == "current_drinkers_1week"){
  
  crosswalk      <- "cv_recall_1week"
  covariates     <- c("age_under_20",
                      "male") 
  keep_more_cols <- NULL
  a <- 0
  y <- 0
  
} 
    
#######################

lib_home <- 'FILEPATH'

library(dplyr)
pacman::p_load(data.table, openxlsx, ggplot2)
library(msm, lib.loc = 'FILEPATH')
library(msm, lib.loc = lib_home)
date <- gsub("-", "_", Sys.Date())
library(gtools)

# SET OBJECTS -------------------------------------------------------------

dem_dir       <- 'FILEPATH'
functions_dir <- 'FILEPATH'
code_dir      <- 'FILEPATH'
mrbrt_helper_dir <- 'FILEPATH'

draws <- paste0("draw_", 0:999)

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_location_metadata.R"))
source(paste0(functions_dir, "get_age_metadata.R"))

functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
            "load_mr_brt_preds_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}

# GET METADATA ------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 22)
age_dt <- get_age_metadata(12)
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]

# DATA PROCESSING FUNCTIONS -----------------------------------------------

# Source mr_brt_functions.R
source('FILEPATH')

# RUN DATA SETUP ----------------------------------------------------------

# Load Data
dem_dt <- get_dem_data(topic, sub_topic, crosswalk = crosswalk, covariates = covariates, keep_more_cols = keep_more_cols) 
dem_dt$standard_error <- as.numeric(dem_dt$standard_error)

table(dem_dt$sex)


# Data Setup

dem_dt <- aggregate_marketscan(dem_dt)
dem_dt <- get_cases_sample_size(dem_dt)
dem_dt <- get_se(dem_dt)
defs   <- get_definitions(dem_dt)
dem_dt <- defs[[1]]
cvs    <- defs[[2]]
dem_dt <- calc_year(dem_dt)

dem_dt <- dem_dt[!is.na(standard_error)]
dem_dt <- dem_dt[, lower := mean - 1.96*standard_error]
dem_dt <- dem_dt[, upper := mean + 1.96*standard_error]
dem_dt <- dem_dt[lower <= 0, lower := 0]
dem_dt <- dem_dt[upper >= 1, upper := 1]


# Generate Data Landscape Graphs
explore <- F
if (explore == T){
  explore_data(dem_dt, crosswalk)
}

age_dts <- get_age_combos(dem_dt)
dem_dt  <- age_dts[[1]]
age_match_dt <- age_dts[[2]] ## DON'T ACTUALLY NEED THIS BUT FOUND IT HELPFUL FOR VETTING
pairs <- combn(dem_dt[, unique(definition)], 2)

# Create matched sets
matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = dem_dt[(age_end-age_start <= 25)], year_span = y, age_span = a, covariates, keep_more_cols)))

if (matches[1,"def2"] == "reference"){
  matches <- setnames(matches, old = c("nid", "location_id", "sex", "year_start", "year_end", "measure", "age_start", "age_end", "mean", "standard_error", "cases", 
                                       "sample_size", "year_match","age_n", "seq", "urbanicity_type", 
                                       "def", "nid2", "location_id2", "sex2", "year_start2", 
                                       "year_end2", "measure2", "age_start2", "age_end2", "mean2","standard_error2", "cases2", "sample_size2", "year_match2", "age_n2", 
                                       "seq2", "urbanicity_type2", "def2"
                                       , covariates, paste0(covariates, 2)), 
  new = c("nid2", "location_id2", "sex2", "year_start2", "year_end2", "measure2", "age_start2", "age_end2", "mean2","standard_error2", "cases2", 
          "sample_size2", "year_match2", "age_n2", "seq2", "urbanicity_type2", 
          "def2", "nid", "location_id", "sex", "year_start", 
          "year_end", "measure", "age_start", "age_end", "mean", "standard_error", "cases", "sample_size", "year_match","age_n", 
          "seq", "urbanicity_type", "def"
          , paste0(covariates, 2),covariates))
}

matches <- create_ratios(matches, x_walk_measure)

write.csv(matches, 'FILEPATH', row.names = F)


mrbrt_setup <- create_mrbrtdt(matches, xcovs = covariates, x_walk_measure = x_walk_measure)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

write.csv(mrbrt_dt, 'FILEPATH', row.names = F)

# RUN MR-BRT MODEL --------------------------------------------------------

cov_list <- lapply(mrbrt_vars, function(x) cov_info(x, "X"))


if (is.null(covariates)){

  cov_list[[1]]["covariate"] <- "intercept"
  cov_list[[1]]["uprior_lb"] <- "-inf"
  cov_list[[1]]["uprior_ub"] <- 0
  remove_x_int_model <- T
  

} else {
  
    cov_list[1] <- NULL
    cov_list_z <- cov_list
    for (i in 1:length(cov_list_z)){
      cov_list_z[[i]]["design_matrix"] <- "Z"
    }
    
    cov_list <- c(cov_list[1], cov_list, cov_list_z)
    cov_list[[1]]["covariate"] <- "intercept"
    cov_list[[1]]["uprior_lb"] <- "-inf"
    cov_list[[1]]["uprior_ub"] <- 0
    remove_x_int_model <- T

}

model_name <- paste0("alc_use_",sub_topic,"_final_",date)


if (x_walk_measure == "logit_difference"){
  mean_variable <- "ldiff"
  se_variable   <- "se_diff_logit"
} 

dem_model <- run_mr_brt(
  output_dir = dem_dir,
  model_label = model_name,
  remove_x_intercept = remove_x_int_model,
  remove_z_intercept = F,            ##CHANGE ME; WILL ALMOST ALWAYS BE F  
  data = mrbrt_dt[se_diff_logit > 1e-05],
  mean_var = mean_variable,
  se_var = se_variable,
  covs = cov_list,
  overwrite_previous = T,
  #study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1
)


fit1 <- dem_model
check_for_outputs(fit1)

#original dataset
reference_var <- "definition"
reference_value <- "reference"
mean_var <- "mean"
se_var <- "standard_error"



if (is.null(covariates)){
  cov_names <- crosswalk
} else {
  cov_names <- covariates
}


orig_vars <- c("seq", "sex", mean_var, se_var, reference_var, cov_names)


tmp_orig <- as.data.frame(dem_dt) %>%
  .[, orig_vars] %>%
  setnames(orig_vars, c("seq", "sex", "mean", "se", "ref", cov_names)) %>%
  mutate(ref = if_else(ref == reference_value, 1, 0, 0))


# this creates a ratio prediction for each observation in the original data
df_pred <- as.data.frame(tmp_orig[, cov_names])
names(df_pred) <- cov_names


if (is.null(covariates)){
    pred1 <- predict_mr_brt(fit1, newdata = df_pred) 
} else {
    tiny <- unique(df_pred)
    pred1 <- predict_mr_brt(fit1, newdata = tiny, z_newdata = tiny) 
 }
check_for_preds(pred1)

pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries

if (!is.null(covariates)){
    preds <- preds[preds$X_male == preds$Z_male,]
    
    if (sub_topic == "current_drinkers_1month"){
      preds <- preds[preds$X_age_50_plus == preds$Z_age_50_plus,]
      preds <- unique(preds)
      preds <- setnames(preds, old = c("X_age_50_plus","X_male"), new = c("age_50_plus","male"))
      tmp_orig <- merge(tmp_orig, preds, by = c("age_50_plus","male"), all.x = T)
      
    }
    if (sub_topic == "current_drinkers_1week"){
      preds <- preds[preds$X_age_under_20 == preds$Z_age_under_20,]
      preds <- unique(preds)
      preds <- setnames(preds, old = c("X_age_under_20", "X_male"), new = c("age_under_20","male"))
      tmp_orig <- merge(tmp_orig, preds, by = c("age_under_20","male"), all.x = T)
      
    }
}



###################### Produce Output- logit model ###################################
if (x_walk_measure == "logit_difference") {


tmp_orig$mean_logit <- qlogis(tmp_orig$mean)
tmp_orig$se_logit <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "se"]
  deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
})

if (!is.null(covariates)){
tmp_orig$adj_logit <- tmp_orig$Y_mean
tmp_orig$se_adj_logit <- (tmp_orig$Y_mean_hi - tmp_orig$Y_mean_lo) / 3.92
}

if (is.null(covariates)){
tmp_orig$adj_logit <- preds$Y_mean
tmp_orig$se_adj_logit <- (preds$Y_mean_hi - preds$Y_mean_lo) / 3.92
}

tmp_orig2 <- tmp_orig %>%
  mutate(
    prev_logit_adjusted_tmp = mean_logit - adj_logit,
    se_prev_logit_adjusted_tmp = sqrt(se_logit^2 + se_adj_logit^2),
    prev_logit_adjusted = ifelse(ref == 1, mean_logit, prev_logit_adjusted_tmp),
    se_prev_logit_adjusted = ifelse(ref == 1, se_logit, se_prev_logit_adjusted_tmp),
    prev_adjusted = exp(prev_logit_adjusted)/(1+exp(prev_logit_adjusted)),
    prev_adjusted2 = inv.logit(prev_logit_adjusted),
    lo_logit_adjusted = prev_logit_adjusted - 1.96 * se_prev_logit_adjusted,
    hi_logit_adjusted = prev_logit_adjusted + 1.96 * se_prev_logit_adjusted,
    lo_adjusted = exp(lo_logit_adjusted)/(1+exp(lo_logit_adjusted)),
    hi_adjusted = exp(hi_logit_adjusted)/(1+exp(hi_logit_adjusted))
  )


tmp_orig2$se_adjusted <- sapply(1:nrow(tmp_orig2), function(i) {
  mean_i <- tmp_orig2[i, "prev_logit_adjusted"]
  mean_se_i <- tmp_orig2[i, "se_prev_logit_adjusted"]
  deltamethod(~exp(x1)/(1+exp(x1)), mean_i, mean_se_i^2)
})


final_data <- merge(dem_dt, 
                    as.data.frame(tmp_orig2)[, c("ref", "prev_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted", "seq", "sex")], by = c("seq", "sex"), all = T)

final_data <- rename(final_data, mean_unadjusted = mean)

final_data$lower <- ifelse(is.na(final_data$lower), final_data$mean_unadjusted - 1.96*final_data$standard_error,final_data$lower)
final_data$upper <- ifelse(is.na(final_data$upper), final_data$mean_unadjusted + 1.96*final_data$standard_error,final_data$upper)


final_data$final_lo <- ifelse(final_data$definition == "reference", final_data$lower, final_data$lo_adjusted)
final_data$final_hi <- ifelse(final_data$definition == "reference", final_data$upper, final_data$hi_adjusted)

final_data <- final_data[ref == 0, adjustment_factor := mean_unadjusted/prev_adjusted]
mean(final_data$adjustment_factor, na.rm = T)
write.csv(final_data, 'FILEPATH', row.names = F)

}

