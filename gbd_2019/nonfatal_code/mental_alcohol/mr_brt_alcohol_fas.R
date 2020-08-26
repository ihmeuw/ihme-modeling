##########################################################################
### Project: GBD Nonfatal Estimation
### Purpose: CROSSWALKING GBD 2019
### Alcohol Use Disorder
##########################################################################

rm(list=ls())

#######################
topics <- c("alc_dep", "fetal alcohol syndrome", "alcohol_use")
sub_topics <- c("recall", "abuse", "audit", "fas", "current_drinkers_1month", "current_drinkers_1week","gday")

# Set Preferences
topic          <- topics[3]
sub_topic      <- sub_topics[6]
x_walk_measure <- "logit_difference"

if (T){
if (topic == "alc_dep" & sub_topic == "recall"){
  crosswalk      <- "cv_recall_1year"
  covariates <- NULL
  keep_more_cols <- c("audit_saq", "abuse_dependence", "is_outlier", "i_crosswalk", "i_model")
  a <- 0
  y <- 0
  
} else if (topic == "alc_dep" & sub_topic == "abuse"){
  
  crosswalk      <- "cv_abuse_dependence"
  covariates     <- NULL # will be "within study" later down 
  keep_more_cols <- c("audit_saq", "recall_1year", "is_outlier", "i_crosswalk", "i_model")
  a <- 0
  y <- 0
  
} else if (topic == "alc_dep" & sub_topic == "audit"){
  
  crosswalk      <- "cv_audit_saq"
  covariates     <- NULL  
  keep_more_cols <- c("abuse_dependence", "recall_1year", "is_outlier", "i_crosswalk", "i_model")
  a <- 0
  y <- 0
  
  
} else if (topic =="fetal alcohol syndrome" & sub_topic == "fas"){
  
  crosswalk      <- "cv_passive_case"
  covariates     <- NULL
  keep_more_cols <- "is_outlier"
  a <- 5
  y <- 5
  
} else if (topic =="alcohol_use" & sub_topic == "current_drinkers_1month"){
  
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
  
} else if (topic =="alcohol_use" & sub_topic == "gday"){
  
  crosswalk      <- "cv_drink_7_days"
  covariates <- NULL
  keep_more_cols <- NULL
  a <- 0
  y <- 5
  
} else {
  print("Invalid Specifications - Code will not run correctly")
}
    
#######################

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/" 
  h_root <- "~/"
  l_root <- "/ihme/limited_use/"
} else { 
  j_root <- "J:/"
  h_root <- "H:/"
  l_root <- "L:/"
}

library(dplyr)
pacman::p_load(data.table, openxlsx, ggplot2)
library(msm, lib.loc = paste0(j_root, "FILEPATH"))
library(msm, lib.loc = lib_home)
date <- gsub("-", "_", Sys.Date())
library(gtools)
# SET OBJECTS -------------------------------------------------------------

cv_drop <- c("cv_nodoctor_diagnosis_dementia")
draws <- paste0("draw_", 0:999)

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0("FILEPATH", "get_location_metadata.R"))
source(paste0("FILEPATH", "get_age_metadata.R"))

functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
            "load_mr_brt_preds_function")
for (funct in functs){
  source(paste0("FILEPATH", funct, ".R"))
}

# GET METADATA ------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 22)
age_dt <- get_age_metadata(12)
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]

# DATA PROCESSING FUNCTIONS -----------------------------------------------

source(paste0("FILEPATH", "99_mrbrt_functions.R"))

# RUN DATA SETUP ----------------------------------------------------------
# Load Data
dem_dt <- get_dem_data(topic, sub_topic, crosswalk = crosswalk, covariates = covariates, keep_more_cols = keep_more_cols) 
dem_dt$standard_error <- as.numeric(dem_dt$standard_error)

table(dem_dt$sex)

if (F){
if (topic == "alcohol_use"){

locs<- get_location_metadata(location_set_id =35, gbd_round_id = 6)
locs<- locs[, c("location_id", "super_region_name", "location_name", "super_region_id", "region_name", "region_id")]

dem_dt <- merge(dem_dt, locs, by = c("location_id", "location_name"))

for (k in unique(dem_dt$super_region_name)){
  
  dem_dt$super_region_name <- gsub(" ", "_",dem_dt$super_region_name)
  k <- gsub(" ", "_",k)
  k <- gsub(",", "",k)
  k <- gsub("-", "_",k)
  
  dem_dt <- dem_dt[super_region_name == k,paste0(k):= 1]
  dem_dt <- dem_dt[super_region_name != k,paste0(k):= 0]
  
}

covariates <- c(covariates,
                "Southeast_Asia_East_Asia_and_Oceania",
                "Central_Europe_Eastern_Europe_and_Central_Asia",
                "High_income",
                "Latin_America_and_Caribbean",
                "South_Asia", 
                "Sub_Saharan_Africa")

}}

if (F){
table(dem_dt$recall)
length(unique(dem_dt[recall == "1year", nid]))
length(unique(dem_dt[recall == "1month", nid]))
length(unique(dem_dt[recall == "1week", nid]))
}

if (sub_topic == "recall"){
  dem_dt <- get_dem_data(topic, sub_topic, crosswalk = crosswalk, covariates = covariates, keep_more_cols = keep_more_cols, recall_expand = T)
}

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
pairs <- combn(dem_dt[, unique(definition)], 2)

if (sub_topic == "audit"){
keep_more_cols <- c("screening_tool", "drinking_status")
dem_dt$seq <- NA
dem_dt$urbanicity_type <- NA

  dem_dt$i_crosswalk <- 1
} 

if (topic == "alc_dep"){
  matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = dem_dt[i_crosswalk == 1], year_span = y, age_span = a, covariates, keep_more_cols)))
} else {
  matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = dem_dt[(age_end-age_start <= 25)], year_span = y, age_span = a, covariates, keep_more_cols)))
}


if (matches[1,"def2"] == "reference"){
  matches <- setnames(matches, old = c("nid", "location_id", "sex", "year_start", "year_end", "measure", "age_start", "age_end", "mean", "standard_error", "cases", 
                                       "sample_size", "year_match","age_n", "seq", "urbanicity_type",
                                       "def", "nid2", "location_id2", "sex2", "year_start2", 
                                       "year_end2", "measure2", "age_start2", "age_end2", "mean2","standard_error2", "cases2", "sample_size2", "year_match2", "age_n2", 
                                       "seq2", "urbanicity_type2",
                                       "def2"
                                       , covariates, paste0(covariates, 2)), 
  new = c("nid2", "location_id2", "sex2", "year_start2", "year_end2", "measure2", "age_start2", "age_end2", "mean2","standard_error2", "cases2", 
          "sample_size2", "year_match2", "age_n2", "seq2", "urbanicity_type2", 
          "def2", "nid", "location_id", "sex", "year_start", 
          "year_end", "measure", "age_start", "age_end", "mean", "standard_error", "cases", "sample_size", "year_match","age_n", 
          "seq", "urbanicity_type", 
          "def"
          , paste0(covariates, 2),covariates))
}


if (topic == "alc_dep" & sub_topic == "recall"){
  
  matches <- matches[audit_saq == audit_saq2]
  matches <- matches[abuse_dependence == abuse_dependence2]
  
} else if (topic == "alc_dep" & sub_topic == "abuse"){
  
  matches <- matches[audit_saq == audit_saq2]
  matches <- matches[recall_1year == recall_1year2]

  #if constraining matches to exact
  matches <- matches[year_end - year_end2 == 0]
  matches <- matches[year_start - year_start2 == 0]
  matches <- matches[age_end - age_end2 == 0]
  matches <- matches[age_start - age_start2 == 0]
  
#if within study
matches <- matches[nid == nid2, within_study:= 1]
matches <- matches[nid != nid2, within_study:= 0]

table(matches$within_study)

matches$urban_match <- ifelse(matches$urbanicity_type == matches$urbanicity_type2, 1, 0)
matches <- matches[within_study == 0 | (within_study == 1 & urban_match == 1)]

matches <- matches[within_study == 1]
}


if (sub_topic == "audit"){
  matches <- matches[screening_tool == screening_tool2]
  matches <- matches[drinking_status == drinking_status2]
}


if (topic == "fetal alcohol syndrome"){
matches <- matches[abs(year_start - year_start2) <= y]
matches <- matches[abs(year_end - year_end2) <= y]
matches <- matches[abs(age_end - age_end2) <= a]
matches <- matches[abs(age_start - age_start2) <= a]
}

if (F){
print(paste0("Age: ",a,"; Year: ",y))
print(nrow(matches))
print("Ref")
print(length(unique(matches$seq)))
print("Alt")
print(length(unique(matches$seq2)))
}
matches <- create_ratios(matches, x_walk_measure)

write.csv(matches, "FILEPATH", row.names = F)


if (F){ 
# Make histogram of ratios
test <- merge(matches, loc_dt, by = "location_id", all.x = T)
ggplot(test, aes(x = ldiff, fill = as.factor(location_name))) + geom_histogram(bins = 50)
}

if (F) {
if (sub_topic == "recall"){
matches$age_middle_test <- (matches$age_start + matches$age_end)/2
table(matches$age_middle_test == matches$age_middle)
matches$age_middle <- (matches$age_start + matches$age_end)/2
}
}

mrbrt_setup <- create_mrbrtdt(matches, xcovs = covariates, x_walk_measure = x_walk_measure)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]



write.csv(mrbrt_dt, "FILEPATH", row.names = F)

# RUN MR-BRT MODEL --------------------------------------------------------

cov_list <- lapply(mrbrt_vars, function(x) cov_info(x, "X"))

if (sub_topic == "audit"){
  cov_list <- NULL
  remove_x_int_model <- F
}

if (is.null(covariates)){
if (topic == "fetal alcohol syndrome" | topic == "alcohol_use"){

  cov_list[[1]]["covariate"] <- "intercept"
  cov_list[[1]]["uprior_lb"] <- "-inf"
  cov_list[[1]]["uprior_ub"] <- 0
  remove_x_int_model <- T
  
  
  if (sub_topic == "gday"){
    cov_list <- NULL
    remove_x_int_model <- F
  }
  
  
  
} else if (sub_topic == "abuse" | sub_topic == "recall"){
  cov_list[[1]]["covariate"] <- "intercept"
  cov_list[[1]]["uprior_lb"] <- 0
  cov_list[[1]]["uprior_ub"] <- "inf"
  
  remove_x_int_model <- T

 
  
  
}} else {
  
  if (topic == "fetal alcohol syndrome" | topic == "alcohol_use"){    
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
    
    
    if (sub_topic == "gday"){
      cov_list <- NULL
      remove_x_int_model <- F
    }
    
    
  } else if (sub_topic == "abuse" | sub_topic == "recall"){
    cov_list[[1]]["covariate"] <- "intercept"
    cov_list[[1]]["uprior_lb"] <- 0
    cov_list[[1]]["uprior_ub"] <- "inf"
    
    remove_x_int_model <- T
    
  }
  
  
}
model_name <- paste0(topic,"_",sub_topic,"_final_",date)



if (x_walk_measure == "logit_difference"){
  mean_variable <- "ldiff"
  se_variable   <- "se_diff_logit"
} else if (x_walk_measure == "odds_ratio"){
  mean_variable <- "log_ratio"
  se_variable   <- "log_rse"
}



dem_model <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = model_name,
  remove_x_intercept = remove_x_int_model,
  remove_z_intercept = F,           
  data = mrbrt_dt[se_diff_logit > 1e-05],
  mean_var = mean_variable,
  se_var = se_variable,
  covs = cov_list,
  overwrite_previous = T,
  method = "trim_maxL",
  trim_pct = 0.1
)

if (F){
ggplot(matches) +
  geom_point(aes(x = (age_start + age_end)/2,y= ldiff, color = sex))

ggplot(dem_dt) +
  geom_point(aes(x = (age_start + age_end)/2,y= mean, color = cv_recall_1m, alpha = 1/5)) + 
  facet_wrap(~super_region_name)
}



fit1 <- dem_model
check_for_outputs(fit1)

#original dataset
reference_var <- "definition"
reference_value <- "reference"
mean_var <- "mean"
se_var <- "standard_error"

if (topic == "fetal alcohol syndrome"){
cov_names <- "cv_passive_case" 
}

if (sub_topic == "audit"){
  cov_names <- "cv_audit_saq"

  dem_dt <- get_dem_data(topic, sub_topic = "abuse", crosswalk = crosswalk, covariates = covariates, keep_more_cols = NULL)
  dem_dt$definition <- ifelse(dem_dt$cv_audit_saq == 0, "reference", "alt")
  }

if (sub_topic == "abuse"){
  cov_names <- "cv_abuse_dependence"
  }


if (sub_topic == "recall"){
  cov_names <- c("cv_recall_1year")
  }

if (topic == "alcohol_use"){
  if (is.null(covariates)){
    cov_names <- crosswalk
  } else {
    cov_names <- covariates
  }
}


orig_vars <- c("seq", "sex", mean_var, se_var, reference_var, cov_names)

library(dplyr)


tmp_orig <- as.data.frame(dem_dt) %>%
  .[, orig_vars] %>%
  setnames(orig_vars, c("seq", "sex", "mean", "se", "ref", cov_names)) %>%
  mutate(ref = if_else(ref == reference_value, 1, 0, 0))


# this creates a ratio prediction for each observation in the original data
df_pred <- as.data.frame(tmp_orig[, cov_names])
names(df_pred) <- cov_names


if (is.null(covariates)){
  
  pred1 <- predict_mr_brt(fit1, newdata = df_pred) 

} 

if (!is.null(covariates)){
    tiny <- unique(df_pred)
    pred1 <- predict_mr_brt(fit1, newdata = tiny, z_newdata = tiny) 
 }
check_for_preds(pred1)

pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries

if (!is.null(covariates)){
  if (topic == "alcohol_use"){
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
}


if (F){
if (sub_topic == "abuse"){
  preds <- preds[preds$X_within_study == preds$Z_within_study,]
  preds <- dplyr::rename(preds, within_study = X_within_study)
  tmp_orig <- merge(tmp_orig, preds, by = "within_study", all.x = T)
}}

if (F){
if (sub_topic == "recall"){
  preds <- preds[preds$X_male == preds$Z_male,]
  preds <- dplyr::rename(preds, male = X_male)
  tmp_orig <- merge(tmp_orig, preds, by = "male", all.x = T)
}}

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
write.csv(final_data, paste0(dem_dir, model_name,".csv"), row.names = F)

}

ggplot(final_data[adjustment_factor & sex == "Male"]) +
  geom_histogram(aes(x = adjustment_factor, fill = as.factor(age_middle)))

ggplot(final_data) +
  geom_point(aes(x = age_start, y = adjustment_factor, color = sex))

tmp_orig$mean_log <- log(tmp_orig$mean)
tmp_orig$se_log <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "se"]
  deltamethod(~log(x1), mean_i, se_i^2)
})

if (topic == "fetal alcohol syndrome"){
  tmp_orig$beta0 <- preds$Y_mean
  tmp_orig$beta0_se_tau <- (preds$Y_mean_hi - preds$Y_mean_lo) / 3.92
}

if (sub_topic == "abuse"){
tmp_orig$beta0 <- tmp_orig$Y_mean
tmp_orig$beta0_se_tau <- (tmp_orig$Y_mean_hi - tmp_orig$Y_mean_lo) / 3.92
}

tmp_orig2 <- tmp_orig %>%
  mutate(
    mean_log_tmp = mean_log - beta0, # adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
    var_log_tmp = se_log^2 + beta0_se_tau^2, # adjust the variance
    se_log_tmp = sqrt(var_log_tmp)
  )



# if original data point was a reference data point, leave as-is
tmp_orig3 <- tmp_orig2 %>%
  mutate(
    mean_log_adjusted = if_else(ref == 1, mean_log, mean_log_tmp),
    se_log_adjusted = if_else(ref == 1, se_log, se_log_tmp),
    lo_log_adjusted = mean_log_adjusted - 1.96 * se_log_adjusted,
    hi_log_adjusted = mean_log_adjusted + 1.96 * se_log_adjusted,
    mean_adjusted_odds = exp(mean_log_adjusted),
    lo_adjusted_odds = exp(lo_log_adjusted),
    hi_adjusted_odds = exp(hi_log_adjusted) )

tmp_orig3$se_adjusted_odds <- sapply(1:nrow(tmp_orig3), function(i) {
  ratio_i <- tmp_orig3[i, "mean_log_adjusted"]
  ratio_se_i <- tmp_orig3[i, "se_log_adjusted"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})


final_data2 <- merge(dem_dt, tmp_orig3[, c("seq","mean_adjusted_odds", "lo_adjusted_odds", "hi_adjusted_odds")], by = "seq", all = T)

write.csv(final_data2, "FILEPATH", row.names = F)



