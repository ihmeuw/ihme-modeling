##################################################################################################
## Purpose: MND Iterative Sex split, Crosswalks, and Age split
## Creation Date: 05/09/20
## Created by: USERNAME
## Based on code by USERNAME
##################################################################################################

rm(list=ls())
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}


library(mortdb, lib = "FILEPATH")
library(Hmisc)
library(data.table)
library(msm)
library(plyr)
library(openxlsx)
##################################################################################################

#Set objects
date <- gsub("-", "_", Sys.Date())
bundle_id <- b_id <- 449
decomp_step <- 'iterative'
gbd_round_id <- 7
bundle_version_id <-32864
draws <- paste0("draw_", 0:999)
cause_name <- cause_path <- "449_neuro_neurone"


pacman::p_load(data.table, openxlsx, ggplot2, magrittr)
date <- gsub("-", "_", Sys.Date())
repo_dir <- paste0("FILEPATH")
functions_dir <- "FILEPTAH"
draws <- paste0("draw_", 0:999)

# GET OBJECTS -------------------------------------------------------------
#Objects for age split
a_cause <- "MOD_ENV_"
id <- 3940 ## this is the meid for age split Dismod me id of cause
ver_id <- 366110 ## Dismod version used for age split
measure_dismod <- 5 #5 = prev, 6 = incidence, 18 = proportion
round_id = 6 #6 for a Dismod model from 2019, 7 for a model from 2020 This matches where we pulled our dismod model from.
step <- 'step4' #step for Dismod version used for age split (usually iterative) This matches where we pulled our dismod model from.
region_pattern <- F #T if want to use super region pattern for age split, F to use global
round_id_functions <- 7 #this is for shared function calls
step_id_functions <- 'iterative' #this is for shared function calls

path_to_data <- paste0("FILEPATH", date, "_", decomp_step, "_",bundle_id, "age_sex_split_crosswalked", ".xlsx")
description <- "Iterative (Step 2) corrected age and sex split and xwalks from 2019 best with new norway subnational and clinical, loc id 4625 outliered duplicate clinical data fixed correctly with 2019 claims"

#File/Dir paths
mrbrt_helper_dir <- paste0(j_root, "FILEPATH")
dem_dir <- paste0("FILEPATH")

##################################################################################################
#Source Functions:
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")

functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
            "load_mr_brt_preds_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}

##################################################################################################
#Function definitions:

get_row <- function(n, dt){
  row <- copy(dt)[n]
  pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                          age_group_years_start >= row[, age_start] & age_group_years_end <= row[, age_end]])
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex")]
  row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
              both_N = agg[sex == "Both", pop_sum])]
  return(row)
}


graph_predictions <- function(dt){
  #Set measure to incidence, prevalence, or proportion depending on model
  graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
  graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
  graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"), allow.cartesian = TRUE)
  graph_dt[, N := (mean*(1-mean)/error^2)]
  graph_dt <- subset(graph_dt, value > 0)
  wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  graph_dt[, midage := (age_end + age_start)/2]
  ages <- c(60, 70, 80, 90)
  graph_dt[, age_group := cut2(midage, ages)]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper)) +
    facet_wrap(~midage) +
    labs(x = "Both Sex Mean", y = " Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex Split Means Compared to Both Sex Mean") +
    scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
    theme_classic()
  return(gg_sex)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

##################################################################################################
#Sex split:
bundle_449_version_all <- get_bundle_version(bundle_version_id = bundle_version_id,
                                             fetch = "all")
#unique(dt$measure) #Checking if we are reading in only prevalence and incidence
dt <- copy(data.table(bundle_449_version_all))
dt <- dt[measure == "prevalence" | measure == "incidence"]
dt <- dt[group_review == 1 | is.na(group_review),] #Onlywant obs which have not been group reviewed out previously
dt <- dt[is_outlier == 0 | is.na(is_outlier),]
dt <- dt[age_end > 99, age_end := 99]

#Correcting crosswalk values:  
dt[field_citation_value %like% "Truven Health" & year_start == 2000, cv_marketscan_all_2000 := 1]
View(sort(unique(dt[field_citation_value %like% "Truven Health" , field_citation_value])))
dt[field_citation_value %like% "Truven Health" & year_start != 2000, cv_marketscan_all_2000 := 0]

dt <- dt[, midage := (age_start + age_end)/2]
dt$midage <- round(dt$midage/5)*5

tosplit_dt <- as.data.table(copy(dt))
nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female")] 
tosplit_dt <- tosplit_dt[sex == "Both"]

tosplit_dt[, midyear := floor((year_start + year_end)/2)]

pred_draws <- as.data.table(read.csv("FILEPATH"))

pred_draws[, c("X_intercept", "Z_intercept") := NULL]
pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)

pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                         year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)])
pops[age_group_years_end == 125, age_group_years_end := 99]

tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt), mc.cores = 9))
tosplit_dt <- tosplit_dt[!is.na(both_N)]
tosplit_dt[, merge := 1]
pred_draws[, merge := 1]
split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
male_dt <- copy(split_dt)
male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = NA, lower = NA,
                cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                      ratio_se, ")"))]
if ("seq" %in% names(male_dt)) {
  male_dt[, crosswalk_parent_seq := seq]
  male_dt[, seq := NA]
}
male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
male_dt <- dplyr::select(male_dt, names(dt))
female_dt <- copy(split_dt)
female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA,
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                        ratio_se, ")"))]
if ("seq" %in% names(female_dt)) {
  female_dt[, crosswalk_parent_seq := seq]
  female_dt[, seq := NA]
}
female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
female_dt <- dplyr::select(female_dt, names(dt))
total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))

predict_sex <- list(final = total_dt, graph = split_dt)

pdf(paste0(dem_dir,"/", date,"_sex_split_graph.pdf"))
graph_predictions(predict_sex$graph)
dev.off()

table(total_dt$location_name)

#Coefficients from 2019 ALS xwalk
als_coefs <- as.data.table(read.csv("FILEPATH"))
#Get model summaries for Marketscan crosswalks
predicted_market_2000 <- as.data.table(read.csv("FILEPATH"))

get_preds_adjustment_sex <- function(raw_data, model){
  dt <- copy(raw_data)
  cv_cols <- names(dt)[grepl("^cv", names(dt))]
  cv_cols <- c(cv_cols, names(dt)[grepl("midage", names(dt))])
  cv_cols <- cv_cols[!cv_cols %in% cv_drop]
  dt <- unique(dplyr::select(dt, cv_cols))
  setnames(dt, names(dt), gsub("^cv_", "", names(dt)))
  dt[, sum := rowSums(.SD), .SDcols = names(dt)]
  dt <- dt[!sum == 0]
  dt[, sum := NULL]
  preds <- predict_mr_brt(model, newdata = dt, write_draws = T)
  pred_dt <- as.data.table(preds$model_draws)
  pred_dt[, logadj := rowMeans(.SD), .SDcols = draws]
  pred_dt[, logadj_se := apply(.SD, 1, sd), .SDcols = draws]
  pred_dt[, c(draws, "Z_intercept") := NULL]
  setnames(pred_dt, names(pred_dt), gsub("^X_", "", names(pred_dt)))
  return(pred_dt)
}
get_preds_adjustment <- function(raw_data, model){
  dt <- copy(raw_data)
  cv_cols <- names(dt)[grepl("^cv", names(dt))]
  cv_cols <- cv_cols[!cv_cols %in% cv_drop]
  dt <- unique(dplyr::select(dt, cv_cols))
  setnames(dt, names(dt), gsub("^cv_", "", names(dt)))
  dt[, sum := rowSums(.SD), .SDcols = names(dt)]
  dt <- dt[!sum == 0]
  dt[, sum := NULL]
  preds <- predict_mr_brt(model, newdata = dt, write_draws = T)
  pred_dt <- as.data.table(preds$model_draws)
  pred_dt[, logadj := rowMeans(.SD), .SDcols = draws]
  pred_dt[, logadj_se := apply(.SD, 1, sd), .SDcols = draws]
  pred_dt[, c(draws, "Z_intercept") := NULL]
  setnames(pred_dt, names(pred_dt), gsub("^X", "cv", names(pred_dt)))
  return(pred_dt)
}
make_adjustment_sex <- function(data_dt, ratio_dt){
  data_dt <- adjust_dt
  ratio_dt <- adjust_preds
  cvs <- names(ratio_dt)[grepl("^cv", names(ratio_dt))]
  dt <- merge(data_dt, ratio_dt, by = "midage", all.x = T, allow.cartesian=TRUE)
  adjust_dt <- copy(dt[!is.na(logadj) & !mean == 0])
  noadjust_dt <- copy(dt[is.na(logadj) | mean == 0])
  noadjust_dt[, c("logadj", "logadj_se") := NULL]
  
  ## ADJUST MEANS
  adjust_dt[, logmean := log(mean)]
  adjust_dt$log_se <- sapply(1:nrow(adjust_dt), function(i){
    mean_i <- adjust_dt[i, "mean"]
    se_i <- adjust_dt[i, "standard_error"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  adjust_dt[, logmean_adj := logmean - logadj]
  adjust_dt[, logmean_adj_se := sqrt(logadj_se^2 + log_se^2)]
  adjust_dt[, mean_adj := exp(logmean_adj)]
  adjust_dt$standard_error_adj <- sapply(1:nrow(adjust_dt), function(i){
    mean_i <- adjust_dt[i, "logmean_adj"]
    se_i <- adjust_dt[i, "logmean_adj_se"]
    deltamethod(~exp(x1), mean_i, se_i^2)
  })
  
  full_dt <- copy(adjust_dt)
  full_dt$note_modeler<-c(rep(NA, length(full_dt[,1])))
  full_dt[, `:=` (mean = mean_adj, standard_error = standard_error_adj, upper = "", lower = "",
                  cases = "", sample_size = "", uncertainty_type_value = "",
                  note_modeler = paste0(note_modeler, " | crosswalked with log(ratio): ", round(logadj, 2), " (",
                                        round(logadj_se), ")"))]
  extra_cols <- setdiff(names(full_dt), names(noadjust_dt))
  full_dt[, c(extra_cols) := NULL]
  final_dt <- rbind(noadjust_dt, full_dt)
  return(list(epidb = final_dt, vetting_dt = adjust_dt))
}
make_adjustment <- function(data_dt, ratio_dt){
  data_dt <- adjust_dt
  ratio_dt <- adjust_preds
  cvs <- names(ratio_dt)[grepl("^cv", names(ratio_dt))]
  dt <- merge(data_dt, ratio_dt, by = cvs, all.x = T)
  adjust_dt <- copy(dt[!is.na(logadj) & !mean == 0])
  noadjust_dt <- copy(dt[is.na(logadj) | mean == 0])
  noadjust_dt[, c("logadj", "logadj_se") := NULL]
  
  ## ADJUST MEANS
  adjust_dt[, logmean := log(mean)]
  adjust_dt$log_se <- sapply(1:nrow(adjust_dt), function(i){
    mean_i <- adjust_dt[i, "mean"]
    se_i <- adjust_dt[i, "standard_error"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  adjust_dt[, logmean_adj := logmean - logadj]
  adjust_dt[, logmean_adj_se := sqrt(logadj_se^2 + log_se^2)]
  adjust_dt[, mean_adj := exp(logmean_adj)]
  adjust_dt$standard_error_adj <- sapply(1:nrow(adjust_dt), function(i){
    mean_i <- adjust_dt[i, "logmean_adj"]
    se_i <- adjust_dt[i, "logmean_adj_se"]
    deltamethod(~exp(x1), mean_i, se_i^2)
  })
  
  full_dt <- copy(adjust_dt)
  full_dt$note_modeler<-c(rep(NA, length(full_dt[,1])))
  full_dt[, `:=` (mean = mean_adj, standard_error = standard_error_adj, upper = "", lower = "",
                  cases = "", sample_size = "", uncertainty_type_value = "",
                  note_modeler = paste0(note_modeler, " | crosswalked with log(ratio): ", round(logadj, 2), " (",
                                        round(logadj_se), ")"))]
  extra_cols <- setdiff(names(full_dt), names(noadjust_dt))
  full_dt[, c(extra_cols) := NULL]
  final_dt <- rbind(noadjust_dt, full_dt)
  return(list(epidb = final_dt, vetting_dt = adjust_dt))
}


##################################################################################################
#Applying ALS and Marketscan xwalks
actual_data <- copy(total_dt)
actual_data$cv_ALS[(is.na(actual_data$cv_ALS))]<-0
actual_data$cv_marketscan_all_2000[(is.na(actual_data$cv_marketscan_all_2000))]<-0
table(actual_data$cv_ALS)
table(actual_data$cv_marketscan_all_2000)

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

actual_data <- get_se(actual_data)
dim(actual_data) 

#Log transform original data
actual_data$mean_log <- log(actual_data$mean)

actual_data$log_se <- sapply(1:nrow(actual_data), function(i) {
  ratio_i <- actual_data[i, mean]
  ratio_se_i <- actual_data[i, standard_error]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


#Prep lit crosswalk (predict_mr_brt) for ALS crosswalk
lit_betas <- als_coefs
variance <- lit_betas$beta_var + lit_betas$gamma_soln^2
Y_mean_lo <- (lit_betas$beta_soln - 1.96*variance^0.5)
Y_mean_hi <- (lit_betas$beta_soln + 1.96*variance^0.5)
Y_mean <- lit_betas$beta_soln
cv_ALS <- 1
cv_marketscan_all_2000 <- 0

predicted_lit <- cbind(Y_mean, Y_mean_lo, Y_mean_hi, cv_ALS, cv_marketscan_all_2000)
predicted_lit <- as.data.frame(predicted_lit)


#Start of marketscan xwalk


setnames(predicted_market_2000, "X_marketscan_all_2000", "cv_marketscan_all_2000")
predicted_market_2000 <- predicted_market_2000[, c("cv_marketscan_all_2000", "Y_mean", "Y_mean_lo", "Y_mean_hi")]

predicted_market_2000$cv_marketscan_all_2000 <- 1
predicted_market_2000$cv_ALS <- 0

predicted_all <- rbind.fill(predicted_lit, predicted_market_2000)
predicted_all <- as.data.table(predicted_all)

predicted_all[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
pred1 <- predicted_all[1,]
pred2 <- predicted_all[2,]
pred1[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
pred1<- pred1[,Y_se_norm]
pred2[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
pred2<- pred2[,Y_se_norm]
Y_se_norm <- c(pred1, pred2)

predicted_all <- cbind(predicted_all,Y_se_norm)

pred0 <- data.frame("cv_ALS" = 0, "cv_marketscan_all_2000"=0, "Y_mean"=0, "Y_mean_lo"=0, "Y_mean_hi"=0, "Y_se"=0, "Y_se_norm"=0)
predicted_all <- rbind.fill(predicted_all, pred0)
predicted_all <- as.data.table(predicted_all)

review_sheet_final <- merge(actual_data, predicted_all, by=c("cv_ALS", "cv_marketscan_all_2000")) 
review_sheet_final <- as.data.table(review_sheet_final)
dim(review_sheet_final)

setnames(review_sheet_final, "mean", "mean_orig")
review_sheet_final[, `:=` (log_mean = log(mean_orig), log_se = deltamethod(~log(x1), mean_orig, standard_error^2)), by = c("mean_orig", "standard_error")]
review_sheet_final[Y_mean != predicted_all[3,Y_mean], `:=` (log_mean = log_mean - Y_mean, log_se = sqrt(log_se^2 + Y_se^2))]
review_sheet_final[Y_mean != predicted_all[3,Y_mean], `:=` (mean_new = exp(log_mean), standard_error_new = deltamethod(~exp(x1), log_mean, log_se^2)), by = c("log_mean", "log_se")]
review_sheet_final[Y_mean == predicted_all[3,Y_mean], `:=` (mean_new = mean_orig, standard_error_new = standard_error)]
review_sheet_final[, `:=` (cases_new = NA, lower_new = NA, upper_new = NA)]
review_sheet_final[, (c("Y_mean", "Y_se", "log_mean", "log_se", "Y_se_norm")) := NULL]

review_sheet_final$covariate <- ifelse(review_sheet_final$cv_ALS==1,1,
                                       ifelse(review_sheet_final$cv_marketscan_all_2000==1,2,3))
review_sheet_final$covariate <- as.character(review_sheet_final$covariate)

gg_scatter <- ggplot(review_sheet_final, aes(x=mean_orig, y=mean_new, color=covariate)) + geom_point() +
  geom_abline(slope=1, intercept = 0) +
  xlim(0,01) + ylim(0,01) + coord_fixed() +
  ggtitle("Adjusted Means Vs Unadjusted Means") +
  theme_classic()

gg_scatter

review_sheet_final[mean_new == mean_orig, crosswalk_parent_seq := ""] #Rows where mean_new and mean_orig match are unadjusted
review_sheet_final[mean_new != mean_orig | standard_error_new != standard_error, crosswalk_parent_seq := seq] #Rows where mean_new does not equal mean_orig
review_sheet_final[, seq := as.character(seq)]
review_sheet_final[mean_new != mean_orig | standard_error_new != standard_error, seq := ""] #Rows where mean_new does not equal mean_orig are the ones that were sex split

# For upload validation
setnames(review_sheet_final, "lower", "lower_orig")
setnames(review_sheet_final, "upper", "upper_orig")
setnames(review_sheet_final, "standard_error", "standard_error_orig")
setnames(review_sheet_final, "lower_new", "lower")
setnames(review_sheet_final, "upper_new", "upper")
setnames(review_sheet_final, "standard_error_new", "standard_error")
setnames(review_sheet_final, "mean_new", "mean")
review_sheet_final[is.na(lower), uncertainty_type_value := NA]


summary(actual_data$mean)       
summary(review_sheet_final$mean) 

summary(actual_data$standard_error)       
summary(review_sheet_final$standard_error) 

review_sheet_final[standard_error > 1, standard_error := 1]

write.xlsx(review_sheet_final, file = path_to_data, sheetname = "extraction")

#########################################################################
### Author: USERNAME, modified by USERNAME
### Date: 5/15/2019, modified Feb/October 2020
### Project: GBD Nonfatal Estimation
### Purpose: Age Splitting GBD 2020
##########################################################################

# GET FUNCTIONS -----------------------------------------------------------
functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))


## FILL OUT MEAN/CASES/SAMPLE SIZE
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

## GET CASES IF THEY ARE MISSING
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "proportion", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}


## MAKE SURE DATA IS FORMATTED CORRECTLY
format_data <- function(unformatted_dt, sex_dt){
  dt <- copy(unformatted_dt)
  dt[, `:=` (mean = as.numeric(mean), sample_size = as.numeric(sample_size), cases = as.numeric(cases),
             age_start = as.numeric(age_start), age_end = as.numeric(age_end), year_start = as.numeric(year_start))]
  dt <- dt[measure %in% c("proportion", "prevalence", "incidence"),]
  dt <- dt[group_review == 1 | is.na(group_review)]
 
  dt <- dt[is_outlier==0 | is.na(is_outlier)]
  dt <- dt[(age_end-age_start)>=25,] #split rows with greater than or equal to 25 year age range
  dt <- dt[!mean == 0 & !cases == 0, ] ##don't split rows with zero prevalence
  dt <- merge(dt, sex_dt, by = "sex")
  dt[measure == "proportion", measure_id := 18]
  dt[measure == "prevalence", measure_id := 5]
  dt[measure == "incidence", measure_id := 6]
  dt[, year_id := round((year_start + year_end)/2, 0)] 
  return(dt)
}

## CREATE NEW AGE ROWS
expand_age <- function(small_dt, age_dt = ages){
  dt <- copy(small_dt)
  
  ## ROUND AGE GROUPS
  dt[, age_start := age_start - age_start %%5]
  dt[, age_end := age_end - age_end %%5 + 4]
  dt <- dt[age_end > 99, age_end := 99]
  
  ## EXPAND FOR AGE
  dt[, n.age:=(age_end+1 - age_start)/5]
  dt[, age_start_floor:=age_start]
  dt[, drop := cases/n.age] ##drop the data points if cases/n.age is less than 1
  dt[!drop<1,]
  expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
  split <- merge(expanded, dt, by="id", all=T)
  split[, age.rep := 1:.N - 1, by =.(id)]
  split[, age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4]
  split <- merge(split, age_dt, by = c("age_start", "age_end"), all.x = T)
  split[age_start == 0 & age_end == 4, age_group_id := 1]
  split <- split[age_group_id %in% age | age_group_id == 1]
  return(split)
}

## GET DISMOD AGE PATTERN
get_age_pattern <- function(locs, id, age_groups){
  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, 
                           measure_id = measure_dismod, location_id = locs, source = "epi", status= 'best',
                           sex_id = c(1,2), gbd_round_id = round_id, decomp_step = step, 
                           age_group_id = age_groups, year_id = 2010) 
  us_population <- get_population(location_id = locs, year_id = 2010, sex_id = c(1, 2),
                                  age_group_id = age_groups, gbd_round_id = round_id_functions, decomp_step = step_id_functions)
  us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]
  
  ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
  age_1 <- copy(age_pattern)
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5), ]
  se <- copy(age_1)
  se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] 
  age_1 <- merge(age_1, us_population, by = c("age_group_id", "sex_id", "location_id"))
  age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
  age_1[, frac_pop := population / total_pop]
  age_1[, weight_rate := rate_dis * frac_pop]
  age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
  age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
  age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
  age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
  age_1[, age_group_id := 1]
  age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5)]
  age_pattern <- rbind(age_pattern, age_1)
  
  ## CASES AND SAMPLE SIZE
  age_pattern[measure_id == 18, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] 
  age_pattern[is.nan(cases_us), cases_us := 0]
  
  ## GET SEX ID 3
  sex_3 <- copy(age_pattern)
  sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, rate_dis := cases_us/sample_size_us]
  sex_3[measure_id == 18, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
  sex_3[measure_id == 5, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)]
  sex_3[measure_id == 6, se_dismod := sqrt(cases_us)/sample_size_us]
  sex_3[is.nan(rate_dis), rate_dis := 0]
  sex_3[is.nan(se_dismod), se_dismod := 0]
  sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
  sex_3[, sex_id := 3]
  age_pattern <- rbind(age_pattern, sex_3)
  
  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
  return(age_pattern)
}

## GET POPULATION STRUCTURE
get_pop_structure <- function(locs, years, age_groups){
  populations <- get_population(location_id = locs, year_id = years, gbd_round_id=round_id_functions, decomp_step = step_id_functions,
                                sex_id = c(1, 2, 3), age_group_id = age_groups)
  age_1 <- copy(populations) ##create age group id 1 by collapsing lower age groups
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5)]
  age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
  age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
  age_1[, age_group_id := 1]
  populations <- populations[!age_group_id %in% c(2, 3, 4, 5)]
  populations <- rbind(populations, age_1)  ##add age group id 1 back on
  return(populations)
}

## ACTUALLY SPLIT THE DATA
split_data <- function(raw_dt){
  dt1 <- copy(raw_dt)
  dt1[, total_pop := sum(population), by = "id"]
  dt1[, sample_size := (population / total_pop) * sample_size]
  dt1[, cases_dis := sample_size * rate_dis]
  dt1[, total_cases_dis := sum(cases_dis), by = "id"]
  dt1[, total_sample_size := sum(sample_size), by = "id"]
  dt1[, all_age_rate := total_cases_dis/total_sample_size]
  dt1[, ratio := mean / all_age_rate]
  dt1[, mean := ratio * rate_dis ]
  dt1 <- dt1[mean < 1, ]
  dt1[, cases := mean * sample_size]
  return(dt1)
}

## FORMAT DATA TO FINISH
format_data_forfinal <- function(unformatted_dt, location_split_id, region, original_dt){
  dt <- copy(unformatted_dt)
  dt[, group := 1]
  dt[, specificity := "age,sex"]
  dt[, group_review := 1]
  dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
  dt[, (blank_vars) := NA]
  dt <- get_se(dt)
  if (region == T) {
    dt[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern", date)]
  } else {
    dt[, note_modeler := paste0(note_modeler, "| age split using the age pattern from location id ", location_split_id, " ", date)]
  }
  split_ids <- dt[, unique(id)]
  dt <- rbind(original_dt[!id %in% split_ids], dt, fill = T)
  dt <- dt[, c(names(df)), with = F] 
  return(dt)
}

# ---------------------------------------------------------------------------
dt <- copy(review_sheet_final)
ages <- get_age_metadata(12, gbd_round_id= 6)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age <- ages[age_start >= 0, age_group_id]


df <- copy(dt)
gbd_id <- id

# AGE SPLIT FUNCTION -----------------------------------------------------------
age_split <- function(gbd_id, df, age, region_pattern, location_pattern_id = 1){
  
  cols<- c(names(df)) 
  
  ## GET TABLES
  sex_names <- get_ids(table = "sex")
  ages[, age_group_weight_value := NULL]
  ages[age_start >= 1, age_end := age_end - 1]
  ages[age_end == 124, age_end := 99]
  super_region_dt <- get_location_metadata(location_set_id = 22, gbd_round_id=round_id_functions) 
  super_region_dt <- super_region_dt[, .(location_id, super_region_id)]
  
  
  ## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQS)
  df[, id := 1:.N]
  original <- copy(df)
  
  
  ## FORMAT DATA
  df[, cases:= as.numeric(cases)]
  df[, sample_size:= as.numeric(sample_size)]
  df <- get_cases_sample_size(df)
  df <- get_se(df)
  df <- calculate_cases_fromse(df)
  dt <- format_data(df, sex_dt = sex_names) 
  
  ## EXPAND AGE
  split_dt <- expand_age(dt, age_dt = ages)
 
  ## GET PULL LOCATIONS
  if (region_pattern == T){
    split_dt <- merge(split_dt, super_region_dt, by = "location_id")
    super_regions <- unique(split_dt$super_region_id) ##get super regions for dismod results
    locations <- super_regions
  } else {
    locations <- location_pattern_id
  }
  
  ##GET LOCS AND POPS
  pop_locs <- unique(split_dt$location_id)
  pop_years <- unique(split_dt$year_id)
  
  ## GET AGE PATTERN
  print("getting age pattern")
  age_pattern <- get_age_pattern(locs = locations, id = gbd_id, age_groups = age)
  
  if (region_pattern == T) {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
  } else {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
  }
  
  ## GET POPULATION INFO
  print("getting pop structure")
  pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
  split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))
  
  #####CALCULATE AGE SPLIT POINTS#######################################################################
  ## CREATE NEW POINTS
  print("splitting data")
  split_dt <- split_data(split_dt)
  ######################################################################################################
  # assign seqs
  split_dt$crosswalk_parent_seq <- split_dt$seq 
  
  final_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                   original_dt = original)
  
  return(final_dt)
}

final_split <- age_split(gbd_id = id, df = dt, age = age, region_pattern, location_pattern_id = 1)

#Check age split was successful
check_dt <- final_split[(final_split$age_end-final_split$age_start>25) & final_split$mean!=0 & final_split$is_outlier!=1 & (final_split$measure=="prevalence" | final_split$measure=="incidence" | final_split$measure=="proportion"), ]

sum(is.na(check_dt$mean)) 
table(check_dt$is_outlier, useNA = "ifany") 
table(check_dt$measure, useNA = "ifany")

check_dt[, n.age:=(age_end+1 - age_start)/5]
check_dt[, drop := cases/n.age]
sum(check_dt[, drop < 1]) 

final_split[recall_type == "", recall_type := "Not Set"]
final_split[unit_type == "", unit_type := "Person"]
final_split[urbanicity_type == "", urbanicity_type := "Unknown"]
final_split[is.na(unit_value_as_published), unit_value_as_published := 1]

final_split <- final_split[location_id != "4625",]

#Write flat file

write.xlsx(final_split, file = path_to_data, sheetName = "extraction", col.names = T)

save_crosswalk_version(
  bundle_version_id = bundle_version_id,
  data_filepath = path_to_data,
  description = description
)