## Epilepsy work flow (post sex split) - 2019 GBD Step 4
## USER
## September 22, 2019
## Run Marketscan, Marketscan 2000, and lifetime recall crosswalks


library(data.table)
library(dplyr)
library(plyr)
library(msm, lib.loc = "FILEPATH")
library(readxl)
library(stringr)

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
}

functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
            "load_mr_brt_preds_function")
mrbrt_helper_dir <- paste0(j_root, "FILEPATH")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}



cause_path <- "epilepsy/FILEPATH"
cause_name <- "EPI_"
main_dir <- "FILEPATH"



#########################################################################################################################################
#Prep original data
actual_data <- as.data.table(read.csv(paste0(main_dir, cause_path, cause_name, "sex.csv")))

actual_data[is.na(mean) & !is.na(cases) & !is.na(sample_size), mean := cases/sample_size]

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


#Log transform original data
actual_data$mean_log <- log(actual_data$mean)

actual_data$log_se <- sapply(1:nrow(actual_data), function(i) {
  ratio_i <- actual_data[i, mean]
  ratio_se_i <- actual_data[i, standard_error]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

#Prep lit crosswalk (predict_mr_brt)
lit_betas <- as.data.table(read.csv("FILEPATH"))
variance <- lit_betas$beta_var + lit_betas$gamma_soln^2
Y_mean_lo <- (lit_betas$beta_soln - 1.96*variance^0.5)
Y_mean_hi <- (lit_betas$beta_soln + 1.96*variance^0.5)
Y_mean <- lit_betas$beta_soln
cv_recall_lifetime <- 1
cv_marketscan <- 0
cv_marketscan_all_2000 <- 0

predicted_lit <- cbind(Y_mean, Y_mean_lo, Y_mean_hi, cv_recall_lifetime, cv_marketscan, cv_marketscan_all_2000)
predicted_lit <- as.data.frame(predicted_lit)

#Get model summaries for Marketscan crosswalks
predicted_market <- as.data.table(read.csv("FILEPATH"))
predicted_market_2000 <- as.data.table(read.csv("FILEPATH"))

setnames(predicted_market, "X_marketscan", "cv_marketscan")
setnames(predicted_market_2000, "X_marketscan_all_2000", "cv_marketscan_all_2000")

predicted_market <- predicted_market[, c("cv_marketscan", "Y_mean", "Y_mean_lo", "Y_mean_hi")]
predicted_market_2000 <- predicted_market_2000[, c("cv_marketscan_all_2000", "Y_mean", "Y_mean_lo", "Y_mean_hi")]

predicted_market$cv_marketscan_all_2000 <- 0
predicted_market$cv_recall_lifetime <- 0
predicted_market_2000$cv_marketscan <- 0
predicted_market_2000$cv_recall_lifetime <- 0

predicted_all <- rbind.fill(predicted_lit, predicted_market, predicted_market_2000)
predicted_all <- as.data.table(predicted_all)

predicted_all[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
pred1 <- predicted_all[1,]
pred2 <- predicted_all[2,]
pred3 <- predicted_all[3,]
pred1[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
pred1<- pred1[,Y_se_norm]
pred2[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
pred2<- pred2[,Y_se_norm]
pred3[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
pred3<- pred3[,Y_se_norm]
Y_se_norm <- c(pred1,pred2,pred3)

predicted_all <- cbind(predicted_all,Y_se_norm)

pred0 <- data.frame("cv_recall_lifetime" = 0, "cv_marketscan" =0, "cv_marketscan_all_2000"=0, "Y_mean"=0, "Y_mean_lo"=0, "Y_mean_hi"=0, "Y_se"=0, "Y_se_norm"=0)
predicted_all <- rbind.fill(predicted_all, pred0)
predicted_all<- as.data.table(predicted_all)

review_sheet_final <- merge(actual_data, predicted_all, by=c("cv_recall_lifetime", "cv_marketscan", "cv_marketscan_all_2000"))
review_sheet_final <-as.data.table(review_sheet_final)

setnames(review_sheet_final, "mean", "mean_orig")
review_sheet_final[, `:=` (log_mean = log(mean_orig), log_se = deltamethod(~log(x1), mean_orig, standard_error^2)), by = c("mean_orig", "standard_error")]
review_sheet_final[Y_mean != predicted_all[4,Y_mean], `:=` (log_mean = log_mean - Y_mean, log_se = sqrt(log_se^2 + Y_se^2))]
review_sheet_final[Y_mean != predicted_all[4,Y_mean], `:=` (mean_new = exp(log_mean), standard_error_new = deltamethod(~exp(x1), log_mean, log_se^2)), by = c("log_mean", "log_se")]
review_sheet_final[Y_mean == predicted_all[4,Y_mean], `:=` (mean_new = mean_orig, standard_error_new = standard_error)]
review_sheet_final[, `:=` (cases_new = NA, lower_new = NA, upper_new = NA)]
review_sheet_final[, (c("Y_mean", "Y_se", "log_mean", "log_se", "Y_se_norm")) := NULL]


# For upload validation #
setnames(review_sheet_final, "lower", "lower_orig")
setnames(review_sheet_final, "upper", "upper_orig")
setnames(review_sheet_final, "standard_error", "standard_error_orig")
setnames(review_sheet_final, "lower_new", "lower")
setnames(review_sheet_final, "upper_new", "upper")
setnames(review_sheet_final, "standard_error_new", "standard_error")
setnames(review_sheet_final, "mean_new", "mean")
review_sheet_final[is.na(lower), uncertainty_type_value := NA]


#THIS IS THE DATASET THAT WILL BE USED FOR NONFATAL MODELING
write.csv(review_sheet_final, paste0("FILEPATH", cause_path, cause_name, "xwalk.csv"), row.names = F)
