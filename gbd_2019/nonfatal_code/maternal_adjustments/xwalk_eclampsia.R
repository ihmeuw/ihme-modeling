library(msm, lib.loc="FILEPATH")
library(plyr)
library(gdata, lib.loc = "FILEPATH")
library(openxlsx)
source("FILEPATH/mr_brt_functions.R")
source("FILEPATH/plot_mr_brt_function.R")
source("FILEPATH/run_mr_brt_function.R")
source("FILEPATH/cov_info_function.R")
source("FILEPATH/get_bundle_version.R")
#You might need the function to transform standard error between linear and log. deltamethod() is from the package “msm”

# Great! Now we need these ratios in log space
#
#load the dataset in this code it is eclampsia claims 
eclampsia_claims<-read.csv("FILEPATH")
eclampsia_claims<-subset(eclampsia_claims, is_reference.y==0)

eclampsia_claims$log_ratio<-log(eclampsia_claims$ratio)

# This is from example code
eclampsia_claims$log_se <- sapply(1:nrow(eclampsia_claims), function(i) {
  ratio_i <- eclampsia_claims[i, "ratio"]
  ratio_se_i <- eclampsia_claims[i, "std_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


#to run MR-BRT
fit8 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "eclampsia_claims_to_inpt_age_0.3_trim_spline_4_test",
  data = eclampsia_claims[eclampsia_claims$log_se<20,], # You can also save this as a CSV and have this read it. for example: data = "FILEPATH"
  mean_var = "log_ratio",
  se_var = "log_se",
  covs = list(cov_info("age","X", degree = 2, i_knots = "20,30, 40, 45")),
  overwrite_previous = TRUE,
  #study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.3
)

check_for_outputs(fit8) # This just checks if the outputs from the model exist
df_pred <- data.frame(intercept=1 ,age=seq(10, 54, 1)) # You need to create a prediction data frame, for covariates 

# Load predictions for chest symptoms
pred8 <- predict_mr_brt(fit8, newdata = df_pred) # Launches a job on the cluster to predict
check_for_preds(pred8)
pred_object <- load_mr_brt_preds(pred8)
preds <- pred_object$model_summaries

preds$age <- preds$X_ageno 

#dose_response plot
plot_mr_brt(fit8, dose_vars="age") 



####adjust claims data! 
df_pred <- data.frame(intercept=1 ,age=seq(10, 54, 1)) # You need to create a prediction data frame, for covariates 

# Load predictions for claims
pred8 <- predict_mr_brt(fit8, newdata = df_pred) # Launches a job on the cluster to predict
check_for_preds(pred8)
pred_object <- load_mr_brt_preds(pred8)
preds <- pred_object$model_summaries
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975) #in example code this is beta0_se_tau
preds$linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i)
})
preds$ratio <- exp(preds$Y_mean) #this is the exponentiated version of beta0 in example code

preds$age <- preds$X_age
#download data to adjust from database
eclampsia<-get_bundle_version(bundle_version_id = 7982, export = TRUE)
eclampsia_to_adjust<-subset(eclampsia, clinical_data_type=="claims, inpatient only")
eclampsia_to_adjust$age<-(eclampsia_to_adjust$age_start+eclampsia_to_adjust$age_end)/2
eclampsia_to_adjust<-merge(eclampsia_to_adjust, preds[,c("age","ratio","linear_se")], by="age")
eclampsia_to_adjust$mrbrt_ratio_claims <- eclampsia_to_adjust$ratio
eclampsia_to_adjust$mrbrt_se_claims <- eclampsia_to_adjust$linear_se
#drop ratio and linear_se so that can merge for clinical adjustment 
eclampsia_to_adjust$ratio<-NULL
eclampsia_to_adjust$linear_se<-NULL
#save the old mean to check 
# Convert the mean by the crosswalk!
eclampsia_to_adjust$mean_original<-eclampsia_to_adjust$mean
eclampsia_to_adjust$mean <- eclampsia_to_adjust$mean / eclampsia_to_adjust$mrbrt_ratio_claims
# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2
#same as example code! 
std_xwalk_claims <- sqrt(with(eclampsia_to_adjust, standard_error^2 * mrbrt_se_claims^2 + standard_error^2*mrbrt_ratio_claims^2 + mrbrt_se_claims^2*mean^2))
# Now do ifelse statement in df
eclampsia_to_adjust$standard_error <- std_xwalk_claims
#saudi arabia
eclampsia_xwalked<-subset(eclampsia_to_adjust, location_id!=152)
eclampsia_xwalked$lower<-""
eclampsia_xwalked$upper<-""
#eclampsia_xwalked<-subset(eclampsia_xwalked, mean!="")

write.xlsx(eclampsia_xwalked, file = "FILEPATH", sheetName="extraction")



##############RUN MR-BRT for CLINICAL TO LIT

eclampsia_hosp<-read.csv("FILEPATH")
#eclampsia_hosp<-subset(eclampsia_hosp, is_reference.y==0)

eclampsia_hosp$log_ratio<-log(eclampsia_hosp$ratio)

# This is from example code
eclampsia_hosp$log_se <- sapply(1:nrow(eclampsia_hosp), function(i) {
  ratio_i <- eclampsia_hosp[i, "ratio"]
  ratio_se_i <- eclampsia_hosp[i, "std_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


fit13 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "eclampsia_clinical_to_lit_age_0.3_trim_spline_4_test",
  data = eclampsia_hosp[eclampsia_hosp$log_se<20,], # You can also save this as a CSV and have this read it. for example: data = "FILEPATH"
  mean_var = "log_ratio",
  se_var = "log_se",
  covs = list(cov_info("age","X", degree = 2, i_knots = "20,30, 40, 45")),
  overwrite_previous = TRUE,
  #study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.3
)



check_for_outputs(fit13) # This just checks if the outputs from the model exist
df_pred <- data.frame(intercept=1 ,age=seq(10, 54, 1)) # You need to create a prediction data frame, for covariates 

# Load predictions for clinical
pred13 <- predict_mr_brt(fit13, newdata = df_pred) # Launches a job on the cluster to predict
check_for_preds(pred13)
pred_object <- load_mr_brt_preds(pred13)
preds <- pred_object$model_summaries

preds$age <- preds$X_age

#dose response plot!
plot_mr_brt(fit13, dose_vars="age") 



# Adjust clinical data
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975) #in example code this is beta0_se_tau
preds$linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i)
})
preds$ratio <- exp(preds$Y_mean) #this is the exponentiated version of beta0 in example code

preds$age <- preds$X_age
eclampsia_xw_claims<-read.xlsx("FILEPATH")
eclampsia_inpt<-get_bundle_version(bundle_version_id = 7982)
eclampsia_inpt<-subset(eclampsia_clinical, clinical_data_type=="inpatient")
eclampsia_inpt$age<-(eclampsia_inpt$age_start+eclampsia_inpt$age_end)/2
eclampsia_clinical_to_adjust<-rbind.fill(eclampsia_xw_claims, eclampsia_inpt)
eclampsia_clinical_to_adjust$age<-(eclampsia_clinical_to_adjust$age_start+eclampsia_clinical_to_adjust$age_end)/2
eclampsia_clinical_to_adjust<-merge(eclampsia_clinical_to_adjust, preds[,c("age","ratio","linear_se")], by="age")
eclampsia_clinical_to_adjust$mrbrt_ratio_clinical <- eclampsia_clinical_to_adjust$ratio
eclampsia_clinical_to_adjust$mrbrt_se_clinical <- eclampsia_clinical_to_adjust$linear_se
#save the old mean to check 
eclampsia_clinical_to_adjust$mean_original_clinical<-eclampsia_clinical_to_adjust$mean
# Convert the mean by the crosswalk
#eclampsia_clinical_to_adjust$mean_original<-eclampsia_clinical_to_adjust$mean
eclampsia_clinical_to_adjust$mean <- eclampsia_clinical_to_adjust$mean / eclampsia_clinical_to_adjust$mrbrt_ratio_clinical
# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2

std_xwalk_clinical <- sqrt(with(eclampsia_clinical_to_adjust, standard_error^2 * mrbrt_se_clinical^2 + standard_error^2*mrbrt_ratio_clinical^2 + mrbrt_se_clinical^2*mean^2))
# Now do ifelse statement in df
eclampsia_clinical_to_adjust$standard_error <- std_xwalk_clinical
eclampsia_clinical_xwalked$lower<-""
eclampsia_clinical_xwalked$upper<-""
#ready for upload!
write.xlsx(eclampsia_clinical_xwalked, file = "FILEPATH", sheetName="extraction")





