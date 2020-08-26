library(msm, lib.loc="FILEPATH")
library(plyr)
library(gdata, lib.loc = "FILEPATH")
library(openxlsx)
source("FILEPATH/mr_brt_functions.R")
source("FILEPATH/plot_mr_brt_function.R")
source("FILEPATH/run_mr_brt_function.R")
source("FILEPATH/cov_info_function.R")
#You might need the function to transform standard error between linear and log. deltamethod() is from the package “msm”

#load the dataset in this code it is ectopic_inpt
ectopic_inpt<-read.csv("FILEPATH")
ectopic_inpt<-subset(ectopic_inpt, is_reference.y==0)
ectopic_inpt<-subset(ectopic_inpt, ratio!=0)

ectopic_inpt$log_ratio<-log(ectopic_inpt$ratio)

# This is from example code
ectopic_inpt$log_se <- sapply(1:nrow(ectopic_inpt), function(i) {
  ratio_i <- ectopic_inpt[i, "ratio"]
  ratio_se_i <- ectopic_inpt[i, "std_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


fit14 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "ectopic_inpt_age_0.1_trim_spline_4_test",
  data = ectopic_inpt[ectopic_inpt$log_se<20,], # You can also save this as a CSV and have this read it. for example: data = "FILEPATH"
  mean_var = "log_ratio",
  se_var = "log_se",
  covs = list(cov_info("age","X", degree = 2, i_knots = "20,30, 40, 50")),
  overwrite_previous = TRUE,
  #study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.1
)

check_for_outputs(fit14) # This just checks if the outputs from the model exist
df_pred <- data.frame(intercept=1 ,age=seq(10, 54, 1)) # You need to create a prediction data frame, for covariates 

# Load predictions for inpt
pred14 <- predict_mr_brt(fit14, newdata = df_pred) # Launches a job on the cluster to predict
check_for_preds(pred14)
pred_object <- load_mr_brt_preds(pred14)
preds <- pred_object$model_summaries

preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975) #in example code this is beta0_se_tau
preds$linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i)
})
preds$ratio <- exp(preds$Y_mean) #this is the exponentiated version of beta0 in example code


preds$age <- preds$X_age

plot_mr_brt(fit14, dose_vars = "age")


########adjust inpatient data 
ectopic<-get_bundle_version(bundle_version_id=15200, export=TRUE)
ectopic_to_adjust<-subset(ectopic, clinical_data_type=="inpatient")
ectopic_to_adjust<-merge(ectopic_to_adjust, preds[,c("age","ratio","linear_se")], by="age")
ectopic_to_adjust$mrbrt_ratio_inpatient <- ectopic_to_adjust$ratio
ectopic_to_adjust$mrbrt_se_inpatient <- ectopic_to_adjust$linear_se
#save the old mean to check 
# Convert the mean by the crosswalk!
ectopic_to_adjust$mean_original<-ectopic_to_adjust$mean
ectopic_to_adjust$mean <- ectopic_to_adjust$mean / ectopic_to_adjust$mrbrt_ratio_inpatient
# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2
#same as source code! 
std_xwalk_inpatient <- sqrt(with(ectopic_to_adjust, standard_error^2 * mrbrt_se_inpatient^2 + standard_error^2*mrbrt_ratio_inpatient^2 + mrbrt_se_inpatient^2*mean^2))
ectopic_to_adjust$standard_error <- std_xwalk_inpatient
ectopic_xwalked_inpatient<-ectopic_to_adjust
ectopic_xwalked_inpatient$lower<-""
ectopic_xwalked_inpatient$upper<-""
write.csv(ectopic_xwalked_inpatient, file ="FILEPATH")


