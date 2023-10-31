library(msm, lib.loc="FILEPATH")
library(plyr)
library(gdata, lib.loc = "FILEPATH")
library(openxlsx)
source("FILEPATH/mr_brt_functions.R")
source("FILEPATH/plot_mr_brt_function.R")
source("FILEPATH/run_mr_brt_function.R")
source("FILEPATH/-get_bundle_version.R")

#You might need the function to transform standard error between linear and log. deltamethod() is from the package “msm”

#load the dataset in this code it is sev_pre_claims
sev_pre_claims<-read.csv("FILEPATH")
sev_pre_claims<-subset(sev_pre_claims, is_reference.y==0)

sev_pre_claims$log_ratio<-log(sev_pre_claims$ratio)

# This is from example code
sev_pre_claims$log_se <- sapply(1:nrow(sev_pre_claims), function(i) {
  ratio_i <- sev_pre_claims[i, "ratio"]
  ratio_se_i <- sev_pre_claims[i, "std_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


fit10 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "sev_pre_claims_age_0.1_trim_spline_4_test",
  data = sev_pre_claims[sev_pre_claims$log_se<20,], # You can also save this as a CSV and have this read it. for example: data = "FILEPATH"
  mean_var = "log_ratio",
  se_var = "log_se",
  covs = list(cov_info("age","X", degree = 2, i_knots = "20,30, 40, 50")),
  overwrite_previous = TRUE,
  #study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.1
)

check_for_outputs(fit10) # This just checks if the outputs from the model exist
df_pred <- data.frame(intercept=1 ,age=seq(10, 54, 1)) # You need to create a prediction data frame, for covariates 

# Load predictions for chest symptoms
pred10 <- predict_mr_brt(fit10, newdata = df_pred) # Launches a job on the cluster to predict
check_for_preds(pred10)
pred_object <- load_mr_brt_preds(pred10)
preds <- pred_object$model_summaries

preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975) #in example code this is beta0_se_tau
preds$linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i)
})
preds$ratio <- exp(preds$Y_mean) #this is the exponentiated version of beta0 in example code


preds$age <- preds$X_age

plot_mr_brt(fit10, dose_vars = "age")


######ADJUST THE DATA AND UPLOAD! 
sev_pre<-get_bundle_version(bundle_version_id = 8972)
sev_pre$age<-(sev_pre$age_start+sev_pre$age_end)/2
sev_pre_to_adjust<-subset(sev_pre, clinical_data_type=="claims, inpatient only")
sev_pre_to_adjust<-merge(sev_pre_to_adjust, preds[,c("age","ratio","linear_se")], by="age")
sev_pre_to_adjust$mrbrt_ratio_claims <- sev_pre_to_adjust$ratio
sev_pre_to_adjust$mrbrt_se_claims <- sev_pre_to_adjust$linear_se

sev_pre_to_adjust$ratio<-NULL
sev_pre_to_adjust$linear_se<-NULL
#save the old mean to check 
# Convert the mean by the crosswalk!
sev_pre_to_adjust$mean_original<-sev_pre_to_adjust$mean
sev_pre_to_adjust$mean <- sev_pre_to_adjust$mean / sev_pre_to_adjust$mrbrt_ratio_claims
# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2
#same as example code! 
std_xwalk_claims <- sqrt(with(sev_pre_to_adjust, standard_error^2 * mrbrt_se_claims^2 + standard_error^2*mrbrt_ratio_claims^2 + mrbrt_se_claims^2*mean^2))
# Now do ifelse statement in df
sev_pre_to_adjust$standard_error <- std_xwalk_claims
sev_pre_xwalked<-sev_pre_to_adjust
sev_pre_xwalked$lower<-""
sev_pre_xwalked$upper<-""
sev_pre_xwalked$crosswalk_parent_seq<-sev_pre_xwalked$seq
sev_pre_xwalked$seq<-""
write.xlsx(sev_pre_xwalked, file = "FILEPATH", sheetName="extraction")


