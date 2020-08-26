library(msm, lib.loc="FILEPATH")
library(plyr)
library(gdata, lib.loc = "FILEPATH")
library(openxlsx)
source("FILEPATH/mr_brt_functions.R")
source("FILEPATH/plot_mr_brt_function.R")
source("FILEPATH/run_mr_brt_function.R")
source("FILEPATH/cov_info_function.R")


#load the dataset in this code it is sepsis_claims
sepsis_claims<-read.csv("FILEPATH")
sepsis_claims<-subset(sepsis_claims, is_reference.y==0)

sepsis_claims$log_ratio<-log(sepsis_claims$ratio)

sepsis_claims$log_se <- sapply(1:nrow(sepsis_claims), function(i) {
  ratio_i <- sepsis_claims[i, "ratio"]
  ratio_se_i <- sepsis_claims[i, "std_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


fit11 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "sepsis_claims_age_0.1_trim_spline_4_nl_tails",
  data = sepsis_claims[sepsis_claims$log_se<20,], # You can also save this as a CSV and have this read it. for example: data = "FILEPATH"
  mean_var = "log_ratio",
  se_var = "log_se",
  covs = list(cov_info("age","X", degree = 2, i_knots = "20,30, 40, 50",
                       bspline_gprior_mean = "0, 0, 0, 0, 0",
                       bspline_gprior_var = "inf, inf, inf, inf, inf", 
                       r_linear=F, l_linear=F)),
  overwrite_previous = TRUE,
  #study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.1
)

check_for_outputs(fit11) # This just checks if the outputs from the model exist
df_pred <- data.frame(intercept=1 ,age=seq(10, 54, 1)) # You need to create a prediction data frame, for covariates 

# Load predictions for claims
pred11 <- predict_mr_brt(fit11, newdata = df_pred) # Launches a job on the cluster to predict
check_for_preds(pred11)
pred_object <- load_mr_brt_preds(pred11)
preds <- pred_object$model_summaries

preds$age <- preds$X_age

#dose response
plot_mr_brt(fit11, dose_vars = "age")


#start adjusting data
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975) #in example code this is beta0_se_tau
preds$linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i)
})
preds$ratio <- exp(preds$Y_mean) #this is the exponentiated version of beta0 in example code

sepsis_clinical<-get_bundle_version(bundle_version_id=15188, export=TRUE)
sepsis_to_adjust<-subset(sepsis_clinical, sepsis_clinical$clinical_data_type=="claims, inpatient only")
sepsis_to_adjust<-merge(sepsis_to_adjust, preds[,c("age","ratio","linear_se")], by="age")
sepsis_to_adjust$mrbrt_ratio_claims <- sepsis_to_adjust$ratio
sepsis_to_adjust$mrbrt_se_claims <- sepsis_to_adjust$linear_se
#drop ratio and linear_se so that can merge for clinical adjustment 
sepsis_to_adjust$ratio<-NULL
sepsis_to_adjust$linear_se<-NULL
#save the old mean to check 
# Convert the mean by the crosswalk!
sepsis_to_adjust$mean_original<-sepsis_to_adjust$mean
sepsis_to_adjust$mean <- sepsis_to_adjust$mean / sepsis_to_adjust$mrbrt_ratio_claims
# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2
#same as example code! 
std_xwalk_claims <- sqrt(with(sepsis_to_adjust, standard_error^2 * mrbrt_se_claims^2 + standard_error^2*mrbrt_ratio_claims^2 + mrbrt_se_claims^2*mean^2))
# Now do ifelse statement in df
sepsis_to_adjust$standard_error <- std_xwalk_claims
write.csv(sepsis_to_adjust, file ="FILEPATH")


######Run second step MR-BRT! 
sepsis_lit_clinical<-read.csv("FILEPATH")
sepsis_lit_clinical<-subset(sepsis_lit_clinical, is_reference.y==0)

sepsis_lit_clinical$log_ratio<-log(sepsis_lit_clinical$ratio)

# This is from example code
sepsis_lit_clinical$log_se <- sapply(1:nrow(sepsis_lit_clinical), function(i) {
  ratio_i <- sepsis_lit_clinical[i, "ratio"]
  ratio_se_i <- sepsis_lit_clinical[i, "std_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


fit12 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "sepsis_lit_clinical_age_0.1_trim_spline_4",
  data = sepsis_lit_clinical, # You can also save this as a CSV and have this read it. for example: data = "FILEPATH"
  mean_var = "log_ratio",
  se_var = "log_se",
  covs = list(cov_info("age","X", degree = 2, i_knots = "20,30, 40, 50")),
  overwrite_previous = TRUE,
  #study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.1
)

check_for_outputs(fit12) # This just checks if the outputs from the model exist
df_pred <- data.frame(intercept=1 ,age=seq(10, 54, 1)) # You need to create a prediction data frame, for covariates 

# Load predictions for clinical
pred12 <- predict_mr_brt(fit12, newdata = df_pred) # Launches a job on the cluster to predict
check_for_preds(pred12)
pred_object <- load_mr_brt_preds(pred12)
preds <- pred_object$model_summaries

preds$age <- preds$X_age


plot_mr_brt(fit12, dose_vars = "age")

#################adjust data 

preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975) #this is beta0_se_tau
preds$linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i)
})
preds$ratio <- exp(preds$Y_mean) #this is the exponentiated version of beta0 

sepsis_inpt<-subset(sepsis_clinical, sepsis_clinical$clinical_data_type=="inpatient")
sepsis_clinical_to_adjust<-rbind.fill(sepsis_to_adjust, sepsis_inpt)

sepsis_clinical_to_adjust<-merge(sepsis_clinical_to_adjust, preds[,c("age","ratio","linear_se")], by="age")
sepsis_clinical_to_adjust$mrbrt_ratio_clinical <- sepsis_clinical_to_adjust$ratio
sepsis_clinical_to_adjust$mrbrt_se_clinical <- sepsis_clinical_to_adjust$linear_se
#save the old mean to check 
# Convert the mean by the crosswalk!
sepsis_clinical_to_adjust$mean_original<-sepsis_clinical_to_adjust$mean
sepsis_clinical_to_adjust$mean <- sepsis_clinical_to_adjust$mean / sepsis_clinical_to_adjust$mrbrt_ratio_clinical
# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2
#same as example code! 
std_xwalk_clinical <- sqrt(with(sepsis_clinical_to_adjust, standard_error^2 * mrbrt_se_clinical^2 + standard_error^2*mrbrt_ratio_clinical^2 + mrbrt_se_clinical^2*mean^2))
# Now do ifelse statement in df
sepsis_clinical_to_adjust$standard_error <- std_xwalk_clinical
#saudi arabia
sepsis_xwalked_clinical$lower<-""
sepsis_xwalked_clinical$upper<-""
write.csv(sepsis_xwalked_clinical, file ="FILEPATH")

