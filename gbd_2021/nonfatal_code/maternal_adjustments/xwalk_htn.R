library(msm, lib.loc="FILEPATH")
library(plyr)
library(gdata, lib.loc = "FILEPATH")
library(openxlsx)
source("FILEPATH/mr_brt_functions.R")
source("FILEPATH/plot_mr_brt_function.R")
source("FILEPATH/run_mr_brt_function.R")
source("FILEPATH/cov_info_function.R")
#You might need the function to transform standard error between linear and log. deltamethod() is from the package “msm”

# Great! Now we need these ratios in log space
#load the dataset in this code it is cv_preec
cv_preec<-read.csv("FILEPATH")
cv_preec<-subset(cv_preec, cv_preeclampsia_only.y==1)
cv_preec<-subset(cv_preec, ratio!=0)
cv_preec$log_ratio<-log(cv_preec$ratio)

# This is from example code
cv_preec$log_se <- sapply(1:nrow(cv_preec), function(i) {
  ratio_i <- cv_preec[i, "ratio"]
  ratio_se_i <- cv_preec[i, "std_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

#to run MR-BRT
fit4 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "pre_eclampsia_age_spline",
  data = cv_preec[cv_preec$log_se<20,], # You can also save this as a CSV and have this read it. for example: data = "FILEPATH"
  mean_var = "log_ratio",
  se_var = "log_se",
  covs = list(cov_info("age","X", degree = 2, i_knots = "20,30, 40, 50")),
  overwrite_previous = TRUE,
  study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.2
)
check_for_outputs(fit4) # This just checks if the outputs from the model exist
df_pred <- data.frame(intercept=1,age=seq(0, 54, 1)) # You need to create a prediction data frame, for covariates 

# Load predictions for preeclampsia
pred4 <- predict_mr_brt(fit4, newdata = df_pred) # Launches a job on the cluster to predict
check_for_preds(pred4)
pred_object <- load_mr_brt_preds(pred4)
preds <- pred_object$model_summaries

## Collect outputs
preec_outputs <- data.frame(log_preec_ratio = preds$Y_mean,
                               log_preec_se = (preds$Y_mean_hi - preds$Y_mean_lo)/2/qnorm(0.975))

#dose response
plot_mr_brt(fit4, dose_vars="age") #copy the command it produces in a qlogin session


preds <- pred_object$model_summaries

# Calculate log se
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
# Convert the mean and standard_error to linear space
# deltamethod only works this way if one row in 'preds'
preds$linear_se <- deltamethod(~exp(x1), preds$Y_mean, preds$se^2)
preds$ratio <- exp(preds$Y_mean)

#adjust hypertension data
#start with pre-eclampsia
#nids to exclude

match_htn<-read.csv("FILEPATH") #matched dataset to get nids that do not need to be adjusted
htn_nid<-unique(match_htn$nid)
drop_nid<-unique(htn$nid[htn$cv_chronichtn_incl==1]) #these do not have matches so cannot be adjusted

# Convert the mean and standard_error to linear space
# deltamethod only works this way if one row in 'preds'
# Convert the mean and standard_error to linear space
preds$linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i)
})
preds$ratio <- exp(preds$Y_mean)
preds<-plyr::rename(preds, c("X_age"="age"))

htn<-read.xlsx("FILEPATH") #bring in the htn data that will need to be adjusted
htn<-merge(htn, preds[,c("age","ratio","linear_se")], by="age")

htn$mrbrt_ratio_preec <- htn$ratio
htn$mrbrt_se_preec <- htn$linear_se

#drop ratio and linear_se so that can merge for pih adjustment 
htn$ratio<-NULL
htn$linear_se<-NULL

#save the old mean to check 
# Convert the mean by the crosswalk!
htn$mean_original<-htn$mean
htn$mean <- ifelse(htn$ cv_preeclampsia_only==1, htn$mean / htn$mrbrt_ratio_preec, htn$mean)
# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2
#same as example code! 
std_xwalk_preec <- sqrt(with(htn, standard_error^2 * mrbrt_se_preec^2 + standard_error^2*mrbrt_ratio_preec^2 + mrbrt_se_preec^2*mean^2))
# Now do ifelse statement in df
htn$standard_error <- ifelse(htn$ cv_preeclampsia_only==1, std_xwalk_preec, htn$standard_error)


###########run mr-brt for pregnancy induded hypertension (pih)
cv_pih<-read.csv("FILEPATH")
cv_pih<-subset(cv_pih, cv_pih_only.y==1)
cv_pih<-subset(cv_pih, ratio!=0)
cv_pih$log_ratio<-log(cv_pih$ratio)

# This is from example code
cv_pih$log_se <- sapply(1:nrow(cv_pih), function(i) {
  ratio_i <- cv_pih[i, "ratio"]
  ratio_se_i <- cv_pih[i, "std_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


#to run MR-BRT
fit6 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "pih_age_spline",
  data = cv_pih, 
  mean_var = "log_ratio",
  se_var = "log_se",
  covs = list(cov_info("age","X", degree = 2, n_i_knots = 4)),
  overwrite_previous = TRUE,
  study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.3
)
check_for_outputs(fit6) # This just checks if the outputs from the model exist
df_pred <- data.frame(intercept=1,age=seq(0, 54, 1)) # You need to create a prediction data frame, for covariates 

# Load predictions for pih
pred5 <- predict_mr_brt(fit6, newdata = df_pred) # Launches a job on the cluster to predict
check_for_preds(pred5)
pred_object <- load_mr_brt_preds(pred5)
preds <- pred_object$model_summaries

## Collect outputs
pih_outputs <- data.frame(log_pih_ratio = preds_pih$Y_mean,
                            log_pih_se = (preds_pih$Y_mean_hi - preds_pih$Y_mean_lo)/2/qnorm(0.975))

#dose response!
plot_mr_brt(fit6, dose_vars="age") #copy the command it produces in a qlogin session


preds <- pred_object$model_summaries

# Calculate log se
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
# Convert the mean and standard_error to linear space
# deltamethod only works this way if one row in 'preds'

#adjust for pih

preds$linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i)
})
preds$ratio <- exp(preds$Y_mean)
preds<-plyr::rename(preds, c("X_age"="age"))
htn<-merge(htn, preds[,c("age","ratio","linear_se")], by="age")

htn$mrbrt_ratio_pih <- htn$ratio
htn$mrbrt_se_pih <- htn$linear_se

#save the old mean to check 
# Convert the mean by the crosswalk!
htn$mean <- ifelse(htn$cv_pih_only==1, htn$mean / htn$mrbrt_ratio_pih, htn$mean)
# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2
#same as example code! 
std_xwalk_pih <- sqrt(with(htn, standard_error^2 * mrbrt_se_pih^2 + standard_error^2*mrbrt_ratio_pih^2 + mrbrt_se_pih^2*mean^2))
# Now do ifelse statement in df

htn$standard_error <- ifelse(htn$cv_pih_only==1, std_xwalk_pih, htn$standard_error)

htn_xwalked<-subset(htn, is_reference==0)
htn_xwalked<-subset(htn_xwalked, !(is_reference==0 & (nid%in%htn_nid))) 
htn_xwalked<-subset(htn_xwalked, !(is_reference==0 & (nid%in%drop_nid)))
htn_xwalked$lower<-""
htn_xwalked$upper<-""
#ready for upload!
write.xlsx(htn_xwalked, file = "FALEPATH")

