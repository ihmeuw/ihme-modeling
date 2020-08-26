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

#load the dataset in this code it is abort_surv
abort_surv<-read.csv("FILEPATH")
abort_surv$cv_claims<-ifelse(abort_surv$clinical_data_type=="claims", 1, 0)
abort_surv$cv_usa_inpt<-ifelse(abort_surv$location_id>100 & abort_surv$clinical_data_type=="inpatient", 1, 0)


abort_surv$log_ratio<-log(abort_surv$ratio)


abort_surv$log_se <- sapply(1:nrow(abort_surv), function(i) {
  ratio_i <- abort_surv[i, "ratio"]
  ratio_se_i <- abort_surv[i, "std_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

#create covariates to tag differnt types of examples
covs <- c("age", "cv_claims", "cv_usa_inpt")

cov_list <- lapply(covs, function(x) cov_info(x, "X"))
for (j in 1:length(cov_list)){
  if (cov_list[[j]]$covariate == "age"){
    cov_list[[j]]$degree <- 2
    cov_list[[j]]$i_knots <- "20, 30, 40, 45"
    cov_list[[j]]$l_linear <- T
    cov_list[[j]]$r_linear <- T
    #cov_list[[j]]$knot_placement_procedure <- "frequency"
    #cov_list[[j]]$bspline_mono <- "increasing" # should have this for most causes - ?
  }
}

#to run MR-BRT
fit12 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "abort_surveillance_age_spline_resub_usa_dummies_fix",
  data = abort_surv[abort_surv$log_se<20,], # You can also save this as a CSV and have this read it. for example: data = "FILEPATH"
  mean_var = "log_ratio",
  se_var = "log_se",
  covs = cov_list,
  overwrite_previous = TRUE,
  #study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.3
)
check_for_outputs(fit11) # This just checks if the outputs from the model exist
#dose response plot!
plot_mr_brt(fit12, dose_vars="age")


# Load predictions and create a more complicated predict dataset
pred_claims<-expand.grid(intercept=1, age=seq(10, 54, 1),  cv_claims= c(0,1))
pred_inpt_usa<-expand.grid(intercept=1, age=seq(10, 54, 1),  cv_usa_inpt= c(1))
df_pred<-rbind.fill(pred_claims, pred_inpt_usa)
df_pred$cv_claims<-ifelse(is.na(df_pred$cv_claims), 0, df_pred$cv_claims)
df_pred$cv_usa_inpt<-ifelse(is.na(df_pred$cv_usa_inpt), 0, df_pred$cv_usa_inpt)
pred11 <- predict_mr_brt(fit11, newdata = df_pred) # Launches a job on the cluster to predict
check_for_preds(pred11)
pred_object <- load_mr_brt_preds(pred11)
preds <- pred_object$model_summaries

preds$age <- preds$X_age

## Collect outputs
abort_outputs <- data.frame(log_abort_ratio = preds_abort$Y_mean,
                              log_abort_se = (preds_abort$Y_mean_hi - preds_abort$Y_mean_lo)/2/qnorm(0.975))


preds <- pred_object$model_summaries


###adjust abort data that are not surveillance by age
# Calculate log se
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975) #in example code this is beta0_se_tau
# Convert the mean and standard_error to linear space
# deltamethod only works this way if one row in 'preds'

preds$linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i)
})
preds$ratio <- exp(preds$Y_mean) #this is the exponentiated version of beta0 in example code

preds$age <- preds$X_age
preds$cv_claims<-preds$X_cv_claims
preds$cv_usa_inpt<-preds$X_cv_usa_inpt

######inpatient all#########

abort_resub_clinical<-get_bundle_version(bundle_version_id=19409, export=TRUE)
abort_to_adjust_inp<-subset(abort_resub_clinical, abort_resub_clinical$clinical_data_type=="inpatient")
usa_locs<-c(102, 523:573)
abort_to_adjust_inp$cv_usa_inpt<-ifelse(abort_to_adjust_inp$location_id%in%usa_locs, 1, 0)
abort_to_adjust_inp$age<-(abort_to_adjust_inp$age_start+abort_to_adjust_inp$age_end)/2

preds_inp<-subset(preds, preds$cv_claims==0)
abort_to_adjust_inp<-merge(abort_to_adjust_inp, preds_inp[,c("age","ratio","linear_se", "cv_usa_inpt")], by=c("age", "cv_usa_inpt"))

abort_to_adjust_inp$mrbrt_ratio_clinical <- abort_to_adjust_inp$ratio
abort_to_adjust_inp$mrbrt_se_clinical <- abort_to_adjust_inp$linear_se
#drop ratio and linear_se so that can merge for claims adjustment 
abort_to_adjust_inp$ratio<-NULL
abort_to_adjust_inp$linear_se<-NULL
#save the old mean to check 
# Convert the mean by the crosswalk!
abort_to_adjust_inp$mean_original<-abort_to_adjust_inp$mean
abort_to_adjust_inp$mean <-  abort_to_adjust_inp$mean / abort_to_adjust_inp$mrbrt_ratio_clinical
# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2
#same as example code! 
std_xwalk_clinical <- sqrt(with(abort_to_adjust_inp, standard_error^2 * mrbrt_se_clinical^2 + standard_error^2*mrbrt_ratio_clinical^2 + mrbrt_se_clinical^2*mean^2))
# Now do ifelse statement in df
abort_to_adjust_inp$standard_error <- std_xwalk_clinical
abort_xwalked_inp<-abort_to_adjust_inp
abort_xwalked_inp$lower<-""
abort_xwalked_inp$upper<-""

write.xlsx(abort_xwalked_inp, file = "FILEPATH", sheetName="extraction")

####claims############
abort_to_adjust_claims<-subset(abort_resub_clinical, abort_resub_clinical$clinical_data_type=="claims, inpatient only")
abort_to_adjust_claims$cv_claims<-1
abort_to_adjust_claims$age<-(abort_to_adjust_claims$age_start+abort_to_adjust_claims$age_end)/2

preds_claims<-subset(preds, preds$cv_claims==1)
abort_to_adjust_claims<-merge(abort_to_adjust_claims, preds_claims[,c("age","ratio","linear_se", "cv_claims")], by=c("age", "cv_claims"))

abort_to_adjust_claims$mrbrt_ratio_clinical <- abort_to_adjust_claims$ratio
abort_to_adjust_claims$mrbrt_se_clinical <- abort_to_adjust_claims$linear_se
#save the old mean to check 
# Convert the mean by the crosswalk!
abort_to_adjust_claims$mean_original<-abort_to_adjust_claims$mean
abort_to_adjust_claims$mean <-  abort_to_adjust_claims$mean / abort_to_adjust_claims$mrbrt_ratio_clinical
# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2
#same as example code! 
std_xwalk_clinical <- sqrt(with(abort_to_adjust_claims, standard_error^2 * mrbrt_se_clinical^2 + standard_error^2*mrbrt_ratio_clinical^2 + mrbrt_se_clinical^2*mean^2))

abort_to_adjust_claims$standard_error <- std_xwalk_clinical
abort_xwalked_claims<abort_to_adjust_claims
abort_xwalked_claims$lower<-""
abort_xwalked_claims$upper<-""

write.xlsx(abort_xwalked_claims, file = "FILEPATH", sheetName="extraction")




