library(msm, lib.loc="FILEPATH")
library(plyr)
library(gdata, lib.loc = "FILEPATH")
library(openxlsx)
source("FILEPATH/mr_brt_functions.R")
source("FILEPATH/plot_mr_brt_function.R")
source("FILEPATH/run_mr_brt_function.R")
source("/FILEPATH/cov_info_function.R")
#load the dataset in this code it is other_inf_claims
other_inf_claims<-read.csv("FILEPATH")

other_inf_claims<-subset(other_inf_claims, is_reference.y==0)

other_inf_claims$log_ratio<-log(other_inf_claims$ratio)

# This is from example code
other_inf_claims$log_se <- sapply(1:nrow(other_inf_claims), function(i) {
  ratio_i <- other_inf_claims[i, "ratio"]
  ratio_se_i <- other_inf_claims[i, "std_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

fit9 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "other_inf_claims_trim_0.2_age_spline",
  data = other_inf_claims, # You can also save this as a CSV and have this read it. for example: data = "FILEPATH"
  mean_var = "log_ratio",
  se_var = "log_se",
  covs = list(cov_info("age","X", degree = 2, i_knots = "20,30, 40, 45")),
  overwrite_previous = TRUE,
  #study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.2
)

check_for_outputs(fit9) # This just checks if the outputs from the model exist
df_pred <- data.frame(intercept=1 ,age=seq(10, 54, 1)) # You need to create a prediction data frame, for covariates 

# Load predictions for claims
pred9 <- predict_mr_brt(fit9, newdata = df_pred) # Launches a job on the cluster to predict
check_for_preds(pred9)
pred_object <- load_mr_brt_preds(pred9)
preds <- pred_object$model_summaries

preds$age <- preds$X_age
plot_mr_brt(fit9, dose_vars="age")


###adjust other infection claims 

preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975) #in example code this is beta0_se_tau
preds$linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i)
})
preds$ratio <- exp(preds$Y_mean) #this is the exponentiated version of beta0 in example code


other_inf<-get_bundle_version(bundle_version_id = 8000)
other_inf$age<-(other_inf$age_start+other_inf$age_end)/2
other_inf_to_adjust<-subset(other_inf, other_inf$clinical_data_type=="claims, inpatient only")
other_inf_to_adjust<-merge(other_inf_to_adjust, preds[,c("age","ratio","linear_se")], by="age" )
other_inf_to_adjust$mrbrt_ratio_claims <- other_inf_to_adjust$ratio
other_inf_to_adjust$mrbrt_se_claims <- other_inf_to_adjust$linear_se
#save the old mean to check 
# Convert the mean by the crosswalk!
other_inf_to_adjust$mean_original<-other_inf_to_adjust$mean
other_inf_to_adjust$mean <- other_inf_to_adjust$mean / other_inf_to_adjust$mrbrt_ratio_claims

#same as example code! 
std_xwalk_claims <- sqrt(with(other_inf_to_adjust, standard_error^2 * mrbrt_se_claims^2 + standard_error^2*mrbrt_ratio_claims^2 + mrbrt_se_claims^2*mean^2))
# Now do ifelse statement in df
other_inf_to_adjust$standard_error <- std_xwalk_claims
#saudi arabia issue
other_inf_xwalked<-subset(other_inf_to_adjust, location_id!=152)

other_inf_xwalked$lower<-""
other_inf_xwalked$upper<-""
other_inf_xwalked$crosswalk_parent_seq<-other_inf_xwalked$seq
other_inf_xwalked$seq<-""
write.xlsx(other_inf_xwalked, file = "FILEPATH", sheetName="extraction")



