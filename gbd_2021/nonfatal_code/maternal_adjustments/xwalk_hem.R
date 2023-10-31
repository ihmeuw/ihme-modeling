library(msm, lib.loc="FILEPATH")
library(plyr)
library(gdata, lib.loc = "FILEPATH")
library(openxlsx)
library(ggplot2)
source("FILEPATH/mr_brt_functions.R")
source("FILEPATH/plot_mr_brt_function.R")
source("FILEPATH/run_mr_brt_function.R")
source("FILEPATH/cov_info_function.R")
#You might need the function to transform standard error between linear and log. deltamethod() is from the package “msm”

####run mr-brt for postpartum hemorrhage
# Great! Now we need these ratios in log space
#bring in the dataset with the matches to run mr-brt on the ratios
#start with postpartum hemorrhage

cv_posthem<-read.csv("FILEPATH")
cv_posthem<-subset(cv_posthem, cv_postparthhemonly.y==1)

cv_posthem$log_ratio<-log(cv_posthem$ratio)

# Use delta method to log standard error 
cv_posthem$log_se <- sapply(1:nrow(cv_posthem), function(i) {
  ratio_i <- cv_posthem[i, "ratio"]
  ratio_se_i <- cv_posthem[i, "std_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

#to run MR-BRT
fit2 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "post_partum_hem_age_spline_4_step4",
  data = cv_posthem[cv_posthem$log_se<20,], # You can also save this as a CSV and have this read it. for example: data = "FILEPATH"
  mean_var = "log_ratio",
  se_var = "log_se",
  covs = list(cov_info("age","X",
                       degree = 2, i_knots = "20, 30, 40, 50",
                       bspline_gprior_mean = "0, 0, 0, 0, 0",
                       bspline_gprior_var = "inf, inf, inf, inf, inf")),
  overwrite_previous = TRUE,
  study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.2
)
check_for_outputs(fit2) # This just checks if the outputs from the model exist
df_pred <- data.frame(intercept=1, age=seq(0, 54, 1)) # You need to create a prediction data frame, for covariates 

# Load predictions for pph
pred2 <- predict_mr_brt(fit2, newdata = df_pred) # Launches a job on the cluster to predict
check_for_preds(pred2)
pred_object <- load_mr_brt_preds(pred2)
preds <- pred_object$model_summaries

## Collect outputs
posthem_outputs <- data.frame(log_posthem_ratio = preds_posthem$Y_mean,
                             log_posthem_se = (preds_posthem$Y_mean_hi - preds_posthem$Y_mean_lo)/2/qnorm(0.975))


#dose response plot
plot_mr_brt(fit2, dose_vars = "age") 


# Calculate log se
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
# Convert the mean and standard_error to linear space
# deltamethod only works this way if one row in 'preds'
preds$linear_se <- deltamethod(~exp(x1), preds$Y_mean, preds$se^2)
preds$ratio <- exp(preds$Y_mean)


#adjust hemorrhage data 
#nids to exclude
match_hem<-read.csv("FILEPATH") #bring in the matched data to get the nids that do not need adjustment
hem_nid<-unique(match_hem$nid)
hem<-read.xlsx("FILEPATH") #bring in the dataset to adjust


# Convert the mean and standard_error to linear space
preds$linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i)
})
preds$ratio <- exp(preds$Y_mean) #this is the exponentiated version of beta0 in example code
preds<-plyr::rename(preds, c("X_age"="age"))

hem_to_adjust<-merge(hem, preds[,c("age","ratio","linear_se")], by="age")

#rename ratio and linear_se so that can merge for aph adjustment later
hem_to_adjust<-plyr::rename(hem_to_adjust, c("ratio"="mrbrt_ratio_post", "linear_se"="mrbrt_se_post"))
#save the old mean to check 
hem_to_adjust$mean_original<-hem_to_adjust$mean

# Convert the mean by the crosswalk!
hem_to_adjust$mean <- ifelse(hem_to_adjust$cv_postparthhemonly==1, hem_to_adjust$mean / hem_to_adjust$mrbrt_ratio_post, hem_to_adjust$mean)

# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2
#same as example code! 
std_xwalk_post <- sqrt(with(hem_to_adjust, standard_error^2 * mrbrt_se_post^2 + standard_error^2*mrbrt_ratio_post^2 + mrbrt_se_post^2*mean^2))
# Now do ifelse statement in df
hem_to_adjust$standard_error <- ifelse(hem_to_adjust$cv_postparthhemonly==1, std_xwalk_post, hem_to_adjust$standard_error)

##################################################
##run mr-brt for antepartum hemorrhage
#load the dataset in this code it is cv_anterpart
cv_antepart<-read.csv("FILEPATH")
cv_antepart<-subset(cv_antepart, cv_anteparthemonly.y==1)
cv_antepart<-subset(cv_antepart, ratio!=0)
cv_antepart$log_ratio<-log(cv_antepart$ratio)

# Use delta method to log SE
cv_antepart$log_se <- sapply(1:nrow(cv_antepart), function(i) {
  ratio_i <- cv_antepart[i, "ratio"]
  ratio_se_i <- cv_antepart[i, "std_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


#to run MR-BRT
fit3 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "ante_partum_hem_age_spline_nl_tail",
  data = cv_antepart[cv_antepart$log_se<20,], # You can also save this as a CSV and have this read it. for example: data = "FILEPATH"
  mean_var = "log_ratio",
  se_var = "log_se",
  covs = list(cov_info("age","X", degree = 2, i_knots = "20, 30, 40, 50", 
                       bspline_gprior_mean = "0, 0, 0, 0, 0",
                       bspline_gprior_var = "inf, inf, inf, inf, inf", 
                       r_linear=F, l_linear=F)),
  overwrite_previous = TRUE,
  study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.1
)
check_for_outputs(fit3) # This just checks if the outputs from the model exist
df_pred <- data.frame(intercept=1,age=seq(0, 54, 1)) # You need to create a prediction data frame, for covariates 

# Load predictions for aph
pred3 <- predict_mr_brt(fit3, newdata = df_pred) # Launches a job on the cluster to predict
check_for_preds(pred3)
pred_object <- load_mr_brt_preds(pred3)
preds <- pred_object$model_summaries

## Collect outputs
antepart_outputs <- data.frame(log_antepart_ratio = preds_antepart$Y_mean,
                              log_antepart_se = (preds_antepart$Y_mean_hi - preds_antepart$Y_mean_lo)/2/qnorm(0.975))

#dose response plot!
plot_mr_brt(fit3, dose_vars = "age") #copy the command it produces in a qlogin session


preds <- pred_object$model_summaries

# Calculate log se
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)


#adjust anterpart hem

# Convert the mean and standard_error to linear space
preds$linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i)
})
preds$ratio <- exp(preds$Y_mean) #this is the exponentiated version of beta0 in example code
preds<-plyr::rename(preds, c("X_age"="age"))

#stat to adjust for aph
hem_to_adjust<-merge(hem_to_adjust, preds[,c("age","ratio","linear_se")], by="age")
hem_to_adjust$mrbrt_ratio_ante <- hem_to_adjust$ratio
hem_to_adjust$mrbrt_se_ante <- hem_to_adjust$linear_se
# Convert the mean by the crosswalk!
hem_to_adjust$mean <- ifelse(hem_to_adjust$cv_anteparthemonly==1, hem_to_adjust$mean / hem_to_adjust$mrbrt_ratio_ante, hem_to_adjust$mean)
# Calculate the standard error as the product of variances
# http://www.odelama.com/data-analysis/Commonly-Used-Math-Formulas/#spanidvarspanthevariance
# variance(X * Y) = varianceX * varianceY + varianceX * meanY^2 + varianceY * meanX^2
#same as example code! 
std_xwalk_ante <- sqrt(with(hem_to_adjust, standard_error^2 * mrbrt_se_ante^2 + standard_error^2*mrbrt_ratio_ante^2 + mrbrt_se_ante^2*mean^2))
# Now do ifelse statement in df
hem_to_adjust$standard_error <- ifelse(hem_to_adjust$cv_anteparthemonly==1, std_xwalk_ante, hem_to_adjust$standard_error)
hem_xwalked<-subset(hem_to_adjust, extractor=="USERNAME")
hem_xwalked<-subset(hem_xwalked, !(is_reference==0 & (nid%in%hem_nid)))
hem_xwalked$lower<-""
hem_xwalked$upper<-""
#data are ready to upload! 
write.xlsx(hem_xwalked, file = "FILEPATH")

