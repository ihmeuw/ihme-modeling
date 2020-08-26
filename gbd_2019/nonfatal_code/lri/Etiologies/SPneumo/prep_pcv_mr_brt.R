##############################################
# Prep files for MR-BRT on S pneumo study attributable fractions
#################################################################
source("filepath/mr_brt_functions.R")
source("filepath/plot_mr_brt_function.R")
source("filepath/run_mr_brt_function.R")

pcv_data <- read.csv("filepath")

pcv_data$age <- floor((pcv_data$age_end + pcv_data$age_start) / 2)
pcv_data <- subset(pcv_data, meas_stdev < 20)
mrbrt <- pcv_data[,c("age","mean_log_paf","std_log_paf","meas_value","meas_stdev","x_befaft")]
mrbrt$intercept <- 1
mrbrt$age_cat <- ifelse(mrbrt$age < 2, 1, ifelse(mrbrt$age < 5, 2, ifelse(mrbrt$age < 60, 3, 4)))
write.csv(mrbrt, "filepath", row.names=F)

fit1 <- run_mr_brt(
  output_dir = "filepath",
  model_label = "age_ve",
  data = mrbrt,
  mean_var = "mean_log_paf",
  se_var = "std_log_paf",
  covs = list(cov_info("age","X",
                       degree = 2, n_i_knots=3,
                       l_linear=TRUE, r_linear = TRUE, bspline_gprior_mean = "0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf")),
  overwrite_previous = TRUE,
 # study_id = "match_id",
  method = "trim_maxL",
  trim_pct = 0.1
)

df_pred <- data.frame(intercept=1, age=seq(0,100,1))
pred1 <- predict_mr_brt(fit1, newdata = df_pred)

pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries
# preds$age_mid <- preds$X_age_mid
preds$sdi <- preds$X_age

## Plot the fit
mod_data <- fit1$train_data
mod_data$outlier <- floor(abs(mod_data$w - 1))
ggplot(preds, aes(x=X_age, y=(Y_mean))) + geom_line() +
  geom_ribbon(aes(ymin=(Y_mean_lo), ymax=(Y_mean_hi)), alpha=0.3) +
  geom_point(data=mod_data, aes(x=age, y=mean_log_paf, size=1/std_log_paf^2, col=factor(outlier))) + 
  guides(size=F) + scale_color_manual(values=c("black","red")) + guides(col=F) +
  geom_hline(yintercept=0, lty=2, col="red") + scale_y_continuous("Log ratio") + 
  scale_x_continuous("Age") + theme_bw() + ggtitle("Log PAF")

# ####################################################################
# ## Good to do this for the overall VE, too ##
# ####################################################################
#   ve_data <- read.csv("filepath")
#   ve_data <- subset(ve_data, is_outlier==0 & use_review==0)
#
#   ve_data$lnrr <- log(1-ve_data$ve_vt_invasive/100)
#   ve_data$lnrr_se <- (log(1-ve_data$ve_vt_invasive_lower/100) - ve_data$lnrr) /  qnorm(0.975)
#   ve_df <- subset(ve_data, !is.na(lnrr) & lnrr_se > 0 & vtype !="PCV23")
#
# # Make some variables
#   ve_df$cv_observational <- ifelse(ve_df$study_type %in% c("Before-after","Cohort"),1,0)
#   ve_df$cv_rct <- ifelse(ve_df$study_type=="RCT",1,0)
#   ve_df$cv_adult <- ifelse(ve_df$age_end > 5, 1, 0)
#   ve_df$cv_pcv7 <- ifelse(ve_df$vtype=="PCV7",1,0)
#   ve_df$cv_pcv10 <- ifelse(ve_df$vtype=="PCV10",1,0)
#   ve_df$cv_pcv13 <- ifelse(ve_df$vtype=="PCV13",1,0)
#   ve_df$linear_vtype <- ifelse(ve_df$vtype=="PCV7",1,ifelse(ve_df$vtype=="PCV9",2,ifelse(ve_df$vtype=="PCV10",3,ifelse(ve_df$vtype=="PCV11",4,5))))
#
# # Test associations
#   pcv_effect <- rma(yi=lnrr, sei=lnrr_se, data=ve_df[ve_df$study_type=="RCT",], slab=paste0(first,"_",iso3))
#     forest(pcv_effect, transf=function(x) 1-exp(x))
#
# # That is the main analysis. 
#   summary(aov(ve_vt_invasive ~ study_type, data=ve_df))
#   ggplot(data=ve_df, aes(x=study_type, y=ve_vt_invasive/100, col=study_type)) + geom_boxplot() + theme_bw() +
#     geom_point(size=3, alpha=0.5)
#
#   ggplot(data=ve_df, aes(x=vtype, y=ve_vt_invasive/100, col=vtype)) + geom_boxplot() + theme_bw() +
#     geom_point(size=3, alpha=0.5)
#   ggplot(data=ve_df, aes(x=as.factor(cv_adult), y=ve_vt_invasive/100, col=as.factor(cv_adult))) + geom_boxplot() + theme_bw() +
#     geom_point(size=3, alpha=0.5)
#   ggplot(data=ve_df, aes(x=as.factor(cv_adult), y=ve_vt_invasive/100, col=as.factor(study_type))) + geom_boxplot() + theme_bw() +
#     geom_point(size=3, alpha=0.5, position=position_dodge(width=0.75))
#   ggplot(data=ve_df, aes(x=as.factor(cv_adult), y=ve_vt_invasive/100, col=as.factor(vtype))) + geom_boxplot() + theme_bw() +
#     geom_point(size=3, alpha=0.5, position=position_dodge(width=0.75))
#
# ## Save that file
#
# mrbrt <- ve_df[,c("age_start","age_end","lnrr","lnrr_se","vtype","cv_observational","cv_rct","cv_adult","cv_pcv7","cv_pcv10","cv_pcv13","linear_vtype")]
# mrbrt$intercept <- 1
#
# write.csv(mrbrt, "filepath", row.names=F)
#
# #########################################
