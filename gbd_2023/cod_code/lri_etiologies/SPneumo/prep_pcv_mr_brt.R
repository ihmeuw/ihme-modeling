##############################################
# Prep files for MR-BRT on S pneumo study attributable fractions
#################################################################
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
date <- "2021_03_25"
library(ggplot2)
model_name <- paste0(date, "_GBD_2019_rerun")

pcv_data <- read.csv("FILEPATH")

pcv_data$age <- floor((pcv_data$age_end + pcv_data$age_start) / 2)
pcv_data <- subset(pcv_data, meas_stdev < 20)
mrbrt <- pcv_data[,c("age","mean_log_paf","std_log_paf","meas_value","meas_stdev","x_befaft")]
mrbrt$intercept <- 1
mrbrt$age_cat <- ifelse(mrbrt$age < 2, 1, ifelse(mrbrt$age < 5, 2, ifelse(mrbrt$age < 60, 3, 4)))


write.csv(mrbrt, paste0("FILEPATH"))

fit1 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = paste0("age_ve_",model_name),
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
p <- ggplot(preds, aes(x=X_age, y=(Y_mean))) + geom_line() +
  geom_ribbon(aes(ymin=(Y_mean_lo), ymax=(Y_mean_hi)), alpha=0.3) +
  geom_point(data=mod_data, aes(x=age, y=mean_log_paf, size=1/std_log_paf^2, col=factor(outlier))) + guides(size=F) + scale_color_manual(values=c("black","red")) + guides(col=F) +
  geom_hline(yintercept=0, lty=2, col="red") + scale_y_continuous("Log ratio") + scale_x_continuous("Age") + theme_bw() + ggtitle("Log PAF")
ggsave(plot = p, filename = paste0("FILEPATH", model_name, ".pdf"))
