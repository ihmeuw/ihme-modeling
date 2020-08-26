#######################################################################################
### Date:     21th May 2019
### Purpose:  Run MR-BRT for RRs for Bullying
#######################################################################################

rm(list=ls())

source("/FILEPATH/mr_brt_functions.R")
library(data.table)
library(openxlsx)
library(msm)

static_cvs <- c("cv_anx", "cv_symptoms", "cv_unadjusted", "cv_b_parent_only", "cv_or", "cv_multi_reg",
                "cv_cyberbullying", "cv_low_threshold_bullying", "cv_baseline_adjust",
                "cv_selection_bias", "cv_male", "cv_female")

## Load RRs ##
dataset <- fread("/FILEPATH.csv")
dataset[, estimate_id := seq_len(.N)]

## Run MR-BRT ##

for(c in static_cvs){
  mean_prior <- NA
  var_prior <- NA
  cov <- list(cov_info(c, "X", gprior_mean = mean_prior, gprior_var = var_prior),
              cov_info(c, "Z", gprior_mean = mean_prior, gprior_var = var_prior))
  if(c == static_cvs[1]){
    cov_list <- cov
  } else {
    cov_list <- c(cov_list, cov)
  }
}

cov_list <- c(cov_list, list(cov_info("time", "X", type = 'continuous', gprior_mean = NA, gprior_var = NA)))

dir.create(file.path("/FILEPATH/"), showWarnings = FALSE)

model <- run_mr_brt(
  output_dir = "/FILEPATH/",
  model_label = "#1",
  data = dataset,
  mean_var = "log_effect_size",
  se_var = "log_effect_size_se",
  covs = cov_list,
  method = "trim_maxL",
  trim_pct = 0.1,
  study_id = "cohort",
  overwrite_previous = TRUE,
  lasso = F)


predict_matrix <- data.table(intercept = 1, time = c(0:50), cv_symptoms=0, cv_unadjusted=0, cv_b_parent_only=0, cv_or=0,
           cv_multi_reg=0, cv_cyberbullying=0, cv_low_threshold_bullying=0, cv_baseline_adjust=0,
           cv_anx=c(rep(0, 51), rep(1, 51)), cv_selection_bias=0, cv_male=0, cv_female=0)
z_predict <- copy(predict_matrix)
z_predict[,time := NULL]
z_predict <- unique(z_predict)

outputs <- load_mr_brt_outputs(model)$model_coefs

write.csv(outputs, "/FILEPATH.csv", row.names=F)

rr_summaries <- as.data.table(predict_mr_brt(model, newdata = predict_matrix)["model_summaries"]) 
names(rr_summaries) <- gsub("model_summaries.", "", names(rr_summaries))
rr_summaries <- rr_summaries[,.(time = X_time, Y_mean, Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)), rr = exp(Y_mean), lower = exp(Y_mean_lo), upper = exp(Y_mean_hi), anx = X_cv_anx)]

rr_draws <- as.data.table(predict_mr_brt(model, newdata = predict_matrix, write_draws = TRUE)["model_draws"]) 
names(rr_draws) <- gsub("model_draws.", "", names(rr_draws))
rr_draws <- rr_draws[, (c("X_cv_anx", "X_time", paste0("draw_", 0:999))), with=F]
setnames(rr_draws, "X_time", "time")
setnames(rr_draws, "X_cv_anx", "cv_anx")
rr_draws <- melt.data.table(rr_draws, id.vars = c("cv_anx", "time"), value.name = "rr", variable.name = "draw")
rr_draws[, mean_rr := mean(rr), by = c("cv_anx", "time")]
rr_draws <- rr_draws[mean_rr > 0, ]
write.csv(rr_draws, "/FILEPATH.csv", row.names=F)

## Estimate adjusted RRs ##
eval(parse(text = paste0("crosswalk <- expand.grid(", paste0("intercept = 0, time = 0, ", paste0(paste0(static_cvs), "=c(0, 1)", collapse = ", "), collapse = ""), ")")))
crosswalk <- as.data.table(predict_mr_brt(model, newdata = crosswalk)["model_summaries"])
names(crosswalk) <- gsub("model_summaries.", "", names(crosswalk))
names(crosswalk) <- gsub("X_", "", names(crosswalk))

crosswalk[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
crosswalk[, (c("time", "intercept", "Z_intercept", "Y_negp", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe")) := NULL]
crosswalk[, (names(crosswalk)[(names(crosswalk) %like% "Z_")]) := NULL]

trained_data <- data.table(load_mr_brt_outputs(model)$train_data)
trained_data <- merge(trained_data, crosswalk, by = (static_cvs))

trained_data[Y_mean != crosswalk[1,Y_mean], `:=` (log_rr_trained = log_effect_size - Y_mean, log_rr_trained_se = sqrt(log_effect_size_se + Y_se))]
trained_data[is.na(log_rr_trained), `:=` (log_rr_trained = log_effect_size, log_rr_trained_se = log_effect_size_se)]
trained_data[, `:=` (log_rr_trained_lower = log_rr_trained - 1.96*log_rr_trained_se, log_rr_trained_upper = log_rr_trained + 1.96*log_rr_trained_se)]
trained_data[, `:=` (rr_trained = exp(log_rr_trained), lower_trained = exp(log_rr_trained_lower), upper_trained = exp(log_rr_trained_upper))]

trained_data[, `:=` (rr = exp(log_effect_size), rr_se = (exp(log_effect_size + log_effect_size_se*1.96) - exp(log_effect_size - log_effect_size_se*1.96))/3.92)]

library(ggplot2)

plot <- ggplot(data=rr_summaries[anx == 0, ], aes(x=time, y=rr), fill = "blue")+
  geom_ribbon(data= rr_summaries[anx == 0, ], aes(x=time, ymin=lower, ymax=upper),  fill="blue", alpha=.7) +
  geom_line(size=1) +
  geom_line(data=rr_summaries[anx == 1, ], aes(x=time, y=rr), color="black", size=1)+
  geom_ribbon(data= rr_summaries[anx == 1, ], aes(x=time, ymin=lower, ymax=upper),  fill="lightgrey", alpha=.5) +
  ylab("Relative risk") +
  xlab("Follow-up (years)") +
  theme_minimal() +
  scale_x_continuous(expand=c(0,0), breaks = seq(5, 60, 5))+
  theme(axis.line=element_line(colour="black")) +
  geom_point(data=trained_data[w ==1 & cv_anx == 0,], aes(x=time, y=rr_trained) , color="blue", size=trained_data[w ==1 & cv_anx == 0, 3/((upper_trained - lower_trained)/3.92)], shape=16) +
  geom_point(data=trained_data[w ==1 & cv_anx == 1,], aes(x=time, y=rr_trained) , color="black", size=trained_data[w ==1 & cv_anx == 1, 3/((upper_trained - lower_trained)/3.92)], shape=16) +
  geom_point(data=trained_data[w ==0,], aes(x=time, y=rr_trained) , color="red", size=trained_data[w ==0, 3/((upper_trained - lower_trained)/3.92)], shape=16) +
  geom_point(data=trained_data[w ==1 & cv_anx == 0,], aes(x=time, y=rr) , color="blue", size=1/trained_data[w ==1  & cv_anx == 0, rr_se], shape=1) +
  geom_point(data=trained_data[w ==1 & cv_anx == 1,], aes(x=time, y=rr) , color="black", size=1/trained_data[w ==1 & cv_anx == 1, rr_se], shape=1) +
  geom_point(data=trained_data[w ==0,], aes(x=time, y=rr) , color="red", size=1/trained_data[w ==0, rr_se], shape=1) +
  geom_hline(yintercept=1, linetype="dashed", color="dark grey", size=1)
plot

ggsave(plot, filename="/FILEPATH.pdf", width = 8, height = 4)
