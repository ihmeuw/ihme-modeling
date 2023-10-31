################################################################################################
### Estimate Vertical Transmission Proportions by Maternal Treatment Statua
#################################################################################################
#SETUP
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}
user <- Sys.info()["user"]
date <- gsub("-","_",Sys.Date())

### SOURCE PACKAGES for CS -----------------------------------------
library(tidyverse)
library(ggplot2)
library(openxlsx)
library(data.table)
library(msm)
library(arm)

library(crosswalk, lib.loc = "FILEPATH")
library(mrbrt001, lib.loc = "FILEPATH") # for R version 3.6.3

### Suicide Risk --------------------------------------------------------------
trans_dir <- "FILEPATH"
model_fpath <- paste0(trans_dir, "FILEPATH")
data <- data.table(read.xlsx(model_fpath, sheet = "new_method"))

candidate_covs <- c("cov_adequate", "cov_inadequate", "cov_untreated")

#transform to logit space
##LOGIT PREPARE-----------------------------------------------------------------------------------------------------------------------------------------------
data[ ,standard_error := sqrt((mean*(1-mean))/(sample_size))]
data_zero <- data[mean == 0]
data_nonzero <- data[mean != 0]

logit_data_nonzero <- data.table(delta_transform(mean = data_nonzero$mean, sd = data_nonzero$standard_error, transformation = "linear_to_logit"))
head(logit_data_nonzero)
dim(logit_data_nonzero)

full_nonzero <- cbind(data_nonzero, logit_data_nonzero)
full_nonzero <- full_nonzero[nid != 414917]

# Set trim percent
pct_trim <- 1 #inlier_pct
mrdata <- MRData()
mrdata$load_df(
  data = full_nonzero,
  col_obs = "mean_logit",
  col_obs_se = "sd_logit",
  col_study_id = "nid",
  col_covs = as.list(candidate_covs)
)
# run covfinder to see if any covariates are significant
covfinder <- CovFinder(
  data = mrdata,
  covs = as.list(candidate_covs),
  pre_selected_covs = list(),
  num_samples = 1000L,
  power_range = list(-4, 4),
  power_step_size = 0.05,
  inlier_pct = pct_trim,
  laplace_threshold = 1e-5
)
covfinder$select_covs(verbose = FALSE) # run covfinder
new_covs <- covfinder$selected_covs
print(new_covs) # prints selected covariate

# re-prep data with selected covariates from lasso
dat1 <- MRData()
dat1$load_df(
  data = full_nonzero,
  col_obs = "mean_logit",
  col_obs_se = "sd_logit",
  col_covs = as.list(new_covs),
  col_study_id = "nid")
# Set covariates, including the lasso selected ones
cov_models1 <- list(
  LinearCovModel("intercept", use_re = T) 
)
for (cov in new_covs) cov_models1 <- append(cov_models1,
                                            list(do.call(
                                              LinearCovModel,
                                              c(list(alt_cov=cov, use_re = T) 
                                              ))))

for (cov in new_covs) cov_models1 <- append(list(do.call(
  LinearCovModel,
  c(list(alt_cov=cov, use_re = T) 
  ))))

#fit mr-brt model
mod1 <- MRBRT(
  data = dat1,
  cov_models = cov_models1,
  inlier_pct = pct_trim
)
mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

#save this MRBRT model
py_save_object(object = mod1, filename = paste0(trans_dir, "FILEPATH"), pickle = "dill")
mod1 <- py_load_object(filename = paste0(trans_dir, "FILEPATH"), pickle = "dill")



#MAKE PREDICTIONS---------------------------------------------------------------------------------------------------------------
pred_combos <- data.table(read.xlsx(paste0(trans_dir, "FILEPATH")))
all_preds <- data.table()

for (i in 1:nrow(pred_combos)) {
  print(paste0("predicting for pred_combo " ,i))

  pred_data <- pred_combos[i]

  # pred_data <- as.data.table(expand.grid("intercept"=c(1),
  #                                        #"cov_nonadequate" = c(0),
  #                                        "cov_untreated" = c(0),
  #                                        "cov_inadequate" = c(0),
  #                                        "cov_adequate" = c(0)))

  dat_pred1 <- MRData()
  dat_pred1$load_df(
    data = pred_data,
    col_covs = as.list(new_covs)
  )
  # resample uncertainty
  print("sampling uncertainty")
  n_samples <- 1000L
  samples3 <- mod1$sample_soln(sample_size = n_samples)

  #make draws withOUT gamma
  print("making draws")
  draws3 <- mod1$create_draws(
     data = dat_pred1,
     beta_samples = samples3[[1]],
     gamma_samples = samples3[[2]],
     random_study = FALSE)

  pred_data$Y_mean <- mod1$predict(dat_pred1, sort_by_data_id = T)
  pred_data$Y_mean_lo_fe <- apply(draws3, 1, function(x) quantile(x, 0.025))
  pred_data$Y_mean_hi_fe <- apply(draws3, 1, function(x) quantile(x, 0.975))

  #make draws WITH gamma
  draws2 <- mod1$create_draws(
    data = dat_pred1,
    beta_samples = samples3[[1]],
    gamma_samples = samples3[[2]],
    random_study = TRUE)
  pred_data$Y_mean_lo_re <- apply(draws2, 1, function(x) quantile(x, 0.025))
  pred_data$Y_mean_hi_re <- apply(draws2, 1, function(x) quantile(x, 0.975))


  all_preds <- rbind(all_preds, pred_data)
}

View(all_preds)
all_preds[,Y_mean_normal := invlogit(Y_mean)]

write.xlsx(x = all_preds, file = "FILEPATH")


#PLOT THE DATA---------------------------------------------------------------------------------------------------------------------

#get appropriate data sets
pred_untreated <- all_preds[intercept == 1 & cov_untreated == 1 & cov_inadequate == 0 & cov_adequate == 0]
pred_inadequate <- all_preds[intercept == 1 & cov_untreated == 0 & cov_inadequate == 1 & cov_adequate == 0]
pred_adequate <- all_preds[intercept == 1 & cov_untreated == 0 & cov_inadequate == 0 & cov_adequate == 1]

#specify which prediction to plot
plot_model <- copy(pred_adequate)
treat_mom <- "adequate"
title_mom <-  "Transmission of Congenital Syphilis among post-neonates born to adequately treated mothers"

model_coef <- plot_model$Y_mean
model_upper <- plot_model$Y_mean_hi_re
model_lower <- plot_model$Y_mean_lo_re

#prep input data for plots
input_data <- copy(full_nonzero)
input_data[ ,`:=` (lower = mean_logit - 1.96*sd_logit,
                   upper = mean_logit + 1.96*sd_logit)] #can I do this?
input_data[ ,location_name := gsub(".*\\|","",location_name)]
input_data[ ,study_name := paste0(author_name, "_", location_name, "_", mid_year)]

setnames(input_data, "model_case_type", "case_definition_type")

#PLOT & SAVE DATA---------

#pdf()

ggplot() +
  geom_point(data = input_data[maternal_treatment %in% c(treat_mom, "pooled")], aes(x = invlogit(mean_logit), y = study_name, color = maternal_treatment), show.legend = TRUE, size = 3) +
  #xlim(0,1) +
  scale_color_manual(name = "maternal_treatment", values = c( "pooled" = "purple", "adequate" = "skyblue")) +
  annotate("rect", xmin = invlogit(plot_model$Y_mean_lo_fe), xmax = invlogit(plot_model$Y_mean_hi_fe), ymin = 0, ymax = Inf,alpha = .2, fill = "black") +
  #annotate("rect", xmin = model_lower[2], xmax = model_upper[2], ymin = 0, ymax = Inf,alpha = .2, fill = "green") +

  geom_errorbarh(data = input_data[maternal_treatment %in% c(treat_mom, "pooled")], aes(y = study_name, x = invlogit(mean_logit), xmin = invlogit(lower), xmax = invlogit(upper), height=0.25)) +
  geom_vline(xintercept = invlogit(plot_model$Y_mean), linetype = "dashed", color = "black") +

  geom_vline(xintercept = 0) +
  #geom_rect(data = model_ci, xmin = model_lower[1], xmax = model_upper[1], ymin = -Inf, ymax = Inf, aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax), alpha = 0.2, fill = "red") +
  labs(x = paste0("Proportion with CS (Value = ", round(invlogit(model_coef)*100, 1),"%)"), y = "Study") +
  #
  ggtitle(label = title_mom) + #confirm title above
  theme_bw()+
  theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(),
        legend.text = element_text(size=10),axis.text.y = element_text(size = 11),
        plot.title = element_text(hjust = 0.5,size = 12))

dev.off()

