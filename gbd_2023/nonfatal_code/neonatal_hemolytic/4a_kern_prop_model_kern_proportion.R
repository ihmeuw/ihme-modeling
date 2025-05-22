################################################################################
## Purpose:  Determine relationship of haqi on proportion of kernicterus with
##           a spline on haqi, random effect on y-intercept, exclude gamma uncertainty,
##           10% trimming
## Input:    Data on probability of kernicterus (bundle 7256). Last systematic review
##           conducted for GBD2019. Five new sources added for GBD2021.
##           Covariates: TSB, haqi
## Output:   FILEPATH
################################################################################

# Source mrbrt functions/libraries ------------------------------------------------------------------------------------------
library(reticulate, lib.loc = "FILEPATH")
Sys.setenv("RETICULATE_PYTHON" = "FILEPATH")
library(mrbrt003, lib.loc = "FILEPATH")

mr <- import("mrtool") # load mrbrt tool

# Source other functions
library('openxlsx')
library('data.table')
library(ggplot2)
library(dplyr)
library(msm)

# set parameters ------------------------------------------------------------------------------------------------------------
bun_id <- 7256
bv_id <- 29633

# pull the bundle version and prep for modeling -----------------------------------------------------------------------------
bv_data <- ihme::get_bundle_version(bundle_version_id = bv_id, fetch = 'all')

# drop the group reviewed data
dt <- bv_data[group_review == 1 | is.na(group_review)]

# also only keep the data that is the proportion of acute impairment, not long-term
# impairment, which is represented in this bundle as case_name == 'kernicterus'
dt <- dt[case_name == "acute bilirubin encephalopathy"]

nrow(dt[is.na(mean)])

dt$mean <- as.numeric(dt$mean)
dt$sample_size <- as.numeric(dt$sample_size)
dt$location_id <- as.numeric(dt$location_id)
dt$year_start <- as.numeric(dt$year_start)
dt$year_end <- as.numeric(dt$year_end)
dt[, year_id := ceiling((year_start + year_end)/2)]

dt[sample_name == "extreme hyperbilirubinemia", from_jaundice := 0]
dt[sample_name == "jaundice", from_jaundice := 1]

dt[sex == 'Male', sex_id := 1]
dt[sex == 'Female', sex_id := 2]
dt[sex == 'Both', sex_id := 3]

setnames(dt, 'sample.TSB.threshold.(mg/dL)', 'TSB')

# load haqi covariate -------------------------------------------------------------------------------------------------------
haqi <- ihme::get_covariate_estimates(covariate_id=1099, release_id = 16)
setnames(haqi, 'mean_value', 'haqi')
dt2 <- merge(dt, haqi, by = c('location_id', 'year_id'), all.x = FALSE)
dt2 <- dt2[!is.na(TSB)]

# drop to just the necessary columns and logit transform --------------------------------------------------------------------
data <- dt2[, .(mean, standard_error, haqi, TSB, from_jaundice, nid, location_id)]

# logit transform and use delta method --------------------------------------------------------------------------------------
# logit transform the original data if bounded by 0 and 1
# -- SEs transformed using the delta method
# -- need to apply an offset to handle data value of 0 and 1, however these
# -- points ultimately are outliered anyway, so DON'T offset the other points.
data$standard_error <- as.numeric(data$standard_error)

# logit transform mean
data[mean == 1, mean := mean - 0.000001]
data[mean == 0, mean := mean + 0.000001]
data$mean_logit <- log(data$mean / (1-data$mean))

# delta transform standard error
data$se_logit <- mapply(FUN = function(mu, sigma) {
  msm::deltamethod(g = ~log(x1/(1-x1)), mean = mu, cov = sigma^2)
}, mu = data$mean, sigma = data$standard_error)

# load mrbrt data -----------------------------------------------------------------------------------------------------------
dat1 <- mr$MRData()
dat1$load_df(
  data = data,  col_obs = "mean_logit", col_obs_se = "se_logit",
  col_covs = list("haqi","TSB"), col_study_id = "nid" )

# optimize parameter values and specify covariates to be used in model-------------------------------------------------------
# placing spline on haqi, 10% trimming, exclude gamma uncertainty
mod1 <- mr$MRBRT(
  data = dat1,
  cov_models = list(
    mr$LinearCovModel("intercept", use_re = TRUE), #random effects should be estimated
    mr$LinearCovModel(
      alt_cov = "TSB",
      prior_spline_monotonicity = "increasing"
    ),
    mr$LinearCovModel(
      alt_cov = "haqi",
      use_spline = TRUE,
      spline_degree = 2L, #quadratic splines
      spline_knots = array(seq(0,1, length.out = 5)), #3 internal knots
      spline_knots_type = "frequency", #knots placed according to data density
      spline_r_linear = TRUE, #right tail is linear
      spline_l_linear = TRUE, #left tail is linear
      prior_spline_monotonicity = "decreasing" #increasing monotonicity prior
    )
  ),
  inlier_pct = 0.9 # trimming 10%
)

# plotting the data ---------------------------------------------------------------------------------------------------------
# from toolbox: We plot the data with size proportional to the inverse of the standard error (SE).
# Observations with lower SE have less uncertainty and are therefore more heavily weighted in the model,
# so we want them to be bigger on the plot.
make_plot <- function(dat1, size_multiplier = 1) {
  if (0) {
    dat1 <- data
    size_multiplier = 0.2
  }
  with(dat1, plot(
    x = TSB,
    y = mean_logit,
    cex = 1/se_logit * size_multiplier
  ))
  grid()
}

make_plot(dat = data, size_multiplier = 0.5)

# plotting spline predictions -----------------------------------------------------------------------------------------------
plot_spline_results <- function(mod_tmp, show_knots = TRUE) {
  df_pred_tmp <- data.frame(TSB = seq(0, 100, by = 0.1))
  dat_pred_tmp <- mr$MRData()
  dat_pred_tmp$load_df(
    data = df_pred_tmp,
    col_covs=list('TSB')
  )
  df_pred_tmp$pred_tmp <- mod_tmp$predict(data = dat_pred_tmp)
  with(df_pred_tmp, lines(TSB, pred_tmp))
  if (show_knots) {
    for (k in mod_tmp$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")
  }
}

plot_spline_results(mod_tmp = mod1)

# begin process of optimizing parameter values ------------------------------------------------------------------------------
mod1$fit_model()

# extract information from a fitted model -----------------------------------------------------------------------------------
mod1$summary()

# saving and loading model objects ------------------------------------------------------------------------------------------
py_save_object(object = mod1,
               filename = file.path("FILEPATH"), pickle = "dill")
mod1 <- py_load_object(filename = file.path("FILEPATH"), pickle = "dill")


# NOTE THAT THIS PREDICTION SECTION IS FOR MODEL FIT AND NOT FOR THE MODELABLE ENTITY #######################################
# plotting model results (excluding gamma uncertainty).
# set prediction frame
#df_pred <- data.frame(intercept = 1)

df_pred1 <- expand.grid(haqi = seq(5,88,0.2), TSB = seq(0,40,1))
dat_pred1 <- mr$MRData()
dat_pred1$load_df(
  data = df_pred1,
  col_covs=list('haqi', 'TSB')
)

n_samples1 <- as.integer(1000)

# samples1 <- mr$other_sampling$sample_simple_lme_beta(
samples1 <- mod1$sample_soln(
  sample_size = n_samples1
)

df_pred1$pred1_pt <- mod1$predict(data = dat_pred1)
head(df_pred1, n=5)

# get a data frame with estimated weights to identify which observations have been trimmed -----------------------------------
df_mod1 <- cbind(mod1$data$to_df(), data.frame(w = mod1$w_soln)) # you need this for plotting the trimmed points

# creating draws
draws1 <- mod1$create_draws(
  data = dat_pred1,
  beta_samples = samples1[[1]],
  gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
  random_study = FALSE )

# obtain uncertainty bounds
df_pred1$pred1_pt <- mod1$predict(data = dat_pred1)
df_pred1$pred1_lo <- apply(draws1, 1, function(x) quantile(x, 0.025))
df_pred1$pred1_up <- apply(draws1, 1, function(x) quantile(x, 0.975))

# plot model results
df_pred1 <- as.data.table(df_pred1)

# with just TSB 25
ggplot() +
  geom_ribbon(data = df_pred1[TSB == 25],
              aes(x = (haqi), ymax = pred1_up, ymin = pred1_lo), alpha = 0.5, fill = "grey") +
  geom_line(data = df_pred1[TSB == 25],
            aes(x = (haqi), y = pred1_pt), color = 'dodgerblue4') +
  geom_point(data=df_mod1,
             aes(x = haqi, y = obs, size = 1 - obs_se, color = TSB, shape = factor(w==0)),
             alpha = 0.8) +
  scale_shape_manual(values=c(19, 13), name = "Trimming", labels = c("Included", "Trimmed")) +
  guides(size = FALSE) +
  xlab("HAQI") + ylab("Kernicterus Proportion") +
  ggtitle("Predicted Kernicterus Proportion by HAQI for TSB Level of 25\nwithout uncertainty on gamma - spline on haqi - 10% trimming (LOGIT)")

# with TBS 15, 25, 35
ggplot() +
  geom_ribbon(data = df_pred1[TSB == 35],
              aes(x = (haqi), ymax = pred1_up, ymin = pred1_lo), alpha = 0.5, fill = "grey") +
  geom_line(data = df_pred1[TSB == 35],
            aes(x = (haqi), y = pred1_pt), color = 'dodgerblue1') +
  geom_ribbon(data = df_pred1[TSB == 25],
              aes(x = (haqi), ymax = pred1_up, ymin = pred1_lo), alpha = 0.5, fill = "grey") +
  geom_line(data = df_pred1[TSB == 25],
            aes(x = (haqi), y = pred1_pt), color = 'dodgerblue4') +
  geom_ribbon(data = df_pred1[TSB == 15],
              aes(x = (haqi), ymax = pred1_up, ymin = pred1_lo), alpha = 0.5, fill = "grey") +
  geom_line(data = df_pred1[TSB == 15],
            aes(x = (haqi), y = pred1_pt)) +
  geom_point(data=df_mod1,
             aes(x = haqi, y = obs, size = 1 - obs_se, color = TSB, shape = factor(w==0)),
             alpha = 0.8) +
  scale_shape_manual(values=c(19, 13), name = "Trimming", labels = c("Included", "Trimmed")) +
  guides(size = FALSE) +
  xlab("HAQI") + ylab("Kernicterus Proportion") +
  ggtitle("Predicted Kernicterus Proportion by HAQI for TSB Levels of 35, 25, and 15\nwithout uncertainty on gamma - spline on haqi - 10% trimming (LOGIT)")

# transform into linear space
df_pred1[, Y_mean_linear := (exp(pred1_pt)/(1 + exp(pred1_pt)))]
df_pred1[, Y_mean_hi_linear := (exp(pred1_up)/(1 + exp(pred1_up)))]
df_pred1[, Y_mean_lo_linear := (exp(pred1_lo)/(1 + exp(pred1_lo)))]
# transform df_mod1 to linear space to be able to show the trimmed points
df_mod1 <- data.table(df_mod1)
df_mod1[, obs_linear := (exp(obs)/(1 + exp(obs)))]

# with just TSB 25
ggplot() +
  geom_ribbon(data = df_pred1[TSB == 25],
              aes(x = (haqi), ymax = Y_mean_hi_linear, ymin = Y_mean_lo_linear), alpha = 0.5, fill = "grey") +
  geom_line(data = df_pred1[TSB == 25],
            aes(x = (haqi), y = Y_mean_linear), color = 'dodgerblue4') +
  geom_point(data=df_mod1,
             aes(x = haqi, y = obs_linear, size = 1 - obs_se, color = TSB, shape = factor(w==0)),
             alpha = 0.8) +
  scale_shape_manual(values=c(19, 13), name = "Trimming", labels = c("Included", "Trimmed")) +
  guides(size = FALSE) +
  #guides(size = none) +
  xlab("HAQI") + ylab("Kernicterus Proportion") +
  ggtitle("Predicted Kernicterus Proportion by HAQI for TSB Level of 25\nwithout uncertainty on gamma - spline on haqi - 10% trimming (LINEAR)")

# plot in linear space
ggplot() +
  geom_ribbon(data = df_pred1[TSB == 35],
              aes(x = (haqi), ymax = Y_mean_hi_linear, ymin = Y_mean_lo_linear), alpha = 0.5, fill = "grey") +
  geom_line(data = df_pred1[TSB == 35],
            aes(x = (haqi), y = Y_mean_linear), color = 'dodgerblue1') +
  geom_ribbon(data = df_pred1[TSB == 25],
              aes(x = (haqi), ymax = Y_mean_hi_linear, ymin = Y_mean_lo_linear), alpha = 0.5, fill = "grey") +
  geom_line(data = df_pred1[TSB == 25],
            aes(x = (haqi), y = Y_mean_linear), color = 'dodgerblue4') +
  geom_ribbon(data = df_pred1[TSB == 15],
              aes(x = (haqi), ymax = Y_mean_hi_linear, ymin = Y_mean_lo_linear), alpha = 0.5, fill = "grey") +
  geom_line(data = df_pred1[TSB == 15],
            aes(x = (haqi), y = Y_mean_linear)) +
  geom_point(data=df_mod1,
             aes(x = haqi, y = obs_linear, size = 1 - obs_se, color = TSB, shape = factor(w==0)),
             alpha = 0.8) +
  scale_shape_manual(values=c(19, 13), name = "Trimming", labels = c("Included", "Trimmed")) +
  guides(size = FALSE) +
  xlab("HAQI") + ylab("Kernicterus Proportion") +
  ggtitle("Predicted Kernicterus Proportion by HAQI for TSB Levels of 35, 25, and 15\nwithout uncertainty on gamma - spline on haqi - 10% trimming (LINEAR)")

# MAKE PREDICTIONS FOR HEMOLYTIC #############################################################################################
# set up df for haqi -----------------------------------------------------------
haqi <- haqi[, .(location_id, location_name, year_id, haqi)]
df_pred <- haqi[, TSB := 25]

dat_pred <- mr$MRData()
dat_pred$load_df(
  data = df_pred,
  col_covs=list('haqi', 'TSB')
)

n_samples1 <- as.integer(1000)

# samples1 <- mr$other_sampling$sample_simple_lme_beta(
samples1 <- mod1$sample_soln(
  sample_size = n_samples1
)

# to get the prediction points
df_pred$pred <- mod1$predict(data = dat_pred)
head(df_pred, n = 5)

# creating draws
draws <- mod1$create_draws(
  data = dat_pred,
  beta_samples = samples1[[1]],
  gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
  random_study = FALSE,
  sort_by_data_id = TRUE) # the sort by data argument ensures that the predictions are following the order of the pred frame

# save csv ---------------------------------------------------------------------
colnames(draws) <- unlist(lapply(0:999, function(x) paste('draw', x, sep = "_")))
draws_final <- cbind(df_pred, draws)
write.csv(draws_final, file = 'FILEPATH')
