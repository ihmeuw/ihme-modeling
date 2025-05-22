
library(data.table)
library(dplyr)
library(msm)
library(mrbrt001, lib.loc = '/FILEPATH/')
library(RhpcBLASctl, lib.loc = "/FILEPATH/")
RhpcBLASctl::omp_set_num_threads(threads = 4)

# Load in dataset and add extra data prep ----------------------------------------------------
dataset <- fread("/FILEPATH/liberal_data_file.csv")

## Enter age and sex information not previously extracted
dataset[author == "Kaltiala-Heino 2010", `:=` (percent_female =  1167/(1167+903), age_mean = 15.5, age_sd = 0.4)]
dataset[author == "Vassallo 2014", `:=` (percent_female =  0.56)]
dataset[author == "Gibb 2011", `:=` (percent_female =  630 / 1265)]
dataset[author == "Zwierzynska 2013", `:=` (percent_female = 1915/( 1915+1777))]
dataset[author == "Bowes 2015", `:=` (percent_female = 1-0.455)]
dataset[author == "Lereya 2015a" & cohort == "Great Smoky Mountains Study", `:=` (percent_female = 0.49)]
dataset[author == "Lereya 2015a" & cohort == "Avon Longitudinal Study of Parents and Children (ALSPAC)", `:=` (percent_female = 2239/4026)]
dataset[author == "Lereya 2015b", `:=` (percent_female = 2285/4101)]
dataset[author == "Stapinski 2014", `:=` (percent_female = (1392+1181+582)/(2845+2247+1116))]
dataset[author == "Takizawa 2014", `:=` (percent_female = 4595/9005)]
dataset[author == "Schoon 1997", `:=` (percent_female = 4595/9005)]
dataset[author == "Rothon 2011", `:=` (percent_female = 1434/(1434+1335), age_mean = (12.18*1381+14.22*1408)/(1381+1408), age_sd = sqrt(((0.33^2)*(1381^2) + (0.33^2)*(1408^2))*((1/(1381+1408))^2)))]
dataset[author == "Haavisto 2004", `:=` (percent_female = 0)]
dataset[author == "Sourander 2007", `:=` (percent_female = 0)]
dataset[author == "Klomek 2008", `:=` (percent_female = 0)]
dataset[author == "Copeland  2013", `:=` (percent_female = 0.49)]
dataset[author == "Patton 2008", `:=` (percent_female = 1)]
dataset[author == "Hemphill 2011", `:=` (percent_female = 0.52, age_mean = 12.9, age_sd = 0.4)]
dataset[author == "Hemphill 2015", `:=` (percent_female = 1-0.46, age_sd = 0.4)]
dataset[author == "Fahy 2016", `:=` (percent_female = 1110/(1110+1370))]
dataset[author == "Farrington 2011", `:=` (percent_female = 0)]
dataset[author == "Moore 2014", `:=` (percent_female = (569+67)/1590)]
dataset[author == "Silberg 2016", `:=` (percent_female = (1547)/(1547+1337))]
dataset[author == "Sigurdson 2015", `:=` (percent_female = 0.567, age_mean = 13.7, age_sd = 0.58)]

dataset[cv_male == 1, `:=` (percent_female = 0)]
dataset[cv_female == 1, `:=` (percent_female = 1)]
dataset <- dataset[cv_cyberbullying == 0,]

dataset <- dataset[author != "Gibb 2011"] ## Does NOT meeting inclusion criteria for the method it captures bullying (no indication of frequency)
dataset <- dataset[!(author == "Zwierzynska 2013" & cv_b_parent_only == 1)] # Parent measure does not meet inclusion criteria for the method it captures bullying (no indication of frequency)
dataset <- dataset[!(author == "Stapinski 2014" & cv_b_parent_only == 1)] # Parent measure does not meet inclusion criteria for the method it captures bullying (no indication of frequency)
dataset <- dataset[author != "Copeland  2013"] # Does NOT meeting inclusion criteria for the method it captures bullying (4 to 6 times between ages 9 and 16)
dataset <- dataset[author != "Vassallo 2014"] # Does NOT meeting inclusion criteria for the method it captures bullying (only considered bullied if indicated being bullied wsa a problem for them)

## make corections to Schoon 1997
dataset[author == "Schoon 1997", cv_multi_reg := 0]
schoon_1997_mr_both <- dataset[author == "Schoon 1997" & cv_low_threshold_bullying == 0,]
schoon_1997_mr_male <- dataset[author == "Schoon 1997" & cv_low_threshold_bullying == 0,]
schoon_1997_mr_female <- dataset[author == "Schoon 1997" & cv_low_threshold_bullying == 0,]
schoon_1997_mr_both[, `:=` (cv_multi_reg = 1, log_effect_size = log(2.15), log_effect_size_se = deltamethod(~log(x1), 2.15, 0.14^2))]
schoon_1997_mr_male[, `:=` (cv_multi_reg = 1, percent_female = 0, log_effect_size = log(2.39), log_effect_size_se = deltamethod(~log(x1), 2.39, 0.21^2))]
schoon_1997_mr_female[, `:=` (cv_multi_reg = 1, percent_female = 1, log_effect_size = log(1.99), log_effect_size_se = deltamethod(~log(x1), 1.99, 0.18^2))]
dataset <- rbind(dataset, schoon_1997_mr_both, schoon_1997_mr_male, schoon_1997_mr_female)

##  There should be an interaction between cv_multi_reg and cv_low_threshold_bullying. cv_multi_reg only over-estimates the RR when
##  cv_low_threshold_bullying == 0, otherwise it underestimates the RR even more than when cv_low_threshold_bullying == 1 and cv_multi_reg == 0.
##  Have therefore decided to exclude estimates where cv_low_threshold_bullying == 1 & cv_multi_reg == 1 as they have a negative impact on the
##  covariates and we do not lose any studies by excluding these estimates.
multi_reg_low_threshold_studies <- dataset[(cv_multi_reg == 1 & cv_low_threshold_bullying == 1), author]

dataset <- dataset[!(cv_multi_reg == 1 & cv_low_threshold_bullying == 1),]

multi_reg_low_threshold_studies[!(multi_reg_low_threshold_studies %in% dataset$author)] ## does not exclude any studies

dataset[, m_percent_female := percent_female - 0.5]


mr_dataset <- MRData()

mr_dataset$load_df(
  data = dataset, col_obs = "log_effect_size", col_obs_se = "log_effect_size_se",
  col_covs = list("cv_anx", "cv_symptoms", "cv_unadjusted", "cv_b_parent_only", "cv_or", "cv_multi_reg",
                  "cv_low_threshold_bullying", "cv_baseline_adjust",
                  "cv_selection_bias", "percent_female", "m_percent_female", "time"), col_study_id = "cohort" )


# 1)	  Run Log-linear model with exposure to get posterior standard deviation for beta --------

model <- MRBRT(
  data = mr_dataset,
  cov_models =list(
    LinearCovModel("intercept", use_re = TRUE),
    LinearCovModel("time", use_re = F)),
  inlier_pct = 0.9)

model$fit_model(inner_print_level = 5L, inner_max_iter = 500L)
samples <- model$sample_soln(sample_size = 1000L)
betas <- data.table(samples[[1]])
names(betas) <- model$cov_names
beta_std <- sd(betas[,intercept])

# 2)	 Covariate selection using log-linear model --------------------------
static_cvs <- c("cv_anx", "cv_unadjusted", "cv_b_parent_only", "cv_multi_reg",
                "cv_low_threshold_bullying", "cv_selection_bias")

candidate_covs <- static_cvs[!static_cvs == "cv_anx"]

covfinder <- CovFinder(
  data = mr_dataset,
  covs = as.list(candidate_covs),
  pre_selected_covs = list("intercept", "time", "percent_female", "cv_anx"),
  beta_gprior_std = 0.1 * beta_std,
  normalized_covs = T,
  num_samples = 1000L,
  power_range = list(-4, 4),
  power_step_size = 1,
  laplace_threshold = 1e-5,
  inlier_pct = 0.9
)
covfinder$select_covs(verbose = TRUE)
selected_covs <- covfinder$selected_covs

# 3)	Run Log-linear model to get slope prior for next stage ---------------

non_bias_covs <- c("intercept", "time", "m_percent_female", "cv_anx")
bias_covs <- selected_covs[!(selected_covs %in% non_bias_covs)]

# Create covlist
cov_list <- list(LinearCovModel("intercept", use_re = TRUE))
for(c in non_bias_covs[non_bias_covs != "intercept"]){cov_list <- c(cov_list, list(LinearCovModel(c, use_re = F)))}
for(c in bias_covs){cov_list <- c(cov_list, list(LinearCovModel(c, use_re = F, prior_gamma_gaussian = array(c(0, 0.1*beta_std)))))}

model <- MRBRT(data = mr_dataset, cov_models =cov_list, inlier_pct = 0.9)

model$fit_model(inner_print_level = 5L, inner_max_iter = 500L)
samples <- model$sample_soln(sample_size = 1000L)

## My plot
used_data <- cbind(model$data$to_df(), data.frame(w = model$w_soln))

eval(parse(text = paste0("predict_matrix <- data.table(intercept = 0, time = c(0:50), m_percent_female = 0, cv_anx=c(rep(0, 51), rep(1, 51)), ", paste0(paste0(bias_covs), "=0", collapse = ", "), ")")))

predict_data <- MRData()
predict_data$load_df(
  data = predict_matrix,
  col_covs=as.list(selected_covs[selected_covs != "intercept"]))
draws <- model$create_draws(
  data = predict_data,
  beta_samples = samples[[1]],
  gamma_samples = samples[[2]],
  random_study = TRUE )
predict_matrix$pred <- model$predict(data = predict_data)
predict_matrix$pred_lo <- apply(draws, 1, function(x) quantile(x, 0.025))
predict_matrix$pred_hi <- apply(draws, 1, function(x) quantile(x, 0.975))
rr_summaries <- copy(predict_matrix)
rr_summaries[1:51, anx := 0]
rr_summaries[52:102, anx := 1]
rr_summaries[, `:=` (rr = exp(pred), lower = exp(pred_lo), upper = exp(pred_hi))]

eval(parse(text = paste0("crosswalk_m <- expand.grid(", paste0("intercept = 0, time = 0, m_percent_female = 0, ", paste0(paste0(c("cv_anx", bias_covs)), "=c(0, 1)", collapse = ", "), collapse = ""), ")")))
crosswalk_d <- MRData()
crosswalk_d$load_df(
  data = crosswalk_m,
  col_covs=as.list(selected_covs[selected_covs != "intercept"]))
cw_draws <- model$create_draws(
  data = crosswalk_d,
  beta_samples = samples[[1]],
  gamma_samples = samples[[2]],
  random_study = F )

crosswalk_m$Y_mean <- model$predict(crosswalk_d) - model$predict(crosswalk_d)[1]
crosswalk_m$Y_mean_lo <- apply(cw_draws-mean(cw_draws[1,]) , 1, function(x) quantile(x, 0.025))
crosswalk_m$Y_mean_hi <- apply(cw_draws-mean(cw_draws[1,]), 1, function(x) quantile(x, 0.975))
crosswalk_m <- data.table(crosswalk_m)[, `:=` (intercept = NULL, time = NULL)]
crosswalk_m[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]

used_data <- data.table(merge(used_data, crosswalk_m, by = c("cv_anx", bias_covs), all.x = T))

used_data[as.numeric(Y_mean) != as.numeric(crosswalk_m$Y_mean[1]), `:=` (log_rr_trained = obs - Y_mean, log_rr_trained_se = sqrt(obs_se^2 + Y_se^2))]
used_data[is.na(log_rr_trained), `:=` (log_rr_trained = obs, log_rr_trained_se = obs_se)]
used_data[, `:=` (log_rr_trained_lower = log_rr_trained - 1.96*log_rr_trained_se, log_rr_trained_upper = log_rr_trained + 1.96*log_rr_trained_se)]
used_data[, `:=` (rr_trained = exp(log_rr_trained), lower_trained = exp(log_rr_trained_lower), upper_trained = exp(log_rr_trained_upper))]
used_data[, `:=` (rr = exp(obs), rr_se = (exp(obs + obs_se*1.96) - exp(obs - obs_se*1.96))/3.92, rr_trained_se = (upper_trained - lower_trained)/3.92)]

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
  geom_point(data=used_data[w ==1 & cv_anx == 0,], aes(x=time, y=rr_trained) , color="blue", size=used_data[w ==1 & cv_anx == 0, 1/rr_trained_se], shape=16) +
  geom_point(data=used_data[w ==1 & cv_anx == 1,], aes(x=time, y=rr_trained) , color="black", size=used_data[w ==1 & cv_anx == 1, 1/rr_trained_se], shape=16) +
  geom_point(data=used_data[w ==0,], aes(x=time, y=rr_trained) , color="red", size=used_data[w ==0, 1/rr_trained_se], shape=16) +
  geom_point(data=used_data[w ==1 & cv_anx == 0,], aes(x=time, y=rr) , color="blue", size=1/used_data[w ==1  & cv_anx == 0, rr_se], shape=1) +
  geom_point(data=used_data[w ==1 & cv_anx == 1,], aes(x=time, y=rr) , color="black", size=1/used_data[w ==1 & cv_anx == 1, rr_se], shape=1) +
  geom_point(data=used_data[w ==0,], aes(x=time, y=rr) , color="red", size=1/used_data[w ==0, rr_se], shape=1) +
  geom_hline(yintercept=1, linetype="dashed", color="dark grey", size=1)
plot


cov_coefs <- data.table(cov = model$cov_names, coef = as.numeric(model$beta_soln))

slope <- cov_coefs[cov == "time", coef]

betas <- data.table(samples[[1]])
names(betas) <- model$cov_names
betas <- data.table(cov = model$cov_names, coef = as.numeric(model$beta_soln),
                    lower = lapply(names(betas), function(x){betas[,quantile(get(x), 0.025)]}),
                    upper = lapply(names(betas), function(x){betas[,quantile(get(x), 0.975)]}),
                    se = lapply(names(betas), function(x){betas[,sd(get(x), 0.025)]}))

betas[,.(cov, coef = round(coef, 3), ci = paste(round(as.numeric(lower), 3), " to ", round(as.numeric(upper), 3)), se = round(as.numeric(se), 3))]
betas[, `:=` (lower = as.numeric(lower), upper = as.numeric(upper), se = as.numeric(se))]

# 4a)	Ensemble spline model: MDD ----------------------------

mr_dataset_mdd <- MRData()
mr_dataset_mdd$load_df(
  data = dataset[cv_anx == 0,], col_obs = "log_effect_size", col_obs_se = "log_effect_size_se",
  col_covs = list("cv_anx", "cv_symptoms", "cv_unadjusted", "cv_b_parent_only", "cv_or", "cv_multi_reg",
                  "cv_low_threshold_bullying", "cv_baseline_adjust", "cv_selection_bias",
                  "percent_female", "time"), col_study_id = "cohort" )

start <- (quantile(dataset$time, 0.1) - 0)/(max(dataset$time)-0)
end <- (quantile(dataset$time, 0.9) - 0)/(max(dataset$time)-0)
min_size <- (end-start)*0.2
interval_sizes <- rbind(c(min_size, 1.), c(min_size, 1.), c(min_size, 1.), c(min_size, 1.))
knot_bounds <- rbind(c(start, end), c(start, end),c(start, end))

knots_samples <- utils$sample_knots(
  num_intervals = 4L,
  knot_bounds = knot_bounds,
  interval_sizes = interval_sizes,
  num_samples = 50L
)

ensemble_cov_model <- LogCovModel(
  alt_cov = "time",
  use_spline = TRUE,
  spline_degree = 3L,
  spline_knots_type = 'domain',
  spline_r_linear = F,
  prior_spline_funval_uniform = array(c(-1 + 1e-6, 19)),
  prior_spline_num_constraint_points = 100L,
  prior_spline_maxder_gaussian = rbind(c(0,0,0,0), c(0.001, 0.01,0.01,Inf)),
  prior_spline_derval_uniform = array(c(-abs(slope), abs(slope))),
  spline_knots = array(seq(0, 1, by = 0.25)),
  prior_spline_monotonicity = 'decreasing'
)

cov_list <- list(LinearCovModel("intercept", use_re = TRUE), LinearCovModel("percent_female", use_re = F, prior_beta_gaussian = array(c(betas[cov == "percent_female",coef], betas[cov == "percent_female",se]))))
for(c in bias_covs){cov_list <- c(cov_list, list(LinearCovModel(c, use_re = F, prior_beta_gaussian = array(c(betas[cov ==c,coef], betas[cov == c,se])), prior_gamma_gaussian = array(c(0, 0.1*beta_std)))))}

model_mdd <- MRBeRT(
  data = mr_dataset_mdd,
  ensemble_cov_model = ensemble_cov_model,
  ensemble_knots = knots_samples,
  cov_models = cov_list,
  inlier_pct = 0.9)


model_mdd$fit_model(inner_print_level = 5L, inner_max_iter = 500L)

samples_mdd <- model_mdd$sample_soln(sample_size = 1000L)

## Plot
used_data <- as.data.table(model_mdd$data$to_df())
used_data[, seq := 1:.N]
num_sub <- 1:model_mdd$num_sub_models # number of submodels
long_df <- rbindlist(lapply(num_sub, function(sub){
  data_temp <- copy(used_data)
  data_temp[, sub_model := sub]
  data_temp <- cbind(data_temp, data.frame(w = model_mdd$sub_models[[sub]]$lt$w))
  data_temp[, sub_model_w := model_mdd$weights[sub]]
  data_temp[, sub_inlier := w*sub_model_w]
  return(data_temp)
}))
long_df[, inlier := sum(sub_inlier), by = "seq"]
used_data <- merge(used_data, unique(long_df[,.(seq, inlier)]), by = "seq")
used_data[, outlier := ifelse(inlier > 0.6, 0, 1)]

eval(parse(text = paste0("predict_matrix <- data.table(intercept = 1, time = c(0:45), percent_female = 0.5, ", paste0(paste0(bias_covs), "=0", collapse = ", "), ")")))
predict_data <- MRData()
predict_data$load_df(
  data = predict_matrix,
  col_covs=as.list(selected_covs[!(selected_covs %in% c("intercept", "cv_anx"))]))

get_gamma_sd <- function(model){
  gamma <- model$gamma_soln
  gamma_fisher <- model$lt$get_gamma_fisher(gamma)
  return(sqrt(diag(solve(gamma_fisher))))
}


gamma_soln <- rep(0, model_mdd$sub_models[[1]]$num_z_vars)
for (i in 1:model_mdd$num_sub_models) {
  sub_gamma <- model_mdd$sub_models[[i]]$gamma_soln
  sub_gamma_sd <- get_gamma_sd(model_mdd$sub_models[[i]])
  gamma_soln <- gamma_soln + model_mdd$weights[[i]]*(sub_gamma + 2.0*sub_gamma_sd)
}

gamma_samples_mdd <-samples_mdd[[2]]
for(i in c(1:50)){
  if(nrow(gamma_samples_mdd[[i]]) > 0){
    for(n in c(1:nrow(gamma_samples_mdd[[i]]))){
      gamma_samples_mdd[[i]][[n]] <- gamma_soln
    }
  }
}

draws <- model_mdd$create_draws(
  data = predict_data,
  beta_samples = samples_mdd[[1]],
  gamma_samples = gamma_samples_mdd,
  random_study = TRUE )
predict_matrix$pred <- model_mdd$predict(data = predict_data)
predict_matrix$pred_lo <- apply(draws, 1, function(x) quantile(x, 0.025))
predict_matrix$pred_hi <- apply(draws, 1, function(x) quantile(x, 0.975))
rr_summaries <- copy(predict_matrix)
rr_summaries[, anx := 0]
rr_summaries[, `:=` (rr = exp(pred), lower = exp(pred_lo), upper = exp(pred_hi))]
rr_summaries <- rr_summaries[rr >= 1 | time <= dataset[,max(time)+1],]

eval(parse(text = paste0("crosswalk_m <- expand.grid(", paste0("intercept = 0, time = 0, percent_female = 0, ", paste0(paste0(bias_covs), "=c(0, 1)", collapse = ", "), collapse = ""), ")")))
crosswalk_d <- MRData()
crosswalk_d$load_df(
  data = crosswalk_m,
  col_covs=as.list(selected_covs[!(selected_covs %in% c("intercept", "cv_anx"))]))

cw_draws <- model_mdd$create_draws(
  data = crosswalk_d,
  beta_samples = samples_mdd[[1]],
  gamma_samples = gamma_samples_mdd,
  random_study = F )

crosswalk_m$Y_mean <- model_mdd$predict(crosswalk_d) - model_mdd$predict(crosswalk_d)[1]
crosswalk_m$Y_mean_lo <- apply(cw_draws-mean(cw_draws[1,]) , 1, function(x) quantile(x, 0.025))
crosswalk_m$Y_mean_hi <- apply(cw_draws-mean(cw_draws[1,]), 1, function(x) quantile(x, 0.975))
crosswalk_m <- data.table(crosswalk_m)[, `:=` (intercept = NULL, time = NULL)]
crosswalk_m[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]

used_data <- data.table(merge(used_data, crosswalk_m, by = bias_covs, all.x = T))

used_data[as.numeric(Y_mean) != as.numeric(crosswalk_m$Y_mean[1]), `:=` (log_rr_trained = obs - Y_mean, log_rr_trained_se = sqrt(obs_se^2 + Y_se^2))]
used_data[is.na(log_rr_trained), `:=` (log_rr_trained = obs, log_rr_trained_se = obs_se)]
used_data[, `:=` (log_rr_trained_lower = log_rr_trained - 1.96*log_rr_trained_se, log_rr_trained_upper = log_rr_trained + 1.96*log_rr_trained_se)]
used_data[, `:=` (rr_trained = exp(log_rr_trained), lower_trained = exp(log_rr_trained_lower), upper_trained = exp(log_rr_trained_upper))]
used_data[, `:=` (rr = exp(obs), rr_se = (exp(obs + obs_se*1.96) - exp(obs - obs_se*1.96))/3.92, rr_trained_se = (upper_trained - lower_trained)/3.92)]
used_data[, w := ifelse(outlier == 0, 1, 0)]

library(ggplot2)

plot <- ggplot(data=rr_summaries[anx == 0, ], aes(x=time, y=rr), fill = "blue")+
  geom_ribbon(data= rr_summaries[anx == 0, ], aes(x=time, ymin=lower, ymax=upper),  fill="blue", alpha=.5) +
  geom_line(size=1) +
  geom_line(data=rr_summaries[anx == 1, ], aes(x=time, y=rr), color="black", size=1)+
  geom_ribbon(data= rr_summaries[anx == 1, ], aes(x=time, ymin=lower, ymax=upper),  fill="lightgrey", alpha=.5) +
  ylab("Relative risk") +
  xlab("Follow-up (years)") +
  theme_minimal() +
  scale_x_continuous(expand=c(0,0), breaks = seq(5, 60, 5))+
  theme(axis.line=element_line(colour="black")) +
  geom_point(data=used_data[w ==1 & cv_anx == 0,], aes(x=time, y=rr_trained) , color="blue", size=used_data[w ==1 & cv_anx == 0, 1/rr_trained_se], shape=16) +
  geom_point(data=used_data[w ==1 & cv_anx == 1,], aes(x=time, y=rr_trained) , color="black", size=used_data[w ==1 & cv_anx == 1, 1/rr_trained_se], shape=16) +
  geom_point(data=used_data[w ==0,], aes(x=time, y=rr_trained) , color="red", size=used_data[w ==0, 1/rr_trained_se], shape=16) +
  geom_point(data=used_data[w ==1 & cv_anx == 0,], aes(x=time, y=rr) , color="blue", size=1/used_data[w ==1  & cv_anx == 0, rr_se], shape=1) +
  geom_point(data=used_data[w ==1 & cv_anx == 1,], aes(x=time, y=rr) , color="black", size=1/used_data[w ==1 & cv_anx == 1, rr_se], shape=1) +
  geom_point(data=used_data[w ==0,], aes(x=time, y=rr) , color="red", size=1/used_data[w ==0, rr_se], shape=1) +
  geom_hline(yintercept=1, linetype="dashed", color="dark grey", size=1)
plot

# 4b)	Ensemble spline model: Anxiety disorders --------------------------------------------------------------------

mr_dataset_anx <- MRData()
mr_dataset_anx$load_df(
  data = dataset[cv_anx == 1,], col_obs = "log_effect_size", col_obs_se = "log_effect_size_se",
  col_covs = as.list(selected_covs[selected_covs != "intercept"]), col_study_id = "cohort" )

start <- (quantile(dataset$time, 0.1) - 0)/(max(dataset$time)-0)
end <- (quantile(dataset$time, 0.9) - 0)/(max(dataset$time)-0)
min_size <- (end-start)*0.2
interval_sizes <- rbind(c(min_size, 1.), c(min_size, 1.), c(min_size, 1.), c(min_size, 1.))
knot_bounds <- rbind(c(start, end), c(start, end),c(start, end))

knots_samples <- utils$sample_knots(
  num_intervals = 4L,
  knot_bounds = knot_bounds,
  interval_sizes = interval_sizes,
  num_samples = 50L
)

ensemble_cov_model <- LogCovModel(
  alt_cov = "time",
  use_spline = TRUE,
  spline_degree = 3L,
  spline_knots_type = 'domain',
  spline_r_linear = F,
  prior_spline_funval_uniform = array(c(-1 + 1e-6, 19)),
  prior_spline_num_constraint_points = 100L,
  prior_spline_maxder_gaussian = rbind(c(0,0,0,0), c(0.001, 0.01,0.01,Inf)),
  prior_spline_derval_uniform = array(c(-abs(slope), abs(slope))),
  spline_knots = array(seq(0, 1, by = 0.25)),
  prior_spline_monotonicity = 'decreasing'
)

cov_list <- list(LinearCovModel("intercept", use_re = TRUE), LinearCovModel("percent_female", use_re = F, prior_beta_gaussian = array(c(betas[cov == "percent_female",coef], betas[cov == "percent_female",se]))))
for(c in bias_covs[bias_covs != "cv_low_threshold_bullying"]){cov_list <- c(cov_list, list(LinearCovModel(c, use_re = F, prior_beta_gaussian = array(c(betas[cov ==c,coef], betas[cov == c,se])), prior_gamma_gaussian = array(c(0, 0.1*beta_std)))))}

model_anx <- MRBeRT(
  data = mr_dataset_anx,
  ensemble_cov_model = ensemble_cov_model,
  ensemble_knots = knots_samples,
  cov_models = cov_list,
  inlier_pct = 0.9)

model_anx$fit_model(inner_print_level = 5L, inner_max_iter = 500L)

samples_anx <- model_anx$sample_soln(sample_size = 1000L)

## Plot

# Get dataframe of outliered data for ensemble models
used_data <- as.data.table(model_anx$data$to_df())
used_data[, seq := 1:.N]
num_sub <- 1:model_anx$num_sub_models # number of submodels
long_df <- rbindlist(lapply(num_sub, function(sub){
  data_temp <- copy(used_data)
  data_temp[, sub_model := sub]
  data_temp <- cbind(data_temp, data.frame(w = model_anx$sub_models[[sub]]$lt$w))
  data_temp[, sub_model_w := model_anx$weights[sub]]
  data_temp[, sub_inlier := w*sub_model_w]
  return(data_temp)
}))
long_df[, inlier := sum(sub_inlier), by = "seq"]
used_data <- merge(used_data, unique(long_df[,.(seq, inlier)]), by = "seq")
used_data[, outlier := ifelse(inlier > 0.6, 0, 1)]

eval(parse(text = paste0("predict_matrix <- data.table(intercept = 0, time = c(0:45), percent_female = 0.5, ", paste0(paste0(bias_covs[bias_covs != "cv_low_threshold_bullying"]), "=0", collapse = ", "), ")")))
predict_data <- MRData()
predict_data$load_df(
  data = predict_matrix,
  col_covs=as.list(selected_covs[!(selected_covs %in% c("intercept", "cv_anx", "cv_low_threshold_bullying"))]))


get_gamma_sd <- function(model){
  gamma <- model$gamma_soln
  gamma_fisher <- model$lt$get_gamma_fisher(gamma)
  return(sqrt(diag(solve(gamma_fisher))))
}
gamma_soln <- rep(0, model_anx$sub_models[[1]]$num_z_vars)
for (i in 1:model_anx$num_sub_models) {
  sub_gamma <- model_anx$sub_models[[i]]$gamma_soln
  sub_gamma_sd <- get_gamma_sd(model_anx$sub_models[[i]])
  gamma_soln <- gamma_soln + model_anx$weights[[i]]*(sub_gamma + 2.0*sub_gamma_sd)
}
gamma_samples_anx <- samples_anx[[2]]
for(i in c(1:50)){
  if(nrow(gamma_samples_anx[[i]]) > 0){
    for(n in c(1:nrow(gamma_samples_anx[[i]]))){
      gamma_samples_anx[[i]][[n]] <- gamma_soln
    }
  }
}

draws <- model_anx$create_draws(
  data = predict_data,
  beta_samples = samples_anx[[1]],
  gamma_samples = gamma_samples_anx,
  random_study = TRUE )
predict_matrix$pred <- model_anx$predict(data = predict_data)
predict_matrix$pred_lo <- apply(draws, 1, function(x) quantile(x, 0.025))
predict_matrix$pred_hi <- apply(draws, 1, function(x) quantile(x, 0.975))
rr_summaries <- copy(predict_matrix)
rr_summaries[, anx := 1]
rr_summaries[, `:=` (rr = exp(pred), lower = exp(pred_lo), upper = exp(pred_hi))]
rr_summaries <- rr_summaries[rr >= 1 | time <= dataset[,max(time)+1],]

eval(parse(text = paste0("crosswalk_m <- expand.grid(", paste0("intercept = 0, time = 0, percent_female = 0, ", paste0(paste0(bias_covs[bias_covs != "cv_low_threshold_bullying"]), "=c(0, 1)", collapse = ", "), collapse = ""), ")")))
crosswalk_d <- MRData()
crosswalk_d$load_df(
  data = crosswalk_m,
  col_covs=as.list(selected_covs[!(selected_covs %in% c("intercept", "cv_anx", "cv_low_threshold_bullying"))]))

cw_draws <- model_anx$create_draws(
  data = crosswalk_d,
  beta_samples = samples_anx[[1]],
  gamma_samples = gamma_samples_anx,
  random_study = F )

crosswalk_m$Y_mean <- model_anx$predict(crosswalk_d) - model_anx$predict(crosswalk_d)[1]
crosswalk_m$Y_mean_lo <- apply(cw_draws-mean(cw_draws[1,]) , 1, function(x) quantile(x, 0.025))
crosswalk_m$Y_mean_hi <- apply(cw_draws-mean(cw_draws[1,]), 1, function(x) quantile(x, 0.975))
crosswalk_m <- data.table(crosswalk_m)[, `:=` (intercept = NULL, time = NULL)]
crosswalk_m[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]

used_data <- data.table(merge(used_data, crosswalk_m, by = bias_covs[bias_covs != "cv_low_threshold_bullying"], all.x = T))

used_data[as.numeric(Y_mean) != as.numeric(crosswalk_m$Y_mean[1]), `:=` (log_rr_trained = obs - Y_mean, log_rr_trained_se = sqrt(obs_se^2 + Y_se^2))]
used_data[is.na(log_rr_trained), `:=` (log_rr_trained = obs, log_rr_trained_se = obs_se)]
used_data[, `:=` (log_rr_trained_lower = log_rr_trained - 1.96*log_rr_trained_se, log_rr_trained_upper = log_rr_trained + 1.96*log_rr_trained_se)]
used_data[, `:=` (rr_trained = exp(log_rr_trained), lower_trained = exp(log_rr_trained_lower), upper_trained = exp(log_rr_trained_upper))]
used_data[, `:=` (rr = exp(obs), rr_se = (exp(obs + obs_se*1.96) - exp(obs - obs_se*1.96))/3.92, rr_trained_se = (upper_trained - lower_trained)/3.92)]
used_data[, w := ifelse(outlier == 0, 1, 0)]

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
  geom_point(data=used_data[w ==1 & cv_anx == 0,], aes(x=time, y=rr_trained) , color="blue", size=used_data[w ==1 & cv_anx == 0, 1/rr_trained_se], shape=16) +
  geom_point(data=used_data[w ==1 & cv_anx == 1,], aes(x=time, y=rr_trained) , color="black", size=used_data[w ==1 & cv_anx == 1, 1/rr_trained_se], shape=16) +
  geom_point(data=used_data[w ==0,], aes(x=time, y=rr_trained) , color="red", size=used_data[w ==0, 1/rr_trained_se], shape=16) +
  geom_point(data=used_data[w ==1 & cv_anx == 0,], aes(x=time, y=rr) , color="blue", size=1/used_data[w ==1  & cv_anx == 0, rr_se], shape=1) +
  geom_point(data=used_data[w ==1 & cv_anx == 1,], aes(x=time, y=rr) , color="black", size=1/used_data[w ==1 & cv_anx == 1, rr_se], shape=1) +
  geom_point(data=used_data[w ==0,], aes(x=time, y=rr) , color="red", size=1/used_data[w ==0, rr_se], shape=1) +
  geom_hline(yintercept=1, linetype="dashed", color="dark grey", size=1)
plot

# 5)	Evidence score : Use the  scorelator function with default settings: --------
# load the Scorelator
repl_python()
scorelator_module <- import("mrtool.evidence_score.scorelator")

# get evidence score
eval(parse(text = paste0("predict_matrix <- data.table(intercept = 1, time = c(0:rr_summaries[,max(time)+1]), percent_female = 0.5, ", paste0(paste0(bias_covs), "=0", collapse = ", "), ")")))

predict_data_mdd <- MRData()
predict_data_mdd$load_df(
  data = predict_matrix,
  col_covs=as.list(selected_covs[!(selected_covs %in% c("intercept", "cv_anx"))]))
draws_mdd <- model_mdd$create_draws(
  data = predict_data_mdd,
  beta_samples = samples_mdd[[1]],
  gamma_samples = samples_mdd[[2]],
  random_study = TRUE )


tdraws_mdd <- t(draws_mdd)
scorelator_mdd <- scorelator_module$Scorelator(
  ln_rr_draws = tdraws_mdd,
  exposures = predict_matrix[, time],
  exposure_domain=array(c(0.1, 0.9)*rr_summaries[,max(time)]),
  score_type = "area"
)
score <- scorelator_mdd$get_evidence_score()
score

score <- scorelator_mdd$get_evidence_score(
path_to_diagnostic = "/FILEPATH/diag_mdd.pdf"
)

eval(parse(text = paste0("predict_matrix <- data.table(intercept = 1, time = c(0:rr_summaries[,max(time)+1]), percent_female = 0.5, ", paste0(paste0(bias_covs[bias_covs != "cv_low_threshold_bullying"]), "=0", collapse = ", "), ")")))
predict_data_anx <- MRData()
predict_data_anx$load_df(
  data = predict_matrix,
  col_covs=as.list(selected_covs[!(selected_covs %in% c("intercept", "cv_anx", "cv_low_threshold_bullying"))]))
draws_anx <- model_anx$create_draws(
  data = predict_data_anx,
  beta_samples = samples_anx[[1]],
  gamma_samples = samples_anx[[2]],
  random_study = TRUE )

tdraws_anx <- t(draws_anx)
scorelator_anx <- scorelator_module$Scorelator(
  ln_rr_draws = tdraws_anx,
  exposures = predict_matrix[, time],
  exposure_domain=array(c(0.1, 0.9)*35),
  score_type = "area"
)

score <- scorelator_anx$get_evidence_score()
score <- scorelator_anx$get_evidence_score(
  path_to_diagnostic = "/FILEPATH/diag_anx.pdf"
)
score

# Create draws for paf estimation-----------------------------------------

## MDD
eval(parse(text = paste0("predict_matrix <- data.table(intercept = 1, time = c(0:60), percent_female = c(rep(0, 61), rep(1, 61)), ", paste0(paste0(bias_covs), "=0", collapse = ", "), ")")))
predict_data <- MRData()
predict_data$load_df(
  data = predict_matrix,
  col_covs=as.list(selected_covs[!(selected_covs %in% c("intercept", "cv_anx"))]))

get_gamma_sd <- function(model){
  gamma <- model$gamma_soln
  gamma_fisher <- model$lt$get_gamma_fisher(gamma)
  return(sqrt(diag(solve(gamma_fisher))))
}

gamma_soln <- rep(0, model_mdd$sub_models[[1]]$num_z_vars)
for (i in 1:model_mdd$num_sub_models) {
  sub_gamma <- model_mdd$sub_models[[i]]$gamma_soln
  sub_gamma_sd <- get_gamma_sd(model_mdd$sub_models[[i]])
  gamma_soln <- gamma_soln + model_mdd$weights[[i]]*(sub_gamma + 2.0*sub_gamma_sd)
}

gamma_sample_sizes <-samples_mdd[[2]]
for(i in c(1:50)){
  if(nrow(gamma_sample_sizes[[i]]) > 0){
    for(n in c(1:nrow(gamma_sample_sizes[[i]]))){
      gamma_sample_sizes[[i]][[n]] <- gamma_soln
    }
  }
}


draws <- model_mdd$create_draws(
  data = predict_data,
  beta_samples = samples_mdd[[1]],
  gamma_samples = gamma_sample_sizes,
  random_study = TRUE )
predict_matrix$pred <- model_mdd$predict(data = predict_data)
draws <- data.table(draws)
names(draws) <- paste0("draw_", 0:999)

predict_matrix_mdd <- predict_matrix[,.(cv_anx = 0, sex_id = percent_female + 1, time, mean_rr = pred)]
predict_matrix_mdd <- cbind(predict_matrix_mdd, draws)

## Anxiety
gamma_soln <- rep(0, model_anx$sub_models[[1]]$num_z_vars)
for (i in 1:model_anx$num_sub_models) {
  sub_gamma <- model_anx$sub_models[[i]]$gamma_soln
  sub_gamma_sd <- get_gamma_sd(model_anx$sub_models[[i]])
  gamma_soln <- gamma_soln + model_anx$weights[[i]]*(sub_gamma + 2.0*sub_gamma_sd)
}

gamma_sample_sizes <-samples_anx[[2]]
for(i in c(1:50)){
  if(nrow(gamma_sample_sizes[[i]]) > 0){
    for(n in c(1:nrow(gamma_sample_sizes[[i]]))){
      gamma_sample_sizes[[i]][[n]] <- gamma_soln
    }
  }
}

eval(parse(text = paste0("predict_matrix <- data.table(intercept = 1, time = c(0:60), percent_female = c(rep(0, 61), rep(1, 61)), ", paste0(paste0(bias_covs[bias_covs != "cv_low_threshold_bullying"]), "=0", collapse = ", "), ")")))
predict_data <- MRData()
predict_data$load_df(
  data = predict_matrix,
  col_covs=as.list(selected_covs[!(selected_covs %in% c("intercept", "cv_anx", "cv_low_threshold_bullying"))]))
draws <- model_anx$create_draws(
  data = predict_data,
  beta_samples = samples_anx[[1]],
  gamma_samples = gamma_sample_sizes,
  random_study = TRUE )
predict_matrix$pred <- model_anx$predict(data = predict_data)
draws <- data.table(draws)
names(draws) <- paste0("draw_", 0:999)

predict_matrix_anx <- predict_matrix[,.(cv_anx = 1, sex_id = percent_female + 1, time, mean_rr = pred)]
predict_matrix_anx <- cbind(predict_matrix_anx, draws)

## Prep draws and save csv
predict_matrix <- rbind(predict_matrix_mdd, predict_matrix_anx)
predict_matrix <- melt.data.table(predict_matrix, id.vars = names(predict_matrix)[!(names(predict_matrix) %like% "draw")], value.name = "rr", variable.name="draw")
write.csv(predict_matrix, "/FILEPATH/rr_mrt_brt_GBD2020.csv", row.names = F)


## for SEVs
sev_template <- fread("/FILEPATH/bullying.csv")
for_sev <- predict_matrix[time == 0,.(draw = as.numeric(gsub("draw_", "", draw)), rr = exp(rr), cause_id = ifelse(cv_anx == 0, 568, 571), age_group_id = 9999, sex_id)]


for(age in c(c(2:3, 388:389, 238, 34), c(6:20, 30:32, 235))){
  for_sev_temp <- for_sev[age_group_id == 9999,]
  for_sev_temp[, age_group_id := age]
  for_sev <- rbind(for_sev, for_sev_temp)
}
for_sev <- for_sev[age_group_id != 9999,]

write.csv(for_sev, "/FILEPATH/bullying.csv", row.names = F, na = "")
