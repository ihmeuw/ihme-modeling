#---------------------------------------------------
# Purpose: create draws for all RO pairs
# Date: 04/01/2021
#---------------------------------------------------

# unpack information from results and create draws
rm(list = ls())
library(mrbrt001, lib.loc = FILEPATH)

# define functions
# =================================================================================================
get_cov_names <- function(signal_model) {
  cov_model <- signal_model$sub_models[[1]]$cov_models[[1]]
  list(alt_covs = cov_model$alt_cov,
       ref_covs = cov_model$ref_cov)
}

get_risk_limits <- function(signal_model) {
  cov_names <- get_cov_names(signal_model)
  risk_data <- signal_model$data$get_covs(unlist(cov_names))
  c(min(risk_data), max(risk_data))
}

get_signal <- function(signal_model, risk) {
  cov_names <- get_cov_names(signal_model)
  risk_limits <- get_risk_limits(signal_model)
  df_covs <- data.frame(
    c(sapply(cov_names$ref_covs, function(x) rep(risk_limits[1], length.out = length(risk)),
             simplify = FALSE, USE.NAMES = TRUE),
      sapply(cov_names$alt_covs, function(x) risk,
             simplify = FALSE, USE.NAMES = TRUE))
  )
  data <- MRData()
  data$load_df(df_covs, col_covs=unlist(cov_names))
  signal_model$predict(data)
}

get_beta <- function(linear_model) {
  beta <- linear_model$beta_soln
  names(beta) <- linear_model$cov_names
  specs <- mrbrt001::core$other_sampling$extract_simple_lme_specs(linear_model)
  beta_hessian <- mrbrt001::core$other_sampling$extract_simple_lme_hessian(specs)
  beta_sd <- 1/sqrt(diag(beta_hessian))
  names(beta_sd) <- linear_model$cov_names
  c(beta["signal"], beta_sd["signal"])
}

get_gamma <- function(linear_model) {
  gamma <- linear_model$gamma_soln[[1]]
  gamma_fisher <- linear_model$lt$get_gamma_fisher(linear_model$gamma_soln)
  gamma_sd <- 1/sqrt(diag(gamma_fisher))
  c(gamma, gamma_sd)
}

get_soln <- function(linear_model) {
  list(
    beta_soln = get_beta(linear_model),
    gamma_soln = get_gamma(linear_model)
  )
}

get_ln_rr_draws <- function(signal_model,
                            linear_model,
                            risk,
                            num_draws = 1000L,
                            normalize_to_tmrel = FALSE,
                            include_re = TRUE) {
  # set seed inside function
  set.seed(1234)
  
  signal <- get_signal(signal_model, risk)
  re_signal <- signal
  soln <- get_soln(linear_model)
  
  fe_samples <- rnorm(num_draws, mean=soln$beta[1], sd=soln$beta[2])
  re_samples <- rnorm(num_draws, mean=0, sd=sqrt(soln$gamma[1] + 2*soln$gamma[2]))
  
  draws <- outer(signal, fe_samples)
  if (include_re) {
    draws <- draws + outer(re_signal, re_samples)
  }
  
  if (normalize_to_tmrel) {
    tmrel_index <- which.min(signal)
    draws <- apply(draws, 2, function(x) x - x[tmrel_index])
  }
  
  df <- as.data.frame(cbind(risk, draws))
  names(df) <- c("risk", sapply(1:num_draws, function(i) paste0("draw_", i)))
  return(df)
}

# process results
# =================================================================================================
level_100 <- T

if(level_100){
  save_dir <- FILEPATH
} else {
  save_dir <- FILEPATH
}

pkl_dir <- FILEPATH

rr_files <- list.files(pkl_dir)
rr_files <- rr_files[grepl("smoking_",rr_files) & !grepl("fracture",rr_files)]

# load models

for (ro in rr_files) {
  print(ro)
  signal_model_path <- paste0(FILEPATH,ro,"/signal_model.pkl")
  linear_model_path <- paste0(FILEPATH,ro,"/new_linear_model.pkl")
  
  signal_model <- py_load_object(filename = signal_model_path, pickle = "dill")
  linear_model <- py_load_object(filename = linear_model_path, pickle = "dill")
  
  # specify risk, you need to input the exposures that you want to predict
  if(level_100){
    risk <- seq(from=0, to=100, by=1)
  } else{
    risk <- seq(from=0, to=99.9, by=0.1)
  } 
  
  # get_draws
  set.seed(123)
  df_re <- get_ln_rr_draws(signal_model,
                        linear_model,
                        risk,
                        num_draws = 1000L,
                        normalize_to_tmrel = FALSE,
                        include_re = T)
  
  write.csv(df_re, paste0(FILEPATH,ro,".csv"), row.names = F)
  
  df_fe <- get_ln_rr_draws(signal_model,
                           linear_model,
                           risk,
                           num_draws = 1000L,
                           normalize_to_tmrel = FALSE,
                           include_re = F)
  write.csv(df_fe, paste0(FILEPATH,ro,"_fe.csv"), row.names = F)
  
  # visual check draws
  draws <- df_re[, 2:ncol(df_re)]
  draw_mean <- apply(draws, 1, function(x) mean(x))
  draw_lower <- apply(draws, 1, function(x) quantile(x, probs=.025))
  draw_upper <- apply(draws, 1, function(x) quantile(x, probs=.975))
  
  summary_rr <- cbind(risk, round(exp(draw_mean), digits = 2), round(exp(draw_lower), digits = 2), round(exp(draw_upper), digits = 2))
  write.csv(summary_rr, paste0(FILEPATH,ro,'.csv'), row.names = F)
  
  # traditional 95% CI
  draws_fe <- df_fe[, 2:ncol(df_fe)]
  draw_mean_fe <- apply(draws_fe, 1, function(x) mean(x))
  draw_lower_fe <- apply(draws_fe, 1, function(x) quantile(x, probs=.025))
  draw_upper_fe <- apply(draws_fe, 1, function(x) quantile(x, probs=.975))
  
  summary_rr_fe <- cbind(risk, round(exp(draw_mean_fe), digits = 2), round(exp(draw_lower_fe), digits = 2), round(exp(draw_upper_fe), digits = 2))
  write.csv(summary_rr_fe, paste0(FILEPATH,ro,'_fe.csv'), row.names = F)
  

}

# for fractures
fracture_model_path <- paste0(FILEPATH,"smoking_hip_fracture","/model.pkl")
fracture_model <- py_load_object(filename = fracture_model_path, pickle = "dill")

n_samples <- 1000L
samples <- fracture_model$sample_soln(sample_size=n_samples)
pred_data <- as.data.table(expand.grid("intercept"=c(1), "cv_adj_L1"=0))

data_pred <- MRData()
data_pred$load_df(
  data = pred_data,
  col_covs = as.list("cv_adj_L1")
)

set.seed(123)
draws <- fracture_model$create_draws(
  data = data_pred, 
  beta_samples = samples[[1]],
  gamma_samples = samples[[2]],
  random_study=TRUE)

draws <- cbind(0:999, as.vector(draws)) %>% data.table
names(draws) <- c("draw", "rr")

write.csv(draws, paste0(FILEPATH,"smoking_fractures",".csv"), row.names = F)

pred_data$Y_mean <- fracture_model$predict(
  data = data_pred, 
  predict_for_study = T, 
  sort_by_data_id = T) 







