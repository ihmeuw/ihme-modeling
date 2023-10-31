###
# Post analysis related functions
###
library(scales)

egger_regression <- function(residual, residual_sd, one_sided = TRUE) {
  weighted_residual <- residual/residual_sd
  r_mean <- mean(weighted_residual)
  r_sd <- 1/sqrt(length(weighted_residual))
  r_pval <- get_pval(r_mean, r_sd, one_sided = one_sided)
  list(mean = r_mean, sd = r_sd, pval = r_pval)
}

get_pval <- function(beta, beta_sd, one_sided = FALSE) {
  zscore <- abs(beta/beta_sd)
  if (one_sided) {
    pval <- 1 - pnorm(zscore)
  } else {
    pval <- 2*(1 - pnorm(zscore))
  }
  pval
}


extract_data_info <- function(model) {
  df <- model$data$to_df()
  df$pred <- model$beta_soln[[1]]
  df$residual <- df$obs - df$pred
  df$residual_se <- sqrt(df$obs_se^2 + model$gamma_soln[[1]])
  df$outlier <- model$get_w_soln() < 0.1
  df
}


get_df_fill <- function(df) {
  # get number of filled data points
  absr_rank <- rep(0L, length=nrow(df))
  absr_rank[order(abs(df$residual))] <- seq(1, nrow(df))
  sort_index <- order(df$residual, decreasing = mean(df$residual/df$residual_se) > 0)
  num_fill <- nrow(df) - absr_rank[tail(sort_index, n=1)]
  
  # get the fill-dataframe
  df_fill <- df[sort_index[1:num_fill],]
  df_fill$study_id <- paste0('fill_', df_fill$study_id)
  df_fill$residual <- - df_fill$residual
  df_fill$obs <- df_fill$pred + df_fill$residual
  df_fill
}

get_gamma_sd <- function(model){
  gamma <- model$gamma_soln
  gamma_fisher <- model$lt$get_gamma_fisher(gamma)
  return(1/sqrt(gamma_fisher[1,1]))
}

get_beta_sd <- function(model){
  model_specs <- mrbrt001::core$other_sampling$extract_simple_lme_specs(model)
  beta_hessian <- mrbrt001::core$other_sampling$extract_simple_lme_hessian(model_specs)
  beta_sd <- sqrt(diag(solve(beta_hessian)))
  names(beta_sd) <- model$cov_names
  return(beta_sd[["intercept"]])
}


get_uncertainty_info <- function(model){
  beta <- model$beta_soln
  names(beta) <- model$cov_names
  beta <- beta[["intercept"]]
  gamma <- model$gamma_soln[[1]]
  beta_sd <- get_beta_sd(model)
  gamma_sd <- get_gamma_sd(model)
  
  inner_draws <- c(qnorm(0.05, mean = beta, sd = sqrt(beta_sd^2 + gamma)),
                   qnorm(0.95, mean = beta, sd = sqrt(beta_sd^2 + gamma)))
  outer_draws <- c(qnorm(0.05, mean = beta, sd = sqrt(beta_sd^2 + gamma + 2*gamma_sd)),
                   qnorm(0.95, mean = beta, sd = sqrt(beta_sd^2 + gamma + 2*gamma_sd)))
  score <- 0.5*sign(beta)*ifelse(beta > 0, outer_draws[1], outer_draws[2])
  list(score = score,
       inner_draws = inner_draws,
       outer_draws = outer_draws)
}

plot_model <- function(df,
                       uncertainty_info,
                       model,
                       uncertainty_info_fill,
                       model_fill,
                       title) {
  # extract information
  beta <- model$beta_soln
  names(beta) <- model$cov_names
  beta <- beta[["intercept"]]
  max_se <- quantile(df$residual_se, 0.99)
  df_fill <- df[grepl('fill', df$study_id, fixed=TRUE),]
  # plot data
  plot(df$obs, df$residual_se,
       pch = 19, col = alpha('gray', 0.6),
       ylim = c(max_se, 0), xlim = c(-2*max_se + beta, 2*max_se + beta),
       yaxs = 'i', xlab = "observations", ylab = "overall_se", main = title)
  
  # plot filled data
  if (nrow(df_fill) > 0){
    points(df_fill$obs, df_fill$residual_se, col=alpha('#008080', 0.6), pch=19)
  }
  
  # plot trimmed data
  if (sum(df$outlier) > 0) {
    df_outlier <- df[df$outlier,]
    points(df_outlier$obs, df_outlier$residual_se, col=alpha('red', 0.6), pch=4)
  }
  
  # plot funnel boundary
  x <- c(beta, -1.96*max_se + beta, 1.96*max_se + beta)
  y <- c(0, max_se, max_se)
  polygon(x, y, col=alpha('#B0E0E6', 0.4))
  lines(c(beta, beta), c(0, max_se), lty='dashed')
  lines(c(beta, -1.96*max_se + beta), c(0, max_se), col='#87CEFA')
  lines(c(beta, 1.96*max_se + beta), c(0, max_se), col='#87CEFA')
  
  # plot 0
  lines(c(0, 0), c(0, max_se), lty = "dashed", col = "red")
  
  # plot score lines
  lines(rep(uncertainty_info$inner_draws[1], 2), c(0, max_se), col = "#69b3a2")
  lines(rep(uncertainty_info$inner_draws[2], 2), c(0, max_se), col = "#69b3a2")
  lines(rep(uncertainty_info$outer_draws[1], 2), c(0, max_se), col = "#256b5f")
  lines(rep(uncertainty_info$outer_draws[2], 2), c(0, max_se), col = "#256b5f")
}

summarize_model <- function(ro_pair,
                            model,
                            model_fill,
                            egger_model,
                            egger_model_all,
                            uncertainty_info) {
  summary <- list(
    ro_pair = ro_pair,
    num_fill = num_fill, 
    gamma = model$gamma_soln[[1]], 
    gamma_sd = get_gamma_sd(model),
    score = uncertainty_info$score
  )
  
  if (!is.null(model_fill)){
    score_fill_result <- get_uncertainty_info(model_fill)
    summary$gamma_adjusted <- model_fill$gamma_soln[[1]]
    summary$gamma_sd_adjusted <- get_gamma_sd(model_fill)
    summary$score_adjusted <- score_fill_result$score
  }
  
  for (name in names(egger_model)){
    summary[[paste0("egger_",name)]] <- egger_model[[name]]
  }
  
  for (name in names(egger_model_all)){
    summary[[paste0("egger_all_",name)]] <- egger_model_all[[name]]
  }
  
  data.frame(summary)
}

get_draws <- function(model, num_draws = 1000L) {
  beta <- model$beta_soln
  names(beta) <- model$cov_names
  beta <- beta[["intercept"]]
  gamma <- model$gamma_soln[[1]]
  beta_sd <- get_beta_sd(model)
  gamma_sd <- get_gamma_sd(model)
  
  outer_fe_sd <- sqrt(beta_sd^2 + gamma + 2*gamma_sd)
  beta_samples <- rnorm(num_draws, mean = beta, sd = outer_fe_sd)
  draws <- data.frame(draw = beta_samples)
}