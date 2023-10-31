.prior_line_MR_BRT <- function(prior,                   # data frame with 'date' and 'val' column
                               data = NULL,             # data frame with 'date' and 'val' column
                               prior_se_mult = 150,     # strength of prior lower = more weight to prior, higher = more weight to data
                               n_knots = 10,            # Number of equidistant spline knots
                               spline_degree = 3L,      # Degree of spline terms (3L is cubic)
                               offset = 1e-5            # model offset
){
  
  if (!is.data.frame(prior)) prior <- as.data.frame(prior)
  if (!is.null(data) & !is.data.frame(data)) data <- as.data.frame(data)
  
  prior$se_mult <- prior_se_mult
  prior$x <- as.integer(prior$date)
  
  data <- data[complete.cases(data),]
  
  if (length(dim(data))) {
    
    data$se_mult <- 1
    full_data <- data.frame(x = c(prior$x, as.integer(data$date)),
                            y = c(prior$val, data$val),
                            y_se_mult = c(prior$se_mult, data$se_mult))
    
  } else {
    
    full_data <- data.frame(x = prior$x,
                            y = prior$val,
                            y_se_mult = prior$se_mult)
  }
  
  full_data$y[full_data$y <= offset] <- offset
  full_data <- full_data[complete.cases(full_data),]
  full_data$y_logit <- qlogis(full_data$y)
  full_data$y_logit_se <- full_data$y_se_mult * sqrt(1/(full_data$y*(1-full_data$y)))
  # full_data$y_logit_se <- full_data$y_se_mult
  
  dat_MR <- MRData()
  
  dat_MR$load_df(
    data = full_data,
    col_obs = "y_logit",
    col_obs_se = "y_logit_se",
    col_covs = list("x")
  )
  
  mod <- MRBRT(
    data = dat_MR,
    cov_models = list(
      LinearCovModel("intercept"),
      LinearCovModel("x",
                     use_re = FALSE,
                     use_spline = TRUE,
                     spline_knots = array(seq(0, 1, length.out = n_knots + 2)),
                     spline_degree = spline_degree,
                     spline_knots_type = "domain",
                     spline_r_linear = T,
                     spline_l_linear = T,
                     prior_spline_monotonicity = "increasing",
                     prior_spline_num_constraint_points = 100L
      )
    )
  )
  
  mod$fit_model(inner_print_level = 5L, inner_max_iter = 10000L)
  
  df_pred <- prior
  dat_pred <- MRData()
  dat_pred$load_df(
    data = df_pred,
    col_covs = list("x")
  )
  
  df_pred$logit_pred <- mod$predict(data = dat_pred)
  df_pred$pred <- plogis(df_pred$logit_pred)
  
  # plot(full_data$x, full_data$y_logit, cex = full_data$y_logit_se/10)
  # lines(df_pred$x, df_pred$logit_pred, col=2,lwd=2)
  
  return(df_pred)
}