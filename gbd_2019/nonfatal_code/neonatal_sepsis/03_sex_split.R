#############################################################################################################
### Project: Neonatal Sepsis and Other Neonatal Infections
### Purpose: Sex-split data in MR-BRT
### Inputs: df: dataframe of bundle data to be sex split
#############################################################################################################

sex_split <- function(df){
  
  dt <- copy(df)
  
  #calculate male to female ratio
  df <- df[sex != 'Both' & measure == 'incidence' & is_outlier != 1]
  df[, year_id := floor((year_start + year_end)/2)]
  
  df <- dcast(df, nid + location_id + year_id + age_start + age_end ~ sex, value.var = c('mean', 'standard_error', 'upper', 'lower'))
  df <- df[mean_Male > 0 & mean_Female > 0]
  
  df[, ratio := (mean_Male / mean_Female)]
  df[, ratio_se := sqrt((mean_Male^2 / mean_Female^2) * (standard_error_Male^2/mean_Male^2 + standard_error_Female^2/mean_Female^2))]
  
  # log transform the meta-regression data
  df[, ratio_log := log(ratio)]
  
  df$ratio_se_log <- sapply(1:nrow(df), function(i) {
    ratio_i <- df[i, "ratio"]
    ratio_se_i <- df[i, "ratio_se"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })

  # run MR-BRT
  fit1 <- run_mr_brt(
    output_dir = paste0('FILEPATH'), 
    model_label = "sepsis_sex_ratio",
    data = df,
    mean_var = "ratio_log",
    se_var = "ratio_se_log",
    method = 'trim_maxL',
    overwrite_previous = TRUE,
    trim_pct = 0.10
  )
  
  check_for_outputs(fit1)
  
  #store the mean_effect 
  df_pred <- data.frame(intercept = 1)
  pred1 <- predict_mr_brt(fit1, newdata = df_pred, write_draws = TRUE)
  check_for_preds(pred1)
  
  #plot results
  plot_mr_brt(pred1)
  
  # load results and apply sex ratio
  pred_object <- load_mr_brt_preds(pred1)
  preds <- pred_object$model_summaries
  beta0 <- exp(preds$Y_mean)
  beta0_se <- exp((preds$Y_mean_hi - preds$Y_mean_lo) / 3.92)
  
  data <- dt[sex=="Both"&measure=="incidence"]
  
  id_vars <- c('seq', 'nid', 'location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'standard_error', 'mean', 'is_outlier')
  data <- data[, id_vars, with=FALSE]
  
  #apply mrbrt sex effect to all both sex data, in draw space
  #load 1000 draws of the sex ratio
  model_coefs <- fread('FILEPATH/model_draws.csv')
  draw_cols <- paste0("draw_", 0:999)
  effect_draws <- as.matrix(model_coefs[,draw_cols, with=FALSE])[1,]
  
  data$index <- 1:nrow(data)
  expand_draws <- as.data.table(expand.grid(index = 1:nrow(data), log_sex_effect = effect_draws))
  data_expanded <- merge(data, expand_draws, by = 'index')
  
  #sample 1000 draws of the both-sex data point, using the mean and se
  data_expanded[, prev_draw := rnorm(1000, mean = mean, sd = standard_error), by=index]
  
  #exponentiate to get out of log space
  data_expanded[, sex_effect := exp(log_sex_effect)]
  
  #apply the sex ratio to the both-sex mean
  data_expanded[, female_mean := (2*prev_draw) / (sex_effect + 1)]
  data_expanded[, male_mean := 2*prev_draw - female_mean]
  
  #calculate the new mean and std errors for each male and female data point
  data_expanded[, `:=` (female_split_mean = mean(.SD$female_mean),
                        male_split_mean = mean(.SD$male_mean),
                        female_se = sd(.SD$female_mean),
                        male_se = sd(.SD$female_mean)),
                by=index]
  id_vars <- c('seq', 'nid', 'location_id', 'year_start', 'year_end', 'age_start', 'age_end',
               'female_split_mean', 'male_split_mean', 'female_se', 'male_se', 'is_outlier')
  data_adjusted <- unique(data_expanded[, id_vars, with=FALSE])
  
  #reshape the data so males and females have separate rows
  id_vars <- c('seq', 'nid', 'location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'is_outlier')
  data_sex_spec <- melt(data_adjusted, id.vars = id_vars,
                        measure.vars = c('female_split_mean', 'male_split_mean', 'female_se', 'male_se'))
  
  data_sex_spec[grepl('male', variable), sex := 'Male']
  data_sex_spec[grepl('female', variable), sex := 'Female']
  
  data_sex_spec[grepl('mean', variable), variable := 'mean']
  data_sex_spec[grepl('se', variable), variable := 'se']
  
  data_sex_spec <- dcast(data_sex_spec, seq + nid + location_id + year_start + year_end + age_start + age_end + is_outlier + sex ~ variable,
                         value.var = 'value')
  
  
  #replace both-sex rows with these sex-specific rows, and resave the bundle
  seqs_to_replace <- unique(data_sex_spec$seq)
  data_sex_spec <- data_sex_spec[, .(seq, sex, mean, se)]
  setnames(data_sex_spec, 'se', 'standard_error')
  
  bundle <- merge(dt, data_sex_spec, by = 'seq', all.x = TRUE)
  bundle[seq %in% seqs_to_replace, sex.x := sex.y]
  bundle[seq %in% seqs_to_replace, mean.x := mean.y]
  bundle[seq %in% seqs_to_replace, standard_error.x := standard_error.y]
  
  bundle <- bundle[, -c('sex.y', 'mean.y', 'standard_error.y')]
  setnames(bundle, c('sex.x', 'mean.x', 'standard_error.x'), c('sex', 'mean', 'standard_error'))
  
  message(paste0("Data sex-split!"))
  return(bundle)
  
}