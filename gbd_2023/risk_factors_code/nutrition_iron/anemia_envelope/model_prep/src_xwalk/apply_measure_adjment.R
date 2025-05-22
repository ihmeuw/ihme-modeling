
# load in libraries -------------------------------------------------------

library(data.table)

# load in mr-brt environment ----------------------------------------------

reticulate::use_python("FILEPATH")
mr <- reticulate::import("mrtool")

# impute all missing measures ---------------------------------------------

impute_missing_measure <- function(input_df, bundle_map, anemia_map, bundle_dir){
  df <- copy(input_df)
  
  i_vec <- which(df$imputed_measure_row == 1 & is.na(df$imputed_elevation_adj_row))
  inverse_i_vec <- setdiff(seq_len(nrow(df)), i_vec)
  to_impute_df <- df[i_vec, ]
  df <- df[inverse_i_vec, ]
  
  to_impute_df <- mrbrt_adj_measure(
    input_df = to_impute_df,
    bundle_map = bundle_map,
    anemia_map = anemia_map,
    bundle_dir = bundle_dir
  )
  
  df <- rbindlist(
    list(df, to_impute_df),
    use.names = TRUE,
    fill = TRUE
  )
  
  return(df)
}

mrbrt_adj_measure <- function(input_df, bundle_map, anemia_map, bundle_dir){
  df <- copy(input_df)
  
  measure_combo_vec <- unique(df$var.reference_measure)
  measure_combo_vec <- measure_combo_vec[!(is.na(measure_combo_vec))]
  
  df <- drop_ref_measure_cols(input_df = df)
  df$measure_row_id <- as.character(df$measure_row_id)
  
  final_df <- data.table()
  
  for(x in measure_combo_vec){
    bun_id <- as.character(bundle_map[var == x & cv_pregnant == unique(df$cv_pregnant), id])
    print(paste(x, unique(df$cv_pregnant)))
    adj_file_name <- file.path(bundle_dir, bun_id, 'age_sex_split_mrbrt.csv')
    print(adj_file_name)
    adj_df <- fread(adj_file_name)
    adj_df <- merge.data.table(
      x = adj_df,
      y = anemia_map,
      by = c('age_group_id', 'sex_id', 'cv_pregnant')
    )
    
    adj_df$row_id <- as.character(adj_df$row_id)
    
    df_w_adj <- merge.data.table(
      x = df,
      y = adj_df[,.(age_group_id, age_start, age_end, sex_id, anemia_category, row_id, var, val, standard_error, variance, upper, lower, sample_size, group, specificity, group_review, orig_sample_size)],
      by.x = 'measure_row_id',
      by.y = 'row_id',
      suffixes = c('', '.reference_measure')
    )
    
    df_w_adj$age_group_id <- df_w_adj$age_group_id.reference_measure
    df_w_adj$age_start <- df_w_adj$age_start.reference_measure
    df_w_adj$age_end <- df_w_adj$age_end.reference_measure
    df_w_adj$sex_id <- df_w_adj$sex_id.reference_measure
    df_w_adj$sample_size <- df_w_adj$sample_size.reference_measure
    df_w_adj$orig_sample_size <- df_w_adj$orig_sample_size.reference_measure
    df_w_adj$group <- df_w_adj$group.reference_measure
    df_w_adj$specificity <- df_w_adj$specificity.reference_measure
    df_w_adj$group_review <- df_w_adj$group_review.reference_measure
    
    df_w_adj <- apply_mrbrt_measure_adj(input_df = df_w_adj)
    if(nrow(df_w_adj) > 0){
      df_w_adj <- transform_predicted_values(
        input_df = df_w_adj, 
        alt_var_measure_type = bundle_map[var == unique(df$var) & cv_pregnant == unique(df$cv_pregnant), measure]
      )
    }
    
    final_df <- rbindlist(
      list(final_df, df_w_adj),
      use.names = TRUE,
      fill = TRUE
    )
  }
  
  return(final_df)
}

apply_mrbrt_measure_adj <- function(input_df){
  df <- copy(input_df)
  
  ref_var <- unique(as.character(df$var.reference_measure))
  alt_var <- unique(as.character(df$var))
  anemia_category <- unique(df$anemia_category)
  
  print(paste(ref_var, alt_var, anemia_category, sep = "-"))
  
  setnames(df, 'val.reference_measure', 'mean.ref')
  
  mrbrt_dir <- 'FILEPATH'
  
  pkl_list <- list.files(
    path = mrbrt_dir,
    pattern = "\\.pkl$",
    full.names = TRUE
  )
  
  final_df <- data.table()
  
  for(cat in anemia_category){
    
    message(cat)
    
    subset_df <- df[anemia_category == cat]
    
    file_string <- paste(ref_var, alt_var, cat, sep = '-')
    print(file_string)
    pkl_file <- pkl_list[grepl(file_string, pkl_list)]
    print(pkl_file)
  
    if(length(pkl_file) == 1 && file.exists(pkl_file)){
      fit1 <- reticulate::py_load_object(pkl_file)
      
      dat_pred <- mr$MRData()
      dat_pred$load_df(data = subset_df, col_covs = list('mean.ref'))
      
      subset_df$pred_mean <- fit1$predict(dat_pred, predict_for_study = F)
      
      # create draws for both FE -----------------------------------------
      
      n_samples <- 100L
      
      reticulate::py_set_seed(123)
      
      mrbrt_samples <- fit1$sample_soln(sample_size = n_samples)
      
      draws_fe_re <- fit1$create_draws(
        data = dat_pred,
        beta_samples = mrbrt_samples[[1]],
        gamma_samples = matrix(rep(0, n_samples * fit1$num_z_vars), ncol = fit1$num_z_vars),
        random_study = FALSE 
      ) 
      
      subset_df$pred2_lo <- apply(
        draws_fe_re, 1, function(x) quantile(x, 0.025)
      )
      subset_df$pred2_up <- apply(
        draws_fe_re, 1, function(x) quantile(x, 0.975)
      )
      subset_df$draw_se_re <- (subset_df$pred2_up - subset_df$pred2_lo) / 
        (qnorm(0.975)*2)
      
      subset_df$temp_row_id <- seq_len(nrow(subset_df))
      template_df <- subset_df[,.(temp_row_id, mean.ref, standard_error.reference_measure)]
      if(grepl('hemog', ref_var)) {
        source(file.path(getwd(), "model_prep/src_xwalk/hb_model_ensemble_draws.R"))
        draw_list <- furrr::future_map2(template_df$mean.ref, template_df$standard_error.reference_measure, \(m, v) {
          hb_draws <- withr::with_seed(
            seed = 123,
            code = hb_me_ensemble_draws(n_samples, m, v ^ 2)
          )
          temp_df <- data.frame(mean.ref = hb_draws)
          
          reticulate::py_set_seed(123)
          dat_pred <- mr$MRData()
          dat_pred$load_df(data = temp_df, col_covs = list('mean.ref'))
          
          temp_df$pred_mean <- fit1$predict(dat_pred, predict_for_study = F) 
          temp_df$pred_mean <- nch::inv_logit(temp_df$pred_mean)
          upper <- quantile(temp_df$pred_mean, 0.975, na.rm = TRUE)
          lower <- quantile(temp_df$pred_mean, 0.025, na.rm = TRUE)
          data.table(new_se = (upper - lower) / (qnorm(0.975) * 2))
        }, .options = furrr::furrr_options(seed = TRUE)) |>
          rbindlist()
        
      } else {
        draw_list <- furrr::future_map2(template_df$mean.ref, template_df$standard_error.reference_measure, \(m, v) {
          anemia_draws <- withr::with_seed(
            seed = 123,
            code = rnorm(n_samples, m, v)
          )
          temp_df <- data.frame(mean.ref = anemia_draws)
          
          reticulate::py_set_seed(123)
          dat_pred <- mr$MRData()
          dat_pred$load_df(data = temp_df, col_covs = list('mean.ref'))
          
          temp_df$pred_mean <- fit1$predict(dat_pred, predict_for_study = F) 
          temp_df$pred_mean <- exp(temp_df$pred_mean)
          upper <- quantile(temp_df$pred_mean, 0.975)
          lower <- quantile(temp_df$pred_mean, 0.025)
          data.table(new_se = (upper - lower) / (qnorm(0.975) * 2))
        }, .options = furrr::furrr_options(seed = TRUE)) |>
          rbindlist()
      }
      subset_df$ref_draw_se <- draw_list$new_se
    }
    
    final_df <- rbindlist(
      list(final_df, subset_df),
      use.names = TRUE,
      fill = TRUE
    )
  }
  
  return(final_df)
}

transform_predicted_values <- function(input_df, alt_var_measure_type){
  df <- copy(input_df)
  
  if(alt_var_measure_type == 'proportion'){
    df$val <- nch::inv_logit(x = df$pred_mean)
    df$measure_standard_error <- nch::inv_logit_se(
      mean_vec = df$pred_mean,
      se_vec = df$draw_se_re
    )
  }else{
    df$val <- exp(x = df$pred_mean)
    df$measure_standard_error <- df$val * df$draw_se_re
  } 
  df$variance <- df$measure_standard_error ^ 2 + df$ref_draw_se ^ 2
  df$standard_error <- sqrt(df$variance)
  df$lower <- df$val - qnorm(0.975) * df$standard_error
  df$upper <- df$val + qnorm(0.975) * df$standard_error
  
  return(df)
}

drop_ref_measure_cols <- function(input_df){
  df <- copy(input_df)
  
  drop_cols <- colnames(df)[grepl('\\.reference_measure', colnames(df))]
  for(c in drop_cols) df[[c]] <- NULL
  
  return(df)
}