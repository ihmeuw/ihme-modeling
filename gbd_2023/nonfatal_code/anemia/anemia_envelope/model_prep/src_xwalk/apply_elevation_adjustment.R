
# load in libraries -------------------------------------------------------

library(data.table)
library(stringr)

set.seed(123)

source(file.path(getwd(), "model_prep/src_bundle/append_elevation.R"))

# load in mr-brt environment ----------------------------------------------

reticulate::use_condaenv("FILEPATH")
mr_brt_env <- reticulate::py_run_string(
  "from mrtool import *"
) 

# impute all missing elevation adj ----------------------------------------

impute_missing_elevation_adj <- function(input_df, bundle_map, anemia_map, bundle_dir){
  df <- copy(input_df)
  
  i_vec <- which(df$imputed_elevation_adj_row == 1) # only impute data that needs to be elevation imputed
  inverse_i_vec <- setdiff(seq_len(nrow(df)), i_vec) 
  to_impute_df <- df[i_vec, ] # isolate elevation impute data
  df <- df[inverse_i_vec, ] # keep non adj elevation data alone
  
  to_impute_df <- append_elevation(
    input_df = to_impute_df,
    gbd_rel_id = 16
  )
  
  to_impute_df <- adj_elevation_var(
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

# xwalk elevation measures ------------------------------------------------

adj_elevation_var <- function(input_df, bundle_map, anemia_map, bundle_dir){
  df <- copy(input_df)
  
  measure_combo_vec <- unique(df$var.reference_elevation_adj) # get all unique elevation bundles that will help impute missing values in this bundle
  measure_combo_vec <- measure_combo_vec[!(is.na(measure_combo_vec))] # remove anything that's NA
  
  df <- drop_ref_elevation_cols(input_df = df) # drop placeholder columns
  df$elevation_adj_row_id <- as.character(df$elevation_adj_row_id)
  
  if('anemia_category' %in% colnames(df)) df$anemia_category <- NULL # remove previously assigned anemia_category, will reassign
  
  final_df <- data.table()
  
  for(x in measure_combo_vec){
    bun_id <- as.character(bundle_map[var == x & cv_pregnant == unique(df$cv_pregnant), id]) # identify bundle data that will help impute missing elevation data
    print(paste(x, unique(df$cv_pregnant)))
    adj_file_name <- file.path(bundle_dir, bun_id, 'square_measure_impute2.csv') #load in that data set
    print(adj_file_name)
    adj_df <- fread(adj_file_name)
    if('anemia_category' %in% colnames(adj_df)) adj_df$anemia_category <- NULL # remove previously assigned anemia_category, will reassign
    
    adj_df$row_id <- as.character(adj_df$row_id) 
    
    df_w_adj <- merge.data.table( # merge current bundle data on with the adjustment data
      x = df,
      y = adj_df[,.(age_group_id, sex_id, row_id, var, val, standard_error, variance, upper, lower, sample_size, orig_sample_size, group, specificity, group_review)],
      by.x = 'elevation_adj_row_id',
      by.y = 'row_id',
      suffixes = c('', '.reference_elevation_adj')
    )
    
    df_w_adj$age_group_id <- df_w_adj$age_group_id.reference_elevation_adj # apply the age/sex split age_group_ids to the current bundle data
    df_w_adj$sex_id <- df_w_adj$sex_id.reference_elevation_adj # apply the age/sex split sex_ids to the current bundle data
    df_w_adj$age_end <- df_w_adj$age_end.reference_elevation_adj
    df_w_adj$sex_id <- df_w_adj$sex_id.reference_elevation_adj
    df_w_adj$sample_size <- df_w_adj$sample_size.reference_elevation_adj
    df_w_adj$orig_sample_size <- df_w_adj$orig_sample_size.reference_elevation_adj
    df_w_adj$group <- df_w_adj$group.reference_elevation_adj
    df_w_adj$specificity <- df_w_adj$specificity.reference_elevation_adj
    df_w_adj$group_review <- df_w_adj$group_review.reference_elevation_adj
    
    df_w_adj <- merge.data.table( # merge on anemia categories
      x = df_w_adj,
      y = anemia_map,
      by = c('age_group_id', 'sex_id', 'cv_pregnant')
    )
    
    if(grepl('hemog', unique(df$var))){
      df_w_adj <- shift_hb_val(input_df = df_w_adj)
    }else{
      if(nrow(df_w_adj) > 0){
        df_w_adj <- apply_mrbrt_elevation_adj(input_df = df_w_adj) # use mr-brt predictions to impute missing values
        if(nrow(df_w_adj) > 0){
          df_w_adj <- transform_predicted_values(
            input_df = df_w_adj, 
            alt_var_measure_type = bundle_map[var == unique(df$var) & cv_pregnant == unique(df$cv_pregnant), measure]
          )
        } 
      }
    }
    
    final_df <- rbindlist(
      list(final_df, df_w_adj),
      use.names = TRUE,
      fill = TRUE
    )
  }
  
  return(final_df)
}

# adjust hemoglobin by using equations ------------------------------------

shift_hb_val <- function(input_df){
  df <- copy(input_df)
  
  df$shift_val <- get_hb_shift_val(
    ref_var = unique(df$var.reference_elevation_adj),
    alt_var = unique(df$var),
    elevation = df$cluster_altitude
  )
  
  i_vec <- which(!is.na(df$shift_val))
  df <- df[i_vec, ]
  
  df$val <- df$val.reference_elevation_adj + df$shift_val
  df$upper <- df$upper.reference_elevation_adj + df$shift_val
  df$lower <- df$lower.reference_elevation_adj + df$shift_val
  df$variance <- df$variance.reference_elevation_adj
  df$standard_error <- df$standard_error.reference_elevation_adj
  
  return(df)
}

get_hb_shift_val <- function(ref_var, alt_var, elevation){
  
  raw_var <- 'hemoglobin_raw'
  who_var <- c('who_adj_hemog', 'hemoglobin_alt_adj')
  brinda_var <- 'brinda_adj_hemog'
  
  shift_val <- NA_real_
  
  if(ref_var == raw_var && alt_var %in% who_var){
    shift_val <- hb_adj_funcs(elevation = elevation, adj_function_name = who_var[1]) * (-1)
  }else if(ref_var == raw_var && alt_var == brinda_var){
    shift_val <- hb_adj_funcs(elevation = elevation, adj_function_name = brinda_var) * (-1)
  }else if(ref_var %in% who_var && alt_var == raw_var){
    shift_val <- hb_adj_funcs(elevation = elevation, adj_function_name = who_var[1])
  }else if(ref_var == brinda_var && alt_var == raw_var){
    shift_val <- hb_adj_funcs(elevation = elevation, adj_function_name = brinda_var)
  }else if(ref_var %in% who_var && alt_var == brinda_var){
    shift_val <- hb_adj_funcs(elevation = elevation, adj_function_name = who_var[1]) -
      hb_adj_funcs(elevation = elevation, adj_function_name = brinda_var)
  }else if(ref_var == brinda_var && alt_var %in% who_var){
    shift_val <- hb_adj_funcs(elevation = elevation, adj_function_name = brinda_var) -
      hb_adj_funcs(elevation = elevation, adj_function_name = who_var[1])
  }else if(ref_var %in% who_var && alt_var %in% who_var){
    shift_val <- 0
  }
  
  return(shift_val)
}

hb_adj_funcs <- function(elevation, adj_function_name) {
  switch(
    adj_function_name,
    "brinda_adj_hemog" = 0.0000003 * elevation**2 + 0.0056384 * elevation, 
    "who_adj_hemog" = .00000257 * elevation**2 - .00105 * elevation
  )
}

# adjust prevalence by using mr-brt models --------------------------------

apply_mrbrt_elevation_adj <- function(input_df){
  df <- copy(input_df)
  
  i_vec <- which(df$val.reference_elevation_adj <= 0)
  df$val.reference_elevation_adj[i_vec] <- 0.00001
  
  i_vec <- which(df$val.reference_elevation_adj >= 1)
  df$val.reference_elevation_adj[i_vec] <- 0.99999
  
  mrbrt_objects <- prep_mr_brt_data(input_df = df)
  df <- copy(mrbrt_objects$df)
  assign('post_subset_df', df, envir = .GlobalEnv)
  slice_vec <- c()
  for(i in mrbrt_objects$covariates){
    i_vec <- which(is.na(df[[i]]))
    slice_vec <- append(slice_vec, i_vec)
  }
  keep_vec <- setdiff(seq_len(nrow(df)), unique(slice_vec))
  df <- df[keep_vec, ]
  
  ref_var <- unique(as.character(df$var.reference_elevation_adj))
  alt_var <- unique(as.character(df$var))
  
  print(paste(ref_var, alt_var, sep = "-"))
  
  mrbrt_dir <- 'FILEPATH'
  
  pkl_list <- list.files(
    path = mrbrt_dir,
    pattern = "\\.pkl$",
    full.names = TRUE
  )
    
  file_string <- paste(ref_var, alt_var, sep = '-')
  print(file_string)
  pkl_file <- pkl_list[grepl(file_string, pkl_list)]
  print(pkl_file)

  if(length(pkl_file) == 1 && file.exists(pkl_file)){
    fit1 <- reticulate::py_load_object(pkl_file)
    
    dat_pred <- mr_brt_env$MRData()
    dat_pred$load_df(data = df, col_covs = mrbrt_objects$covariates)
    
    df$pred_mean <- fit1$predict(dat_pred, predict_for_study = F) + 
      nch::logit(df$val.reference_elevation_adj)
    
    n_samples <- 100L
    
    reticulate::py_set_seed(123)
    
    mrbrt_samples <- fit1$sample_soln(sample_size = n_samples)
    
    gamma_vals2 <- matrix(
      data = fit1$gamma_soln, 
      nrow = n_samples, 
      ncol = length(fit1$gamma_soln), 
      byrow = TRUE 
    )
    
    draws_fe_re <- fit1$create_draws(
      data = dat_pred,
      beta_samples = mrbrt_samples[[1]],
      gamma_samples = gamma_vals2,
      random_study = TRUE 
    ) + nch::logit(df$val.reference_elevation_adj)
    
    df$pred2_lo <- apply(draws_fe_re, 1, function(x) quantile(x, 0.025))
    df$pred2_up <- apply(draws_fe_re, 1, function(x) quantile(x, 0.975))
    df$draw_se_re <- (df$pred2_up - df$pred2_lo)/(qnorm(0.975)*2)
  }
  
  return(df)
}

prep_mr_brt_data <- function(input_df){
  df <- copy(input_df)
  
  raw_prev_map <- fread(file.path(getwd(), "model_prep/param_maps/raw_prev_map.csv"))
  for(r in 1:nrow(raw_prev_map)){
    prev_cat <- as.numeric(raw_prev_map[r,raw_prev_category])
    u_prev <- as.numeric(raw_prev_map[r,upper_prev])
    l_prev <- as.numeric(raw_prev_map[r,lower_prev])
    df <- df[val.reference_elevation_adj >= l_prev & val.reference_elevation_adj < u_prev, raw_prev_category := prev_cat]
  }
  
  elevation_cat_map <- fread(file.path(getwd(), "model_prep/param_maps/elevation_category_map.csv"))
  for(r in 1:nrow(elevation_cat_map)){
    elevation_cat <- as.character(elevation_cat_map$cluster_cat[r])
    u_elevation <- as.numeric(elevation_cat_map$end_elevation[r])
    l_elevation <- as.numeric(elevation_cat_map$start_elevation[r])
    df <- df[!(is.na(cluster_altitude)) & cluster_altitude >= l_elevation & cluster_altitude < u_elevation, cluster_elevation_id := elevation_cat]
  }
  
  df[,elevation_number_cat := as.numeric(str_remove(cluster_elevation_id,"elevation_cat_"))]
  
  elevation_cov_df <- data.table(
    cluster_elevation_id = elevation_cat_map$cluster_cat,
    elevation_number_cat = as.numeric(str_remove(elevation_cat_map$cluster_cat,"elevation_cat_"))
  )
  elevation_cov_df <- elevation_cov_df[order(elevation_number_cat)]
  
  elevation_covs <- elevation_cov_df$cluster_elevation_id
  elevation_covs <- elevation_covs[elevation_covs!="elevation_cat_0"]
  
  for(x in elevation_covs){
    i_vec <- which(df$cluster_elevation_id==x)
    set(x = df, i = i_vec,j = x, value = 1)
    
    i_vec <- which(!(df$cluster_elevation_id==x))
    set(x = df, i = i_vec,j = x, value = 0)
  }
  
  anemia_categories <- 1:7
  anemia_categories <- anemia_categories[anemia_categories!=5] # use non-pregnant females as the reference
  anemia_cat_covs <- lapply(anemia_categories, function(x) paste("anemia_cov",x,sep = "_"))
  
  for(x in anemia_categories){
    j_val <- paste("anemia_cov",x,sep = "_")
    
    i_vec <- which(df$anemia_category==x)
    set(x = df, i = i_vec,j = j_val, value = 1)
    
    i_vec <- which(!(df$anemia_category==x))
    set(x = df, i = i_vec,j = j_val, value = 0)
  }
  
  raw_prev_categories <- 1:5
  raw_prev_categories <- raw_prev_categories[raw_prev_categories!=1] #use lowest raw anemia prevalence as reference
  raw_prev_covs <- lapply(raw_prev_categories, function(x) paste("raw_prev",x,sep = "_"))
  
  for(x in raw_prev_categories){
    j_val <- paste("raw_prev",x,sep = "_")
    
    i_vec <- which(df$raw_prev_category==x)
    set(x = df, i = i_vec,j = j_val, value = 1)
    
    i_vec <- which(!(df$raw_prev_category==x))
    set(x = df, i = i_vec,j = j_val, value = 0)
  }
  
  covariates <- as.list(append(elevation_covs,raw_prev_covs))
  covariates <- append(covariates, anemia_cat_covs)
  
  return(list(
    df = df,
    covariates = covariates
  ))
}

transform_predicted_values <- function(input_df, alt_var_measure_type){
  df <- copy(input_df)
  
  if(alt_var_measure_type == 'proportion'){
    df$val <- nch::inv_logit(x = df$pred_mean)
    df$elevation_adj_standard_error <- nch::inv_logit_se(
      mean_vec = df$pred_mean,
      se_vec = df$draw_se_re
    )
  }else{
    df$val <- exp(x = df$pred_mean)
    df$elevation_adj_standard_error <- exp(x = df$pred_mean) * df$draw_se_re
  }
  df$variance <- df$elevation_adj_standard_error ^ 2 + 
    df$variance.reference_elevation_adj
  df$standard_error <- sqrt(df$variance)
  df$lower <- df$val - qnorm(0.975) * df$standard_error
  df$upper <- df$val + qnorm(0.975) * df$standard_error
  
  return(df)
}

drop_ref_elevation_cols <- function(input_df){
  df <- copy(input_df)
  
  drop_cols <- colnames(df)[grepl('\\.reference_elevation_adj', colnames(df))]
  for(c in drop_cols) df[[c]] <- NULL
  
  return(df)
}
