
# age sex split function --------------------------------------------------

#' @export
age_sex_split_anemia <- function(input_df, cascade_dir, release_id){
  df <- data.table::copy(input_df)
  
  message('Prepping data...')
  df$mean <- df$val
  df$orig_val <- df$val
  df$orig_se <- df$standard_error
  df$orig_variance <- df$variance
  df$orig_sample_size <- df$sample_size
  df$orig_cases <- df$cases
  
  loc_df <- ihme::get_location_metadata(
    location_set_id = 35,
    release_id = release_id
  )
  
  hierarchy_col_names <- c(
    'global_id',
    'super_region_id',
    'region_id',
    'country_id',
    'subnat1_id',
    'subnat2_id',
    'subnat3_id'
  )
  
  age_df <- ihme::get_age_metadata(release_id = release_id)
  age_df <- age_df[order(age_group_years_start)]
  age_df$ordinal_val <- seq_len(nrow(age_df))
  
  message('Identifying data to split...')
  
  df <- prep_age_sex_split(
    input_df = df,
    age_df = age_df,
    gbd_rel_id = release_id
  ) |>
    dplyr::mutate(split_id = seq_len(nrow(df))) |>
    split_data(input_age_df = age_df)
  print(nrow(df))
  
  to_split <- df |> 
    dplyr::filter(to_split == 1)
  df <- df |> 
    dplyr::filter(to_split != 1)
  
  if(nrow(to_split) > 0){
    
    message('Getting age weights and applying age/sex split...')
  
    to_split <- to_split |>
      get_age_sex_weights(
        cascade_dir = cascade_dir,
        hierarchy_col_names = hierarchy_col_names,
        input_loc_df = loc_df
      ) |>
      apply_weights()
    
    message('Writing data out...')
    
    df <- data.table::rbindlist(
      list(df, to_split),
      use.names = TRUE,
      fill = TRUE
    )
    
  }else{
    message('No data needs to be split, returning as is...')
  }
  
  return(df)
}

# prep bundle data --------------------------------------------------------

prep_age_sex_split <- function(input_df, age_df, gbd_rel_id){
  df <- data.table::copy(input_df)
  
  df$age_start <- df$orig_age_start
  df$age_end <- df$orig_age_end
  df$age_group_id <- NULL
  
  sex_list <- list(
    Male = 1,
    Female = 2,
    Both = 3
  )
  df$sex_id <- 0
  for(i in names(sex_list)){
    i_vec <- which(df$sex == i)
    df$sex_id[i_vec] <- sex_list[[i]]
  }
  
  df$age_floor <- unlist(lapply(df$age_start, function(x){
    grab_floor(x, sort(age_df$age_group_years_start))
  }))
  df$age_ceil <- unlist(lapply(df$age_end, function(x){
    grab_ceil(x, sort(age_df$age_group_years_end))
  }))
  
  df$age_ceil <- update_age_1(
    age_start = df$age_start, 
    age_end = df$age_end,
    age_ceil = df$age_ceil
  )
  
  df <- merge_age_groups(
    input_df = df,
    age_df = age_df
  )
  
  df <- set_age_sex_params(input_df = df)
  
  return(df)
}

# helper functions --------------------------------------------------------

grab_floor <- function(num, start_years){
  for (i in rev(start_years)){
    if (i <= num){
      return(i)
    }
  }
}
grab_ceil <- function(num, end_years){
  for (i in end_years){
    if (i >= num){
      return(i)
    }
  }
}

update_age_1 <- function(age_start, age_end, age_ceil){
  i_vec <- which(age_start == 1 & age_end == 1)
  age_ceil[i_vec] <- 2
  return(age_ceil)
}

merge_age_groups <- function(input_df, age_df){
  df <- data.table::copy(input_df)
  
  df <- data.table::merge.data.table(
    x = df,
    y = age_df[,.(age_group_id, ordinal_val, age_group_years_start)],
    by.x = "age_floor",
    by.y = "age_group_years_start",
    all.x = TRUE
  )
  data.table::setnames(
    x = df, 
    old = c('age_group_id', 'ordinal_val'), 
    new = c('age_group_id_floor', 'ordinal_val_floor')
  )
  
  df <- data.table::merge.data.table(
    x = df,
    y = age_df[,.(age_group_id, ordinal_val, age_group_years_end)],
    by.x = "age_ceil",
    by.y = "age_group_years_end",
    all.x = TRUE
  )
  data.table::setnames(
    x = df, 
    old = c('age_group_id', 'ordinal_val'), 
    new = c('age_group_id_ceil', 'ordinal_val_ceil')
  )
  
  return(df)
}

set_age_sex_params <- function(input_df = df){
  df <- data.table::copy(input_df)
  
  df$n_age <- df$ordinal_val_ceil - df$ordinal_val_floor + 1
  
  i_vec <- which(df$n_age == 1)
  df$good_range <- 0
  df$good_range[i_vec] <- 1
  
  i_vec <- which(df$sex == "Both" | df$sex_id == 3)
  df$good_sex <- 1
  df$good_sex[i_vec] <- 0
  
  df$good_cols <- df$good_range + df$good_sex
  
  i_vec <- which(df$good_cols == 2 & df$is_outlier == 0)
  df$to_train <- 0
  df$to_train[i_vec] <- 1
  
  df$to_split <- 0 ^ df$to_train
  
  df$crosswalk_parent_seq <- df$seq
  
  return(df)
}

# split data --------------------------------------------------------------

split_data <- function(input_split_df, input_age_df){
  split_df <- data.table::copy(input_split_df)
  
  split_df$n_sex <- 1
  i_vec <- which(split_df$sex_id == 3)
  split_df$n_sex[i_vec] <- 2
  
  expanded_df <- data.table::data.table(
    split_id = rep(split_df$split_id, split_df$n_age)
  )
  
  split_df <- data.table::merge.data.table(
    x = expanded_df,
    y = split_df,
    by = "split_id",
    all = TRUE
  )
  
  split_df <- split_df |>
    dplyr::group_by(split_id) |>
    dplyr::mutate(age_rep = dplyr::row_number() - 1) |>
    data.table::setDT()
  
  split_df$age_id_split <- split_df$ordinal_val_floor + split_df$age_rep
  split_df$sex_split_id <- paste(
    split_df$split_id, 
    split_df$age_id_split, 
    sep = "_"
  )
  
  expanded_df <- data.table::data.table(
    sex_split_id = rep(split_df$sex_split_id, split_df$n_sex)
  )
  
  split_df <- data.table::merge.data.table(
    x = expanded_df,
    y = split_df,
    by = "sex_split_id",
    all = TRUE
  )
  
  split_df <- split_df |>
    dplyr::group_by(sex_split_id) |>
    dplyr::mutate(sex_id = ifelse(
      sex_id == 3, 
      dplyr::row_number(), 
      sex_id)
    ) |>
    data.table::setDT() 
  
  data.table::setnames(
    x = split_df,
    old = "age_id_split",
    new = "ordinal_val"
  )
  
  age_df <- data.table::copy(input_age_df)
  data.table::setnames(
    x = age_df,
    old = c('age_group_years_start', 'age_group_years_end'),
    new = c('age_start', 'age_end')
  )
  
  split_df$age_start <- NULL
  split_df$age_end <- NULL
  
  keep_cols <- c('age_start', 'age_end', 'age_group_id', 'ordinal_val')
  
  split_df <- data.table::merge.data.table(
    x = split_df,
    y = age_df[, keep_cols, with = FALSE],
    by = 'ordinal_val'
  )
  
  return(split_df)
}

# calculate age sex weights -----------------------------------------------

get_age_sex_weights <- function(input_df, input_loc_df, cascade_dir, hierarchy_col_names){
  df <- data.table::copy(input_df)
  
  df <- split_input_location_levels(
    input_df = df,
    input_loc_df = input_loc_df,
    cascade_dir = cascade_dir,
    hierarchy_col_names = hierarchy_col_names
  ) |>
    get_mr_brt_weights()
  
  return(df)
}

split_input_location_levels <- function(input_df, input_loc_df, cascade_dir, hierarchy_col_names){
  df <- data.table::copy(input_df)
  loc_df <- data.table::copy(input_loc_df)
  loc_df <- loc_df |> dplyr::filter(level >= 3) |> data.table::setDT()
  
  for(i in hierarchy_col_names) loc_df[[i]] <- NA_integer_
  
  for(r in seq_len(nrow(loc_df))){
    parent_vec <- as.integer(
      stringr::str_split(
        string = loc_df$path_to_top_parent[r],
        pattern = ",",
        simplify = TRUE
      )
    )
    
    for(i in seq_len(length(parent_vec))){
      loc_df[[hierarchy_col_names[i]]][r] <- parent_vec[i]
    }
  }
  
  loc_df <- data.table::rbindlist(
    list(
      loc_df |> dplyr::mutate(sex_id = 1, cv_pregnant = 0, var = unique(df$var), age_cutoff = '2_plus'),
      loc_df |> dplyr::mutate(sex_id = 2, cv_pregnant = 0, var = unique(df$var), age_cutoff = '2_plus'),
      loc_df |> dplyr::mutate(sex_id = 2, cv_pregnant = 1, var = unique(df$var), age_cutoff = '2_plus'),
      loc_df |> dplyr::mutate(sex_id = 1, cv_pregnant = 0, var = unique(df$var), age_cutoff = '2_under'),
      loc_df |> dplyr::mutate(sex_id = 2, cv_pregnant = 0, var = unique(df$var), age_cutoff = '2_under')
    )
  )
  loc_df$pkl_file <- NA_character_
  
  for(r in seq_len(nrow(loc_df))){
    for(c in rev(hierarchy_col_names)){
      if(!(is.na(loc_df[[c]][r]))){
        cascade_file <- file.path(
          cascade_dir,
          loc_df$age_cutoff[r],
          paste('age_sex_cascade', loc_df$var[r], loc_df$sex_id[r], loc_df$cv_pregnant[r], sep = "_"),
          'pickles',
          if(c == 'global_id') 'stage1__stage1.pkl' else paste0(c, "__", loc_df[[c]][r], ".pkl")
        )
        if(file.exists(cascade_file)){
          loc_df$pkl_file[r] <- cascade_file
          break
        }
      }
    }
  }
  
  keep_cols <- c(
    hierarchy_col_names,
    'sex_id', 'cv_pregnant', 'var', 'age_cutoff', 'pkl_file', 'location_id'
  )
  
  df[['age_cutoff']] <- '2_plus'
  i_vec <- which(df$age_start < 2)
  df[['age_cutoff']][i_vec] <- '2_under'
  
  df <- data.table::merge.data.table(
    x = df,
    y = loc_df[, keep_cols, with = FALSE],
    by = c('location_id', 'age_cutoff', 'var', 'sex_id', 'cv_pregnant')
  )
  
  return(df)
}

get_mr_brt_weights <- function(input_df){
  
  df <- data.table::copy(input_df)
  
  reticulate::use_condaenv("FILEPATH")
  pd <- reticulate::import("pandas")
  mrtool <- reticulate::import("mrtool")
  
  pkl_vec <- unique(df$pkl_file)
  
  final_df <- data.table::data.table()
  
  final_df <- furrr::future_map(pkl_vec, \(p){
    temp_df <- df |> 
      dplyr::filter(pkl_file == p) |>
      dplyr::mutate(
        sqrt_age_start = sqrt(age_start),
        age_mid = (age_start + age_end) / 2,
        log_age_mid = log(age_mid)
      )
    
    fit_obj <- reticulate::py_load_object(p, pickle = 'dill')
    
    covariate <- if(grepl("2_plus", p)) 'age_start' else if(grepl('2_under', p) && grepl('hemog', p)) 'log_age_mid' else 'sqrt_age_start'
    
    dat_pred <- mrtool$MRData()
    dat_pred$load_df(data = temp_df, col_covs = list(covariate))
    
    n_samples <- 100L
    
    reticulate::py_set_seed(123)
    
    mrbrt_samples <- fit_obj$sample_soln(
      sample_size = n_samples
    )
    
    # each row is the same; point estimates of gamma rather than samples
    draws1 <- fit_obj$create_draws(
      data = dat_pred,
      beta_samples = mrbrt_samples[[1]],
      gamma_samples = matrix(rep(0, n_samples), ncol = 1),
      random_study = FALSE 
    ) |>
      as.matrix() |>
      as.data.frame() 
    
    colnames(draws1) <- paste0('draw_', seq_len(n_samples))
    min_val <- if(grepl('hemog', p)) log(25) else nch::logit(0.000001)
    max_val <- if(grepl('hemog', p)) log(220) else nch::logit(0.999999)
    for(c in colnames(draws1)) {
      if(any(draws1[[c]] < min_val)) {
        i_vec <- which(draws1[[c]] < min_val)
        draws1[[c]][i_vec] <- min_val
      }
      if(any(draws1[[c]] > max_val)) {
        i_vec <- which(draws1[[c]] > max_val)
        draws1[[c]][i_vec] <- max_val
      }
    }
    
    temp_df <- cbind(
      temp_df, draws1
    ) |> data.table::setDT()
    
    return(temp_df)
  }, .options = furrr::furrr_options(seed = TRUE)) |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE)
  
  return(final_df)
}

# apply age sex weights ---------------------------------------------------

apply_weights <- function(input_df){
  df <- data.table::copy(input_df)
  
  draws_cols <- setdiff(colnames(df), nch::non_draw_cols(df))
  
  if(grepl('hemog', unique(df$var))){
    df$measure_name <- 'continuous'
    for(i in draws_cols) df[[i]] <- exp(df[[i]])
  }else{
    df$measure_name <- 'proportion'
    for(i in draws_cols) df[[i]] <- nch::inv_logit(df[[i]])
  }
  
  population_df <- ihme::get_population(
    age_group_id = unique(df$age_group_id),
    year_id = unique(df$year_id),
    sex_id = unique(df$sex_id),
    location_id = unique(df$location_id),
    release_id = 16
  )
  
  df <- data.table::merge.data.table(
    x = df,
    y = population_df,
    by = c('age_group_id', 'location_id', 'year_id', 'sex_id')
  )
  
  df <- split_sample_size(df)
  
  df <- df |> dplyr::arrange(seq)
  
  df <- purrr::map(split(df, df$seq), function(seq_data) {
    input_draws <- simulate_input_draws(
      n = n_draws(seq_data),
      mean = unique(seq_data$mean),
      standard_error = unique(seq_data$standard_error),
      sample_size = seq_data$sample_size,
      measure_name = unique(seq_data$measure_name)
    )
    mean_draws <- get_mean_draws(input_draws, seq_data)
    result <- dplyr::select(seq_data, !tidyselect::matches("draw_\\d+"))
    result$new_mean <- purrr::map_dbl(mean_draws, base::mean)
    result$new_standard_error <- purrr::map_dbl(mean_draws, stats::sd)
    result$new_lower <- purrr::map_dbl(
      mean_draws, \(x) stats::quantile(x, .025)
    )
    result$new_upper <- purrr::map_dbl(
      mean_draws, \(x) stats::quantile(x, .975)
    )
    return(result)
  }) |> purrr::list_rbind()
  
  df$standard_error <- df$new_standard_error
  df$lower <- df$new_lower
  df$upper <- df$new_upper
  
  df$age_sex_mean_diff <- df$new_mean - df$val
  df$val <- df$new_mean
  df$mean <- NULL
  df$variance <- df$new_standard_error ^ 2
  
  if(grepl('hemog', unique(df$var))){
    df$cases <- df$sample_size
  } else{
    df$cases <- df$sample_size * df$val
  }
  
  return(df)
  
}

split_sample_size <- function(dat) {
  dat |>
    dplyr::group_by(.data$seq) |>
    dplyr::mutate(
      sample_size = split_sample_size_seq(.data$sample_size, .data$population)
    ) |>
    dplyr::ungroup() |>
    data.table::as.data.table()
}

split_sample_size_seq <- function(sample_size, population) {
  sample_size * (population / sum(population))
}

get_mean <- function(input_draw, mean_model, population) {
  result <- input_draw * mean_model / stats::weighted.mean(mean_model, population)
  if (all(mean_model == 0)) {
    result <- rep(input_draw, length(mean_model))
  }
  result
}

get_mean_draws <- function(input_draws, seq_data) {
  purrr::map(seq_len(n_draws(seq_data)), function(i) {
    get_mean(
      input_draw = purrr::chuck(input_draws, i),
      mean_model = purrr::chuck(seq_data, paste0("draw_", i)),
      population = purrr::chuck(seq_data, "population")
    )
  }) |> purrr::list_transpose()
}

n_draws <- function(dat) {
  sum(grepl("draw_\\d+", names(dat)))
}

simulate_input_draws <- function(n, mean, standard_error, sample_size,
                                 measure_name) {
  checkmate::assert_int(n)
  checkmate::assert_number(mean)
  checkmate::assert_number(standard_error)
  checkmate::assert_numeric(sample_size)
  withr::local_seed(123)
  if (measure_name %in% c("prevalence", "proportion")) {
    result <- simulate_p(n, mean, standard_error, sample_size)
  } else {
    result <- stats::rnorm(n = n, mean = mean, sd = standard_error)
    checkmate::assert_numeric(
      result, any.missing = FALSE, finite = TRUE, len = n
    )
  }
  result
}

simulate_p <- function(n, mean, sd, sample_size) {
  if (sd^2 < mean * (1 - mean)) {
    result <- simulate_p_beta(n, mean, sd)
  } else {
    result <- simulate_p_binom(n, mean, sample_size)
  }
  checkmate::assert_numeric(
    result, lower = 0, upper = 1, any.missing = FALSE, len = n
  )
  result
}

simulate_p_beta <- function(n, mean, sd) {
  params <- beta_params_for_mean_var(mean, sd^2)
  stats::rbeta(n = n, shape1 = params$alpha, shape2 = params$beta)
}

simulate_p_binom <- function(n, mean, sample_size) {
  sample_size_orig <- round(sum(sample_size))
  if (sample_size_orig > 0) {
    stats::rbinom(n = n, size = sample_size_orig, prob = mean) /
      sample_size_orig
  } else {
    rep(mean, n)
  }
}

beta_params_for_mean_var <- function(mu, variance) {
  # See https://en.wikipedia.org/wiki/Beta_distribution#Mean_and_variance for
  # details on the algorithm to get alpha and beta from mean and variance.
  stopifnot(variance < mu * (1 - mu))
  v <- ((mu * (1 - mu)) / variance) - 1
  list(
    alpha = mu * v,
    beta = (1 - mu) * v
  )
}

check_prevalence_above_one <- function(mean, lower, upper, seq,
                                       measure_name = "prevalence") {
  checkmate::assert_numeric(mean, any.missing = FALSE)
  checkmate::assert_numeric(lower, any.missing = FALSE)
  checkmate::assert_numeric(upper, any.missing = FALSE)
  above_1 <- mean > 1 | lower > 1 | upper > 1
  if (any(above_1)) {
    cli::cli_warn(message = c(
      "Some split {measure_name} estimates produced are above 1. This may indicate \\
       that the assumptions used in splitting are violated for those observations.",
      "i" = "{.var seq} values with {measure_name}s above 1: \\
             {.val {unique(seq[above_1])}}"
    ))
  }
}

