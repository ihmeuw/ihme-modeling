make_factors <- function(data) {
  factor_cols = c(
    "age_group_id",
    "year_id",
    "year_bin",
    "country_id",
    "subnat_id",
    "location_id",
    "subreg",
    "subreg_yr"
  )
  factor_cols <- factor_cols[factor_cols %in% colnames(data)]
  data[, (factor_cols) := lapply(.SD, as.factor), .SDcols = factor_cols]
  return(data)
}

test_model_convergence <- function(model) {
  status <- TRUE
  if (any(grepl("fail(ed|ure) to converge", model@optinfo$conv$lme4$messages)) |
      any(grepl("fail(ed|ure) to converge", model@optinfo$warnings))) {
    status <- FALSE
  }
  return(status)
}

report_model_convergence <- function(converged) {
  print(paste0("Poisson converged? ", converged))
}

generate_result_names <- function(predict_col) {
  predict_col_name <- paste0('pred_', predict_col)
  se_col_name <- paste0('std_err_', predict_col)
  return(c(predict_col_name, se_col_name))
}

run_model <- function(predict_col,
                      model_data,
                      covariates,
                      random_intercept = NULL,
                      maxit = 100) {
  formula <- determine_formula(model_data, predict_col, covariates, random_intercept = random_intercept)
  if (!grepl("\\(1 \\| ", format(formula))) {
    random_intercept <- NULL
  }
  
  print(formula)
  
  if (is.null(random_intercept)) {
    glm.control(maxit = maxit)
    pois_model <- glm(formula = formula,
                      family = poisson(),
                      data = model_data)
    
    pois_converged <- pois_model$converged
    
    if (!pois_converged) {
      return(fill_with_average(model_data, predict_col))
    } else {
      pois_results <-
        predict.glm(pois_model, new_data = model_data, se.fit = TRUE)
      fit_col <- exp(pois_results$fit)
      se_col <- pois_results$se.fit
      model_result <- data.table(fit_col, se_col)
      names(model_result) <- generate_result_names(predict_col)
      
      return(list(model_result = model_result, converged = T))
    }
  } else {
    
    pois_model <- glmer(
      formula = formula,
      data = model_data,
      family = poisson(),
      verbose = 1,
      control = glmerControl(optCtrl = list(
        maxfun = maxit, verbose = 1
      ))
    )
    
    pois_converged <- test_model_convergence(pois_model)
    if (!pois_converged) {
      return(fill_with_average(model_data, predict_col))
    } else {
      fit_col <-
        exp(predict(pois_model, newdata = model_data, re.form = NA))
      
      X <- getME(pois_model, "X")
      V <- vcov(pois_model)
      se_col <-
        apply(X, 1, function(x)
          sqrt(as.numeric(x %*% V %*% x)))
      
      model_result <- data.table(fit_col, se_col)
      names(model_result) <- generate_result_names(predict_col)
      return(list(model_result = model_result, converged = T))
    }
  }
}

determine_formula <- function(data,
                              predict_col,
                              covariates,
                              random_intercept = NULL) {
  
  formula <- paste(predict_col, "~ offset(log(sample_size))")
  for (cov in covariates) {
    if (length(unique(data[[cov]])) > 1) {
      formula <- paste(formula, "+", cov)
    }
  }
  
  if (!is.null(random_intercept)) {
    stopifnot(length(random_intercept) < 2)
    if (length(unique(data[[random_intercept]])) > 1) {
      stopifnot(class(data[[random_intercept]]) == 'factor')
      formula <- paste0(formula, " + (1 | ", random_intercept, ")")
    }
  }
  return(formula)
}

sufficient_df <- function(data, model_group) {
  time_series <- model_group_is_time_series(model_group)
  data <-
    data[, row_is_time_series := source_is_time_series(source)]
  if (!time_series) {
    total_rows <- data[!(row_is_time_series), .N]
    total_nonzero <- data[cf > 0 & !(row_is_time_series), .N]
  } else {
    total_rows <- data[, .N]
    total_nonzero <- data[cf > 0, .N]
  }
  sufficient <- total_rows > 6 & total_nonzero > 0
  return(sufficient)
}

fill_with_average <- function(df_orig, predict_col) {
  df <- copy(df_orig)
  df[, fit_col := sum(get(predict_col)) / sum(sample_size) * sample_size, by = age_group_id]
  df[, se_col := 0]
  df <- df[, .(fit_col, se_col)]
  
  names(df) <- generate_result_names(predict_col)
  
  return(list(model_result = df, converged = F))
}

coreNum <- function() {
  if (interactive()) {
    cores <- readline(prompt = "Cores in use: ")
  } else {
    cores <- strtoi(Sys.getenv("OMP_NUM_THREADS"))
  }
  cores <- as.numeric(cores)
  
  return(cores)
}


run_model_for_column <- function(predict_col,
                                 cause_sex_data,
                                 covariates,
                                 random_intercept = NULL,
                                 maxit = 100) {
  predicted_col_with_se <- tryCatch({
    run_model(
      predict_col,
      cause_sex_data,
      covariates,
      random_intercept = random_intercept,
      maxit = maxit
    )
  }, error = function(e) {
    fill_with_average(cause_sex_data, predict_col)
  })
  return(predicted_col_with_se)
}

get_data_dir <- function(nr_process_dir = NULL,
                         model_group = NULL,
                         cause_id = "NONE") {
  stopifnot(!is.null(nr_process_dir) & !is.null(model_group))
  data_dir <- "FILEPATH"
  if (cause_id != "NONE") {
    data_dir <- "FILEPATH"
  }
  return(data_dir)
}

is_va_champs <- function(model_group) {
  return(grepl("^(VA|malaria|CHAMPS)", model_group))
}

is_se_asia <- function(model_group) {
  return(grepl("^VA-4$", model_group))
}


is_ssa_malaria <- function(model_group) {
  return(grepl("SSA", model_group))
}

is_matlab <- function(model_group) {
  return(grepl("Matlab", model_group))
}

is_ind_srs <- function(model_group) {
  ind_srs <- grepl("VA-SRS-IND", model_group)
  ind_srs_malaria <- grepl("malaria_IND_SRS", model_group)
  return(any(ind_srs, ind_srs_malaria))
}

model_group_is_time_series <- function(model_group) {
  return(any(is_matlab(model_group), is_ind_srs(model_group)))
}

source_is_time_series <- function(source) {
  is_matlab <- grepl("Matlab", source)
  return(is_matlab)
}

read_and_clean_data <- function(nr_process_dir = NULL,
                                model_group = NULL,
                                cause_id = "NONE",
                                launch_set_id = NULL) {
  data_dir <- get_data_dir(
    nr_process_dir = nr_process_dir,
    model_group = model_group,
    cause_id = cause_id
  )
  data <- read_parquet("FILEPATH")
  data <- as.data.table(data)
  
  if (is_va_champs(model_group)) {
    data[, subreg := .GRP, by = c("nid", "location_id", "site_id")]
    data[, subreg_yr := .GRP, by = c("nid", "location_id", "site_id", "year_id")]
  }
  
  data <- make_factors(data)
  
  if (!('deaths' %in% names(data)) ||
      length(data[is.na(data$deaths) > 0])) {
    data$deaths <- data$cf * data$sample_size
  }
  
  data <- data[order(data$year_id), ]
  return(data)
}

save_data <- function(data,
                      nr_process_dir = NULL,
                      model_group = NULL,
                      cause_id = "NONE",
                      launch_set_id = NULL) {
  data$deaths <- NULL
  if ("survey_type" %in% names(data)) {
    data$survey_type <- NULL
  }
  data_dir <- get_data_dir(
    nr_process_dir = nr_process_dir,
    model_group = model_group,
    cause_id = cause_id
  )
  write_parquet(
    data,
    paste0("FILEPATH")
  )
}
