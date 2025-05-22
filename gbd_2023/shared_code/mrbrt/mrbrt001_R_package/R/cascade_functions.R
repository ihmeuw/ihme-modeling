#
# cascade_functions.R
#
# September 2020
#
# Functions: run_spline_cascade, predict_spline_cascade
#

#' Fit a cascading spline model
#'
#' \code{run_spline_cascade} implements hierarchical spline estimation, using the estimated spline
#' coefficients from a parent model (e.g. global) as Bayesian priors in child models
#' (e.g. for each region)
#'
#' @param stage1_model_object a MR-BRT model object (post-fitting) that includes one spline
#' @param df a data frame with all needed variables for running the model
#' @param col_obs column name for the dependent variable
#' @param col_obs_se column name for the standard error of the dependent variable
#' @param col_study_id column name for the variable with unique group IDs, for random effects
#' @param stage_id_vars a vector of column names identifying cascade stages
#' (e.g. \code{c(super_region_id, region_id, location_id)});
#' must have a nested structure and be in hierarchical order from left to right
#' @param thetas multiplier for the standard deviations of the priors on the spline betas;
#' a vector with an element for each varialbe in \code{stage_id_vars}
#' @param output_dir path to a directory in which the working directory will be created
#' @param model_label name of the model; will also be the name of the working directory
#' @param inner_print_level integer indicating the amount of information to be displayed by IPOPT
#' during the fitting process; e.g. \code{1L} or \code{5L}
#' @param inner_max_iter maximum number of iterations allowed per model
#' @param overwrite_previous logical; whether to overwrite pre-existing results
#' at \code{file.path(output_dir, model_label)}
#' @return list of model info, including where the working directory with saved outputs is located
#' @examples
#' fit1 <- run_spline_cascade(
#'   stage1_model_object = mod3,
#'   df = df,
#'   col_obs = "logit_mort",
#'   col_obs_se = "logit_mort_se",
#'   col_study_id = "location_id",
#'   stage_id_vars = c("super_region_name", "location_id"),
#'   thetas = c(1,1),
#'   output_dir = "FILEPATH",
#'   model_label = "mrbrt_cascade_test2",
#'   overwrite_previous = TRUE
#' )
#'
run_spline_cascade <- function(
  stage1_model_object, df, col_obs, col_obs_se, col_study_id,
  stage_id_vars, thetas, output_dir, model_label,
  inner_print_level = 2L, inner_max_iter = 1000L,
  overwrite_previous = FALSE, verbose = FALSE) {

  dev <- FALSE
  if (dev) {
    stage1_model_object <- mod3
    df <- df
    col_obs <- "logit_mort"; col_obs_se <- "logit_mort_se"; col_study_id <- "location_id"
    stage_id_vars <- c("super_region_name", "location_id")
    thetas <- c(1,1)
    output_dir <- "FILEPATH"
    model_label <- "mrbrt_cascade_test2"
    inner_print_level <- 1L
    inner_max_iter <- 1000L
    overwrite_previous <- FALSE
    verbose <- TRUE
    # tmp <- lapply(names(mod3), function(x) mod3[x])
    # tmp <- lapply(names(mod3$data), function(x) mod3$data[x])
  }

  require(dplyr)

  #####
  # set up the working directory
  if (!dir.exists(output_dir)) stop(paste0("Output directory ", output_dir, " not found"))

  working_dir <- file.path(output_dir, model_label)

  if (dir.exists(working_dir) & !overwrite_previous) {
    stop(paste0(
      "Working directory already exists at ", working_dir, "; ",
      "to overwrite, set the 'overwrite_previous' argument to TRUE"
    ))
  } else if (dir.exists(working_dir) & overwrite_previous) {
    system(paste0("rm -rf ", working_dir))
  }

  dir.create(working_dir)
  dir.create(file.path(working_dir, "pickles"))


  #####
  # check consistency of inputs
  ob <- stage1_model_object

  if (length(thetas) != length(stage_id_vars)) {
    stop("'thetas' must have the same number of elements as 'stage_id_vars'")
  }

  required_vars <- c(
    col_obs, col_obs_se, col_study_id, stage_id_vars,
    ob$cov_names[!ob$cov_names == "intercept"]
  )
  if (!all(required_vars %in% names(df))) {
    stop(paste0("Variable(s) not found in 'df': ", paste(
      required_vars[!required_vars %in% names(df)], collapse = ", "
    )))
  }

  # -- prep variables
  df <- as.data.frame(df)
  df[, union(stage_id_vars, col_study_id)] <- lapply(
    df[, union(stage_id_vars, col_study_id)], as.character
  )
  df[, paste0(c("y", "y_se", "y_ranef"), "__cascade")] <- df[, c(col_obs, col_obs_se, col_study_id)]


  #####
  # functions

  # -- function for determining which covariate is the spline
  cov_is_spline <- function(model_object) {
    out <- sapply(1:length(model_object$cov_models), function(i) {
      !is.null(model_object$cov_models[[i]]$spline_knots)
    })
    if (sum(out) != 1) stop("Number of splines in the model is not 1")
    return(out)
  }

  # -- function for retrieving the posterior distributions on the spline betas
  get_spline_posteriors <- function(model_object) {
    dev <- FALSE
    if (dev) {
      model_object <- ob
    }
    is_spline <- cov_is_spline(model_object = model_object)
    n_betas_before <- which(is_spline) - 1
    n_betas_after <- length(is_spline) - which(is_spline)
    is_spline_beta <- (n_betas_before+1):(length(model_object$beta_soln)-n_betas_after)

    stdevs_tmp <- apply(X = mrbrt001::core$other_sampling$sample_simple_lme_beta(
      sample_size = 1000L, model = model_object
    ), MARGIN = 2, FUN = sd)

    list(
      betas = model_object$beta_soln[is_spline_beta],
      stdevs = stdevs_tmp[is_spline_beta]
    )
  }

  # -- function for retrieving the knot values in the stage1 model
  get_knot_locs <- function(model_object) {
    dev <- FALSE
    if (dev) {
      model_object <- ob
    }
    is_spline <- cov_is_spline(model_object = model_object)
    model_object$cov_models[[which(is_spline)]]$spline_knots
  }

  #####
  # prep inputs to the cascade

  # -- define identifiers for all stages/models
  df_stage_ids <- df %>%
    mutate(stage1 = "stage1") %>%
    select(all_of(c("stage1", stage_id_vars))) %>%
    filter(!duplicated(.))

  # -- retrieve info from the stage1 model
  spline_posteriors <- list(stage1__stage1 = get_spline_posteriors(model_object = ob))
  # stage1_knot_locs <- get_knot_locs(model_object = ob)

  # -- save info from stage1 model
  py_save_object(
    object = ob,
    filename = file.path(working_dir, "pickles", "stage1__stage1.pkl"),
    pickle = "dill"
  )

  # -- loop through stages
  for (stage_tmp in stage_id_vars) {
    dev <- FALSE
    if (dev) {
      stage_tmp <- "super_region_name"
      # stage_tmp <- "location_id"
    }

    # -- loop through models within a stage
    for (submodel_tmp in unique(df[, stage_tmp])) {
      dev <- FALSE
      if (dev) {
        submodel_tmp <- "High-income"
        # submodel_tmp <- "Southeast Asia, East Asia, and Oceania"
        # submodel_tmp <- "Central Europe, Eastern Europe, and Central Asia"
        # [1] "Southeast Asia, East Asia, and Oceania"           "Central Europe, Eastern Europe, and Central Asia"
        # [3] "High-income"                                      "Latin America and Caribbean"
        # [5] "North Africa and Middle East"                     "Sub-Saharan Africa"
        # [7] "South Asia"
        # submodel_tmp <- "530"
      }

      ob2 <- py_load_object(filename = file.path(working_dir, "pickles", "stage1__stage1.pkl"))
      stage1_cov_list <- lapply(1:length(ob2$cov_models), function(i) ob2$cov_models[[i]])

      model_id <- paste0(stage_tmp, "__", submodel_tmp)
      cat(model_id, "\n")

      # -- subset data to observations for the submodel
      df_submodel <- df %>%
        filter(!!sym(stage_tmp) == submodel_tmp)

      # -- if there's only 1 unique level of the random effect ID variable, turn off random effects
      #    by restricting the variance of random effects to be exactly zero

      submodel_cov_list <- stage1_cov_list

      if (length(unique(df_submodel[, "y_ranef__cascade"])) == 1) {
        for (i in 1:length(submodel_cov_list)) {
          submodel_cov_list[[i]]$prior_gamma_uniform[1:2,] <- 0
        }
      }

      # -- get the posterior distributions of spline betas from the previous stage,
      #    multiply them by the stage-specific theta to adjust the strength,
      #    and pass them in as a prior to the spline in this stage
      prior_stage <- names(df_stage_ids)[which(names(df_stage_ids) == stage_tmp)-1]

      prior_id <- df_stage_ids %>%
        filter(!!sym(stage_tmp) == submodel_tmp) %>%
        select(!!sym(prior_stage)) %>%
        filter(!duplicated(.)) %>%
        .[,1]

      if (length(prior_id) > 1) {
        stop(paste0(
          "Levels must be nested; '", submodel_tmp, "' is classified under multiple ",
          "parent IDs (variable '", prior_stage, "'): ", paste(prior_id, collapse = ", ")
        ))
      }

      theta_idx <- which(stage_id_vars == stage_tmp)
      prior_dat <- spline_posteriors[[paste0(prior_stage, "__", prior_id)]]
      prior_dat2 <- sapply(names(prior_dat), function(x) {
        if (x == "stdevs") {
          prior_dat[[x]] * thetas[theta_idx]
        } else {
          prior_dat[[x]]
        }
      }, simplify = FALSE)

      spline_prior <- do.call("rbind", prior_dat2)
      submodel_cov_list[[which(cov_is_spline(ob))]]$prior_beta_gaussian <- spline_prior

      # -- prep the MRBRT data object
      dat_submodel <- MRData()
      dat_submodel$load_df(
        data = df_submodel,
        col_obs = "y__cascade", col_obs_se = "y_se__cascade",
        col_covs = as.list(ob$cov_names[ob$cov_names != "intercept"]),
        col_study_id = "y_ranef__cascade"
      )

      submodel <- MRBRT(
        data = dat_submodel,
        cov_models = submodel_cov_list
      )
      submodel$attach_data(ob$data)

      dev <- FALSE
      if (dev) {
        nms <- names(submodel$cov_models[[2]])
        # sapply(nms, function(nm) submodel$cov_models[[2]][nm], simplify = FALSE)
        sapply(nms, function(nm) ob$cov_models[[2]][nm], simplify = FALSE)

      }

      try({
        submodel$fit_model(
          inner_print_level = inner_print_level,
          inner_max_iter = inner_max_iter
        )

        # -- save information for subsequent stages (and future reference)
        posterior_out <- get_spline_posteriors(submodel)
        spline_posteriors[[model_id]] <- posterior_out
        py_save_object(
          object = submodel,
          filename = file.path(working_dir, "pickles", paste0(model_id, ".pkl")),
          pickle = "dill"
        )

      })

      dev <- FALSE
      if (dev) {
        df_pred <- expand.grid(
          age_start = seq(0, 100, by = 1)) %>%
          mutate(
            age_end = age_start,
            data_id = 1:nrow(.)
          )

        dat_pred <- MRData()

        dat_pred$load_df(
          data = df_pred,
          col_covs=list('age_start', 'age_end', 'data_id')
        )

        sr_files <- list.files(file.path(working_dir, "pickles"), pattern = "super_region_name")
        # mod_tmp <- py_load_object(file.path(working_dir, "pickles", "stage1__stage1.pkl"))
        mod_global_tmp <- py_load_object(file.path(working_dir, "pickles", "stage1__stage1.pkl"))
        df_pred$pred_global <- mod_global_tmp$predict(dat_pred)
        with(df_pred, plot(age_start, pred_global, ylim = c(-18, 0), type = "l", lwd = 3))

        for (f in sr_files) {
          mod_tmp <- py_load_object(file.path(working_dir, "pickles", f))
          df_pred$pred <- mod_tmp$predict(dat_pred)
          with(df_pred, lines(age_start, pred))
        }

      }
    }

  }

  write.csv(df_stage_ids, file.path(working_dir, "df_stage_ids.csv"), row.names = FALSE)
  write.csv(df, file.path(working_dir, "input_data.csv"), row.names = FALSE)
  saveRDS(spline_posteriors, file.path(working_dir, "spline_posteriors.RDS"))


  return(list(
    working_dir = working_dir,
    df_stage_ids = df_stage_ids,
    spline_posteriors = spline_posteriors,
    input_data = df
  ))

}


#' Predict from a cascading spline model
#'
#' \code{predict_spline_cascade} uses a model fit by \code{run_spline_cascade} to make predictions
#'
#' @param fit the output of a \code{run_spline_cascade} function call, or otherwise a list that
#' identifies where on the file system to find the saved cascade model,
#' e.g. \code{list(working_dir = "/path/to/output_dir/model_label/")}
#' @param newdata a data frame with covariate values to use for prediction; must also include the
#' variables passed to \code{stage_id_vars} in \code{run_spline_cascade},
#' (e.g. \code{super_region_id} and \code{location_id}). To predict for levels higher in the
#' hierarchy, pass in \code{NA} values to the stage ID variables lower in the cascade
#' @return data frame with predictions and an ID that identifies which model in the cascade
#' made the prediction
#' @examples
#' require(dplyr)
#' df_pred <- expand.grid(
#'   stringsAsFactors = FALSE,
#'   age_start = seq(0, 100, by = 10),
#'   location_id = c(NA, "102")) %>%
#'     mutate(
#'       age_end = age_start,
#'       data_id = 1:nrow(.),
#'       super_region_name = "High-income"
#'     )
#'
#' preds1 <- predict_spline_cascade(
#'   fit = fit1,
#'   newdata = df_pred
#' )
#'
predict_spline_cascade <- function(fit, newdata) {
  dev <- FALSE
  if (dev) {
    fit <- fit1
    # newdata <- expand.grid(stringsAsFactors = FALSE,
    #                        age_start = seq(0, 100, by = 1),
    #                        location_id = c(NA, "102")) %>%
    #   mutate(
    #     age_end = age_start,
    #     data_id = 1:nrow(.),
    #     super_region_name = "High-income"
    #   )
    newdata <- df_pred
  }

  require(dplyr)

  newdata <- as.data.frame(newdata)
  df_stage_ids <- read.csv(file.path(fit$working_dir, "df_stage_ids.csv"), as.is = TRUE)
  stage_id_vars <- names(df_stage_ids)[!names(df_stage_ids) == "stage1"]

  all_pred_model_ids <- do.call("c", lapply(rev(stage_id_vars), function(x) {
    paste0(x, "__", unique(df_stage_ids[, x]))
  }))

  if (!all(stage_id_vars %in% names(newdata))) {
    stop(paste0(
      "All 'stage_id_vars' variables must be present in the 'newdata' data frame. ",
      "To predict with upstream models in the cascade, use NA values ",
      "for the downstream 'stage_id_vars' variables"
    ))
  }

  ob <- py_load_object(file.path(fit$working_dir, "pickles/stage1__stage1.pkl"))
  cov_names <- ob$cov_names[!ob$cov_names == "intercept"]

  if (!all(cov_names %in% names(newdata))) {
    stop(paste0(
      "All covariates used in the cascade models must be present in the 'newdata' data frame. ",
      "Missing covariates: ", paste(cov_names[!cov_names %in% names(newdata)], collapse = ", ")
    ))
  }

  pred_stage_ids <- as.data.frame(newdata[, stage_id_vars], stringsAsFactors = FALSE) %>%
    mutate(pred_model = "")
  names(pred_stage_ids) <- c(stage_id_vars, "pred_model")

  for (var in rev(stage_id_vars)) {
    pred_stage_ids <- pred_stage_ids %>%
      mutate(
        pred_model = paste0(pred_model, ";;", paste0(var, "__", !!sym(var)))
      )
  }


  pred_stage_ids$use_this_id <- sapply(1:nrow(pred_stage_ids), function(i) {
    ids <- strsplit(pred_stage_ids[i, "pred_model"], split = ";;")[[1]]
    matching_ids <- ids[ids %in% all_pred_model_ids]
    if (length(matching_ids) > 0) {
      matching_ids[1]
    } else {
      "stage1__stage1"
    }
  })

  newdata$row_id__cascade <- 1:nrow(newdata)
  newdata$cascade_prediction_id <- pred_stage_ids$use_this_id

  df_preds <- do.call("rbind", lapply(unique(newdata$cascade_prediction_id), function(x) {
    dev <- FALSE
    if (dev) {
      x <- newdata$cascade_prediction_id[1]
    }

    mod_tmp <- py_load_object(file.path(fit$working_dir, "pickles", paste0(x, ".pkl")))
    df_tmp <- filter(newdata, cascade_prediction_id == x)
    dat_tmp <- MRData()

    dat_tmp$load_df(
      data = df_tmp,
      col_covs = as.list(cov_names)
    )

    df_tmp$pred <- mod_tmp$predict(data = dat_tmp)
    return(df_tmp)
  })) %>%
    arrange(row_id__cascade) %>%
    select(-row_id__cascade)

  return(df_preds)

}


