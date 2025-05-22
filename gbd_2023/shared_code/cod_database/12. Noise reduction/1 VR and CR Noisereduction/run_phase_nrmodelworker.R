rm(list = ls())
Sys.setenv(MKL_VERBOSE = 0)
library(stats)
library(MASS)
library(data.table)
library(parallel)
library(stringr)
library(lme4)
library(arrow)

source("FILEPATH")

args <- commandArgs(TRUE)

if (length(args) == 0) {
  model_group <- "VR-MDA"
  launch_set_id <- "0"
  nr_process_dir <-
    "FILEPATH"
  n_draws <- 100
  cause_id <- "297"
} else {
  model_group <- as.character(args[1])
  launch_set_id <- as.character(args[2])
  nr_process_dir <- as.character(args[3])
  n_draws <- as.integer(args[4])
  if (length(args) > 4) {
    cause_id <- as.character(args[5])
  } else {
    cause_id <- "NONE"
  }
}

PARALLEL <- FALSE

NUM_WORKERS <- coreNum()
if (is.na(NUM_WORKERS)) {
  NUM_WORKERS <- 3
}
if (NUM_WORKERS == 0) {
  PARALLEL <- FALSE
}

data <- read_and_clean_data(
  nr_process_dir = nr_process_dir,
  model_group = model_group,
  cause_id = cause_id,
  launch_set_id = launch_set_id
)

sexes <- unique(data$sex_id)
is_loc_agg <- unique(data$is_loc_agg)
result_appended <- data.frame()
covariates <- c(
  "age_group_id",
  "country_id",
  "subnat_id",
  ifelse("year_bin" %in% names(data), "year_bin", "year_id")
)


if (n_draws > 0) {
  n_draws <- n_draws - 1
  predict_cols <- c("deaths", paste0("draw_", 0:n_draws))
} else {
  predict_cols <- c("deaths")
}


if (PARALLEL) {
  cl <- tryCatch({
    makeCluster(NUM_WORKERS, type = "FORK")
  }, error = function(e) {
    print(e)
    tryCatch({
      makeCluster(NUM_WORKERS, type = "FORK")
    }, error = function(e) {
      print(e)
      tryCatch({
        makeCluster(NUM_WORKERS, type = "FORK")
      }, error = function(e) {
        print(e)
        NULL
      })
    })
  })
  if (is.null(cl)) {
    PARALLEL <- FALSE
  }
}
for (loc_agg in is_loc_agg) {
  loc_agg_data <- data[data$is_loc_agg == loc_agg, ]
  for (sex in sexes) {
    sex_data <- loc_agg_data[loc_agg_data$sex_id == sex, ]
    causes <- unique(sex_data$cause_id)
    for (cause in causes) {
      cause_sex_data <- sex_data[sex_data$cause_id == cause, ]
      if (nrow(cause_sex_data) > 0) {
        if (sum(cause_sex_data$deaths) > 0) {
          if (PARALLEL) {
            predicted_col_with_ses <-
              parLapply(cl,
                        predict_cols,
                        run_model_for_column,
                        cause_sex_data,
                        covariates)
            model_results <-
              lapply(predicted_col_with_ses, function(l)
                l$model_result)
            converged <-
              lapply(predicted_col_with_ses, function(l)
                l$converged)
            pred_cols_df <- do.call(cbind, model_results)
            cause_sex_data <- cbind(cause_sex_data, pred_cols_df)
          } else {
            converged <- c()
            for (predict_col in predict_cols) {
              predicted_col_with_se <-
                run_model_for_column(predict_col, cause_sex_data, covariates)
              cause_sex_data <-
                cbind(cause_sex_data,
                      predicted_col_with_se$model_result)
              converged = c(converged, predicted_col_with_se$converged)
            }
          }
          converged_report <- paste(predict_cols,
                                    converged,
                                    sep = " = ",
                                    collapse = ", ")
          print(
            str_interp(
              "Model converged? loc_agg = ${loc_agg}, sex = ${sex}, cause = ${cause}, ${converged_report}"
            )
          )
          cause_sex_data[, average_prior_sample_size := mean(sample_size), by = age_group_id]
          result_appended <- rbind(result_appended, cause_sex_data)
        }
      }
    }
  }
}
if (PARALLEL) {
  stopCluster(cl)
}

save_data(
  result_appended,
  nr_process_dir = nr_process_dir,
  model_group = model_group,
  cause_id = cause_id,
  launch_set_id = launch_set_id
)
