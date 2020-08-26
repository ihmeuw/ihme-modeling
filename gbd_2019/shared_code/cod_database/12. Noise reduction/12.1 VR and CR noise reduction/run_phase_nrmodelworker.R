library(stats)
library(MASS)
library(data.table)
library(parallel)

make_factors <- function(data) {
  data$year_id <- as.factor(data$year_id)
  data$age_group_id <- as.factor(data$age_group_id)
  data$country_id <- as.factor(data$country_id)
  data$subnat_id <- as.factor(data$subnat_id)
  return(data)
}

run_model <- function(model_data, cause_id, sex, loc_agg, predict_col) {
  # Setting max iterations of the fisher scoring technique to 100
  # 100 is what we used in our STATA code
  # Old code runs a negative binomial if our poisson converged.
  # Previous comments apply as the process is the same though different model.
  # The negative binomial model loosens the poisson assumption of the mean being
  # equal to the variance in the underlying data. If you have overdispered data,
  # i.e. the data shows variance greater than the mean, negative binomial regression
  # produces similar results with the standard errors likely to be less biased than 
  # the poisson on the same underlying data.

  formula <- determine_formula(model_data, predict_col)
  glm.control(maxit=100)
  # Specify the model parameters
  pois_model <- glm(formula = formula,
                              family = poisson(),
                              data=model_data)
  
  if (SAVE) {
    print("Saving model object")
    save(pois_model, file=paste0(model_object_dir, "/model_", loc_agg, "_", sex, "_", cause_id, "_", launch_set_id, ".rda"))
  }
  pois_converged <- pois_model$converged

  # We are able to check if the maximum likelihood estimation converged 
  print(paste0("Poisson converged? ", pois_converged))
  if (!pois_converged) {
    model_result = fill_with_average(model_data, predict_col)
  } else {
    pois_results <- predict.glm(pois_model, new_data=model_data, se.fit=TRUE)
    # We exponentiate the results as the deaths come out in the log space
    fit_col <- exp(pois_results$fit)
    se_col <- pois_results$se.fit
    model_result <- data.table(fit_col, se_col)
    
    predict_col_name <- paste0('pred_', predict_col)
    se_col_name <- paste0('std_err_', predict_col)
    names(model_result) <- c(predict_col_name, se_col_name)
  
  # This produces the fitted values, including standard error for each 
  # observation. The standard error is for the mean result given each row's
  # specific set of independent variables, i.e. sets of {location, age, year}
  return(model_result)
  }
}

determine_formula <- function(a_df, predict_col) {
  # Return the formula to use based on whether the factor variables
  # have only one value
  
  num_years = length(unique(a_df$year_id))
  num_ages = length(unique(a_df$age_group_id))
  num_countries = length(unique(a_df$country_id))
  num_subnats = length(unique(a_df$subnat_id))
  
  formula <- paste0(predict_col, " ~ offset(log(sample_size))")
  if (num_years > 1) {
    formula <- paste0(formula, " + year_id")
  }
  if (num_ages > 1) {
    formula <- paste0(formula, " + age_group_id")
  }
  if (num_countries > 1) {
    formula <- paste0(formula, " + country_id")
  }
  if (num_subnats > 1) {
    formula <- paste0(formula, " + subnat_id")
  }
  print(formula)
  return(formula)
}

fill_with_average <- function(df, predict_col) {
  # When there are under 6 observations for a sex-cause 
  # we replace the cause fractions with the mean cause_fraction
  # across age and apply that to the rows to get new deaths
  print("Not enough observations, filling with average by age group")
  
  df$index_value <- as.integer(row.names(df))
  df$pred_col_cf <- df[, predict_col, with=FALSE] / df$sample_size
  aggregated <- aggregate(pred_col_cf ~ age_group_id, df, mean)
  names(aggregated)[names(aggregated) == 'pred_col_cf'] <- 'mean_pred_col_cf'
  df <- merge(df, aggregated, by='age_group_id')
  
  df$pred_col_deaths <- df$mean_pred_col_cf * df$sample_size
  df$mean_pred_col_cf <- NULL
  df$std_err <- rep(0, nrow(df))
  predict_col_name <- paste0('pred_', predict_col)
  se_col_name <- paste0('std_err_', predict_col)
  
  # set index back to what it was originally, before merge changed it
  stopifnot(setequal(df$index_value, row.names(df)))
  df <- df[order(df$index_value), ]

  df <- df[, pred_col_deaths, std_err]
  names(df) <- c(se_col_name, predict_col_name)
  
  return(df)
}


coreNum <- function() {
  # Get slots
  if (interactive()) {
    cores <- readline(prompt="Cores in use: ")
  } else cores <- strtoi(Sys.getenv("OMP_NUM_THREADS"))
  cores <- as.numeric(cores)
  
  return(cores)
}


run_model_for_column <- function(predict_col, cause_sex_data, model_group, cause, sex, loc_agg) {
  predicted_col_with_se <- tryCatch({
      run_model(cause_sex_data, cause, sex, loc_agg, predict_col)
    }, error = function(e) {
      fill_with_average(cause_sex_data, predict_col)})
  return(predicted_col_with_se)
}

args <- commandArgs(TRUE)

if (length(args) < 2) {
  model_group <- "VR-BHR"
  data_dir <- paste0("FILEPATH", model_group, "/")
  cause_id <- "491"
  launch_set_id <- 0
  n_draws <- 100
} else {
  
  model_group = as.character(args[1])
  launch_set_id = as.character(args[2])
  n_draws = as.integer(args[3])
  if (length(args) > 3) {
    cause_id <- as.character(args[4])
  } else {
    cause_id <- "NONE"
  }
  data_dir <- paste0("FILEPATH", model_group, "/")
}

PARALLEL <- grepl("VR", model_group)
MATERNAL_HH <- grepl("MATERNAL-HH_SURVEYS", model_group)

NUM_WORKERS <- coreNum()
if (is.na(NUM_WORKERS)) {
  NUM_WORKERS <- 3
}
if (NUM_WORKERS == 0) {
  PARALLEL <- FALSE
}
print(paste0("PARALLEL? ", PARALLEL, "; NUM WORKERS: ", NUM_WORKERS))

SAVE <- FALSE
# save model objects in one separate directory, not having directories by cause
model_object_dir <- paste0(data_dir, "model_objects/")
if (model_group == "VA-SRS-IND") {
  SAVE <- TRUE
}

if (SAVE) {
  # ensure a model object dir exists and is empty of previous ones
  if (file.exists(model_object_dir)) {
    # delete it
    unlink(model_object_dir, recursive=TRUE)
  }
  # create new one (should not need to recursively create this)
  dir.create(model_object_dir)
}

if (cause_id != "NONE") {
  data_dir <- paste0(data_dir, cause_id, "/")
}

original_data <- fread(paste0(data_dir, "FILEPATH", launch_set_id, ".csv"))
data <- make_factors(original_data)

# if deaths is missing or null remake column
if (!('deaths' %in% names(data)) || length(data[is.na(data$deaths) > 0])) {
  data$deaths <- data$cf * data$sample_size
}

data <- data[order(data$year_id),]
sexes <- unique(data$sex_id)
is_loc_agg <- unique(data$is_loc_agg)
result_appended <- data.frame()


if (n_draws > 0){
  # subtract 1 because R and Python have different indexing
  n_draws <- n_draws - 1
  predict_cols <- c("deaths", paste0("draw_", 0:n_draws))
} else {
  predict_cols <- c("deaths")
}


if(PARALLEL) {
  # try three times to make a cluster then set parallel to false
  cl <- tryCatch({
    makeCluster(NUM_WORKERS, type="FORK")
  }, error = function(e) {
    print(e)
    tryCatch({
      makeCluster(NUM_WORKERS, type="FORK")
    }, error = function(e) {
      print(e)
      tryCatch({
        makeCluster(NUM_WORKERS, type="FORK")
      }, error = function(e) {
        print(e)
        NULL
      })
    })
  })
  if (is.null(cl)) {
    print("MAKING CLUSTER FAILED THREE TIMES! Setting PARALLEL to FALSE")
    PARALLEL <- FALSE
  }
}
print(paste0("Running NR models for model group ", model_group))
for(loc_agg in is_loc_agg) {
  print(paste0("Modeling is_loc_agg: ", loc_agg))
  loc_agg_data <- data[data$is_loc_agg == loc_agg,]
  for(sex in sexes) {
    sex_data <- loc_agg_data[loc_agg_data$sex_id == sex,]
    causes <- unique(sex_data$cause_id)
    for (cause in causes) {
      print(paste0("Modeling cause ", cause, " for sex: ", sex))
      cause_sex_data <- sex_data[sex_data$cause_id == cause,]
      if (nrow(cause_sex_data) > 0) {
        if (sum(cause_sex_data$deaths) > 0) {

          if (PARALLEL) {
            predicted_col_with_ses <- parLapply(
              cl, predict_cols, run_model_for_column, cause_sex_data,
              model_group, cause, sex, loc_agg
            )
            pred_cols_df <- do.call(cbind, predicted_col_with_ses)
            cause_sex_data <- cbind(cause_sex_data, pred_cols_df)
          } else {
            for (predict_col in predict_cols) {
              predicted_col_with_se <- run_model_for_column(
                predict_col, cause_sex_data, model_group, cause, sex, loc_agg
              )
              cause_sex_data <- cbind(cause_sex_data, predicted_col_with_se)
            }
          }
          result_appended <- rbind(result_appended, cause_sex_data, fill=TRUE)
        }
      }
    }
  }
}
if (PARALLEL) {
  stopCluster(cl)
}
result_appended$deaths <- NULL
if ("survey_type" %in% names(result_appended)) {
  result_appended$survey_type <- NULL
}
write.csv(result_appended, paste0(data_dir, "/nr_out_", launch_set_id, ".csv"),
          row.names=F)


