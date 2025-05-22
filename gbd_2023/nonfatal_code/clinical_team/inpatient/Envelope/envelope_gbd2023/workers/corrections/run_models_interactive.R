# Title: Produce sex-specific age-splitting model with cascading splines
# Input: dt informing splines and prediction frame from format_split_inputs.R
# Output: filled prediction frame with stage2_mean, stage2_upper, stage2_lower per every country-year having any data points requiring splitting
# Author: USERNAME

# Workflow explained:
# The inputs are the age/sex specific subset of bundle data and a prediction frame with location-year-age-sex rows that we predict for
# After setting the model parameters, we run a model and then generate predictors in two stages

# Stage 1 is a MR-BRT model producing global predictors - these are the same for all country-years.
# To compute those, we use fitStage1Model() and then setReEstimationAndPredictStage1(),
# and get a prediction frame with stage1_mean, stage1_lower, and stage1_upper.

## Import and define MR-BRT and cascading splines functions

### Import reticulate to source from Python
library(reticulate)

### Initialize object for mrtool (MR-BRT)
mrtool <- NULL
xwalk <- NULL # don't need this here

###  Import mrtool ----
.onLoad <- function(libname, pkgname) {
  use_python(python = "FILEPATH")
  mrtool <<- import("mrtool")
  for (nm in names(mrtool)) assign(nm, mrtool[[nm]], parent.env(environment()))

  xwalk <<- import("crosswalk")
  for (nm2 in c("linear_to_log", "linear_to_logit", "log_to_linear", "logit_to_linear")) {
    assign(nm2, xwalk[["utils"]][[nm2]], parent.env(environment()))
  }
}

.onLoad()

mr <- mrtool

path <- paste0(code_dir, "utils/split_utils.R")
source(path)


# create outputs/ within run_dir/data/splines/ if it doesn't exist yet
if (!dir.exists(paste0(run_dir, "data/splines/outputs/"))) {
  cat("Creating output directory...\n")
  dir.create(paste0(run_dir, "data/splines/outputs/"), recursive = TRUE)
}

# Import input data and prediction frame ----
config_path <- paste0(top_dir, "FILEPATH")
config <- openxlsx::read.xlsx(config_path) # read in the config.xlsx file
config <- as.data.table(config)

# Prompt for user input
model_index_id_input <- as.integer(readline(prompt = "Please enter the model_index_id: "))
filtered_config <- filter(config, model_index_id == model_index_id_input)

# Assuming filtered_config contains only one row, convert it to a list
if (nrow(filtered_config) == 1) {
  config_list <- as.list(filtered_config)
  names(config_list) <- names(filtered_config)
} else {
  warning("Filtered configuration does not uniquely identify a single row.")
}
config_list

## Specify IDs
location_set_id <- 35
bundle_id <- 7919
release_id <- 16
age_group_set_id <- 24
bundle_version_id <- 43245
model_index_id <- config_list$model_index_id

## Input for splines
dt <- fread("FILEPATH")


### Check for missing values
na_counts <- sapply(dt, function(x) sum(is.na(x)))
total_na <- sum(na_counts)

if (total_na == 0) {
  cat("Cascading splines input data has no missing values. Proceeding...\n")
} else {
  missing_info <- na_counts[na_counts > 0]
  cat(sprintf(
    "Cascading splines input data has %d missing values in the following columns: %s\n",
    total_na, toString(names(missing_info))
  ))
}


## Load prediction frame
# a data table to be filled in with prediction coefficients from cascading splines
pred_frame <- fread("FILEPATH")

### Check for missing values
na_counts <- sapply(pred_frame, function(x) sum(is.na(x)))
total_na <- sum(na_counts)

if (total_na == 0) {
  cat("Prediction frame for cascading splines has no missing values. Proceeding...\n")
} else {
  missing_info <- na_counts[na_counts > 0]
  cat(sprintf(
    "Prediction frame for cascading splines has %d missing values in the following columns: %s\n",
    total_na, toString(names(missing_info))
  ))
}

# Configure the model

## Sex-specific config
model_parameters <- list(
  Male = list(
    covariate_list = list("age_start", "age_end"),
    # specify frequency or domain knots here. an ifelse statement in the modeling function
    # will pull the information from this config and plcae the knots accordingly
    spline_knots_type = as.character(config_list$spline_knots_type),
    n_knots = as.integer(config_list$knot_count_male),
    domain_knots_ages = as.numeric(unlist(strsplit(config_list$domain_knots_ages_male, ","))),
    # quadratic (2L) or cubic (3L) spline
    spline_degree = as.integer(config_list$spline_degree),
    # left and right linear tails
    spline_r_is_linear = as.logical(config_list$spline_r_is_linear),
    spline_l_is_linear = as.logical(config_list$spline_l_is_linear),
    # set maximum order derivative for the tails
    r_tail_maxder_lower = as.numeric(config_list$r_tail_maxder_lower),
    r_tail_maxder_upper = as.numeric(config_list$r_tail_maxder_upper),
    l_tail_maxder_lower = as.numeric(config_list$l_tail_maxder_lower),
    l_tail_maxder_upper = as.numeric(config_list$l_tail_maxder_upper),
    stage_id_vars = as.character(unlist(strsplit(config_list$stage_id_vars, ",")))
  ),
  Female = list(
    # asfr_x_ifd = age-specific fertility rate times in-facility delivery ratio per location-year
    covariate_list = list("age_start", "age_end", "asfr_x_ifd"),
    spline_knots_type = as.character(config_list$spline_knots_type),
    n_knots = as.integer(config_list$knot_count_female),
    domain_knots_ages = as.numeric(unlist(strsplit(config_list$domain_knots_ages_female, ","))),
    spline_degree = as.integer(config_list$spline_degree),
    spline_r_is_linear = as.logical(config_list$spline_r_is_linear),
    spline_l_is_linear = as.logical(config_list$spline_l_is_linear),
    r_tail_maxder_lower = as.numeric(config_list$r_tail_maxder_lower),
    r_tail_maxder_upper = as.numeric(config_list$r_tail_maxder_upper),
    l_tail_maxder_lower = as.numeric(config_list$l_tail_maxder_lower),
    l_tail_maxder_upper = as.numeric(config_list$l_tail_maxder_upper),
    stage_id_vars = as.character(unlist(strsplit(config_list$stage_id_vars, ",")))
  )
)

# adjust the flexibility of the curve per each level of the cascade (greater values, higher flex)
# making sure the length is the same as # stage_id_vars
# Now define and append thetas for each gender separately. That is because it is dependent on stage_id_vars
# that has to be defined prior.
model_parameters$Male$thetas <- c(10000, rep(10000, length(model_parameters$Male$stage_id_vars) - 1))
model_parameters$Female$thetas <- c(10000, rep(10000, length(model_parameters$Female$stage_id_vars) - 1))

# set uniform prior to linear tales
model_parameters[["Male"]]$maxder_uniform <- cbind(
  matrix(c(model_parameters[["Male"]]$l_tail_maxder_lower, model_parameters[["Male"]]$l_tail_maxder_upper), ncol = 1),
  do.call("cbind", lapply(1:(model_parameters[["Male"]]$n_knots - 3), function(x) matrix(c(-Inf, Inf), ncol = 1))),
  matrix(c(model_parameters[["Male"]]$r_tail_maxder_lower, model_parameters[["Male"]]$r_tail_maxder_upper), ncol = 1)
)

model_parameters[["Female"]]$maxder_uniform <- cbind(
  matrix(c(model_parameters[["Female"]]$l_tail_maxder_lower, model_parameters[["Female"]]$l_tail_maxder_upper), ncol = 1),
  do.call("cbind", lapply(1:(model_parameters[["Female"]]$n_knots - 3), function(x) matrix(c(-Inf, Inf), ncol = 1))),
  matrix(c(model_parameters[["Female"]]$r_tail_maxder_lower, model_parameters[["Female"]]$r_tail_maxder_upper), ncol = 1)
)



# Spread frequency knots from 0 to 100 to allow enev data distribution
## Set the type of spline knots ----
# (frequency vs. domain)
spline_knots_type <- config_list$spline_knots_type

if (config_list$spline_knots_type == "frequency") {
  model_parameters[["Male"]]$spline_knots <- array(seq(0, 1, length.out = model_parameters[["Male"]]$n_knots))
  model_parameters[["Female"]]$spline_knots <- array(seq(0, 1, length.out = model_parameters[["Female"]]$n_knots))
} else if (config_list$spline_knots_type == "domain") {
  model_parameters[["Male"]]$spline_knots <- array(model_parameters[["Male"]]$domain_knots_ages / 100) # should be bound by [0,1]
  model_parameters[["Female"]]$spline_knots <- array(model_parameters[["Female"]]$domain_knots_ages / 100)
}



# Modeling ----

# COMPUTE GLOBAL MODEL ----

sexes <- c("Male", "Female") # loop over those
asfr_reest_level <- "global" # effect of the cov re-estimated freely for each location
nmx_reest_level <- "global" # effect of the cov fixed at the global level with a strict uniform prior


# try fitting global model for sex == "Female"
# This function returns the global model
fitStage1Model <- function(dt, model_parameters, model_sex) {
  # MRData is for formatting input data and for constructing a prediction frame
  # initialize the environment with model metadata
  model_metadata <- mr$MRData()

  # filter dt to sex-specific inputs
  dt_subset <- dt %>% filter(sex == model_sex)

  # populate the environment
  model_metadata$load_df(
    data = dt_subset, # indicate the sex-specific input dataset informing c-splines
    col_obs = "log_mean", # indicate the log-transformed dependent variable
    col_obs_se = "log_se", # indicate the standard error variable for the dependent variable
    # make model_parameters[[sex]]$covariate_list a character vector to avoid errors
    col_covs = unlist(model_parameters[[model_sex]]$covariate_list), # indicate the covariates
    col_study_id = "ranef_id"
  ) # indicate the RANdom EFfect ID (level_3_location_id_year_id_sex [e.g., 47_2012_Female])

  # extract model parameters for the specified sex
  params <- model_parameters[[model_sex]]

  # initialize cov_models list with elements common to both sexes
  cov_models_list <- list(
    mr$LinearCovModel("intercept", use_re = TRUE),
    mr$LinearCovModel("mean_nmx"),
    mr$LinearCovModel(
      alt_cov = c("age_start", "age_end"),
      use_spline = TRUE,
      spline_degree = unlist(params$spline_degree),
      spline_knots_type = unlist(params$spline_knots_type),
      spline_knots = unlist(params$spline_knots),
      spline_r_linear = unlist(params$spline_r_is_linear),
      spline_l_linear = unlist(params$spline_l_is_linear),
      prior_spline_maxder_uniform = params$maxder_uniform,
      name = "age_spline"
    )
  )

  # Conditionally add "asfr_x_ifd" for "Female"
  if (model_sex == "Female") {
    cov_models_list <- c(cov_models_list, mr$LinearCovModel("asfr_x_ifd"))
  }

  # Note: The order of models in the list might matter based on how they are processed in MRBRT.
  # Make sure to arrange them as required. In this example, "asfr_x_ifd" is added for females,
  # as shown in your provided structure.

  # set the global model parameters and fit the model with the dynamically constructed list
  stage1_model <- mr$MRBRT(
    data = model_metadata,
    cov_models = cov_models_list,
    inlier_pct = 0.95 # 10% trimming
  )

  # fit the global model
  stage1_model$fit_model()

  return(stage1_model)

  # to make sure the model is not overwritten, reassign the model to a sex-specific object
  print(sex)
  print(stage1_model)
  assign(paste0("stage1_model_", sex), stage1_model, envir = .GlobalEnv)
}

# initialize a list containing the models
models_list <- list()

# store each model in the list
sexes <- c("Female", "Male")
for (i in sexes) {
  print(paste("Computing stage 1 model for", i))
  models_list[[paste0("stage1_model_", i)]] <- fitStage1Model(
    dt = dt,
    model_parameters = model_parameters,
    model_sex = i
  )
}

# access models directly from the list
stage1_model_Male <- models_list[["stage1_model_Male"]]
stage1_model_Female <- models_list[["stage1_model_Female"]]

# models vector
stage1_models <- names(models_list)




# COMPUTE GLOBAL PREDICTORS ----

# This function applies the stage1_model from the last function and fills in the prediction
# frame with global prediction coefficients. The function accounts for whether we want
# the effects of the covariates re-estimated freely per each location or fixed at the global level.
setReEstimationAndPredictStage1 <- function(model_sex,
                                            pred_frame,
                                            asfr_reest_level,
                                            nmx_reest_level,
                                            get_ui = FALSE,
                                            n_draws) {
  # identify the sex-specific global model by object name (stage1_model_Male vs. stage1_model_Female)
  stage1_model <- get(paste0("stage1_model_", model_sex))

  # for nmx
  if (nmx_reest_level == "global") {
    estimated_beta_nmx <- stage1_model$summary()[[1]][1, "nmx"]
    stage1_model$cov_models[[4]]$prior_beta_uniform <- matrix(rep(estimated_beta_nmx, 3), ncol = 1)
  }

  # for age-specific fertility rate
  if (model_sex == "Female") {
    if (asfr_reest_level == "global") {
      estimated_beta_asfr <- stage1_model$summary()[[1]][1, "asfr_x_ifd"]
      stage1_model$cov_models[[3]]$prior_beta_uniform <- matrix(rep(estimated_beta_asfr, 2), ncol = 1) # asfr_x_ifd is 3rd - find out with View(stage1_model_Female)
    }
  }

  spline_idx <- which(stage1_model$cov_model_names == "age_spline") # spline index

  # initialize another mr-brt object (like we did for the stage 1 model)
  dat_pred_frame <- mr$MRData()

  # extract model parameters for the specified sex
  params <- model_parameters[[model_sex]]

  # subset to sex specific pred_frame
  pred_frame_subset <- pred_frame %>% filter(sex == model_sex)

  # populate the environment
  dat_pred_frame$load_df(
    data = pred_frame_subset,
    # indicate the covariates from the config
    col_covs = model_parameters[[model_sex]]$covariate_list # should be list, not vector
  )

  # compute 95% UI (lower and upper) from fixed effects
  # take samples (draws) from the variance-covariance matrix using the sample_soln function
  n_draws <- as.integer(n_draws) # number of samples to draw, important to pass it an integer

  samples1 <- stage1_model$sample_soln(n_draws)

  draws1 <- stage1_model$create_draws(
    data = dat_pred_frame,
    beta_samples = samples1[[1]],
    gamma_samples = matrix(rep(0, n_draws), ncol = 1),
    random_study = FALSE
  )

  # compute predictions using the global fit with UI
  pred_frame_subset$stage1_mean <- stage1_model$predict(data = dat_pred_frame) # changes LHS to df from dat (MR-BRT object)

  # if get_ui is TRUE, we will compute the 95% UI for the predictions
  if (get_ui == TRUE) {
    pred_frame_subset$stage1_lower <- apply(draws1, 1, function(x) quantile(x, 0.025))
    pred_frame_subset$stage1_upper <- apply(draws1, 1, function(x) quantile(x, 0.975))
  } else {
    cat("Skipping UI sampling per user input.")
  }

  # to make sure the model is not overwritten, reassign the model to a sex-specific object and return
  assign(paste0("pred_frame_stage1_", model_sex), pred_frame_subset, envir = .GlobalEnv)
  return(pred_frame_subset)
}

# execute the function and compute the sex-specific prediction frames with global predictors
asfr_reest_level <- "global" # effect of the cov re-estimated freely for each location
nmx_reest_level <- "global" # effect of the cov fixed at the global level with a strict uniform prior
sexes <- c("Female", "Male")
for (i in sexes) {
  print(paste("Filling in stage 1 predictors with 95% UI in the prediction frame for", i))
  setReEstimationAndPredictStage1(
    model_sex = i,
    pred_frame = pred_frame,
    asfr_reest_level = asfr_reest_level,
    nmx_reest_level = nmx_reest_level,
    get_ui = FALSE,
    n_draws = 1
  )
}



# STAGE 2 Model ----


# This function runs location-specific c-splines and fills in the prediction frame with location-specific
# prediction coefficients.
runSplineCascade <- function(model_sex,
                             stage1_model_object,
                             dt,
                             col_obs = "log_mean",
                             col_obs_se = "log_se",
                             col_study_id = "ranef_id",
                             gaussian_prior_is_true = TRUE,
                             overwrite_previous_is_true = TRUE) {
  # reference the prediction frame and the input data
  pred_frame <- get(paste0("pred_frame_stage1_", model_sex))

  # reference the input data
  dt_subset <- dt %>% filter(sex == model_sex)

  # ensure we have model_label defined or passed appropriately for each sex
  model_label <- paste0(model_sex, "_", model_index_id, "_", gsub("[-: ]", "_", Sys.time()))

  # stage1_model_object is the global model
  stage1_model_object <- get(paste0("stage1_model_", model_sex))

  # source run_spline_cascade2 function
  source(paste0("FILEPATH"))

  # run the second stage of the spline cascade model
  stage2_model <- run_spline_cascade2(
    stage1_model_object = stage1_model_object,
    df = dt_subset,
    col_obs = col_obs,
    col_obs_se = col_obs_se,
    col_study_id = col_study_id,
    stage_id_vars = model_parameters[[model_sex]]$stage_id_vars, # location-year, location, region, s-region
    thetas = model_parameters[[model_sex]]$thetas,
    output_dir = config_list$output_dir,
    model_label = model_label,
    gaussian_prior = gaussian_prior_is_true,
    overwrite_previous = gaussian_prior_is_true
  )

  # to make sure the model is not overwritten, reassign the model to a sex-specific object and return
  assign(paste0("stage2_model_", model_sex), stage2_model, envir = .GlobalEnv)

  # need this to save pred_tmp in the same dir
  assign(paste0("model_label_", model_sex), model_label, envir = .GlobalEnv) # produces objects model_label_Male and model_label_Female later used in naming

  # it's important to return the objects in a list; otherwise, the function will only execute the 1st `return` statement, because
  # an R function will exit as soon as it encounters one.
  return(list(stage2_model = stage2_model, model_label = model_label))
}


# this function will produce pickle files with model results. each pickle file corresponds to a single model
# one model per cascade_prediction_id
sexes <- c("Female", "Male")
# sexes <- "Male"

for (i in sexes) {
  print(paste("Computing stage 2 cascading splines model for", i))
  stage1_model <- get(paste0("stage1_model_", i))
  result <- runSplineCascade(
    stage1_model_object = stage1_model,
    model_sex = i,
    dt = dt,
    col_obs = "log_mean",
    col_obs_se = "log_se",
    col_study_id = "ranef_id",
    gaussian_prior_is_true = TRUE,
    overwrite_previous_is_true = TRUE
  )

  # Access the model and label
  stage2_model <- result$stage2_model
  model_label <- result$model_label

  # Assuming you want to do something with stage2_model and model_label here
  # For example, print the model_label
  print(model_label)
}


# this function will generate mean, lower, and upper from cascading splines (stage2_model)!
predictSplineCascadeMean <- function(model_sex) {
  pred_frame <- get(paste0("pred_frame_stage1_", model_sex)) # read in sex-specific pred_frame with stage1 predictors
  stage2_model <- get(paste0("stage2_model_", model_sex))

  # generate location-year specific predictors using the stage2_model
  print(paste("Generating stage 2 mean predictor values for", model_sex))

  pred_tmp <- predict_spline_cascade(
    fit = stage2_model,
    newdata = pred_frame
  ) # at this point pred_frame has stage1_pred, stage1_lower, stage1_upper
  pred_tmp <- as.data.table(pred_tmp) # for further processing with data.table

  # the predict_spline_cascade() function generates `pred` variable, rename.
  setnames(pred_tmp, old = "pred", new = "stage2_mean")

  # to make sure the model is not overwritten, reassign the model to a sex-specific object and return
  assign(paste0("pred_frame_stage2_mean_", model_sex), pred_tmp, envir = .GlobalEnv)
  return(pred_tmp)
}

# execute the function to get pred_frame_final_Female and pred_frame_final_Male
sexes <- c("Female", "Male")

for (i in sexes) {
  # print(paste("Filling in location-year specific predictors with lower and upper in the prediction frame for", i))
  predictSplineCascadeMean(model_sex = i)
}

# output directories labeled with sex_modelID_timestamp
output_dir_Female <- paste0("FILEPATH", model_label_Female, "/")
output_dir_Male <- paste0("FILEPATH", model_label_Male, "/")

# save prediction frames with means (without uncertainty)
# If paste0(home_dir, "config_backups/") doesn't exist, create one
# Create prediction_frames under output_dir if doesn't exist
if (!dir.exists(paste0(output_dir_Male, "prediction_frames/"))) {
  dir.create(paste0(output_dir_Male, "prediction_frames/"))
}

if (!dir.exists(paste0(output_dir_Female, "prediction_frames/"))) {
  dir.create(paste0(output_dir_Female, "prediction_frames/"))
}

# Saving
stage2_predictors_Male_path <- paste0(output_dir_Male, "prediction_frames/pred_frame_stage2_mean_", model_label_Male, ".csv")
stage2_predictors_Female_path <- paste0(output_dir_Female, "prediction_frames/pred_frame_stage2_mean_", model_label_Female, ".csv")

fwrite(pred_frame_stage2_mean_Male, stage2_predictors_Male_path)
fwrite(pred_frame_stage2_mean_Female, stage2_predictors_Female_path)

## Save input data with inliers and outliers

### Get a data frame with estimated weights

#### Male
dt_stage1_Male <- cbind(stage1_model_Male$data$to_df(), data.frame(w = stage1_model_Male$w_soln)) %>%
  mutate(sex = "Male") %>%
  as.data.table()

#### Female
dt_stage1_Female <- cbind(stage1_model_Female$data$to_df(), data.frame(w = stage1_model_Female$w_soln)) %>%
  mutate(sex = "Female") %>%
  as.data.table()

### Process input data with outliers for plotting

#### merge on dt to get `w` (1 = inlier, !=1 = outlier)
dt_stage1_Both <- rbind(dt_stage1_Male, dt_stage1_Female, fill = TRUE)
dt_stage1_Both <- dt_stage1_Both %>%
  select(study_id, age_start, age_end, w) %>%
  unique()
setnames(dt_stage1_Both, old = "w", new = "is_inlier")

#### Warn if n_distinct(dt_stage1_Both) is not equal to nrow(dt_stage1_Both) otherwise print it's alright
if (n_distinct(dt_stage1_Both) != nrow(dt_stage1_Both)) {
  warning("There are duplicated rows in dt_stage1_Both")
} else {
  print("No duplicated rows in input data table with inliers for both males and females")
}

### Now the input data contains `is_inlier` binary indicator, will be used for plotting
dt_with_inliers <- merge(dt, dt_stage1_Both,
  by.x = c("age_start", "age_end", "ranef_id"),
  by.y = c("age_start", "age_end", "study_id"), all.x = TRUE
)

#### Split into sex-specific input data tables
dt_with_inliers_Male <- dt_with_inliers %>% filter(sex == "Male")
dt_with_inliers_Female <- dt_with_inliers %>% filter(sex == "Female")

#### Save sex-specific input data tables with inliers
fwrite(dt_with_inliers_Male, paste0(output_dir_Male, "/input_total_with_inliers_", model_label_Male, ".csv"))
fwrite(dt_with_inliers_Female, paste0(output_dir_Female, "/input_total_with_inliers_", model_label_Female, ".csv"))
