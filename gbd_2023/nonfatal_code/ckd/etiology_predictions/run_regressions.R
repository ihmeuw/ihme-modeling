# ------------------------------------------------------------------------------
# Project: Non-fatal GBD
# Purpose: Create stage-specific etiology proportions for all age/sex/years in 
# a location -- adjust DM1/2 props based on DM1/2 prev for that location.
#
# This step does the following:
# 1. Source regression coefficient + variance covariance matrix & 
# create prediction matrix.
# 2. Run functions to generate draws of your predictions.
# 3. Run function to process those draws so they're in proportion space + 
# combine other/unknown.
# ------------------------------------------------------------------------------

# ---SETUP----------------------------------------------------------------------

rm(list = ls())

# ---LOAD LIBRARIES-------------------------------------------------------------

library(assertable)
library(openxlsx)

# ---SET OBJECTS----------------------------------------------------------------

message("Read in input arguments")
args <- commandArgs(trailingOnly = TRUE)

loc_id <- as.numeric(args[1])
dm_1_me <- as.numeric(args[2])
dm_2_me <- as.numeric(args[3])
dm_correction <- args[4]
general_funcs <- args[5]
predict_funcs <- args[6]
draw_processing_funcs <- args[7]
validation_funcs <- args[8]
plotting_funcs <- args[9]
coef_filepath <- args[10]
covmat_filepath <- args[11]
results_dir <- args[12]
diagnostics_dir <- args[13]
release_id <- as.numeric(args[14])
extrapolate_under_20 <- as.logical(args[15])

message(paste("location_id:", loc_id))
message(paste("Diabetes type 1:", dm_1_me))
message(paste("Diabetes type 2:", dm_2_me))
message(paste("Diabetes correction:", dm_correction))
message(coef_filepath)
message(covmat_filepath)
message(results_dir)
message(diagnostics_dir)
message(release_id)
message(extrapolate_under_20)


# ---SOURCE FUNCTIONS-----------------------------------------------------------

source(general_funcs)
source(predict_funcs)
source(draw_processing_funcs)
source(validation_funcs)
source(plotting_funcs)
source_shared_functions(c("get_demographics", 
                          "get_draws", 
                          "get_age_metadata", 
                          "get_ids"))

# ---READ INPUT FILES-----------------------------------------------------------
# Read in regression coefficient and variance covariance-matrix. This data comes 
# from a cohort analysis preformed by the CKDPC collaborators
# ------------------------------------------------------------------------------

message("Pulling coefficient values & variance-covariance matrix. 
        Creating predictor matrix.")
# Source coefficient values
coefficients <- as.data.table(openxlsx::read.xlsx(coef_filepath))
# Assign the relative risk ratio (rrr) column to the logarithm of the rrr.
coefficients[, rrr := log(rrr)]

# Source variance covariance matrix. 
vcovars <- as.data.table(read.csv(covmat_filepath))
vcovars[, beta := NULL]

# ---CREATE PREDICTORS----------------------------------------------------------
# Generate variables to predict for.
# ------------------------------------------------------------------------------

# Obtain age metadata to merge with the variance covariance data set.
ages <- get_age_metadata(release_id = release_id)
ages <- ages[, c('age_group_id',
                 'age_group_name',
                 'age_group_years_start', 
                 'age_group_years_end')]

# Create data.table of variables to predict for.
predictors <- as.data.table(expand.grid(sex_id = c(1, 2), 
                                        age_group_id = c(ages[, age_group_id]), 
                                        stage = unique(coefficients[, stage])))
predictors <- merge(predictors, ages, by = "age_group_id")

# Fit the age curve of the predictors by centering the age group metadata.
if (extrapolate_under_20) {
  predictors[, age := center_age(age_group_years_start = age_group_years_start, 
                                 age_group_years_end = age_group_years_end)]
  
} else {
  predictors[age_group_years_start >= 20, 
             age := center_age(age_group_years_start = age_group_years_start, 
                               age_group_years_end = age_group_years_end)]
  predictors[age_group_years_start < 20, 
             age := center_age(age_group_years_start = 20, 
                               age_group_years_end = 24)]
  
}

predictors[, age2 := age^2]
predictors[, const := 1]

# ---RUN STAGE PREDECTIONS------------------------------------------------------
# Use a regression to predict for each specified predictor for each stage and 
# etiology of CKD.
# ------------------------------------------------------------------------------

predictions <- create_stage_etio_predictions(predictors = predictors, 
                                             coefficients = coefficients, 
                                             vcovars = vcovars)

message("extrapolate_under_20: ", extrapolate_under_20)
if (!extrapolate_under_20) {
  predictions[, c("age_group_years_start", 
                  "age_group_years_end", 
                  "age_group_name") := NULL]
  predictions <- merge(predictions, ages, by = "age_group_id", all = T)
}

# ---PROCESS DRAWS--------------------------------------------------------------
# Execute the following processing steps to complete the predictions:
# 1. Exponentiate prediction draws to obtain odds ratios.
# 2. Generate the denominator.
# 3. Fill in RRR as 1 for the base category.
# 4. Calculate probability.
# 5. Aggregate the other and unknown etiologies into a single etiology
# represented by other.
# ------------------------------------------------------------------------------

message("Processing draws")

# Exponentiate prediction draws
predictions <- exponentiate_draws(draws = predictions)

# Generate the denominator of the Odds ratios.
predictions <- create_denominator_draws(draws = predictions,
                                        aggregation_cols = c("stage", 
                                                             "sex_id", 
                                                             "age_group_id"),
                                        denominator_col = "denom_")

# Fill in RRR as 1 for the base category because that is saying the relative 
# risk between the base group and itself is the same which is what we'd expect.
draw_columns <- paste0("draw_", 0:999)
predictions[etio == "unknown", (draw_columns) := 1]

# Calculate probability
predictions <- convert_draws_to_prob(draws = predictions,
                                     prob_col = "prop_",
                                     denominator_col = "denom_")

# Aggregate other and unknown
message("Aggregating proportions for CKD due to other causes and CKD due to unknown causes")
# Separate other and unknown from dm1/dm2/htn/gn
other_ckd <- copy(predictions)
non_oth_etios <- copy(predictions)
other_ckd <- other_ckd[etio %in% c("oth", "unknown")]
non_oth_etios <- non_oth_etios[!(etio %in% c("oth", "unknown"))]
# Melt long
other_ckd <- melt(other_ckd, 
                  id.vars = c("age_group_id", 
                              "sex_id", 
                              "stage", 
                              "age_group_name", 
                              "age_group_years_start", 
                              "age_group_years_end", 
                              "age", 
                              "age2", 
                              "const", 
                              "etio"))
# Aggregate etiologies at the draw level
other_ckd <- other_ckd[, 
                       sum(value), 
                       by = c("age_group_id", 
                              "sex_id", 
                              "stage", 
                              "age_group_name", 
                              "age_group_years_start", 
                              "age_group_years_end", 
                              "age", 
                              "age2", 
                              "const", 
                              "variable")]
# Label as "other"
other_ckd[, etio := "oth"]
# Cast wide
other_ckd <- dcast(other_ckd, 
                   formula = age_group_id + sex_id + stage + 
                     age_group_name + age_group_years_start + 
                     age_group_years_end + age + age2 + const + etio ~ variable, 
                   value.var = "V1")
predictions <- rbindlist(list(non_oth_etios, other_ckd), use.names = T)

message("Done processing draws")

# ---FORMAT PRE-DIABETES CORRECTION DRAWS---------------------------------------
# The predictions do not contain years or the measure_id. This section pulls the 
# demographic metadata necessary to merge all year_ids onto the predictions.
# ------------------------------------------------------------------------------

# Fix all the metadata stuff
# Check the squareness of the predictions here. 
age_group_ids <- unique(predictions$age_group_id)
sex_ids <- unique(predictions$sex_id)
stages <- unique(predictions$stage)
etios <- unique(predictions$etio)
prediction_ids <- list(age_group_id = age_group_ids, 
                       sex_id = sex_ids,
                       stage = stages,
                       etio = etios)
assertable::assert_ids(data = predictions, id_vars = prediction_ids)

predictions[, measure_id := 18]

# Create a data.table of all the id combos for which we need results in-order to 
# add year_ids  for the predictions (given by get_demographics). Please note 
# that the list of location_id is dropped since this code is parallelizing by 
# location and only one location is included as a result. 
demographics <- get_demographics("epi", release_id = release_id)
demographics[["location_id"]] <- NULL

template <- as.data.table(expand.grid(demographics))
predictions <- merge(predictions, 
                     template, 
                     by = c("age_group_id", "sex_id"), 
                     allow.cartesian = T)

# ---EXECUTE DIABETES CORRECTION------------------------------------------------
# Ensure the prevalence of CKD due to diabetes type 1 and CKD due to diabetes 
# type 2 is less than prevalence of diabetes type 1 and diabetes type 2 in each 
# location/year/age/sex.To ensure this, we calculate for each diabetic subtype 
# (e) for a given location (l), age (a), and sex (g) the ratio of 
# subtype-specific diabetes prevalence to total diabetes prevalence (r). With 
# this ratio we adjust the proportion of CKD due to a given diabetic subtype (p) 
# for a given CKD stage (s), l, a, and g by scaling the predicted proportion of 
# CKD due to that subtype (k) by the ratio of total DM due to e in l to the 
# ratio of total DM due to e in the United States (USA). 
#
# Estimates for the U.S. are used as the base of the ratio because the Geisinger 
# data is from U.S., so estimates for the U.S. are effectively the gold standard
# and are used to create the standardized correction elsewhere. No corrections 
# are done to the U.S. estimates for this reason.
# ------------------------------------------------------------------------------

if (dm_correction) {
  if (loc_id != 102) {
    message("Beginning diabetes correction process")
    
    # Set draw names
    draw_columns <- paste0("draw_", 0:999)
    
    # Get draws for given meids, location and the U.S.
    message(paste("Getting diabetes draws (meid",dm_1_me,"for dm1 and meid",
      dm_2_me,"for dm2 for U.S. and loc_id", loc_id))
    dm_draws <- get_draws(
      gbd_id_type = "modelable_entity_id",
      gbd_id = c(dm_1_me, dm_2_me),
      source = "epi",
      measure_id = 5,
      age_group_id = unique(template$age_group_id),
      location_id = c(102, loc_id),
      sex_id = c(1, 2),
      release_id = release_id
    )
    
    message("Dm model versions")
    mvid <- unique(dm_draws$model_version_id)
    message(paste0(mvid, ","))
    
    # Adding on additional metadata for diagnostic purposes
    age_names <- copy(ages)
    age_names <- age_names[, c('age_group_id', 'age_group_name')]
    dm_draws <- merge(dm_draws, age_names, by = "age_group_id")
    loc_name <- get_ids("location")
    loc_name <- loc_name[, c('location_id', 'location_name')]
    dm_draws <- merge(dm_draws, loc_name, by = "location_id")
    
    # Melt draws
    message("Processing dm draws")
    id_vars <- c("age_group_id", 
                 "sex_id", 
                 "year_id", 
                 "location_id", 
                 "modelable_entity_id",
                 "location_name",
                 "age_group_name")
    dm_draws_long <- melt(dm_draws[, 
                                   -c("model_version_id", 
                                        "measure_id", 
                                        "metric_id"), 
                                   with = F], 
                          id.vars = id_vars, 
                          variable.name = "draw",
                          value.name = "prev")
    
    # Check to determine if proportions are over 1
    check_matching_conditions(dataset = dm_draws_long, 
                              val_col = "prev", 
                              value = 1, 
                              evaluation = "lte", 
                              descriptor = paste0("original_draws_loc_", 
                                                  loc_id),
                              release_id = release_id,
                              output_path = diagnostics_dir)
    
    # Switch to using the etiologies as the main identifier instead of the 
    # modelable_entity_id.
    dm_draws_long[, etio := ifelse(modelable_entity_id == dm_1_me, 
                                   "dm1", 
                                   "dm2")]
    dm_draws_long[, modelable_entity_id := NULL]
    
    # Cast wide by diabetes type
    dm_draws_long <- dcast(
      data = dm_draws_long, 
      formula = year_id + age_group_id + age_group_name + sex_id + draw + location_id + location_name ~ etio,
      value.var = ("prev"), fun.aggregate = sum) # added the fun.aggregate
    
    # Check to determine if proportions are over 1 for diabetes type 1
    check_matching_conditions(dataset = dm_draws_long, 
                              val_col = "dm1", 
                              value = 1, 
                              evaluation = "lte", 
                              descriptor = paste0("dm1_reshape_loc_", loc_id),
                              release_id = release_id,
                              output_path = diagnostics_dir)

    # Check to determine if proportions are over 1 for diabetes type 2
    check_matching_conditions(dataset = dm_draws_long, 
                              val_col = "dm2", 
                              value = 1, 
                              evaluation = "lte", 
                              descriptor = paste0("dm2_reshape_", loc_id),
                              release_id = release_id,
                              output_path = diagnostics_dir)
    
    # Sum prevalence of diabetes 1 and diabetes 2 by location, year, age, sex, 
    # for each draw column.
    dm_draws_long[, 
                  total := sum(dm1, dm2), 
                  by = c("location_id",
                         "location_name",
                         "year_id", 
                         "age_group_name",
                         "age_group_id", 
                         "sex_id",
                         "draw")]
    
    # Check to determine if proportions are over 1
    check_matching_conditions(dataset = dm_draws_long, 
                              val_col = "total", 
                              value = 1, 
                              evaluation = "lte", 
                              descriptor = paste0("total_reshape_loc_", loc_id),
                              release_id = release_id,
                              output_path = diagnostics_dir)
    
    # Calculate the ratios of diabetes type 1 and diabetes type 2 to total 
    # diabetes.
    etios <- c("dm1", "dm2")
    dm_draws_long$dm1[dm_draws_long$dm1 == 0] <- 0 
    dm_draws_long[, 
                  (etios) := lapply(1:length(etios), 
                                    function(x) get(etios[x]) / total), 
                  by = c("location_id", 
                         "location_name",
                         "year_id", 
                         "age_group_id",
                         "age_group_name",
                         "sex_id", 
                         "draw")
                  ]
    
    # Check to determine if proportions are over 1 for diabetes type 1
    check_matching_conditions(dataset = dm_draws_long, 
                              val_col = "dm1", 
                              value = 1, 
                              evaluation = "lte", 
                              descriptor = paste0("dm1_ratio_loc_", loc_id),
                              release_id = release_id,
                              output_path = diagnostics_dir)

    # Check to determine if proportions are over 1 for diabetes type 2
    check_matching_conditions(dataset = dm_draws_long, 
                              val_col = "dm2", 
                              value = 1, 
                              evaluation = "lte", 
                              descriptor = paste0("dm2_ratio", loc_id),
                              release_id = release_id,
                              output_path = diagnostics_dir)

    # Drop draw columns for total diabetes
    dm_draws_long[, total := NULL]
    
    # Melt t1 and t2 long
    dm_draws_long <- melt(data = dm_draws_long,
                          id.vars = c("year_id", 
                                      "age_group_id",
                                      "age_group_name",
                                      "sex_id", 
                                      "draw", 
                                      "location_id",
                                      "location_name"),
                          variable.name = "etio", 
                          value.name = "prev")
    
    # Rename location_id before casting
    dm_draws_long[, location_id := paste0("loc_", location_id)]
    
    # Check to determine if proportions are over 1
    check_matching_conditions(dataset = dm_draws_long, 
                              val_col = "prev", 
                              value = 1, 
                              evaluation = "lte", 
                              descriptor = paste0("reshape_prev_loc_", loc_id),
                              release_id = release_id,
                              output_path = diagnostics_dir)
    
    # Cast location_id wide
    dm_draws_long <- dcast(
      data = dm_draws_long,
      formula = year_id + age_group_id + age_group_name + sex_id + draw + etio ~ location_id,
      value.var = "prev"
    )
    
    # Check to determine if proportions are over 1 for loc_102 values Please 
    # note that the diagnostic output csv files will include the location name 
    # due to the formatting.
    check_matching_conditions(dataset = dm_draws_long, 
                              val_col = "loc_102", 
                              value = 1, 
                              evaluation = "lte", 
                              descriptor = paste0("reshape_by_loc_id_loc_", 
                                                  loc_id),
                              release_id = release_id,
                              output_path = diagnostics_dir)
    
    # Check to determine if proportions are over 1 loc_33 values. Please note 
    # that the diagnostic output csv files will include the location name due 
    # to the formatting.
    check_matching_conditions(dataset = dm_draws_long, 
                              val_col = "loc_33", 
                              value = 1, 
                              evaluation = "lte", 
                              descriptor = paste0("reshape_by_loc_id_loc_", 
                                                  loc_id),
                              release_id = release_id,
                              output_path = diagnostics_dir)
    
    # Calculate ratio of loc x to US for diabetes type 1 and diabetes type 2 
    # because the geissinger data is for the U.S> meaning it is the gold 
    # standard.
    loc_var <- paste0("loc_", loc_id)
    loc_us <- "loc_102"
    
    # Set the ratio to 1 if values for the specified location and U.S. are equal
    # to each other.
    dm_draws_long[get(loc_var) == get(loc_us), ratio := 1] 
    dm_draws_long[is.na(ratio) & get(loc_var) == 0, (paste(loc_var)) := 1e-6]
    dm_draws_long[is.na(ratio) & get(loc_us) == 0, (paste(loc_us)) := 1e-6]
    dm_draws_long[is.na(ratio), ratio := get(loc_var) / get(loc_us)]
    
    # Set max ratio at 99th 
    dm_draws_long[!is.na(ratio) & ratio > quantile(dm_draws_long$ratio, .99),
                  ratio := quantile(dm_draws_long$ratio, .99)]
    

    summary(dm_draws_long$ratio)
    # Drop loc-specific proportions
    dm_draws_long[, c(loc_var, loc_us) := NULL]
    
    # Cast draws wide
    dm_draws <- dcast(
      dm_draws_long, 
      formula = year_id + age_group_id + age_group_name + sex_id + etio ~ draw, 
      value.var = "ratio")
    
    # Change the names of the draws so we can add them to the predictions
    ratios <- paste0("ratio_", 0:999)
    setnames(dm_draws, draw_columns, ratios)
    
    # Merge diabetes correction values on predictions
    message("Merging diabetes draws with predictions. Correcting predictions")
    predictions[, location_id := loc_id]
    predictions <- merge(predictions, 
                         dm_draws, 
                         by = c("age_group_id", 
                                "age_group_name", 
                                "year_id", 
                                "sex_id", 
                                "etio"), 
                         all.x = TRUE)
    
    # Correct diabetes type 1 and 2 proportions
    predictions[etio %in% c("dm1", "dm2"), 
                (draw_columns) := lapply(1:1000, function(x) {
                  get(draw_columns[x]) * get(ratios[x])
                  }), 
                by = c("sex_id", 
                       "age_group_id",
                       "year_id", 
                       "location_id", 
                       "etio")]
    
    # Drop ratio draw columns
    predictions[, c(ratios) := NULL]
    message("Done with diabetes correction process")
  }
}

# ---FORMAT POST-DIABETES CORRECTION DRAWS--------------------------------------
# Make formatting adjustments now that the reshaping required for the 
# diabetes correction has finished.
# ------------------------------------------------------------------------------

# Re-add the location_id and location_name columns now that draws for the U.S. 
# have been dropped.
predictions[, location_id := loc_id]
predictions <- merge(predictions, loc_name, by = "location_id")

# ---ADJUST QUANTILE VALUE------------------------------------------------------
# Reduce diabetes type 1 and diabetes type 2 to 975 quantile value. 
# ------------------------------------------------------------------------------

data <- data.table::melt(predictions,
                         measure.vars = grep("draw", names(predictions)),
                         variable.name = "draw", value.name = "value")

dm1_99 <- quantile(data$value[data$etio == "dm1"], .975)
data$value[data$etio == "dm1" & data$value > 1] <- dm1_99
dm2_99 <- quantile(data$value[data$etio == "dm2"], .975) 
data$value[data$etio == "dm2" & data$value > 1] <- dm2_99

# Check to determine if proportions are over 1
# Some of the values in the value column are over 60,000. Is this expected?

tryCatch(expr = {
  check_matching_conditions(dataset = data, 
                            val_col = "value", 
                            value = 1, 
                            evaluation = "lte", 
                            descriptor = paste0("adj_quantile_val_loc_", 
                                                loc_id),
                            release_id = release_id,
                            output_path = diagnostics_dir)
  },
  error = function(e) {
    message(paste("Outputting stacked bar charts"))
    plot_predictions(dataset = data, 
                     output_path = diagnostics_dir, 
                     version_name = "adj_agg_quantile_val", 
                     cohort = "geisinger", 
                     release_id = release_id)
    stop(e)
  })


predictions <- dcast(data, formula = year_id + age_group_id + sex_id + etio 
                     + stage + age_group_name + age_group_years_start 
                     + age_group_years_end + age + age2 + const + measure_id 
                     + location_name + location_id ~ draw, 
                     value.var = "value")

# Check the squareness of the proportions prior to writing them to disk.
age_group_ids <- unique(predictions$age_group_id)
sex_ids <- unique(predictions$sex_id)
year_ids <- unique(predictions$year_id)
stages <- unique(predictions$stage)
etios <- unique(predictions$etio)
prediction_ids <- list(age_group_id = age_group_ids, 
                       sex_id = sex_ids,
                       year_id = year_ids,
                       stage = stages,
                       etio = etios)
assertable::assert_ids(data = predictions, id_vars = prediction_ids)

# ---WRITE PROPORTIONS TO DISK--------------------------------------------------
# Predictions dt contains results by stage and etiology. 
# Write files for each stage and etiology, for the specified location being 
# parallelized by.
# ------------------------------------------------------------------------------

# rename stage4_5 to just stage4. When we upload results for stage 5, we will 
# use the stage 4 folder 
predictions[stage == "stage4_5", stage := "stage4"]

keep <- c(names(predictions)[grepl("draw_", names(predictions))], 
          names(predictions)[grepl("_id", names(predictions))])

message(paste("Writing files to", results_dir))
for (stage_name in unique(predictions[, stage])) {
  for (etio_name in unique(predictions[, etio])) {
    dt <- copy(predictions[stage == stage_name & etio == etio_name, ])
    dt <- dt[, keep, with = F]

    dir <- paste0(results_dir, stage_name, "/", etio_name, "/")
    if (!dir.exists(dir)) {
      dir.create(dir, recursive = TRUE)
    }
    write.csv(dt, paste0(dir, loc_id, ".csv"), row.names = F)
  }
  message(paste0("Wrote ", dir, loc_id, ".csv"))
}
