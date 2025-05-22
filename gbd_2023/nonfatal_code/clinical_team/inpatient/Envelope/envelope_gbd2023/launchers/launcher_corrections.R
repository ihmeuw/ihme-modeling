# Title: Data corrections laucnher
# Purpose: Performs data transformations and uploading processed data as a crosswalk version for STGPR modeling

# Split ----
source(paste0(code_dir, "workers/split/format_split_inputs.R"))

## Specify inclusion criteria for age/sex splitting data input
min_sample_size <- 10
min_age_span <- 80
max_val <- 2
asfr_bool <- TRUE
ifd_bool <- TRUE

# If country_id column doesn't exist, append_country_ids_names to bundle_dt
if (!"country_id" %in% colnames(bundle_dt)) {
  bundle_dt <- append_country_ids_names(bundle_dt)
}

# Specify outliers for age/sex splitting model inputs
outliering_sheet_path <- "FILEPATH"

# Import covariates separately to use it as an argument in the following functions
covariate_table <- get_country_level_covariate_values(asfr = asfr_bool, 
                                                      ifd = ifd_bool)


# Generate input data informiing the splines models
splines_input <- generate_splines_input(bundle_dt,
                                        min_sample_size,
                                        min_age_span,
                                        covariate_table,
                                        outliering_sheet_path)

# Generate the prediction frame to be filled with model outputs
splines_prediction_frame <- generate_prediction_frame(bundle_dt = bundle_dt,
                                                      age_metadata = age_metadata,
                                                      location_metadata = location_metadata,
                                                      covariate_table = covariate_table)

# Save to disk
inputs_paths <- save_splines_inputs(splines_input, 
                                    splines_prediction_frame, 
                                    run_id, 
                                    bundle_version_id)

# Run interactive splines model
source(paste0(code_dir, "workers/corrections/run_models_interactive.R"))

# Apply splines: results in `xwalk_data`, plots split data and all data post split
source(paste0(code_dir, "workers/corrections/apply_split.R"))

# Outlier ----
# Apply outliering sheet
source(paste0(code_dir, "workers/corrections/apply_outliering_sheet.R"))

# Save crosswalk version
source(paste0(code_dir, "workers/corrections/save_crosswalk_version.R"))
