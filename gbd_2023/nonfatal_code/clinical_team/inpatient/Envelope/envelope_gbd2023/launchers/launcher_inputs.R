# These are the columns that every input source has to have before formatting and bundle upload.
template_dt <- data.table(
  # metadata
  nid = NA,
  underlying_nid = NA,
  field_citation_value = NA,
  underlying_field_citation_value = NA,
  series_nid = NA,
  series_field_citation_value = NA,
  data_type_name = NA,
  measure = NA,
  recall_type = NA,
  recall_type_value = NA,
  # demographics
  location_id = NA,
  age_start = NA,
  age_end = NA,
  sex_id = NA,
  year_id = NA,
  # clinical data specs
  uses_env = NA,
  is_clinical = NA,
  # data points
  cases = NA,
  sample_size = NA
  # clinical_run_id = NA
)

# These are the variables every formatted table must have
minimum_vars <- names(template_dt)

# These are the variables that must not have missing values. The ones excluded may have NA.
vars_no_missing_values <- minimum_vars[!minimum_vars %in% c("underlying_nid", "underlying_field_citation_value", "series_nid", "series_field_citation_value")]

# Run formatting functions ----
# Inpatient runs
source(paste0(code_dir, "workers/inputs/format/format_inpatient_run.R"))
formatted_inpatient_run <- format_inpatient_run()

# Extracted reports
source(paste0(code_dir, "workers/inputs/format/format_extraction_sheet_reports.R"))
col_names <- c("nid", "location_name", "level", "year_id", "age_group_name", "sex_id", "cases", "admission_type", "measure")  
path <- extracted_data_file_path
asly_vars <- c("location_id", "year_id", "age_group_id", "sex_id") # demographics for merging
formatted_extraction_spreadsheet <- format_extraction_spreadsheet(path = path,
                                                                  col_names = col_names,
                                                                  release_id = release_id)

# Eurostat
source(paste0(code_dir, "workers/inputs/format/format_eurostat.R"))
path <- eurostat_path
formatted_eurostat <- format_eurostat(path)

# OECD
source(paste0(code_dir, "workers/inputs/format/format_oecd.R"))
path <- oecd_path
asly_vars <- c("location_id", "year_id", "age_group_id", "sex_id") # demographics for merging
formatted_oecd <- format_oecd(path)

# Health for All
source(paste0(code_dir, "workers/inputs/format/format_health_for_all.R"))
path <- health_for_all_path
sheet <- "Data (table)"
asly_vars <- c("location_id", "year_id", "age_group_id", "sex_id") # demographics for merging
formatted_health_for_all <- format_health_for_all(path, sheet)

# Poland claims
source(paste0(code_dir, "workers/inputs/format/format_pol_nhf.R"))
path <- pol_nhf_path
formatted_pol_nhf <- format_pol_nhf(path)

# Mongolia claims
source(paste0(code_dir, "workers/inputs/format/format_mng_h_info.R"))
path <- mng_h_info_path
formatted_mng_h_info <- format_mng_h_info(path)

# Singapore claims
source(paste0(code_dir, "workers/inputs/format/format_sgp_claims.R"))
path <- sgp_claims_path
password <- "PASSWORD"
sheet <- "Sheet1"
formatted_sgp_claims <- format_sgp_claims(path, password, sheet)

# GBD 2021 surveys
path <- surveys_path
source(paste0(code_dir, "workers/inputs/format/format_surveys.R"))

# combine datasets
formatted_list <- list()
formatted_list[["inpatient_run"]] <- formatted_inpatient_run
formatted_list[["extraction_spreadsheet"]] <- formatted_extraction_spreadsheet
formatted_list[["eurostat"]] <- formatted_eurostat
formatted_list[["oecd"]] <- formatted_oecd
formatted_list[["health_for_all"]] <- formatted_health_for_all
formatted_list[["pol_nhf"]] <- formatted_pol_nhf
formatted_list[["mng_h_info"]] <- formatted_mng_h_info
formatted_list[["sgp_claims"]] <- formatted_sgp_claims
formatted_list[["surveys"]] <- formatted_surveys

# Prepare sources for bundle upload ----
source(paste0(code_dir, "workers/inputs/upload/prepare_for_upload_bundle.R"))
new_bundle_data <- prepare_sources(formatted_list, 
                                   exclude_zero_cases = exclude_zero_cases_bool, 
                                   include_uses_env = include_uses_env_bool,
                                   include_proportion_measure = include_proportion_measure_bool,
                                   max_inlier_val = max_inlier_val_int)
