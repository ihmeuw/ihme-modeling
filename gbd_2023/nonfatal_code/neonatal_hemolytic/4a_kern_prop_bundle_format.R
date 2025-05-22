params <- readr::read_rds("4b_kern_impair/params.rds")

data.table::fread(file = purrr::chuck(params, "path_csv")) |>
  dplyr::transmute(
    seq = NA,
    input_type = NA,
    underlying_nid = NA,
    nid = 143264,
    source_type = "Surveillance - other/unknown",
    location_id = location_id,
    sex = nch::name_for("sex", sex_id),
    year_start = year_id,
    year_end = year_id,
    age_start = 0,
    age_end = 7/365,
    age_group_id = age_group_id,
    measure = nch::name_for("measure", measure_id),
    mean = mean,
    lower = lower,
    upper = upper,
    effective_sample_size = NA,
    design_effect = NA,
    unit_type = "Person",
    unit_value_as_published = 1,
    uncertainty_type = "Confidence interval",
    uncertainty_type_value = 95,
    representative_name = "Nationally and subnationally representative",
    urbanicity_type = "Unknown",
    recall_type = "Not Set",
    sampling_type = NA,
    is_outlier = 0,
    # Columns that per documentation shouldn't be required, but validations fail
    # without them. See DisMod shape validations:
    # FILEPATH
    recall_type_value = NA,
    standard_error = NA,
    cases = NA,
    sample_size = NA
  ) |>
  openxlsx::write.xlsx(
    file = purrr::chuck(params, "path_xlsx"),
    sheetName = "extraction"
  )
