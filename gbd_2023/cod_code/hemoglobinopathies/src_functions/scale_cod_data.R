#' Scale subcauses (dismod interpolated plus "other hemog" pooled cod VR data) to parent hemog cod model (cause id 613)
#' Note: Pulls in whichever model is marked best
#' Calculation: divide subcause by summed subcauses and then multiply by parent


scale_cod_data <- function(subcause_data, loc, out_dir, value_var = "value") {
  calculate_csmr_fraction(subcause_data) |>
    scale_to_parent(loc, out_dir) |>
    pivot_draws_wider(value_var)
}

# calculate "subcause deaths by l/y/a/s divided by total"
calculate_csmr_fraction <- function(subcause_data) {
  message("Calculating CSMR fractions for subcause data...")
  subcause_data <- subcause_data |> 
    nch::pivot_draws_longer() |>
    # sum subcause deaths
    dplyr::mutate(summed_subs = sum(value), 
                  .by = c("age_group_id", "sex_id", "year_id", "location_id", "draw_id")) |>
    # divide subcause deaths by summed subcause deaths
    dplyr::mutate(csmr_fraction = value/summed_subs)
    # check whether csmr_fraction sums to 1
  check_sum_to_1 <- subcause_data |>
    dplyr::summarize(sum_csmr_fraction = sum(csmr_fraction), .by = c(age_group_id, sex_id, year_id, location_id, draw_id)) |>
    dplyr::mutate(check_sum = dplyr::near(sum_csmr_fraction, 1, tol = .01)) |>  # using a tolerance for floating point comparison
    dplyr::filter(!check_sum)
  if(nrow(check_sum_to_1) > 0) {
    stop("Assertion failed: csmr_fraction does not sum to 1 within one or more groups.")
  }
  message("Assertion passed: csmr_fractions sum to 1")
  return(subcause_data)
}

# multiply new csmr_fraction by hemog parent model draws to get scaled draws
scale_to_parent <- function(subcause_data, loc, out_dir) {
  message("Scaling to parent model draws...")
  parent_draws <- qs::qread(file.path(out_dir, 'model_draws', glue::glue("model_hemog_{loc}.qs"))) |>
    data.table::setDT() |>
    nch::pivot_draws_longer() |>
    data.table::setnames("value", "parent_value")
  
  # merge parent and subcause draws, then multiply csmr fractions by corresponding parent draws
  parent_draws <- parent_draws[, !c("cause_id")]
  
  dplyr::left_join(
    subcause_data, 
    parent_draws, 
    by = c("age_group_id", "sex_id", "year_id", "location_id", "draw_id")
  ) |>
    dplyr::mutate(scaled_value = parent_value * csmr_fraction) |>
    dplyr::select(tidyselect::all_of(c(
      "age_group_id", 
      "sex_id", 
      "year_id", 
      "location_id",
      "cause_id",
      "draw_id", 
      "value" = "scaled_value"
    )))
}

pivot_draws_wider <- function(draws_long, value_var = "value") {
  tidyr::pivot_wider(
    draws_long,
    id_cols = c(age_group_id, sex_id, year_id, location_id, cause_id),
    names_from = "draw_id",
    values_from = "value"
  )
}

get_subcause_draws <- function(cause_id, sex_id, input_dir, location_id) {
  # create df of all cause-sex ID combinations
  cause_sex_combos <- expand.grid(cause_id = cause_id, sex_id = sex_id)
  # create vector of file paths
  file_paths <- file.path(input_dir, cause_sex_combos$cause_id, cause_sex_combos$sex_id, glue::glue("synth_hemog_{location_id}.qs"))
  message("Using file paths: ", file_paths)
  # pass file paths to function to create subcauses df
  lapply(file_paths, qs::qread) |> 
    data.table::rbindlist(use.names = TRUE)
}