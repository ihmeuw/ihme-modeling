#' Generate scalars to prevent substantial dips in HIV-free mortality due to differences between
#' 45q15 estimation and HIV/Spectrum estimation.
#' For all locations except for South Africa and its subnationals, ensure that the variance
#' in the relative change of qx should be less than 0.001, and qx should never drop more than 25%,
#' for years 1994 and onwards
#' For South Africa and subnationals, start measurement from 1997 onwards due to time-based
#' discrepancies b/t HIV and 45q15, and raise variance threshold to .02 to avoid high-bump curves
#' from making it into the analysis.
#'
#' @param run_id_45q15_est numeric or character, either "best" for best run of 45q15 estimates or numeric
#'     for a specific 45q15 estimate run
#'
#' @return data.table of scalar values, with variables: ihme_loc_id, sex, chosen_scalar
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable

gen_hiv_scalars <- function(run_id_45q15_est = "best") {

  loc_map <- setDT(get_locations(level="countryplus", hiv_metadata = T))
  hiv_locations <- unique(loc_map[group == "1A", ihme_loc_id])

  selected_45q15_run_id <- get_proc_version("45q15", "estimate", run_id_45q15_est)
  prepped_45q15 <- get_mort_outputs("45q15", "estimate", run_id = selected_45q15_run_id, locations = hiv_locations, estimate_stage_id = 3)
  prepped_hiv <- fread(paste0("FILEPATH", selected_45q15_run_id, "/data/hiv_covariate.csv"))

  create_scaled_hiv <- function(hiv_cdr, scalar) {
    scaled_hiv <- copy(hiv_cdr)
    scaled_hiv[, scalar_value := scalar]
    scaled_hiv[, scaled_hiv := adult_hiv_cdr * scalar]
    return(scaled_hiv)
  }

  scalar_values <- c(seq(.5, 1, .1))
  prepped_hiv[, year := floor(year)]
  prepped_hiv <- rbindlist(lapply(scalar_values, create_scaled_hiv, hiv_cdr = prepped_hiv))

  prepped_45q15 <- prepped_45q15[estimate_stage_name == "GPR", .(year = year_id, ihme_loc_id, sex_id, mean)]
  prepped_45q15[sex_id == 1, sex := "male"]
  prepped_45q15[sex_id == 2, sex := "female"]
  prepped_45q15[, sex_id := NULL]
  prepped_45q15[, with_hiv_45m15 := qx_to_mx(mean, t = 45)]

  scaled_hiv <- merge(prepped_hiv, prepped_45q15, by = c("year", "ihme_loc_id", "sex"))
  scaled_hiv <- scaled_hiv[ihme_loc_id %in% hiv_locations]
  scaled_hiv[, hiv_free_mx := with_hiv_45m15 - scaled_hiv]
  scaled_hiv[, hiv_free_qx := mx_to_qx(hiv_free_mx, t = 45)]

  scaled_id_vars <- c("ihme_loc_id", "sex", "scalar_value")
  setkeyv(scaled_hiv, scaled_id_vars)
  setorderv(scaled_hiv, c(scaled_id_vars, "year"))
  scaled_hiv[hiv_free_qx < .000001, hiv_free_qx := .000001]
  scaled_hiv[, rel_change_hiv_free_qx := (hiv_free_qx - shift(hiv_free_qx, type = "lag")) / hiv_free_qx, by = scaled_id_vars]
  scaled_hiv[rel_change_hiv_free_qx > .25, rel_change_violation := 1]
  scaled_hiv[is.na(rel_change_violation), rel_change_violation := 0]

  hiv_violations <- scaled_hiv[year >= 1994,
                               list(var_rel_change_qx = var(rel_change_hiv_free_qx[!grepl("ZAF", ihme_loc_id) | year >= 1997]),
                                    num_violations = length(rel_change_violation[rel_change_violation == 1]),
                                    min_qx = min(hiv_free_qx)),
                               by = scaled_id_vars]

suppressWarnings(
    hiv_scalars <- hiv_violations[, list(chosen_scalar = max(scalar_value[(var_rel_change_qx < .001
                                                                            | (grepl("ZAF", ihme_loc_id) & var_rel_change_qx < .02))
                                                                            & num_violations == 0
                                                                            & ((min_qx >= .15 & sex == "male") | (min_qx >= .09 & sex == "female"))]),
                                         min_var_scalar = scalar_value[var_rel_change_qx == min(var_rel_change_qx)],
                                         max_hiv_pass_scalar = max(scalar_value[(min_qx >= .15 & sex == "male") | (min_qx >= .09 & sex == "female")])),
                                    by = c("ihme_loc_id", "sex")]
  )

  ## In the case of places where the variance is too high, max will output as -Inf. Then, use the scalar that minimizes variance.
  hiv_scalars[is.na(max_hiv_pass_scalar), max_hiv_pass_scalar := .5]
  hiv_scalars[chosen_scalar == -Inf & min_var_scalar >= max_hiv_pass_scalar, chosen_scalar := min_var_scalar]
  hiv_scalars[chosen_scalar == -Inf & min_var_scalar < max_hiv_pass_scalar, chosen_scalar := max_hiv_pass_scalar]

  hiv_scalars[, num_chosen := .N, by = c("ihme_loc_id", "sex")]
  hiv_scalars[num_chosen > 1, min_chosen := min(min_var_scalar), by = c("ihme_loc_id", "sex")]
  hiv_scalars[num_chosen > 1, chosen_scalar := min_chosen]

  hiv_scalars <- hiv_scalars[, .(ihme_loc_id, sex, scalar = chosen_scalar)]
  hiv_scalars <- unique(hiv_scalars)

  return(hiv_scalars)
}
