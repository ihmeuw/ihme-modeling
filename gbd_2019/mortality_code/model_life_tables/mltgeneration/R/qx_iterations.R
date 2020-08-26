#' Wrapper to initialize variables for qx iteration process
#'
#' Iterate scalar values to find ideal scalars to apply to age-specific qx or mx values, to better-match target 5q0 and 45q15 values
#' Target values are generally HIV-free or with-HIV 5q0 and 45q15 calculated from entry simulations, with scaled HIV CDRs applied to them
#' Initialize required variables for select_scalars function and the HIV adjustment functions called within.
#' After scalars are selected, apply the best scalars to the final values to generate final age-specific adjusted values.
#'
#' @param current_lt data.table with age-specific qx or mx values, along with aggregate 5q0 and 45q15
#'        For stage 1, should contain standard LT qx; for stage 2, should contain mx values (if non-ZAF, HIV-free; if ZAF, with-HIV)
#' @param target_qx data.table with calculated aggregate 5q0 and 45q15 qx values that iteration should attempt to match to.
#' @param hiv_type character, options: "hiv_free", "with_hiv", "hiv_free_ZAF", "with_hiv_ZAF"
#' @param scalar_set_max numeric, the initial upper value of scalars to be tested
#' @param scalar_set_min numeric, the initial lower value of scalars to be tested
#' @param num_iterations numeric whole number, the maximum number of times the function will test different ranges of scalars 
#' @param num_scalars numeric whole number, the number of scalar values to be tested within each iteration
#' @param by_vars character vector, the variables within stan_qx and hiv_free_qx group by
#' @param age_group character. Options: "5q0" or "45q15"
#' 
#' @return a data table with by_vars, best_scalar, target_5q0, target_45q15. Includes qx if stage 1, mx and ax if stage 2.
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable
#' @import tidyr

qx_iterations <- function(current_lt, target_qx, hiv_type = "", scalar_set_max = 0.06, scalar_set_min = 0.0001, num_iterations = 30, num_scalars = 11, by_vars, age_group) {      
  if(hiv_type == "hiv_free") {
    current_vars <- c("sq5", "sq45")
    value_vars <- c("stan_qx")
    target_vars <- c("target_5q0", "target_45q15")
  } else if(hiv_type == "with_hiv") {
    current_vars <- c("stage_1_5q0", "stage_1_45q15")
    value_vars <- c("mx", "ax", "hiv_rr")
    target_vars <- c("entry_5q0", "entry_45q15")
  } else if(hiv_type == "hiv_free_ZAF") {
    current_vars <- c("stage_1_5q0", "stage_1_45q15")
    value_vars <- c("mx", "ax", "hiv_rr")
    target_vars <- c("calc_5q0", "calc_45q15")
  } else if(hiv_type == "with_hiv_ZAF") {
    current_vars <- c("sq5", "sq45")
    value_vars <- c("stan_qx")
    target_vars <- c("target_5q0", "target_45q15")
  } else {
    stop(paste0("Given hiv_type:", hiv_type, " is not a valid option for this argument. Valid hiv_types are 'hiv_free', 'with_hiv', 'hiv_free_ZAF', 'with_hiv_ZAF'"))
  }

  # assert data types
  check_args <- function(current_lt, target_qx, hiv_type, scalar_set_max, scalar_set_min, num_iterations, num_scalars, by_vars, age_group) {

    if (!is.vector(by_vars, mode="character")) {
      stop(paste0("Expected argument: by_vars to be a character vector, instead received object of class: ", class(by_vars)))
    }

    assert_colnames(current_lt, c(by_vars, current_vars, value_vars, "age"), only_colnames = F)
    assert_colnames(target_qx, c(by_vars, target_vars), only_colnames = F)

    if (!is.numeric(scalar_set_max)) {
      stop(paste0("Expected argument: scalar_set_max to be numeric, instead received object of class: ", class(scalar_set_max)))
    }

    if (!is.numeric(scalar_set_min)) {
      stop(paste0("Expected argument: scalar_set_min to be numeric, instead received object of class: ", class(scalar_set_min)))
    }

    if (scalar_set_max < scalar_set_min) {
      stop(paste0("Expected: scalar_set_max > scalar_set_min. Instead scalar_set_max (", scalar_set_max, ") is less than scalar_set_min (", scalar_set_min, ")"))
    } else if (scalar_set_max == scalar_set_min) {
      stop(paste0("Expected: scalar_set_max > scalar_set_min. Instead scalar_set_max (", scalar_set_max, ") is equal to scalar_set_min (", scalar_set_min, ")"))
    }

    if (scalar_set_max < 0) {
      stop("Argument: scalar_set_max must be non-negative.")
    }
    
    if (scalar_set_min < 0) {
      stop("Argument: scalar_set_min must be non-negative.")
    }

    if (!all.equal(num_iterations, as.integer(num_iterations))) {
      stop(paste0("Expected argument: num_iterations to be integer or a whole number, instead received object of class: ", class(num_iterations), " with value: ", num_iterations))
    }
    
    if (num_iterations < 0) {
      stop(paste0("Argument: num_iterations must be non-negative."))
    }
    
    if (!all.equal(num_scalars, as.integer(num_scalars))) {
      stop(paste0("Expected argument: num_scalars to be integer or a whole number, instead received object of class: ", class(num_scalars), " with value: ", num_scalars))
    }
    
    if (num_scalars < 0) {
      stop(paste0("Argument: num_scalars must be non-negative."))
    }

    if(!age_group %in% c("5q0", "45q15")){
      stop(paste0("Given age_group:", age_group, " is not a valid option for this argument. Try '5q0' or '45q15'"))
    }
  }
  
  check_args(current_lt, target_qx, hiv_type, scalar_set_max, scalar_set_min, num_iterations, num_scalars, by_vars, age_group)
  
  # Normalize datasets
  current_lt <- copy(current_lt)
  target_qx <- copy(target_qx)

  setnames(current_lt, current_vars, c("current_5q0", "current_45q15"))
  setnames(target_qx, target_vars, c("target_5q0", "target_45q15"))

  current_columns <- c(by_vars, "age", "current_5q0", "current_45q15", value_vars)
  target_columns <- c(by_vars, c("target_45q15", "target_5q0"))

  # Subset HIV-free dt to the appropriate age groups
  if(age_group == "5q0") keep_ages <- c(0, 1, 5, 10)
  if(age_group == "45q15") keep_ages <- seq(15, 110, 5)
  qx_vars <- paste0("qx_", keep_ages)

  current_lt <- current_lt[age %in% keep_ages, .SD, .SDcols = current_columns]

  ## Reshape dataset wide by age, and prepare variables for iteration
  if(hiv_type %in% c("hiv_free", "with_hiv_ZAF")) {
    setnames(current_lt, value_vars, "qx")
    current_lt[, new_age := paste0("qx_", age)]
    current_lt[, age := NULL]
    setnames(current_lt, "new_age", "age")

    current_lt <- dcast(current_lt, 
                        ihme_loc_id+sex+year+sim+current_5q0+current_45q15~age, 
                        value.var = "qx")
  } else if(hiv_type %in% c("with_hiv", "hiv_free_ZAF")) {
    current_lt <- melt(current_lt, id = c(by_vars, "age", "current_5q0", "current_45q15"))
    current_lt[, age := as.character(paste0(variable, "_", age))]
    current_lt <- dcast(current_lt, 
                        ihme_loc_id+sex+year+sim+current_5q0+current_45q15~age, 
                        value.var = "value")
  }

  current_lt[, min_scalar := scalar_set_min]
  current_lt[, max_scalar := scalar_set_max]

  # set aside rows that have little difference between aggregated 5q0 or 45q15 and their calculated counterparts
  iter_qx <- merge(current_lt, target_qx[, .SD, .SDcols = target_columns], by = by_vars)

  ## Select and apply the scalars
  apply_stage_1_scalars <- function(dt, qx_vars, target_age) {
      adjust_qx <- function(x) (x/dt[, get(paste0("qx_", target_age))]) * dt$best_scalar
      dt[, (qx_vars) := lapply(.SD, adjust_qx), .SDcols = qx_vars]
  }

  apply_with_hiv_scalars <- function(dt) {
    adjust_mx <- function(age) dt[, get(paste0("mx_", age))] + dt[, get(paste0("hiv_rr_", age))] * dt$best_scalar * dt$abs_mx_diff

    ## Adjust mx for 80 and under only -- assume no HIV in older ages. 
    ## Requires HIV RRs for 80+ if we wanted to go higher
    mx_vars <- paste0("mx_", keep_ages[keep_ages < 80])
    loop_ages <- keep_ages[keep_ages < 80]
    dt[, (mx_vars) := lapply(loop_ages, adjust_mx)]
  }

  apply_hiv_free_ZAF_scalars <- function(dt) {
    adjust_mx <- function(age) dt[, get(paste0("mx_", age))] - dt[, get(paste0("hiv_rr_", age))] * dt$best_scalar * dt$abs_mx_diff

    ## Adjust mx for 80 and under only -- assume no HIV in older ages. 
    ## Requires HIV RRs for 80+ if we wanted to go higher
    mx_vars <- paste0("mx_", keep_ages[keep_ages < 80])
    loop_ages <- keep_ages[keep_ages < 80]
    dt[, (mx_vars) := lapply(loop_ages, adjust_mx)]
  }

  if (age_group == "5q0") {
    if(hiv_type %in% c("hiv_free", "with_hiv_ZAF")) {
      # Set aside rows with little difference in 5q0
      ndiff_rows <- iter_qx[round(current_5q0 / target_5q0, 10) == 1]
      iter_qx <- iter_qx[round(current_5q0 / target_5q0, 10) != 1]
      iter_qx[, c("current_5q0", "current_45q15") := NULL]

      iter_qx <- select_scalars(iter_qx, hiv_type, num_iterations, num_scalars, 
                             target_var = "target_5q0", adjustment_function = stage_1_5q0_adjustment)

      apply_stage_1_scalars(iter_qx, qx_vars, target_age = 1)

    } else if(hiv_type == "with_hiv") {
      # Set aside rows with little difference in mx values implied by 5q0
      iter_qx[, abs_mx_diff := abs(qx_to_mx(current_5q0, 5) - qx_to_mx(target_5q0, 5))]
      ndiff_rows <- iter_qx[abs_mx_diff <= 0.00001]
      iter_qx <- iter_qx[abs_mx_diff > 0.00001]

      iter_qx[, c("current_5q0", "current_45q15") := NULL]
      iter_qx <- select_scalars(iter_qx, hiv_type, num_iterations, num_scalars, 
                             target_var = "target_5q0", adjustment_function = with_hiv_5q0_adjustment)

      apply_with_hiv_scalars(iter_qx)
    } else if(hiv_type == "hiv_free_ZAF") {
      # Set aside rows with little difference in mx values implied by 5q0
      iter_qx[, abs_mx_diff := abs(qx_to_mx(current_5q0, 5) - qx_to_mx(target_5q0, 5))]
      ndiff_rows <- iter_qx[abs_mx_diff <= 0.00001]
      iter_qx <- iter_qx[abs_mx_diff > 0.00001]

      iter_qx[, c("current_5q0", "current_45q15") := NULL]

      ## Create a scalar_cap variable for use in capping scalars in select_scalars (to avoid negative mx)
      cap_vars <- paste0("cap_", keep_ages)
      gen_cap <- function(age) iter_qx[, get(paste0("mx_", age))] / (iter_qx[, get(paste0("hiv_rr_", age))] * iter_qx$abs_mx_diff)
      iter_qx[, (cap_vars) := lapply(keep_ages, gen_cap)]
      iter_qx[, scalar_cap := do.call(pmin, c(.SD, na.rm = TRUE)), .SDcols = cap_vars]
      iter_qx[, (cap_vars) := NULL]

      iter_qx <- select_scalars(iter_qx, hiv_type, num_iterations, num_scalars, 
                             target_var = "target_5q0", adjustment_function = hiv_free_ZAF_5q0_adjustment)

      apply_hiv_free_ZAF_scalars(iter_qx)
    }

  } else if (age_group == "45q15") {
    if(hiv_type %in% c("hiv_free", "with_hiv_ZAF")) {
      # Set aside rows with little difference in 45q15
      ndiff_rows <- iter_qx[round(current_45q15 / target_45q15, 10) == 1]
      iter_qx <- iter_qx[round(current_45q15 / target_45q15, 10) != 1]
      iter_qx[, c("current_5q0", "current_45q15") := NULL]

      iter_qx <- select_scalars(iter_qx, hiv_type, num_iterations, num_scalars, 
                             target_var = "target_45q15", adjustment_function = stage_1_45q15_adjustment)

      apply_stage_1_scalars(iter_qx, qx_vars, target_age = 55)

    } else if(hiv_type == "with_hiv") {
      # Set aside rows with little difference in mx values implied by 45q15
      iter_qx[, abs_mx_diff := abs(qx_to_mx(current_45q15, 45) - qx_to_mx(target_45q15, 45))]
      ndiff_rows <- iter_qx[abs_mx_diff <= 0.00001]
      iter_qx <- iter_qx[abs_mx_diff > 0.00001]

      iter_qx[, c("current_5q0", "current_45q15") := NULL]

      iter_qx <- select_scalars(iter_qx, hiv_type, num_iterations, num_scalars, 
                                 target_var = "target_45q15", adjustment_function = with_hiv_45q15_adjustment)

      apply_with_hiv_scalars(iter_qx)
    } else if(hiv_type == "hiv_free_ZAF") {
      # Set aside rows with little difference in mx values implied by 5q0
      iter_qx[, abs_mx_diff := abs(qx_to_mx(current_45q15, 45) - qx_to_mx(target_45q15, 45))]
      ndiff_rows <- iter_qx[abs_mx_diff <= 0.00001]
      iter_qx <- iter_qx[abs_mx_diff > 0.00001]

      iter_qx[, c("current_5q0", "current_45q15") := NULL]

      ## Create a scalar_cap variable for use in capping scalars in select_scalars (to avoid negative mx)
      cap_vars <- paste0("cap_", keep_ages)
      gen_cap <- function(age) iter_qx[, get(paste0("mx_", age))] / (iter_qx[, get(paste0("hiv_rr_", age))] * iter_qx$abs_mx_diff)
      iter_qx[, (cap_vars) := lapply(keep_ages, gen_cap)]
      iter_qx[, scalar_cap := do.call(pmin, c(.SD, na.rm = TRUE)), .SDcols = cap_vars]
      iter_qx[, (cap_vars) := NULL]

      iter_qx <- select_scalars(iter_qx, hiv_type, num_iterations, num_scalars, 
                             target_var = "target_45q15", adjustment_function = hiv_free_ZAF_45q15_adjustment)

      apply_hiv_free_ZAF_scalars(iter_qx)
    }
  } 
  

  if(hiv_type %in% c("hiv_free", "with_hiv_ZAF")) {
    iter_qx <- iter_qx[, .SD, .SDcols = c(target_columns, qx_vars, "best_scalar")]
    
    # add back skipped rows
    ndiff_rows <- ndiff_rows[, .SD, .SDcols = c(target_columns, qx_vars)]
    iter_qx <- rbindlist(list(iter_qx, ndiff_rows), use.names = T, fill = T)

    # Melt back to long by age
    iter_qx <- melt(iter_qx, id = c(target_columns, "best_scalar"),
                     value.name = "qx")

    iter_qx[, age := as.integer(gsub("qx_", "", variable))]
    iter_qx[, variable := NULL]

  } else if(hiv_type %in% c("with_hiv", "hiv_free_ZAF")) {
    iter_qx <- rbindlist(list(iter_qx, ndiff_rows), use.names = T, fill = T)

    drop_vars <- c("min_scalar", "max_scalar", "abs_mx_diff", 
                   "current_scalar", "adjusted_value", "diff", "best_diff",
                   "current_5q0", "current_45q15")
    if(hiv_type == "hiv_free_ZAF") drop_vars <- c(drop_vars, "scalar_cap")
    iter_qx[, (drop_vars) := NULL]

    iter_qx <- melt(iter_qx, id = c(target_columns, "best_scalar"),
                     value.name = "outcome_value")
    iter_qx <- iter_qx[!grepl("hiv_rr", variable)]
    iter_qx <- tidyr::separate(iter_qx, variable, sep = "_", into=c("varname", "age"))
    iter_qx[, age := as.numeric(age)]
    iter_qx <- dcast(iter_qx, 
                     ihme_loc_id+sex+year+sim+age+best_scalar+target_5q0+target_45q15~varname, 
                     value.var = "outcome_value")

    iter_qx <- iter_qx[, .SD, .SDcols = c(target_columns, "age", "mx", "ax", "best_scalar")]
  }

  return(iter_qx)
}
