#' Functions to apply to qx iteration process. 
#'
#' Separate functions for stage 1 adjustment, which is same across all countries, and stage 2,
#' which is different for non-ZAF and ZAF (non-ZAF uses with_hiv_*_adjustment functions, while ZAF uses hiv_free_ZAF_*_adjustment).
#' 
#' Stage 1 5q0/45q15 adjustment: take qx_age / qx_ref_age scalar. Multiply by the scalar, then generate px_age. 
#'    Aggregate px values, then take 1 - agg_px = agg_qx, and use to compare to output.
#'
#' Stage 2 with-HIV adjustment: adjusted_mx = mx_age + hiv_rr_age * abs_mx_diff * scalar. Getting to with-HIV mx from HIV-free mx
#' Where abs_mx_diff is the difference in absolute mx between the current and target mx values
#' Then, generate qx from ax and scaled mx, convert to px, aggregate, and generate aggregated qx
#' 
#' Stage 2 HIV-free ZAF adjustment: adjusted_mx = mx_age - hiv_rr_age * abs_mx_diff * scalar. Getting to HIV-free mx from with-HIV mx
#' Where abs_mx_diff is the difference in absolute mx between the current and target mx values
#'
#' @param input data.table with current_scalar variable. Wide by age, either containing qx_* or mx_* variables. Stage 2 adjustments also require hiv_rr_* and abs_mx_diff.
#'
#' @return data.table with adjusted_value representing either the scaled/adjusted 5q0 or 45q15, for comparison with the target 5q0 or 45q15
#'
#' @examples
#'
#' @name iteration_adjustment_functions
#'
#' @export
#' @import data.table
NULL

#' @rdname iteration_adjustment_functions
stage_1_5q0_adjustment <- function(input) {
  # take input data.table, apply scalar to data table, return data table
  input[, adjusted_value := 1 - (1 - (qx_0 / qx_1) * current_scalar) * (1 - current_scalar)]
}

#' @rdname iteration_adjustment_functions
stage_1_45q15_adjustment <- function(input) {
  scale_qx <- function(x) return(1 - (x/input$qx_55) * input$current_scalar)
  input[, adjusted_value := 1 - (scale_qx(qx_15) * scale_qx(qx_20) * scale_qx(qx_25) * scale_qx(qx_30) * scale_qx(qx_35) * scale_qx(qx_40) * scale_qx(qx_45) * scale_qx(qx_50) * (1 - current_scalar))]
}

#' @rdname iteration_adjustment_functions
with_hiv_5q0_adjustment <- function(input) {
  apply_whiv_rrs <- function(age) return(input[, get(paste0("mx_", age))] + input[, get(paste0("hiv_rr_", age))] * input$abs_mx_diff * input$current_scalar)
  gen_px <- function(age) {
    if(age == 0) age_length <- 1
    if(age == 1) age_length <- 4
    return(1 - mx_ax_to_qx(apply_whiv_rrs(age), input[, get(paste0("ax_", age))], age_length))
  }
  input[, adjusted_value := 1 - (gen_px(0) * gen_px(1))]
}

#' @rdname iteration_adjustment_functions
with_hiv_45q15_adjustment <- function(input) {
  apply_whiv_rrs <- function(age) return(input[, paste0("mx_", age), with = F] + input[, paste0("hiv_rr_", age), with = F] * input$abs_mx_diff * input$current_scalar)
  gen_px <- function(age) {
    if(age != 110) age_length <- 5
    if(age == 110) age_length <- 15
    return(1 - mx_ax_to_qx(apply_whiv_rrs(age), input[, paste0("ax_", age), with = F], age_length))
  }  
  input[, adjusted_value := 1 - (gen_px(15) * gen_px(20) * gen_px(25) * gen_px(30) * gen_px(35) * gen_px(40) * gen_px(45) * gen_px(50) * gen_px(55))]
}

#' @rdname iteration_adjustment_functions
hiv_free_ZAF_5q0_adjustment <- function(input) {
  apply_hiv_rrs <- function(age) return(input[, get(paste0("mx_", age))] - input[, get(paste0("hiv_rr_", age))] * input$abs_mx_diff * input$current_scalar)
  gen_px <- function(age) {
    if(age == 0) age_length <- 1
    if(age == 1) age_length <- 4
    return(1 - mx_ax_to_qx(apply_hiv_rrs(age), input[, get(paste0("ax_", age))], age_length))
  }
  input[, adjusted_value := 1 - (gen_px(0) * gen_px(1))]
}

#' @rdname iteration_adjustment_functions
hiv_free_ZAF_45q15_adjustment <- function(input) {
  apply_hiv_rrs <- function(age) return(input[, paste0("mx_", age), with = F] - input[, paste0("hiv_rr_", age), with = F] * input$abs_mx_diff * input$current_scalar)
  gen_px <- function(age) {
    if(age != 110) age_length <- 5
    if(age == 110) age_length <- 15
    return(1 - mx_ax_to_qx(apply_hiv_rrs(age), input[, paste0("ax_", age), with = F], age_length))
  }  
  input[, adjusted_value := 1 - (gen_px(15) * gen_px(20) * gen_px(25) * gen_px(30) * gen_px(35) * gen_px(40) * gen_px(45) * gen_px(50) * gen_px(55))]
}

