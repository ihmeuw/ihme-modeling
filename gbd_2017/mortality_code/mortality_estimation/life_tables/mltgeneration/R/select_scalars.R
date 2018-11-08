#' Run the iteration process to select the best scalars for the stage 1 and 2 iterations.
#'
#' Run scalar selection process within the stage 1 and 2 iteration processes. 
#' Test x equally-spaced scalar values. After finding the closest fit to target value,
#' re-center on the best scalar value and redefine the scalar value window to 4 times the step-size of the current-run (narrowing the window).
#' Then, run another iteration on new scalar values. Repeat for num_iterations times.
#'
#' @param input data.table to be processed. Contains all variables needed in the given adjustment_function, along with min_scalar and max_scalar variables.
#'        If hiv_type == "hiv_free_ZAF", must also include scalar_cap variable. 
#' @param hiv_type character, name of HIV processing type used for scalar iteration. Used to determine whether to apply caps on scalar values
#' @param num_iterations numeric, the number of time select_scalars should create a new set of scalars and find an optimal scalar value within the set
#' @param num_scalars numeric, the number of values to generate and test within each scalar_set
#' @param target_var character, name of variable that is being compared to (target variable)
#' @param adjustment_function numeric whole number, the number of scalar values to be tested within each iteration
#'
#' @return numeric value representing the optimal scalar given the input and other argument definitions
#' @export
#'
#' @examples
#' @import data.table

select_scalars <- function(input, hiv_type, num_iterations, num_scalars, target_var, adjustment_function) {  
  for (i in 1:num_iterations) {
    # Reset scalars for ZAF to avoid age-specific mx from going under 0
    if(hiv_type == "hiv_free_ZAF") input[max_scalar > scalar_cap, max_scalar := scalar_cap * 0.99]

    for(j in 0:num_scalars) {
      input[, current_scalar := min_scalar + (max_scalar - min_scalar) * j / num_scalars]
      adjustment_function(input)

      input[, diff := abs(adjusted_value - get(target_var))]
      
      # Find scalar and difference value with smallest difference between adjusted value and comparison value
      if(j == 0) {
        input[, best_scalar := current_scalar]
        input[, best_diff := diff]
      } else {
        input[diff < best_diff, best_scalar := current_scalar]
        input[diff < best_diff, best_diff := diff]
      }
    }

    # Create a window that is 4 times the width of the scalar width from the just-completed run
    #   e.g. if the step for each scalar was .01, the next window of scalars would be .04 wide.
    input[, scalar_set_adjustment := abs(max_scalar - min_scalar) / num_scalars]
    input[, max_scalar := best_scalar + 2 * scalar_set_adjustment]
    input[, scalar_min_new := best_scalar - 2 * scalar_set_adjustment]

    # Only use the new minimum scalar if it's over or equal to 0
    input[scalar_min_new >= 0, min_scalar := scalar_min_new]
    input[, c("scalar_min_new", "scalar_set_adjustment") := NULL]
  }

  return(input)
}
