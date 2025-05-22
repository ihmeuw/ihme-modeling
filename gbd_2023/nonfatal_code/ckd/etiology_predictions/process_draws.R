#-------------------------------------------------------------------------------
# Project: Nonfatal CKD Estimation - Stage-specific etiology proportions
# Purpose: Home to functions for doing draw level manipulations.
#-------------------------------------------------------------------------------

# ---LOAD LIBRARIES-------------------------------------------------------------

library(assertthat)
library(data.table)
library(magrittr)

# ---FUNCTIONS------------------------------------------------------------------

exponentiate_draws <- function(draws, draw_col_start = 0, draw_col_end = 999) {
  # Exponentiate draw values for each specified draw column.
  #
  # Arguments:
  # draws (data.table): Data.table of draws to be exponentiated.
  # draw_col_start (numeric): A single number representing the starting point in 
  # a dataset of draws. Defaults to 0 (for a 1000 draws).
  # draw_col_end (numeric): A single number representing the end point in a 
  # dataset of draws. Defaults to 999 (for a 1000 draws). 
  #
  # Returns:
  # Data.table of exponentiated draws.
  #
  
  assertthat::assert_that(is.data.table(draws))
  assertthat::assert_that(is.numeric(draw_col_start))
  assertthat::assert_that(is.numeric(draw_col_end))
  
  message("Exponentiate draws")
  draw_columns <- paste0("draw_", draw_col_start:draw_col_end)
  draws[, (draw_columns) := lapply(.SD, exp), .SDcols = draw_columns]
  
  return(draws)
}


create_denominator_draws <- function(draws,
                                     aggregation_cols,
                                     denominator_col,
                                     draw_col = "draw_",
                                     draw_col_start = 0, 
                                     draw_col_end = 999) {
  # Take a data set of draws and create another data set of draws representing 
  # the sum of the draws for specified identifier columns.
  #
  # Arguments:
  # draws (data.table): Data.table of draws for calculating the denominator 
  # draws.
  # aggregation_cols (character vector): Vector of identifier columns to 
  # aggregate the draws by.
  # denominator_col (character): The naming pattern for each draw column 
  # representing the denominator.
  # draw_col (character): The naming pattern for each draw column (e.g. 
  # draw_{n}). Defaults to "draw_".
  # draw_col_start (numeric): A single number representing the starting point in 
  # a dataset of draws. Defaults to 0 (for a 1000 draws).
  # draw_col_end (numeric): A single number representing the end point in a 
  # dataset of draws. Defaults to 999 (for a 1000 draws). 
  #
  # Returns:
  # Data.table containing two sets of draws. The first is the initial set of 
  # draws and the second is a set of draws representing the denominator.
  #
  
  assertthat::assert_that(is.data.table(draws))
  assertthat::assert_that(is.character(aggregation_cols))
  assertthat::assert_that(is.character(denominator_col))
  assertthat::assert_that(is.character(draw_col))
  assertthat::assert_that(is.numeric(draw_col_start))
  assertthat::assert_that(is.numeric(draw_col_end))
  
  message("Generate denominator for predictions")
  draw_columns <- paste0(draw_col, draw_col_start:draw_col_end)
  denominator_columns <- paste0(denominator_col, draw_col_start:draw_col_end)
  draws[, (denominator_columns) := lapply(
    1:1000, 
    function(x) 1 + sum(get(draw_columns[x]), na.rm = T)), 
    by = aggregation_cols]
  
  return(draws)
}


convert_draws_to_prob <- function(draws,
                                  prob_col,
                                  denominator_col,
                                  draw_col = "draw_",
                                  draw_col_start = 0, 
                                  draw_col_end = 999) {
  # Calculate probability draws using two specified sets of draws.
  #
  # Arguments:
  # draws (data.table): Data.table of two sets of draws representing the 
  # numerator and the denominator for the probability calculation.
  # prob_col (character): The naming pattern for each draw column representing 
  # the probabilities that are generated.
  # denominator_col (character): The naming pattern for each draw column 
  # representing the denominator.
  # draw_col (character): The naming pattern for each draw column (e.g. 
  # draw_{n}). Defaults to "draw_".
  # draw_col_start (numeric): A single number representing the starting point in 
  # a dataset of draws. Defaults to 0 (for a 1000 draws).
  # draw_col_end (numeric): A single number representing the end point in a 
  # dataset of draws. Defaults to 999 (for a 1000 draws). 
  #
  # Returns:
  # Data.table of probability draws.
  #
  
  assertthat::assert_that(is.data.table(draws))
  assertthat::assert_that(is.character(prob_col))
  assertthat::assert_that(is.character(denominator_col))
  assertthat::assert_that(is.character(draw_col))
  assertthat::assert_that(is.numeric(draw_col_start))
  assertthat::assert_that(is.numeric(draw_col_end))
  
  message("Convert predictions to probability space")
  draw_columns <- paste0(draw_col, draw_col_start:draw_col_end)
  denominator_columns <- paste0(denominator_col, draw_col_start:draw_col_end)
  prob_columns <- paste0(prob_col, draw_col_start:draw_col_end)
  draws[, (prob_columns) := lapply(
    1:1000, function(x) get(draw_columns[x]) / get(denominator_columns[x]))]
  
  # Drop original draws and denominators -- rename proportions as draws
  draws[, c(draw_columns, denominator_columns) := NULL]
  setnames(draws, prob_columns, draw_columns)
  
  return(draws)
}
