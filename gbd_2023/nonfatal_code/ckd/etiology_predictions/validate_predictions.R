#-------------------------------------------------------------------------------
# Project: Non-fatal GBD 
# Purpose: Quality check functions for performing quality assurance on 
# stage-specific etiology proportions.
#-------------------------------------------------------------------------------

user <- Sys.info()["user"]

# ---LOAD LIBRARIES-------------------------------------------------------------

library(assertable)
library(assertthat)
library(data.table)

# ---FUNCTIONS------------------------------------------------------------------

check_matching_conditions <- function(dataset, 
                                      val_col, 
                                      value, 
                                      evaluation, 
                                      descriptor,
                                      release_id,
                                      output_path = "") {
  # Uses assertable's assert_values function to determine if a specific 
  # condition is met for a specified data set, column and value and outputs a 
  # csv of data at the moment that a specified condition is not met and a pdf 
  # of stacked bar charts using that data.
  #
  # Arguments:
  # dataset (data.frame/data.table): The dataset to compare.
  # val_col (character): The name of the column with the values to compare. 
  # Please note that at this point, this argument does not support vectors/lists 
  # of columns meaning that checking multiple columns such as mean and UI 
  # columns or 1000 draw columns in wide format is not yet possible.
  # value (numeric): The value to compare the maximum data point to.
  # evaluation (character): The type of evaluation you want to check the 
  # specified "val_col" and "value" arguments against. Must be one of "not_na", 
  # "not_nan", "not_inf", "lt", "gt", "gte", "equal", "not_equal" and "in". 
  # Details on the definitions of these arguments can be found in the 
  # "assert_values" section of 
  # https://cran.r-project.org/web/packages/assertable/assertable.pdf.
  # descriptor (character): An identifier to help identify the "dataset" and
  # file name (for writing purposes).
  # release_id (numeric): The GBD release_id to pull age metadata for.
  # output_path (character): The path to output a csv of rows with values in 
  # "val_col" that are greater than the value specified for "value". Defaults to 
  # a character of length zero. 
  #
  # Returns:
  # NONE
  #
  # Raises:
  # Errors if the maximum value in the dataset is greater than the specified 
  # value.
  #
  
  # Convert to a data.table
  dt <- copy(dataset)
  dt <- as.data.table(dt)
  
  # Validate input arguments
  assertthat::assert_that(is.data.table(dt))
  assertthat::assert_that(is.character(val_col))
  assertthat::assert_that(is.numeric(value))
  assertthat::assert_that(is.character(evaluation))
  assertthat::assert_that(evaluation %in% c("not_na", 
                                            "not_nan", 
                                            "not_inf", 
                                            "lt", 
                                            "lte", 
                                            "gt", 
                                            "gte",
                                            "equal",
                                            "not_equal",
                                            "in"))
  assertthat::assert_that(is.character(descriptor))
  assertthat::assert_that(is.numeric(release_id))
  assertthat::assert_that(is.character(output_path))
  
  tryCatch(expr = {
    assertable::assert_values(data = dt,
                              colnames = val_col,
                              test = evaluation,
                              test_val = value,
                              display_rows = FALSE)},
    error = function(e) {
      if (nchar(output_path) > 0) {

        if (!file.exists(output_path)) {
          dir.create(paste0(output_path), recursive = TRUE)
        }

        # Output csv of data at the time the condition was found to be invalid.
        csv_file_path <- paste0(output_path, descriptor, ".csv")
        message(paste(
          "Outputting csv with data.table for troubleshooting purposes to",
          csv_file_path))
        write.csv(dt, csv_file_path, row.names = FALSE)
      }
      stop(e)
    }
  )
}
