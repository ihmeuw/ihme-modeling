#' Generate an age-length variable containing the number of years that the age group spans (inclusive)
#'
#' Generate an age-length variable for a data.table that has an "age" variable.
#' age_length is inclusive, so if ages 5 and 10 exist, the age_length for age 5 would be 5
#' \strong{NOTE:} Assumes that the dataset has a consistent age structure 
#'       e.g. one set of rows in the dataset can't have ages 0,1,5,10 while another has ages 0,5,10
#'
#' @param dt data.table
#' @param terminal_age numeric, the terminal age group. Default: 110.
#' @param terminal_length numeric, the span of years to be covered by the last age group. Set to 15 by default. 
#' @param process_terminal logical, whether to expect and process the terminal age group. Set to F if generating age-length for a dataset without terminal age groups
#'
#' @return None. Modifies the given data.table in-place
#' @export
#'
#' @examples
#' 
#' @import data.table

gen_age_length <- function(dt, terminal_age = 110, terminal_length = 15, process_terminal = T) {
  ## First, assert columns -- do a hard assertion because dt$age does a fuzzy match and may accidentally pickup age_group_id instead
  if(!"age" %in% colnames(dt)) stop("Column age not found")
  ages <- sort(unique(dt$age))
  if(max(ages) != terminal_age & process_terminal == T) stop(paste0("Max age in input dataset is ", max(ages), " but terminal_age argument is ", terminal_age))

  for(i in 1:(length(ages) - 1)) {
    current_age <- ages[i]
    next_age <- ages[i+1]
    target_length <- next_age - current_age
    dt[age == current_age, age_length := target_length]
  }

  if(process_terminal == T) {
    dt[age == terminal_age, age_length := terminal_length]
  } else {
    ## Assume that the max age has an age length of 5
    dt[age == max(ages), age_length := 5]
  }
}
