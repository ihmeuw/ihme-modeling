

message(sprintf("Working directory is %s", getwd()))
source(file.path("FILEPATH", Sys.info()['user'], 'FILEPATH/aggregate_up_to_parents.R'))
library(data.table)
library(testthat)

test_that("aggregate_to_parents works as expected: simple case", {
  # In this simple example, 98 and 99 are the children of parent 0. 
  # There are no NAs. 
  
  sample_data = data.table(
    'value' = c(70, 66, 41),
    'location_id' = c(98, 99, 0),
    'parent_id' = c(0, 0, 42),
    'date' = rep('2022-01-01', 3)
  )
  
  hierarchy = data.table(
    'level' = c(4, 4, 3),
    'location_id' = c(98, 99, 0),
    'parent_id' = c(0, 0, 42), 
    'most_detailed' = c(1, 1, 0),
    'region_name' = rep('somewhere', 3),
    'location_name' = rep('nowhere', 3)
  )
  
  expected = data.table(
    'value' = c(70, 66, 136),
    'location_id' = c(98, 99, 0),
    'parent_id' = c(0, 0, 42),
    'date' = rep('2022-01-01', 3)
  )
  
  result =  aggregate_up_to_parents(
    dt = sample_data, 
    hierarchy = hierarchy,
    interpolate = FALSE, 
    agg_cols = c('value')
  )
  
  
  result = result[, .(value, location_id, parent_id, date)]
  expect_equal(result, expected)
})



test_that("aggregate_to_parents works as expected: harder case", {
  # In this simple example, there are levels 3, 4, and 5 in the hierarchy.
  # There are NAs in the data.  
  
  # Requirement - data is identified by location_id and date. 
  sample_non_unique_data = data.table(
    'value' = c(7, NA, 22, 70, 80, 66, NA, 41),
    'location_id' = c(12, 12, 13, 98, 98, 99, 99, 0),
    'parent_id' = c(99, 99, 99, 0, 0, 0, 0, 42),
    'date' = rep('2022-01-01', 8)
  )
  
  # Requirement - no NAs in the most detailed nodes.  
  sample_na_data = data.table(
    'value' = c(7, NA, 80, 66, 41),
    'location_id' = c(12, 13, 98, 99, 0),
    'parent_id' = c(99, 99, 0, 0, 42),
    'date' = rep('2022-01-01', 5)
  )
  
  sample_data = data.table(
    'value' = c(7, 0, 80, 66, 41),
    'location_id' = c(12, 13, 98, 99, 0),
    'parent_id' = c(99, 99, 0, 0, 42),
    'date' = rep('2022-01-01', 5)
  )
  
  hierarchy = data.table(
    'level' = c(5, 5, 4, 4, 3),
    'location_id' = c(12, 13, 98, 99, 0),
    'parent_id' = c(99, 99, 0, 0, 42), 
    'most_detailed' = c(1, 1, 1, 0, 0),
    'region_name' = rep('somewhere', 5),
    'location_name' = rep('nowhere', 5)
  )
  
  # This is assuming NAs are treated like zeros. 
  expected = data.table(
    'value' = c(7, 0, 80, 7, 87),
    'location_id' = c(12, 13, 98, 99, 0),
    'parent_id' = c(99, 99, 0, 0, 42),
    'date' = rep('2022-01-01', 5)
  )
  
  expect_error(
      aggregate_up_to_parents(
        dt = sample_non_unique_data, 
        hierarchy = hierarchy,
        interpolate = FALSE, 
        agg_cols = c('value')
    ), regex = "Data must be uniquely identified by location_id and date!"
  )
  
  expect_error(
    aggregate_up_to_parents(
      dt = sample_na_data, 
      hierarchy = hierarchy,
      interpolate = FALSE, 
      agg_cols = c('value')
    ), regex = "There cannot be NA values in leaf nodes! Found NA for agg_col value with parent ID 9."
  )
  
  result =  aggregate_up_to_parents(
    dt = sample_data, 
    hierarchy = hierarchy,
    interpolate = FALSE, 
    agg_cols = c('value')
  )
  
  
  result = result[, .(value, location_id, parent_id, date)]
  expect_equal(result, expected)
})