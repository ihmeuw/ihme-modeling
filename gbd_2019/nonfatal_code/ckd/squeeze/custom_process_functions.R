##### Function Library of Useful Things #####

###############################################

# Checks for duplicate values in a bundle


get_list_of_draws <- function(gbd_ids, gbd_id_type, year_id = "None", location = "None",
                              sex_ids = "None", source, age_ids = "None", measure_ids = "None", status, gbd_round_id,
                              decomp_step, n_draws = "None", downsample = NULL) {

  # This function makes it easier to get draws for multiple different causes
  # Useful for squeezes or proportions that we need to compare
  # Takes a list of get_draw values and returns the draws in a list format
  # Use double brackets [[]] to access

  require(tidyverse)
  require(purrr)

  draws <- map(.x = gbd_ids,
               .f = ~get_draws(gbd_id = .x, gbd_id_type = gbd_id_type, year_id = year_id,
                               location_id = location, age_group_id = age_ids, sex_id = sex_ids,
                               source = source, measure_id = measure_ids, status = status,
                               gbd_round_id = gbd_round_id, decomp_step = decomp_step, n_draws = n_draws,
                               downsample = downsample))
  
  return(draws)
}

convert_draws_long <- function(dataframe, draw_name = "draw", reformat = TRUE) {

  # Use draw_name to specify the different names (prop, sev) that appear as draws
  # reformat rearranges dataframe by YASL

  cols_draw <- grep(draw_name, names(dataframe)) # get index of draw columns

  dataframe <- melt(dataframe,
                    measure.vars = cols_draw,
                    variable.name = "draw"  ,
                    value.name = "values")

  if (reformat == TRUE ) {
    dataframe <- dataframe %>% arrange(draw, year_id, age_group_id, sex_id, location_id)
  }
  return(dataframe)
}

convert_draws_wide <- function(dataframe) {

  # Converts draws wide. Only works if draw s

  dataframe <- dataframe %>% spread(draw, values) 
  dataframe
}

scale_draws_to_one_and_return_draws <- function(list_of_draws, return_wide = TRUE) {
  
  # Takes a list of draws by cause
  # Divides each individual draw by the sum of all draws
  # Returns a list of scaled draws

  # Can return draws in wide or long format depending on workflow needs


  message("Converting to long format")
  draws_list <- map(.x = list_of_draws,
                    .f = convert_draws_long)
  
  message("Getting Sums")
  sum_of_values <- 0
  for (i in 1:length(draws_list)) {
    sum_of_values <- sum_of_values + draws_list[[i]]$values
  }
  
  message("Scaling draws to Sum")
  for (i in 1:length(draws_list)) {
    draws_list[[i]]$values <- draws_list[[i]]$values / sum_of_values
  }
  
  # Resets any null value to 0. If all draw sum is 0, will lead to NA, which is 0.
  for (i in 1:length(draws_list)) {
    draws_list[[i]]$values[is.na(draws_list[[i]]$values)] <- 0
  
  }

  ### This will grab only relevant columns to upload. Metric ID might be necessary later...
  for (i in 1:length(draws_list)) {
    draws_list[[i]] <- draws_list[[i]] %>% dplyr::select(year_id, age_group_id, sex_id, location_id, measure_id, draw, values)
  }

  message("Printing head of each draws")
  for (i in 1:length(draws_list)) {
    draws_list[[i]]$values %>% head() %>% print()
  }
  
  # Return draws either long or wide depending on workflow
  if (return_wide == TRUE) {
    message("Converting back to wide format")
    draws_list <- map(.x = draws_list,
                      .f = convert_draws_wide)
    return(draws_list)
  } 
  else {
    message("Returning long format draws")
    return(draws_list)
  }
  
}

squeeze_scaled_draws <- function(child_draws_dataframe_list, parent_draw_dataframe) {
  
  # Takes a list of child draws
  # A parent draw data frame
  # Multiplies scaled child draws by the parent draw
  # This creates squeezed child draws that are returned

  message("If this throws an error, your draws aren't in long format")
  
  draws_list <- child_draws_dataframe_list
  
  parent <- parent_draw_dataframe %>% 
    arrange(draw, year_id, age_group_id, sex_id, location_id, measure_id)
  
  # For each draw, multiply values
  for (i in 1:length(draws_list)) {
    data <- draws_list[[i]] %>% 
      arrange(draw, year_id, age_group_id, sex_id, location_id, measure_id) 
    
    data$values <- data$values * parent$values
    draws_list[[i]] <- data %>% dplyr::select(year_id, age_group_id, sex_id, location_id, measure_id, draw, values)
  }
  
  return(draws_list)
  
}

save_draws_to_file_as_csv <- function(dataframe, directory_to_save, change_measure_id = NULL) {
  
  # Saves files from directory to csv
  # You can change the measure id if needed
    # Mostly for switching from proportion to prevalence
  
  loc_id <- unique(dataframe$location_id)
  file_path <- paste0(directory_to_save, loc_id, ".csv")
  message(file_path)

  if (is.null(change_measure_id) == FALSE) {
    message(paste("Change measure id to", change_measure_id))
    dataframe$measure_id <- change_measure_id
  }

  fwrite(dataframe, file = file_path, row.names = FALSE)
}


