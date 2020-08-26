##################################################
## Purpose: Get values of certain variables from user
##################################################

user_inputs.decomp_steps <- 1:4
tracker_folder <- 'FILEPATH'

get_me_from_user <- function() {
  
  input_is_correct_format <- FALSE
  
  while (!(input_is_correct_format)) {
    user_input <- readline(prompt='sbp or ldl?: ')
    me <- tolower(user_input)
    
    input_is_correct_format <- (me == 'sbp') | (me == 'ldl')
    if (!(input_is_correct_format)) {
      message('Input must be either sbp or ldl!')
    }
  }
  
  message(sprintf('Using me %s', me))
  invisible(return(me))
}

get_step_from_user <- function() {
  
  accepted_by_user <- FALSE
  
  while (!(accepted_by_user)) {
    user_input <- readline(prompt="Enter decomp step (1,2,3,4,iterative): ")
    
    suppressWarnings(int_input <- as.integer(user_input))
    
    if (int_input %in% user_inputs.decomp_steps) {
      decomp_step <- paste0('step', int_input)
    } else{
      decomp_step <- 'iterative'
    }
    
    message(sprintf('Using decomp step %s', decomp_step))
    user_input <- readline(prompt='Is this what you want? (y/[n]): ')
    if (tolower(user_input) == 'y') {
      accepted_by_user <- TRUE
    }
    
  }
  invisible(return(decomp_step))
}

# Get ID using help from tracker file
get_id_from_tracker <- function(me_data) {
  
  id <- me_data[nrow(me_data), get(data_type)]
  
  message(sprintf('Most recent %s for %s is %d', id_string, me, id))
  use_recent_id_prompt <- sprintf('Would you like to use this %s? ([y]/n): ', id_string)
  use_recent_id <- readline(prompt=use_recent_id_prompt)
  
  if (tolower(use_recent_id) == 'n') {
    message(sprintf('Here are the last few saved %ss for %s in the tracker:', id_string, me))
    rows_to_display <- min(10, nrow(me_data))
    print(tail(me_data, rows_to_display))
    
    id_prompt <- sprintf('Enter the %s you would like to use: ', id_string)
    id <- readline(prompt=id_prompt)
    id <- as.integer(id)
  }
  return(invisible(id))
}

# Get bundle version ID, crosswalk version ID, or ST-GPR run ID from tracker files or user 

get_id_from_user <- function(me, data_type) {
  
  # String formatted for printing ID type
  id_string <- gsub('_', ' ', gsub('_id', ' ID', data_type))
  
  tracker_file <- gsub('_id', '_tracker.csv', data_type)
  tracker_path <- paste0(tracker_folder, tracker_file)
  tracker <- fread(tracker_path)
  me_data <- subset(tracker, me_name==paste0('metab_', me))
  
  if (nrow(me_data)==0) {
    message('No data in tracker!')
    empty_tracker_prompt <- sprintf('Enter the %s you would like to use: ', 
                                    id_string)
    id <- readline(prompt=empty_tracker_prompt)
    id <- as.integer(id)
  } else {
    id <- get_id_from_tracker(tracker)
  }
  message(sprintf('Using %s %s', id_string, id))
  return(invisible(id))
}

#Specify fetch type for function
get_fetch_from_user <- function() {
  
  input_is_correct_format <- FALSE
  
  while (!(input_is_correct_format)) {
    user_input <- readline(prompt='all or new?: ')
    fetch_type <- tolower(user_input)
    
    input_is_correct_format <- (fetch_type == 'all') | (fetch_type == 'new')
    if (!(input_is_correct_format)) {
      message('Input must be either all or new!')
    }
  }
  
  message(sprintf('Using fetch %s', fetch_type))
  invisible(return(fetch_type))
}