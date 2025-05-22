# load libraries ----------------------------------------------------------

library(data.table)

# prep data for xwalk upload ----------------------------------------------

main_xwalk_prep_function <- function(input_df){
  df <- copy(input_df)
  
  df <- clean_age_values(input_df = df) |>
    update_sex_value() |>
    clean_stat_values() |>
    calculate_case_counts() |>
    filter_group_review() |>
    assign_seq() |>
    trim_character_columns()
  
  return(df)
}

# assign seq --------------------------------------------------------------

# For unadjusted rows, set crosswalk_parent_seq to null.
# 
# For adjusted rows, set crosswalk_parent_seq to seq.
# 
# For new rows that were split, set seq to null and crosswalk_parent_seq to the seq of the original, non-split row.

assign_seq <- function(input_df){
  df <- copy(input_df)
  
  df <- df[, num_copies := .N, .(seq)]
  
  df <- df[, crosswalk_parent_seq := ifelse(num_copies <= 1, NA_integer_, seq)]
  df <- df[!(is.na(crosswalk_parent_seq)), seq := NA_integer_]
  
  return(df)
}

# function to clean age values --------------------------------------------

clean_age_values <- function(input_df){
  df <- copy(input_df)
  
  age_df <- ihme::get_age_metadata(release_id = 16)
  for(r in seq_len(nrow(age_df))){
    age_low <- age_df$age_group_years_start[r]
    age_high <- age_df$age_group_years_end[r]
    age_id <- age_df$age_group_id[r]
    if(age_high >= 2){
      age_high <- age_high - 1
    }else{
      age_high <- map_age_end(age_id = age_id)
    }
    
    i_vec <- which(df$age_group_id == age_id)
    df$age_start[i_vec] <- age_low
    df$age_end[i_vec] <- age_high
  }
  return(df)
}

map_age_end <- function(age_id){
  age_id <- as.character(age_id)
  new_age_end <- switch (age_id,
                         '2' = 0.01915068493150685,
                         '3' = 0.07668493150684931,
                         '388' = 0.499,
                         '389' = 0.999
  )
  return(new_age_end)
}

# update the sex value based on sex id ------------------------------------

update_sex_value <- function(input_df){
  df <- copy(input_df)
  
  i_vec <- which(df$sex_id %in% 1:2)
  df <- df[i_vec, ]
  
  df$sex <- ifelse(
    df$sex_id == 1,
    'Male',
    'Female'
  )
  
  return(df)
}

# clean stat values -------------------------------------------------------

clean_stat_values <- function(input_df){
  df <- copy(input_df)
  
  if('mean' %in% colnames(df)) df$mean <- NULL
  
  i_vec <- which(!(is.na(df$variance)))
  df <- df[i_vec, ]
  
  if(grepl('hemog', unique(df$var))){
    i_vec <- which(!(is.na(df$val)) & df$val > 0)
    df <- df[i_vec, ]
    
    i_vec <- which(df$val < 25)
    df$val[i_vec] <- 25
    
    i_vec <- which(df$val > 220)
    df$val[i_vec] <- 220
  }else{
    i_vec <- which(!(is.na(df$val)))
    df <- df[i_vec, ]
    
    i_vec <- which(df$val < 0)
    df$val[i_vec] <- 0
    
    i_vec <- which(df$val > 1)
    df$val[i_vec] <- 1
  }
  
  return(df)
}

# update case counts ------------------------------------------------------

calculate_case_counts <- function(input_df) {
  df <- copy(input_df)
  
  if(grepl('hemog', unique(df$var))){ 
    df$cases <- df$sample_size
  } else {
    df$cases <- df$sample_size * df$val
  }
  
  return(df)
}

# filter group review -----------------------------------------------------

filter_group_review <- function(input_df){
  df <- copy(input_df)
  
  i_vec <- which(df$group_review == 1 | is.na(df$group_review))
  
  df <- df[i_vec, ]
  
  return(df)
}

# trim character columns --------------------------------------------------

trim_character_columns <- function(input_df){
  df <- copy(input_df)
  
  class_vec <- sapply(colnames(df), function(x){
    class(df[[x]])
  }, simplify = TRUE)
  
  i_vec <- which(class_vec == 'character')
  char_cols <- colnames(df)[i_vec]
  
  MAX_CHARS <- 1999
  
  for(i in char_cols){
    df[[i]] <- substr(df[[i]], 1, MAX_CHARS)
  }
  
  return(df)
}
