
# source libraries --------------------------------------------------------

library(data.table)

# function to qc vmins input data -----------------------------------------

qc_vmnis_data <- function(input_df, gbd_rel_id){
  df <- copy(input_df)
  
  check_columns(colnames(df))
  check_valid_gbd_ids(df, gbd_rel_id)
  check_age_bounds(df$age_start, df$age_end)
  check_year_bounds(df$year_start, df$year_end)
  df_list <- validate_se_imputation(df)
  
  return(df_list)
}

check_columns <- function(cols){
  req_cols <- c(
    'age_start',
    'age_end',
    'year_start',
    'year_end',
    'location_id',
    'sex_id',
    'note_adjusted',
    'cv_pregnant',
    'case_name',
    'mean',
    'lower',
    'upper',
    'standard_error',
    'sample_size',
    'cases'
  )
  
  checkmate::assert_subset(
    x = req_cols,
    choices = cols,
    empty.ok = FALSE
  )
}

check_valid_gbd_ids <- function(df, gbd_rel_id){
  gbd_cols_to_check <- c("location_id", "sex_id")
  loc_df <- get_location_metadata(location_set_id = 35, release_id = gbd_rel_id)
  gbd_demographic_list <- list(
    location_id = loc_df$location_id,
    sex_id = 1:3
  )
  for(c in gbd_cols_to_check){
    df[[c]] <- as.integer(df[[c]])
    if(!all(df[[c]] %in% gbd_demographic_list[[c]])){
      bad_loc_ids <- unique(df[[c]][!(df[[c]] %in% gbd_demographic_list[[c]])])
      stop("Bad location IDs: ", paste(bad_loc_ids, collapse = ", "))
    }
  }
}

check_age_bounds <- function(age_start, age_end){
  if(any(is.na(age_start)) || any(is.na(age_end)) || any(age_start > age_end)){
    stop("Invalid age values. Please make sure all ages are defined and that age_start <= age_end.")
  }
}

check_year_bounds <- function(year_start, year_end){
  if(any(is.na(year_start)) || any(is.na(year_end)) || any(year_start > year_end)){
    stop("Invalid age values. Please make sure all years are defined and that year_start <= year_end")
  }
}

validate_se_imputation <- function(input_df){
  df <- copy(input_df)
  
  i_vec <- which(!(is.na(df$standard_error)) | ((
      !(is.na(df$cases)) & 
        !(is.na(df$sample_size)) &
        df$sample_size > 0 &
        df$sample_size >= df$cases
    ) |
      (
        !(is.na(df$mean)) & 
          !(is.na(df$sample_size)) &
          df$sample_size > 0 &
          df$mean >= 0
      ) |
      (
        !(is.na(df$upper)) & 
          !(is.na(df$lower)) &
          df$upper >= df$lower
      )
    )
  )
  
  inverse_i_vec <- setdiff(
    seq_len(nrow(df)),
    i_vec
  )
  
  return(list(
    valid_df = copy(df)[i_vec, ],
    to_check_df = copy(df)[inverse_i_vec, ]
  ))
}
