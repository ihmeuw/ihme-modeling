
# source libraries --------------------------------------------------------

library(data.table)
library(stringr)

# function for getting extracted survey names -----------------------------

get_extracted_file_names <- function(input_df, winnower_dir, lu_winnower_dir = NULL){
  df <- copy(input_df)
  
  df$extracted_file_name <- file.path(
    winnower_dir,
    paste0(
      str_replace_all(
        str_replace_all(
          string = paste(
            df$survey_name,
            df$nid,
            df$survey_module,
            df$ihme_loc_id,
            df$year_start,
            df$year_end,
            sep = "_"
          ),
          pattern = "/",
          replacement = "_"
        ),
        pattern = " ",
        replacement = "_"
      ),
      ".dta"
    )
  )
  
  if(!(is.null(lu_winnower_dir))){
    df <- update_lu_file_paths(
      input_df = df,
      winnower_dir = winnower_dir,
      lu_winnower_dir = lu_winnower_dir
    )
  }
  
  df <- df[, num_files := .N, .(extracted_file_name)]
  
  if(max(df$num_files) > 1){
    df <- update_duplicates(input_df = df)
  }
  
  df$num_files <- NULL
  
  return(df)
}

update_lu_file_paths <- function(input_df, winnower_dir, lu_winnower_dir){
  df <- copy(input_df)
  
  i_vec <- which(grepl(
    pattern = "limited_use",
    x = df$file_path, 
    ignore.case = TRUE
  ))
  
  file_vec <- df$extracted_file_name[i_vec]
  
  set(
    x = df,
    i = i_vec,
    j = 'extracted_file_name',
    value = str_replace_all(
      string = file_vec,
      pattern = winnower_dir,
      replacement = paste0(lu_winnower_dir, "/")
    )
  )
  
  return(df)
}

update_duplicates <- function(input_df){
  df <- copy(input_df)
  
  to_fix <- df[num_files > 1]
  df <- df[num_files == 1]
  
  to_fix$extracted_file_name <- str_replace_all(
    string = to_fix$extracted_file_name,
    pattern = ".dta",
    replacement = paste0("_", to_fix$cb_basic_id, ".dta")
  )
  
  df <- rbindlist(
    list(df, to_fix),
    use.names = T,
    fill = T
  )
  
  return(df)
}
