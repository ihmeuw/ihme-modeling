library(data.table)
library(haven)
library(stringr)

# define global split variables -------------------------------------------

sex_vec <- 1:3 # male, female, both
age_df <- ihme::get_age_metadata(release_id = 16)

# split demographic functions ---------------------------------------------

main_split_function <- function(file_vec,out_dir){
  x <- furrr::future_map(file_vec, \(i){
    temp_list <- compile_split_data(i,out_dir)
    
    write_new_files(
      temp_list$df_list,
      temp_list$new_file_name
    )
  })
  message("... done splitting.")
}

compile_split_data <- function(win_file_name, out_dir_vec){
  df <- as.data.table(read_dta(win_file_name))
  temp_list <- list()
  name_vec <- c()
  total_rows <- nrow(df)
  row_count <- 0
  LARGE_OBJ_SIZE <- 10000000 # 10 MB
  MEDIUM_OBJ_SIZE <- 1000000 # 1 MB
  
  for(i in sex_vec){
    for(r in 1:nrow(age_df)){
      age_start <- as.numeric(age_df[r,age_group_years_start])
      age_end <- as.numeric(age_df[r,age_group_years_end])
      age_id <- as.numeric(age_df[r,age_group_id])
      
      temp_df <- df[
        sex_id == i & 
          age_year >= age_start & 
          age_year < age_end
      ]
      
      subset_size <- as.numeric(lobstr::obj_size(temp_df))
      out_dir <- if(subset_size < MEDIUM_OBJ_SIZE) {
        out_dir_vec[1]
      } else if(subset_size < LARGE_OBJ_SIZE) {
        out_dir_vec[2]
      } else {
        out_dir_vec[3]
      }
      
      file_nombre <- get_new_file_name(win_file_name,out_dir,i,age_id)
      
      if(nrow(temp_df) >= 5){
        row_count <- row_count + nrow(temp_df)
        temp_list <- append(temp_list,list(temp_df))
        name_vec <- append(name_vec,file_nombre)
      }
      
    }
  }
  return(list(df_list = temp_list, new_file_names = name_vec))
}

get_new_file_name <- function(file_name,out_dir,sex_id,age_id,admin_id = NULL){
  x <- unlist(str_split(file_name,"/"))
  x <- x[length(x)]
  x <- str_remove(x,".dta")
  
  new_file_name <- if(is.null(admin_id)) {
    paste(x,sex_id,age_id,sep = "_") 
  } else {
    admin_id <- stringr::str_remove_all(admin_id, '\\r')
    admin_id <- stringr::str_remove_all(admin_id, '\\n')
    admin_id <- stringr::str_remove_all(admin_id, '\\t')
    paste(x,sex_id,age_id,admin_id,sep = "_") 
  }
  new_file_name <- paste0(new_file_name,".dta")
  new_file_name <- file.path(out_dir,new_file_name)
  
  return(new_file_name)
}

write_new_files <- function(df_list, file_name_vec){
  for(i in 1:length(file_name_vec)){
    df <- df_list[[i]]
    df <- as.data.frame(df)
    file_name <- file_name_vec[i]
    
    write_dta(data = df, path = file_name)
  }
}
