library(data.table)
library(haven)
library(stringr)
library(reticulate)

# sift through winnower surveys and split up by size ----------------------

sift_winnower_files <- function(extract_dir,config_file_path,topic_name,usr_name, split_file_flag = TRUE, run_collapse_flag = TRUE){
  if(split_file_flag) {
    file_vec <- list.files(path = extract_dir,full.names = T) #get all extracted winnower files
    message("Adding temporary rows to config file...")
    dir_vec <- reformat_config_file(extract_dir,config_file_path,topic_name,usr_name) #add new rows to the config file
    message("Copying smaller files to a new directory...")
    small_files <- organize_extract_files(in_dir = extract_dir,lower_sift_val = "0M",upper_sift_val = "1M",out_dir = dir_vec[1],mv_or_cp = "cp") #copy over small extracted files to small dir
    medium_files <- organize_extract_files(in_dir = extract_dir,lower_sift_val = "1M",upper_sift_val = "10M",out_dir = dir_vec[2],mv_or_cp = "cp")
    file_vec <- remove_similar_files(file_vec,c(small_files, medium_files))
    message("Splitting up larger extract files by collapse demographcis...")
    main_split_function(file_vec,dir_vec) #split the remaining larger files based on age and sex and admin unit (if applicable)
    message("Moving the smaller split collapse files to a new directory...")
    
    to_return <- list(
      small_files = list.files(dir_vec[1], full.names = T),
      med_files = list.files(dir_vec[2], full.names = T),
      large_files = list.files(dir_vec[3], full.names = T)
    )
  } else {
    config_df <- read.csv(config_file_path)
    to_return <- list(
      small_files = list.files(
        path = config_df |>
          dplyr::filter(topic == paste(topic_name, 'small', sep = '_')) |>
          purrr::chuck('input.root'), 
        full.names = TRUE
      ),
      med_files = list.files(
        path = config_df |>
          dplyr::filter(topic == paste(topic_name, 'medium', sep = '_')) |>
          purrr::chuck('input.root'), 
        full.names = TRUE
      ),
      large_files = list.files(
        path = config_df |>
          dplyr::filter(topic == paste(topic_name, 'large', sep = '_')) |>
          purrr::chuck('input.root'), 
        full.names = TRUE
      )
    )
  }
  
  output_dir <- 'FILEPATH'
  
  if(run_collapse_flag) {
    if(length(to_return$small_files) > 0){
      message("Collapsing extracted files less than 1MB...")
      # launch collapse for jobs extract files smaller than 1MB
      
      param_file_path <- file.path(getwd(), 'extract/winnower_collapse/sos/param_maps/small_file_list.csv')
      data.table::fwrite(
        x = data.frame(file_name = to_return$small_files),
        file = param_file_path
      )
      
      array_string <- paste0("1-", length(to_return$small_files), '%250')
      
      small_collapse_job_id <- nch::submit_job(
        script = file.path(getwd(), 'extract/winnower_collapse/sos/custom_collapse.R'),
        script_args = c(param_file_path, paste(topic_name,"small",sep = "_"), config_file_path, output_dir),
        memory = 4,
        ncpus = 1,
        time = 60,
        archive = FALSE,
        job_name = 'collapse_small',
        array = array_string
      )
      
      small_combine_job_id <- nch::submit_job(
        script = file.path(getwd(), 'extract/winnower_collapse/sos/combine_collapse_data.R'),
        script_args = c(param_file_path, paste(topic_name,"small",sep = "_"), config_file_path, output_dir),
        memory = 10,
        ncpus = 2,
        time = 60,
        archive = FALSE,
        job_name = 'combine_small',
        dependency = small_collapse_job_id
      )
    }
    
    if(length(to_return$med_files) > 0){
      message("Collapsing extracted files less than 1MB...")
      # launch collapse for jobs extract files smaller than 1MB
      
      param_file_path <- file.path(getwd(), 'extract/winnower_collapse/sos/param_maps/medium_file_list.csv')
      data.table::fwrite(
        x = data.frame(file_name = to_return$med_files),
        file = param_file_path
      )
      
      array_string <- paste0("1-", length(to_return$med_files), '%125')
      
      medium_collapse_job_id <- nch::submit_job(
        script = file.path(getwd(), 'extract/winnower_collapse/sos/custom_collapse.R'),
        script_args = c(param_file_path, paste(topic_name,"medium",sep = "_"), config_file_path, output_dir),
        memory = 8,
        ncpus = 2,
        time = 60,
        archive = FALSE,
        job_name = 'collapse_medium',
        array = array_string,
        dependency = if(exists('small_combine_job_id')) small_combine_job_id else NULL
      )
      
      medium_combine_job_id <- nch::submit_job(
        script = file.path(getwd(), 'extract/winnower_collapse/sos/combine_collapse_data.R'),
        script_args = c(param_file_path, paste(topic_name,"medium",sep = "_"), config_file_path, output_dir),
        memory = 10,
        ncpus = 2,
        time = 15,
        archive = FALSE,
        job_name = 'combine_medium',
        dependency = medium_collapse_job_id
      )
    }
  
    # launch collapse for jobs extract files greater than 10MB
    if(length(to_return$large_files) > 0){
      message("Collapsing extracted files more than 10MB...")
      param_file_path <- file.path(getwd(), 'extract/winnower_collapse/sos/param_maps/large_file_list.csv')
      data.table::fwrite(
        x = data.frame(file_name = to_return$large_files),
        file = param_file_path
      )
      
      array_string <- paste0("1-", length(to_return$large_files), '%40')
      
      large_collapse_job_id <- nch::submit_job(
        script = file.path(getwd(), 'extract/winnower_collapse/sos/custom_collapse.R'),
        script_args = c(param_file_path, paste(topic_name,"large",sep = "_"), config_file_path, output_dir),
        memory = 25,
        ncpus = 6,
        time = 120,
        archive = FALSE,
        job_name = 'collapse_large',
        array = array_string,
        dependency = if(exists('medium_combine_job_id')) medium_combine_job_id else NULL
      )
      
      large_combine_job_id <- nch::submit_job(
        script = file.path(getwd(), 'extract/winnower_collapse/sos/combine_collapse_data.R'),
        script_args = c(param_file_path, paste(topic_name,"large",sep = "_"), config_file_path, output_dir),
        memory = 10,
        ncpus = 2,
        time = 15,
        archive = FALSE,
        job_name = 'combine_large',
        dependency = large_collapse_job_id
      )
    }
  }

  remove_new_dirs(extract_dir,topic_name,usr_name)
}

# reformat config file ----------------------------------------------------

reformat_config_file <- function(extract_dir,config_fp, og_topic_name, usr_name){
  config_df <- fread(config_fp)
  good_row <- config_df[topic==og_topic_name]
  
  new_topic_ext <- c("small","medium","large")
  new_dir_vec <- c()
  for(i in new_topic_ext){
    new_row <- copy(good_row)
    new_topic_name <- paste(og_topic_name,i,sep = "_")
    if(new_topic_name %in% config_df$topic){
      config_df <- config_df[topic!=new_topic_name]
    }
    new_row[,topic:=new_topic_name]
    
    temp_extract_dir <- extract_dir
    dir_len <- str_length(temp_extract_dir)
    if(substr(temp_extract_dir,dir_len,dir_len)=="/"){
      dir_len <- dir_len - 1
      temp_extract_dir <- substr(temp_extract_dir,1,dir_len)
    }
    new_dir <- paste(temp_extract_dir,usr_name,i,sep = "_")
    new_dir <- manage_directories(new_dir,add_new = T)
    new_dir_vec <- append(new_dir_vec,new_dir)
    new_row[,input.root := new_dir]
    
    config_df <- rbindlist(list(config_df,new_row),use.names = T,fill = T)
  }
  
  write.csv(x = config_df,file = config_fp,row.names = F)
  return(new_dir_vec)
}

# make/remove new directories for files to be stored ----------------------

manage_directories <- function(dir_name,add_new = F){
  if(dir.exists(dir_name)){
    unlink(dir_name,recursive = T)
  }
  if(add_new){
    dir.create(dir_name)
    return(dir_name)
  }
}

remove_new_dirs <- function(extract_dir,og_topic_name,usr_name){
  new_topic_ext <- c("small","medium","large")
  for(i in new_topic_ext){
    new_topic_name <- paste(og_topic_name,i,sep = "_")
    new_dir <- paste(extract_dir,usr_name,i,sep = "_")
    new_dir <- manage_directories(new_dir,add_new = F)
  }
}

# manage file vectors -----------------------------------------------------

remove_similar_files <- function(main_vec, new_vec){
  i <- 1
  while (i <= length(main_vec)) {
    x <- main_vec[i]
    if(x %in% new_vec){
      main_vec <- main_vec[-i]
    }else{
      i <- i+1
    }
  }
  return(main_vec)
}

# split up files by file size ---------------------------------------------

organize_extract_files <- function(in_dir, lower_sift_val, upper_sift_val, out_dir, mv_or_cp){
  lower_sift_val <- convert_byte_size(lower_sift_val)
  upper_sift_val <- convert_byte_size(upper_sift_val)
  extract_file_vec <- list.files(
    path = in_dir,
    full.names = TRUE
  )
  return_vec <- c()
  for(f in extract_file_vec){
    file_size <- fs::file_info(f)$size |> 
      convert_byte_size()
    
    if(file_size >= lower_sift_val && file_size < upper_sift_val){
      cmmd <- paste(mv_or_cp, f, out_dir)
      system(cmmd) 
      
      return_vec <- append(return_vec, f)
    }
  }
  return(return_vec)
}

convert_byte_size <- function(val){
  val <- toupper(val) |>
    stringr::str_remove_all(pattern = ' ')
  unit_vec <- c('B', 'K', 'M', 'G', 'T')
  unit_index <- NA_integer_
  for(i in seq_len(length(unit_vec))){
    if(grepl(unit_vec[i], val)){
      unit_index <- i
      break
    }
  }
  if(is.na(unit_index)){
    stop(
      'Please provide a valid memory unit to `sift_val` : ', 
      paste(unit_vec, collapse = ', ')
    )
  }
  byte_size <- stringr::str_remove(
    string = val, pattern = unit_vec[unit_index]
  )
  unit_index <- unit_index - 1
  byte_size <- as.numeric(byte_size) * ((2 ^ 10) ^ unit_index)
  return(byte_size)
}

# split larger files into smaller ones ------------------------------------

source(file.path(getwd(), "extract/winnower_collapse/src/split_demographic_files.R"))
