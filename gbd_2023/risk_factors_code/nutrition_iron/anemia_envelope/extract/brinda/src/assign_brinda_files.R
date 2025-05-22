
# source libraries --------------------------------------------------------

library(haven)
library(data.table)
library(stringr)

# identify what files receive the brinda adjustment -----------------------

assign_brinda_files <- function(input_df){
  
  codebook <- copy(input_df)
  codebook$brinda_adjustment_type <- NA
  
  for(r in seq_len(nrow(codebook))){
    if(file.exists(codebook$extracted_file_name[r])){
      df <- read_dta(
        file = codebook$extracted_file_name[r],
        n_max = 10
      )
      adj_type <- assign_adj_type(
        extract_df = df,
        codebook = codebook,
        row_num = r
      )
      codebook$brinda_adjustment_type[r] <- adj_type
    }
  }
  
  return(codebook)
}

assign_adj_type <- function(extract_df, codebook, row_num){
  if(any(grepl("brinda", colnames(extract_df), ignore.case = TRUE))){
    return('microdata')
  }
  
  if("hemoglobin_raw" %in% colnames(extract_df)){
    gps_file <- find_gps_file(codebook$file_path[row_num])
    if(!(is.na(gps_file))){
      return('gps')
    }
    
    good_admin_cols <- check_admin_cols(df = extract_df)
    ihme_mapped_admin_flag <- check_ihme_admin_mapping(colnames(extract_df)) 
    if(ihme_mapped_admin_flag){
      return('ihme_admin')
    }else if(good_admin_cols){
      return('google')
    }
  }
  
  return('mrbrt')
}

find_gps_file <- function(file_name){
  file_vec <- unlist(str_split(string = file_name, pattern = "/"))
  file_vec <- file_vec[-length(file_vec)]
  survey_dir <- paste(file_vec, collapse = "/")
  
  survey_files <- list.files(
    path = survey_dir,
    full.names = TRUE,
    pattern = "_gps_.*dta",
    ignore.case = TRUE
  )
  
  return(survey_files[1])
}

check_admin_cols <- function(df){
  i_vec <- NULL
  for(i in 1:6){
    temp_vec <- startsWith(colnames(df), "admin_") & 
      endsWith(colnames(df), as.character(i))
    if(is.null(i_vec)) i_vec <- temp_vec
    else i_vec <- i_vec | temp_vec
  }
  vec <- colnames(df)[i_vec]
  if(!(is.null(vec)) && length(vec) > 0){
    for(i in vec){
      admin_col <- df[[i]]
      if(all(is.na(suppressWarnings(as.numeric(admin_col))))){
        return(TRUE)
      }
    }
  }
  return(FALSE)
}

check_ihme_admin_mapping <- function(vec){
  if(any(startsWith(vec, "admin_") & endsWith(vec, "_mapped"))){
    return(TRUE)
  }
  return(FALSE)
}