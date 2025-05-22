library(googlesheets4)
library(rjson)
library(stringr)
library(data.table)
library(reticulate)

"%ni%" <- Negate("%in%")
launch_script <- file.path(getwd(), "extract/winnower_extract/submit_extract_job.R") #the extraction launch script

format_cb <- function(topic){
  
  winn_url <- paste0('URL',topic)
  j <- suppressWarnings(fromJSON(paste(readLines(winn_url), collapse="")))
  
  col_names <- c()
  for(i in 1:length(j$headers)){
    x <- j$headers[[i]]$column_name
    col_names <- append(col_names,x)
  }
  
  num_cols <- length(col_names)
  dt <- data.table()
  for(r in 1:length(j$rows)){
    temp_row <- matrix(data = rep(NA,num_cols),nrow = 1,ncol = num_cols)
    for(c in 1:num_cols){
      val <- j$rows[[r]][[c]]$v
      if(val != "None") temp_row[1,c] <- val
    }
    temp_row <- as.data.table(temp_row)
    colnames(temp_row) <- col_names
    dt <- rbindlist(list(dt,temp_row),use.names = T,fill = T)
  }
  return(dt)
}

#this function looks for potential files that could have identical file output names, therefore overwriting each other. We can flag them in this script and ensure they all get extracted properly by renaming them in your winnower extract output folder
check_4_winnower_duplicates <- function(df){
  good_val_vec <- c()
  good_ubcov_vec <- c()
  bad_val_vec <- c()
  bad_ubcov_vec <- c()
  for(r in 1:nrow(df)){
    if(!(is.na(as.character(df$cb_basic_id[r]))) && as.character(df$cb_basic_id[r])!="" && as.character(df$cb_basic_id[r])!="#N/A" && as.character(df$cb_basic_id[r])!="NA"){
      dis_val <- paste(as.character(df$survey_name[r]),as.character(df$nid[r]),as.character(df$survey_module[r]),as.character(df$ihme_loc_id[r]),as.character(df$year_start[r]),as.character(df$year_end[r]),sep = "_")
      dis_val <- str_replace_all(dis_val,pattern = "/",replacement = "_")
      dis_val <- paste0(dis_val,".dta")
      if(dis_val %in% good_val_vec){ #if it's in the good val_vec
        bad_index <- match(dis_val,good_val_vec) #find it
        
        bad_val_vec <- append(bad_val_vec,dis_val) #add it to bad val vec
        bad_ubcov_vec <- append(bad_ubcov_vec,good_ubcov_vec[bad_index])
        
        good_val_vec <- good_val_vec[-bad_index] #remove it
        good_ubcov_vec <- good_ubcov_vec[-bad_index]
        
        bad_val_vec <- append(bad_val_vec,dis_val) #add the bad vals
        bad_ubcov_vec <- append(bad_ubcov_vec,as.numeric(as.character(df$cb_basic_id[r])))
      }else if(dis_val %in% bad_val_vec){ #if it's already in the bad vec
        bad_val_vec <- append(bad_val_vec,dis_val) #add the bad vals
        bad_ubcov_vec <- append(bad_ubcov_vec,as.numeric(as.character(df$cb_basic_id[r])))
      }else{
        good_val_vec <- append(good_val_vec,dis_val)
        good_ubcov_vec <- append(good_ubcov_vec,as.numeric(as.character(df$cb_basic_id[r])))
      }
    }
  }
  message("Returning list of potential duplicate files with respective UbCov ID's.")
  to_return <- data.frame(bad_val_vec,bad_ubcov_vec)
  colnames(to_return) <- c("ubcov_out_file_name","cb_basic_id")
  return(to_return)
}

#this function gets a list of UbCov IDs that have been completed OR not completed based on user input
get_ubcov_ids <- function(df,j_path,l_path=NULL,completed_flag=F,bad_id_vec=NULL){
  
  #' 
  #' @param df -> the anemia codebook
  #' @param j_path -> the output directory where surveys will be extracted to if they originated on the J drive
  #' @param l_path -> the output directory where surveys will be extracted to if they originated on the L drive
  #' @param completed_flag -> TRUE returns a list of extracted UbCov IDs; FALSE (default) returns a list of non-extracted UbCov IDs
  #' @param bad_id_vec -> a vector of UbCov IDs that errored out during the extraction process (defaulted to NULL if extraction process hasn't started yet)
  #' 
  
  finished_ubcov_ids <- c()
  still_need_ubcov <- c()
  
  j_files <- list.files(j_path) #get a list of files that have already been extracted in our J drive output directory
  l_files <- NULL
  if(!(is.null(l_path))){ #if Limited Use files have been extracted and a file path has been supplied
    l_files <- list.files(l_path) #get a list of files that have already been extracted in our L drive output directory
  }
  for(r in 1:nrow(df)){ #for each row in our anemia codebook
    if(!(is.na(df$file_path[r])) && (is.null(l_path) && toupper(substr(df$file_path[r],2,6))=="SNFS1") || !(is.null(l_path))){ #if current cell isn't NA, and j_path is at least supplied
      if(!(is.na(as.character(df$cb_basic_id[r]))) && as.character(df$cb_basic_id[r])!="" && as.character(df$cb_basic_id[r])!="#N/A" && as.character(df$cb_basic_id[r])!="NA"){ #ensure it's a valid ubcov id
        #create a file name just as the winnower extract process would
        dis_val <- paste(as.character(df$survey_name[r]),as.character(df$nid[r]),as.character(df$survey_module[r]),as.character(df$ihme_loc_id[r]),as.character(df$year_start[r]),as.character(df$year_end[r]),sep = "_")
        dis_val <- str_replace_all(dis_val,pattern = "/",replacement = "_") #get rid of slashes
        dis_val <- str_replace_all(dis_val,pattern = " ",replacement = "_") #get rid of spaces
        dis_val <- paste0(dis_val,".dta") #append the .dta at the end
        ubcov_id <- as.numeric(as.character(df$cb_basic_id[r])) #get the ubcov id associated with this file
        if(!(is.na(ubcov_id))){
          if(dis_val %in% j_files || (!is.null(l_files) && dis_val %in% l_files)){ #make sure the extract attempt has been made and file is present
            finished_ubcov_ids <- append(finished_ubcov_ids,ubcov_id) #add to completed vec
          }else{
            still_need_ubcov <- append(still_need_ubcov,ubcov_id) #else, add to still need vec
          }
        }
      }
    }
  }
  newDF <- NULL #plce holder for the vector to be returned
  if(completed_flag){ #if the user wanted the completed ids to be returned
    cat("\n")
    newDF <- finished_ubcov_ids #return the completed vec
  }else{
    message("Returning list of NON-extracted UbCov IDs.") #else, return the unextracted ubcov ids
    cat("\n")
    if(!(is.null(bad_id_vec))){ #if the user supplied the ubcov ids that encountered an error and wants them omitted from the "still need to extract vector", then remove the ubcov ids that errored out
      i <- 1
      while(i <= length(still_need_ubcov)){
        if(still_need_ubcov[i] %in% bad_id_vec){
          still_need_ubcov <- still_need_ubcov[-i]
        }else{
          i <- i+1
        }
      }
    }
    newDF <- still_need_ubcov
  }
  return(newDF)
}

#this function allows you to run run_extract in rstudio session
run_local_extract <- function(codebook_name,ubcov_ids = c(),full_debug = F){
  for(id in ubcov_ids){
    if(full_debug){ #set full debug to True if you want to see all winnower outputs to help better identify the error
      system(paste(file.path(getwd(), "extract/winnower_extract/extract_shell.sh -vv",codebook_name,id)))
    }else{
      system(paste(file.path(getwd(), "extract/winnower_extract/extract_shell.sh",codebook_name,id)))
    }
  }
}

#this function finds all ubcov IDs that errored out and didn't finish their extraction process
find_errors <- function(output_path,job_name,cb,j_path,l_path=NULL){
  source_python(file.path(getwd(), "extract/winnower_extract/error_finder.py")) #the python script that reads the winnower output text
  file_vec <- list.files(paste0(output_path)) #get the list of files in our output directory
  error_vec <- c() #initialize and error vector
  for(f in file_vec){ #for eaach file in the output log directory
    file_nombre <- paste0(output_path,f) #create the full file name with its file path included
    if(grepl(job_name,file_nombre,T)){
      temp_vec <- parse_error_file(file_nombre) #calls the python function that reads the error log file and compiles a list of UbCov IDs that errored out
      error_vec <- append(error_vec,temp_vec) #append those to our local error_vec
    }
  }
  error_vec <- unique(unlist(error_vec)) #convert the list of ubcov ids into a vector
  complete_vec <- get_ubcov_ids(cb,j_path = j_path,l_path = l_path,completed_flag = T) #get a list of completed ubcov ids. this is important because the error finder could have picked up an old error that has since been taken care of
  i <- 1
  while (i <= length(error_vec)) {
    if(error_vec[i] %in% complete_vec){ #if an ubcov id that errored out initially has since been fixed
      error_vec <- error_vec[-i] #remove the ubcov id from the error_vec
    }else{
      i <- i+1
    }
  }
  error_vec <- error_vec[error_vec > 0]
  return(error_vec) #return all IDs that errored out as a vector
}

#this function takes all of the ubcov IDs that encountered an error and makes a data.table of the anemia codebook only containing those "bad" UbCov IDs
compile_error_codebook <- function(cb,bad_id_vec){
  cb <- data.table(cb) #convert the codebook supplied to a data.table
  df <- NULL #make a placeholder for the new data.table to be returned
  for(id in bad_id_vec){ #for each of the ubcov ids that encountered an error
    if(is.null(df)){ #if df hasn't been added to yet
      df <- cb[cb_basic_id==id,]
    }else{
      temp_df <- cb[cb_basic_id==id,] #else, add a new row to the new df
      df <- rbind(df,temp_df)
    }
  }
  return(df)
}

#this function prints error logs in terminal window in R session
print_error_log <- function(output_dir,job_name,bad_id_vec,out_log_file = NULL){
  if(!(is.null(out_log_file))){
    if(file.exists(out_log_file)) unlink(out_log_file)
    system(paste("touch",out_log_file))
  }
  for(id in bad_id_vec){ #for each ubcov ID that encountered an error
    search_string <- NULL #placeholder for search string to be run through grep
    if(grepl("errors",output_dir,T)){
      search_string <- paste("Running extraction for",id) #the format for winnower's run_extract output
    }else if(grepl("output",output_dir,T)){
      search_string <- paste("EXTRACTING UbCov ID#: ",id) #the format for anemia custom code output
    }
    file_list <- system(paste0("grep '",search_string,"' ",output_dir,job_name,"*"),intern = T) #obtain a list of files that contain that ubcov ID
    for(f in file_list){ #for all files that contain that ubcov ID
      dis_file <- str_split(f,":") #spilt the string so the file name can be extracted
      if(is.null(out_log_file)){
        system(paste("cat",dis_file[[1]][1])) #print the file contents in terminal/console
        cat("______________\n\n") #provide buffer space between files
      }else{
        system(paste("cat",dis_file[[1]][1],">>",out_log_file)) #print the file contents into a file
        system(paste("echo  '\n'_____________'\n\n' >>",out_log_file))
      }
    }
  }
}

# function to rename duplicated files -------------------------------------

rename_duplicated_file <- function(dat, id_vec, extration_dir) {
  dat <- dat |>
    dplyr::filter(cb_basic_id %in% id_vec)
  
  for(r in seq_len(nrow(dat))) {
    og_file_name <- file.path(
      extration_dir,
      dat$ubcov_out_file_name[r]
    )
    
    new_file_name <- file.path(
      extration_dir,
      paste0(
        tools::file_path_sans_ext(dat$ubcov_out_file_name[r]), '_',
        dat$cb_basic_id[r],
        '.dta'
      )
    )
    
    stmt <- paste('mv', og_file_name, new_file_name)
    print(stmt)
    system(stmt)
  }
}


