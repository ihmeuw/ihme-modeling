###########################
##Purpose: small functions that are useful to reuse
###########################


##################################################################################
################### EXIT SCRIPT WITH MESSAGE #####################################
##################################################################################
# 
#'This function terminates execution of a script cleanly, with the option of  
#'displaying a message to the user
#'
#'@param message (optional) the user will see this message as the program exits
#'

exit_script <- function(text=NULL) {
  if (!(is.null(text))) {
    message(text)
  }
  opt <- options(show.error.messages = FALSE)
  on.exit(options(opt))
  stop()
}

##################################################################################
################### GET MOST RECENT FILE #########################################
##################################################################################
# 
#'This function gets the most recent file in a folder, 
#'optionally using pattern matching
#'
#'@param folder: folder in which to look for files
#'@param pattern: subset to a specific pattern, needs to be in string.  
#'                the list.files() function inherits this argument
#'                  -ex: pattern=".csv"
#'                  -ex: pattern="_WARNINGS_"
#'@param sheet: name of a sheet to read in. For .xlsx files only. Default is NULL
#'@param path Boolean, whether to return the path as string, otherwise read the file
#'            defaults to TRUE 
#'@return Either the path to the file as a string or a data table reading the file

get_recent<-function(folder, pattern=NULL, sheet=NULL, path=F){
  require(data.table)
  
  files<-list.files(folder, full.names=T, pattern=pattern)
  files<-files[!grepl("\\~\\$", files)] ##sy: drops any temp open files; this may break the stuffs
  infoo<-file.info(files)
  most_recent_path<-row.names(infoo[infoo$mtime==max(infoo$mtime),])
  if(path==T){
    message(paste("Most recent file: ", most_recent_path))
    return(most_recent_path)
  }else{
    
    ##sy: get file type
    if(grepl(".csv", most_recent_path)){
      recent<-fread(most_recent_path)
    }
    if(grepl(".rds", most_recent_path)){
      recent<-readRDS(most_recent_path)
    }
    if(grepl(".xlsx", most_recent_path)){
      require(openxlsx)
      if(length(sheet)==0){
        message(" Reading an xlsx file, but no sheet name given, reading first sheet")
        sheet<-1
      }
      recent<-read.xlsx(most_recent_path, sheet=sheet)
      recent<-as.data.table(recent)
    }
    message(paste("Most recent file: ", most_recent_path))
    return(recent)
  }
}