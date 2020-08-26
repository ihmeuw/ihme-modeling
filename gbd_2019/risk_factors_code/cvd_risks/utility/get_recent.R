################################
##  arguments:
##    -folder: folder in which to look for files
##    -pattern: subset to a specific pattern, needs to be in string.  The list.files() function inherits this argument
##      -ex: pattern=".csv"
##      -ex: pattern="_WARNINGS_"
##		-sheet: name of a sheet to read in. For .xlsx files only. Default is NULL
#######################################

################### GET MOST RECENT FILE #########################################

get_recent <- function(folder, pattern=NULL, sheet=NULL, path=F){
require(data.table)

  files <- list.files(folder, full.names=T, pattern=pattern)
  files <- files[!grepl("\\~\\$", files)] ##sy: drops any temp open files; this may break the stuffs
  infoo <- file.info(files)
  most_recent_path <- row.names(infoo[infoo$mtime==max(infoo$mtime),])
  if(path==T){
    message(paste("Most recent file: ", most_recent_path))
    return(most_recent_path)
  }else{
    
    ## get file type
    if(grepl(".csv", most_recent_path)){
  	recent <- fread(most_recent_path)
    }
    if(grepl(".rds", most_recent_path)){
  	recent <- readRDS(most_recent_path)
    }
    if(grepl(".xlsx", most_recent_path)){
      require(openxlsx)
      if(length(sheet)==0){
  		message(" Reading an xlsx file, but no sheet name given, reading first sheet")
  	  sheet <- 1
  	}
  	recent <- read.xlsx(most_recent_path, sheet=sheet)
  	recent <- as.data.table(recent)
    }
    message(paste("Most recent file: ", most_recent_path))
    return(recent)
  }
}