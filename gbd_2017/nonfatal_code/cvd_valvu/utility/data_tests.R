################################
##purpose: Data checks and tests
##notes:  placeholder for tests, plan to expand gradually
# source("FILEPATH/utility/data_tests.R")
#######################################



################### DETACH PACKAGES #########################################
######################################################

detachAllPackages <- function() { ## function to clear libraries from here: https://stackoverflow.com/questions/7505547/detach-all-packages-while-working-in-r
  
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  
  package.list <- setdiff(package.list,basic.packages)
  
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
  message("Non-base libraries detached")
}

################### SHOW MEMORY USAGE #########################################
######################################################

showMemoryUse <- function(sort="size", decreasing=T, limit=10) { ##function to show memory usage from here: https://stackoverflow.com/questions/1358003/tricks-to-manage-the-available-memory-in-an-r-session
  
  objectList <- ls(parent.frame())
  
  oneKB <- 1024
  oneMB <- 1048576
  oneGB <- 1073741824
  
  memoryUse <- sapply(objectList, function(x) as.numeric(object.size(eval(parse(text=x)))))
  
  memListing <- sapply(memoryUse, function(size) {
    if (size >= oneGB) return(paste(round(size/oneGB,2), "GB"))
    else if (size >= oneMB) return(paste(round(size/oneMB,2), "MB"))
    else if (size >= oneKB) return(paste(round(size/oneKB,2), "kB"))
    else return(paste(size, "bytes"))
  })
  
  memListing <- data.frame(objectName=names(memListing),memorySize=memListing,row.names=NULL)
  
  if (sort=="alphabetical") memListing <- memListing[order(memListing$objectName,decreasing=decreasing),] 
  else memListing <- memListing[order(memoryUse,decreasing=decreasing),] #will run if sort not specified or "size"
  
  if(!missing(limit)) memListing <- memListing[1:limit,]
  
  print(memListing, row.names=FALSE)
  return(invisible(memListing))
}


################### CHECK IF VAR EXISTS #########################################
######################################################

# check_exists(var, envir = parent.frame() ){
#   if(!var %in% ls(envir = envir){
#     if(warn==T){
#       message(var, " does not exist in ", envir)
#     }else{
#       stop(paste0(var, " does not exist in ", envir, ", please fix!"))
#     }
#   }
# }

################### CLEAN NUMBERS #########################################
######################################################

##this function preps any numbers that are stored as characters for coercion to numeric. Removes any spaces, commas, or extra decimal pts.
numclean <- function(x) {
  if (class(x) == "character") {
    x <- gsub(" ", "", x)
    x <- gsub("[..]", ".", x)
    x <- gsub("[,]", "", x)
  }
  #x <- as.numeric(x) ## do this coerecion w/ check_class instead
  
  return(x)
}

################### CHECK FOR NAS #########################################
######################################################

## if warn is false, any missing values will break the code
check_missing<-function(col, df, warn=T){
  mean_miss<-df[is.na(get(col))]
  if(nrow(mean_miss)>0){
    if(warn==T){
      message("There are ", nrow(mean_miss), " rows missing ",  col)
    }else{
      stop("There are ", nrow(mean_miss), " rows missing ",  col, ", please fix!")
    }
  }
}

################### CHECK CLASS #########################################
######################################################

check_class<-function(col, df, class, coerce=T){
  df.t<-copy(df)
  orig_class<-class(df[[col]])
  if(orig_class!=class){
    message(col, " of class ", orig_class)
    if(coerce==T){
      suppressWarnings(df[, (col):=as(get(col), class)])

      
      ## get number of rows that were coereced to NA
      coereced<-df.t[is.na(df[,get(col)]) & !is.na(df.t[,get(col)])]
      if(nrow(coereced)>0){message(" ", nrow(coereced), " rows coerced to NA for ", col)}else{message(" Coerced ", col, " to ", class,  " without creating new NAs")}
    }
  }
}


