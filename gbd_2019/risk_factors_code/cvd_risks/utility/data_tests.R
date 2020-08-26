################################
## Data checks and tests
#######################################

##  List of Functions
#     - detachAllPackages()
#     - showMemoryUse()
#     - check_exists()
#     - numclean()
#     - check_missing()
#     - check_class()
#     - check_extremes()
#     - outlier_extremes()

################### DETACH PACKAGES #########################################
## function to clear libraries from here: https://stackoverflow.com/questions/7505547/detach-all-packages-while-working-in-r

detachAllPackages <- function() { 
  basic.packages <- c("package:stats","package:graphics","package:grDevices","package:utils","package:datasets","package:methods","package:base")
  package.list <- search()[ifelse(unlist(gregexpr("package:",search()))==1,TRUE,FALSE)]
  package.list <- setdiff(package.list,basic.packages)
  if (length(package.list)>0)  for (package in package.list) detach(package, character.only=TRUE)
  message("Non-base libraries detached")
}

################### SHOW MEMORY USAGE #########################################
## function to show memory usage from here: https://stackoverflow.com/questions/1358003/tricks-to-manage-the-available-memory-in-an-r-session

showMemoryUse <- function(sort="size", decreasing=T, limit=10) { 
 
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

check_exists<-function(var, df, not_exist=F, warn=F){
  error<-0
  strng<-" "
  if(!var %in% names(df) & not_exist==F){
    error<-1
    strng<-" not "
  }
  if(var %in% names(df) & not_exist==T){
    error<-1
  }
  if(error==1 & warn==T){
    message(var, " does", strng,  "exist in df")
  }
  if(error==1 & warn==F){
    stop(paste0(var, " does", strng, "exist in df, please fix!"))
  }
}


################### CLEAN NUMBERS #########################################
## this function preps any numbers that are stored as characters for coercion to numeric. Removes any spaces, commas, or extra decimal pts.

numclean <- function(x) {
  if (class(x) == "character") {
    x <- gsub(" ", "", x)
    x <- gsub("[..]", ".", x)
    x <- gsub("[,]", "", x)
  }
  #x <- as.numeric(x) ##sy: do this coerecion w/ check_class instead
  
  return(x)
}

################### CHECK FOR NAS #########################################
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
## checks class of variables in data.table
check_class<-function(col, df, class, coerce=T){
  df.t<-copy(df)
  
  ## report original class
  orig_class<-class(df[[col]])
  if(orig_class!=class){
    message(col, " of class ", orig_class)
    ## coerce if necessary
    if(coerce==T){
      suppressWarnings(df[, (col):=as(get(col), class)])

      
      ## get number of rows that were coereced to NA
      coereced<-df.t[is.na(df[,get(col)]) & !is.na(df.t[,get(col)])]
      if(nrow(coereced)>0){message(" ", nrow(coereced), " rows coerced to NA for ", col)}else{message(" Coerced ", col, " to ", class,  " without creating new NAs")}
    }
  }
}


################### CHECK EXTREME VALUES #########################################

check_extremes<-function(col, df, min, max, warn=T){
  
  ## identify extremes
  extremes<-df[get(col)<min | get(col)>max]
  if(nrow(extremes)>0){
      message(nrow(extremes), " values of ", col, " less than ", min, " or greater than ", max, "!")
    if(warn==F){
      stop("  Fix these before moving on.")
    } else {
      
      ## mark rows with new column
      message("  Marking these rows with new column: ", col, "_extreme_val")
      if(paste0(col, "_extreme_val") %in% names(df)){
        stop(paste0(col, "_extreme_val already a column in df!"))
      } else {
        df[get(col)<min | get(col)>max, (paste0(col, "_extreme_val")):=1]
      }
    }
  }
}


################### MARK EXTREME VALUES AS OUTLIERS #########################################
outlier_extremes<-function(df, outlier=T, drop=F){
  
  extreme_cols<-grep("_extreme_val", names(df))
  if(length(extreme_cols)>0){
    message("Marking extremes values with is_outlier==1")
    lapply(extreme_cols, function(x){
      df[x==1, is_outlier:=1]
    })
    
    ## drop extreme markers
    df[, (extreme_cols):=NULL]
  }
}

################### GET LIMITS AND LABELS FOR MAP FUNS #########################################
get_mapvar_lims<-function(x, probs=c(.1, .25, .5, .75, .9),
                          always_include=NULL, round_digits=2){
  
  quants<-quantile(x, probs=probs)
  lims<-sort(c(min(x), always_include, quants))
  labs<-vector(length=length(lims))
  for(lim in 2:length(lims)){
    labs[lim]<-paste0(round(lims[lim], digits=round_digits), " to ", round(lims[lim+1], digits=round_digits))
  }
  labs[1]<-c(paste0("<", round(lims[2], digits=round_digits)))
  labs[length(labs)]<-paste0(">", round(lims[length(lims)], digits=round_digits))
  lims<-c(lims, max(x))
  
  return(list(limits=lims, labels=labs))
}

