###Paste this into any code to load these files
# os <- .Platform$OS.type
# if (os == "windows") {
#   source("FILEPATH")
# } else {
#   source("FILEPATH")
# }

os <- .Platform$OS.type

'%not in%' <- function(x,y){
  sapply(x,function(z){if(z %in% y){
    return(FALSE)
  }
  else{
    return(TRUE)
  }
  })
}

fix_path <- function(x){
  os <- .Platform$OS.type
  if(os != "windows"){
    x <- gsub(x=x,pattern="ADDRESS",replacement="ADDRESS")
    x <- gsub(x=x,pattern="ADDRESS",replacement="ADDRESS")
    x <- gsub(x=x,pattern="ADDRESS",replacement="ADDRESS")
    x <- gsub(x=x,pattern="ADDRESS",replacement="ADDRESS")
  } else{
    x <- gsub(x=x,replacement="ADDRESS",pattern="ADDRESS")
    x <- gsub(x=x,replacement="ADDRESS",pattern="ADDRESS")
    x <- gsub(x=x,replacement="ADDRESS",pattern="ADDRESS")
    x <- gsub(x=x,replacement="ADDRESS",pattern="ADDRESS")
  }
  return(x)
}

if(os != "windows"){
  source("FILEPATH")
}

clc <- function() cat(rep("\n", 50))

#round_sigfig <- function(x,n){
#  rounds the given number to an arbitrary number of significant figures
#}

#is a some rearrangement of b
some_combo <- function(a,b,split=" "){
  #convert a,b to strings
  a <- as.character(a)
  b <- as.character(b)
  #split string by SPLIT
  a <- unlist(strsplit(a,split=split))
  b <- unlist(strsplit(b,split=split))
  #make tables
  t_a <- table(a)
  t_b <- table(b)
  #check if counts match
  if(length(names(t_a))!=length(names(t_b))){
    return(FALSE)
  }
  return(all(names(t_a)==names(t_b))&all(t_a==t_b))
}

#creates a "sex_id" column
make_sex_id <- function(dat){
  sex_df <- data.frame(sex=c("Male","Female","Both"),sex_id=c(1,2,3))
  if("sex_id" %not in% names(dat)){
    dat <- merge(dat,sex_df)
  }
}

#creates a "sex" column
make_sex_name <- function(dat){
  make_sex_id <- function(dat){
    sex_df <- data.frame(sex=c("Male","Female","Both"),sex_id=c(1,2,3))
    if("sex" %not in% names(dat)){
      dat <- merge(dat,sex_df)
    }
  }
}

#moves error log from ADDRESS to ADDRESS
move_error <- function(filename){
  in_folder <- "ADDRESS"
  out_folder <- fix_path("ADDRESS")
  file.copy(from=paste0(in_folder,filename),to=out_folder)
}

show_errors <- function(){
  in_folder <- "ADDRESS"
  dir(in_folder)
}

#prints error log for a particular job_id
read_error <- function(job_id,do_print=FALSE,in_errors=TRUE){
  in_folder <- fix_path("ADDRESS")

  fn_df <- data.frame(filename=dir(in_folder))
  fn_df$job_id <- sapply(fn_df$filename,function(f){
    f <- as.character(f)
    n <- unlist(strsplit(x=f,split=".e ",fixed=TRUE))[2]
  })
  filename <- fn_df[which(fn_df$job_id==job_id),]$filename
  out <- readLines(paste0(in_folder,filename))
  if(do_print){
    print(out)
  }
  return(out)
}

#return column names for draw and non-draw columns in a data frame
draw_inds <- function(df,draw_label="draw"){
  col_inds <- grep(names(df),pattern=draw_label)
  non_col_inds <- which(1:ncol(df) %not in% col_inds)
  return(list(draw_cols=col_inds,non_draw_cols=non_col_inds))
}

remove_cols <- function(df,cols){
  return(df[,-which(names(df) %in% cols)])
}

#generates location_id list for a location and all locations with it as a direct parent
grab_loc_and_sublocs <- function(location_id){
  loc_id <- location_id
  h <- get_location_hierarchy(location_set_id=22)
  level0 <- subset(out,location_id==loc_id)$level
  level_df <- data.frame(level=0:6,
                         level_name=c("level_0","level_1","level_2","level_3","level_4","level_5","level_6"))
  level_name <- subset(level_df,level==level0)$level_name
  eval(parse(text=paste0("sub_h <- subset(h,",level_name,"==loc_id)")))
  return(sub_h$location_id)
}

empty_folder <- function(folder_path){
  unlink(fix_path(paste0(folder_path,"/*")),recursive=TRUE)
}
