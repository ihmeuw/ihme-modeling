## Function library ##

## Shared functions ##
source_shared_functions<-function(functions, gbd_rnd=6){
  if(gbd_rnd ==5){
    functions<-paste0("FILEPATH",functions,".R")
  }
  if (gbd_rnd == 6){
    functions<-paste0("FILEPATH",functions,".R")
  }
  for (func in functions){
    source(func)
  }
}

## Packages ## 
  require(data.table)
  require(stringr)
  require(magrittr)
  require(assertthat)

## Utility ## 

construct_qsub<-function(mem_free, threads, runtime, script, pass, jname, submit=FALSE,
                         shell = "FILEPATH", 
                         project = "FILEPATH", q = "all.q", j = TRUE,
                         errors=NULL, output = NULL){
    user<-Sys.info()[['user']]
    if (is.null(errors)) errors<-paste0("FILEPATH")
    if (is.null(output)) output<-paste0("FILEPATH")
    if (class(pass)=="list") pass<-unlist(pass)
    pass<-paste0(pass,collapse=" ")
    sys_sub<- paste0('qsub -cwd -P ', project, " -N ",jname ," -e ", errors, ' -o ',output, ' -l m_mem_free=', mem_free, 'G', " -l fthread=", threads,
                     " -l h_rt=", runtime, " -q ", q)
    if (j==T) {sys_sub<-paste0(sys_sub, " -l archive=", j )}
    print(paste(sys_sub, shell, script, pass))
    if(submit==T) system(paste(sys_sub, shell, script, pass))
}

### job.array.master(): function to submit an array job from the master script

job.array.master <- function(tester, paramlist, username, project, threads, mem_free, 
                             runtime, q, jobname, errors, childscript, shell, args = NULL) {
  
  require(magrittr, data.table)
  print("Starting Job Array Master")

  # create the parameter grid and save as a flat file to pull in in the child script
  folder <- paste0("FILEPATH")
  cat(folder, "\n")
  
  # create the folder
  dir.create(folder)
  filepath <- paste0("FILEPATH")
  cat(filepath, "\n")
  
  print("Expanding Array Grid for Parallelization")
  params <- expand.grid(paramlist)
  write.csv(params, filepath, row.names = F)
  cat("Wrote CSV", "\n")
  
  # submit the number of jobs for testing (2)
  # OR to the length of the parameters
  print("Setting tester")
  n <- ifelse(tester, 2, nrow(params))
  print(paste(n, "jobs"))
  
  print("Creating qsub command")
  # create the qsub command
  sys_sub <- paste0("qsub -P ", project, " -N ", jobname ," -e ", errors, " -t 1:", n, " -tc 500 -l m_mem_free=", mem_free, 
                   "G", " -l fthread=", threads, " -l h_rt=", runtime, " -q ", q, " -l archive=TRUE")

  cat("Sys sub: ", sys_sub, "\n")
  
  # get the additional arguments to pass to the QSUB
  # plus send it the filepath it needs for the parameter grid
  
  args <- list(filepath, paste(args, collapse = " ")) %>% paste(collapse = " ")
  cat("Args: ", args, "\n")
  
  qsub <- paste(sys_sub, shell, childscript, args)
  print(paste("QSUB COMMAND:", qsub))
  
  # QSUB COMMAND SUBMIT
  system(paste(qsub))
}

job.array.child <- function(){
  
  require(magrittr, data.table)
  
  # grab the task_id from the SGE environment
  task_id <- Sys.getenv("SGE_TASK_ID") %>% as.numeric
  cat("This task_id is ", task_id)
  params <- fread(commandArgs()[6])[task_id,]
  args <- commandArgs()[3:length(commandArgs())]
  
  return(list(params, args))
}
  
  mround_floor <- function(x,base){ 
    if (0<=x&x<0.01917808){return(0)}
    if (0.01917808<=x&x<0.07671233){return(0.01917808)}
    if (0.07671233<=x&x<1){return(0.07671233)}
    if (1<=x&x<4){return(1)}
    else{
      base*floor(x/base)
    }
  } 
  
  mround_ceiling <- function(x,base){ 
    if (0<x&x<=0.01917808){return(0.01917808)}
    if (0.01917808<x&x<=0.07671233){return(0.07671233)}
    if (0.07671233<x&x<=1){return(1)}
    if (1<x&x<=4){return(4)}
    if (x%%5==0){
      ceiling((base*ceiling(x/base)))+4
    }
    else{
      ceiling((base*ceiling(x/base)))-1
    }
  } 

  simpleCap <- function(s, each=F) {
    if (each==T){
      s <- strsplit(s, " ")[[1]]
    }
    paste(toupper(substring(s, 1,1)), substring(s, 2),
          sep="", collapse=" ")
  }
  
  
## Epi Functions ##
  
  order_epi_cols<-function(dt,cv_cols=NULL,suppress_output=F,add_columns=T,delete_columns=T){
    dt<-as.data.table(dt)
    orig_cols<-names(dt)
    note_cols<-c("note_modeler","note_sr1","note_sr2")
    note_cols<-note_cols[note_cols%in%orig_cols]
    col_ord<-c("seq","bundle_id","bundle_name","nid","underlying_nid","field_citation_value","underlying_field_citation_value",
               "input_type","page_num","table_num","source_type","location_id","location_name","smaller_site_unit", "site_memo",
               "sex","sex_issue","year_start","year_end","year_issue","age_start","age_end","age_issue","age_demographer",
               "measure","mean","lower","upper","standard_error","effective_sample_size","cases","sample_size","design_effect",
               "unit_type","unit_value_as_published","measure_issue","measure_adjustment","uncertainty_type","uncertainty_type_value",
               "representative_name","urbanicity_type",	"recall_type","recall_type_value","sampling_type","response_rate","case_name",
               "case_definition",	"case_diagnostics",	"group","specificity","group_review",note_cols,"extractor","is_outlier")
    if (!is.null(cv_cols)){col_ord<-append(x=col_ord,values=cv_cols)}
    add_cols<-col_ord[which(!col_ord%in%names(dt))]
    if (length(add_cols)>0) {
      if (add_columns==T){
        verb<-ifelse(length(add_cols)==1,"was","were")
        article<-ifelse(length(add_cols)==1,"this col","these cols")
        if (suppress_output==F){
          print(paste(simpleCap(paste(add_cols,collapse=",")), verb, "missing from original dataframe. Added", 
                      article,"and set value to NA"))
        }
        dt[,(add_cols):=NA]
      }else{
        print(paste("Dataset is missing the following standard column(s):",paste(add_cols,collapse=",")))
      }
    }
    deleted_cols<-base::setdiff(orig_cols,col_ord)
    if (delete_columns==T){
      if (length(deleted_cols)>0){
          if (suppress_output==F){
            print(paste("Deleted", deleted_cols, "from dataset")) 
          }
        }
      dt[,(deleted_cols):=NULL]
    }else{
      print(paste("Dataset has the following extra columns(s):",deleted_cols))
    }
    if (add_columns==T & delete_columns==T){
      dt<-setcolorder(x = dt,neworder = col_ord)
    }
    if (add_columns==F & delete_columns==T){
      col_ord<-col_ord[!col_ord%in%add_cols]
      dt<-setcolorder(x = dt,neworder = col_ord)
    }
    if (add_columns==T & delete_columns==F){
      col_ord<-append(col_ord,deleted_cols)
      dt<-setcolorder(x = dt,neworder = col_ord)
    }
    if (add_columns==F & delete_columns==F){
      col_ord<-append(col_ord,deleted_cols)
      col_ord<-col_ord[!col_ord%in%add_cols]
      dt<-setcolorder(x = dt,neworder = col_ord)
    }
    return(dt)
  }

  get_age_map<-function(age_group,add_under_1=F){
    source_shared_functions("get_ids")
    if(!age_group%in%c(1,5)){
      stop("Can only generate age map for 1 or 5 year age groups")
    }
    age_dt<- get_ids(table='age_group')
    if (age_group==1){
      keep<-c(2:4,49:147)
    }else{
      keep<-c(2:20,30:32,235)
    }
    age_dt<- age_dt[age_group_id %in% keep]
    if (age_group==1){
      age_dt[,age_start:=c(0,0.01917808,0.07671233,1,seq(0,99,age_group)[3:length(seq(0,99,age_group))])]
      age_dt[age_start>=age_group,age_end:=age_start]
      age_dt[age_start<1,age_end:=c(0.01917808,0.07671233,1)]
    }else{
      age_dt[,age_start:=c(0,0.01917808,0.07671233,1,seq(0,99,age_group)[2:length(seq(0,99,age_group))])]
      age_dt[age_start>=age_group,age_end:=age_start+4]
      age_dt[age_start<age_group,age_end:=c(0.01917808,0.07671233,1,4)]
    }
    if(add_under_1==T){
      under_1<-data.frame(age_group_id=28,age_group_name="Under 1",age_start=0,age_end=0.999)
      age_dt<-age_dt[!age_group_id%in%c(2:4)]
      age_dt<-rbindlist(list(age_dt,under_1),use.names = T)
    }
    return(age_dt)
  }
  
check_missing_locs<-function(indir, filepattern, team, round = 6, step = 'step4'){
  files_lst <- dir(indir)
  print(paste("Number of files:", length(files_lst)))
  print("First five files")
  print(files_lst[1:5])
  
  source_shared_functions(c("get_demographics","get_location_metadata"))
  
  # which locations should you have? 
  demographics <- get_demographics(team, gbd_round_id = round)
  locs <- demographics$location_id
  
  # which locations do you have 
  filepattern <- gsub(pattern = "\\..*", replacement = "", x = filepattern) #remove file extension 
  index <- grep(x = strsplit(x=strsplit(x = filepattern, split = "\\{")[[1]], split="\\}"), pattern = "location")-1
  saved_locs <- strsplit(x = gsub(pattern="\\..*",replacement="", x=files_lst), split = "_")
  saved_locs <- sapply(saved_locs, `[[`, index)
  
  missing <- setdiff(locs, saved_locs)
  
  loc_dt <- get_location_metadata(22, gbd_round_id = round, decomp_step = step)
  missing_loc_dt <- loc_dt[location_id%in%missing][,.(location_id, location_name)]
  
  if(length(missing)>=1){
    print(paste("Missing the following locations:", paste(missing_loc_dt[,location_name],collapse=",")))
    return(missing_loc_dt)
  }else{
    print("Not missing any locations")
    return(FALSE)
  }
}

# Return a string with the path to a given bundle upload or download folder in the FILEPATH
# structure
  get_epi_path<-function(bundle_id,acause,upload){
    if (upload==T){folder<-"02_upload"}else{folder<-"01_download"}
    path<-paste0("FILEPATH")
    return(path)
  }

# Read in a given request number from a specified bundle/acuase combination from the 01_download folder
# in the FILEPATH structure
  read_request<-function(bundle_id,request_id,acause){
    path<-get_epi_path(bundle_id,acause,upload=F)
    file<-paste0(path,"request_",request_id,".xlsx")
    file<-as.data.table(read.xlsx(file))
    return(file)
  }
  
# Read in a given upload file (via filename) from a specified bundle/acause combiation from the 02_upload
# folder in the /FILEPATH structure
  read_upload<-function(bundle_id,file_name,acause){
    path<-get_epi_path(bundle_id,acause,upload=T)
    file<-paste0(path,file_name,".xlsx")
    file<-as.data.table(read.xlsx(file))
    return(file)
  }
  
# Write a file to the 02_upload folder in the FILEPATH structure for a given bundle_id/acuase
# combination. Name the sheet "extraction"
  write_upload<-function(object,bundle_id,acause,file_name){
    path<-get_epi_path(bundle_id,acause,upload=T)
    file<-paste0(path,file_name,".xlsx")
    write.xlsx(object,file,sheetName="extraction")
  }

# Delete all data from a given bundle via bundle_id/acuase combination
  wipe_db<-function(bundle_id,acause){
    source_shared_functions(c("get_bundle_data","upload_epi_data"))
    path<-get_epi_path(bundle_id,acause,upload=T)
    dt<-get_bundle_data(bundle_id)
    clear<-names(dt)[which(names(dt)!="seq")]
    dt[,(clear):=NA]
    date<-gsub("-","_",Sys.Date())
    write_upload(dt,bundle_id,acause,paste0("wipe_db_",date))
    upload_epi_data(bundle_id,paste0(path,"wipe_db_",date,".xlsx"))
  }

# Calculate SE using Wilson formula
  wilson_se<-function(prop=NULL,x=NULL,n){
    if(is.null(prop) & is.null(x)){stop("need to specify either mean or cases in addition to sample size")}
    if(is.null(prop)){prop<-x/n}
    se<-(1/(1+((1.96^2)/n)))*sqrt(((prop*(1-prop))/n)+((1.96^2)/(4*(n^2))))
    return(se)
  }

# Checks for duplicate data in a bundle 
are_there_duplicates <- function(dataframe, return_dataframe = FALSE, n = 20) {
  data <- dataframe %>% 
    select(nid, field_citation_value, location_name, 
          sex, year_start, year_end, age_start, age_end,
          sample_size, mean, standard_error) %>% 
    duplicated() %>% as.data.frame()
  
  # Change names
  names(data) <- c("duplicate")
  
  # Bind columsn to see which ones are duplicated
  dataframe <- bind_cols(dataframe, data)
  
  # View some of the duplicate rows in the bundle
  print("Printing n random rows")
  dataframe %>% select(nid,location_name, 
                  sex, year_start, year_end, age_start, age_end,
                  sample_size, mean, standard_error, duplicate) %>%
    sample_n(n) %>% print(n)
  
  print(paste("There are", sum(dataframe$duplicate), "duplicate rows"))
  
  if (return_dataframe == TRUE) {
    print("Returning data frame with new column 'duplicate'")
    return(dataframe)
  } else {
    print("If you want a dataframe, add return_dataframe = TRUE")
  }
}

# Adds age and location meta data to bundle
add_age_location_metadata <- function(dataframe, round = 6, age_group_set_id = 12, step = 'step4', which = "both") {
  message("which should be assigned as 'both', 'age', or 'location' ")
  if (which == "both"){

    message("Getting both location and age metadata")
    source("/FILEPATH/get_age_metadata.R")
    source("FILEPATH/get_location_metadata.R")
    ages <- get_age_metadata(age_group_set_id=age_group_set_id, gbd_round_id=round)
    ages <- select(ages, age_group_id, age_group_years_start, age_group_years_end)
    dataframe <- left_join(dataframe, ages, by = "age_group_id")

    locations_set <- get_location_metadata(location_set_id = 35, gbd_round_id = round, decomp_step = step)
    location_info <- locations_set %>% select(location_id, parent_id, level, location_name_short, region_name)
    dataframe <- left_join(dataframe, location_info)
    return(dataframe)
  } else if(which == "age") {
      message("Getting age metadata")
      source("FILEPATH/get_age_metadata.R")
      ages <- get_age_metadata(age_group_set_id=age_group_set_id, gbd_round_id=round)
      ages <- select(ages, age_group_id, age_group_years_start, age_group_years_end)
      dataframe <- left_join(dataframe, ages, by = "age_group_id")
      return(dataframe)
  } else if (which == "location") {
      message("Getting location metadata")
      source("FILEPATH/get_location_metadata.R")
      locations_set <- get_location_metadata(location_set_id = 35, gbd_round_id = round, decomp_step = step)
      location_info <- locations_set %>% select(location_id, parent_id, level, location_name_short, region_name)
      dataframe <- left_join(dataframe, location_info)
      return(dataframe)
  }
}