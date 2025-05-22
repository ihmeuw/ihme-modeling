##########################################
# Title: Age Split data


arguments <- commandArgs(trailingOnly = TRUE)

f          <- commandArgs(trailingOnly = T)[1]
indicators <- commandArgs(trailingOnly = T)[2]
version    <- commandArgs(trailingOnly = T)[3]
admin      <- commandArgs(trailingOnly = T)[4]
part       <- commandArgs(trailingOnly = T)[5]

print(f)
print(indicators)
print(version)
print(admin)
print(part)

user <- Sys.info()[["user"]]
os <- Sys.info()[1]
jpath <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"

pacman::p_load(binom,dplyr,data.table,RMySQL,haven,readr,survey,Hmisc)
source(paste0('FILEPATH'))
source(paste0('FILEPATH'))
source(paste0('FILEPATH'))

for(i in indicators){
  if (i == "binary"){
    vars<- "drinker"
    input_root <- paste0('FILEPATH')
    output_root <- paste0('FILEPATH')
    dir.create(paste0('FILEPATH'),showWarnings = F)
    dir.create(paste0(output_root),showWarnings = F) # create a directory with the same name as the file
  } else if (i == "gday"){
    vars<- "gday"
    input_root <- paste0('FILEPATH')
    output_root <- paste0('FILEPATH')
    dir.create(paste0('FILEPATH'),showWarnings = F)
    dir.create(paste0(output_root),showWarnings = F) # create a directory with the same name as the file
  }
}


#read in data
data <- fread(paste0(f))

######################################################################################################################
message("Creating config")

## Load config
config_master <- list(
  ## Collapse vars
  vars = vars,                 ## Variables to collapse options
  calc.sd = TRUE,                    ## Whether to calculate standard deviation
  ## Collapse over
  cv.manual = c("recall", "type", "alc_group"),                   ## List of other variables to collapse by
  cv.detect = TRUE,                   ## if TRUE, detects columns with cv_* to collapse by
  ## Demographics
  by_age = TRUE,                      ## if TRUE, collapses by vars.age
  gbd_age_cuts = FALSE,                ## if TRUE, uses default GBD age cuts
  aggregate_under1 = FALSE,            ## if TRUE, aggregates < 1
  custom_age_cuts = c(0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 125),             ## List of custom age cuts
  cond_age_cuts = NULL,               ## List of conditional custom age cuts eg: list(list("cv_pregnant==1", c(0, 15, 49, 125)), list("cv_test==2", c(0, 12, 19, 140)))
  ## Settings
  #sample_threshold = 10,              ## Minimum sample size threshold, drops result if sample_size < sample_threshold #PK - take out for now so that we see unfiltered results
  quiet = FALSE,
  ## Meta vars
  vars.meta = c("nid", "survey_name",
                "ihme_loc_id", "year_start",
                "year_end", "survey_module",
                "file_path"),
  ## Subnational vars
  #vars.subnat = subnat_vars,  ## Default subnational vars
  #vars.subnat = "rando_vars",      #important
  
  ## Age variable
  vars.age = "age_year",                       ## Default age variable
  ## Design vars
  vars.design = c("strata","psu","pweight") ## Default survey design variables
)
#### Quick Clean ##################################################################################################

age_cat_ref<-fread(paste0('FILEPATH'))
age_cat_ref<-unique(age_cat_ref)

if ("age_categorical" %in% names(data) & !("age_year" %in% names(data))){
  print("age categorical")
  data <- data[age_categorical != ""]
  data$age_categorical <- as.character(data$age_categorical)
  data <- merge(data, age_cat_ref, by = "age_categorical", all.x = T)
  data <- data[age_year != 999]
  
  print("Any still need to be mapped:")
  print(table(is.na(data$age_year)))
  print("Those that need to be mapped:")
  print(table(data[is.na(age_year), "age_categorical"]))
  data$age_categorical <- NULL
}




### split #################################################################
if (admin == "not_split"){
  
  out  <- collapse.run(df = data, config = config_master, cores = 1) 
  
} else if (admin != "none" & admin != "not_split"){
  
  file_of_interest_admin <- copy(data[admin_1_id == admin])
  data <- copy(file_of_interest_admin)
  
  out  <- collapse.run(df = data, config = config_master, cores = 1) 
  unique(out$ihme_loc_id)
  out <- out[ihme_loc_id == admin]
  
} else{
  
  # remove admin values:
  names_admin <- names(data)[names(data) %like% "admin"]
  data[,(names_admin) := NULL]
  
  out  <- collapse.run(df = data, config = config_master, cores = 1) 
}

for(i in indicators){
  if(admin=='unnecessary'|admin=='none'){
    write.csv(out, paste0(output_root,sub(".*/", "", f)))
  } else {
    #save with admin_1 name in file name
    name <- sub(".*/", "", f)
    name
    name <- sub(paste0('_',str_sub(admin,end=3)),paste0('_',admin),name)
    name
    write.csv(out, paste0(output_root,name))
    
  }
  
}
