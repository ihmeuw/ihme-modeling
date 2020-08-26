#-------------------Header------------------------------------------------
# Purpose: Custom Code to check HAP extractions and generate indicators
#       Reads in files from a_ubcov_extract (FILEPATH)
#       Saves files to ubcov_extract_prepped (FILEPATH)
#          
# Purpose: Update code to extract from FILEPATH (or FILEPATH) and write to FILEPATH (or FILEPATH)
#       Reads in files from a_ubcov_extract (FILEPATH or FILEPATH)
#       Saves files to b_ubcov_extract_prepped (FILEPATH or FILEPATH)
#
# source("FILEPATH", echo=T)
#***************************************************************************

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

user <- "USERNAME"

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- paste0("ADDRESS",user,"/")
  l_root <- "ADDRESS"
  erf    <- "ADDRESS"
  share <- "ADDRESS"
  
  } else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  l_root <- "ADDRESS"
  erf    <- "ADDRESS"
  share <- "ADDRESS"
  }

if (Sys.getenv('SGE_CLUSTER_NAME') == "ADDRESS" ) {
  
  project <- "-P erf " # -p must be set on the production cluster in order to get slots and not be in trouble
  sge.output.dir <- " FILEPATH "
  #sge.output.dir <- "" # toggle to run with no output files
  
} else {
  
  project <- "" 
  sge.output.dir <- " FILEPATH "
  #sge.output.dir <- "" # toggle to run with no output files
  
}

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","dplyr")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){    install.packages(p)
  }
  library(p, character.only = T)
}

#------------------DIRECTORIES and SETTINGS--------------------------------------------------
#Toggles and settings
prep_all <- F #toggle if we want to reprep all raw extracted data
limited_use <- F #toggle for limited use or IDENT data

date <- Sys.Date() %>% as.character #used for output directory and log

#These are the pweights used in collapse code
pweights <- c("pweight","pweight_admin_1","pweight_admin_2","pweight_admin_3")
#These are the variables for which I am creating missingness indicators
missing_vars <- c("strata","psu","pweight","pweight_admin_1","pweight_admin_2","pweight_admin_3","hh_size","hhweight","cooking_fuel_mapped")

#fuel categorization
solid_fuels <- c("wood", "crop", "coal", "dung")
non_solid_fuels <- c("none", "electricity", "gas", "kerosene")

#directories
if(limited_use==F){
  home_dir <- file.path(erf,"FILEPATH")
  org_dir  <- file.path(j_root, "FILEPATH")
}else if(limited_use==T){
  home_dir <- file.path(l_root,"FILEPATH")
  org_dir <- file.path(l_root, "FILEPATH")
}

out_dir <- file.path(home_dir,"FILEPATH",date)
dir.create(out_dir, recursive = T, showWarnings = F)

#handy function
'%ni%' <- Negate('%in%')

# GBD location information
file.path(share,"FILEPATH") %>% source
locations <- get_location_metadata(35)
parent_locs <- locations[level > 2 & most_detailed==0, ihme_loc_id]
admin_1_parents <- substr(locations[location_type %in% c("admin1","ethnicity") & most_detailed==1, ihme_loc_id],1,3) %>% unique
admin_2_parents <- substr(locations[location_type=="admin2" & most_detailed==1, ihme_loc_id],1,3) %>% unique
admin_1_urban_parents <- "IND"


#This is the log where we can check to see if the data has already been prepped
log_filepath <- file.path(erf, "FILEPATH")
source_log <- fread(log_filepath)

#list of all extracted data
files <- list.files(file.path(org_dir,"FILEPATH"))

#limit to files we have not yet prepped by checking log
if(prep_all == F){
  files <- setdiff(files, source_log[reprep==0,file_name])
}


prep_and_save <- function(file){
  #reads in raw data created by "extract.do" script
  dt <- fread(file.path(org_dir,"FILEPATH",file))
  # replace "" with NA (weird STATA string thing)
  dt[dt == ""] <- NA
  # drop columns that are all NA
  dt <- dt[,which(unlist(lapply(dt, function(x)!all(is.na(x))))),with=F]
}
  
## if source is a HH module, check to make sure that hhweight variable is present (sometimes hhweight gets codebooked into the pweight variable instead)
## if not, individual-level estimates and household-level estimates will be identical - unclear why this happens but we don't want that, since there are a different number of people and households
if (unique(dt$survey_module) == "HH" & "hhweight" %ni% names(dt)) {
  stop(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
             "- is a HH module but does not have hhweight. Please re-extract!"))
}

  #check to make sure subnats are mapped propperly for GBD subnational countries
  
  if(unique(dt$ihme_loc_id) %in% parent_locs){
    loc <- substr(unique(dt$ihme_loc_id),1,3)
    if(loc %in% admin_1_parents & "admin_1_id" %ni% names(dt)){
      warning(paste0("NID: ", unique(dt$nid),". This location is a GBD subnational, but it appears that it has not been subnationally mapped at the admin 1 level. Check the data to see if we can extract subnationally!"))
    } else if(loc %in% admin_2_parents & "admin_2_id" %ni% names(dt) & loc!="IND"){
      warning(paste0("NID: ", unique(dt$nid),". This location is a GBD subnational, but it appears that it has not been subnationally mapped at the admin 2 level. Check the data to see if we can extract subnationally!"))
    } else if(loc %in% admin_1_urban_parents & "admin_1_urban_id" %ni% names(dt)){
      warning(paste0("NID: ", unique(dt$nid),". This location is a GBD subnational, but it appears that it has not been subnationally mapped at the admin 1 urban/rural level. Check the data to see if we can extract subnationally!"))
    }
    
  }
  
  collapse_vars <- intersect(names(dt),c("nid","admin_1_id","admin_1_urban_id","admin_2_id")) #These are the variables the collapse script uses, so we want missingness within each group
  
  for(mvar in missing_vars){
    if(mvar %ni% names(dt)){dt[,c(paste0("missing_",mvar)):=1]}else{
      dt[,temp:=0]
      dt[get(mvar) %in% c(NA,NaN,Inf,-Inf,"other","unknown"),temp:=1]
      dt[,paste0("missing_",mvar):=mean(temp),by=collapse_vars]
      dt[,temp:=NULL]
    }
  }
  
  #Replace missing or unrealistic hh_size
  if("hh_size" %in% names(dt)){
    
    # if household size is >35, replace with missing and give a warning, holdover from WASH
    if(nrow(dt[hh_size>35])>0){
      warning(paste0("NID: ", unique(dt$nid),". This survey has ",nrow(dt[hh_size>35])," rows with XL HH size out of ",nrow(dt)," total rows. Replacing these with median."))
    }
    dt[hh_size>35, hh_size:=NA] #this is a holdover from WASH
    
    #if household size is missing, replace with median
    dt[is.na(hh_size),hh_size:=median(dt$hh_size, na.rm = T)]
    
  }
  
  
  #Generate a dataset where each row is a HH. We can do this for HH modules or HHM modules with unique HH identifiers
  if("hh_id" %in% names(dt)){ #if we have a household id we will subset to the first row of each household
    dt_HH <- copy(dt)
    
    #These are variables which should give us a unique household
    hh_collapse_vars <- intersect(c(collapse_vars,"hh_id","psu","strata"),names(dt))
    
    #Only keep the first row for each unique household
    dt_HH[,cv_HH:=1]
    dt_HH[,keep:=1:.N,by=hh_collapse_vars]

    #calculate average pweight for hh if available
    for(weight in pweights){
      if(weight %in% names(dt_HH)){
        dt_HH[,paste0("hh_avg_",weight):=mean(get(weight),na.rm=T),by=hh_collapse_vars]
      }
    }


    dt_HH <- dt_HH[keep==1][,keep:=NULL]
    
    }else if(unique(dt$survey_module)=="HH"){ #here we are assuming all modules labeled "HH" without hh_id are indeed HH modules
      
    dt_HH <- copy(dt)
    dt_HH[,cv_HH:=1]
    
    #rename pweight for hh if available
    for(weight in pweights){
      if(weight %in% names(dt_HH)){
        dt_HH[,paste0("hh_avg_",weight):=get(weight)]
      }
    }
    
    }else if(unique(dt$survey_module)!="HH" & "hh_id" %ni% names(dt)){
    warning(paste0("NID: ", unique(dt$nid),". This appears to be a HHM module, but we do not have hh_id. Please check the data to see if this variable is available!"))
    }
  
  #for HH surveys, we should be using hhweights instead of pweights but the collapse code only takes pweights 
  if(exists("dt_HH")){
     
    #first choice is to use hhweight, second choice is to use average household pweight (this should be the same for each household member)
    if("hhweight" %in% names(dt_HH)){
      if(length(intersect(pweights,names(dt_HH)))>0){dt_HH[,intersect(names(dt),pweights):=NULL]}
      dt_HH[,pweight:=hhweight]
      
    }else if(length(intersect(pweights,names(dt)))>0){
      for(weight in intersect(pweights,names(dt))){
        dt_HH[,paste(weight):=get(paste0("hh_avg_",weight))]
      }
    }else{
      dt_HH[,pweight:=1]
      warning(paste0("NID: ", unique(dt$nid),". Does not have hhweight or pweight. Setting weights to 1."))
    }
  }
  
  #convert HH modules to HHM if they have hh_size
  if(unique(dt$survey_module)=="HH" & "hh_size" %in% names(dt)){
    dt_HHM <- copy(dt_HH)
    dt_HHM[,cv_HH:=0]
    
    #first choice is to use hhweight * hh_size, second choice is to just use hh_size. NOTE that pweight here has already been converted to hhweight
    if("pweight" %in% names(dt_HHM)){
      dt_HHM[,pweight:=pweight*hh_size]
    }else{ 
      dt_HHM[,pweight:=hh_size]
      warning(paste0("NID: ", unique(dt$nid),". To expand HH to HHM we need hhweight and hh_size, but we only have hh_size. Setting weight to hh_size."))
    }
  }else if(unique(dt$survey_module)=="HH" & "hh_size" %ni% names(dt)){
       warning(paste0("NID: ", unique(dt$nid),". This appears to be a HH module, but we do not have hh_size. Please check the data to see if this variable is available!"))
  }
  
  #Now deal with HHM modules where we don't have hh_id
  if(unique(dt$survey_module)!="HH"){
    dt_HHM <- copy(dt)
    dt_HHM[,cv_HH:=0]
    if(length(intersect(pweights,names(dt_HHM)))<1){
      dt_HHM[,pweight:=1]
      warning(paste0("NID: ", unique(dt$nid),". HHM module does not have pweights. Setting to 1"))
    }
  }
  
  #Bind together HH and HHM data tables
  if(exists("dt_HH") & exists("dt_HHM")){
     dt <- rbind(dt_HH,dt_HHM,fill=T)
  }else if(exists("dt_HH")){                                            
     dt <- dt_HH
  }else if(exists("dt_HHM")){
     dt <- dt_HHM
  }else(warning(paste0("NID: ", unique(dt$nid),". Something went terribly wrong with this extraction. UHOH!")))
 
  
  #Drop "other" and "unknown" and "NAs" as fuel_type
  dt <- dt[cooking_fuel_mapped %ni% c(NA,"other","unknown")]
  
  #Make indicators wide
  
  #binary solid v. non-solid indicator                        
  dt[,hap_solid:=0]
  dt[cooking_fuel_mapped %in% solid_fuels, hap_solid:=1]
  
  #indicator for each fuel type
  fuels <- c(solid_fuels,non_solid_fuels)
  for(fuel in fuels){
    dt[,paste0("hap_",fuel):=0]
    dt[cooking_fuel_mapped==fuel, paste0("hap_",fuel):=1]
  }
  
  #write to log
  log_row <- unique(dt[,.(file_path,ihme_loc_id,nid,survey_module,survey_name,year_end,year_start)])
  log_row[,prep_date:=date]
  log_row[,file_name:=gsub(pattern= "/", replacement = "_", x= paste0(paste(survey_name,nid,survey_module,ihme_loc_id,year_start,year_end,sep="_"),".csv"))]
  log_row[,reprep:=0]
  
  #save file by date or to separate folder for Censuses
  write.csv(dt,ifelse(grepl("CENSUS",file),
                      file.path(home_dir,"FILEPATH",log_row$file_name),
                      file.path(out_dir,log_row$file_name)),
            row.names=F)
  
  source_log <- fread(log_filepath)
  source_log <- rbind(source_log[file_name != file],log_row,fill=T)
  write.csv(source_log,log_filepath,row.names=F)
  
  print(paste0("NID: ", unique(dt$nid)," complete :)"))
  
}


save <- lapply(files,prep_and_save)