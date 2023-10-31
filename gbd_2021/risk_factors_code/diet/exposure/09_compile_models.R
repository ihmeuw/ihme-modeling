################################################################################
## DESCRIPTION ##  Compile sales and fao models from stgpr & make whole grains ratio (using 099 helper stata script)
################################################################################

rm(list = ls())

# System info & drives
os <- Sys.info()[1]
user <- Sys.info()[7]
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""

library(ggplot2)
library(data.table)
library(openxlsx)
library(reshape2)
library(msm)
# source
source("/FILEPATH/get_location_metadata.R")
source("FILEPATH/utility.r")
library("RStata")
library(dplyr)
library("data.table")

## set  version

version <- "GBD2021"


# save the runids 

runids <- fread(paste0("FILEPATH", version, ".csv")

runids <- runids[2,]

print(runids$run_id)

runids <- xxx

compiled_data_file <- paste0("FILEPATH/FAO_sales_clean_compiled_",models_compile_version, ".csv" )

##########

compile_models <- function(runids){
  
  compiled_estimates <- data.table()
  
  for( i in 1:nrow(runids)){
    row <- runids[i,]
    sales <- row$sales_model
    
    input_data <- model_load(row$run_id, "data")
    nid2 <- unique(input_data$nid)
    
    
    #we only keep modeled estimates for locations that we have input data for
    locs_to_keep <- unique(input_data$location_id)
    years_to_keep <- c(min(input_data$year_id):max(input_data$year_id))
    #however we don't want any data before 1980
    years_to_keep <- years_to_keep[which(years_to_keep %in% c(1980:2020))]
    modeled_estimates <- model_load(row$run_id, "raked")
    modeled_estimates[, nid:=nid2]
    modeled_estimates[, gbd_cause := row$me_name]
    modeled_estimates <- modeled_estimates[location_id %in% locs_to_keep & year_id %in% years_to_keep ,]                            
     compiled_estimates <- rbind(compiled_estimates, modeled_estimates)
  }
  
  compiled_estimates[, back_se:=(gpr_upper-gpr_lower)/(2*1.96)]
  compiled_estimates[, variance:=(back_se)^2]
  compiled_estimates[ grep("sale", gbd_cause), cv_sales_data:=1]
  compiled_estimates[ !(grep("sale", gbd_cause)), cv_fao:=1]
  
 
   compiled_estimates[ grep("grains", gbd_cause), ihme_risk:="diet_total_grains"]
   compiled_estimates[ grep("refined_grains", gbd_cause), ihme_risk:="diet_refined_grains"]
   compiled_estimates[ grep("fiber", gbd_cause), ihme_risk:="diet_fiber"]
   compiled_estimates[ grep("fruit", gbd_cause), ihme_risk:="diet_fruit"]
   compiled_estimates[ grep("milk", gbd_cause), ihme_risk:="diet_milk"]
   compiled_estimates[ grep("veg", gbd_cause), ihme_risk:="diet_veg"]
   compiled_estimates[ grep("salt", gbd_cause), ihme_risk:="diet_salt"]    
   compiled_estimates[ grep("calcium", gbd_cause), ihme_risk:="diet_calcium_low"]
   compiled_estimates[ grep("zinc", gbd_cause), ihme_risk:="diet_zinc"]
   compiled_estimates[ grep("eggs", gbd_cause), ihme_risk:="diet_eggs"]
   compiled_estimates[ grep("satfat", gbd_cause), ihme_risk:="diet_satfat"]
   compiled_estimates[ grep("pufa", gbd_cause), ihme_risk:="diet_pufa"]
   compiled_estimates[ grep("legumes", gbd_cause), ihme_risk:="diet_legumes"]
   compiled_estimates[ grep("pulses", gbd_cause), ihme_risk:="diet_legumes"]
   compiled_estimates[ grep("starch", gbd_cause), ihme_risk:="diet_starchy_veg"]
   compiled_estimates[ grep("poultry", gbd_cause), ihme_risk:="diet_poultry"]
   compiled_estimates[ grep("sugar", gbd_cause), ihme_risk:="diet_total_sugar"]
   compiled_estimates[ grep("fish", gbd_cause), ihme_risk:="diet_fish"]
   compiled_estimates[ grep("procmeat", gbd_cause), ihme_risk:="diet_procmeat"]
   compiled_estimates[ grep("nuts", gbd_cause), ihme_risk:="diet_nuts"]
   compiled_estimates[ grep("redmeat", gbd_cause), ihme_risk:="diet_redmeat"]
   compiled_estimates[ grep("red_meat", gbd_cause), ihme_risk:="diet_redmeat"]
   compiled_estimates[ grep("dairy", gbd_cause), ihme_risk:="diet_total_dairy"]
   compiled_estimates[ grep("omega", gbd_cause), ihme_risk:="diet_omega_3"]
   compiled_estimates[ grep("butter", gbd_cause), ihme_risk:="diet_butter"]
   compiled_estimates[ grep("cheese", gbd_cause), ihme_risk:="diet_cheese"]
   compiled_estimates[ grep("yogurt", gbd_cause), ihme_risk:="diet_yogurt"]
   compiled_estimates[ grep("energy", gbd_cause), ihme_risk:="diet_energy"]
   compiled_estimates[ grep("ssb", gbd_cause), ihme_risk:="diet_ssb"]
   compiled_estimates[ grep("transfat", gbd_cause), ihme_risk:="diet_transfat"]
   compiled_estimates[ grep("hvo", gbd_cause), ihme_risk:="diet_transfat"]
   compiled_estimates[ grep("oil", gbd_cause), ihme_risk:="diet_total_oil"]
   
  return(compiled_estimates)
  
}


make_and_add_whole_grains <- function(models_compile_version, gbd_round, just_append = FALSE){

  ##########################
  compiled_data_file <- paste0("FILEPATH/FAO_sales_clean_compiled_",models_compile_version, ".csv" )
  compiled_data <- fread(compiled_data_file)
  if("diet_whole_grains" %in% compiled_data$ihme_risk){ stop( "No need to append whole grains-- it is already in there!")}
  
  whole_grains_file <- paste0("FILEPATH/whole_grains_", models_compile_version, ".csv")
  if(!just_append){
    
    if(file.exists(whole_grains_file)){ stop(paste0("You already have whole grain data from compiled dataset ", models_compile_version))}
  jobname <- paste0("make_whole_grain")
  code <- paste0(code_dir, "FILEPATH/099_make_fao_whole_grains.do")
  arg_list <- list(version = models_compile_version, gbd_round = gbd_round)
  qsub(jobname=jobname, code, pass_argparse=arg_list, mem=2, cores=1, wallclock= "00:30:00", proj="proj_covariates", queue = "all", submit=F, shell = paste0(code_dir,"FILEPATH/stata_shell.sh"), archive_node = TRUE)
  message("pausing for 3 minutes to let job finish")
  Sys.sleep(180)   #pause for 3 minutes
  
  #check if qsub worked
  if( !file.exists(whole_grains_file)){ stop(" Warning. Make whole grains qsub seems not to have worked. Check log files and rerun.")}
  }
  if(just_append){if( !file.exists(whole_grains_file)){ stop(" Warning. There is no whole grains data to append!")}}
  whole_grains <- fread(whole_grains_file)
  whole_grains[, cv_sales_data:=as.integer(cv_sales_data)]
  compiled_data <- rbind(whole_grains, compiled_data)
  
  return(compiled_data)
  
}


#-----------------------------------------------------------------------------------
###################################################################################
#----------------------------------------------------------------------------------


compiled_estimates <- compile_models(stgpr_runids)
write.csv(compiled_estimates, compiled_data_file, row.names = FALSE, na="")

compiled_estimates_w_grains <- make_and_add_whole_grains(models_compile_version, just_append = FALSE)
write.csv(compiled_estimates_w_grains, compiled_data_file, row.names = FALSE)
