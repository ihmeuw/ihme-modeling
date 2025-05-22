################################################################################
## DESCRIPTION ##  Do network diet analysis; called by launch_crosswalk. Three parts to this script. 1- prep data, 2-do the network analysis in mr-brt, 3- adjust the data
## INPUTS ##
## OUTPUTS ##
## AUTHOR ##   
## DATE ##    
## UPDATES ## 
## UPDATE SUMMARY: Updated for GBD 2023.
################################################################################

rm(list = ls())

## Config ----------------------------------------------------------------------#

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "H:/"
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""

# load libraries
library(ggplot2)
library(data.table)
library(openxlsx)
library(reshape2)
library(msm)
library(plyr)
library(dplyr)
library(readstata13)
library(readr)

# load shared functions
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
library(reticulate)
reticulate::use_python("/FILEPATH/python")
mr <- import("mrtool")
cw <- import("crosswalk")


#-----------------------#
#Set Args
#-----------------------#

#Risk(s) to run

risks <- c("diet_fruit","diet_fiber","diet_milk","diet_veg","diet_calcium_low","diet_salt","diet_pufa","diet_legumes",
           "diet_procmeat","diet_nuts","diet_transfat", "diet_omega_3","diet_whole_grains","diet_redmeat","diet_ssb")

# argument assignment

  release <- 16
  location_set <- 35
  version <- "v_no" 
  do_data_prep <- T 
  do_xwalk <- T
  adjdata <- T 
  save <- TRUE
  base_save_dir <- "FILEPATH/cross_walk/"
  base_data_dir <- "FIELPATH/as_split/"
  
  global = FALSE #as of gbd2023, we use a SR specific cw, where data is sufficient. 
                #Some RFs end up using a global cw for all SRs, and "global" is recoded in those cases"
  bundle = "wo_fao_stgpr" 
  
  
#Setting directories for the input data and where to save files based on bundle used and global or SR-specific cws
  
  if(bundle=="wo_fao_stgpr"){
    dir_data_to_crosswalk <- paste0(base_data_dir, "split_data/gbd2021_outliers_applied/") 
    if(global==TRUE)  { save_dir <- paste0(base_save_dir, "fit_crosswalk_global/")} #global
    if(global==FALSE) { save_dir <- paste0(base_save_dir, "fit_crosswalk/") } #super region specific crosswalks saved here
  }
  
  if(bundle=="fao_stgpr"){
    dir_data_to_crosswalk <- paste0(base_data_dir, "split_data_fao_stgpr/gbd2021_outliers_applied/")
    if(global==TRUE)  { save_dir <- paste0(base_save_dir, "with_fao_stgpr/fit_crosswalk_global/")} #global
    if(global==FALSE) { save_dir <- paste0(base_save_dir, "with_fao_stgpr/fit_crosswalk/") } #super region specific crosswalks saved here
  }
 
   if(bundle=="gbd2021"){
    dir_data_to_crosswalk <- paste0(base_data_dir, "split_data_gbd2021/")
    if(global==TRUE)  { save_dir <- paste0(base_save_dir, "applied_to_gbd2021/fit_crosswalk_global/")} #global
    if(global==FALSE) { save_dir <- paste0(base_save_dir, "applied_to_gbd2021/fit_crosswalk/") } #super region specific crosswalks saved here
  }
 

if (!interactive()){
args = commandArgs(trailingOnly=TRUE)
risk <- args[1]
print(risk)
}

######################################################################################
# Part 1: #Load data and functions
######################################################################################


for (i in risks){
  risk <- i

## Risks that will use a global model:    
  if(risk == "diet_legumes" | risk == "diet_omega_3" | risk == "diet_transfat" | risk == "diet_procmeat"){
    global = TRUE
  }
  
  message(paste0("Running script for: ", risk))
  message(paste0("global model: ", global))
  
  #Confirm planned actions: 
  message(paste0("Running data prep code: ", do_data_prep))
  message(paste0("Running crosswalk models: ", do_xwalk))
  message(paste0("Adjusting data: ", adjdata))
  message(paste0("Saving results: ", save))
  }

## Import data -----------------------------------------------------------------#

  
#diet data to be crosswalked
  all_diet_data2 <- fread(paste0(dir_data_to_crosswalk, risk, ".csv"))

    setnames(all_diet_data2, "val", "mean")

    all_diet_data2 <- all_diet_data2 %>%
      mutate (sex_id = case_when(
        sex == "male" ~ 1, 
        sex == "female" ~ 2, 
        sex == "both" ~ 3
      ))
    
    all_diet_data2 <- all_diet_data2[!is.na(mean)] 
    all_diet_data2 <- all_diet_data2[, location_id := ifelse(location_id == 4940, 93, location_id)] #recode sweden except stockholm to sweden
    all_diet_data2 <- all_diet_data2[, location_id := ifelse(location_id == 4944, 93, location_id)] #recode stockholm to sweden
    
    
    #Risk-specific Outliers

    all_diet_data2[(location_id == 6 & cv_fao== 1), is_outlier:=1] #aggregate China fao data 
    

    # outlier FAO data for locations with unrealstic data(unreallstically extremely high or unrealsstically extremely low compared to the region)

    if(risk == "diet_legumes"){ 
      all_diet_data2[(location_id == 26 & cv_fao== 1), is_outlier:=1] 
      all_diet_data2[(location_id == 25 & cv_fao== 1), is_outlier:=1]  
      all_diet_data2[(location_id == 173 & cv_fao== 1), is_outlier:=1] 
      }
    
    if(risk == "diet_fruit"){   
      all_diet_data2[(location_id == 27 & cv_fao== 1), is_outlier:=1]  
      all_diet_data2[(location_id == 26 & cv_fao== 1), is_outlier:=1]  
      all_diet_data2[(location_id == 30 & cv_fao== 1), is_outlier:=1]  
      all_diet_data2[(location_id == 215 & cv_fao== 1), is_outlier:=1] 
      }
    
    if(risk == "diet_redmeat"){   
      all_diet_data2[(location_id == 23 & cv_fao== 1), is_outlier:=1] 
      all_diet_data2[(location_id == 27 & cv_fao== 1), is_outlier:=1] 
      all_diet_data2[(location_id == 26 & cv_fao== 1), is_outlier:=1] 
      }
    
    if(risk == "diet_milk"){
      all_diet_data2[(location_id == 23  & cv_fao== 1), is_outlier:=1] 
      all_diet_data2[(location_id == 26  & cv_fao== 1), is_outlier:=1] 
      all_diet_data2[(location_id == 132 & cv_fao== 1), is_outlier:=1]  
      all_diet_data2[(location_id == 192 & cv_fao== 1), is_outlier:=1]
      all_diet_data2[(location_id == 61  & cv_fao== 1), is_outlier:=1] 
       }
    
    if(risk == "diet_calcium_low"){
    }
    
    if(risk == "diet_nuts"){ 
      all_diet_data2[(location_id == 204 & cv_fao== 1), is_outlier:=1] 
    }
    
    if(risk == "diet_ssb"){ 
      all_diet_data2[(location_id == 93 & nid== 229834), is_outlier:=1] 
    }
   
     if(risk == "diet_fiber"){ 
      all_diet_data2[(cv_FFQ == 1), is_outlier:=1] 
      all_diet_data2[(cv_fao == 1 & location_id == 522 & year_id < 2010), is_outlier:=1] 
    }
    
    if(risk == "diet_nuts"){
     all_diet_data2 <- all_diet_data2[cv_cv_hhbs!=1]
       }
       
    ##Changes to the FAO data - need to use bundle for crosswalk
    all_diet_data <- all_diet_data2      
       
    
    # Data for crosswalk model - using gbd 2021 file for legumes 
    if(risk == "diet_legumes" ){ 
      all_diet_data <- fread("/FILEPATH/2019_3/all_diet_data_post_as_2019_3.csv") #
      setnames(all_diet_data, "val", "mean")
      all_diet_data <- all_diet_data[ihme_risk==risk]  
    }       
    
    
#read in location metadata 
loc <- data.table(get_location_metadata(location_set_id = location_set, release_id = release)) 
  loc <- loc[, .(location_id, location_name, map_id, path_to_top_parent, super_region_name, region_name, super_region_id, region_id, location_type_id, location_type, level)]
  loc_short <- loc[, .(location_id, map_id, path_to_top_parent, super_region_name, super_region_id)]
  sr_ids <- loc[, .(location_id, super_region_id)]
 

## Load Functions -----------------------------------------------------------------#

## Function predict_data_prep: purpose to prepare the data that the crosswalk adjustments will be applied to
predict_data_prep <- function(risk, save = TRUE ){

    if(!exists("all_diet_data2")){ stop("all_diet_data2 is not loaded in the environment")}
  
  if(!risk %in% unique(all_diet_data2$ihme_risk)){
    stop("risk is not one of the gbd diet risk")}
  
  risk_data <- all_diet_data2[ihme_risk==risk]
  
  setnames(risk_data, "cv_sales_data", "cv_sales")
  if(risk!="diet_transfat"){setnames(risk_data, "se_adj", "was_se_imputed")}
  
  # Generate a single variable for data type
  cols <- c("cv_dr", "cv_FFQ", "cv_cv_hhbs", "cv_fao", "cv_cv_urinary_sodium", "cv_sales")
  types <- c("cv_dr", "cv_FFQ", "cv_cv_hhbs", "cv_fao", "cv_cv_urinary_sodium", "cv_sales")
  for(i in seq_along(cols)) { # Loop through each column name
    risk_data[get(cols[i]) == 1, type := types[i]] # Update 'type' where column value is 1
  }
  if (anyNA(risk_data$type)) {
    message("NA values found in the required columns: 'type. Dropping these obs")
    risk_data <- risk_data[!is.na(type)]
  }
  
  #Generate a variable to identify where both sexes are represented 
  risk_data[sex=="Both", c("sex_id", "was_both_sex"):=list(1,1)]
  risk_data[sex!="Both", was_both_sex:=0]
  rep_both <- risk_data[sex=="Both"]
  rep_both$sex_id <- 2
  risk_data <- rbind(risk_data,rep_both)
  
  risk_data$male <- ifelse(!is.na(risk_data$sex_id) & risk_data$sex_id == 1, 1, 0)
  risk_data$female <- ifelse(!is.na(risk_data$sex_id) & risk_data$sex_id == 2, 1, 0)
  
  risk_data <- risk_data %>%
    mutate(sex_id = ifelse(is.na(sex_id) & male != 1 & female != 1 & sex == "Male", 1,
                           ifelse(is.na(sex_id) & male != 1 & female != 1 & sex == "Female", 2, sex_id)))
  risk_data <- risk_data %>%
    mutate(male = ifelse(male != 1 & sex_id ==1, 1, male))
  risk_data <- risk_data %>%
    mutate(female = ifelse(female != 1 & sex_id ==2, 1, female))
  risk_data <- risk_data %>%
    mutate(sex = ifelse(sex_id == 1, "Male",
                        ifelse(sex_id == 2, "Female", sex)))
  
  
  risk_data <- merge(risk_data, loc_short, by = c("location_id"), all.x = TRUE) 
  
  #Zero values need to be adjusted = 0.5 x lowest non-zero value

  lowest_non_zero <- min(risk_data$mean[risk_data$mean != 0], na.rm = TRUE)
  lowest_non_zero_half<- lowest_non_zero*0.5
  n <- sum(risk_data$mean == 0)
  risk_data[mean == 0, mean := lowest_non_zero_half] 
    
  #Some risk-data pairs did not make it in the crosswalk model, therefore must be dropped!

  if ( risk == "diet_legumes" | risk ==  "diet_whole_grains"){
    risk_data[cv_cv_hhbs==1, is_outlier:=1] #hhbs data did not get crosswalked and should be dropped 
  }
  
  if ( risk == "diet_procmeat"){  
    risk_data[cv_fao==1, is_outlier:=1] #fao data did not get crosswalked and should be dropped
  }
  
  if ( risk == "diet_nuts"){  
    risk_data[cv_sales==1, is_outlier:=1] #sales data did not get crosswalked and should be dropped
  }
  
  if (risk == "diet_fruit"   | risk == "diet_milk"){
    risk_data[(cv_cv_hhbs == 1 & super_region_id == 158), is_outlier:=1] #outlier South Asia Household budget data - because the data is unrealistically high
  }
  
  if (risk == "diet_veg"){
    risk_data[(cv_cv_hhbs == 1 & super_region_id == 158), is_outlier:=1] #outlier South Asia Household budget data - because the data is unrealistically high
    risk_data[(cv_cv_hhbs == 1 & location_id == 62), is_outlier:=1] ##outlier Russia Household budget data - because the data is unrealistically high
  }
  
  if ( risk == "diet_milk"){ 
    risk_data[(location_id == 33 & year_id== 2006 & cv_cv_hhbs == 1), is_outlier:=1] # outlier negative values 
  }
  
  if(risk=="diet_whole_grains"){
    risk_data[(location_id == 86 & year_id== 1994 & cv_fao == 1), is_outlier:=1] #outlier negative values 
  }
  
  if (risk == "diet_transfat"){
      #outlier FFQ for selected locations where the intake is unrealistically high
    risk_data[(cv_FFQ == 1 & location_id == 35513), is_outlier:=1] 
      #outlier sales data for selected locations where the intake is unrealistically high
    risk_data[(cv_sales == 1 & location_id == 180 & year_id < 2008), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 123 & year_id < 2008), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 130 & year_id < 2008), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 141 & year_id < 2008), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 142 & year_id < 2008), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 101 & year_id < 2009), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 102 & year_id < 2009), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 89 & year_id < 2008), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 84 & year_id < 2008), is_outlier:=1] 
    }
  
  if(risk=="diet_omega_3"){
    risk_data[cv_FFQ ==1, is_outlier := 1] #excluded FFQ data from CW for omega-3 because there are only FFQ data points
  }
  
  risk_data <- risk_data[is_outlier==0]
  
  risk_data <- risk_data[!is.na(standard_error)]
  
  if (save){write.csv(risk_data, paste0(save_dir, "adjustment_inputs/", risk, "_pre_xwalk.csv"))
  message(paste0("saved data to: ", save_dir, "adjustment_inputs/", risk, "_pre_xwalk.csv"))
  }else{
    message(paste0("Data for ", risk, " NOT saved"))
  }

   return(risk_data)
}

## Function data_prep: purpose to prepare data for matching and crosswalk fit

data_prep <- function(risk) {
  if(!exists("all_diet_data")){ stop("all_diet_data is not loaded in the environment")}
  if(!risk %in% unique(all_diet_data$ihme_risk)){
    stop("risk is not one of the gbd diet risk")}

  risk_data <- all_diet_data[ihme_risk==risk & is_outlier==0] # - removing outliers
  
  risk_data <- merge(risk_data, sr_ids, by = c("location_id"))

  setnames(risk_data, c("cv_sales_data","mean"), c("cv_sales", "data"))

  print(paste0("There are ", nrow(risk_data[sex=="Both"]), " rows of both sex data. These are duplicated out for males and females"))
  print("The data distribution of both sex  datapoints is: ")
  print(risk_data[sex=="Both", .(datapoint_count = .N), by = list(cv_fao, cv_sales, cv_FFQ, cv_cv_hhbs)])
  
  #duplicate both sex 
  risk_data$sex_id[risk_data$sex == "Male"] <- 1 
  risk_data$sex_id[risk_data$sex == "Female"] <- 2
  risk_data$sex_id[risk_data$sex == "Both"] <- 1
  rep_both <- risk_data[sex=="Both"]
  rep_both$sex_id <- 2
  risk_data <- rbind(risk_data,rep_both)
  
  risk_data$male <- ifelse(!is.na(risk_data$sex_id) & risk_data$sex_id == 1, 1, 0)
  risk_data$female <- ifelse(!is.na(risk_data$sex_id) & risk_data$sex_id == 2, 1, 0)
  

  #outlier some risk specific things--------------------------------------------#
  
  if(risk=="diet_milk"){
    risk_data[(location_id == 33 & year_id== 2006 & cv_cv_hhbs == 1), is_outlier:=1]  # outlier hhbs milk data for Armenia 2006 because it is unrealstic(negative)
  }
  
  if ( risk == "diet_nuts"){  
    risk_data[cv_sales==1, is_outlier:=1] #outlier sales data for selected locations where the intake is high

  }
  
  if (risk == "diet_fruit"  | risk=="diet_milk"){
    risk_data[(cv_cv_hhbs == 1 & super_region_id == 158), is_outlier:=1] #outlier hhbs data for South Asia - unrealistically high
  }
  
  if (risk == "diet_veg"){
    risk_data[(cv_cv_hhbs == 1 & super_region_id == 158), is_outlier:=1] #outlier hhbs data for South Asia - unrealistically high
    risk_data[(cv_cv_hhbs == 1 & location_id == 62), is_outlier:=1] ##outlier hhbs data for Russia - unrealistically high
  }
  
  if(risk=="diet_whole_grains"){
    risk_data[(location_id == 86 & year_id== 1994 & cv_fao == 1), is_outlier:=1] # outlier FAO data for Italy 1994 because it is unrealiastic(negative)
  }
  
  if(risk=="diet_ssb"){
    risk_data[svy=="National Health and Nutrition Examination Survey", is_outlier:=1]
  }
  
  if(risk=="diet_whole_grains"){
    risk_data[location_id==130, is_outlier:=1]
    risk_data[(location_id == 86 & year_id== 1994 & cv_fao == 1), is_outlier:=1] # outlier FAO data for Italy 1994 because it is unrealiastic(negative)
  }

  if(risk=="diet_legumes"){
  risk_data[cv_cv_hhbs==1, is_outlier:=1]  # outlier Brazil hhbs data-it is unrealstic. 
  risk_data[(nid==301094 & data == 0), is_outlier:=1] # drop duplicates data sources.
  risk_data[(nid==3011101 & location_id == 90 & year_id == 2000 & sex_id == 2 & age_group_id ==14), is_outlier:=1] #very small recall data point with large SE
  risk_data[(nid==112656 & location_id == 68 & year_id == 2009 & sex_id == 1 & age_group_id ==30), is_outlier:=1] #very small recall data point with large SE
  risk_data[(nid==135721 & location_id == 68 & year_id == 2012 & sex_id == 2 & age_group_id ==31), is_outlier:=1] #very small recall data point with large SE
}
  
  if(risk=="diet_omega_3"){
    risk_data[cv_FFQ ==1, is_outlier := 1] 
    risk_data[(nid==414325 & location_id == 68 & year_id == 2013 & age_group_id ==13), is_outlier:=1] # outlier because the values are unrealstically low values
    risk_data[(cv_fao==1 & location_id ==102), is_outlier:=1] #outlier US FAO data
    message("dropping some things")
  }

  if(risk=="diet_procmeat"){

     #outlier: data points are unrealstic: 

    risk_data[cv_fao==1, is_outlier:=1]     
    loc <- data.table(get_location_metadata(location_set_id = location_set, release_id = release))
    india_loc <- loc[location_id==6 | parent_id==6, location_id]
    risk_data[location_id %in% india_loc, is_outlier:=1]   
    risk_data[location_id==68 & cv_sales ==1, is_outlier:=1] 
    risk_data[location_id==62 & cv_cv_hhbs ==1, is_outlier:=1] 
    }
  
  if (risk == "diet_transfat"){
          #outlier FFQ for selected locations where the intake is unrealistically high
    risk_data[(cv_FFQ == 1 & location_id == 35513)] 
          #outlier sales data for selected locations where the intake is unrealistically high
    risk_data[(cv_sales == 1 & location_id == 180 & year_id < 2008), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 123 & year_id < 2008), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 130 & year_id < 2008), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 141 & year_id < 2008), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 142 & year_id < 2008), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 101 & year_id < 2009), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 102 & year_id < 2009), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 89 & year_id < 2008), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 84 & year_id < 2008), is_outlier:=1] 
    risk_data[(cv_sales == 1 & location_id == 90), is_outlier:=1] 
  }

#--------------------------------------------------------------------------------#

 risk_data <- risk_data[is_outlier==0] 
  
  risk_data$age_midpt <- (risk_data$age_end + risk_data$age_start)/2
  
  keepvar <- c("location_id", "year_id","data","standard_error","se_adj","nid","sex_id","age_group_id", 
              "sample_size","cv_fao","cv_dr","cv_sales","cv_FFQ","cv_cv_hhbs", "cv_cv_urinary_sodium", "age_midpt")
 
  risk_data <- risk_data[, keepvar, with=FALSE]
  print("The data distribution of all datapoints (after duplicating both sex ones) is: ")
  print(risk_data[, .(datapoint_count = .N), by = list(cv_fao, cv_dr, cv_sales, cv_FFQ, cv_cv_hhbs)])
  
  
  ################
  print("se adjustment is necessary if there is no se ")
  print(risk_data[se_adj==1, .(se_adjustment = .N), by = list(cv_fao, cv_dr, cv_sales, cv_FFQ, cv_cv_hhbs)])
  ################

  return(risk_data)
  
}

## Function match_data: purpose to format data as matched on sex-age-location-year so that the data can be read into a crosswalk model.
match_data <- function(risk) {
  
  #first load in data
  print(paste0("-------------- ", risk, " ------------"))
  print("--------------------------------------")
   bundle_prep <- data_prep(risk) 
  
  #determine which types of data the risk has and which reference-alternate definitions to find matches for
  all_cvs <- c("cv_fao", "cv_sales", "cv_FFQ","cv_cv_hhbs","cv_cv_urinary_sodium")
  alternate_defs <- c("cv_dr")
  for(cv in all_cvs){
    if(sum(bundle_prep[,get(cv)], na.rm=TRUE) > 1){alternate_defs <- c(alternate_defs, cv)}}
  
  xwalks <- as.data.table(t(combn(alternate_defs,2)))
  
  setnames(xwalks, c("V1","V2"), c("reference","alternate"))
  if(risk=="diet_salt"){setnames(xwalks, c("reference","alternate"), c("alternate","reference")) #klh03 - Why?
    setcolorder(xwalks, c("reference", "alternate"))}

    if(risk=="diet_legumes"){
    xwalks <- xwalks[!(reference=="cv_fao" & alternate=="cv_sales")] 
  }
  alternate_defs <- xwalks$alternate
  reference_defs <- xwalks$reference
  print(xwalks)
  
  # for each reference-alternate definition, find all common "full" matches, reshapes wide and makes ratio and se columns. 
  risk_matched <- data.table()
  for( n in 1:nrow(xwalks)){
    pair_1 <- xwalks[n,1]$reference
    pair_2 <- xwalks[n,2]$alternate
    
    message(n)
    bundle_prep <- data_prep(risk)
    
    if(risk== "diet_salt"){ 
      bundle_prep[, year_id:=round_any(year_id, 10)]
     bundle_prep[age_group_id %in% c(1:20), age_group_id:=round_any(age_group_id, 2)]} 
    
    if( pair_1== "cv_FFQ" | pair_2== "cv_FFQ"){
     bundle_prep[, year_id:=round_any(year_id, 5)]}  #trying to get more FFQ matches by expanding to 5 year bins.
  
    if(pair_1=="cv_fao" & pair_2=="cv_sales"){
      bundle_prep <- bundle_prep[year_id %in% c(1980, 1985, 1990, 1995, 2000, 2005, 2010) & age_group_id %in% c(seq(2, 20, by=3),30)] #limiting FAO & sale pairs to every 5 years between 1980-2010 and collapses age_grou_id into groups of 3
    }
    
    
    bundle_prep[, full := paste(location_id, age_group_id, year_id, sex_id, sep = "-")] 
    #-------------------------------------------------------------#
   
     for (def in unique(c(reference_defs,alternate_defs))) {
      col <- paste0("has_", def)
      bundle_prep[, paste0(col) := as.numeric(sum(get(def)) > 0), by=full]
     }
    
    matched_data <- bundle_prep[get(paste0("has_", pair_1))==1 & get(paste0("has_", pair_2))==1]
    matched_data <- matched_data[get(pair_1)==1 | get(pair_2)==1]
  
    
    if( nrow(matched_data)>1){
      matched_data$def <- "ref"
      matched_data[get(pair_2)==1, def:="alt"]

      ref <- matched_data[def=="ref"]
      alt <- matched_data[def=="alt"]
      matched_wide <- merge(ref, alt, by=c("full","location_id", "year_id","sex_id","age_group_id"), allow.cartesian = TRUE)
     
      
      if(risk=="diet_salt" & (pair_1=="cv_cv_urinary_sodium" & pair_2=="cv_dr")){
        validation_data <- fread("FILEPATH/sodium_validation_data_cleaned.csv")
        validation_data <- validation_data[comparison=="dr"]
        place <- colnames(validation_data)[which(colnames(validation_data) %in% colnames(matched_wide))]
        validation_data <- validation_data[,place, with=FALSE]
        matched_wide <- rbind(matched_wide, validation_data, fill=TRUE)
      }
      
      
      if(risk=="diet_salt" & (pair_1=="cv_cv_urinary_sodium" & pair_2=="cv_FFQ")){
        validation_data <- fread("FILEPATH/sodium_validation_data_cleaned.csv")
        validation_data <- validation_data[comparison=="FFQ"]
        place <- colnames(validation_data)[which(colnames(validation_data) %in% colnames(matched_wide))]
        validation_data <- validation_data[,place, with=FALSE]
        matched_wide <- rbind(matched_wide, validation_data, fill=TRUE)
      }
      
      #Zero values need to be adjusted = 0.5 x lowest non-zero value: diet_legumes, diet_nuts, whole grains
        lowest_non_zero_x <- min(matched_wide$data.x[matched_wide$data.x != 0], na.rm = TRUE)
        lowest_non_zero_x_half<- lowest_non_zero_x*0.5
        n <- sum(matched_wide$data.x == 0)
        matched_wide[data.x == 0, data.x := lowest_non_zero_x_half]
        
        lowest_non_zero_y <- min(matched_wide$data.y[matched_wide$data.y != 0], na.rm = TRUE)
        lowest_non_zero_y_half<- lowest_non_zero_y*0.5
        n <- sum(matched_wide$data.y == 0)
        matched_wide[data.y == 0, data.y := lowest_non_zero_y_half]
      
      # Creating the dependent variable  
      matched_wide[, c("ref_mean2", "ref_se2")] <- cw$utils$linear_to_log(
        mean = array(matched_wide$data.x),
        sd = array(matched_wide$standard_error.x)
      )
      matched_wide[, c("alt_mean2", "alt_se2")] <- cw$utils$linear_to_log(
        mean = array(matched_wide$data.y),
        sd = array(matched_wide$standard_error.y)
      )
      matched_wide <- matched_wide %>%
        mutate(
          ydiff_log_mean =  alt_mean2 - ref_mean2,
          ydiff_log_se = sqrt(alt_se2^2 + ref_se2^2)
        )
      
      setnames(matched_wide, c("nid.x","nid.y", "data.x", "data.y", "age_midpt.x", "age_midpt.y"), c("nid.ref","nid.alt", "data.ref", "data.alt", "age_midpt.ref", "age_midpt.alt"))
     
      keep_vars <- c("location_id", "year_id","sex_id","age_group_id", "nid.alt","nid.ref", "data.ref", "data.alt", 
                     "ref_mean2", "ref_se2", "alt_mean2", "alt_se2", "ydiff_log_mean", "ydiff_log_se", "age_midpt.ref", "age_midpt.alt")
      
      matched_wide <- matched_wide[,keep_vars, with=FALSE]
      matched_wide[, reference := pair_1]
      matched_wide[, alternate := pair_2]
      matched_wide[, comp := paste0(pair_1, "-", pair_2)]
      matched_wide[, id := paste0(nid.alt, "-", nid.ref)]
      #need to set up dummy vars for MR BRT
      clean_data <- matched_wide
      risk_matched <- rbind(risk_matched, clean_data)
    
    }
  }
  
  ## Average age 
  risk_matched$avg_age <- (risk_matched$age_midpt.alt + risk_matched$age_midpt.ref)/2
  
  #### set it up for network
  large_se <- which(risk_matched$ydiff_log_se > 9999)
  large_rr <- which(risk_matched$ydiff_log_mean > 9999)
  if(length(large_se) > 0 | length(large_rr) > 0 ){
    n <- unique(c(large_se, large_rr))
    print(paste0("There are ", length(n), " impossibly large se or rr values that have been dropped."))
    risk_matched <- risk_matched[-n, ]      
  }
  
  if(nrow(risk_matched > 0)){
  if(risk=="diet_salt"){ risk_matched[alternate=="cv_dr", cv_dr:=1]}
  if("cv_fao" %in% risk_matched$alternate){risk_matched[alternate=="cv_fao",cv_fao:=1]}
  if("cv_sales" %in% risk_matched$alternate){risk_matched[alternate=="cv_sales",cv_sales:=1]}
  if("cv_FFQ" %in% risk_matched$alternate){risk_matched[alternate=="cv_FFQ",cv_FFQ:=1]}
  if("cv_cv_hhbs" %in% risk_matched$alternate){risk_matched[alternate=="cv_cv_hhbs",cv_cv_hhbs:=1]}
  
  #assign reference vals -1 if they are indirect
  if("cv_fao" %in% risk_matched$reference){risk_matched[reference=="cv_fao",cv_fao:=-1]}
  if("cv_sales" %in% risk_matched$reference){risk_matched[reference=="cv_sales",cv_sales:=-1]}
  if("cv_FFQ" %in% risk_matched$reference){risk_matched[reference=="cv_FFQ",cv_FFQ:=-1]}
  if("cv_cv_hhbs" %in% risk_matched$reference){risk_matched[reference=="cv_cv_hhbs",cv_cv_hhbs:=-1]}
  risk_matched[is.na(risk_matched)] <- 0
  
  risk_matched <- merge(risk_matched, loc_short, by = c("location_id"), all.x = TRUE)
  
  #####
  print(paste0("The number of matches for ", risk, " are:"))
  print(table(risk_matched$comp))
  print("The location id breakdown is: ")
  print(risk_matched[, .(loc_count = .N), by = location_id])
  print("--------------------------------------")
  risk_matched$comp <- NULL
  }else{
    print("Ooops there are no matches for the data!")
  }

return(risk_matched)
}


#-----------------------------------------------------------------------------#
#Crosswalk-Specific Functions

#cw model to generate priors for SR models
cw_for_sr_priors <- function(df_matched, cv_gold, inlier){
  
  #Fit global model
  df_glob <- cw$CWData(
    df = df_matched,         
    obs = "log_diff",         
    obs_se = "log_diff_se",  
    alt_dorms = "altvar",     
    ref_dorms = "refvar",     
    study_id = "group_id" ,   
    add_intercept = TRUE    
  )
  
  fit_glob <- cw$CWModel(
    cwdata = df_glob,        
    obs_type = "diff_log",  
    cov_models = list(       
      cw$CovModel(cov_name = "intercept")), 
    gold_dorm = cv_gold   
  )
  
  fit_glob$fit(inlier_pct=inlier)
  
  df_tmp_glob <- fit_glob$create_result_df()
  df_tmp_glob <- df_tmp_glob %>%
    select(dorms, cov_names, beta, beta_sd, gamma)
  
  setDT(df_tmp_glob)
  
  return(df_tmp_glob)
  
}


#Global cw
fit_and_adjust_global_cw <- function(df_matched, data_covs , cv_gold, inlier, save_dir, risk, risk_data, prior_beta_gaussian_list = NULL){
  
  # Create a list to store the covariate models
  model_covs <- list()
  
  # Always add the intercept model, with or without prior_beta_gaussian
  if (!is.null(prior_beta_gaussian_list) && "intercept" %in% names(prior_beta_gaussian_list)) {
    model_covs[[length(model_covs) + 1]] <- cw$CovModel(
      cov_name = "intercept",
      prior_beta_gaussian = prior_beta_gaussian_list[["intercept"]]
    )
  } else {
    model_covs[[length(model_covs) + 1]] <- cw$CovModel(cov_name = "intercept")
  }
  
  # Add other covariate models as specified in data_covs
  for (cov_name in data_covs) {
    if (!is.null(prior_beta_gaussian_list) && cov_name %in% names(prior_beta_gaussian_list)) {
      model_covs[[length(model_covs) + 1]] <- cw$CovModel(
        cov_name = cov_name,
        prior_beta_gaussian = prior_beta_gaussian_list[[cov_name]]
      )
    } else {
      model_covs[[length(model_covs) + 1]] <- cw$CovModel(cov_name = cov_name)
    }
  }
  
  df_male <- cw$CWData(
    df = df_matched,         
    obs = "log_diff",        
    obs_se = "log_diff_se",  
    alt_dorms = "altvar",     
    ref_dorms = "refvar",    
    covs = data_covs,     
    study_id = "group_id" ,   
    add_intercept = TRUE      
  )
  
  fit_cwmodel <- cw$CWModel(
    cwdata = df_male,          
    obs_type = "diff_log",  
    cov_models = model_covs,
    gold_dorm = cv_gold  
  )
  
  fit_cwmodel$fit(inlier_pct=inlier)
  
  df_tmp1 <- fit_cwmodel$create_result_df()
  
  cw_results_glob <- df_tmp1 %>%
    select(dorms, cov_names, beta, beta_sd, gamma)
  cw_results_glob$male <- ifelse(!is.na(cw_results_glob$cov_names) & cw_results_glob$cov_names == "male", 1, 0)
  cw_results_glob$female <- ifelse(!is.na(cw_results_glob$cov_names) & cw_results_glob$cov_names == "intercept", 1, 0)
  
  risk_data_global <- risk_data
  
  pred_temp <- fit_cwmodel$adjust_orig_vals(
    df = risk_data_global, 
    orig_dorms = "type",
    orig_vals_mean = "mean",
    orig_vals_se = "standard_error"
  )
  
  head(pred_temp) 
  
  
  risk_data_global[, c(                                              
    "meanvar_adjusted", "sdvar_adjusted", "pred_logit", 
    "pred_se_logit", "data_id")] <- pred_temp
  
  head(risk_data_global)
  
  return(list(fit1 = fit_cwmodel, dt_coeffs = cw_results_glob, dt_adjusted_data = risk_data_global, df_tmp1 = df_tmp1))
}



fit_and_adjust_sr_model <- function(df_matched_sr, risk_data_sr, prior_beta_gaussian_list, 
                                    risk, sr_id, theta, cv_gold, inlier) {
  # Create CWData object
  df_sr <- cw$CWData(
    df = df_matched_sr,
    obs = "log_diff",
    obs_se = "log_diff_se",
    alt_dorms = "altvar",
    ref_dorms = "refvar",
    covs = list("male"),
    study_id = "group_id",
    add_intercept = TRUE
  )
  
  # Create CWModel object
  fit_sr <- cw$CWModel(
    cwdata = df_sr,
    obs_type = "diff_log",
    cov_models = list(
      cw$CovModel(cov_name = "male"),
      cw$CovModel(cov_name = "intercept", prior_beta_gaussian = prior_beta_gaussian_list)
    ),
    gold_dorm = cv_gold
  )
  
  # Fit the CWModel
  fit_sr$fit(inlier_pct=inlier)
  message(paste0(risk, " - SR ", sr_id, "; theta = ", theta, " : SR-specific cw model has been fit"))
  
  # Output cw results
  df_tmp_sr <- cw_results(fit_sr)
  df_tmp_sr$super_region_id <- sr_id
  df_tmp_sr$theta <- theta

 
  # Apply the adjustment
  pred_temp <- fit_sr$adjust_orig_vals(
    df = risk_data_sr,
    orig_dorms = "type",
    orig_vals_mean = "mean",
    orig_vals_se = "standard_error"
  )
  
  # Add adjusted values to risk_data_sr
  risk_data_sr[, c(
    "meanvar_adjusted", "sdvar_adjusted", "pred_logit",
    "pred_se_logit", "data_id"
  )] <- pred_temp
  risk_data_sr$theta <- theta
  risk_data_sr$sr_id <- sr_id
  
  # Return the results as a list
  return(list(
    df_tmp_sr = df_tmp_sr,
    risk_data_sr = risk_data_sr
  ))
}



# cw results: cw_results(fit)
cw_results <- function(fit) {
  df_tmp_sr <- fit$create_result_df()
  df_tmp_sr <- df_tmp_sr %>%
    select(dorms, cov_names, beta, beta_sd, gamma)

  print(df_tmp_sr)
  return(df_tmp_sr)
}


# Define a helper function to check for non-zero values in a column - applied for prior_beta_gaussian
has_nonzero_values <- function(column_name, data_frame) {
  if (column_name %in% names(data_frame) && any(data_frame[[column_name]] != 0)) {
    TRUE
  } else {
    FALSE
  }
}



##################################################################################
## Implement crosswalk models and adjusting the data 

#-----------------------------------------------------------------------------------#
# Step 1: Prep data
#-----------------------------------------------------------------------------------#

# 1a. Prepare matched data for the crosswalk model-----#

if(do_data_prep){
  
    data <- match_data(risk)
    cols <- names(data)[which(colnames(data) %in% c("cv_fao","cv_sales","cv_cv_hhbs","cv_FFQ", "cv_dr"))]
    data[, comp := paste0(reference, "-", alternate)]
    num_pairs <- length(unique(data$comp))
    table(data$comp)
    area_n <- uniqueN(data$location_id)
    comp_n <- uniqueN(data$comp)
    inlier <- 1 # 1-trim% 
    if (risk == "diet_nuts"){inlier <- 0.9}
    cv_gold <- "cv_dr"
    if(risk=="diet_salt"){cv_gold <- "cv_cv_urinary_sodium"} 
  
#Network or not
  if ( num_pairs > 1){
    message(paste0(risk, " requires a network analysis"))   
  }else{
    message(paste0(risk, " Not doing a network here"))
  }
  
# Data Statement
  message(paste0(risk, " includes ", comp_n, " different comparison combinations between ref-alt pairs and includes data from ", area_n, " different areas/countries"))
  message(paste0(risk, " crosswalk will include ", inlier, " proportion of the data in the models"))
  
   table(data$comp)
   table(data$super_region_name)
  
#finalize data prep for crosswalk model
  df_matched <- data
  setnames(df_matched, c("ydiff_log_mean","ydiff_log_se", "alternate", "reference", "id"), c("log_diff","log_diff_se", "altvar", "refvar", "group_id"))
  df_matched$male <- ifelse(!is.na(df_matched$sex_id) & df_matched$sex_id == 1, 1, 0)
  df_matched$female <- ifelse(!is.na(df_matched$sex_id) & df_matched$sex_id == 2, 1, 0)
  
  summary(df_matched$log_diff)
 
    
  if (save){ write.csv(df_matched, paste0(save_dir, "cw_inputs/", risk, "_matched_data.csv"), row.names=FALSE )
  message(paste0("saved matched data to: ", save_dir, "cw_inputs/", risk, "_matched_data.csv"))
}else{
  message(paste0("Matched data for ", risk, " NOT saved"))
}
  
  }
  
  
## 1b. Prepare data for applying adjustment---------------------#
 
  if(do_data_prep){
    risk_data <- predict_data_prep(risk, save)
    
    summary_data_type <- risk_data[, .(
      mean = mean(mean, na.rm = TRUE),
      min = min(mean, na.rm = TRUE),
      max = max(mean, na.rm = TRUE),
      med = median(mean, na.rm = TRUE)
    ), by = .(male, type)]

  }

  
#-----------------------------------------------------------------------------------#
# Step 2: Run Crosswalks 
#-----------------------------------------------------------------------------------#

if(do_xwalk){
  
  #### CROSSWALKS -----------------------------------------------------#
  
  # Step 1. Global model all-sex to produce priors for SR-specific models
  
  if (global == FALSE){
  
    
  cw_priors <- cw_for_sr_priors(df_matched, cv_gold, inlier)
  
  if("cv_FFQ" %in% cw_priors$dorms){
    prior_ffq <- cw_priors[dorms == "cv_FFQ", .(beta)]$beta
    prior_sd_ffq <- cw_priors[dorms == "cv_FFQ", .(beta_sd)]$beta_sd
  }
  
  if("cv_fao" %in% cw_priors$dorms){
    prior_fao <- cw_priors[dorms == "cv_fao", .(beta)]$beta
    prior_sd_fao <- cw_priors[dorms == "cv_fao", .(beta_sd)]$beta_sd
  }
  
  if("cv_sales" %in% cw_priors$dorms){
    prior_sales <- cw_priors[dorms == "cv_sales", .(beta)]$beta
    prior_sd_sales <- cw_priors[dorms == "cv_sales", .(beta_sd)]$beta_sd
  }
  
  if("cv_cv_hhbs" %in% cw_priors$dorms){
    prior_hhbs <- cw_priors[dorms == "cv_cv_hhbs", .(beta)]$beta
    prior_sd_hhbs <- cw_priors[dorms == "cv_cv_hhbs", .(beta_sd)]$beta_sd
  }
  
  if(risk=="diet_salt" & "cv_dr" %in% cw_priors$dorms){
    prior_dr <- cw_priors[dorms == "cv_dr", .(beta)]$beta
    prior_sd_dr <- cw_priors[dorms == "cv_dr", .(beta_sd)]$beta_sd
  }
  
  }

   
## STEP 2a: GLOBAL CROSSWALK WITH SEX COV  -----------------------------------## 
   
if(risk=="diet_omega_3" & release ==16) {
  df_matched <- fread("FILEPATH/diet_omega_3_matched_data_no_US.csv")
}
   
data_covs <-  list("male")
prior_beta_gaussian_list= NULL
 
  global_crosswalk <- fit_and_adjust_global_cw(
    df_matched=df_matched, 
    data_covs = list("male"), 
    cv_gold, 
    inlier, 
    save_dir, 
    risk, 
    risk_data, 
    prior_beta_gaussian_list= NULL)
  
  fit_glob <- global_crosswalk$fit1
  cw_coefficients <- global_crosswalk$dt_coeffs
  risk_data_global <- global_crosswalk$dt_adjusted_data

  if(save){
    write.csv(cw_coefficients, paste0(save_dir, "coefficients/", risk, "_cw_coeffs_global_model.csv"), row.names=FALSE )  
    message(paste0("Crosswalk beta coefficients from global model have been prepped for ", risk, " and saved to: ", save_dir, "coefficients/", risk, "_cw_coeffs_global_model.csv"))
  }else{
    message(paste0("Crosswalk beta coefficients from global model have been prepped for ", risk, " and NOT saved"))
  }

  
##Step 2b - SR-specific crosswalk -----------------------------------## 
  
  if (global == FALSE){
  
 #Super Region specific models with global model priors ---------#
  super_region_ids <- unique(df_matched$super_region_id)
  theta_values <- c(2)
  
  if(risk=="diet_fruit"){
    df_matched[, flag := ifelse(comp == "cv_dr-cv_fao" & super_region_id == 137, 1, 0)] 
    df_matched <- df_matched[flag !=1]
    df_matched[, flag := ifelse(comp == "cv_dr-cv_sales" & super_region_id == 137, 1, 0)] 
    df_matched <- df_matched[flag !=1]
  }

  #Super Regions with gold standard data (gold_std)
  if(risk!="diet_salt"){
  sr_ids_with_gold_std <- c()
    for (region_id in super_region_ids) {
      if ("cv_dr" %in% df_matched[df_matched$super_region_id == region_id,]$refvar) {
        sr_ids_with_gold_std <- c(sr_ids_with_gold_std, region_id)
      }
    }
        if(risk == "diet_ssb"){sr_ids_with_gold_std <- 64
        message(paste0("SR-specific models were thrown for ", risk, ". Revisit with new data."))}
        
        if(risk == "diet_pufa"){sr_ids_with_gold_std <- 64
        message(paste0("SR-specific models were thrown for ", risk, ". Revisit with new data."))} 
  
        if(risk == "diet_milk"){sr_ids_with_gold_std <- c(31, 64, 103)
        message(paste0("SR-specific models were thrown for ", risk, ". Revisit with new data."))}
  
        if(risk == "diet_veg"){sr_ids_with_gold_std <- c(4, 64, 103, 166)
        message(paste0("SR-specific models were thrown for ", risk, ". Revisit with new data."))}
  
        if(risk == "diet_redmeat"){sr_ids_with_gold_std <- c(64)
        message(paste0("SR-specific models were thrown for ", risk, ". Revisit with new data."))}
  
        if(risk == "diet_nuts"){sr_ids_with_gold_std <- c(64)
        message(paste0("SR-specific models were thrown for ", risk, ". Revisit with new data."))}
  
    all_srs <- c(4, 31, 64, 103, 137, 158, 166)
  sr_ids_no_gold_std <- setdiff(all_srs, sr_ids_with_gold_std)
  }
  

  #Sodium specific 
  if(risk=="diet_salt"){
    sr_ids_with_gold_std <- c()
    for (region_id in super_region_ids) {
      if ("cv_cv_urinary_sodium" %in% df_matched[df_matched$super_region_id == region_id,]$refvar) {
        sr_ids_with_gold_std <- c(sr_ids_with_gold_std, region_id)
        sr_ids_with_gold_std <- c(4, 64) 
      }
    }
    all_srs <- c(4, 31, 64, 103, 137, 158, 166)
    sr_ids_no_gold_std <- setdiff(all_srs, sr_ids_with_gold_std)
  }

  
  #initiate lists for result storage
  list_df_tmp_sr <- list()
  list_risk_data_sr <- list()
 
  
  for(i in seq_along(super_region_ids)) { # Loop through each sr
      sr_id <- super_region_ids[i] 

      if (sr_id %in% sr_ids_with_gold_std) {
        
        message(paste0(risk, " - SR = ", sr_id, " : has gold std data. SR-specific cw proceeds"))
        
        #subset data for SR
        df_matched_sr <- df_matched[super_region_id %in% sr_id]
        risk_data_sr <- risk_data[super_region_id %in% sr_id] 
        
       
        #risk-sr pair censoring based on lack of data
        if(risk=="diet_salt" & sr_id ==64){
          risk_data_sr[, flag := ifelse(cv_dr == 1 & super_region_id == 64, 1, 0)] 
          risk_data_sr <- risk_data_sr[flag !=1]
        }
        if(risk=="diet_redmeat" & sr_id ==64){
          risk_data_sr[, flag := ifelse(cv_FFQ == 1 & super_region_id == 64, 1, 0)] 
          risk_data_sr <- risk_data_sr[flag !=1]
        }

        ##loop over values of interest for theta
        for (theta in theta_values) {
          
            prior_beta_gaussian_list <- list()
            
            # Add priors conditionally based on the presence of non-zero values
            if (has_nonzero_values("cv_fao", df_matched_sr)) {
              fao_theta <- prior_sd_fao*theta
              prior_beta_gaussian_list$cv_fao <- c(prior_fao, fao_theta)
            }
            
            if (has_nonzero_values("cv_FFQ", df_matched_sr)) {
              ffq_theta <- prior_sd_ffq*theta
              prior_beta_gaussian_list$cv_FFQ <- c(prior_ffq, ffq_theta)
            }
            
            if (has_nonzero_values("cv_hhbs", df_matched_sr)) {
              hhbs_theta <- prior_sd_hhbs*theta
              prior_beta_gaussian_list$cv_cv_hhbs <- c(prior_hhbs, hhbs_theta)
            }
            
            if (has_nonzero_values("cv_sales", df_matched_sr)) {
              sales_theta <- prior_sd_sales*theta
              prior_beta_gaussian_list$cv_sales <- c(prior_sales, sales_theta)
            }
            
            if (risk=="diet_salt" & has_nonzero_values("cv_dr", df_matched_sr)) {
              dr_theta <- prior_sd_dr*theta
              prior_beta_gaussian_list$cv_dr <- c(prior_dr, dr_theta)
            }
            
            result <- fit_and_adjust_sr_model(
              df_matched_sr = df_matched_sr,
              risk_data_sr = risk_data_sr,
              prior_beta_gaussian_list = prior_beta_gaussian_list,
              risk = risk,
              sr_id = sr_id,
              theta = theta,
              cv_gold = cv_gold,
              inlier = inlier
            )
            
            df_tmp_sr <- result$df_tmp_sr
            risk_data_sr <- result$risk_data_sr
            
            list_df_tmp_sr[[paste0("df_tmp_sr_", sr_id, "_theta_", theta)]] <- df_tmp_sr
            list_risk_data_sr[[paste0("risk_data_sr_", sr_id, "_theta_", theta)]] <- risk_data_sr
            
        }
                   }else{
                     message(paste0(risk, " - SR ", sr_id, " : does NOT have gold std data. Global cw should be applied"))
                     }
  }
      
  df_cw_betas <- do.call(rbind, list_df_tmp_sr)
  risk_data_sr_combo <- do.call(rbind.fill, list_risk_data_sr)

  table(df_cw_betas$super_region_id)
  table(df_cw_betas$dorms)
  
  if(save){
    write.csv(df_cw_betas, paste0(save_dir, "coefficients/", risk, "_cw_coeffs_sr_model.csv"), row.names=FALSE)  
    message(paste0("Crosswalk beta coefficients from SR-specific models have been prepped for ", risk, " and saved to: ", save_dir, "coefficients/", risk, "_cw_coeffs_sr_model.csv"))
  }else{
    message(paste0("Crosswalk beta coefficients from SR-specific models  have been prepped for ", risk, " and NOT saved"))
  }
  
  }
  
  }
  

  #Step 3. Merge crosswalked data files - global model and sr-specific models with various thetas-----------------------------------## 
if(adjdata){
  if(global==FALSE){
    
    risk_data_sr_combo <- setDT(risk_data_sr_combo)
    
    if(risk == "diet_fiber"){
      risk_data_sr_combo <- risk_data_sr_combo[super_region_id != 158]
      message(paste0("SR-specific model for South Asia was removed for ", risk))
    }
    
    risk_data_sr_short <- risk_data_sr_combo[, .(nid, location_id, year_id, sex_id, age_group_id, super_region_id, mean, standard_error, meanvar_adjusted, sdvar_adjusted, pred_logit, pred_se_logit, data_id, theta)]
    
    setnames(risk_data_sr_short, c("meanvar_adjusted", "sdvar_adjusted", "pred_logit", "pred_se_logit", "data_id"), 
             c("meanvar_adjusted_sr", "sdvar_adjusted_sr", "pred_logit_sr", "pred_se_logit_sr", "data_id_sr")) 
    
    risk_data_all_model <- merge(risk_data_global, risk_data_sr_short, by = c('location_id', 'super_region_id', 'year_id', 'sex_id', 'age_group_id', 'nid', 'mean', 'standard_error'), all.x = TRUE) 
    
    risk_data_all_model[, cw_model := ifelse(!is.na(meanvar_adjusted_sr), "sr", "global")]
    
  }
  
  if(global==TRUE){
    risk_data_all_model <- risk_data_global
    risk_data_all_model$cw_model <- "global"
  }
  
  #Add variables & location data  
  risk_data_all_model$age_midpt <- (risk_data_all_model$age_end + risk_data_all_model$age_start)/2
  risk_data_all_model$invert_variance <- 1/ risk_data_all_model$variance
  
  risk_data_all_model$meanvar_adjusted_low <- risk_data_all_model$meanvar_adjusted - (risk_data_all_model$sdvar_adjusted * qnorm(0.975))
  risk_data_all_model$meanvar_adjusted_high <- risk_data_all_model$meanvar_adjusted + (risk_data_all_model$sdvar_adjusted * qnorm(0.975))
  
  if(global==FALSE){
    risk_data_all_model$meanvar_adjusted_sr_low <- risk_data_all_model$meanvar_adjusted_sr - (risk_data_all_model$sdvar_adjusted_sr * qnorm(0.975))
    risk_data_all_model$meanvar_adjusted_sr_high <- risk_data_all_model$meanvar_adjusted_sr + (risk_data_all_model$sdvar_adjusted_sr * qnorm(0.975))
    
    risk_data_all_model$male <- as.factor(risk_data_all_model$male)
    
  }

  #Generate summary tables of adjustment factors for sr and global models 

  values_glob   <- risk_data_all_model[, .(
    pred_logit = mean(pred_logit, na.rm = TRUE),
    pred_se = mean(pred_se_logit, na.rm = TRUE),
    mean_unadj = mean(mean, na.rm = TRUE),
    mean_adj = mean(meanvar_adjusted, na.rm = TRUE),
    se_unadj = mean(standard_error, na.rm = TRUE),
    se_adj = mean(sdvar_adjusted, na.rm = TRUE),
    n = .N),
    by= .(type, male)]
  setDT(values_glob)
  values_glob$super_region_name <- "GLOBAL"
  
  values   <- risk_data_all_model[, .(
    pred_logit = mean(pred_logit, na.rm = TRUE),
    pred_se = mean(pred_se_logit, na.rm = TRUE),
    mean_unadj = mean(mean, na.rm = TRUE),
    mean_adj = mean(meanvar_adjusted, na.rm = TRUE),
    se_unadj = mean(standard_error, na.rm = TRUE),
    se_adj = mean(sdvar_adjusted, na.rm = TRUE),
    n = .N),
    by= .(type, male, super_region_name)]
  setDT(values)
  
  values_all <- rbind(values_glob, values)
  
  if(save){ 
    write.csv(values_all, paste0(save_dir, "coefficients/", risk, "_adjustment_factor_summary.csv"), row.names=FALSE)
    message(paste0(risk, " adjustment factor summary has been saved here: ", save_dir, "coefficients/", risk, "_adjustment_factor_summary.csv"))
  }
}

###--- To prep final adjustments & bundle format-----------------#
if(adjdata){

  dt <- risk_data_all_model
  setDT(dt)

  setnames( dt, c("seq", "mean", "variance", "lower", "upper"), c("origin_seq", "orig_data", "orig_variance", "orig_data_lower", "orig_data_upper"))
  
if(global==FALSE){  
    dt[, val := ifelse(!is.na(meanvar_adjusted_sr), meanvar_adjusted_sr, meanvar_adjusted)]
    
    dt[, sdvar := ifelse(!is.na(sdvar_adjusted_sr), sdvar_adjusted_sr, sdvar_adjusted)]
    
    dt[, variance := ifelse(!is.na(sdvar), (sdvar)^2, NULL)]
    
    dt[, lower := ifelse(!is.na(meanvar_adjusted_sr), meanvar_adjusted_sr_low, meanvar_adjusted_low)]
    dt[, upper := ifelse(!is.na(meanvar_adjusted_sr), meanvar_adjusted_sr_high, meanvar_adjusted_high)]
    
    dt[, pred_logit_final := ifelse(!is.na(pred_logit_sr), pred_logit_sr, pred_logit)]
    dt[, pred_se_logit_sr_final := ifelse(!is.na(pred_se_logit_sr), pred_se_logit_sr, pred_se_logit)]
    
    setnames(dt, c( "pred_logit_final", "pred_se_logit_sr_final"),
             c("pred_logit", "pred_se_logit"))
    
    cols_to_delete <- c("meanvar_adjusted_sr", "meanvar_adjusted", "meanvar_adjusted_sr_low", "meanvar_adjusted_low", "sdvar_adjusted_sr", "sdvar_adjusted",
                        "meanvar_adjusted_sr_high", "meanvar_adjusted_high", "pred_logit_sr", "pred_logit", "pred_se_logit_sr", "pred_se_logit", "invert_variance", 
                        "sdvar", "map_id", "path_to_top_parent", "super_region_name")
  
}
  
  if(global==TRUE){
    setnames(dt, c("meanvar_adjusted", "sdvar_adjusted", "meanvar_adjusted_low", "meanvar_adjusted_high"), 
             c("val", "sdvar", "lower", "upper"))
    
    dt[, variance := ifelse(!is.na(sdvar), (sdvar)^2, NULL)]
    
    cols_to_delete <- c( "invert_variance", "sdvar", "map_id", "path_to_top_parent", "super_region_name")
    
  }
  
  dt[, (cols_to_delete) := NULL]
  
  
  if (risk == "diet_transfat"){    
    dt[, year_start := ifelse(is.na(year_start), year_id, year_start)]
    dt[, year_end := ifelse(is.na(year_end), year_id, year_end)]
    
    dt[, measure := ifelse(is.na(measure), 'continuous', measure)]
    
    dt[, is_outlier := ifelse(is.na(is_outlier), 0, is_outlier)]
    
    dt[, ihme_risk := ifelse(is.na(ihme_risk), 'diet_transfat', ihme_risk)]
    
    dt[, cv_dr := ifelse(dt$cv_sales == 1, 0, cv_dr)]
    
    dt[, cv_FFQ := ifelse(dt$cv_sales == 1, 0, cv_FFQ)]
  }
   
  dt <- dt[, crosswalk_parent_seq := ifelse(was_both_sex == 0 & was_age_split == 0, NA, origin_seq)]
  dt <- dt[, seq := ifelse(was_both_sex == 0 & was_age_split == 0, origin_seq, NA)]

  
if(risk =="diet_salt" | risk =="diet_whole_grains" | risk =="diet_transfat"){
  dt[, source_type := ifelse(is.na(dt$source_type) | dt$source_type == "" , "Survey - other/unknown", dt$source_type)]
}

  setnames(dt, c("age_start", "age_end"), c("orig_age_start", "orig_age_end"))
 
  dt[, origin_seq := NULL]
  
  if(risk =="diet_procmeat"){
      dt[, measure_id := 19]
  }
  
dt <- dt[is_outlier ==0]
  
table(dt$ihme_risk) 
 
if(save){
  write.csv(dt, paste0(save_dir, "outputs/", risk, "_crosswalked_data_final.csv"), row.names=FALSE)  
  message(paste0("Adjusted data for ", risk, " has been prepped and saved to: ", save_dir, "outputs/", risk, "_crosswalked_data_final.csv"))
}else{
  message(paste0("Adjusted data for ", risk, " and NOT saved"))
}

}

###---File needed to save crosswalk version-------------------#
if(save){
write.xlsx(dt,  paste0(save_dir, "outputs/bundle_prep/", risk, "_crosswalked_data_final.xlsx"), sheetName = "extraction")
message(paste0(risk, " final crosswalked data has been saved here: ", save_dir, "outputs/bundle_prep/"))
message(paste0("crosswalk is complete for ", risk))
}





