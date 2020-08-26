#######################################################
## Name of script: expandAgeGroups.R
## Description: Expands the SEER survival data to all age groups 
##    needed for matching to GBD MIR draws.
## Arguments: Requires formatted SEER data in appropriate format
##    **User will need to check that age groups remain appropriate
##      at the start of each GBD cycle**
## Outputs: Age_group-specific SEER relative survival curve data in 
##    cancer-specific *.csv files.
## Author(s): NAME
## Last updated: 2018-10-04


## Load packages
library(data.table)
library(assertthat)
library(tidyr)
require(yaml)

if (!exists("code_repo"))  {
  code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
  if (!grepl("cancer_estimation", code_repo)) {
    code_repo <- file.path(code_repo, 'cancer_estimation')
  }
}
source(file.path(code_repo, '_database/cdb_utils.r'))
source(file.path(code_repo, 'r_utils/utilities.r'))
source(file.path("FILEPATH/get_age_metadata.R"))

## Set paths
pathToSEERcurve <- c("FILEPATH")


## Set list of cancers to loop through
cancerList <- c("neo_eye", "neo_lymphoma_burkitt", "neo_lymphoma_other", "neo_neuro", "neo_eye_rb", "neo_bone", "neo_tissue_sarcoma")
#c('neo_bone', 'neo_eye', 'neo_liver_hbl', 'neo_lymphoma_burkitt','neo_lymphoma_other','neo_neuro','neo_liver_hbl','neo_tissue_sarcoma') 
#c("neo_bladder", "neo_brain", "neo_breast", "neo_colorectal", "neo_esophageal", "neo_gallbladder",  
#                "neo_hodgkins", "neo_kidney", "neo_larynx", "neo_leukemia_ll_acute", "neo_leukemia_ll_chronic",
#                "neo_leukemia_ml_acute", "neo_leukemia_ml_chronic", "neo_leukemia_other", "neo_liver", "neo_lung",
#                "neo_lymphoma", "neo_melanoma", "neo_meso", "neo_mouth", "neo_myeloma", "neo_nasopharynx",
#                "neo_other_cancer", "neo_otherpharynx", "neo_pancreas", "neo_stomach", "neo_thyroid")
cancerListM <- c() #c("neo_prostate", "neo_testicular")
cancerListF <- c() #c("neo_cervical", "neo_ovarian", "neo_uterine")
restrictCases <- 25

## Define functions (check the plots to ensure that all possibilities are covered)
# function to replace young age groups in males
replaceMyoung <- function(x){     
  if(x > 1){                
    x_minus <- x - 1
    tempCopy <- tempM[age_group_id >= x]
    youngestCopy <- tempM[age_group_id == x]
    counter <- c(x_minus:1)       # want to replace age groups down through 1 with data from youngest age group with n>=25
  }
  if(x == 1){
    x_minus <- x - 1
    tempCopy <- tempM
    youngestCopy <- tempM[age_group_id == x]
    counter <- c(2:5)       # want to replace age groups 2, 3, 4, and 5 with data from 1
  }
  youngestCopyRename <- copy(youngestCopy)   
  for(i in counter){
    youngestCopyRenameCounter <- copy(youngestCopyRename[, age_group_id := i])           # want to replace age_group_id values with the counter values (eg 3)
    tempCopy <- rbind(tempCopy, youngestCopyRenameCounter) # want to append the rows with the values copied above
  }
  return(tempCopy)   #output a dataframe that contains all the added rows
}

# function to replace young age groups in Females
replaceFyoung <- function(x){     # feed in the youngest age group for Females, output copied young age groups
  if(x > 1){                # there is a special case when x == 1
    x_minus <- x - 1
    tempCopy <- tempF[age_group_id >= x]   # will be replacing all age_groups younger than the one with >= 25
    youngestCopy <- tempF[age_group_id == x]
    counter <- c(x_minus:1)       # want to replace age groups down through 1 with data from youngest age group with n>=25
  }
  if(x == 1){
    x_minus <- x - 1
    tempCopy <- tempF
    youngestCopy <- tempF[age_group_id == x]   # do not need to replace any age groups
    counter <- c(2:5)       # want to replace age groups 2, 3, 4, and 5 with data from 1
  }
  youngestCopyRename <- copy(youngestCopy)   # copy a version we can change of whichever was created above
  for(i in counter){
    youngestCopyRenameCounter <- copy(youngestCopyRename[, age_group_id := i])   # replace age_group_id values with the counter values (eg 3)
    tempCopy <- rbind(tempCopy, youngestCopyRenameCounter) # append the rows with the values copied above
  }
  return(tempCopy)   #output a dataframe that contains all the added rows
}

# function to replace old age groups
replaceOldAges <- function(x){
  tempCopy <- x
  print(unique(tempYoungest[,age_group_id]))
  tempCopy <- tempYoungest[age_group_id != 21]  # drop data for age group 80+
  assert_that(30 %in% tempYoungest$age_group_id %>% unique, msg = "missing age group 30 to copy for old age groups")
  oldestCopy <- tempYoungest[age_group_id == 30]  # copy of data for age group 85+
  oldestCopyRename <- oldestCopy
  counter <- c(31, 32, 235)      # will add age groups for 85-89, 90-94, and 95+
  for(i in counter){
    oldestCopyRenameCounter <- oldestCopyRename[, age_group_id := i]  # replace age_group_id values with i
    tempCopy <- rbind(tempCopy, oldestCopyRenameCounter)  # append the rows with values copied above
  }
  return(tempCopy)
}


### Run script

## loop across cancers affecting both sexes
for(cancer in cancerList){
  
  gbd_ages <- c(1, 6:20, 30:32, 235)
  inData <- fread(paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_formatted.txt"))
  
  if(nrow(inData[is.na(SEER_surv) & surv_year <= 10 & age_group_id %in% gbd_ages]) != 0){
    print(inData[is.na(SEER_surv) & surv_year <= 10 & age_group_id %in% gbd_ages,
                                list(sex_id, acause, age_group_id, surv_year, SEER_surv)] %>% unique)
    stop("Missing SEER surv in survival years < 10")
  }
  
  initial_ages <- inData[sex_id == 2]$age_group_id %>% unique %>% sort
  
  ## restrict further to only the years ranging from 2001 to 2010
  
  inData <- inData[year_range=="yrs_2001_2010"]
  assert_that(all(inData[sex_id == 2]$age_group_id %>% unique %>% sort == initial_ages), 
              msg = "Some ages dropped due to year_range criteria")
  
  if(cancer == "neo_neuro"){
    # for neuro replacing surv year 7-10 with surv year 6 for males, age group id 30 (80-84)
    replace_num <- inData[age_group_id == 30 & surv_year == 6 & sex_id == 1, SEER_surv]
    
    inData[surv_year %in% c(7:10) & age_group_id == 30  
           & sex_id == 1, SEER_surv := replace_num]
  }
  
  # checking if restrictCases is reasonable and keeps all age groups and sexes otherwise
  # uses the biggest cases that still gives us n>25
  
  ## split and restrict the data into sex-specific datasets with at least 25 cases
  tempM <- inData[n >= restrictCases & sex_id == 1,]
  tempF <- inData[n >= restrictCases & sex_id == 2,]
  
  # tests for checking ages
  assert_that(all(initial_ages %in% (tempM$age_group_id %>% unique %>% sort)), 
              msg = "Some ages dropped due to cases criteria in tempM")
  assert_that(all(initial_ages %in% (tempF$age_group_id %>% unique %>% sort)), 
              msg = "Some ages dropped due to cases criteria in tempF")
  
  ## determine the youngest age_group_id that has at least 25 cases
  youngestM <- min(tempM$age_group_id)
  youngestF <- min(tempF$age_group_id)
  
  ## get the dataset with the young M age_groups copied 
  tempYoungM <- replaceMyoung(youngestM)
  ## get the dataset with the young F age_groups copied
  tempYoungF <- replaceFyoung(youngestF)
  
  ## combine the Males and Females with the youngest age groups copied
  tempYoungest <- rbind(tempYoungM, tempYoungF)
  
  ## get the dataset with the oldest age_groups copied
  tempOldest <- replaceOldAges(tempYoungest)
  
  # check for missing ages (standard non fatal pipeline code)
  age_restrict <- FILEPATH.get_table("registry_input_entity")
  setDT(age_restrict)
  cur_rest <- age_restrict[acause == cancer & is_active == 1 & gbd_round_id == get_gbd_parameter("current_gbd_round")]
  
  ages <- get_age_metadata(age_group_set_id = 12)
  allowed_ages <- ages[age_group_years_start >= cur_rest$yll_age_start & age_group_years_start >=5 & 
                         age_group_years_end <= cur_rest$yll_age_end, age_group_id] %>% unique
  if(cur_rest$yll_age_end == 95) allowed_ages <- c(allowed_ages, 235)
  
  assert_that(allowed_ages %in% tempOldest[sex_id == 2]$age_group_id %>% unique %>% sort, 
              msg = paste0("Missing some ages in cleaned SEER dataset for sex_id = 2: ",
                           paste0(setdiff(allowed_ages, tempOldest$age_group_id %>% unique %>% sort), collapse = " ") ))
  
  assert_that(allowed_ages %in% tempOldest[sex_id == 1]$age_group_id %>% unique %>% sort, 
              msg = paste0("Missing some ages in cleaned SEER dataset for sex_id = 1: ",
                           paste0(setdiff(allowed_ages, tempOldest$age_group_id %>% unique %>% sort), collapse = " ") ))
  
  ## remove the 80+ age_group
  tempCopied <- tempOldest[age_group_id != 160]  

  
  #tmp <- tempCopied[age_group_id==1,]
  #tmp[,age_group_id := 6]
  #tempCopied <- rbind(tempCopied,tmp)
  #tmp[,age_group_id := 7]
  #tempCopied <- rbind(tempCopied,tmp)

  ## set list of columns to keep in output dataset
  colKeepList = c("acause", "sex_id", "year_range", "age_group_id", "surv_year", "SEER_surv")
  
  ## remove unnessary columns
  tempCleaned <- tempCopied[, colKeepList, with=FALSE]
  
  ## export cleaned and formatted data, for use in next step
  fwrite(tempCleaned, paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_cleaned.txt"))
}


## loop across Male cancers 


for(cancer in cancerListM){
  inData <- fread(paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_formatted.txt"))
  
  ## restrict further to only the years ranging from 2001 to 2010
  inData <- inData[year_range=="yrs_2001_2010"]

  ## split and restrict the data into sex-specific datasets with at least 25 cases
  tempM <- inData[n>=25 & sex_id==1,]

  ## determine the youngest age_group_id that has at least 25 cases
  youngestM <- min(tempM$age_group_id)

  ## get the dataset with the young M age_groups copied 
  tempYoungM <- replaceMyoung(youngestM)

  ## combine the Males and Females with the youngest age groups copied
  tempYoungest <- tempYoungM
  
  ## get the dataset with the oldest age_groups copied
  tempOldest <- replaceOldAges(tempYoungest)
  
  ## remove the 80+ age_group
  tempCopied <- tempOldest[age_group_id != 160]  # drop data for ages 80+
  
  ## set list of columns to keep in output dataset
  colKeepList = c("acause", "sex_id", "year_range", "age_group_id", "surv_year", "SEER_surv")
  
  ## remove unnessary columns
  tempCleaned <- tempCopied[, colKeepList, with=FALSE]
  
  ## export cleaned and formatted data, for use in next step
  fwrite(tempCleaned, paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_cleaned.txt"))
}


## loop across Female cancers


for(cancer in cancerListF){
  inData <- fread(paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_formatted.txt"))
  
  ## restrict further to only the years ranging from 2001 to 2010
  inData <- inData[year_range=="yrs_2001_2010"]
  
  ## split and restrict the data into sex-specific datasets with at least 25 cases
  tempF <- inData[n>=25 & sex_id==2,]
  
  ## determine the youngest age_group_id that has at least 25 cases
  youngestF <- min(tempF$age_group_id)
  
  ## get the dataset with the young F age_groups copied
  tempYoungF <- replaceFyoung(youngestF)
  
  ## combine the Males and Females with the youngest age groups copied
  tempYoungest <- tempYoungF
  
  ## get the dataset with the oldest age_groups copied
  tempOldest <- replaceOldAges(tempYoungest)
  
  ## remove the 80+ age_group
  tempCopied <- tempOldest[age_group_id != 160]  # drop data for ages 80+
  
  ## set list of columns to keep in output dataset
  colKeepList = c("acause", "sex_id", "year_range", "age_group_id", "surv_year", "SEER_surv")
  
  ## remove unnessary columns
  tempCleaned <- tempCopied[, colKeepList, with=FALSE]
  
  ## export cleaned and formatted data, for use in next step
  fwrite(tempCleaned, paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_cleaned.txt"))
}

###################################################
## exception handling

## need to add in age group 6 for testicular (m), CML (m,f)
cancerExceptions <- c() # c("neo_testicular", "neo_leukemia_ml_chronic")

for(cancer in cancerExceptions){
  inData <- fread(paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_cleaned.txt"))
  replaceData <- copy(inData[age_group_id==7])
  replaceData <- replaceData[, age_group_id := 6]
  outData <- rbind(inData, replaceData)
  fwrite(outData, paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_cleaned.txt"))
}



