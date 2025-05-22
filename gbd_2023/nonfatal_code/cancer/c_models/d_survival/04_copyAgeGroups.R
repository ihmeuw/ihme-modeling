####################################################################
## Name of script: 04_copyAgeGroups.R
## Description: Expands the SEER survival data to all age groups 
##    needed for matching to GBD MIR draws.
## Arguments: Requires formatted SEER data in appropriate format
##    **User will need to check that age groups remain appropriate
##      at the start of each GBD cycle**
## Outputs: Age_group-specific SEER relative survival curve data in 
##    cancer-specific *.csv files.
## Author(s): USERNAME
####################################################################

##########################
## Load packages         #
##########################
library(here)
library(data.table)
library(assertthat)
library(tidyr)
require(yaml)

##########################
## Load Functions         #
##########################
source(file.path('FILEPATH/cdb_utils.r'))
source(file.path('FILEPATH/utilities.r'))
source(file.path("FILEPATH/get_age_metadata.R"))

##########################
## Set Parameters        #
##########################
## Set list of cancers to loop through
cancerList <- c('neo_liver_hbl', "neo_bladder", "neo_brain", "neo_breast", "neo_colorectal", "neo_esophageal", "neo_gallbladder",  
                "neo_hodgkins", "neo_kidney", "neo_larynx", "neo_leukemia_ll_acute", "neo_leukemia_ll_chronic",
                "neo_leukemia_ml_acute", "neo_leukemia_ml_chronic", "neo_leukemia_other", "neo_liver", "neo_lung",
                "neo_lymphoma", "neo_melanoma", "neo_meso", "neo_mouth", "neo_myeloma", "neo_nasopharynx",
                "neo_other_cancer", "neo_otherpharynx", "neo_pancreas", "neo_stomach", "neo_thyroid",
                "neo_eye", "neo_lymphoma_burkitt", "neo_lymphoma_other", "neo_neuro", "neo_eye_rb", "neo_bone", "neo_tissue_sarcoma",
                'neo_liver_hbl','neo_eye_other')

newCauses <- c("neo_eye", "neo_lymphoma_burkitt", "neo_lymphoma_other", "neo_neuro", 
               "neo_eye_rb", "neo_bone", "neo_tissue_sarcoma", "neo_eye_other", "neo_liver_hbl")

cancerListM <- c("neo_prostate", "neo_testicular")
cancerListF <- c("neo_cervical", "neo_ovarian", "neo_uterine")

# current default for minimum cases to restrict
restrictCases <- 25 

# causes to replace SEER survival values in
replaceValueCauses <- c('neo_gallbladder', 'neo_neuro', 'neo_otherpharynx', 'neo_nasopharynx')

##########################
## Helper Functions      #
##########################
## Define functions (check the plots to ensure that all possibilities are covered)
# function to replace young age groups in males
replaceMyoung <- function(x){ 
  # feed in the youngest age group for Males, output copied young age groups
  if(x > 1){                # there is a special case when x == 1
    x_minus <- x - 1
    tempCopy <- tempM[age_group_id >= x]
    youngestCopy <- tempM[age_group_id == x]
    counter1 <- c(get_gbd_parameter("young_ages_new")$young_ages_new) # want to replace age groups down through 1 with data from youngest age group with n>=25
    counter2 <- c(x_minus:1) 
    counter <- unique(c(counter1, counter2))
  }
  if(x == 1){
    x_minus <- x - 1
    tempCopy <- tempM
    youngestCopy <- tempM[age_group_id == x]
    counter1 <- get_gbd_parameter("young_ages_new")$young_ages_new # want to replace young ages with data from 1
    counter2 <- c(2:5)
    counter <- unique(c(counter1, counter2))
  }
  youngestCopyRename <- copy(youngestCopy)   # want a version we can change of whichever was created above
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
    counter1 <- c(get_gbd_parameter("young_ages_new")$young_ages_new)       # want to replace age groups down through 1 with data from youngest age group with n>=25
    counter2 <- c(x_minus:1)
    counter <- unique(c(counter1,counter2))
  }
  if(x == 1){
    x_minus <- x - 1
    tempCopy <- tempF
    youngestCopy <- tempF[age_group_id == x]   # do not need to replace any age groups
    counter1 <- get_gbd_parameter("young_ages_new")$young_ages_new       # want to replace age groups 2, 3, 4, and 5 with data from 1
    counter2 <- c(2:5)
    counter <- unique(c(counter1,counter2))
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
  tempCopy <- tempYoungest[age_group_id != 21]  # drop data for ages 80+
  assert_that(30 %in% tempYoungest$age_group_id %>% unique, msg = "missing age group 30 to copy for old age groups")
  oldestCopy <- tempYoungest[age_group_id == 30]  # copy of data for 85+
  oldestCopyRename <- oldestCopy
  counter <- c(31, 32, 235)      # will add age groups for 85-89, 90-94, and 95+
  for(i in counter){
    oldestCopyRenameCounter <- oldestCopyRename[, age_group_id := i]  # replace age_group_id values with i
    tempCopy <- rbind(tempCopy, oldestCopyRenameCounter)  # append the rows with values copied above
  }
  return(tempCopy)
}


get_allowed_ages <- function(cancer, sex_id = 3){
  # Function gets the allowed age group ids for that sex and cause 
  # Args: cancer - str, the cancer to get age restricted
  #                 age_group_ids for 
  #       sex_id - int, for sex specific cancers
  #
  # Returns: vector of age_group_ids for specific cancer
  #
  age_restrict <- cdb.get_table("registry_input_entity")
  setDT(age_restrict)
  cur_rest <- age_restrict[acause == cancer & is_active == 1 & 
                           gbd_round_id == get_gbd_parameter("current_gbd_round") & 
                           decomp_step == 3 & refresh == 2]
  assert_that(nrow(cur_rest) == 1, 
              msg = paste0("ERROR: More than one unique row of age restrictions for ", cancer))
  ages <- get_age_metadata(age_group_set_id = 19)

  # only return ages if it's the correct sex
  if((sex_id == 1 & cur_rest$male == 1) | 
     (sex_id == 2 & cur_rest$female == 1) | 
     (cur_rest$male == 1 & cur_rest$female == 1)){
    allowed_ages <- ages[age_group_years_start >= cur_rest$yld_age_start &
                         age_group_years_start <= cur_rest$yld_age_end, age_group_id] %>% unique
    assert_that(length(allowed_ages) > 0, 
               msg = paste0("ERROR: There are no restricted age_group_ids for ", cancer))
  } else{
    allowed_ages <- c()
  }
  return(allowed_ages)
}


detect_missing <- function(inData, cancer, allowed_ages){
  # Function gets missing/0 SEER survival values
  #
  # Args: inData - data.table, input data
  #       cancer - str, the cancer to get age restricted
  #                 age_group_ids for 
  #       allowd_ages - int, for sex specific cancers
  #
  # Returns: subset of data.table with missing or 0 SEER values
  flagged <- inData[age_group_id %in% allowed_ages & 
                    surv_year <= 10 & (is.na(SEER_surv) | SEER_surv < 0.0000001)]
  return(flagged)
}


replace_missing <- function(inData, cancer, allowed_ages){
  # Args: inData - data.table, our input data to be case restricted
  #       cancer - str, our cause to replace values in
  #       allowed_ages - vector, of age restricted age_group_ids that 
  #                       the cancer should have
  #
  # Returns: data.table of input data with replaced missing values

  # copy nearest surv year down
  if(cancer %in% replaceValueCauses){
    missing_age_sex <- detect_missing(inData, cancer,
                                      allowed_ages)[, list(age_group_id, 
                                                           sex_id)] %>% unique
    replaced <- inData %>% copy
    
    #replace val by age sex pair
    for(i in 1:nrow(missing_age_sex)){
      cur_row <- missing_age_sex[i, ]
      
      if(cancer == "neo_gallbladder" & cur_row$age_group_id == 9 & cur_row$sex_id == 1){
        # GBD2020 step 3: special case scenario for gallbladder where SEER survival is 0
        # for age_group_id = 9 and sex_id = 1
        # replace with age_group_id = 10 and sex_id = 1's survival values
        
        replace_val <- inData[age_group_id == 10 & sex_id == 1, 
                              list(age_group_id, SEER_surv, surv_year, sex_id, acause)]
        replace_val[surv_year > 5, SEER_surv := replace_val[surv_year == 5]$SEER_surv]
        replace_val[, age_group_id := 9]
        setnames(replace_val, "SEER_surv", "SEER_surv_to_replace")
        replaced <- merge(replaced, replace_val, 
                          by = c("sex_id", "acause", "surv_year", "age_group_id"), 
                          all.x = T)
        replaced[!is.na(SEER_surv_to_replace), SEER_surv := SEER_surv_to_replace]
        replaced$SEER_surv_to_replace <- NULL
        assert_that(nrow(replaced[age_group_id == 9 & 
                                  sex_id == 1 & 
                                  (is.na(SEER_surv) | SEER_surv < 0.000001)]) == 0,
                    msg = paste0("There are still NA and/or 0s for SEER survival for \n", 
                                 "neo_gallbladder for sex_id 1 and age_group_id 10"))
      
      } else{ # replace data with the nearest age group and survival values
        replace_val <- inData[!is.na(SEER_surv) & SEER_surv > 0.00000001 &
                                age_group_id == cur_row$age_group_id & 
                                sex == cur_row$sex]
        replace_val <- replace_val[surv_year == max(replace_val$surv_year)]$SEER_surv
        replaced[(is.na(SEER_surv)|SEER_surv < 0.00000001) & 
                   age_group_id == cur_row$age_group_id & 
                   sex == cur_row$sex, SEER_surv := replace_val]
      }
      print(paste0("Done replacing for age: ", cur_row$age_group_id, 
                   ", sex:", cur_row$sex_id))
    } 
    assert_that(nrow(replaced) == nrow(inData), 
                msg = "Rows of replaced dataset doesn't match input!")
    
    # make sure replacing worked
    assert_that(nrow(replaced[(is.na(SEER_surv)|SEER_surv < 0.00000001) & 
                                age_group_id %in% allowed_ages & 
                                surv_year <=10]) == 0,
                msg = "This cancer still has 0 or NA SEER survival values")
    return(replaced)
  } else{
    return(inData)
  }
}


restrict_cases <- function(inData, allowed_ages, restrictCases = 25, sex_id = 3){
  # 
  # Args: inData - data.table, our input data to be case restricted
  #       allowed_ages - vector, of age restricted age_group_ids that 
  #                       the cancer should have
  #       restrictCases - int, of the default number of min cases 
  #                       to retain in the dataset
  #       sex_id - int, specified for sex specific cancers otherwise, 
  #                       defaults to both (3)
  #
  # Returns: data.table of case restricted data
  #
  
  # GBD2020 step 3 causes that needed restrictCases lowered to max n in 
  # each sex, age, and surv_year are: 
  #           neo_leukemia_ml_chronic, neo_gallbladder, neo_larynx
  #           neo_prostate, neo_myeloma
  prev_miss <- inData[!is.na(SEER_surv)]
  restrict_df <- inData[age_group_id %in% allowed_ages, .(maxN = max(n)), 
                          by = c("sex", "age_group_id", "acause", "surv_year")]
  cur_cancer <- restrict_df$acause %>% unique
  outData <- merge(inData, restrict_df, 
                  by = c("sex", "age_group_id", "acause", "surv_year"), 
                  all.x = T)
  
  outData[maxN > restrictCases | is.na(maxN), maxN := restrictCases]
  outData <- outData[n >= maxN]
  outData$maxN <- NULL

  # check for each sex, for sex specific cancers, only check one
  for(cur_sex in c(1, 2)){
    if (cur_sex == sex_id | sex_id == 3){
      check_dt_ages <- outData[!is.na(SEER_surv) & surv_year <= 10 
                         & age_group_id %in% allowed_ages
                         & sex == cur_sex]$age_group_id %>% unique
      
      # check that we have surv data for allowed ages
      # check for previous missing seer survival ages prior to restricting 
      prev_miss_ages <- prev_miss[sex_id == cur_sex]$age_group_id %>% unique
      prev_miss_ages <- setdiff(allowed_ages, prev_miss_ages)
      
      any_miss <- setdiff(setdiff(allowed_ages, check_dt_ages), prev_miss_ages)
      
      assert_that(length(any_miss) == 0,
                  msg = paste0(" Missing SEER surv in survival years < 10 for sex_id = ", 
                               cur_sex, ", \nfor cause: ", cur_cancer, " for ages: ",
                               paste0(any_miss, collapse = ",")))
    }
  }
  print(paste0("Finished restricting cases for ", cancer))
  return(outData)
}

##########################
## Run Everything        #
##########################
### Run script
## loop across cancers affecting both sexes
for(cancer in cancerList){
  print(cancer)
  # handling for new causes
  if(cancer %in% newCauses){
    pathToSEERcurve <- FILEPATH
  } else{
    pathToSEERcurve <- FILEPATH
  }
  
  # read in formatted SEER data
  gbd_ages <- c(1, 6:20, 30:32, 235)
  inData <- fread(paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_formatted.txt"))
  
  initial_ages <- inData[sex_id == 2]$age_group_id %>% unique %>% sort
  
  ## restrict further to only the years ranging from 2001 to 2010
  inData <- inData[year_range=="yrs_2001_2010"]
  
  if(!all(inData[sex_id == 2]$age_group_id %>% unique %>% sort == initial_ages)){
    warning("Some ages dropped due to year_range criteria")
  }
  
  # checking if restrictCases is reasonable and keeps all age groups and sexes otherwise
  # uses the biggest cases by age, cause, sex and surv_year
  allowed_ages <- get_allowed_ages(cancer)
  inData <- replace_missing(inData, cancer, allowed_ages)
  inData <- restrict_cases(inData, allowed_ages, restrictCases)
  
  # check to make sure there are no NA SEER surv within age restrictions
  if(nrow(inData[is.na(SEER_surv) & surv_year <= 10 & age_group_id %in% allowed_ages]) != 0){
    warning(paste0("Missing SEER surv in survival years < 10 within age restrictions for", cancer))
  }
  
  ## split and restrict the data into sex-specific datasets
  tempM <- inData[sex_id == 1]
  tempF <- inData[sex_id == 2]
  
  ## determine the youngest age_group_id that has at least 25 cases
  youngestM <- min(tempM$age_group_id)
  youngestF <- min(tempF$age_group_id)
  
  ## get the dataset with the young M age_groups copied 
  tempYoungM <- replaceMyoung(youngestM)
  ## get the dataset with the young F age_groups copied
  tempYoungF <- replaceFyoung(youngestF)
  
  ## combine the Males and Females with the youngest age groups copied
  tempYoungest <- rbind(tempYoungM, tempYoungF)
  
  print(paste0('allowed ages...', allowed_ages))
  
  age_restrict <- cdb.get_table("registry_input_entity")
  setDT(age_restrict)
  cur_rest <- age_restrict[acause == cancer & is_active == 1 & 
                             gbd_round_id == get_gbd_parameter("current_gbd_round") & 
                             decomp_step == 3 & refresh == 2]
  if(cur_rest$yld_age_end == 95) allowed_ages <- c(allowed_ages, 235) %>% unique
  
  ## get the dataset with the oldest age_groups copied
  if(cur_rest$yld_age_end >= 80) tempOldest <- replaceOldAges(tempYoungest)
  else tempOldest <- copy(tempYoungest)

  ## remove the 80+ age_group
  tempCopied <- tempOldest[age_group_id != 160] 

  tmp <- tempCopied[age_group_id==1,]
  #tmp[,age_group_id := 6]
  #tempCopied <- rbind(tempCopied,tmp)
  #tmp[,age_group_id := 7]
  #tempCopied <- rbind(tempCopied,tmp)

  ## set list of columns to keep in output dataset
  colKeepList = c("acause", "sex_id", "year_range", "age_group_id", "surv_year", "SEER_surv")
  
  ## remove unnessary columns
  tempCleaned <- tempCopied[, colKeepList, with=FALSE]
  
  # one more check on ages present
  for(cur_sex in c(1,2)){
    check_ages <- tempCleaned[!is.na(SEER_surv)]$age_group_id %>% unique()
    assert_that(length(setdiff(allowed_ages, check_ages)) == 0,
                msg = paste0("Final cleaned dataset has NA SEER_surv for ", 
                             cancer, " for sex = ", cur_sex, "ages: ",
                             setdiff(allowed_ages, check_ages)))
                
  }
  
  # subset on restricted ages
  tempCleaned <- tempCleaned[age_group_id %in% allowed_ages]
  
  assert_that(nrow(tempCleaned) > 0, 
              msg = paste0("No rows of data for ", cancer))
  
  # check for no duplicates
  assert_that(nrow(tempCleaned[duplicated(tempCleaned[, list(surv_year, age_group_id, sex_id, acause)])]) == 0, 
              msg = paste0("There are duplicates present for ", cancer, " by surv_year, age_group_id, sex_id, acause"))
  ## export cleaned and formatted data, for use in next step
  fwrite(tempCleaned, paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_cleaned_gbd2020.txt"))
  print(paste0("Finished outputting SEER surv 10 years for ", cancer))
}


## loop across female - male specific cancers
for(cancer in c(cancerListF, cancerListM)){
  pathToSEERcurve <- FILEPATH
  restrictCases <- 25
  inData <- fread(paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_formatted.txt"))
  
  ## restrict further to only the years ranging from 2001 to 2010
  inData <- inData[year_range=="yrs_2001_2010"]
  
  ## split and restrict the data into sex-specific datasets
  if (cancer %in% cancerListF){
    allowed_ages <- get_allowed_ages(cancer, sex_id = 2)
    tempF <- restrict_cases(inData[sex_id==2], allowed_ages, restrictCases, sex_id = 2)
    youngestF <- min(tempF$age_group_id)
    tempYoungF <- replaceFyoung(youngestF)
    tempYoungest <- tempYoungF
  } else if (cancer %in% cancerListM){
    allowed_ages <- get_allowed_ages(cancer, sex_id = 1)
    tempM <- restrict_cases(inData[sex_id == 1], allowed_ages, restrictCases, sex_id = 1)
    youngestM <- min(tempM$age_group_id)
    tempYoungM <- replaceMyoung(youngestM)
    tempYoungest <- tempYoungM
  }
  ## get the dataset with the oldest age_groups copied
  tempOldest <- replaceOldAges(tempYoungest)
  
  ## remove the 80+ age_group
  tempCopied <- tempOldest[age_group_id != 160]  # drop data for ages 80+

  ## set list of columns to keep in output dataset
  colKeepList = c("acause", "sex_id", "year_range", "age_group_id", "surv_year", "SEER_surv")
  
  ## remove unnessary columns
  tempCleaned <- tempCopied[, colKeepList, with=FALSE]
  
  # one more check on ages present
  check_ages <- tempCleaned[!is.na(SEER_surv)]$age_group_id %>% unique()
  assert_that(length(setdiff(allowed_ages, check_ages)) == 0,
              msg = paste0("Final cleaned dataset has NA SEER_surv for ", 
                           cancer, "ages: ",
                           setdiff(allowed_ages, check_ages)))
    
  # subset on restricted ages
  tempCleaned <- tempCleaned[age_group_id %in% allowed_ages]
  
  assert_that(nrow(tempCleaned) > 0, 
              msg = paste0("No rows of data for ", cancer))
  
  ## export cleaned and formatted data, for use in next step
  fwrite(tempCleaned, paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_cleaned_gbd2020.txt"))
}

###################################################
## exception handling                             #
###################################################
## need to add in age group 6 for testicular (m), CML (m,f)
cancerExceptions <- c() #c("neo_testicular", "neo_leukemia_ml_chronic")

for(cancer in cancerExceptions){
  inData <- fread(paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_cleaned_gbd2020.txt"))
  replaceData <- copy(inData[age_group_id==7])
  replaceData <- replaceData[, age_group_id := 6]
  outData <- rbind(inData, replaceData)
  fwrite(outData, paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_cleaned_gbd2020.txt"))
}


