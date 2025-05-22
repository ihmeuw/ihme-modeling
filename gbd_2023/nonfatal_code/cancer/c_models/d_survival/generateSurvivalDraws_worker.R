#######################################################
## Name of script: generateSurvivalDraws.R
## Description: Generates survival estimates using SEER data
##    and GBD MIR draws.
## Arguments: Requires formatted SEER data in appropriate format
##    and GBD MIR draws in appropriate format.
##    **User can change cancers and file paths at top**
## Outputs: Cancer-specific draws of relative survival in 
##    location-specific *.csv files.
## Author(s): USERNAME


# workflow description
# 1) load functions and libraries needed by each cancer
# 2) import cancer-specific SEER MIR/survival data for regression
# 3) import cancer-specific SEER 10-year survival scalar data
# 4) generate cancer-specific prediction equation from SEER MIR/survival data
# 5) import location-specific GBD MIR draw data 
# 6) transform MIR to inverse-MIR (1 - MIR)
# 7) generate location-specific predictions of 5-year relative survival 
# 8) back-transform from logit-survival predictions to survival proportions
# 9) add age/sex/year/location information back in
# 10) merge the SEER values with the GBD-predicted values
# 11) generate location-specific scalars from SEER/GBD data
# 12) merge in the 1-10year survival SEER data (expands by rows)
# 13) generate location-specific scaled 1-10 year relative survival
# 14) ensure all draw values are >0 and <1
# 15) delete columns that aren't needed in the output
# 16) output the relative survival draws


####
## 1) load functions and libraries needed by each cancer
library(here)
library(dplyr)
library(data.table)
library(parallel)
library(assertthat)
source(file.path("FILEPATH/get_location_metadata.R"))
source(file.path("FILEPATH/get_age_metadata.R"))

if (!exists("code_repo"))  {
  code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
  if (!grepl("cancer_estimation", code_repo)) {
    code_repo <- file.path(code_repo, 'cancer_estimation')
  }
}
source(file.path('FILEPATH'))
source(file.path('FILEPATH'))
source(file.path('FILEPATH'))

gbd_round <- get_gbd_parameter("current_gbd_name")$current_gbd_name

#### Set paths and cancers to loop through below ##
# MIR draws are here: FILEPATH/<cancer>/<location_id>.csv
# survival draws should go here: FILEPATH/<cancer>/<location_id>.csv
# <<update to GBD2019 folder for next cycle>>
#2017: pathToGBDMIRs <- FILEPATH
pathToGBDMIRs <- paste0(FILEPATH)
#2017: pathToOutput <- FILEPATH

# above for cluster, below for local testing
#pathToGBDMIRs <- FILEPATH
#pathToSEERMIRdata <- FILEPATH
#pathToOutput <- FILEPATH
#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# change above paths to run on cluster, below ok (until these get moved to /share)


# list of cancers to pass to import function 
#cancerList <- c("neo_bladder", "neo_brain", "neo_breast", "neo_colorectal", "neo_esophageal", "neo_gallbladder",  
#                "neo_hodgkins", "neo_kidney", "neo_larynx", "neo_leukemia_ll_acute", "neo_leukemia_ll_chronic",
#                "neo_leukemia_ml_acute", "neo_leukemia_ml_chronic", "neo_leukemia_other", "neo_liver", "neo_lung",
#                "neo_lymphoma", "neo_melanoma", "neo_meso", "neo_mouth", "neo_myeloma", "neo_nasopharynx",
#                "neo_other_cancer", "neo_otherpharynx", "neo_pancreas", "neo_stomach", "neo_thyroid",
#                "neo_prostate", "neo_testicular", "neo_cervical", "neo_ovarian", "neo_uterine")
#cancerList <- c("neo_bladder")
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#### Set paths and cancers to loop through above ##
args <- commandArgs(trailingOnly = TRUE)
if(length(args) < 2){ # for array jobs
  task_id <- Sys.getenv("SGE_TASK_ID") %>% as.numeric
  params <- fread(args[1])[task_id, ]
  cancer <- unique(params$cancer)
  cnf_vers_id <- unique(params$cnf_version_id) 
  cur_locationID <- unique(params$location_id)
} else{
  cancer <- args[1] 
  cnf_vers_id <- args[2] %>% as.numeric
  cur_locationID <- args[3] %>% as.numeric
}

if(cnf_vers_id >= 21){
  pathToOutput <- FILEPATH
} else{
  pathToOutput <- FILEPATH
}


print(cancer)  #print statement for log file to see what cancer is running
if(cancer %in% c("neo_eye", "neo_lymphoma_burkitt", "neo_lymphoma_other", "neo_neuro", 
                 "neo_eye_rb", "neo_bone", "neo_tissue_sarcoma", "neo_eye_other", "neo_liver_hbl")){
  pathToSEERcurve <- FILEPATH
  pathToSEERMIRdata <- FILEPATH
} else{
  pathToSEERcurve <- FILEPATH
  pathToSEERMIRdata <- FILEPATH
}

# set list of draws
### FOR GBD 2019, CHANGES PER DECOMP ROUND
# for decomp steps 1-3, 100 draws:
#drawNumber <- c(0:99)
# for decomp step 4, 1000 draws:
drawNumber <- c(0:999)     
new_causes <-  c("neo_eye", "neo_lymphoma_burkitt", "neo_lymphoma_other", "neo_neuro", 
                 "neo_eye_rb", "neo_bone", "neo_tissue_sarcoma", "neo_eye_other", "neo_liver_hbl")
# function to predict survival from the inverse MIR (GLM)
GBDpredictGLM <- function(x, tempInvMIR){
  tempInvMIR <- data.frame(tempInvMIR)         # setting data as a data.frame (necessary for predict.glm)
  i <- x 
  tmp_col <- tempInvMIR[, paste0('invmir_',i)]   # grabbing one column 
  tmp_col <- data.frame(tmp_col)                   # casting list into a data.frame column
  tmp_col_name <- paste0('pred_',i)                # preserving the original column name 
  colnames(tmp_col) <- 'invmir'                    # renaming to enable prediction function 
  gbdPredGLMsurvlogitTest <- predict.glm(seerPredGLMsurv, newdata=tmp_col)   # predict survival from invMIR
  output <- data.frame(gbdPredGLMsurvlogitTest)
  colnames(output) <- tmp_col_name
  return((output))
} 

 
# function to import all nonfatal location 
load_nf_locations <- function() { 
  gbd_locs <- get_location_metadata(location_set_id=35)
  estimation_locs <- gbd_locs[most_detailed==1, ]
  return(unique(estimation_locs[,location_id]))
}

format_surv_mir <- function(this_acause, this_location_id, cnf_model_version_id) {
    ## Retrieves and formats mi_ratio model results for the indicated gbd cause 
    ##      and location_id, then finalizes (thus saving) the results
    ## copied from format_mir_draws.r in nonfatal, but excludes saving draws as output
    print(paste("formatting mi for", this_acause, this_location_id))
    uid_cols = c('location_id', 'year_id', 'sex_id', 'age_group_id')
    output_folder <- file.path(get_path("mir_draws_output", process="nonfatal_model"), 
                                this_acause)
    output_file = paste0(output_folder,"/", this_location_id, ".csv")
    ensure_dir(output_file)
    input_dt <- load_data(this_acause, this_location_id, cnf_model_version_id)
    # Use data from the smallest existing age group to fill-in any missing data
    full_dt <- mir.replace_missing_ages(input_dt)
    if (this_acause %in% c('neo_bone','neo_eye','neo_eye_other','neo_eye_rb','neo_liver_hbl','neo_lymphoma_burkitt','neo_lymphoma_other','neo_neuro','neo_tissue_sarcoma')) { 
        has_caps <- copy(full_dt)
        names(has_caps)[names(has_caps) == 'year'] <- 'year_id'
        names(has_caps)[names(has_caps) == 'sex'] <- 'sex_id'
    }
    else { 
        has_caps <- add_caps(full_dt, this_acause, cnf_model_version_id)
    } 
    # revert to cartesian space and adjust outputs by the caps, first ensuring
    #       that data are within the correct boundaries
    print("Calculating final mi and saving...")
    draw_cols = names(has_caps)[grepl('draw', names(has_caps))]
    mir_cols = gsub("draw", "mir", draw_cols)
    mir_draws <- as.data.table(sapply(draw_cols, revertAndAdjust, draws = has_caps))
    # save and output as csv
    setnames(mir_draws, colnames(mir_draws), mir_cols)
    final_data <- cbind(subset(has_caps,,uid_cols), mir_draws)
    final_data$age_group_id <- as.integer(as.character(final_data$age_group_id))
    final_data <- subset(final_data, age_group_id %ni% c(21, 27, 28), )
    #
    return(final_data)
}

## function to import MIR draws
importLocationDraws <- function(cancerType, locationID) {
  tempMIRdraw <- format_surv_mir(cancerType, locationID, cnf_vers_id)
  return(tempMIRdraw) 
}

get_allowed_ages <- function(cancer, sex_id = 3){
  #
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
########################################
## beginning of working steps

# loop across cancer types
#for(cancer in cancerList){     
  
  
####
## 2) import cancer-specific SEER MIR/survival data for regression

## (by cancer) import SEER datasets into R
SEERdata <- fread(paste0(pathToSEERMIRdata, cancer, "_SEER_5yr_MIRsurv.txt"))


####
## 3) import cancer-specific SEER 10-year survival scalar data

# import age-specifc survival data (1- through 10-year survival)

SEERsurv <- fread(paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_cleaned_gbd2020.txt"))

SEERsurv <- as.data.frame(SEERsurv)   # need as data.frame for later step 12

# restrict to 5-year survival for scaling
SEERscalar <- SEERsurv[SEERsurv$surv_year==5,]  

####
## 4) generate cancer-specific prediction equation from SEER MIR/survival data
## note that three rare cancers have a different minimum case number (10 instead of 25)
if(!("source_mir" %in% (SEERdata %>% colnames))){
  setnames(SEERdata, "source_MIR", "source_mir")
}

# (by cancer) run a GLM regression on MIR/survival (for use in prediction)
if(cancer %in% c("neo_leukemia_ll_acute", "neo_meso", "neo_nasopharynx")){
  seerPredGLMsurv <- glm(rel ~ invmir, data=SEERdata, which(SEERdata$source_mir==c('SEER') & n > 9 & interval == 5), family=quasibinomial(link=logit), weights = n)
} else if (cancer %ni% new_causes) {
  seerPredGLMsurv <- glm(rel ~ invmir, data=SEERdata, which(SEERdata$source_mir==c('SEER') & n > 24 & interval == 5), family=quasibinomial(link=logit), weights = n)
} else { 
  seerPredGLMsurv <- glm(rel ~ invmir, data=SEERdata, which(SEERdata$source_mir==c('GBD') & n > 24 & interval == 5), family=quasibinomial(link=logit), weights = n)
}
print('regression')

####
## 5) import location-specific GBD MIR draw data (*.csv by cancer and location_id)

# (Local) set path for location_id files
cancerFilePath <- paste0(pathToGBDMIRs, cancer, "/")

# get list of location_ids (consider updating to central code)
locationList <- load_nf_locations() 
#list.files(cancerFilePath) # TODO: create function 
#locationList <- paste0(get_location_metadata(location_set_id = 35)$location_id %>% unique, ".csv")


get_loc_draw <- function(locationID){
  # (by cancer, by location) import GBD MIR draws
  #if (file.exists(paste0('FILEPATH'))) { 
  #  file.remove(paste0('FILEPATH'))
  #}
  tempMIRdraw <- importLocationDraws(cancer, locationID)
  # show current place in working through files
  print(paste0("Generating survival draws for ", cancer, ", location_id ", locationID)) 
  
  # save the age/year/sex/location info
  tempInfo <- subset(tempMIRdraw, select = c("location_id", "year_id", "sex_id", "age_group_id")) 
  #tempInfo <- copy(tempMIRdraw[, c("location_id", "year_id", "sex_id", "age_group_id"), with=FALSE])
  
  
  ## 6) transform MIR to inverse-MIR (1 - MIR)
  
  # get list of MIR draw columns
  draw_col <- names(tempMIRdraw)[grepl("mir_", names(tempMIRdraw))]   
  # make temp copy for inv_mir step (as data.frame so next step works)
  tempInvMIR <- as.data.frame(tempMIRdraw)
  # calculate inv_mir
  tempInvMIR[, draw_col] <- 1 - tempInvMIR[, draw_col]     
  # rename columns to be inv_mir
  colnames(tempInvMIR) <- gsub("mir", "invmir", colnames(tempInvMIR))  
  
  
  ## 7) generate location-specific predictions of 5-year relative survival 
  
  # predict relative survival draws (GLM)
  tempPredDrawLogit <- sapply(drawNumber, GBDpredictGLM, tempInvMIR = tempInvMIR)    
  # convert output to a data.frame
  tempPredDrawLogit <- as.data.frame(do.call(cbind, tempPredDrawLogit))   
  
  ## 8) back-transform from logit-survival predictions to survival proportions
  
  # get list of prediction draw columns
  pred_col <- names(tempPredDrawLogit)[grepl("pred_", names(tempPredDrawLogit))] 
  tempPredDraw <- tempPredDrawLogit %>% copy
  tempPredDraw[, pred_col] <- exp(tempPredDraw[, pred_col]) / (1 + exp(tempPredDraw[, pred_col]))
  
  
  ## 9) add age/sex/year/location information back in
  
  tempPredDraw$location_id <- tempInfo$location_id
  tempPredDraw$year_id <- tempInfo$year
  tempPredDraw$sex_id <- tempInfo$sex
  tempPredDraw$age_group_id <- tempInfo$age_group_id
  
  
  ## 10) merge the SEER values with the GBD-predicted values
  
  adjDF <- merge(SEERscalar, tempPredDraw, 
                by=c('sex_id', 'age_group_id'), all.x=FALSE, all.y=TRUE)
  setDT(adjDF)
  # checking for no merge issues
  allowed_values <- get_allowed_ages(cancer)
  assert_that(nrow(adjDF[is.na(SEER_surv) & sex_id != 3 & surv_year <= 10 & age_group_id %in% allowed_values]) == 0, 
              msg = "NA SEER surv after merging with GBD predicted values")
  adjDF <- adjDF[sex_id != 3] %>% as.data.frame
  
  ## 11) generate location-specific scalars from SEER/GBD data
  
  # get list of prediction draw columns
  survival_col = names(adjDF)[grepl("pred_", names(adjDF))]
  tempScalar <- as.data.frame(adjDF)     # (as data.frame so next step works)
  #calculate the scalar for each draw (which here replaces the column from gbd predicted survival to the gbd/seer scalar value)
  tempScalar[,survival_col] <- tempScalar[, survival_col] / tempScalar[,'SEER_surv'] 
  #rename the draw columns as the scalar value
  colnames(tempScalar) <- gsub("pred", "surv_scalar", colnames(tempScalar))
  

  ## 12) merge in the 1-10year survival SEER data (expands by rows)
  
  # drop duplicate data from previous step
  tempScalar$SEER_surv <- NULL   
  tempScalar$surv_year <- NULL
  tempScalar$year_range <- NULL
  tempScalar$acause <- NULL
  # merge SEER data
  adjDF2 <- merge(SEERsurv, tempScalar,
                  by=c('sex_id', 'age_group_id'), all.x=FALSE, all.y=TRUE, allow.cartesian = TRUE)
  
  setDT(adjDF2)
  # checking for no merge issues
  assert_that(nrow(adjDF2[is.na(SEER_surv) & sex_id != 3 & surv_year <= 10 & age_group_id %in% allowed_values]) == 0, 
              msg = "Error: NA SEER surv after merging with GBD predicted values")
  adjDF2 <-adjDF2 %>% as.data.frame()
  
  
  ## 13) generate location-specific scaled 1-10 year relative survival
  
  # get a list of all the scalar draw column names
  scalar_col = names(adjDF2)[grepl("surv_scalar_", names(adjDF2))]
  # calculate the scaled SEER curve
  adjDF2[, scalar_col] <- adjDF2[, scalar_col] * adjDF2[, 'SEER_surv']
  # rename columns as the scaled survival
  colnames(adjDF2) <- gsub("surv_scalar", "scaled_surv", colnames(adjDF2))
  
  
  ## 14) ensure all draw values are <1 (add >0?)
  
  # get a list of all the scaled survival draw column names
  scaledSurv_cols <- grep("scaled_surv_", names(adjDF2), value  = TRUE)
  adjDF3 <- as.data.table(adjDF2)
  # set order for next for loop
  setcolorder(adjDF3, c(scaledSurv_cols, setdiff(names(adjDF3), scaledSurv_cols)))
  # replace survival predictions > 1 with 1
  for(j in seq_along(scaledSurv_cols)){
    set(adjDF3, i = which(adjDF3[[j]]>1), j=j, value = 1)
  }
  
  ## 15) delete columns that aren't needed in the output
  adjDF3$SEER_surv <- NULL
  adjDF3$year_range <- NULL
  adjDF3$acause <- NULL
  
  # checking for no NAs for random draw col
  assert_that(nrow(adjDF3[is.na(scaled_surv_83) & sex_id != 3 & surv_year <= 10 & age_group_id %in% allowed_values]) == 0, 
              msg = "Error: NA SEER surv after merging with GBD predicted values")
  
  ## 16) output the relative survival draws
  if(!dir.exists(paste0(pathToOutput, cancer))){
    dir.create(paste0(pathToOutput, cancer))
  }
  fwrite(adjDF3, paste0(pathToOutput, cancer, "/", locationID, '.csv'))
}


# parallelize generation of relative survival draws
#lapply(c("527"), FUN = get_loc_draw) # option for running specific locations only relative survival
#mclapply(locationList, FUN = get_loc_draw, mc.cores = 30) 
get_loc_draw(cur_locationID)

######################
## End of script






