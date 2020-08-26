#######################################################
## Name of script: generateSurvivalDraws.R
## Description: Generates survival estimates using SEER data
##    and GBD MIR draws.
## Arguments: Requires formatted SEER data in appropriate format
##    and GBD MIR draws in appropriate format.
##    **User can change cancers and file paths at top**
## Outputs: Cancer-specific draws of relative survival in 
##    location-specific *.csv files.
## Author(s): NAME
## Last updated: 2018-09-27	


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

library(data.table)
library(parallel)
library(assertthat)

#### Set paths and cancers to loop through below ##
# MIR draws are here: FILEPATH/<cancer>/<location_id>.csv
# survival draws should go here: FILEPATH/<cancer>/<location_id>.csv

pathToGBDMIRs <- c("FILEPATH")
pathToOutput <- c("FILEPATH")
pathToSEERMIRdata <- c("FILEPATH")
pathToSEERcurve <- c("FILEPATH")

# list of cancers to pass to import function (use whatever subset being run, or entire list)
#cancerList <- c("neo_bladder", "neo_brain", "neo_breast", "neo_colorectal", "neo_esophageal", "neo_gallbladder",  
#                "neo_hodgkins", "neo_kidney", "neo_larynx", "neo_leukemia_ll_acute", "neo_leukemia_ll_chronic",
#                "neo_leukemia_ml_acute", "neo_leukemia_ml_chronic", "neo_leukemia_other", "neo_liver", "neo_lung",
#                "neo_lymphoma", "neo_melanoma", "neo_meso", "neo_mouth", "neo_myeloma", "neo_nasopharynx",
#                "neo_other_cancer", "neo_otherpharynx", "neo_pancreas", "neo_stomach", "neo_thyroid",
#                "neo_prostate", "neo_testicular", "neo_cervical", "neo_ovarian", "neo_uterine")


#### Set paths and cancers to loop through above ##
cancer <- commandArgs(trailingOnly=TRUE)[1]
print(cancer)  #print statement for log file to see what cancer is running

# set list of draws
### FOR GBD 2019, CHANGES PER DECOMP ROUND
# for decomp steps 1-3, 100 draws:
#drawNumber <- c(0:99)
# for decomp step 4, 1000 draws:
drawNumber <- c(0:999)         

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

## function to import MIR draws
importLocationDraws <- function(cancerType, locationID) {
  tempMIRdraw <- fread(paste0(pathToGBDMIRs, cancerType, "/", locationID)) #, ".csv"))
  return(tempMIRdraw)  
}

########################################
## beginning of working steps

# loop across cancer types (not needed if running parallel jobs on cluster)
#for(cancer in cancerList){     
  
  
####
## 2) import cancer-specific SEER MIR/survival data for regression

## (by cancer) import SEER datasets into R
SEERdata <- fread(paste0(pathToSEERMIRdata, cancer, "_SEER_5yr_MIRsurv.txt"))



####
## 3) import cancer-specific SEER 10-year survival scalar data

# import age-specifc survival data (1- through 10-year survival)
SEERsurv <- fread(paste0(pathToSEERcurve, cancer, "_SEERsurv10yrs_cleaned.txt"))

SEERsurv <- as.data.frame(SEERsurv)   # need as data.frame for later step 12

# restrict to 5-year survival for scaling
SEERscalar <- SEERsurv[SEERsurv$surv_year==5,]  
print('A')

####
## 4) generate cancer-specific prediction equation from SEER MIR/survival data
## note that three rare cancers have a different minimum case number (10 instead of 25)

# (by cancer) run a GLM regression on MIR/survival (for use in prediction)
if(paste0(cancer) %in% c("neo_leukemia_ll_acute", "neo_meso", "neo_nasopharynx"))
{
  seerPredGLMsurv <- glm(rel ~ invmir, data=SEERdata, which(SEERdata$source_mir==c('SEER') & n > 9 & interval == 5), family=quasibinomial(link=logit), weights = n)
} else {
  seerPredGLMsurv <- glm(rel ~ invmir, data=SEERdata, which(SEERdata$source_mir==c('GBD') & n > 24 & interval == 5), family=quasibinomial(link=logit), weights = n)
}
print('regression')
####
## 5) import location-specific GBD MIR draw data (*.csv by cancer and location_id)

# (Local) set path for location_id files
cancerFilePath <- paste0(pathToGBDMIRs, cancer, "/")

# get list of location_ids (consider updating to central code)
locationList <- list.files(cancerFilePath)


get_loc_draw <- function(locationID){
  # (by cancer, by location) import GBD MIR draws
  if (file.exists(paste0('FILEPATH', cancer, "/", locationID))) { 
    file.remove(paste0('FILEPATH', cancer, "/", locationID))
  }
  tempMIRdraw <- importLocationDraws(cancer, locationID) 
  # show current place in working through files
  print(paste0("Generating survival draws for ", cancer, ", location_id ", locationID)) 
  
  # save the age/year/sex/location info
  tempInfo <- copy(tempMIRdraw[, c("location_id", "year", "sex", "age_group_id"), with=FALSE])
  
  
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
  tempPredDraw <- tempPredDrawLogit
  tempPredDraw[, pred_col] <- exp(tempPredDraw[, pred_col]) / (1 + exp(tempPredDraw[, pred_col]))
  
  
  ## 9) add age/sex/year/location information back in
  
  tempPredDraw$location_id <- tempInfo$location_id
  tempPredDraw$year_id <- tempInfo$year
  tempPredDraw$sex_id <- tempInfo$sex
  tempPredDraw$age_group_id <- tempInfo$age_group_id
  
  
  ## 10) merge the SEER values with the GBD-predicted values
  
  adjDF <- merge(SEERscalar, tempPredDraw, 
                by=c('sex_id', 'age_group_id'), all.x=FALSE, all.y=TRUE)
  
  
  ## 11) generate location-specific scalars from SEER/GBD data
  
  # get list of prediction draw columns
  survival_col = names(adjDF)[grepl("pred_", names(adjDF))]
  tempScalar <- as.data.frame(adjDF)     # (as data.frame so next step works)
  #calculate the scalar for each draw (which here replaces the column from gbd predicted survival to the gbd/seer scalar value)
  tempScalar[,survival_col] <- tempScalar[,survival_col] / tempScalar[,'SEER_surv'] 
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
                  by=c('sex_id', 'age_group_id'), all.x=FALSE, all.y=TRUE)
  
  
  ## 13) generate location-specific scaled 1-10 year relative survival
  
  # get a list of all the scalar draw column names
  scalar_col = names(adjDF2)[grepl("surv_scalar_", names(adjDF2))]
  # calculate the scaled SEER curve
  adjDF2[, scalar_col] <- adjDF2[, scalar_col] * adjDF2[, 'SEER_surv']
  # rename columns as the scaled survival
  colnames(adjDF2) <- gsub("surv_scalar", "scaled_surv", colnames(adjDF2))
  
  
  ## 14) ensure all draw values are <1 
  
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
  
  ## 16) output the relative survival draws
  fwrite(adjDF3, paste0(pathToOutput, cancer, "/", locationID))
  


# parallelize generation of relative survival draws
mclapply(locationList, FUN = get_loc_draw, mc.cores = 30) 


######################
## End of script







