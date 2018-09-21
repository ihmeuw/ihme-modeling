################################################################################
## Description: Run the first stage of the prediction model for adult mortality
##              and calculate inputs to GPR 
################################################################################

rm(list=ls())
library(foreign); library(zoo); library(nlme); library(data.table); library(assertable)

## Set local working directory (toggles by GIT user) 
if (Sys.info()[1] == "Linux") {
  root <- "filepath"
  user <- Sys.getenv("USER")
  code_dir <- paste0("filepath")
  sim <- as.numeric(commandArgs()[3])  ## this gets passed as 1 if not doing HIV sims, but gets ignored in the rest of the code, so it doesn't matter
  hivsims <- as.logical(as.numeric(commandArgs()[4]))

} else {
  root <- "filepath"
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0("filepath")
  sim <- 1 ## this gets passed as 1 if not doing HIV sims, but gets ignored in the rest of the code, so it doesn't matter
  hivsims <- F
}

data_dir <- paste("filepath", sep="")
source(paste("filepath", sep = ""))

print(sim)
print(hivsims)

locations_2015 <- fread(paste0("filepath" ))
## set transformation of the data for the GPR stage: choose from c("log10", "ln", "logit", "logit10")
transform <- "logit"

## Create sim importing function
import_sim <- function(sss, data) {
  subhiv <- read.csv(paste0("filepath"),stringsAsFactors=F)
  subhiv$year <- subhiv$year + .5
  if (is.null(subhiv$ihme_loc_id)) subhiv$ihme_loc_id <- subhiv$iso3
  subhiv$iso3 <- NULL
  subhiv$sex <- as.character(subhiv$sex)
  subhiv$sex[subhiv$sex == "1"] <- "male"
  subhiv$sex[subhiv$sex == "2"] <- "female"
  subhiv$sim <- NULL
  data <- data[data$ihme_loc_id %in% unique(subhiv$ihme_loc_id),]
  data <- merge(data,subhiv,all.x=T,by=c("year","sex","ihme_loc_id"))
  data$hiv[!is.na(data$hiv_cdr)] <- data$hiv_cdr[!is.na(data$hiv_cdr)]
  data$hiv_cdr <- NULL
  data$hiv[is.na(data$hiv)] <- 0
  return(data)
}

###############
## Read in data
###############
read_data <- function(hivsims, sim){
  setwd(data_dir)
  data <- read.csv("filepath", stringsAsFactors=F)
  data <- data[!is.na(data$sex),] 

  if(hivsims == 1) {
    setwd("filepath")
    data <- import_sim(sim, data) 
  }
  return(data)
}
data <- read_data(hivsims, sim)

################
# Fit first stage model
################

## Create first stage regression function
run_first_stage <- function(data, gbd_year = 2016) {
  #solve for mx
  data$mx <- log(1-data$mort)/(-45)
  data$tLDI <- log(data$LDI_id)
  
  #First run: Only 2015 locations
  #Second run: All GBD 2016 locations, then drop 2015 locations
  if (gbd_year == 2015){
  	data <- data[data$ihme_loc_id %in% locations_2015$ihme_loc_id,]
  }
  
  
  data$ihme_loc_id <- as.factor(data$ihme_loc_id)
  
  # Get starting values for stage 1 overall model (the model is not sensitive to starting values)
  start0 <- vector("list", 2)
  names(start0) <- c("male", "female")
  for (sex in unique(data$sex)) {
    start0[[sex]] <- c(beta1 = 0, 
                       beta2 = 0, 
                       beta3 = 0, 
                       beta5 = 0)
    names(start0[[sex]]) <- c("beta1","beta2","beta3","beta5")
  }
  
  ##Fit first stage model
  
  #grouped data object
  gr_dat <- vector("list", 2)
  names(gr_dat) <- c("male","female")
  for (sex in unique(data$sex)) {
    gr_dat[[sex]] <- groupedData(mx~ 1 | ihme_loc_id, data = data[data$sex == sex & !is.na(data$mort),])
  }
  
  #pre-specifying fixed effects, random effects, and formula
  fixed <- list(beta1 + beta2 + beta3 + beta5 ~ 1)
  random <- list(ihme_loc_id = beta4 ~ 1)
  form <- as.formula("mx ~ exp(beta1*tLDI + beta2*mean_yrs_educ + beta4 + beta5) + beta3*hiv")
  
  ##model with a random effect on country
  # set list to store models
  stage1.models <- vector("list", 2)
  names(stage1.models) <- c("male","female")
  for (sex in unique(data$sex)) {
    stage1.models[[sex]] <- nlme(form,
                                 data = gr_dat[[sex]],
                                 fixed = fixed,
                                 random = random, 
                                 groups = ~ihme_loc_id,
                                 start = c(start0[[sex]]),
                                 control=nlmeControl(maxIter=300,
                                                     pnlsMaxIter=30),
                                 verbose = F)
  }
  
  ## Save stage 1 model

  if (hivsims) {
    setwd(paste0("filepath"))
    save(stage1.models, file=paste0("filepath"))
    save(stage1.models, file=paste("filepath", sep=""))
  } else {
    save(stage1.models, file=paste0("filepath"))
    save(stage1.models, file=paste("filepath", sep=""))
  }
  
  #Merge iso3 random effects into data
  for (sex in unique(data$sex)) {
    for (ii in rownames(stage1.models[[sex]]$coefficients$random$ihme_loc_id)) data$ctr_re[data$ihme_loc_id == ii & data$sex == sex] <- stage1.models[[sex]]$coefficients$random$ihme_loc_id[ii,1]
  }
  data$ctr_re[is.na(data$ctr_re)] <- 0
  
  #Get data back in order
  data <- data[order(data$sex,data$ihme_loc_id, data$year),]
  
  #predictions w/o any random effects
  pred.mx <- vector("list", 2)
  names(pred.mx) <- c("male","female")
  for (sex in unique(data$sex)) {
    data$pred.mx.noRE[data$sex == sex] <- exp(stage1.models[[sex]]$coefficients$fixed[1]*data$tLDI[data$sex == sex] 
                                              + stage1.models[[sex]]$coefficients$fixed[2]*data$mean_yrs_educ[data$sex == sex] 
                                              + stage1.models[[sex]]$coefficients$fixed[4]) + stage1.models[[sex]]$coefficients$fixed[3]*data$hiv[data$sex == sex]
  }
  data$pred.1.noRE <- 1-exp(-45*data$pred.mx.noRE)
  
  #Predictions with random effects (or, if RE == NA, then return without RE)
  for (sex in unique(data$sex)) {
    data$pred.mx.wRE[data$sex == sex] <- exp(stage1.models[[sex]]$coefficients$fixed[1]*data$tLDI[data$sex == sex] 
                                             + stage1.models[[sex]]$coefficients$fixed[2]*data$mean_yrs_educ[data$sex == sex]  
                                             + stage1.models[[sex]]$coefficients$fixed[4] 
                                             + data$ctr_re[data$sex == sex]) + stage1.models[[sex]]$coefficients$fixed[3]*data$hiv[data$sex == sex] 
  } 
  data$pred.1.wRE <- 1-exp(-45*(data$pred.mx.wRE))
  
  # calculate residuals from final first stage regression
  data$resid <- logit(data$mort) - logit(data$pred.1.noRE)
  return(data)
} # End run_first_stage function

## Run first stage model -- if it fails on this sim, use the data from the previous sim as input into the function
#First, just run on 2015 locations
#Next, run on all locations and only keep 2016 estimates
temp <- copy(data)
result_2015 <- tryCatch({
  run_first_stage(temp, gbd_year = 2015)
}, error = function(err) {
  sim_minus = sim - 1
  if (sim == 1){sim_minus = 250}
  temp <- read_data(hivsims, sim_minus)  # Use previous sim's data
  run_first_stage(temp, gbd_year = 2015)           # Run first stage off of previous sim's data
}) # End tryCatch

result_2016 <- tryCatch({
  run_first_stage(data)
}, error = function(err) {
  sim_minus = sim - 1
  if (sim == 1){sim_minus = 250}
  data <- read_data(hivsims, sim_minus)  # Use previous sim's data
  run_first_stage(data)           # Run first stage off of previous sim's data
}) # End tryCatch

#Remove 2015 locations from the 2016 run, then put both together
result_2016 <- result_2016[!(result_2016$location_id %in% locations_2015$location_id), ]
result <- rbind(result_2015, result_2016)

if (hivsims) {
  setwd(paste0("filepath"))
  write.csv(result, paste0("filepath"))
} else {
  setwd(data_dir)
  write.csv(result, paste0("filepath"))
}
