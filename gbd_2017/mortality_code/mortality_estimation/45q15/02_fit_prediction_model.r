################################################################################
## Description: Run the first stage of the prediction model for adult mortality
##              and calculate inputs to GPR
################################################################################
rm(list=ls())
library(foreign); library(zoo); library(nlme); library(data.table); library(assertable); library(readr)
library(ltcore, lib = "FILEPATH")


model_target_locations <- fread(model_targets_file)


## set transformation of the data for the GPR stage: choose from c("log10", "ln", "logit", "logit10")
transform <- "logit"

## Create sim importing function
import_sim <- function(input_dir, sss, data) {
  filename <- "FILEPATH"

  subhiv <- read.csv(filename, stringsAsFactors=F)
  subhiv$year <- subhiv$year + .5
  if (is.null(subhiv$ihme_loc_id)) subhiv$ihme_loc_id <- subhiv$iso3
  subhiv$iso3 <- NULL
  subhiv$sex <- as.character(subhiv$sex)
  subhiv$sex[subhiv$sex == "1"] <- "male"
  subhiv$sex[subhiv$sex == "2"] <- "female"
  subhiv$sim <- NULL

  data <- data[data$ihme_loc_id %in% unique(subhiv$ihme_loc_id), ]
  data <- merge(data, subhiv, all.x=T, by=c("year", "sex", "ihme_loc_id"))
  data$hiv[!is.na(data$hiv_cdr)] <- data$hiv_cdr[!is.na(data$hiv_cdr)]
  data$hiv_cdr <- NULL
  data$hiv[is.na(data$hiv)] <- 0

  return(data)
}

###############
## Read in data
###############
read_data <- function(input_dir, hivsims, sim){
  data <- read.csv("FILEPATH", stringsAsFactors=F)
  data <- data[!is.na(data$sex), ]

  if(hivsims == 1) {
    data <- import_sim(input_dir, sim, data)
  }
  return(data)
}
data <- read_data(output_dir, hivsims, sim)


################
# Fit first stage model
################
## Create first stage regression function
run_first_stage <- function(data, locations, current_gbd_year) {

  #solve for mx
  data$mx <- log(1-data$mort)/(-45)
  data$tLDI <- log(data$LDI_id)

  #First run: Only previous gbd round locations
  #Second run: All current GBD locations, then drop 2015 locations (occurs after the function)
  data <- data[data$ihme_loc_id %in% locations$ihme_loc_id, ]

  # Transform location and sex into factors
  data$ihme_loc_id <- as.factor(data$ihme_loc_id)
  data$sex <- as.factor(data$sex)

  # Prepare grouped data for model
  gr_dat <- groupedData(mx ~ 1 | ihme_loc_id, data = data[!is.na(data$mort), ])

  # Get starting values for stage 1 overall model
  start0 <- c(rep(0, 4 + length(unique(data[data$data == 1, "sex"]))))

  # Specify fixed effects, random effects, and formula
  fixed <- list(beta1 + beta2 + beta3 + beta5 ~ 1, beta6 ~ sex)
  random <- list(ihme_loc_id = beta4 ~ 1)
  form <- as.formula("mx ~ exp(beta1*tLDI + beta2*mean_yrs_educ + beta4 + beta5*m5q0 + beta6) + beta3*hiv")

  ##model with a random effect on country
  # set list to store models
  stage1.models <- nlme(form,
                        data = gr_dat,
                        fixed = fixed,
                        random = random,
                        groups = ~ihme_loc_id,
                        start = start0,
                        control=nlmeControl(maxIter=300,
                                            pnlsMaxIter=30),
                        verbose = F)

  ## Save stage 1 model
  filename <- "FILEPATH"
  save(stage1.models, file=filename)
  # save coefficients for csv
  stage_1_coefficients <- data.table(ldi = c(stage1.models$coefficients$fixed[1]),
                                     education = c(stage1.models$coefficients$fixed[2]),
                                     hiv = c(stage1.models$coefficients$fixed[3]),
                                     m5q0 = c(stage1.models$coefficients$fixed[4]),
                                     sex_intercept = c(stage1.models$coefficients$fixed[5]),
                                     sex_male_intercept = c(stage1.models$coefficients$fixed[6])
  )
  write_csv(stage_1_coefficients, path = "FILEPATH")

  #Merge iso3 random effects into data
  for (ii in rownames(stage1.models$coefficients$random$ihme_loc_id)) {
    data$ctr_re[data$ihme_loc_id == ii] <- stage1.models$coefficients$random$ihme_loc_id[ii, 1]
  }
  data$ctr_re[is.na(data$ctr_re)] <- 0

  #Get data back in order
  data <- data[order(data$sex, data$ihme_loc_id, data$year), ]

  #predictions w/o any random effects
  for (sex in unique(data$sex)) {
    if (sex == "male") {
      data$pred.mx.noRE[data$sex == sex] <- (exp(stage1.models$coefficients$fixed[1]*data$tLDI[data$sex == sex]
                                                 + stage1.models$coefficients$fixed[2]*data$mean_yrs_educ[data$sex == sex]
                                                 + stage1.models$coefficients$fixed[4]*data$m5q0[data$sex == sex]
                                                 + stage1.models$coefficients$fixed[5]
                                                 + stage1.models$coefficients$fixed[6])
                                             + stage1.models$coefficients$fixed[3]*data$hiv[data$sex == sex])
    } else {
      data$pred.mx.noRE[data$sex == sex] <- (exp(stage1.models$coefficients$fixed[1]*data$tLDI[data$sex == sex]
                                                 + stage1.models$coefficients$fixed[2]*data$mean_yrs_educ[data$sex == sex]
                                                 + stage1.models$coefficients$fixed[4]*data$m5q0[data$sex == sex]
                                                 + stage1.models$coefficients$fixed[5])
                                             + stage1.models$coefficients$fixed[3]*data$hiv[data$sex == sex])
    }
  }
  data$pred.1.noRE <- 1-exp(-45*data$pred.mx.noRE)

  #Predictions with random effects (or, if RE == NA, then return without RE)
  for (sex in unique(data$sex)) {
    if (sex == "male") {
      data$pred.mx.wRE[data$sex == sex] <- (exp(stage1.models$coefficients$fixed[1]*data$tLDI[data$sex == sex]
                                                 + stage1.models$coefficients$fixed[2]*data$mean_yrs_educ[data$sex == sex]
                                                 + stage1.models$coefficients$fixed[4]*data$m5q0[data$sex == sex]
                                                 + stage1.models$coefficients$fixed[5]
                                                 + stage1.models$coefficients$fixed[6]
                                                 + data$ctr_re[data$sex == sex])
                                             + stage1.models$coefficients$fixed[3]*data$hiv[data$sex == sex])
    } else {
      data$pred.mx.wRE[data$sex == sex] <- (exp(stage1.models$coefficients$fixed[1]*data$tLDI[data$sex == sex]
                                                 + stage1.models$coefficients$fixed[2]*data$mean_yrs_educ[data$sex == sex]
                                                 + stage1.models$coefficients$fixed[4]*data$m5q0[data$sex == sex]
                                                 + stage1.models$coefficients$fixed[5]
                                                 + data$ctr_re[data$sex == sex])
                                             + stage1.models$coefficients$fixed[3]*data$hiv[data$sex == sex])
    }
  }
  data$pred.1.wRE <- 1-exp(-45*(data$pred.mx.wRE))

  # calculate residuals from final first stage regression
  data$resid <- logit(data$mort) - logit(data$pred.1.noRE)
  return(data)
} # End run_first_stage function

## Run first stage model -- if it fails on this sim, use the data from the previous sim as input into the function
#First, just run primary locations
#Next, run on all locations and only keep secondary estimates
primary_model_locs <- model_target_locations[primary == T, ]
secondary_model_locs <- model_target_locations[secondary == T, ]

temp <- copy(data)
result_primary <- run_first_stage(temp, primary_model_locs, gbd_year - 1)
result_secondary <- run_first_stage(data, secondary_model_locs, gbd_year)

#Remove 2015 locations from the 2016 run, then put both together
result_secondary <- result_secondary[!(result_secondary$location_id %in% primary_model_locs$location_id), ]
result <- rbind(result_primary, result_secondary)

filename <- "FILEPATH"
write_csv(result, filename)
