################################################################################
## Description: Run the first stage of the prediction model for adult mortality
##              and calculate inputs to GPR
################################################################################
rm(list=ls())
library(foreign); library(zoo); library(argparse); library(nlme);
library(data.table); library(assertable); library(readr)
library(ltcore, lib = "FILEPATH")
library(lme4)

# Parse arguments
if(!interactive()) {
  parser <- ArgumentParser()
  parser$add_argument('--version_id', type="integer", required=TRUE,
                      help='The version_id for this run')
  parser$add_argument('--sim', type="integer", required=TRUE,
                      help='Sim for this run')
  parser$add_argument('--hivsims', type="integer", required=TRUE,
                      help='total hiv sims')
  parser$add_argument('--gbd_year', type="integer", required=TRUE,
                      help="GBD year")
  parser$add_argument('--code_dir', type="character", required=TRUE,
                      help="Directory where code is cloned")
  args <- parser$parse_args()
  list2env(args, .GlobalEnv)

  hivsims <- as.logical(hivsims)
} else {
  gbd_year <-
  version_id <-
  sim <-
  code_dir <- "FILEPATH"
  hivsims <-
}

use_covid <-

output_dir <- paste0("FILEPATH")
data_dir <- paste0("FILEPATH")
dir.create(paste0(output_dir), showWarnings = FALSE)
dir.create(paste0("FILEPATH"), showWarnings = FALSE)
model_targets_file <- paste0("FILEPATH")

model_target_locations <- fread(model_targets_file)

## set transformation of the data for the GPR stage
transform <- "logit"

## Create sim importing function
import_sim <- function(input_dir, sss, data) {
  filename <- paste0("FILEPATH")

  subhiv <- fread(filename)
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
  data <- fread(paste0("FILEPATH"))
  data <- as.data.frame(data)
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
#solve for mx
data$mx <- log(1-data$mort)/(-45)
data$tLDI <- log(data$LDI_id)

# set pandemic years (2020 and beyond)
data$pandemic_year <- data$year >= 2020

primary_model_locs <- model_target_locations[primary == T, ]
current_gbd_year <- gbd_year - 1
# Step 1. Run stage 1 mixed effects model for standard locations only

df <- data[data$ihme_loc_id %in% primary_model_locs$ihme_loc_id, ]

if (use_covid) {
  # set pandemic years (2020 and beyond)
  df$pandemic_year <- df$year >= 2020
}

# Transform location and sex into factors
df$ihme_loc_id <- as.factor(df$ihme_loc_id)
df$sex <- as.factor(df$sex)

# Prepare grouped data for model
gr_dat <- groupedData(mx ~ 1 | ihme_loc_id, data = df[!is.na(df$mort), ])

# Specify fixed effects, random effects, and formula
random <- list(ihme_loc_id = beta4 ~ 1)
if (use_covid) {
  # Get starting values for stage 1 overall model
  start0 <- c(rep(0, 5 + length(unique(df[df$data == 1, "sex"]))))
  fixed <- list(beta1 + beta2 + beta3 + beta5 + beta7 ~ 1, beta6 ~ sex)
  form <- as.formula("mx ~ exp(beta1*tLDI + beta2*mean_yrs_educ + beta4 + beta5*m5q0 + beta6) + beta7*covid_inf_pc + beta3*hiv")
} else {
  # Get starting values for stage 1 overall model
  start0 <- c(rep(0, 4 + length(unique(df[df$data == 1, "sex"]))))
  fixed <- list(beta1 + beta2 + beta3 + beta5 ~ 1, beta6 ~ sex)
  form <- as.formula("mx ~ exp(beta1*tLDI + beta2*mean_yrs_educ + beta4 + beta5*m5q0 + beta6) + beta3*hiv")
}

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
                      verbose = T)

## Save stage 1 model
if (hivsims) {
  filename <- paste0("FILEPATH")
} else {
  filename <- paste0("FILEPATH")
}
save(stage1.models, file=filename)

# save coefficients for csv
if (use_covid) {
  stage_1_coefficients <- data.table(ldi = c(stage1.models$coefficients$fixed["beta1"]),
                                     education = c(stage1.models$coefficients$fixed["beta2"]),
                                     hiv = c(stage1.models$coefficients$fixed["beta3"]),
                                     m5q0 = c(stage1.models$coefficients$fixed["beta5"]),
                                     covid_inf_pc = c(stage1.models$coefficients$fixed["beta7"]),
                                     sex_intercept = c(stage1.models$coefficients$fixed["beta6.(Intercept)"]),
                                     sex_male_intercept = c(stage1.models$coefficients$fixed["beta6.sexmale"])
  )
} else {
  stage_1_coefficients <- data.table(ldi = c(stage1.models$coefficients$fixed["beta1"]),
                                     education = c(stage1.models$coefficients$fixed["beta2"]),
                                     hiv = c(stage1.models$coefficients$fixed["beta3"]),
                                     m5q0 = c(stage1.models$coefficients$fixed["beta5"]),
                                     sex_intercept = c(stage1.models$coefficients$fixed["beta6.(Intercept)"]),
                                     sex_male_intercept = c(stage1.models$coefficients$fixed["beta6.sexmale"])
  )
}
fwrite(stage_1_coefficients, paste0("FILEPATH"))

#predictions w/o any random effects
#Step 2. Take predicted fixed effect coefficients, apply to all location-year-sex combinations
if (use_covid) {
  for (sex in unique(data$sex)) {
    if (sex == "male") {
      data$pred.mx.noRE[data$sex == sex] <- (exp(stage_1_coefficients$ldi*data$tLDI[data$sex == sex]
                                                 + stage_1_coefficients$education*data$mean_yrs_educ[data$sex == sex]
                                                 + stage_1_coefficients$m5q0*data$m5q0[data$sex == sex]
                                                 + stage_1_coefficients$sex_intercept
                                                 + stage_1_coefficients$sex_male_intercept)
                                             + stage_1_coefficients$hiv*data$hiv[data$sex == sex]
                                             + stage_1_coefficients$covid_inf_pc*data$covid_inf_pc[data$sex == sex])
    } else {
      data$pred.mx.noRE[data$sex == sex] <- (exp(stage_1_coefficients$ldi*data$tLDI[data$sex == sex]
                                                 + stage_1_coefficients$education*data$mean_yrs_educ[data$sex == sex]
                                                 + stage_1_coefficients$m5q0*data$m5q0[data$sex == sex]
                                                 + stage_1_coefficients$sex_intercept)
                                             + stage_1_coefficients$hiv*data$hiv[data$sex == sex]
                                             + stage_1_coefficients$covid_inf_pc*data$covid_inf_pc[data$sex == sex])
    }
  }
} else {
  for (sex in unique(data$sex)) {
    if (sex == "male") {
      data$pred.mx.noRE[data$sex == sex] <- (exp(stage_1_coefficients$ldi*data$tLDI[data$sex == sex]
                                                 + stage_1_coefficients$education*data$mean_yrs_educ[data$sex == sex]
                                                 + stage_1_coefficients$m5q0*data$m5q0[data$sex == sex]
                                                 + stage_1_coefficients$sex_intercept
                                                 + stage_1_coefficients$sex_male_intercept)
                                             + stage_1_coefficients$hiv*data$hiv[data$sex == sex])
    } else {
      data$pred.mx.noRE[data$sex == sex] <- (exp(stage_1_coefficients$ldi*data$tLDI[data$sex == sex]
                                                 + stage_1_coefficients$education*data$mean_yrs_educ[data$sex == sex]
                                                 + stage_1_coefficients$m5q0*data$m5q0[data$sex == sex]
                                                 + stage_1_coefficients$sex_intercept)
                                             + stage_1_coefficients$hiv*data$hiv[data$sex == sex])
    }
  }
}
data$pred.1.noRE <- 1-exp(-45*data$pred.mx.noRE)

#Step 3. Subtract FE predictions from all 45q15 data values to generate FE-residual
data$fe_resid <- logit(data$mort) - logit(data$pred.1.noRE)

#Step 4. Then run a random effect only model on the residuals.
stage1_re_models <- lmer(fe_resid ~ 1 + (1 | ihme_loc_id) , data = data[!is.na(data$fe_resid), ], REML= FALSE)

#Merge iso3 random effects into data
re =  random.effects(stage1_re_models)$ihme_loc_id
for (ii in rownames(re)) {
  data$ctr_re[data$ihme_loc_id == ii] <- re[ii, 1]
}
data$ctr_re[is.na(data$ctr_re)] <- 0

#Get data back in order
data <- data[order(data$sex, data$ihme_loc_id, data$year), ]

#Predictions with random effects
if (use_covid) {
  for (sex in unique(data$sex)) {
    if (sex == "male") {
      data$pred.mx.wRE[data$sex == sex] <- (exp(stage_1_coefficients$ldi*data$tLDI[data$sex == sex]
                                                + stage_1_coefficients$education*data$mean_yrs_educ[data$sex == sex]
                                                + stage_1_coefficients$m5q0*data$m5q0[data$sex == sex]
                                                + stage_1_coefficients$sex_intercept
                                                + stage_1_coefficients$sex_male_intercept
                                                + data$ctr_re[data$sex == sex])
                                            + stage_1_coefficients$hiv*data$hiv[data$sex == sex]
                                            + stage_1_coefficients$covid_inf_pc*data$covid_inf_pc[data$sex == sex])
    } else {
      data$pred.mx.wRE[data$sex == sex] <- (exp(stage_1_coefficients$ldi*data$tLDI[data$sex == sex]
                                                + stage_1_coefficients$education*data$mean_yrs_educ[data$sex == sex]
                                                + stage_1_coefficients$m5q0*data$m5q0[data$sex == sex]
                                                + stage_1_coefficients$sex_intercept
                                                + data$ctr_re[data$sex == sex])
                                            + stage_1_coefficients$hiv*data$hiv[data$sex == sex]
                                            + stage_1_coefficients$covid_inf_pc*data$covid_inf_pc[data$sex == sex])
    }
  }
} else {
  for (sex in unique(data$sex)) {
    if (sex == "male") {
      data$pred.mx.wRE[data$sex == sex] <- (exp(stage_1_coefficients$ldi*data$tLDI[data$sex == sex]
                                                + stage_1_coefficients$education*data$mean_yrs_educ[data$sex == sex]
                                                + stage_1_coefficients$m5q0*data$m5q0[data$sex == sex]
                                                + stage_1_coefficients$sex_intercept
                                                + stage_1_coefficients$sex_male_intercept
                                                + data$ctr_re[data$sex == sex])
                                            + stage_1_coefficients$hiv*data$hiv[data$sex == sex])
    } else {
      data$pred.mx.wRE[data$sex == sex] <- (exp(stage_1_coefficients$ldi*data$tLDI[data$sex == sex]
                                                + stage_1_coefficients$education*data$mean_yrs_educ[data$sex == sex]
                                                + stage_1_coefficients$m5q0*data$m5q0[data$sex == sex]
                                                + stage_1_coefficients$sex_intercept
                                                + data$ctr_re[data$sex == sex])
                                            + stage_1_coefficients$hiv*data$hiv[data$sex == sex])
    }
  }
}
data$pred.1.wRE <- 1-exp(-45*(data$pred.mx.wRE))

# calculate residuals from final first stage regression
data$resid <- logit(data$mort) - logit(data$pred.1.noRE)


if (hivsims) {
  filename <- paste0("FILEPATH")
} else {
  filename <- paste0("FILEPATH")
}
fwrite(data, filename)

