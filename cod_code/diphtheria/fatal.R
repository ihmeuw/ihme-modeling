#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: PART ONE -   Prepare negative binomial regression of diphtheria cases for input into codcorrect
#          PART TWO -   Format for CodCorrect and save results to database
#          PART THREE - Run DisMod model for CFR
#          PART FOUR -  Calculate nonfatal outcomes from mortality
#                       Use mortality to calculate prevalence (prev = mort/cfr*duration)
#                       Calculate incidence from prevalence (inc = prev/duration)
#          PART FIVE -  Format for COMO and save results to database
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr, foreign, stats, MASS, data.table, rhdf5, plyr, data.table, parallel, dplyr)
if (Sys.info()["sysname"] == "Linux") {
  require(mvtnorm, lib="FILEPATH")
} else { 
  pacman::p_load(mvtnorm)
}
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause <- "diptheria"
age_start <- 4
age_end <- 16
cause_id <- 338
me_id <- 1421

### make folders on cluster
cl.death.dir <- file.path("FILEPATH")                                    
dir.create(cl.death.dir, recursive = T)

cl.version.dir <- file.path("FILEPATH")                                               
dir.create(file.path(cl.version.dir), recursive = T)

### directories
home <- file.path(j_root, "FILEPATH")
j.version.dir <- file.path("FILEPATH")
j.version.dir.inputs <- file.path(j.version.dir, "model_inputs")
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
dir.create(j.version.dir.inputs, recursive = T)
dir.create(j.version.dir.logs, recursive = T)

### save description of model run
write.table(description, file.path(j.version.dir, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### load shared functions
file.path(j_root, "FILEPATH/get_population.R") %>% source
file.path(j_root, "FILEPATH/get_location_metadata.R") %>% source
file.path(j_root, "FILEPATH/get_covariate_estimates.R") %>% source
file.path(j_root, "FILEPATH/get_envelope.R") %>% source
file.path(j_root, "FILEPATH/get_draws.R") %>% source
#*********************************************************************************************************************** 


########################################################################################################################
##### PART ONE: COD NEG BIN REGRESSION #################################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=4, location_set_id=22) ## id=22 is from covariates team, id=9 is from epi
locations <- subset(locations, select=c("location_id", "ihme_loc_id", "location_name", "region_id", "super_region_id", "level", 
                                        "location_type", "parent_id", "super_region_name"))
pop_locs <- unique(locations$location_id)

### pop_env
# get envelope
envelope <- get_envelope(location_id=pop_locs, sex_id=1:2, age_group_id=c(age_start:age_end), with_hiv=0, year_id=1980:2016)
envelope <- subset(envelope, select=c("location_id", "year_id", "age_group_id", "sex_id", "mean"))
names(envelope)[names(envelope)=="mean"] <- "envelope"
# get population data
population <- get_population(location_id=pop_locs, year_id=1980:2016, age_group_id=c(age_start:age_end), sex_id=1:2)
population <- subset(population, select=c("location_id", "year_id", "age_group_id", "sex_id", "population"))
# bring together
pop_env <- merge(population, envelope, by=c("location_id", "year_id", "age_group_id", "sex_id"))


if (CALCULATE_FATAL == "yes") {

### covar
# covariate: DTP3_coverage_prop, covariate_id=32
covar <- get_covariate_estimates(covariate_name_short="DTP3_coverage_prop")
covar <- covar[covar$year_id >= 1980, ]
names(covar)[names(covar)=="mean_value"] <- "DTP3_coverage_prop"
covar <- subset(covar, select=c("location_id", "year_id", "DTP3_coverage_prop"))

### raw
# get raw data
raw <- read.csv(file.path(home, "data", updated_cod_data_location))
raw <- raw[raw$acause=="diptheria", ]

raw <- raw[!is.na(raw$cf_corr), ]
raw <- subset(raw, year_id >= 1980 & sample_size != 0 & age_group_id %in% c(age_start:age_end))

# death counts
raw$deaths <- raw$cf_corr * raw$sample_size

# round to whole number (counts!)
raw$deaths[raw$deaths < 0.5] <- 0

# inform model with national data only
raw <- merge(raw, subset(locations, select=c("location_id", "location_type")), by="location_id")
raw <- subset(raw, location_type=="admin0")

# drop outliers (cf greather than 99th percentile)
cf_99 <- quantile(raw$cf_corr, 0.99)
raw <- raw[raw$cf_corr <= cf_99, ]

# make ages into levels
raw$age_group_id <- as.factor(raw$age_group_id)

# bring together variables for regression
regress <- merge(raw, covar, by=c("location_id", "year_id"))

# save file for reference
write.csv(regress, file.path(j.version.dir.inputs, "inputs_for_nbreg_COD.csv"), row.names=FALSE)
#***********************************************************************************************************************


#----NEG BIN MODEL------------------------------------------------------------------------------------------------------
### run negative binomial regression
GLM <- glm.nb(deaths ~ DTP3_coverage_prop + age_group_id + offset(log(sample_size)), data=regress)
summary(GLM)

# save log
capture.output(summary(GLM), file = file.path(j.version.dir.logs, "log_deaths_nbreg.txt"), type="output")
#***********************************************************************************************************************


#----DRAWS--------------------------------------------------------------------------------------------------------------
set.seed(0311)

pred_death <- merge(pop_env, covar, by=c("year_id", "location_id"))
N <- length(pred_death$year_id)

### 1000 draws for uncertainty
# coefficient matrix
coefmat <- c(coef(GLM))
names(coefmat)[1] <- "constant"
names(coefmat) <- paste("b", names(coefmat), sep = "_")
coefmat <- matrix(unlist(coefmat), ncol=14, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))

# covariance matrix
vcovmat <- vcov(GLM)

# create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
betas <- t(betadraws)
rownames(betas) <- c("beta0", "beta1", paste0("age", 5:16))

betas <- betas %>% data.frame
#colnames(betas) <- paste0("beta_draw_", 0:999)

# betas for age group
betas_age <- betas
colnames(betas_age) <- paste0("age_draw_", 0:999)
betas_age$age_group_id <- c(NA, NA, 5:16)
betas_age <- betas_age[betas_age$age_group_id %in% c(5:16), ]
beta_4 <- c(rep(0, 1000), 4)
betas_age <- rbind(beta_4, betas_age)

# merge together predictions with age draws by age_group_id
pred_death <- merge(pred_death, betas_age, by="age_group_id", all.x=TRUE)

# create draws of disperion parameter
alphas <- 1 / exp(rnorm(1000, mean=GLM$theta, sd=GLM$SE.theta))

if (GAMMA_EPSILON == "with") {
	for (draw in 0:999) { 
    	alpha <- alphas[draw + 1]
    	pred_death[, paste0("death_draw_", draw)] <- rgamma( N, scale=(alpha * exp( betas[1, (draw+1)] + 
                                                                      (betas[2, (draw+1)]*pred_death$DTP3_coverage_prop) + 
                                                                      pred_death[[paste0("age_draw_", draw)]] ) * pred_death$envelope ), 
                                                				shape=(1 / alpha) )
	}
} else if (GAMMA_EPSILON == "without") {
	for (draw in 0:999) { 
    	alpha <- alphas[draw + 1]
    	pred_death[, paste0("death_draw_", draw)] <- exp( betas[1, (draw+1)] + 
                                                                    (betas[2, (draw+1)]*pred_death$DTP3_coverage_prop) + 
                                                                    pred_death[[paste0("age_draw_", draw)]] ) * pred_death$envelope
	}
}

# save results
pred_death_save <- subset(pred_death, select=c("location_id", "year_id", "age_group_id", "sex_id",
                                             colnames(pred_death)[grepl("death_draw_", colnames(pred_death))]))
colnames(pred_death_save) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))
#***********************************************************************************************************************

}


if (SAVE_FATAL == "yes") {

########################################################################################################################
##### PART TWO: FORMAT FOR CODCORRECT ##################################################################################
########################################################################################################################


#----SAVE RESULTS-------------------------------------------------------------------------------------------------------
### format for codcorrect
ids <- unique(pred_death_save$location_id)
years <- unique(pred_death_save$year_id)
sexes <- c(1,2)

pred_death_save <- pred_death_save %>% data.table

# save .csv for location-year-sex
write <- function(x,y,z) { write.csv(pred_death_save[pred_death_save$location_id==x & pred_death_save$year_id==y & pred_death_save$sex_id==z, ], 
                                    file.path(cl.death.dir, paste0(x, "_", y, "_", z, ".csv")), row.names=FALSE) }
for (location_id in ids) {
  for (year in years) {
    for (sex in sexes) {
      write(x=location_id, y=year, z=sex)
    }
  }
}

print(paste0("death draws saved for upload in ", cl.death.dir))
#*********************************************************************************************************************** 

}