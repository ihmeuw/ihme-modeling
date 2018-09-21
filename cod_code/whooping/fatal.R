#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Model pertussis fatal and nonfatal outcomes for GBD
#          PART ONE -    Incidence natural history model
#          PART TWO -    Rescale case estimates
#          PART THREE -  Age-sex split cases
#          PART FOUR -   Calculate prevalence and incidence from split case estimates
#          PART FIVE -   Format for COMO and save results to the database
#          PART SIX -    Model fatal outcomes with negative binomial model
#          PART SEVEN -  Calculate deaths from CFR
#          PART EIGHT -  Rescale death estimates
#          PART NINE -   Age-sex split deaths
#          PART TEN -    Replace modeled death estimates in select locations with CODEm data-rich feeder model
#          PART ELEVEN - Format for codcorrect and save results to the database
#***********************************************************************************************************************


########################################################################################################################
##### START-UP #########################################################################################################
########################################################################################################################


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr, foreign, stats, MASS, data.table, plyr, dplyr, lme4, parallel)
if (Sys.info()["sysname"] == "Linux") {
  library(rhdf5, lib="FILEPATH")
  require(mvtnorm, lib="FILEPATH")
} else { 
  pacman::p_load(mvtnorm, rhdf5)
}
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause <- "whooping"
age_start <- 4             ##
age_end <- 16              ## age 55-59 years
a <- 3                     ## birth cohort years before 1980
cause_id <- 339
me_id <- 1424

### make folders on cluster
cl.death.dir <- file.path("FILEPATH")                                                
dir.create(cl.death.dir, recursive = T)

cl.version.dir <- file.path("FILEPATH")                                              
dir.create(file.path(cl.version.dir), recursive = T)

### directories
home <- file.path(j_root, "FILEPATH")
j.version.dir <- file.path(home, "models", custom_version)
j.version.dir.inputs <- file.path(j.version.dir, "model_inputs")
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
dir.create(j.version.dir.inputs, recursive = T)
dir.create(j.version.dir.logs, recursive = T)

### save description of model run
write.table(description, file.path(j.version.dir, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### reading .h5 files
read_block_names <- function(str){
  # Use string splitting to get the column names of a value block
  split_vec <- strsplit(str, "'")[[1]]
  N <- length(split_vec)
  split_vec[seq(from=2, to=N, by=2)]
}
value_blocks <- function(attrib){
  # get all the value block names from the attributes of an hdf5 table
  grep(glob2rx("values_block_*_kind"), names(attrib), value=TRUE)
}
h5_value_col_names <- function(h5File, key){
  # return a list where each element are the column names of the differnt
  # value block types i.e. float, int, string
  attrib <- h5readAttributes(h5File, paste0(key, "/table/"))
  value_block_names <- value_blocks(attrib)
  nam <- lapply(value_block_names, function(x) read_block_names(attrib[[x]]))
  names(nam) <- gsub("_kind", "", value_block_names)
  nam
}
mat_to_df <- function(mat, mat_names){
  # pytables hdf5 saves values as matrices when more than one column exists 
  # We need to transpose it then apply the names
  if (length(dim(mat)) == 1){
    mat <- matrix(data=mat, nrow=length(mat), ncol=length(mat_names))
  }
  else{
    mat <- t(mat)
  }
  df <- as.data.frame(mat)
  names(df) <- mat_names
  df
}
read_hdf5_table <- function(h5File, key){
  # read in the indices  values as well as the value blocks
  # only indices values may be strings
  data_list <- h5read(h5File, paste0(key, "/table/"), compoundAsDataFrame=F)
  data_value_names <- h5_value_col_names(h5File, key)
  indices <- setdiff(names(data_list), c("index", names(data_value_names)))
  df_index <- data.frame(data_list[indices])
  df_values <- lapply(names(data_value_names), function(x) 
    mat_to_df(data_list[[x]], data_value_names[[x]]))
  do.call(cbind, c(list(df_index), df_values))
}

### load shared functions
file.path(j_root, "FILEPATH/get_population.R") %>% source
file.path(j_root, "FILEPATH/get_location_metadata.R") %>% source
file.path(j_root, "FILEPATH/get_covariate_estimates.R") %>% source
file.path(j_root, "FILEPATH/get_envelope.R") %>% source

### load personal functions
file.path(home, "FILEPATH/age_sex_split.R") %>% source
file.path(home, "FILEPATH/collapse_point_and_CI.R") %>% source
file.path(home, "FILEPATH/rescaling_function_GBD2016.R") %>% source
#***********************************************************************************************************************


########################################################################################################################
##### PART ONE: INCIDENCE NAT HIST MODEL ###############################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=4, location_set_id=22) ## id=22 is from covariates team, id=9 is from epi
locations <- subset(locations, select=c("location_id", "ihme_loc_id", "location_name", "region_id", "super_region_id", 
										"level", "location_type", "parent_id"))
pop_locs <- unique(locations$location_id)

### get population for age less than 1 year
population <- get_population(location_id=pop_locs, year_id=(1980-a):2016, age_group_id=2:4)
population <- subset(population, select=c("location_id", "year_id", "age_group_id", "population"))

# add on ihme_loc_id
population <- merge(population, subset(locations, select=c("location_id", "ihme_loc_id")), by="location_id", all.x=TRUE)

# collapse by location year
sum_pop <- ddply(population, c("location_id","ihme_loc_id", "year_id"), summarise, pop=sum(population) )
sum_pop$year_id <- sum_pop$year_id + a
sum_pop <- sum_pop[sum_pop$year_id <= 2016, ]                                                                                                                               

### prep WHO case notification data, downloaded from http://www.who.int/immunization/monitoring_surveillance/data/en/
cases <- read.csv(file.path(home, "data", "WHO_incidence_series_whooping_compatible.csv"))
names(cases)[names(cases)=="ISO_code"] <- "ihme_loc_id"
cases <- cases[, names(cases)!="WHO_REGION" & names(cases)!="Disease" & names(cases)!="Cname"]
cases <- melt(cases, id.var="ihme_loc_id")
names(cases)[names(cases)=="variable"] <- "year_id"
names(cases)[names(cases)=="value"] <- "cases"
cases$year_id <- gsub("X", "",  cases$year_id)
cases$year_id <- as.numeric(cases$year_id)
cases <- na.omit(cases)  
cases$whodata <- 1
# drop 1980 and 1981, which yielded implausibly high cases numbers
cases <- cases[!cases$year_id==1980 & !cases$year_id==1981, ]

### prep US territories 
US_terr <- read.csv(file.path(home, "data", "US_territories.csv"), 1)
US_terr <- US_terr[, c("ihme_loc_id", "year_start", "cases")]
names(US_terr)[names(US_terr)=="year_start"] <- "year_id"
US_terr <- na.omit(US_terr)
#US_terr$year <- as.character(US_terr$year_id)

### prep pertussis historical data
UK <- read.csv(file.path(home, "FILEPATH"))
UK <- UK[UK$iso3=="XEW", ]
names(UK)[names(UK)=="iso3"] <- "ihme_loc_id"
names(UK)[names(UK)=="year"] <- "year_id"
UK$ihme_loc_id <- gsub("XEW", "GBR",  UK$ihme_loc_id)
UK <- UK[, grep(paste(c("ihme_loc_id", "year_id", "^pop"), collapse="|"), colnames(UK), value=TRUE)]

# collapse UK pop by year
UK_pop <- ddply(UK, c("ihme_loc_id", "year_id"), summarise, ukpop=sum(pop2) )
UK_pop$year_id <- UK_pop$year_id + a
UK_pop$ukpop <- UK_pop$ukpop * 100000

# pull together all historical data files
pne <- read.csv(file.path(j_root, "FILEPATH"))
names(pne) <- tolower(names(pne))
pne <- pne[pne$iso3=="XEW", ]
names(pne)[names(pne)=="iso3"] <- "ihme_loc_id"
colnames(pne)[colnames(pne)=="year"] <- "year_id"
pne$notifications <- as.integer(pne$notifications)
# GBR 1940+
pne1940 <- read.dta(file.path(j_root, "FILEPATH"))
names(pne1940)[names(pne1940)=="iso3"] <- "ihme_loc_id"
names(pne1940)[names(pne1940)=="year"] <- "year_id"
historical <- bind_rows(pne, pne1940, cases)

#replace missing cases with notification data
historical$cases[is.na(historical$cases)] <- historical$notifications[is.na(historical$cases)]
historical$source[historical$whodata==1] <- "WHO"
historical <- historical[, c("year_id", "cases", "ihme_loc_id", "vacc_rate", "source")]
historical$ihme_loc_id <- gsub("XEW", "GBR",  historical$ihme_loc_id)
historical$vacc_rate[historical$year_id <= 1923] <- 0
historical <- bind_rows(historical, US_terr)

### get covariate
# covariate: DTP3_coverage_prop, covariate_id=32
covar <- get_covariate_estimates(covariate_name_short="DTP3_coverage_prop")
covar <- covar[covar$year_id >= 1980, ]
names(covar)[names(covar)=="mean_value"] <- "DTP3"
covar <- subset(covar, select=c("location_id", "year_id", "DTP3"))

### prep inputs for regression
# collapse duplicates
regr <- ddply(historical, c("ihme_loc_id", "year_id"), summarise, 
              cases=max(cases, na.rm=TRUE),
              vacc_rate=min(vacc_rate, na.rm=TRUE) )
regr <- join(regr, sum_pop[, c("ihme_loc_id", "year_id", "location_id", "pop")], by=c("ihme_loc_id", "year_id"))
regr <- join(regr, UK_pop, by=c("ihme_loc_id", "year_id"))
regr$pop[is.na(regr$pop)] <- regr$ukpop[is.na(regr$pop)]
regr$location_id[regr$ihme_loc_id=="GBR"] <- 95
regr <- regr[!is.na(regr$pop), ]

# add covariate
regr <- join(regr, covar, by=c("location_id", "year_id"))
regr$vacc_rate[is.na(regr$vacc_rate)] <- regr$DTP3[is.na(regr$vacc_rate)]
regr <- regr[!is.na(regr$vacc_rate), ]
regr <- regr[!is.na(regr$pop), ]

# calculate log incidence
regr <- regr[, c("year_id", "cases", "ihme_loc_id", "vacc_rate", "pop")] 
#regr<-na.omit(regr) # no NAs
regr$pop <- round(regr$pop, 0)
regr$incidence <- (regr$cases/regr$pop) * 100000
regr$ln_inc <- log(regr$incidence)

# calculate log vaccination status
regr$ln_vacc <- log(regr$vacc_rate)
regr$ln_unvacc <- log(1-regr$vacc_rate)

# replace Inf with NA
regr <- do.call(data.frame, lapply(regr, function(x) replace(x, is.infinite(x), NA)))

# drop if incidence greater than 20,000 cases/100,000 pop
regr <- regr[regr$incidence <= 20000, ]

# save input for reference
write.csv(regr, file.path(j.version.dir.inputs, paste0("incidence_regression_input.csv")), row.names=FALSE)
#*********************************************************************************************************************** 


#----MIXED EFFECTS MODEL------------------------------------------------------------------------------------------------
### run mixed effects regression model
me_model <- lmer(ln_inc ~ ln_unvacc + (1 | ihme_loc_id), data=regr)

# save log
capture.output(summary(me_model), file = file.path(j.version.dir.logs, "log_incidence_mereg.txt"), type="output")
#*********************************************************************************************************************** 


#----DRAWS--------------------------------------------------------------------------------------------------------------
set.seed(0311)

### prep data
draws <- join(covar, sum_pop, by=c("location_id", "year_id"), type="inner")
draws$ln_unvacc <- log(1-draws$DTP3)

N <- length(draws$year_id)

### 1000 draws for uncertainty 
# prep random effects
reffect <- data.frame(ranef(me_model)[[1]])
#standard random effect set to Switzerland, where the pertussis monitoring system is thought to capture a large percentage of cases
reffect_CHE <- reffect["CHE",]

#coefficient matrix
beta1 <- coef(me_model)[[1]] %>% data.frame
beta1 <- beta1["CHE", "ln_unvacc"]
beta0 <- fixef(me_model)[[1]] %>% data.frame
coeff <- cbind(beta0, beta1, reffect_CHE)
colnames(coeff) <- c("constant", "b_unvax", "b_reffect_CHE")
# remove RE term
coefmat <- coeff[, !colnames(coeff)=="b_reffect_CHE"]
coefmat <- matrix(unlist(coefmat), ncol=2, byrow=TRUE,
                  dimnames=list(c("coef"), c(as.vector(names(coefmat)))))

# covariance matrix
vcovs <- vcov(me_model)
vcovlist <- c(vcovs[1,1], vcovs[1,2], vcovs[2,1], vcovs[2,2])
vcovmat <- matrix(vcovlist, ncol=2, byrow=TRUE)

# create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)

# add random effect of CHE to coefficient draws
betadraws <- cbind(betadraws, rep(reffect_CHE, 1000))

# transpose coefficient matrix
betas <- t(betadraws)

draws <- draws %>% data.frame
# generate draws of the prediction using coefficient draws
for (i in 0:999) { 
    draws[, paste0("case_draw_", i)] <- exp( betas[1, (i+1)] + 
                                           ( betas[2, (i+1)] * draws$ln_unvacc ) + 
                                             betas[3, (i+1)] ) *
                                        ( draws$pop / 100000 )
}
#*********************************************************************************************************************** 


#----SAVE---------------------------------------------------------------------------------------------------------------
# save results
save_draws <- draws[, c("location_id", "year_id", colnames(draws)[grepl("case_draw_", colnames(draws))])]

if (WRITE_FILES == "yes") {
  write.csv(save_draws, file.path(j.version.dir, "01_case_draws_for_rescaling.csv"), row.names=FALSE)
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART TWO: RESCALE CASES ##########################################################################################
########################################################################################################################


#----RESCALE------------------------------------------------------------------------------------------------------------
### run custom rescale function
rescale_GBD2016(input_file=save_draws, measure="case")
rescaled_cases <- rescaled_draws
#*********************************************************************************************************************** 

 
#----SAVE---------------------------------------------------------------------------------------------------------------
if (WRITE_FILES == "yes") {
  # save results
  write.csv(rescaled_cases, file.path(j.version.dir, "02_case_predictions_rescaled_for_split.csv"), row.names=FALSE)
}
#*********************************************************************************************************************** 


if (CALCULATE_NONFATAL == "yes") {

########################################################################################################################
##### PART THREE: CASES AGE-SEX SPLIT ##################################################################################
########################################################################################################################


#----SPLIT--------------------------------------------------------------------------------------------------------------
age_sex_split(acause=acause, input_file=rescaled_cases, measure="case")
split_cases <- split_data_frame
#*********************************************************************************************************************** 


#----SAVE---------------------------------------------------------------------------------------------------------------
if (WRITE_FILES == "yes") {
  # save split draws
  write.csv(subset(split_cases, select=c("location_id", "year_id", "age_group_id", "sex_id", "population", 
            colnames(split_cases)[grep("split_case_draw_", colnames(split_cases))])), file.path(j.version.dir, "03_split_case_draws.csv"), row.names=FALSE)
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART FOUR: CASES TO PREVALENCE AND INCIDENCE #####################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
predictions_conversion <- subset(split_cases, select=c("location_id", "year_id", "age_group_id", "sex_id",
                            colnames(split_cases)[grep("split_case_draw_", colnames(split_cases))], "population"))
#***********************************************************************************************************************


#----PREVALENCE---------------------------------------------------------------------------------------------------------
### convert cases to prevalence, duration of 50 days
for (ii in 0:999) {
  predictions_conversion[[paste0("prev_draw_", ii)]] <- 
    ( predictions_conversion[[paste0("split_case_draw_", ii)]] * (50/365) ) / predictions_conversion$population
}
                      
# save results
predictions_prev_save <- subset(predictions_conversion, select=c("location_id", "year_id", "age_group_id", "sex_id",
                                     colnames(predictions_conversion)[grep("prev_draw", colnames(predictions_conversion))]))
#***********************************************************************************************************************


#----INCIDENCE---------------------------------------------------------------------------------------------------------
### convert cases to incidence rate
for (ii in 0:999) {
  predictions_conversion[[paste0("inc_draw_", ii)]] <- 
    predictions_conversion[[paste0("split_case_draw_", ii)]] / predictions_conversion$population
}

# save results
predictions_inc_save <- subset(predictions_conversion, select=c("location_id", "year_id", "age_group_id", "sex_id",
                                     colnames(predictions_conversion)[grep("inc_draw", colnames(predictions_conversion))]))
#*********************************************************************************************************************** 


#----SAVE---------------------------------------------------------------------------------------------------------------
if (WRITE_FILES == "yes") {
  # save incidence
  write.csv(predictions_prev_save, file.path(j.version.dir, "05_prev_draws.csv"), row.names=FALSE)

  # save prevalence
  write.csv(predictions_inc_save, file.path(j.version.dir, "07_inc_draws.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART FIVE: FORMAT FOR COMO #######################################################################################
########################################################################################################################


#----SAVE RESULTS-------------------------------------------------------------------------------------------------------
### format prevalence for como
# prevalence, measure_id==5
colnames(predictions_prev_save) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))
predictions_prev_save$measure_id <- 5

### format incidence for como
# incidence, measure_id==6
colnames(predictions_inc_save) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))
predictions_inc_save$measure_id <- 6

save_nonfatal <- rbind(predictions_prev_save, predictions_inc_save)
save_nonfatal <- save_nonfatal[save_nonfatal$age_group_id %in% c(age_start:age_end), ]
#save_nonfatal <- save_nonfatal %>% data.table
lapply(unique(save_nonfatal$location_id), function(x) write.csv(save_nonfatal[save_nonfatal$location_id==x, ],
                            file.path(cl.version.dir, paste0(x, ".csv"))))
print(paste0("nonfatal estimates saved in ", cl.version.dir))
#***********************************************************************************************************************

}


if (CALCULATE_COD == "yes") {

########################################################################################################################
##### PART SIX: MODEL CFR ##############################################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get covariates
# covariate: LDI_pc, covariate_id=57
ldi <- get_covariate_estimates(covariate_name_short="LDI_pc")
ldi <- ldi[ldi$year_id >= 1980, ]
ldi$ln_ldi <- log(ldi$mean_value)
ldi$ldi <- ldi$mean_value
ldi <- subset(ldi, select=c("location_id", "year_id", "ln_ldi", "ldi"))

# covariate: HSA, covariate_id=208, covariate_name_short="health_system_access_capped"
hsa <- get_covariate_estimates(covariate_name_short="health_system_access_capped")
hsa <- hsa[hsa$year_id >= 1980, ]
colnames(hsa)[colnames(hsa)=="mean_value"] <- "health"
hsa <- subset(hsa, select=c("location_id", "year_id", "health"))

# under-2 malnutrition, covariate_id=66
mal_cov <- get_covariate_estimates(covariate_name_short="underweight_prop_waz_under_2sd")
mal_cov <- mal_cov[mal_cov$year_id >= 1980, ]
mal_cov$ln_mal <- log(mal_cov$mean_value)
mal_cov <- subset(mal_cov, select=c("location_id", "year_id", "ln_mal"))

# healthcare access and quality index
haqi <- get_covariate_estimates(covariate_name_short="haqi")
haqi$HAQI <- haqi$mean_value / 100
haqi <- subset(haqi, select=c("location_id", "year_id", "HAQI"))

# merge covars together
cfr_covar <- join(hsa, ldi, by=c("location_id", "year_id"))
cfr_covar <- join(cfr_covar, haqi, by=c("location_id", "year_id"))

### read in CFR data - GBD 2010 literature review
cfr <- read.csv(file.path(j_root, "FILEPATH"))

# drop outliers
cfr$ignore[is.na(cfr$ignore)] <-0
cfr <- cfr[cfr$ignore != 1, ]
cfr <- cfr[!(cfr$iso3=="UGA" & cfr$Year.Start==1951), ]

# prep cfr 
names(cfr)[names(cfr)=="iso3"] <- "ihme_loc_id"
names(cfr)[names(cfr)=="Parameter.Value"] <- "cfr"
names(cfr)[names(cfr)=="numerator_number_of_cases_with_the_condition"] <- "deaths"
names(cfr)[names(cfr)=="effective_sample_size"] <- "cases"
cfr$mid_point_year_of_data_collection[cfr$mid_point_year_of_data_collection < 1980] <- 1980
names(cfr)[names(cfr)=="mid_point_year_of_data_collection"] <- "year_id"
names(cfr)[names(cfr)=="Age.Start"] <- "age_start"
names(cfr)[names(cfr)=="Age.End"] <- "age_end"
cfr <- cfr[, c("ihme_loc_id", "year_id", "age_start", "age_end", "deaths", "cases", "cfr")]

# prep cfr_expert - additional CFR data from experts - GBD 2013 update
cfr_expert <- read.csv(file.path(j_root, "FILEPATH"))
names(cfr_expert)[names(cfr_expert)=="iso3"] <- "ihme_loc_id"
names(cfr_expert)[names(cfr_expert)=="mean"] <- "cfr"
names(cfr_expert)[names(cfr_expert)=="numerator"] <- "deaths"
names(cfr_expert)[names(cfr_expert)=="denominator"] <- "cases"
cfr_expert$year_id <- cfr_expert$year_start
cfr_expert <- cfr_expert[, c("ihme_loc_id", "year_id", "age_start", "age_end", "deaths", "cases", "cfr")]

# add in new CFR data from 2016 literature review
cfr_2016 <- read.csv(file.path(home, "FILEPATH"))
names(cfr_2016)[names(cfr_2016)=="cases"] <- "deaths"
names(cfr_2016)[names(cfr_2016)=="sample_size"] <- "cases"
cfr_2016$cfr <- cfr_2016$deaths / cfr_2016$cases
cfr_2016$year_id <- (cfr_2016$year_start + cfr_2016$year_end) / 2
cfr_2016$year_id <- round(cfr_2016$year_id, 0)
cfr_2016 <- cfr_2016[, c("ihme_loc_id", "year_id", "age_start", "age_end", "deaths", "cases", "cfr")]
#cfr_2016$new_data <- "yes"

# merge CFR data
cfr_data <- bind_rows(cfr, cfr_expert, cfr_2016)

# merge on location_id
cfr_data <- merge(cfr_data, subset(locations, select=c("location_id", "ihme_loc_id")), by="ihme_loc_id", all.x=TRUE)

### add covariates
cfr_data <- merge(cfr_data, cfr_covar, by=c("location_id", "year_id"), all.x=TRUE)
cfr_data <- cfr_data[, colnames(cfr_data) != "cfr"]

# calculate CFR
cfr_data$cfr <- cfr_data$deaths / cfr_data$cases
cfr_data <- cfr_data[cfr_data$cfr <= 0.5, ]
cfr_data$cases[!is.na(cfr_data$cases) & cfr_data$cases < 1 & cfr_data$cases > 0] <- 1

# save CFR regression inputs 
write.csv(cfr_data, file.path(j.version.dir.inputs, "cfr_regression_input.csv"), row.names=FALSE)
#***********************************************************************************************************************


#----CFR REGRESSION-----------------------------------------------------------------------------------------------------
theta <- 1 / 1.666942
cfr_data$deaths <- round(cfr_data$deaths, 0)

if (HAQI_or_HSA == "use_HSA") {
  nb_model <- glm.nb(deaths ~ ln_ldi + health + offset(log(cases)), data=cfr_data)
  summary(nb_model)

  # save log
  capture.output(summary(nb_model), file = file.path(j.version.dir.logs, "log_cfr_negbin.txt"), type="output")
} else if (HAQI_or_HSA == "use_HAQI") {
  nb_model <- glm.nb(deaths ~ ln_ldi + HAQI + offset(log(cases)), data=cfr_data)
  summary(nb_model)

  # save log
  capture.output(summary(nb_model), file = file.path(j.version.dir.logs, "log_cfr_negbin.txt"), type="output")
} else if (HAQI_or_HSA == "HAQI_only") {
  nb_model <- glm.nb(deaths ~ HAQI + offset(log(cases)), data=cfr_data)
  summary(nb_model)

  # save log
  capture.output(summary(nb_model), file = file.path(j.version.dir.logs, "log_cfr_negbin.txt"), type="output")
}   
#***********************************************************************************************************************


#----DRAWS--------------------------------------------------------------------------------------------------------------
set.seed(0311)

### predict out for all country-year-age-sex
pred_CFR <- join(sum_pop, cfr_covar, by=c("location_id", "year_id"), type="inner")
N <- length(pred_CFR$year_id)

### 1000 draws for uncertainty
# coefficient matrix
coefmat <- c(coef(nb_model))
names(coefmat)[1] <- "constant"
names(coefmat) <- paste("b", names(coefmat), sep = "_")
coefmat <- matrix(unlist(coefmat), ncol=length(coefmat), byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))

# covariance matrix
vcovmat <- vcov(nb_model)

# create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
betas <- t(betadraws)

# create draws of disperion parameter
alphas <- 1 / exp(rnorm(1000, mean=nb_model$theta, sd=nb_model$SE.theta))

### estimate deaths
if (HAQI_or_HSA == "use_HSA") {
    if (GAMMA_EPSILON == "without") {
        # generate draws of the prediction using coefficient draws
        for (i in 0:999) { 
            pred_CFR[, paste0("cfr_draw_", i)] <- exp( betas[1, (i+1)] + 
                                                     ( betas[2, (i+1)] * pred_CFR$ln_ldi ) +
                                                     ( betas[3, (i+1)] * pred_CFR$health ) )
        } 
    } else if (GAMMA_EPSILON == "with") {
        for (i in 0:999) { 
            pred_CFR[, paste0("cfr_draw_", i)] <- rgamma( N, scale=(alphas[i+1] * 
                                                          exp( betas[1, (i+1)] + 
                                                        ( betas[2, (i+1)] * pred_CFR$ln_ldi ) +
                                                        ( betas[3, (i+1)] * pred_CFR$health ) )), shape=(1 / alphas[i+1]) )
        }
    }
} else if (HAQI_or_HSA == "use_HAQI") {
    if (GAMMA_EPSILON == "without") {
        # generate draws of the prediction using coefficient draws
        for (i in 0:999) { 
            pred_CFR[, paste0("cfr_draw_", i)] <- exp( betas[1, (i+1)] + 
                                                     ( betas[2, (i+1)] * pred_CFR$ln_ldi ) +
                                                     ( betas[3, (i+1)] * pred_CFR$HAQI ) )
        } 
    } else if (GAMMA_EPSILON == "with") {
        for (i in 0:999) { 
            pred_CFR[, paste0("cfr_draw_", i)] <- rgamma( N, scale=(alphas[i+1] * 
                                                          exp( betas[1, (i+1)] + 
                                                        ( betas[2, (i+1)] * pred_CFR$ln_ldi ) +
                                                        ( betas[3, (i+1)] * pred_CFR$HAQI ) )), shape=(1 / alphas[i+1]) )
        }
    }
} else if (HAQI_or_HSA == "HAQI_only") {
    if (GAMMA_EPSILON == "without") {
        # generate draws of the prediction using coefficient draws
        for (i in 0:999) { 
            pred_CFR[, paste0("cfr_draw_", i)] <- exp( betas[1, (i+1)] + 
                                                     ( betas[2, (i+1)] * pred_CFR$HAQI ) )
        } 
    } else if (GAMMA_EPSILON == "with") {
        for (i in 0:999) { 
            pred_CFR[, paste0("cfr_draw_", i)] <- rgamma( N, scale=(alphas[i+1] * 
                                                          exp( betas[1, (i+1)] + 
                                                        ( betas[2, (i+1)] * pred_CFR$HAQI ) )), shape=(1 / alphas[i+1]) )
        }
    }
}

# save results
CFR_draws_save <- subset(pred_CFR, select=c("location_id", "year_id", 
                                             colnames(pred_CFR)[grepl("cfr_draw_", colnames(pred_CFR))]))

if (WRITE_FILES == "yes") {
  write.csv(CFR_draws_save, file.path(j.version.dir, paste0("09_CFR_draws.csv")), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART SEVEN: CALCULATE DEATHS #####################################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
# merge
predictions_deaths <- join(rescaled_cases, CFR_draws_save, by=c("location_id", "year_id"), type="inner")
#*********************************************************************************************************************** 


#----CALCULATE DEATHS---------------------------------------------------------------------------------------------------
for (ii in 0:999) {
  predictions_deaths[, paste0("death_draw_", ii)] <- 
            predictions_deaths[, paste0("cfr_draw_", ii)] * predictions_deaths[, paste0("case_draw_", ii)]
}

# save results
predictions_deaths_save <- predictions_deaths[, c("location_id", "year_id",
                                      colnames(predictions_deaths)[grep("death_draw", colnames(predictions_deaths))])]

if (WRITE_FILES == "yes") {
  write.csv(predictions_deaths_save, file.path(j.version.dir, "10_deaths_for_rescaling.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART EIGHT: RESCALE DEATHS #######################################################################################
########################################################################################################################


#----RESCALE------------------------------------------------------------------------------------------------------------
### run custom rescale function
rescale_GBD2016(input_file=predictions_deaths_save, measure="death")
rescaled_deaths <- rescaled_draws
#*********************************************************************************************************************** 


#----SAVE---------------------------------------------------------------------------------------------------------------
# save results
if (WRITE_FILES == "yes") {
  write.csv(rescaled_deaths, file.path(j.version.dir, "11_death_predictions_rescaled_for_split.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART NINE: DEATHS AGE-SEX SPLIT ##################################################################################
########################################################################################################################


#----SPLIT--------------------------------------------------------------------------------------------------------------
age_sex_split(acause=acause, input_file=rescaled_deaths, measure="death")    
split_deaths <- split_data_frame
#*********************************************************************************************************************** 


#----SAVE---------------------------------------------------------------------------------------------------------------
# save split draws
split_deaths_save <- subset(split_deaths, select=c("location_id", "year_id", "age_group_id", "sex_id", 
            colnames(split_deaths)[grep("split_death_draw_", colnames(split_deaths))]))

if (WRITE_FILES == "yes") {
  write.csv(split_deaths_save, file.path(j.version.dir, "12_split_death_draws.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART TEN: COMBINE CODEm DEATHS ##################################################################################
########################################################################################################################


#----COMBINE------------------------------------------------------------------------------------------------------------
### read CODEm COD results for data-rich countries
cod_M <- read_hdf5_table(file.path("FILEPATH"), key="data")
cod_F <- read_hdf5_table(file.path("FILEPATH"), key="data")

# combine M/F CODEm results
cod_DR_M <- subset(cod_M, select=c("location_id", "year_id", "age_group_id", "sex_id", colnames(cod_M)[grep("draw_", colnames(cod_M))]))
cod_DR_F <- subset(cod_F, select=c("location_id", "year_id", "age_group_id", "sex_id", colnames(cod_F)[grep("draw_", colnames(cod_F))]))
cod_DR <- rbind(cod_DR_M, cod_DR_F)

data_rich <- unique(cod_DR$location_id)
split_deaths_glb <- split_deaths_save[!split_deaths_save$location_id %in% data_rich, ]
colnames(split_deaths_glb) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))
split_deaths_hyb <- rbind(split_deaths_glb, cod_DR)

# keep only needed age groups
split_deaths_hyb <- split_deaths_hyb[split_deaths_hyb$age_group_id %in% c(age_start:age_end), ]

# save results
if (WRITE_FILES == "yes") {
  write.csv(split_deaths_hyb, file.path(j.version.dir, "14_split_death_draws_with_codem_data_rich.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART ELEVEN: FORMAT FOR CODCORRECT ###############################################################################
########################################################################################################################


#----SAVE RESULTS-------------------------------------------------------------------------------------------------------
### format for codcorrect
# death, measure_id==1
ids <- unique(split_deaths_hyb$location_id)
years <- unique(split_deaths_hyb$year_id)
sexes <- c(1,2)

# save .csv for location-year-sex
write <- function(x,y,z) { write.csv(split_deaths_hyb[split_deaths_hyb$location_id==x & split_deaths_hyb$year_id==y & split_deaths_hyb$sex_id==z, ], 
                                    file.path(cl.death.dir, paste0(x, "_", y, "_", z, ".csv"))) }
for (location_id in ids) {
  for (year in years) {
    for (sex in sexes) {
      write(x=location_id, y=year, z=sex)
    }
  }
}

print(paste0("whooping death draws saved in ", cl.death.dir))
#***********************************************************************************************************************

}