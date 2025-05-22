########################################################################
# Description:                                                         #
# Generate draws of case detection rate and use to calculate draws of  #
# undetected cases, total number of cases, and deaths. Output results  #
# to (1). Produce age/sex-specific estimates of incidence and deaths   #
# and split out by sequela. Output results to (2).                     #
#                                                                      #
# Output files:                                                        #
# (1) 02a_deathAndCaseDraws.rds                                        #
# (2) 02a_incAndDeathDraws.rds                                         #
########################################################################

### ========================= BOILERPLATE ========================= ###

rm(list=ls())
data_root <- "FILEPATH"
cause <- "ntd_afrtryp"

## Define paths 
params_dir  <- "FILEPATH"
draws_dir   <- "FILEPATH"
interms_dir <- "FILEPATH"
logs_dir    <- "FILEPATH"

##	Source relevant libraries
library(lme4)
library(arm)
library(tidyr)
library(dplyr)
library(readstata13)
source("FILEPATH/get_population.R")

## Define Constants
release_id = ADDRESS

### ========================= MAIN EXECUTION ========================= ###

## (1) Import case data
Data2Model <- readRDS(file = paste0("FILEPATH"))
Data2Model <- Data2Model[,.(location_id,year_id, cause_id,value_ppl_risk,value_coverage,
                            value_ln_coverage, reported_tgb, reported_tgr, total_reported,
                            region_id, location_name)][order(location_id, year_id)]


## (1)a Data manipulation prior to model fit

#179, 193, and 213 need rows for 1990-2023
rows <- Data2Model[location_id %in% c(179, 193, 213) & year_id==1989][rep(seq_len(3), each=35),]
rows$year_id <- rep(1990:2023, times=3)
Data2Model <- rbind(Data2Model, rows)

#185 needs 1990-2015
rows <- Data2Model[location_id==185 & year_id==1989][rep(1, each=26),]
rows$year_id <- rep(1990:2015)
Data2Model <- rbind(Data2Model, rows)

#209 needs 1990-2016
rows <- Data2Model[location_id==209 & year_id==1989][rep(1, each=27),]
rows$year_id <- rep(1990:2016)
Data2Model <- rbind(Data2Model, rows)

Data2Model <- Data2Model[order(location_id, year_id)]

# Drop years 1977-1979 
Data2Model <- Data2Model[! year_id %in% c(1977:1979)]

# Drop locs 175, 206, 210, and 216
Data2Model <- Data2Model[! location_id %in% c(175, 206, 210, 216)]

# Replace missings with zeros for certain locs
Data2Model <- Data2Model[is.na(total_reported) & location_id
                         %in% c(179, 185, 193, 209, 213, 217), total_reported := 0]

## (2) Smoothing of case data pre/post 1990
# Use a Poisson model to replace 0 case counts for years pre-1990 in
# order to smooth pre-1990 inputs (otherwise estimates are
# jagged pre/post 1990)

# Set aside rows for years 2019-2022 to be appended back in after
# fitting the Poisson model 
# Also set aside some locations with no value_ppl_risk that prevent
# the model from converging
leave_out <- Data2Model[year_id %in% 2023:2024 | location_id %in% c(179, 185, 193, 209, 213)]
Data2Model <- Data2Model[year_id < 2023 & ! is.na(value_ppl_risk)]

# Fit a poisson model for each location separately
for (location in unique(Data2Model$location_id)){
  # fit the model one location at a time
  m1 <- glm(total_reported ~ year_id, offset=log(value_ppl_risk), family="poisson", data=Data2Model[location_id==location])
  # extract the predicted value for all years
  predicted <- predict(m1)
  Data2Model[location_id == location, new_case_counts := exp(predicted)]
}

# Replace 0 case counts prior to 1990 with new_case_counts 
Data2Model[total_reported == 0 & year_id < 1990, total_reported := new_case_counts][, new_case_counts := NULL]

# Add held back observations back into the dataset
Data2Model <- rbind(setDT(Data2Model), leave_out)

Data2Model <- Data2Model[order(location_id, year_id)]
Data2Model[, incidence_risk := total_reported/value_ppl_risk]
Data2Model[, ln_inc_risk := ifelse(incidence_risk==0, NA, log(incidence_risk))]


## (3) Obtain uncertainty for draws of case detection rate (CDR)

# Fit a mixed-effects linear regression model
m2 <- lmer(ln_inc_risk ~ value_ln_coverage + (1|location_id), data=Data2Model,
           REML = FALSE) #matching to stata, which fits using ML (not REML) by default

# Save the random effects and their standard errors for each modeled location_id
random <- ranef(m2)
random_eff <- as.data.table(random)[, .(location_id = as.numeric(as.character(grp)), random = condval, randomSE = condsd)]
Data2Model <- merge(Data2Model, random_eff, by="location_id", all.x = TRUE)

# Calculate the variance of the random effects for only those observations used to fit the model
# (i.e. only the rows where neither value_ln_coverage nor ln_inc_risk is missing)
varRE <- var(Data2Model[! (is.na(value_ln_coverage) | is.na(ln_inc_risk)), random])

# Calculate the mean of the SEs of the random effects for only those observations used to fit the model
meanSE <- mean(Data2Model[! (is.na(value_ln_coverage) | is.na(ln_inc_risk)), randomSE])

# Generate an estimate for all missing values of randomSE. This value will be the same for all
# missing rows. We will use randomSE later on to generate uncertainty.
Data2Model[is.na(randomSE), randomSE := sqrt(varRE + meanSE^2)]


## (4) Impute estimates for missing values of incidence risk using rate of change

# Create a flag for missing incidence risk
Data2Model[, has_data:= ifelse(is.na(incidence_risk), 0, 1)]
# Sort
Data2Model <- Data2Model[order(location_id, has_data, year_id)]
# Calculate the rate of change of incidence risk 
Data2Model[, roc := (shift(incidence_risk, n=1,
                           type="lead")/incidence_risk)^(1/(shift(year_id, n=1, type="lead") - year_id)),
           by=.(location_id, has_data)]
# If roc = 0/0, then R sets roc=NaN. If roc=[any non-zero number]/0, then R sets roc=Inf
# Set NaN and Inf to NA
Data2Model[! is.finite(roc), roc:=NA]

#if roc is missing, inherit value of roc from previous year
Data2Model <- Data2Model[order(location_id, year_id)]
Data2Model <- Data2Model %>%
  group_by(location_id) %>%
  fill(roc)
Data2Model <- as.data.table(Data2Model)

# If incidence_risk is missing, calculate a value using previous year's incidence_risk and roc
Data2Model[, `:=` (lag_inc_risk = shift(incidence_risk, n=1, type="lag"), lag_roc = shift(roc, n=1, type="lag")), by=location_id]
Data2Model[year_id==2023, incidence_risk := lag_inc_risk * lag_roc]
Data2Model[location_id %in% c(208, 168, 173) & year_id==2023, incidence_risk := lag_inc_risk]
#2024
Data2Model[, `:=` (lag_inc_risk = shift(incidence_risk, n=1, type="lag"), lag_roc = shift(roc, n=1, type="lag")), by=location_id]
Data2Model[year_id==2024, incidence_risk := lag_inc_risk * lag_roc]
Data2Model[location_id %in% c(208, 168, 173) & year_id==2024, incidence_risk := lag_inc_risk]

# Drop the lagged vars
Data2Model[, `:=` (lag_inc_risk = NULL, lag_roc = NULL)]

## (5) Fill in missing values of total_reported
Data2Model[is.na(total_reported), total_reported := incidence_risk * value_ppl_risk]


## (6) Fill in gaps in population screening coverage (needed for prediction of case detection rate)
##     For locations without any coverage data, use regional average 
##     For regions without any coverage data, use average of regional averages (grand mean)
Data2Model <- Data2Model %>%
  group_by(location_id) %>%
  fill(value_ln_coverage) %>%
  fill(value_ln_coverage, .direction = "up")
Data2Model <- as.data.table(Data2Model)

# Calculate yearly regional averages
Data2Model[, mean_ln_cov := mean(value_ln_coverage, na.rm=T), by=.(region_id, year_id)]

# Calculate yearly average across all regions
Data2Model[, mean_mean_ln_cov := mean(mean_ln_cov, na.rm=T), by=.(year_id)]

# Replace missing values of value_ln_coverage, using regional means and grand means,
# where appropriate:
Data2Model[is.na(mean_ln_cov), mean_ln_cov := mean_mean_ln_cov]
Data2Model[is.na(value_ln_coverage), value_ln_coverage := mean_ln_cov]



## (7) Generate draws of mortality among treated cases
# We assume that 0.7% - 6.0% (95% CI) of all treated (reported) cases die
# (Source: GBD 2010, which refers to Balasegaram 2006, Odiit et al. 1997,
# and Priotto et al. 2009)
# Note: we assume 100% of untreated cases are fatal
sd_mort_treat <- (log(0.06) - log(0.007)) / (qnorm(0.975) * 2)
mu_mort_treat <- (log(0.06) + log(0.007)) / 2
mort_treated <- exp(rnorm(n=1000, mean = mu_mort_treat, sd = sd_mort_treat))


## (8) Generate 1000 draws of CDR given screening coverage

# Retrieve the beta coefficients (beta0 and beta1) from the regression that we fit earlier:
# ln_inc_risk ~ value_ln_coverage + (1|location_id)
m <- summary(m2)$coefficients[,1]
# Retrieve the var-covar matrix from the regression ln_inc_risk ~ value_ln_coverage + (1|location_id)
C <- vcov(m2)
# Draw from a multivariate normal distribution to simulate 1000 values of the beta coefs
betas <- mvrnorm(n=1000, mu=m, Sigma = C)

#    The counterfactual log-incidence risk is calculated as follows:
#    ln_inc_counterfact = ln_inc_risk - ln_coverage * b_ln_coverage[j]
#
#    The case detection rate is calculated as follows:
#    cdr = observed cases / (observed + unobserved)
#    cdr = # of reported cases / total # of cases
#    cdr = inc_risk/inc_counterfact
#    cdr = exp(ln_inc_risk)/exp(ln_inc_counterfact)
#    cdr = exp(ln_inc_risk - ln_inc_counterfact)
#
#    Now substitute in for ln_inc_counterfact
#    cdr = exp(ln_inc_risk - (ln_inc_risk - ln_coverage * b_ln_coverage[j]))
#    cdr = exp(ln_inc_risk - ln_inc_risk + ln_coverage * b_ln_coverage[j])
#    cdr = exp(ln_coverage * b_ln_coverage[j])
#    
#    We will use this final expression to generate 1000 draws of cdr using
#    ln_coverage and draws of beta1


# Generate 1000 draws of CDR per location-year from the 1,000 betas
A <- as.matrix(Data2Model[,"value_ln_coverage"]) #956x1 matrix containing ln(coverage) data
B <- t(as.matrix(betas[ ,"value_ln_coverage"])) #1x1000 matrix containing simulated beta1's
cdr_draws <- as.data.table(exp(A %*% B)) #956 x 1000 matrix containing 1000 draws of cdr per row

setnames(cdr_draws, c(paste0("cdr_", 0:999)))
draws <- cbind(cdr_draws, total_reported = Data2Model$total_reported, randomSE = Data2Model$randomSE)


## (9) Using CDR, generate draws of:
## a. undetected cases
## b. total cases
## c. deaths

# (a) Undetected Cases
# undetected = total - reported  --> we can estimate the total number of cases using CDR
# undetected = (reported/cdr) - reported
# Incorporate additional uncertainty to the draws as well
draws[, paste0("undetected_", 0:999) := lapply(0:999, function(x)
  (get("total_reported")/get(paste0("cdr_", x)) - get("total_reported")) * exp(rnorm(n=1, mean=randomSE^2/-2, sd=randomSE)) )]

# (b) Total Cases
# total cases = undetected + reported
draws[, paste0("total_", 0:999) := lapply(0:999, function(x)
  get("total_reported") + get(paste0("undetected_", x)) )]

# (c) Deaths
# Assume that all undetected cases are fatal
# Thus, deaths = undetected + (reported * mort_treated)
draws[, paste0("deaths_", 0:999) := lapply(0:999, function(x)
  get(paste0("undetected_", x)) + get("total_reported") * mort_treated[x+1] )]


## (10) Clean the dataset and save 
##      all-age draws of cases and deaths

draws[,c("total_reported", "randomSE") := NULL]
all_ages <- cbind(Data2Model, draws)
all_ages <- all_ages[, c("location_name", "location_id", "region_id", "year_id", "total_reported",
                         paste0("deaths_", 0:999), paste0("undetected_", 0:999), paste0("cdr_", 0:999),
                         paste0("total_", 0:999))]

#calculate means across draws
all_ages[, cdrMean := rowMeans(.SD), .SDcols = paste0("cdr_", 0:999)]
all_ages[, undetectedMean := rowMeans(.SD), .SDcols = paste0("undetected_", 0:999)]
all_ages[, deathsMean := rowMeans(.SD), .SDcols = paste0("deaths_", 0:999)]

# save intermediate file
saveRDS(all_ages, file = paste0("FILEPATH"))


## (11) Produce age-sex-specific estimates of cases and deaths

# (a) Merge in the ageSexCurve
ageSexCurve <- readRDS(file = paste0("FILEPATH"))
ageSexCurve <- as.data.frame(ageSexCurve)
all_ages <- as.data.frame(all_ages)
hat <- merge(all_ages, ageSexCurve, all=TRUE) 
hat <- as.data.table(hat)
# drop locations 179, 193, 213 (this should be done in step 1)
hat <- hat[! location_id %in% c(179,193,213)]

# (b) Get age-sex-specific population data
locations <- unique(hat$location_id)
years <- unique(hat$year_id)
age_groups <- unique(hat$age_group_id)

pops <- get_population(age_group_id=age_groups , location_id=locations,
                       year_id=(years), sex_id=c(1,2), release_id=release_id)
pops[, run_id := NULL]

hat <- dplyr::inner_join(hat, pops, by=c("location_id", "age_group_id", "sex_id", "year_id"))
hat <- as.data.table(hat)

setcolorder(hat, c("location_id", "location_name", "year_id",
                   "age_group_id", "ageSexCurve", "ageSexCurveSe", "sex_id",
                   "population"))
hat <- hat[order(location_id, year_id, sex_id, age_group_id)]

# save intermediate file to avoid having to perform the above merge multiple times
saveRDS(hat, file=paste0("FILEPATH"))

# (c) Apply age-sex-split
# Estimate number of cases per location, year, age, and sex
# Zero out estimates for ages below 2 years
hat[, temp1:= ifelse(age_group_id %in% c(2, 3, 388, 389, 238), 0,
                     exp(rnorm(n=.N, mean=ageSexCurve, sd=ageSexCurveSe)) * population)]

# total number of cases per location, year
hat[ , temp2 := sum(temp1), by=.(location_id, year_id)]

# update number of deaths based on the proportion of cases occuring in each age/sex group
hat[, paste0("deaths_", 0:999) := lapply(0:999, function(x)
  get(paste0("deaths_", x)) * temp1/temp2 )]

# update number of undetected cases in the same way
hat[, paste0("undetected_", 0:999) := lapply(0:999, function(x)
  get(paste0("undetected_", x)) * temp1/temp2 )]

# update total number of cases in the same way
hat[, paste0("total_", 0:999) := lapply(0:999, function(x)
  get(paste0("total_", x)) * temp1/temp2 )]

# drop temp1 and temp2
hat[, c("temp1", "temp2") := NULL]


## (12) Produce age/sex-specific draws of incidence

# Generate 1000 draws of incidence by dividing total number of cases by population size
hat[, paste0("inc_", 0:999) := lapply(0:999, function(x)
  get(paste0("total_",x))/population )]


## (13) Produce draws of incidence by sequelae

# First, generate 1000 draws of the splitting proportion for the 2 sequela:
# (1) Severe motor and cognitive impairment due to sleeping disorder
# (2) Disfiguring skin disease
# Assume an approx 70/30 split based on GBD 2010, which refers to Blum et al. 2006,
# who reports on the presence of symptoms at time of admission to treatment centers.

A1 <- ADDRESS1       ## number of cases with sleeping disorder (Blum et al 2006)
A2 <- ADDRESS2 - A1  ## number of cases without sleeping disorder 

a1 <- rgamma(n=1000, shape=A1, scale=1)
a2 <- rgamma(n=1000, shape=A2, scale=1)
prop_sleep <- a1/(a1 + a2)
# The above implies that the proportion of cases with sleeping disorder follows
# a beta distribution. Specifically, prop_sleep ~ beta(positives, negatives)


# Second, calculate draws of the number of incident cases for each sequela,
# using draws of splitting proportion. Assume that the same splitting
# proportion applies among treated and untreated cases.

# Generate draws of incidence of sleep disorder
# incidence of sleeping disorder = total incidence x prop with sleeping disorder
hat[, paste0("inc_sleep_", 0:999) := lapply(0:999, function(x)
  get(paste0("inc_",x)) * prop_sleep[x+1])]

# Genreate draws of incidence of skin disfigurement
# incidence of disfigurement = total incidence - incidence of sleeping disorder
hat[, paste0("inc_disf_", 0:999) := lapply(0:999, function(x)
  get(paste0("inc_",x)) - get(paste0("inc_sleep_",x)) )]


## (14) Clean the dataset and save
##      age-sex-specific draws of incidence by sequela and deaths

# Subset to desired variables
cols <- c("location_id", "year_id", "age_group_id", "sex_id", #"population",
          paste0("deaths_", 0:999),     #need for mortality
          paste0("undetected_", 0:999), #need for prevalence because duration differs by detected vs undetected cases
          paste0("inc_", 0:999),        #need for total incidence
          paste0("inc_sleep_", 0:999),  #need for sequela-specific incidence
          paste0("inc_disf_", 0:999))   #need for sequela-specific incidence
hat <- hat[, ..cols]

# Save results
saveRDS(hat, file = paste0("FILEPATH"))
