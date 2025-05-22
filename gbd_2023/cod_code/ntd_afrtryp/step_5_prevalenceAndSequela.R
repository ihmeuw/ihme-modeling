##########################################################################
# Description: Create non-fatal draws file containing draws of incidence #
# and prevalence by location, year, age, sex, species and sequela.       #
#                                                                        #
##########################################################################

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
source("FILEPATH/get_population.R")

## Define Constants
release_id <- ADDRESS

### ========================= MAIN EXECUTION ========================= ###

### ------ PART 1: PREVALENCE ------ ###
## (1) Import case draws
all_ages <- readRDS(file = paste0(interms_dir,"FILEPATH"))

# Drop vars
cols_to_drop <- c(grep("deaths_", names(all_ages), value=T),
                  grep("cdr_", names(all_ages), value=T),
                  paste0("total_", 0:999), #ensures that total_reported is retained
                  "cdrMean", "undetectedMean", "deathsMean")
all_ages[, (cols_to_drop) := NULL]


## (2) Merge with speciesSplit (left join)
speciesSplit <- readRDS(file=paste0(interms_dir,"FILEPATH"))
all_ages <- merge(all_ages, speciesSplit, by=c("location_id", "year_id"), all.x = TRUE)

# Drop locations
all_ages <- all_ages[! location_id %in% c(175, 206, 210, 216, 213, 139, 217, 193, 179)]


## (3) Generate 1000 draws of the splitting proportion for the 2 sequela among detected cases:
# * Disfiguring skin disease (skin)
# * Severe motor and cognitive impairment due to sleeping disorder (sleep/neuro)

# Assume that approx 70-74% of cases will have developed neurological symptoms by the time
# the case is detected. This split is based on GBD 2010, which refers to Blum et al. 2006,
# who reports on the presence of neuro symptoms at time of admission to treatment centers.

A1 <- ADDRESS1       ## number of detected cases with sleeping disorder (Blum et al 2006)
A2 <- ADDRESS2 - A1  ## number of detected cases without sleeping disorder 
a1 <- rgamma(n=1000, shape=A1, scale=1)
a2 <- rgamma(n=1000, shape=A2, scale=1)
prop_sleep <- a1/(a1 + a2)

# The above implies that the proportion of cases with sleeping disorder follows
# a beta distribution. Specifically, prop_sleep ~ beta(positives, negatives)


# A NOTE ON DURATION:
#
# Species g:
# Untreated: 3 years (then fatal)
# Treated: 6 months (note: there is limited data on post-treatment period, but
# this seems reasonable given that treatment involves hospitalization; it's very
# hard to find a definitive source as it partly depends on the stage of disease
# at the time of treatment)
#
# Species r:
# Untreated: 6 months (then fatal)
# Treated: 6 months  (same caveats as above)


## (4) Generate 1000 draws of duration for each species separately among UNTREATED
# cases (based on Checci 2008 BMC Inf Dis). 

# Species G
mean_g <- 1026 / 365  # avg duration in years = 2.8 years (then fatal)
lower_g <- 702 / 365
upper_g <- 1602 / 365
sd_g <- (log(upper_g) - log(lower_g))/(qnorm(0.975)*2)
mu_g <- log(mean_g) - 0.5*(sd_g^2) #the mean of a log-normal distribution = exp(mu + 0.5*sigma^2)
duration_g <- exp(rnorm(n=1000, mean=mu_g, sd=sd_g))

# Species R
mean_r <- 0.5 # assume 6-month duration (then fatal)
sd_r = sd_g * mean_r / mean_g
mu_r = log(mean_r) - 0.5*sd_r^2  # the mean of a log-normal distribution = exp(mu + 0.5*sigma^2)
duration_r <- exp(rnorm(n=1000, mean=mu_r, sd=sd_r))


## (5) Generate 1000 draws of prevalence for each species and sequela. 
# To do this, we must handle treated and untreated cases separately, because they exhibit differing
# durations of each sequela. Duration also varies by species.

# Among treated cases of either species, we assume a total duration of 6 months between symptom onset
# and treatment. Further, we assume that approx 70% of cases will have developed sleep disorder by the
# time of detection and that these symptoms will have been present for half the total duration (i.e. 3
# months) and the remaining half (3 months) spent suffering from skin disorder. For those who don't
# develop sleeping disorder, we assume that the entire duration (i.e. 6 months) was spent with skin disorder.

# Among untreated cases, we assume half of the total duration was spent with sleeping disorder and the
# other half with skin disfigurement. For species r, this means approx 3 months of each. For species
# g, this means 1.5 years spent with each.

# (a) Calculate 1000 draws of # of prevalent cases for each species based on incident cases

# species g (detected only - will add in undetected later)
all_ages[ , paste0("prev_total_g_", 0:999) := lapply(0:999, function(x)
  0.5 * total_reported * pr_g )]

# species r (detected + undetected)
all_ages[ , paste0("prev_total_r_", 0:999) := lapply(0:999, function(x)
  0.5 * (total_reported + get(paste0("undetected_",x))) * pr_r  )]

# (b) Calculate 1000 draws of # of prevalent cases by species and sequela

# species g, sleeping disorder (detected only)
all_ages[ , paste0("prev_sleep_g_", 0:999) := lapply(0:999, function(x)
  prop_sleep[x+1] * get(paste0("prev_total_g_",x)) * 0.5 )]

# species g, disfigurement (detected only)
all_ages[ , paste0("prev_disf_g_", 0:999) := lapply(0:999, function(x)
 get(paste0("prev_total_g_",x)) - get(paste0("prev_sleep_g_",x)) )]

# species r, sleeping disorder
all_ages[ , paste0("prev_sleep_r_", 0:999) := lapply(0:999, function(x)
  prop_sleep[x+1] * get(paste0("prev_total_r_",x)) * 0.5 )]

# species r, disfigurement
all_ages[ , paste0("prev_disf_r_", 0:999) := lapply(0:999, function(x)
  get(paste0("prev_total_r_",x)) - get(paste0("prev_sleep_r_",x)) )]

# (c) Special handling for species g to account for undetected cases

# initialize years to allow for calculation of prevalence based on a multi-year duration
all_ages <- rbind(setDT(all_ages),
              all_ages[year_id==2024,][, year_id := year_id + 1],
              all_ages[year_id==2024,][, year_id := year_id + 2],
              all_ages[year_id==2024,][, year_id := year_id + 3],
              all_ages[year_id==2024,][, year_id := year_id + 4],
              all_ages[year_id==2024,][, year_id := year_id + 5],
              all_ages[year_id==2024,][, year_id := year_id + 6],
              all_ages[year_id==2024,][, year_id := year_id + 7]
              )[order(location_id, year_id)]

for (y in 0:999){
  
  # Define duration variables for the given draw
  full_year <- floor(duration_g)[y+1]
  remainder <- duration_g[y+1] - full_year
  lead_vars <- full_year + 1  #define number of lead columns that need to be created
  
  # Create lead columns
  cols <- paste0("undet_lead_", seq(lead_vars))
  all_ages[, (cols) := shift(.SD, seq(lead_vars), type="lead"), .SDcols=paste0("undetected_",y), by=location_id]
  
  # Sum undetected cases across full years
  if (full_year > 0){
    all_ages[, undet_sum := rowSums(.SD), .SDcols=paste0("undet_lead_",seq(full_year))]
  } else {
    all_ages[, undet_sum := 0]
  }
  
  # Calculate # undetected cases for fractional year
  all_ages[, undet_frac := get(paste0("undet_lead_",lead_vars)) * remainder]
  
  # Calculate total number of undetected cases attributable to species g
  all_ages[ , undet_tot := (undet_sum + undet_frac) * pr_g]
  
  # Update prevalent case count (reported + undetected)
  all_ages[ , paste0("prev_total_g_",y) := get(paste0("prev_total_g_",y)) + undet_tot] 
  all_ages[ , paste0("prev_sleep_g_", y) := get(paste0("prev_sleep_g_",y)) + (undet_tot * 0.5)]
  all_ages[ , paste0("prev_disf_g_", y) := get(paste0("prev_disf_g_",y)) + (undet_tot * 0.5)]
  
  # Delete lagged columns
  cols_to_drop <- grep("undet_", names(all_ages), value=T)
  all_ages[, (cols_to_drop) := NULL]

}

# Drop years
all_ages <- all_ages[year_id <= 2024]


## (6) Subset vars (drop draws of undetected_i)
cols_to_keep <- c("location_id", "year_id", grep("prev_", names(all_ages), value=T))
all_ages <- all_ages[, .SD, .SDcols=cols_to_keep]


## (7) Merge all_ages file with the ageSexCurve data file
ageSexCurve <- readRDS(file = paste0(interms_dir,"FILEPATH"))
ageSexCurve <- as.data.frame(ageSexCurve)
all_ages <- as.data.frame(all_ages)
prev <- merge(all_ages, ageSexCurve, all=TRUE) 
prev <- as.data.table(prev)


## (8) Retrieve population data for all loc/year/age/sex groups 
locations <- unique(prev$location_id)
years <- unique(prev$year_id)
age_groups <- unique(prev$age_group_id)
pops <- get_population(location_id = locations,
                       year_id = years,
                       age_group_id = age_groups,
                       sex_id = c(1,2),
                       release_id = release_id)

# inner join
prev <- dplyr::inner_join(prev, pops, by=c("location_id", "age_group_id", "sex_id", "year_id"))
prev <- as.data.table(prev)

## (9) Age and sex splitting

# Simulate number of cases per location, year, age, and sex
prev[, temp1:= ifelse(age_group_id %in% c(2,3,388,389,238), 0,
                     exp(rnorm(n=.N, mean=ageSexCurve, sd=ageSexCurveSe)) * population)]

# Total number of cases per location, year
prev[ , temp2 := sum(temp1), by=.(location_id, year_id)]

# Age/sex splitting for species g
prev[, paste0("prev_total_g_",0:999) := lapply(0:999, function(x)
  get(paste0("prev_total_g_",x)) * (temp1/temp2) / population )]

prev[, paste0("prev_sleep_g_",0:999) := lapply(0:999, function(x)
  get(paste0("prev_sleep_g_",x)) * (temp1/temp2) / population )]

prev[, paste0("prev_disf_g_",0:999) := lapply(0:999, function(x)
  get(paste0("prev_disf_g_",x)) * (temp1/temp2) / population )]

# Age/sex splitting for species r
prev[, paste0("prev_total_r_",0:999) := lapply(0:999, function(x)
  get(paste0("prev_total_r_",x)) * (temp1/temp2) / population )]

prev[, paste0("prev_sleep_r_",0:999) := lapply(0:999, function(x)
  get(paste0("prev_sleep_r_",x)) * (temp1/temp2) / population )]

prev[, paste0("prev_disf_r_",0:999) := lapply(0:999, function(x)
  get(paste0("prev_disf_r_",x)) * (temp1/temp2) / population )]

# Drop temp1 and temp2
prev[, c("temp1", "temp2") := NULL]


## (10) Clean up
names(prev) <- sub("prev_", "", names(prev))
prev[, measure_id := 5]



### ------  PART 2: INCIDENCE ------ ###
## (11) Import incidence data
inc <- readRDS(file = paste0(interms_dir,"FILEPATH"))
cols <- c("location_id", "age_group_id", "sex_id", "year_id",
          paste0("inc_", 0:999),
          paste0("inc_sleep_", 0:999),
          paste0("inc_disf_", 0:999))
inc <- inc[, ..cols]


## (12) Merge with speciesSplit
inc <- dplyr::left_join(inc, speciesSplit, by=c("location_id", "year_id"))
inc <- as.data.table(inc)

# drop locations - subsetting of locations should be occuring in step 1
inc <- inc[! location_id %in% c(175, 206, 210, 216, 193, 213, 217, 179)]


## (13) Calculate draws of incidence for each species and sequela 

#species g, total
inc[, paste0("total_g_",0:999) := lapply(0:999, function(x)
  get(paste0("inc_",x)) * pr_g )]

# species g, sleeping disorder
inc[, paste0("sleep_g_",0:999) := lapply(0:999, function(x)
  get(paste0("inc_sleep_",x)) * pr_g )]

# species g, disfigurement
inc[, paste0("disf_g_",0:999) := lapply(0:999, function(x)
  get(paste0("inc_disf_",x)) * pr_g )]

# species r, total
inc[, paste0("total_r_",0:999) := lapply(0:999, function(x)
  get(paste0("inc_",x)) * pr_r )]

# species r, sleeping disorder
inc[, paste0("sleep_r_",0:999) := lapply(0:999, function(x)
  get(paste0("inc_sleep_",x)) * pr_r )]

# species r, disfigurement
inc[, paste0("disf_r_",0:999) := lapply(0:999, function(x)
  get(paste0("inc_disf_",x)) * pr_r )]


## (14) Prepare for appending
# drop vars
cols_to_drop <- grep("inc_", names(inc), value=T)
inc[, (cols_to_drop) := NULL]

# Create measure_id var (6 = incidence)
inc[, measure_id := 6]


## (15) Append prevalence and incidence draw files
# ensure both files have same vars before appending
cols <- c("location_id", "year_id", "sex_id", "age_group_id", "measure_id",
          paste0("total_g_",0:999), paste0("sleep_g_",0:999), paste0("disf_g_",0:999),
          paste0("total_r_",0:999), paste0("sleep_r_",0:999), paste0("disf_r_",0:999))

prev <- prev[, ..cols]
inc <- inc[, ..cols]
nf_draws <- rbind(inc, prev, fill=TRUE)


## (16) Clean up and save
# Keep desired years for reporting
nf_draws <- nf_draws[year_id %in% c(1990, 1995, 2000, 2005, 2010, 2015, 2020, 2022, 2023, 2024)]

# Save results
saveRDS(nf_draws, file = paste0(interms_dir,"FILEPATH"))
