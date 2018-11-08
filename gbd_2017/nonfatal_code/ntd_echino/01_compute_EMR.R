# calculate EMR for DisMod

#================
# program set-up
#================

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
} else {
  j <- "FILEPATH"
}

gbd <- "gbd2017" # gbd version
date <- Sys.Date() # time stamp of current run
envir <-

localRoot <- "FILEPATH" #local root
clusterRoot <- dir.create("FILEPATH")

code_dir <- paste0(localRoot, "/01_code") # directory for code
in_dir <- paste0(localRoot, "/02_inputs") #directory for external inputs
tmp_dir <- paste0(clusterRoot, "/tmp") # directory for temporary outputs on cluster
local_tmp_dir <- paste0(localRoot, "/04_outputs") #local temporary directory for other things:
out_dir <- paste0(clusterRoot, "/outputs") #directory for output of draws > on ihme/scratch
gbd_function <- "FILEPATH"

mvidF <- 427922
mvidM <- 428105


# shared functions
source(paste0(gbd_function, "get_epi_data.R"))
source(paste0(gbd_function, "get_demographics.R"))
source(paste0(gbd_function, "get_location_metadata.R"))
source(paste0(gbd_function, "get_age_metadata.R"))
source(paste0(gbd_function, "get_population.R"))
source(paste0(gbd_function, "get_ids.R"))
source(paste0(gbd_function, "get_draws.R"))

#=================================
# clean incidence data from bundle
#=================================
incidence <- get_epi_data(bundle_id = 60)
incidence <- incidence[measure == "incidence"]
incidence <- incidence[is_outlier == 0,]

# build cleaner dataset w/ columns needed for calculations
incidence <- data.table(location_id = incidence$location_id,
                        sex = incidence$sex,
                        year_start = incidence$year_start,
                        year_end = incidence$year_end,
                        age_start = incidence$age_start,
                        age_end = incidence$age_end,
                        age_demographer = incidence$age_demographer,
                        mean = incidence$mean,
                        lower = incidence$lower,
                        upper = incidence$upper,
                        group_review = incidence$group_review,
                        is_outlier = incidence$is_outlier)

# recode sex variables to sex_ids, not strings
incidence[sex == "Both", sex_id := 3]
incidence[sex == "Male", sex_id := 1]
incidence[sex == "Female", sex_id := 2]

# recode years
incidence <- incidence[, year_id := round((year_start + year_end) / 2)] # take average of year start/end

#==============================================================================
# get age intervals to use to match with death counts later & create unique IDs
#==============================================================================

incidence[, age_start := round(age_start)] # round age start
incidence[, age_end := round(age_end)] # round age end

# assign a unqique identifier number to each unique age interval
incidence[, ID := .GRP, by = .(age_start, age_end)]

#===============================================================================
# get unique age intervals present in echino incidence data and expand by number
# of years to get single_age years in long form
#==============================================================================
echino_ages_table <- unique(incidence[, c("location_id", "year_id", "sex_id", "age_start", "age_end", "ID")]) # keep unique age-location-sex combos

echino_ages_table[, interval_length := age_end-age_start+1]
echino_ages_table <- echino_ages_table[rep(1:nrow(echino_ages_table), echino_ages_table$interval_length)] # one row per count from age range
# get single year age_start, age_end
echino_ages_table[, count := 1:.N, by = c("location_id", "year_id", "sex_id", "ID")]
echino_ages_table[, age_single := (age_start + count - 1)]

echino_ages_table[, c("count", "interval_length") := NULL]

#===================================================================================
# build ages table to use as key with merging with incidence data and gbd ages later
#===================================================================================
demographics <- get_demographics(gbd_team = "ADDRESS")
gbd_ages <- demographics$age_group_id # gbd ages
gbd_years <- demographics$year_id # gbd years
gbs_sexes <- demographics$sex_id # gbd sexes

# Get single year age groups intable: need this to request population for each
single_ages_table <- data.table(age_single = 0:99,
                                age_start = 0:99,
                                age_end = 0:99)

single_ages_table[, age_group_id := age_single + 48]
single_ages_table[age_group_id == 48, age_group_id := 28]

# Create table of all ages needed for gbd estimates and their age_start and age_end
ages <- as.data.table(get_ids("age_group")) # get all age group ids
ages <- ages[age_group_id %in% gbd_ages,]
ages[age_group_id == 2, age_start :=  0]
ages[age_group_id == 2, age_end :=  0.019178]
ages[age_group_id == 3, age_start :=  0.019178]
ages[age_group_id == 3, age_end :=  0.076712]
ages[age_group_id == 4, age_start :=  0.076712]
ages[age_group_id == 4, age_end :=  1]
ages[age_group_id %in% c(5:32), age_start := gsub(" .*", '', age_group_name)]
ages[age_group_id %in% c(5:32), age_end := gsub("*.* ", '', age_group_name)]
ages[age_group_id == 235, age_start :=  95]
ages[age_group_id == 235, age_end :=  100]

# append single age ids and gbd age ids to get complete ages skeleton
ages_table <- as.data.table(dplyr::bind_rows(single_ages_table, ages))
ages_table[, age_group_name := NULL]

#===================================
# pull population data for later use
#===================================

# single age group population estimates
population_single <- get_population(location_id = unique(incidence$location_id),
                                    sex_id = c(1, 2, 3),
                                    year_id = min(incidence$year_id):max(incidence$year_id),
                                    age_group_id = single_ages_table$age_group_id,
                                    single_year_age = TRUE,
                                    gbd_round_id = 5)

# get population data for gbd age group ids
population_agg <- get_population(location_id = unique(incidence$location_id),
                                 sex_id = c(1, 2, 3),
                                 year_id = min(incidence$year_id):max(incidence$year_id),
                                 age_group_id = ages$age_group_id,
                                 gbd_round_id = 5)

# append population data for single age and gbd age groups to merge
population_table <- as.data.table(dplyr::bind_rows(population_single, population_agg))
population_table[, run_id := NULL]

# merge population with age table constructed earlier
population_skeleton <- merge(ages_table, population_table, by = "age_group_id") #now have table with population and age_start/end

#==================================
# prep and merge population weights
#==================================

#==========================
# prep and merge death data
#==========================

male_death_draws <- get_draws(gbd_id_type = "cause_id", gbd_id = 353, measure_id = 1,
                              location_id = unique(incidence$location_id),  year_id = min(incidence$year_id):max(incidence$year_id),
                              version_id = mvidM, source = "ADDRESS")

female_death_draws <- get_draws(gbd_id_type = "cause_id", gbd_id = 353, measure_id = 1,
                                location_id = unique(incidence$location_id),  year_id = min(incidence$year_id):max(incidence$year_id),
                                version_id = mvidF, source = "ADDRESS")

# append draws for males and females
death_draws <- as.data.table(dplyr::bind_rows(male_death_draws, female_death_draws))

# merge deaths with population data
death_draws <- merge(death_draws, population_skeleton, by = c("location_id", "sex_id", "year_id", "age_group_id"))

# aggregate death draws
draw_names <- c()
for(i in 0:999){
  draw_names <- c(draw_names, paste0("draw_", i)) # create list of column names to aggregate
}

death_draws[, mean_death := apply(.SD, 1, mean), .SDcols = draw_names]


#===================================
# calculating CSMR in GBD age groups
#===================================
# CSMR for "Males" and "Females"
CSMR <- data.table(location_id = death_draws$location_id,
                   sex_id = death_draws$sex_id,
                   year_id = death_draws$year_id,
                   age_group_id = death_draws$age_group_id,
                   deaths = death_draws$mean_death,
                   population = death_draws$population)

# need to calculate CSMR for "Both" sexes (i.e. sex_id = 3)
CSMR_both_sex <- data.table(location_id = death_draws$location_id,
                            sex_id = death_draws$sex_id,
                            year_id = death_draws$year_id,
                            age_group_id = death_draws$age_group_id,
                            deaths = death_draws$mean_death,
                            population = death_draws$population)

CSMR_both_sex[, deaths_both := sum(deaths), by = c("location_id", "age_group_id", "year_id")] # get deaths for both sexes
CSMR_both_sex[, population_both := sum(population), by = c("location_id", "age_group_id", "year_id")]
CSMR_both_sex[, c("deaths", "population") := NULL]
setnames(CSMR_both_sex, "deaths_both", "deaths") # rename death column to match with CSMR table
setnames(CSMR_both_sex, "population_both", "population") # rename death column to match with CSMR table
CSMR_both_sex[, sex_id := 3]
CSMR_both_sex <- unique(CSMR_both_sex)

# combine sex-specific and both sex data needed for CSMR calculation
CSMR <- dplyr::bind_rows(CSMR, CSMR_both_sex)
CSMR <- merge(CSMR, ages_table, by = "age_group_id")
CSMR[, csmr := deaths/population]
CSMR[, age_single := NULL]

#=====================================================
# merge age_weights onto CSMR to get weighted averages
#=====================================================

# Get age weight to calculate age-standardized death rate
age_weights <- get_age_metadata(age_group_set_id = 12)

# merge 1:1 age_group_id using "`local_tmp_dir'/ages.dta", assert(3) nogenerate
CSMR <- merge(CSMR, age_weights, by = "age_group_id")

#============================================================
# make combinations of CSMR age groups to population weighted
# average to align with incidence systematic review data
#===========================================================

# filtering incidence to get only data points that match with combo of gbd age groups
incidence <- incidence[age_start %in% CSMR$age_start, ]
incidence <- incidence[age_end %in% CSMR$age_end, ]

# we know that all of these incidence rows match some combo of gbd age start/end now
for (i in 1:length(incidence$location_id)) {
  # subset CSMR to location in row trying to match
  filtered_CSMR <- CSMR[location_id == incidence$location_id[i] & sex_id == incidence$sex_id[i] & year_id == incidence$year_id[i],]
  age_id_start <- filtered_CSMR[age_start == incidence$age_start[i],]$age_group_id
  age_id_end <-filtered_CSMR[age_end == incidence$age_end[i],]$age_group_id
  rows_to_aggregate <- subset(filtered_CSMR, age_group_id %in% age_id_start:age_id_end)
  rows_to_aggregate$weighted_CSMR <- rows_to_aggregate$csmr * rows_to_aggregate$age_weight
  incidence$csmr[i] <- sum(rows_to_aggregate$weighted_CSMR)
}


#===============
# calculate EMR
#===============
# calculate EMR as EMR=CSMR/(incidence * average duration). For CE assume remission of 0.15 to 0.25 i.e 2-6.7years(average = 0.2 i.e 5years)
incidence[, EMR := csmr / (mean*5)]
