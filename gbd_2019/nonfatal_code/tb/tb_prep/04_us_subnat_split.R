## EMPTY THE ENVIRONMENT
rm(list = ls())

## ESTABLISH FOCAL DRIVES
os <- .Platform$OS.type
if (os=="windows") {
  j <-"J:/"
  h <-"H:/"
  k <-"K:/"
} else {
  j <-"/home/j/"
  h <-paste0("homes/", Sys.info()[7], "/")
  k <-"/ihme/cc_resources/"
}

## LOAD FUNCTIONS
library(writexl, lib.loc = paste0(j, "temp/jledes2/libraries/"))
source(paste0(k, "libraries/current/r/get_population.R"))
source(paste0(k, "libraries/current/r/get_age_metadata.R"))

#############################################################################################
###                                   DATA PREPARATION                                    ###
#############################################################################################

## HELPER FUNCTIONS
decomp_step  <- "iterative"
date         <- "2020_05_26"
bundle_dir   <- paste0(j, "WORK/12_bundle/tb/712/03_review/01_download/")
adj_data_dir <- paste0(j, "WORK/12_bundle/tb/712/01_input_data/04_crosswalk/", date, "/")

## GET DATA
all <- fread(paste0(bundle_dir, decomp_step, "_", date, "_download.csv"))
all <- all[(measure == "incidence") & (ihme_loc_id %like% "USA")]
all <- all[year_start %in% c(2013, 2018)]

## SUBSET COLUMNS
data <- all[, .(location_id, ihme_loc_id, location_name, year_start, year_end, sex, age_start, age_end, cases)]
data <- data[order(ihme_loc_id, year_start, sex, age_start)]

## SUBSET ROWS
subnat   <- data[year_start == 2013]
national <- data[year_start == 2018]

## COMPUTE SCALAR
subnat_agg <- subnat[, .(subnat_cases = sum(cases)), by = .(sex, age_start, age_end)]
national   <- merge(national, subnat_agg)
national[, ratio := subnat_cases/cases]

## COMPUTE SUBNATIONAL CASES
scalar <- national[, .(sex, age_start, age_end, ratio)]
subnat <- merge(subnat, scalar, by = c("sex", "age_start", "age_end"))
subnat[, new_cases := cases/ratio]

## CLEAN
new <- subnat[, .(location_id, ihme_loc_id, location_name, year_start, year_end, sex, age_start, age_end, new_cases)]
new <- new[order(ihme_loc_id, year_start, year_end, sex, age_start)]
new[, `:=` (year_start = 2018, year_end = 2018)]
setnames(new, old = "new_cases", "cases")

#############################################################################################
###                                  GET POPULATIONS                                      ###
#############################################################################################

## PULL POPLULATIONS
pops <- get_population(age_group_id = c(2:20, 30:32, 235),
                       location_id  = unique(new$location_id),
                       year_id      = 2018,
                       sex_id       = 1:2,
                       decomp_step  = "step4",
                       gbd_round_id = 6)

## GET AGE METADATA
ages <- get_age_metadata(age_group_set_id = 12, gbd_round_id=6)
ages <- ages[, .(age_group_id, age_group_years_start, age_group_years_end)]
setnames(ages, old = c("age_group_years_start", "age_group_years_end"), new = c("age_start", "age_end"))

## CREATE GROUPS TO AGGREGATE
for (myage in seq(5, 65, 10)) ages[age_start %in% c(myage, myage+5), indicator := myage]
for (myage in seq(65, 85, 5)) ages[age_start == myage, indicator := myage]
ages[age_group_id < 6,  indicator := 0]
ages[age_group_id > 31, indicator := 90]

## MERGE AGE METADATA AND AGGREGATE POPULATIONS
pops     <- merge(pops, ages)
pops_agg <- pops[, .(sample_size = sum(population)), by = .(location_id, sex_id, indicator)]

## PREP FOR MERGE
pops_agg[, `:=` (year_start = 2018, year_end = 2018)]
pops_agg[sex_id == 1, sex := "Male"][sex_id == 2, sex := "Female"]
pops_agg[, sex_id := NULL]
setnames(pops_agg, old = "indicator", new = "age_start")

## MERGE SAMPLE SIZE
new <- merge(new, pops_agg)

## COMPUTE SUMMARIES
new[, mean := cases/sample_size]
new[, standard_error := sqrt(cases)/sample_size]
new[, `:=` (lower = NA, upper = NA)]

#############################################################################################
###                                 MERGE REMAINING META-DATA                             ###
#############################################################################################

## MERGE EXTRA COLUMNS
national <- all[year_start == 2018]
national <- national[, .SD, .SDcols = -names(new)[!(names(new) %like% "age|year|sex")]]
new      <- merge(new, national, by = c("age_start", "age_end", "sex", "year_start", "year_end"))

## FIX COLUMNS
new[, note_modeler := "split to subnational level using 2013 pattern"]
new[, effective_sample_size := NA]
new[, uncertainty_type_value := NA]
new[, year_id := NULL]

## SAVE
write.csv(new, file = paste0(adj_data_dir, "us_subnat_split.csv"), row.names = F, na = "")

#############################################################################################
###                                        DONE                                           ###
#############################################################################################
