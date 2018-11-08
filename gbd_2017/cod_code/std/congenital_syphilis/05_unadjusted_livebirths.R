#'####################################`INTRO`##########################################
#' @author: 
#' 7/3/18 - refactoring 2/8 07 script
#' @purpose: By multiplying the age-specific fertility rate by the population (females of child-bearing age),
#'           we can calculate the total number of livebirths for a location-year. Multiplying this by the 
#'           seroprevalence of syphilis (from the early syphilis dismod model) gives the number of live births
#'           at risk for syphilis, unadjusted by the increased risk of fetal loss in those with syphilis 
#'           infections.
#'          @1 Calculate total livebirths
#'          @2 Interpolate the early syphilis dismod model
#'          @3 Calculate livebirths at risk of congenital syphilis, unadjusted for fetal loss
#'          @4 Split unadjusted livebirths into those from women with early/late syphilis
#'          @5 Save as a csv
#'          @6 Source next step: `06_adjusted_livebirths.R`
#'
#' @outputs: unadjusted livebirths as csv at `EX/{location_id}.csv`
#'
#'####################################`INTRO`##########################################

library("ihme", lib.loc = "FILEPATH")
ihme::setup()

library(magrittr)
library(dplyr)
library(readxl)

source_functions(get_covariate_estimates = T, get_population = T, interpolate = T)

# Read in parameters ------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
out_dir     <- args[1]
root_dir    <- args[2]
location_id <- args[3]
root_dir    <- args[4]
# location_id <- 10 # cambodia as example


source(paste0(root_dir, "cs_functions.R"))

# Calculate live births from uninfected women ----------------------------------------------
# Calculate the number of livebirths at risk of congenital syphilis. Historically, this was simply
#          a multiplication of age-specific fertility rate and population (by age/year/location ofc),
#          but that doesn't take into account the increased risk of adverse pregnancy outcomes (APO)
#          seen in syphilitic women, namely an increase in miscarriages and fetal loss that would lower
#          the actual number of livebirths. So later we work in the proportion of women with early/late syphilis
#          and the differential fetal loss rates.

message(paste(Sys.time(), "Calculate live births from uninfected women for location_id", location_id))

# get the estimate for age-specific fertility rate
# and grab data for women of child-bewaring age
asfr_id       <- 13
female_id     <- 2
age_group_ids <- 6:16 # 5 - 59 (5-year groups). Both end age groups should be 0 but I'm just being safe here

asfr <- get_covariate_estimates(covariate_id = asfr_id, location_id = location_id, sex_id = female_id,
                                age_group_id = age_group_ids) %>% 
  filter(year_id >= 1980)

years <- unique(asfr$year_id)
pop   <- get_population(year_id = years, location_id = location_id, sex_id = female_id, age_group_id = age_group_ids)

# merge population onto asfr
asfr_pop <- left_join(asfr, pop, by = c("age_group_id", "year_id", "location_id")) %>% 
  mutate(live_births = mean_value * population)

# calculate the number of live births for each location - year - age group
live_births <- asfr_pop %>% select(location_id, location_name, age_group_id, year_id, live_births)

readr::write_csv(live_births, paste0(out_dir, "EX/live_births_", location_id, ".csv"))

# Interpolate -------------------------------------------------------------

# other variables are listed for asfr call
me_id       <- 3948          # early syphilis infection
measure_id  <- 5             # prevalence
year_bounds <- range(years)  # "Tuple" of start and end years
limit       <- 40            # number of interpolate calls allowed to run at once. 40 comes from 20 age groups * 2 sexes, which was suggested

# Pull estimates via interpolate function
message(paste(Sys.time(), "Starting interpolation for me_id", me_id, "from", year_bounds[1], "to", year_bounds[2], ". This may take some time..."))

draws <- interpolate_safe(limit = limit, dir = paste0(out_dir, "tmp"), unique_id = location_id,
                          gbd_id_type = "modelable_entity_id", gbd_id = me_id, source = "epi", measure_id = measure_id, 
                          reporting_year_start = year_bounds[1], reporting_year_end = year_bounds[2], sex_id = female_id,
                          status = "best", location_id = location_id, age_group_id = age_group_ids)

message(paste(Sys.time(), "Finished interpolation."))

# merge on livebirths
draws_long <- merge(draws, live_births, by = c("year_id", "age_group_id", "location_id"), all.x = TRUE)

# Reshape data long for easier computations. 
draws_long <- melt(draws_long, id.vars = c("location_id","year_id", "age_group_id", "live_births"), 
                   measure.vars = patterns("draw_"),
                   variable.name = "draw_num", value.name = "draw")

# Calculate unadjusted live births, early/late split ----------------------------------------
# do the early/late split here since it differs by age,
# it needs to be applied BEFORE we collapse on age group

message(paste(Sys.time(), "Calculate unadjusted live births"))

# multiply draw for early syph prevalence times number of live births 
# to get total number of live births at risk of congenital syphilis
draws_long[, unadj_live_births := draw * live_births]
draws_long[, c("live_births", "draw") := NULL]

message(paste(Sys.time(), "Apply early/late split"))

# Read in early/late syphilis data from Japan notifications
stage_split <- read_excel(paste0(j_root, "FILEPATH/japan_female_syph_split_by_age.xlsx"), sheet = "extraction") %>% 
  select(age_group_id, prop_early, prop_late) %>% 
  as.data.table()

# need to use a different name 
stage_split[, age_group := age_group_id]

# essentially a lookup table
# gives the proportion early/late passed on the col name and the age_group_id
#'`NOTE:` the age_group argument was specifically not named age_group_id 
split_for <- function(col_name, age_group) {
  # the [[1]] is to drop the name so we're left with just a simple numeric
  stage_split[age_group == age_group, get(col_name)][[1]]
}

# split live births into early/late
draws_long[, early_unadj_live_births := unadj_live_births * split_for("prop_early", age_group = age_group_id)]
draws_long[, late_unadj_live_births  := unadj_live_births * split_for("prop_late",  age_group = age_group_id)]

# sum unadjusted live births for a given location - year to get
# total number of births for a location-year regardless of age of mother
unadjusted_births <- draws_long[, lapply(.SD, sum, na.rm = TRUE), .SD = c("early_unadj_live_births", "late_unadj_live_births"), by = c("location_id", "year_id", "draw_num")] %>% 
  setNames(c("location_id", "year_id", "draw_num", "early_unadj_live_births", "late_unadj_live_births"))

rm(draws_long)


# Save and source next script ---------------------------------------------

message(paste(Sys.time(), "Saving as csv for location id ", location_id))

readr::write_csv(unadjusted_births, paste0(out_dir, "EX/", location_id, ".csv"))

# clear up memory by deleting all but end data set
rm(list=setdiff(ls(), c("out_dir", "root_dir", "location_id", "unadjusted_births")))



source(paste0(root_dir, "06_adjusted_livebirths.R"))



