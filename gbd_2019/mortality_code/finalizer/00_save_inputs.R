
rm(list=ls())
library(data.table); library(assertable); library(DBI); library(readr)
library(plyr); library(argparse)
library(mortdb, lib.loc = FILEPATH)

username <- Sys.getenv("USER")

parser <- ArgumentParser()
parser$add_argument('--shock_death_number_estimate_version', type="integer", required=TRUE,
                    help='The with shock death number estimate version for this run')
parser$add_argument('--shock_life_table_estimate_version', type="integer", required=TRUE,
                    help='The with shock life table estimate version for this run')
parser$add_argument('--no_shock_death_number_estimate_version', type="integer", required=TRUE,
                    help='The no shock death number estimate version for this run')
parser$add_argument('--shock_aggregator_version', type="integer", required=TRUE,
                    help='The shocks addition version for this run')
parser$add_argument('--population_estimate_version', type="integer", required=TRUE,
                    help='Population estimate version')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD Year')
parser$add_argument('--aggregate_locations', type="character", required=TRUE,
                    help='True/False whether or not to aggregate locations')

args <- parser$parse_args()
shock_death_number_estimate_version <- args$shock_death_number_estimate_version
shock_aggregator_version <- args$shocks_aggregator_version
shock_life_table_estimate_version <- args$shock_life_table_estimate_version
no_shock_death_number_estimate_version <- args$no_shock_death_number_estimate_version
population_estimate_version <- args$population_estimate_version
gbd_year <- args$gbd_year
aggregate_locations <- args$aggregate_locations


locations <- query(FILEPATH) # pull estimate locations

locations <- locations[location_id != 6] # drop china 6

country_locations <- locations[level==3]  # pull only level 3 locations, for aggregation purposes
run_years <- c(1950 : gbd_year)

all_locations <- query(FILEPATH)
if(aggregate_locations) {
  sdi_locations <- query(FILEPATH)
  region_sdi_locations <- rbindlist(list(all_locations[level <= 2], sdi_locations[level == 0]))

  special_location_list <- c("sdi", "who", "world bank", "eu", "commonwealth", "four world regions",
                             "world bank income levels", "oecd", "g20", "african union", "nordic")
  special_locations <- rbindlist(lapply(special_location_list, function(x) setDT(get_locations(gbd_type = x, gbd_year = gbd_year, level = "all"))[level != max(level)]))
  newnor <- query(FILEPATH)
  special_locations <- rbind(special_locations, newnor, use.names=T)

  all_plus_special_locations <- rbindlist(list(all_locations, special_locations), use.names = T)

  all_locations <- rbindlist(list(all_locations, sdi_locations[level == 0]), use.names = T)

  write_csv(region_sdi_locations, FILEPATH)
  write_csv(all_plus_special_locations, FILEPATH)
}

write_csv(locations, FILEPATH)
write_csv(all_locations, FILEPATH)
write_csv(country_locations, FILEPATH)

# also create lowest location map
lowest_locations <- query(FILEPATH)
write_csv(lowest_locations, FILEPATH)

age_map <- query(FILEPATH)
full_age_map <- query(FILEPATH)
write_csv(age_map,FILEPATH)
write_csv(full_age_map, FILEPATH)

## Pull population data
pop_age_ids <- c(age_map[age_group_id <= 32, age_group_id], 235, 2, 3, 4) # Subset to through 95+ age groups, and include NN population
pops <- query(FILEPATH)

pops <- pops[, .(location_id, sex_id, year_id, age_group_id, population = mean)]

# temp: add on new norway subnationals
norway_locs <- query(FILEPATH)
norway_pop <- merge(pops, norway_locs[, .(location_id, level, parent_id)], by='location_id')
new_norway <- norway_pop[level == 5, .(population=sum(population)), by=c('parent_id', 'year_id', 'sex_id', 'age_group_id')]
setnames(new_norway, 'parent_id', 'location_id')
pops <- rbindlist(list(pops, new_norway), use.names=T)

assert_values(pops, colnames(pops), "not_na")
assert_ids(pops, list(sex_id = c(1:3),
                      year_id = run_years,
                      age_group_id = pop_age_ids,
                      location_id = c(unique(all_locations$location_id), 60132:60137)))

write_csv(pops, FILEPATH)


## Create symlinks from the feeder processes to point to the new results
reckoning_dir <- FILEPATH
shocks_addition_dir <- FILEPATH

system(paste0("mkdir ", shocks_addition_dir))

flt_dir <- FILEPATH
dir.create(flt_dir)


# DONE