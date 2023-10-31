# Use to identify any failed locations where errors didn't show up in jobmon logs




# Source everything necessary
if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$'ROOT', 'FILEPATH/get_location_metadata.R'))

# Pull the expected locations (all most detailed)
# GBD location set
expected_locs <- get_location_metadata(
  location_set_id=35,
  gbd_round_id=roots$gbd_round,
  decomp_step=roots$decomp_step
)[most_detailed==1]

# FHS location set
expected_locs <- get_location_metadata(
  location_set_id=39,
  gbd_round_id=roots$gbd_round
)[most_detailed==1]

# COVID location set
#expected_locs <- get_location_metadata(
#  location_set_id=111,
#  release_id = 9, location_set_version_id=1009
#)[most_detailed==1]

locs <- expected_locs$location_id
locs

# Pull the locs that finished properly at each stage
short_completed_locs <- .get_successful_locs('2022-02-23.02', 'stage_1')
long_completed_locs <- .get_successful_locs('2022-02-23.02', 'stage_2')
final_completed_locs <- .get_successful_locs('2022-02-23.02', 'final')

final_completed_locs$num <- 1
final_completed_locs <- final_completed_locs[, lapply(.SD, sum, na.rm=T), 
                                 by=c('location_id'), 
                                 .SDcols='num']
locs <- final_completed_locs$location_id[final_completed_locs$num<max(final_completed_locs$num) & !is.na(final_completed_locs$location_id)]
locs


# Compare the differences between expected and completed
final_missing_locs <- expected_locs[
  unique(expected_locs$location_id) %ni% unique(final_completed_locs$location_id)
]
print(unique(final_missing_locs$location_ascii_name))
print(unique(final_missing_locs$location_id))
locs <- unique(final_missing_locs$location_id)
locs

# Compare the differences between expected and completed
short_missing_locs <- expected_locs[
  unique(expected_locs$location_id) %ni% unique(short_completed_locs$location_id)
]
print(unique(short_missing_locs$location_ascii_name))
print(unique(short_missing_locs$location_id))
locs <- unique(short_missing_locs$location_id)
locs
length(locs)



long_missing_locs <- expected_locs[
  unique(expected_locs$location_id) %ni% unique(long_completed_locs$location_id)
]
print(unique(long_missing_locs$location_ascii_name))
print(unique(long_missing_locs$location_id))
locs <- unique(long_missing_locs$location_id)
locs

