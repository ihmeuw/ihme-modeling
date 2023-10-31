output_dir <- "FILEPATH" #FHS output
gbd_output_dir <- 'FILEPATH' #GBD output to fill in for missing FHS locs

source(file.path("FILEPATH/get_location_metadata.R"))
gbd_h <- get_location_metadata(location_set_id = 39, release_id = 9) #run for FHS, sub gbd_hier for FHS location set = 39

# for zero-covid/no data locs, sub in gbd estimates through 2022 and then fill with zeros for remaining time series
gbd_subs <- c(24,25,27,298,361,320,369,374,413,416,131,189)
for (g in gbd_subs){
  print(g)
  dt <- readRDS(paste0(gbd_output_dir, g, '.rds')) #gbd path
  # Fill with zeros forwards to end of 2023
  max_dt <- dt[date == max(date)]
  max_dt[, paste0("draw_", 0:999) := 0]
  max_d <- unique(max_dt$date)
  fill_d <- seq(as.Date(max_d) + 1, as.Date("2023-12-31"), by = "1 day")
  fill_dt <- rbindlist(lapply(fill_d, function(d) {
      copy(max_dt)[, date := d]
    }))
  dt[, date := as.Date(date)]
  dt <- rbind(dt, fill_dt)
  dt <- dt[order(date)]
  saveRDS(dt, paste0(output_dir, 'daily/final/', g, '.rds'))

  annual <- dt[, lapply(.SD, sum),
                   by = .(location_id, age_group_id, sex_id, measure_name, metric_id, year_id, measure_id, method_name),
                   .SDcols = paste0("draw_", 0:999)]

  saveRDS(annual, file.path(output_dir, 'annual', 'final', paste0(g, '.rds')))
}

exists <- as.integer(gsub(".rds", "", list.files(file.path(output_dir, "daily/final/"))))
missing_locs <- setdiff(gbd_h[most_detailed == 1]$location_id, exists)
parents <- gbd_h[most_detailed==0 & level>2 & !location_id %in% exists, .(location_id, level)]
parents <- parents[order(level, decreasing=TRUE)]

# Aggregate to GBD parents
message("Aggregating subnationals to GBD parents...")

for(p in parents$location_id) {
  print(p)
  children <- gbd_h[parent_id == p]$location_id
  dt <- rbindlist(lapply(paste0(output_dir, "daily/final/", children, ".rds"), readRDS))
  dt[, location_id := p]
  collapse_dt <- dt[, lapply(.SD, sum), by = .(location_id, age_group_id, sex_id, measure_name, metric_id, measure_id, date, method_name, year_id)]
  saveRDS(collapse_dt, file.path(output_dir, "daily/final/", paste0(p, ".rds")))
}

totally_absent <- setdiff(missing_locs, parents)
if(length(totally_absent) > 0) {
  stop(paste0("Missing the locations from age-sex output:", totally_absent))
}
