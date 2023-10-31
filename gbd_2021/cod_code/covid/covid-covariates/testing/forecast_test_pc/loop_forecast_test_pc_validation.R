############################################################################
## Loop through testing data to get covariates for predictive validity
############################################################################

## Setup
rm(list = ls())
Sys.umask("002")
Sys.setenv(MKL_VERBOSE = 0)
suppressMessages(library(data.table))
suppressMessages(library(parallel))
setDTthreads(1)

## install ihme.covid (always use newest version)
tmpinstall <- system(SYSTEM_COMMAND)
.libPaths(c(tmpinstall, .libPaths()))
devtools::install_github("ihmeuw/ihme.covid", upgrade = "never", quiet = T)
##

SEIR_COVARIATES_ROOT <- "FILEPATH"

## Arguments
  user <- Sys.info()["user"]
  code_dir <- paste0("FILEPATH", user, "FILEPATH")
  
  loop_dates <- as.Date(seq(as.Date("2020-03-27"),as.Date("2020-05-29"), by=7))
  loop_dates <- c(loop_dates, as.Date("2020-03-31"))
  
  release <- "2020_06_19.02"
  lsvid <- 723
  output_dir <- file.path("FILEPATH", release)
  seir_covariates_version <- "2021_11_16.01" CLUSTER_NAME
  
  mark_best <- F

ncores <- 10
frontier <- 500 / 1e5

## Paths
seir_testing_reference <- file.path(SEIR_COVARIATES_ROOT, seir_covariates_version, "testing_reference.csv")
input_data_path <- file.path(output_dir, "data_smooth.csv")
first_case_path <- file.path(output_dir, "first_case_date.csv")
detailed_out_path <- file.path(output_dir, "forecast_raked_test_pc_detailed.csv")
simple_out_path <- file.path(output_dir, "forecast_raked_test_pc_simple.csv")
scenario_dict_out_path <- file.path(output_dir, "testing_scenario_dict.csv")
plot_scenarios_out_path <- file.path(output_dir, "forecast_test_pc.pdf")
plot_prod_comp_out_path <- file.path(output_dir, "plot_comp_w_previous.pdf")

## Functions
source(file.path("FILEPATH/get_location_metadata.R"))
source(file.path("FILEPATH/get_covariate_estimates.R"))
source(file.path(code_dir, "forecast_all_locs.R"))
source(file.path(code_dir, "rake_vals.R"))
source(file.path(code_dir, "gen_better_worse.R"))
source(file.path(code_dir, "diagnostics.R"))

## Tables
hierarchy <- get_location_metadata(location_set_id = 115, location_set_version_id = lsvid)
hier_supp <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
hierarchy[, c("super_region_id", "super_region_name", "region_id", "region_name") := NULL]
hier_supp[, merge_id := location_id]
hierarchy[location_id %in% hier_supp$location_id, merge_id := location_id]
hierarchy[is.na(merge_id), merge_id := parent_id]
hier_supp <- unique(hier_supp[, .(merge_id, super_region_id, super_region_name, region_id, region_name)])
hierarchy <- merge(hierarchy, hier_supp, by = c("merge_id"), all.x = T)

## Loop
for(d in as.Date(loop_dates)){
  d <- as.Date(d, origin = "1970-01-01")
  # Prep data
  dt <- fread(input_data_path)
  dt[, date := as.Date(date)]
  dt <- subset(dt, date <= d)
  in_dt <- copy(dt)
  dt$daily_total <- ifelse(dt$location_id==488, dt$daily_total_reported, dt$daily_total)
  dt <- dt[, .(location_id, location_name, date, daily_total, population)]
  dt[, total_pc_100 := daily_total / population * 1e5]
  
  ## Forecast
  # Hot drop for name change
  dt <- subset(dt, location_id != 488)
  # Hot drop for Brazil national
  dt <- subset(dt, location_id != 135)
  
  dt <- subset(dt, !is.na(total_pc_100))
  
  out_dt <- forecast_all_locs(dt, var_name = "total_pc_100", ncores = ncores)
  # Don't pass on negatives
  out_dt[fcast_total_pc_100 < 0, fcast_total_pc_100 := 0]
  
  ## Rake South Africa provinces to national
  zaf_nat_id <- 196
  zaf_sub_ids <- 482:490
  if(all(c(zaf_nat_id, zaf_sub_ids) %in% hierarchy$location_id)) {
    message("Raking South Africa provinces to national...")
    out_dt[, test_count := fcast_total_pc_100 / 1e5 * population] # Need to rake in count space
    out_dt <- rake_vals(out_dt, zaf_sub_ids, zaf_nat_id, "test_count")
    out_dt[, raked_ref_test_pc := raked_test_count / population]
    out_dt[is.na(raked_ref_test_pc), raked_ref_test_pc := fcast_total_pc_100 / 1e5]
  } else {
    out_dt[, raked_ref_test_pc := fcast_total_pc_100 / 1e5]
  }
  
  ## Generate better worse
  out_dt <- gen_better_worse_all(dt, out_dt)
  
  # Use national values for missing subnationals
  missing_children <- setdiff(hierarchy[most_detailed == 1 & level > 3]$location_id, unique(out_dt$location_id))
  parents <- unique(hierarchy[location_id %in% missing_children]$parent_id)
  missing_children_dt <- rbindlist(lapply(parents, function(p) {
    parent_dt <- copy(out_dt[location_id == p])
    all_children <- setdiff(hierarchy[parent_id == p]$location_id, p)
    impute_locs <- intersect(missing_children, all_children)
    rbindlist(lapply(impute_locs, function(i) {
      child_dt <- copy(parent_dt)
      child_dt[, location_id := i]
      child_dt[, location_name := hierarchy[location_id == i]$location_name]
      child_dt[, population := NA]
    }))
  }))
  
  out_dt <- rbind(out_dt, missing_children_dt)
  
  # Replace negatives with zero's
  out_dt[raked_ref_test_pc < 0, raked_ref_test_pc := 0]
  out_dt[better_fcast < 0, better_fcast := 0]
  out_dt[worse_fcast < 0, worse_fcast := 0]
  
  # Cap at frontier
  out_dt[raked_ref_test_pc > frontier & observed == F, raked_ref_test_pc := frontier]
  
  ## Check for missing locations
  missing_locs <- setdiff(hierarchy[most_detailed == 1]$location_id, unique(out_dt$location_id))
  if (length(missing_locs) > 0) {
    message(paste0("Missing: ", paste(hierarchy[location_id %in% missing_locs]$location_name, collapse = ", ")))
  }

  ## Flatline South Korea and Japan
  message("Flatlining South Korea and Japan in the future")
  # Also flatline locations that exceed frontier at last observation
  obs_dt <- out_dt[observed == T]
  max_date <- obs_dt[, .(date = max(date)), by = location_id]
  max_dt <- merge(obs_dt, max_date)
  exceed_locs <- max_dt[raked_ref_test_pc > frontier]$location_id
  # korea only had 1 row and was dropped! (loc_id 68)
  for(loc_id in c(67, exceed_locs)) {
    print(loc_id)
    impute_dt <- out_dt[location_id == loc_id & observed == T]
    impute_val <- impute_dt[date == max(date)]$raked_ref_test_pc
    out_dt[location_id == loc_id & observed == F, raked_ref_test_pc := impute_val]
  }
  
  ## Need to merge with SDI ##
  sdi <- get_covariate_estimates(location_id = "all", year_id=2019, gbd_round_id = 6, decomp_step = "step4", covariate_id = 881)
  setnames(sdi, "mean_value","sdi")
  
  out_dt <- merge(out_dt, sdi[,c("location_id","sdi")], by="location_id", all.x=T)
  #setdiff(unique(out_dt$location_name), unique(out_dt1$location_name))
  
  # Missing some locations (Germany, Spain, Canada, subnationals, new ones in India)
  
  # Linear regression ARC and SDI
  plot_data <- aggregate(cbind(arc, sdi) ~ location_id, data=data.frame(out_dt), function(x) mean(x))
  lm_arc_sdi <- lm(arc ~ sdi, data=plot_data)
  log_arc_sdi <- lm(log(arc) ~ sdi, data=plot_data)
  
  out_dt$sdi_pred_arc <- exp(predict(log_arc_sdi, newdata=out_dt))
  sdi$sdi_pred_arc <- exp(predict(log_arc_sdi, newdata=sdi))
  
  sdi_pred <- data.frame(sdi = seq(0,1,0.01))
  sdi_pred$prediction <- predict(lm_arc_sdi, newdata=sdi_pred)
  sdi_pred$log_pred <- exp(predict(log_arc_sdi, newdata=sdi_pred))
  
  plot_data <- aggregate(cbind(arc, sdi) ~ location_id, data=data.frame(out_dt), function(x) mean(x))
  ggplot(sdi_pred, aes(x=sdi)) + ylab("ARC") + 
    geom_line(aes(y=prediction), col="purple") +
    geom_line(aes(y=log_pred), col="blue") + 
    theme_minimal() +
    geom_point(data=plot_data, aes(x=sdi, y=arc), alpha=0.4)
  
  # Predict onto full dataset
  # Find locations that aren't in SDI
  missing_children_sdi <- rbindlist(lapply(parents, function(p) {
    parent_dt <- copy(sdi[location_id == p])
    all_children <- setdiff(hierarchy[parent_id == p]$location_id, p)
    impute_locs <- intersect(missing_children, all_children)
    rbindlist(lapply(impute_locs, function(i) {
      child_dt <- copy(parent_dt)
      child_dt[, location_id := i]
      child_dt[, location_name := hierarchy[location_id == i]$location_name]
    }))
  }))
  
  # sdi_locs <- sdi$location_id
  # missing_sdi_locs <- setdiff(missing_children, sdi_locs)
  # 
  # missing_children_sdi <- subset(missing_children_sdi, (location_id %in% missing_sdi_locs))
  # sdi <- rbind(sdi, missing_children_sdi)
  
  # Make a regional average for missing locations
  message("Imputing missing locations with an SDI prediction")
  first_case_dt <- fread(first_case_path)
  setnames(first_case_dt, "first_case_date", "date")
  first_case_dt[, date := as.Date(date)]
  regions <- unique(hierarchy[location_id %in% missing_locs]$region_id)
  
  # So this function/lapply uses the regional mean arc (?) to apply to locations without data
  missing_dt <- rbindlist(lapply(regions, function(r) {
    print(r)
    all_region_locs <- hierarchy[region_id == r]$location_id
    region_locs <- setdiff(all_region_locs, missing_locs)
    region_dt <- unique(out_dt[!is.na(arc) & location_id %in% region_locs, .(location_id, arc)])
    arc <- mean(region_dt$arc) / 1e5
    region_missing_children <- hierarchy[region_id == r & location_id %in% missing_locs]$location_id
    rbindlist(lapply(region_missing_children, function(c) {
      print(c)
      child_dt <- first_case_dt[location_id == c]
      if(nrow(child_dt) == 0) { # No first case date => min date of region
        child_dt <- data.table(
          location_id = c,
          date = min(first_case_dt[location_id %in% all_region_locs]$date)
        )
      }
      first_date <- child_dt$date
      extend_date <- as.Date("2021-01-01") - first_date # Make this date an argument
      child_dt <- rbind(
        child_dt, 
        data.table(
          location_id = c,
          date = (first_date + seq(extend_date))
        )
      )
      # ARC should be the predicted value, clean this up
      arc <- unique(sdi$sdi_pred_arc[sdi$location_id==c]) / 1e5
      child_dt[, raked_ref_test_pc := arc + as.integer(date - first_date) * arc]
      child_dt$arc <- arc
      child_dt$sdi <- sdi$sdi[sdi$location_id==c]
      child_dt[, location_id := c]
      child_dt[, location_name := hierarchy[location_id == c]$location_name]
      child_dt[, observed := 0]
      
      return(child_dt)
    }))
  }))
  
  out_dt <- rbind(out_dt, missing_dt, fill = T)
  
  # Cap at frontier after regional averaging
  out_dt[raked_ref_test_pc > frontier & observed == F, raked_ref_test_pc := frontier]
  
  
  ## Check for missing locations
  missing_locs <- setdiff(hierarchy[most_detailed == 1]$location_id, unique(out_dt$location_id))
  if (length(missing_locs) > 0) {
    stop(paste0("Missing from most_detailed: ", paste(hierarchy[location_id %in% missing_locs]$location_name, collapse = ", ")))
  }
  
  # Save
  write.csv(out_dt, paste0("FILEPATH",release,"/forecast_raked_test_pc_detailed_",d,".csv"), row.names = F)
  
  # Generate a simpler file
  out_dt[, test_pc := raked_ref_test_pc]
  # out_dt[is.na(test_pc), test_pc := test_count / population]
  out_dt[, test_pc_better := better_fcast]
  out_dt[, test_pc_worse := worse_fcast]
  simple_dt <- out_dt[location_id %in% hierarchy$location_id, .(location_id, location_name, date, observed, test_pc, test_pc_better, test_pc_worse, population)]
  
  ## Generate aggregates from most_detailed locations
  parents <- subset(parents, parents != 135) # Brazil is missing
  
  most_dt <- simple_dt[location_id %in% hierarchy[most_detailed == 1]$location_id | location_id %in% parents]
  aggs <- hierarchy[most_detailed != 1 & order(level) & !(location_id %in% parents), .(location_id, level)]
  for(loc in rev(aggs$location_id)) {
    children <- setdiff(hierarchy[parent_id == loc]$location_id, loc)
    child_dt <- most_dt[location_id %in% children]
    child_dt[, c("test", "test_better", "test_worse") := .(test_pc * population, test_pc_better * population, test_pc_worse * population)]
    na_dt <- child_dt[, .(all_na = all(is.na(test))), by = .(date)]
    parent_dt <- child_dt[, lapply(.SD, sum, na.rm = T), by = .(date), .SDcols = c("test", "test_better", "test_worse", "observed", "population")]
    parent_dt <- merge(parent_dt, na_dt)
    parent_dt[(all_na), c("test", "test_better", "test_worse") := NA]
    parent_dt[, all_na := NULL]
    parent_dt[, location_id := loc]
    parent_dt[, location_name := hierarchy[location_id == loc]$location_name]
    parent_dt[, c("test_pc", "test_pc_better", "test_pc_worse") := .(test / population, test_better / population, test_worse / population)]
    parent_dt[, c("test", "test_better", "test_worse") := NULL]
    parent_dt[observed != 0, observed := 1]
    most_dt <- rbind(most_dt, parent_dt, use.names = T)
  }
  simple_dt <- most_dt
  
  ## Check for missing locations
  missing_locs <- setdiff(hierarchy$location_id, unique(simple_dt$location_id))
  if (length(missing_locs) > 0) {
    stop(paste0("Missing: ", paste(hierarchy[location_id %in% missing_locs]$location_name, collapse = ", ")))
  }
  
  ## Check all non-NA values after first non-NA
  first_non_na <- simple_dt[!is.na(test_pc), .(min_date = min(date)), by = .(location_id)]
  test_dt <- merge(simple_dt, first_non_na)
  offending_dt <- test_dt[date > min_date][is.na(test_dt[date > min_date]$test_pc),]
  if(nrow(offending_dt) > 0) {
    stop(paste0("NAs after first non-NA in: ", paste(unique(offending_dt$location_name), collpase = ", ")))
  }
  
  # Save
  write.csv(simple_dt, paste0("FILEPATH",release,"/forecast_raked_test_pc_simple_",d,".csv"), row.names = F)
}
