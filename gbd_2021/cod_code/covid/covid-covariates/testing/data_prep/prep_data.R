#########################################################
## Filter and smoothing total testing data
## Imports data from "FILEPATH/all_locations_tests.csv"
## Filters and smoothes those data, pins to date of first observed
## case by location_id. 

## Output is a file that is smoothed daily testing percapita
## that is used in forecast_test_pc.R to make final estimates
#########################################################

# rm(list = ls())
# 
# ##--------------------------------------------------------------
# ## Setup
#   Sys.umask("0002")
#   Sys.setenv(MKL_VERBOSE = 0)
#   suppressMessages(library(data.table))
#   suppressMessages(library(zoo))
#   setDTthreads(1)
#   
#   ## install ihme.covid (always use newest version)
#   # tmpinstall <- system(SYSTEM_COMMAND)
#   # .libPaths(c(tmpinstall, .libPaths()))
#   # devtools::install_github("ihmeuw/ihme.covid", upgrade = "never", quiet = T, ref = "main")
#   invisible(loadNamespace("ihme.covid", lib.loc = "FILEPATH"))

## Arguments
if (interactive()) {

  lsvid <- lsvid_covar
  rake <- F
  use_INDIVIDUAL_NAME_smooth <- T
  
} else {
  
  code_dir <- ihme.covid::get_script_dir()

  parser <- argparse::ArgumentParser()
  parser$add_argument("--lsvid", type = "integer", required = TRUE, help = "location set version id to use")
  parser$add_argument("--outputs-directory", required = TRUE, help = "Directory in which to write outputs")
  parser$add_argument("--rake", action = "store_true", default = FALSE, help = "Enable raking (off by default)")
  parser$add_argument("--model-inputs-version", default = "best", help = "Version of model-inputs. Defaults to 'best'. Pass a full YYYY_MM_DD.VV or 'latest' if you provide")
  parser$add_argument("--INDIVIDUAL_NAME-smooth", action = "store_true", default = FALSE, help = "Enable INDIVIDUAL_NAME smooth (off by default)")
  args <- parser$parse_args()

  lsvid <- args$lsvid
  output_dir <- args$outputs_directory
  rake <- args$rake
  model_inputs_version <- args$model_inputs_version
  use_INDIVIDUAL_NAME_smooth <- args$barber_smooth
  
}

## Paths
  missing_locs_out_path <- file.path(output_dir, paste0("missing_testing_locs_", Sys.Date(), ".csv"))
  data_out_path <- file.path(output_dir, "data_smooth.csv")
  first_case_out_path <- file.path(output_dir, "first_case_date.csv")
  dot_plot_out_path <- file.path(output_dir, "testing_dot_plot.pdf")
  loc_time_series_out_path <- file.path(output_dir, "data_smooth.pdf")
  metadata_path <- file.path(output_dir, "prep_data.metadata.yaml")

## Functions
  source(file.path(code_dir, "get_populations.R")) 
  source(file.path(code_dir, "get_first_case_date.R"))
  source(file.path(code_dir, "get_raw_testing_data.R"))
  source(file.path(code_dir, "process_raw_data.R"))
  source(file.path(code_dir, "INDIVIDUAL_NAME_smooth.R"))
  source(file.path(code_dir, "shift_and_fill.R"))
  source(file.path(code_dir, "rake_to_report.R"))
  source(file.path(code_dir, "weekly_interp.R"))
  source(file.path(code_dir, "diagnostics.R"))
  source(file.path(code_dir, "filter_data.R"))

## Tables

  # hierarchy <- get_location_metadata(location_set_id = lsid_covar, location_set_version_id = lsvid, release_id = release_covar)
  # gbd_hier <- fread(glue("FILEPATH/gbd_analysis_hierarchy.csv"))
  # all_locs <- hierarchy[, location_id]

##-----------------------------------------------------------------------------------
### Import, process data
message("Reading populations...")
#dt_pop <- get_populations(hierarchy, data_version = model_inputs_version)

  dt_pop <- fread("FILEPATH/all_populations.csv")
  dt_pop <- dt_pop[age_group_id == 22 & sex_id == 3]
  
  dt_pop <- dt_pop[, lapply(.SD, function(x) sum(x)), by="location_id", .SDcols="population"] # create single all ages
  
message("Reading date of first case...")
first_case_date <- get_first_case_date(all_locs, data_version = model_inputs_version)
# Add Indonesia subnationals with Indonesia parent date
first_case_date <- rbind(first_case_date, data.table(location_id = unique(hierarchy[parent_id==11]$location_id),
                                                     first_case_date = first_case_date[location_id==11]$first_case_date))

# Want to have pinned date be first case date - 14
first_case_date[, first_case_date := first_case_date - 14]
first_case_date[first_case_date < as.Date("2020-01-01"), first_case_date := as.Date("2020-01-01")]

write.csv(first_case_date, first_case_out_path, row.names = F)

## Get Raw Data ---------------------------------------------
message("Reading raw testing data...")



  message("prep_data: get_raw_testing_data")
  raw_testing_dt <- get_raw_testing_data(testing_data_version)
  
  ## Filter Data ---------------------------------------------
  message("prep_data: filter_data")
  raw_testing_dt <- filter_data(raw_testing_dt)
  
  # VERIFY -----
  # Keep to vet filtering step
  # 
  # verify <- copy(raw_testing_dt)
  # verify[, filtered_daily := c(0, diff(filtered_cumul)), by = .(location_id)]
  # location <- "Salvador"
  # plot(verify[location_name %like% location, date],
  #      verify[location_name %like% location, filtered_cumul])
  # plot(verify[location_name %like% location, date],
  #      verify[location_name %like% location, filtered_daily])
  # tail(verify[location_name %like% location])
  
## Drop HHS data (14 days)
message("Remove last two weeks of HHS data")
  # Puerto Rico, USVI, US states
  hhs_locs <- c(385, 422, hierarchy[parent_id == 102, location_id])
  max_hhs_date <- max(raw_testing_dt[location_id %in% hhs_locs, date])
  raw_testing_dt <- raw_testing_dt[!(location_id %in% hhs_locs & date > max_hhs_date - 14)]
message("US Virgin Islands way too high")
  raw_testing_dt <- raw_testing_dt[!(location_id == 422 & date > "2021-03-18")]

# Date errors
  raw_testing_dt[date < "2020-01-15", filtered_cumul := NA]
  raw_testing_dt <- raw_testing_dt[!(location_id == 4770 & raw_cumul == 126020)]
  raw_testing_dt <- raw_testing_dt[!(location_id == 97 & raw_cumul == 4215412)]
  raw_testing_dt <- raw_testing_dt[!(location_id == 4770 & date == "2020-12-31")]
  
# Modify Austria (test)
  # raw_testing_dt[location_name == "Austria", prop := raw_daily / 3919571]
  # raw_testing_dt[location_name == "Austria" & date < "2021-01-12", filtered_cumul2 := raw_daily + prop * 2885871]
  # raw_testing_dt[location_name == "Austria" & is.na(filtered_cumul2), filtered_cumul2 := 0]
  # raw_testing_dt[location_name == "Austria", filtered_cumul2 := cumsum(filtered_cumul2)]
  # raw_testing_dt[location_name == "Austria" & date >= "2021-01-12", filtered_cumul2 := filtered_cumul]
  # raw_testing_dt[location_name == "Austria", filtered_cumul := filtered_cumul2]
  

##-----------------------------------------------------
## Diagnostic plots for this stage
  
  
  # The shift calc below is causing the weird symmetric positive negative patterns in the data
  # THis fixes all non-cumulative issues with filtered_cumul before this
    raw_testing_dt <- 
    do.call(
      rbind,
      lapply(
        split(raw_testing_dt, by='location_id'),
        FUN=function(x) {
          
          # Remove observations that cause dips in cumulative quantity and fill with linear interp
          # Cycle through until all non-cumulative values are replaced
          while (sum(diff(x$filtered_cumul) < 0, na.rm=T) > 0) {
            
            sel <- which(diff(x$filtered_cumul) < 0)
            x$filtered_cumul[sel+1] <- NA
            x$filtered_cumul <- zoo::na.approx(x$filtered_cumul, na.rm=F)
            
          }
          
          return(x)
        })
    )
  
  # Gap Filler --------------------------------------------------------------
  # Fill gaps of zero and low reporting days (often from blind negative removals)
  message("prep_data: Filling gaps in low/zero reporting (see interactive plots).")
  
  # VERIFY -----
  # Keep to vet pre-gap filler processing
  # 
  # verify <- copy(raw_testing_dt)
  # verify[, filtered_daily := c(0, diff(filtered_cumul)), by = .(location_id)]
  # location <- "Salvador"
  # plot(verify[location_name %like% location, date],
  #      verify[location_name %like% location, filtered_cumul])
  # plot(verify[location_name %like% location, date],
  #      verify[location_name %like% location, filtered_daily])
  # verify_loc <- verify[location_name %like% location]
  

  raw_testing_dt <- fill_reporting_gap(raw_testing_dt, 35506, "2021-08-01", "2021-08-26", N_day_average = 7) # Lazio
  raw_testing_dt <- fill_reporting_gap(raw_testing_dt, 35494, "2020-12-17", "2021-01-16", N_day_average = 7) # Piemonte
  raw_testing_dt <- fill_reporting_gap(raw_testing_dt, 35495, "2020-12-03", "2020-12-27") # Valle d'Aosta
  raw_testing_dt <- fill_reporting_gap(raw_testing_dt, 93,    "2020-06-29", "2020-08-23") # Sweden
  raw_testing_dt <- fill_reporting_gap(raw_testing_dt, 127, "2021-04-23", "2021-09-15",  N_day_average = 30) # El Salvador
  
  # This is causing the weird symmetric positive negative patterns in the data
  raw_testing_dt[, shift_cumul := filtered_cumul - shift(filtered_cumul), by="location_id"]

##-----------------------------------------------------
  
## Continue Onwards
  
  message("Processing raw testing data...")
  
  redist_testing_dt <- process_raw_data(df = raw_testing_dt, 
                                        dt_pop = dt_pop, 
                                        locs = all_locs, 
                                        first_case_date = first_case_date)

  ## Place to review
  tail(redist_testing_dt[location_id == 35], 10)
  # tail(raw_testing_dt[location_name == "Jamaica"], 10)
  # 
  ggplot(redist_testing_dt[location_id == 35], aes(x=as.Date(date), y=daily_total_redistributed / 100000)) + geom_point()

##-------------------------------------------------------------------------------------------------------
message("Binning to weekly and interpolating...")

  testing_data <- weekly_interp(redist_testing_dt)

  ## Place to review
  # tail(testing_data[location_id == 4650], 20)
  # ggplot(testing_data[location_name == "Colorado"], aes(x=as.Date(date), y=daily_total / pop * 100000)) + geom_point()

  ## Import reported cases, make sure that testing is always greater than (cases * case_scalar)
  full_data <- fread(glue("FILEPATH/full_data_unscaled.csv"))
  full_data[, cases := Confirmed - shift(Confirmed), by = "location_id"]
  full_data[, date := as.Date(Date)]
  testing_data <- merge(testing_data, full_data[,c("location_id","cases","date")], by = c("location_id","date"), all.x = T)
  testing_data[, daily_total_unadjusted := daily_total]
  testing_data[, daily_total := pmax(cases * case_scalar, daily_total_unadjusted)]
  #testing_data[, daily_total := ifelse(daily_total < cases * case_scalar, cases * case_scalar, daily_total)]
  testing_data[is.na(daily_total), daily_total := daily_total_unadjusted]
  

  ##############################################################################
  ##############################################################################
  # Quick edit of zero data in Roraima
  
  break_date <- '2020-08-17'
  
  sel <- testing_data$location_id == 4771
  tmp <- testing_data[sel,]
  tmp <- tmp[date < as.Date(break_date), daily_total := NA] # remove spike prior to Aug 17
  mean_level <- mean(tmp$daily_total, na.rm=TRUE)
  tmp <- tmp[date == min(date, na.rm = T), daily_total := 0] # Anchor start date at zero
  tmp <- tmp[date == as.Date(break_date)-75, daily_total := mean_level] # Add inflection point for when cases/testing appear to reach mean level
  tmp$daily_total <- zoo::na.approx(sqrt(tmp$daily_total), na.rm=F)
  tmp$daily_total <- tmp$daily_total^2

  testing_data <- rbind(testing_data[!sel], tmp)
  
  ##############################################################################
  ##############################################################################
  
  
## INDIVIDUAL_NAME smooth ---------------------------------------------------------------------------------

message("INDIVIDUAL_NAME smoothing...")
  #testing_data[, daily_total := zoo::na.approx(daily_total, na.rm=FALSE), by = "location_id"]
  testing_data[!is.na(daily_total) & daily_total != 0, daily_total_smoothed := INDIVIDUAL_NAME_smooth(daily_total, n_neighbors = 5, times = 10), by = "location_id"]

  
  #x <- testing_data[location_name == 'Roraima',]
  
  #par(mfrow=c(4,1))
  #y_max <- max(x$cases*1.1, na.rm=T)
  #plot(x$date, x$daily_total_unadjusted, ylim=c(0,y_max))
  #plot(x$date, x$daily_total, ylim=c(0,y_max))
  #abline(h=mean_level, lty=2, col='green', lwd=2)
  #abline(v=as.Date(break_date), col='blue')
  #abline(v=as.Date(break_date)-75, col='red')
  #plot(x$date, x$daily_total_smoothed, ylim=c(0,y_max))
  #plot(x$date, x$cases, ylim=c(0,y_max))
  #abline(v=as.Date(break_date), col='blue')
  #abline(v=as.Date(break_date)-75, col='red')
  
  
  if(use_INDIVIDUAL_NAME_smooth) {
    message("Replacing output with INDIVIDUAL_NAME smoothed...")
    testing_data[, daily_total := daily_total_smoothed]
  }
  

# Rake
  if (rake) {
    message("Data are being raked")
    rake_to_report(testing_data, output_dir)
  }
  testing_data <- testing_data[order(location_id, date)]
  testing_data <- testing_data[, .(location_id, location_name, date, raw_cumul, raw_daily, combined_cumul, filtered_cumul, daily_total_reported, daily_total_smoothed, daily_total, population)]


##-------------------------------------------------------------------------------------------------
## Backfill NAs prior to first to make the dataset square
  back_cast_dates <- CJ( # I think this is like expand.grid but data.table style
    date = seq(as.Date("2020-01-01"), max(first_case_date$first_case_date), by = "1 day"),
    location_id = unique(testing_data$location_id)
  )
  back_cast_dates <- merge(back_cast_dates, unique(testing_data[, .(location_id, location_name, population)]), by = "location_id")
  testing_data <- merge(testing_data, back_cast_dates, by = c("date", "location_name", "location_id", "population"), all = T)
  testing_data <- testing_data[order(location_id, date)]

## Check for duplicates
  if (nrow(testing_data[, .N, by = c("location_id", "date")][N > 1]) > 1) {
    stop(paste("Duplicate location-days in the data", print(unique(testing_data[, .N, by = c("location_id", "location_name", "date")][N > 1]$location_name))))
  }

## Check location set against the location hierarchy
  missing_locs <- setdiff(all_locs, unique(testing_data$location_id))
  if (length(missing_locs) > 0) {
    subnats <- intersect(hierarchy[level > 0]$location_id, missing_locs)
    parents <- unique(hierarchy[location_id %in% subnats]$parent_id)
    message(paste(
      "Missing subnational data but national data present for:",
      paste(hierarchy[location_id %in% intersect(parents, unique(testing_data$location_id)), location_name], collapse = ", ")
    ))
    missing_parents <- setdiff(parents, unique(testing_data$location_id))
    if(length(missing_parents) > 0) {
      warning(paste(
        "Missing subnational data and no national data present for:",
        paste(hierarchy[location_id %in% missing_parents, location_name], collapse = ", ")
      ))
    }
    missing_nats <- setdiff(missing_locs, subnats)
    if(length(missing_nats) > 0) {
      message(paste(
        "Missing data for:",
        paste(hierarchy[location_id %in% missing_nats, location_name], collapse = ", ")
      ))
    }
    # Write file with missing locations relative to a full hierarchy
    write.csv(hierarchy[!(location_id %in% unique(raw_testing_dt$location_id)), .(location_name, location_id)], missing_locs_out_path, row.names = F)
  }
  
## Import reported cases, make sure that testing is always greater than that
  
  #full_data <- fread("FILEPATH/full_data.csv")
  #full_data[, cases := Confirmed - shift(Confirmed), by = "location_id"]
  #full_data[, date := as.Date(Date)]
  #
  #testing_data <- merge(testing_data, full_data[,c("location_id","cases","date")], by = c("location_id","date"), all.x = T)
  #testing_data[, daily_total_unadjusted := daily_total_smoothed]
  #testing_data[, daily_total_smoothed := ifelse(daily_total_smoothed < cases * case_scalar, 
  #                                                       cases * case_scalar,
  #                                                       daily_total_smoothed)]
  #testing_data[!is.na(daily_total_smoothed), daily_total := daily_total_smoothed]
  
  # Fix Bahamas outliers
  sel <- which(testing_data$location_id == 106 & testing_data$date == as.Date("2021-02-17"))
  new_val <- testing_data[sel - 1, daily_total_reported] + testing_data[sel, daily_total_reported]
  testing_data$daily_total_reported[sel] <- new_val
  testing_data$daily_total_reported[sel - 1] <- NA
  
  
## Save
  write.csv(testing_data, data_out_path, row.names = F)

##--------------------------------------------------------------------
## Check locations against previous best (lots of issues with losing data)
  cur_best_data <- fread(glue("FILEPATH/data_smooth.csv"))
  
  new_data_locs <- setdiff(unique(testing_data$location_id), unique(cur_best_data$location_id))
  missing_data_locs <- setdiff(unique(cur_best_data$location_id), unique(testing_data$location_id))
  
  message(paste0("New data in ", paste(hierarchy[location_id %in% new_data_locs, location_name], collapse = ", ")))
  message(paste0("Previously there were data but are now missing in ", 
                 paste(hierarchy[location_id %in% missing_data_locs, location_name], collapse = ", ")))

## Plot
# message("Generating dot plot...")
# dot_plot(data_out_path, dot_plot_out_path)
# message("Generating location-specific time-series plots...")
# loc_time_series(data_out_path, loc_time_series_out_path, hierarchy)

message(paste("\nDone! Output directory:", output_dir))

yaml::write_yaml(
  list(
    script = "prep_data.R",
    location_set_version_id = lsvid,
    output_dir = output_dir,
    input_files = ihme.covid::get.input.files()
  ),
  file = metadata_path
)
