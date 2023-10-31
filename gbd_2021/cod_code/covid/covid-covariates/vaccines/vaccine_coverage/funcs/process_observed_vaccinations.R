#------------------------------------------------------------------------------#
## Process the Observed/Reported vaccine doses delivered data
#------------------------------------------------------------------------------#

#-------------------------Main Method -----------------------------------------#

process_observed_vaccinations <- function(vaccine_output_root) {
  
  # vaccine_output_root = .output_path
  
  # Param / Hier / Pop -----------------------------------------------------
  
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  model_inputs_root <- model_parameters$model_inputs_path
  
  message('Loading data')
  hierarchy <- gbd_data$get_covid_modeling_hierarchy()
  
  adult_population <- model_inputs_data$load_adult_population(model_inputs_root, model_parameters)
  #adult_population <- model_inputs_data$load_total_population(model_inputs_root)
  #setnames(adult_population, 'population', 'adult_population')
  
  # Over 5 Populations ---------------------------------------------------------
  
  # Use 5+ population for the following locations
  # o5_locs <- c(6, 101, 156) # China, Canada, UAE, (US gets 5+ in post-hoc scenario)
  o5_locs <- ihme.covid::children_of_parents(c(6, # China and children
                                               101, # Canada and children
                                               156 # UAE and children
                                               ), hierarchy, "loc_ids", include_parent = TRUE)
  
  over5_population <- model_inputs_data$load_o5_population(model_inputs_root)
  
  setnames(over5_population, 'over5_population', 'adult_population')
  sel <- which(adult_population$location_id %in% o5_locs)
  adult_population <- rbind(adult_population[-sel,], over5_population[sel,])
  adult_population <- adult_population[order(location_id),]
  
  
  # Over 3 Populations ---------------------------------------------------------
  
  
  # Crude adjust for China vaccinating 3+
  population <- model_inputs_data$load_all_population(model_inputs_root)
  china_and_children <- ihme.covid::children_of_parents(parent_loc_ids = 6, hierarchy = hierarchy, output = "loc_ids", include_parent = TRUE)
  # china_subnats <- ihme.covid::children_of_parents(parent_loc_ids = 6, hierarchy = hierarchy, output = "loc_ids", include_parent = FALSE)
  # china_provinces <- ihme.covid::children_of_parents(44533, hierarchy, "loc_ids")
  # china_subnats_most_detailed <- hierarchy[location_id %in% china_subnats & most_detailed == 1, location_id]
  
  for (i in china_and_children) {
    
    tmp_3_to_5_addition <- population[location_id == i & age_group_id == 1 & sex_id == 3, population] * 2/5
    sel <- adult_population$location_id == i
    adult_population[sel, 'adult_population'] <- adult_population[sel, 'adult_population'] + tmp_3_to_5_addition
    
  }

    
  # LOADERS --------------------------------------------------------------------

  
  ### get observed and process data
  #message('China CDC')
  #observed_chn <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "china", template="one")
  #observed_chn <- .process_observed_chn(observed_chn)
  
  message('OWiD')
  observed_owid <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "our_world_in_data", template="one")
  observed_owid <- .process_observed_owid(observed_owid, adult_population, hierarchy)
  observed_owid <- .make_missing_china_subnats(observed_owid,  adult_population, hierarchy, vaccine_output_root) 
  
  message('CDC')
  observed_cdc <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "cdc", template="one")
  observed_cdc <- .process_observed_cdc(observed_cdc, adult_population, hierarchy)
  
  message('Canada')
  observed_can <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "canada", template="one")
  observed_can <- .process_observed_can(observed_can)
  
  message('Mexico')
  observed_mex <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "mexico", template="one")
  observed_mex <- .process_observed_mex(observed_mex, observed_owid, vaccine_output_root, population = over5_population, model_parameters = model_parameters)
  
  message('Brazil')
  # observed_bra <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "brazil", template="one") # new data on 2022-07-29
  observed_bra <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "brazil_linelist", template="one")
  observed_bra <- .process_observed_bra(observed_bra)
  
  message('Italy')
  observed_ita <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "italy", template="one")
  observed_ita <- .process_observed_ita(observed_ita, observed_owid, adult_population, model_parameters)
  
  message('India')
  observed_ind <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "india", template="one")
  observed_ind <- .process_observed_ind(observed_ind)
  
  message('Germany')
  observed_ger <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "germany", template="one")
  observed_ger <- .process_observed_ger(observed_ger)
  
  message('Spain')
  observed_esp <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "spain", template="one")
  observed_esp <- .process_observed_esp(observed_esp, adult_population)
  
  # These datasets are loaded to add second booster (booster_2) information
  message('ECDC')
  observed_ecdc <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "europe", template = "one")
  observed_ecdc <- .process_observed_ecdc(observed_ecdc, observed_owid)
  
  message('CDC age stratified (for boosters)')
  observed_age <- model_inputs_data$load_new_vaccination_data(model_inputs_root, type = "age")
  observed_cdc_age <- observed_age[source == 'cdc']; rm(observed_age)
  # chk <- copy(observed_cdc_age)
  # observed_cdc_age <- copy(chk)
  observed_cdc <- .process_observed_cdc_age(observed_cdc_age, observed_cdc)
  
  message('South Africa')
  # Adding for NPIs requirements 2022-08-18
  observed_saf <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "southafrica", template="one")
  observed_saf <- .process_observed_safr(observed_saf, observed_owid, over5_population)
  
  message('Australia')
  # Adding second boosters 2022-12-11 - directly replace OWiD data
  observed_aus <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "australia", template = "one")
  observed_owid <- .process_observed_aus(observed_aus, observed_owid)
  rm(observed_aus)
  
  message('Fixing start dates') 
  ## USA, Spain and Germany have late start dates for subs
  start_ger <- min(observed_owid[location_id == 81, date])
  # start_esp <- min(observed_owid[location_id == 92, date])
  start_cdc <- as.Date("2020-12-20")
  observed_ger <- .fix_late_start_date_for_subs(observed_ger, start_ger, 'ger', hierarchy)
  observed_cdc <- .fix_late_start_date_for_subs(observed_cdc, start_cdc, 'cdc', hierarchy)
  
  
  # Make Full Observed  --------------------------------------------------------
  
  
  message('Merging')
  keep_cols <- c("location_id","date",
                 "reported_vaccinations","fully_vaccinated","people_vaccinated",
                 "boosters_administered", "booster_1", "booster_2", "data_filename")
  observed_locations <- c(observed_owid,observed_cdc,observed_can, observed_mex,
                          observed_ger,observed_esp,observed_ita,observed_ind,observed_bra,
                          observed_ecdc, observed_saf)
  # make observed data
  # More detailed data available
  drop_from_owid <- c(92, #Italy
                      86, #Spain
                      163, #India
                      135, # Brazil
                      196, # South Africa
                      unique(observed_ecdc$location_id) # ECDC w/ booster_2 data
  )
  
  observed <- rbind(observed_owid[!(location_id %in% drop_from_owid), ..keep_cols], 
                    observed_cdc[, ..keep_cols],
                    observed_can[, ..keep_cols],
                    observed_mex[, ..keep_cols],
                    observed_ger[, ..keep_cols],
                    observed_esp[, ..keep_cols],
                    observed_ita[, ..keep_cols],
                    observed_ind[, ..keep_cols],
                    observed_bra[, ..keep_cols],
                    observed_ecdc[, ..keep_cols],
                    observed_saf[, ..keep_cols],
                    fill = T)
  
  message('Processing and hierarchy stuff')
  observed <- .process_observed_data(observed)
  
  # Booster tails ----
  # Allows more locations to get booster correction w/o full booster time series
  observed <- .build_booster_tails(observed, model_parameters$booster_courses)
  
  # make observed children
  obs_children <- .make_observed_children(observed, adult_population, hierarchy)
  
  observed <- rbind(
    observed,
    obs_children[,c("location_id","date","observed","reported_vaccinations","daily_reported_vaccinations","people_vaccinated","boosters_administered",
                    "min_date","max_date","fully_vaccinated", "ratio_reported_people", "booster_1", "booster_2", "data_filename")]
  )
  observed[, daily_reported_vaccinations := reported_vaccinations - shift(reported_vaccinations, 1, fill = 0), by = "location_id"]
  
  # Prep for start date shift
  
  parent_loc_min_dates <- observed[location_id %in% hierarchy[level == 3 & most_detailed == 0, location_id]]
  parent_loc_min_dates <- parent_loc_min_dates[!is.na(reported_vaccinations),
                                               lapply(.SD, function(x) min(x)),
                                               by = "location_id",
                                               .SDcols = "date"]
  # Merge with children
  setnames(parent_loc_min_dates, "location_id","parent_id")
  parent_loc_min_dates <- merge(hierarchy[,c("location_id","parent_id")], parent_loc_min_dates, by = "parent_id")
  loc_min_dates <- observed[!is.na(reported_vaccinations),
                            lapply(.SD, function(x) min(x)),
                            by = "location_id",
                            .SDcols = "date"]
  
  reported_start_dates <- rbind(
    parent_loc_min_dates[,c("location_id","date")],
    loc_min_dates[!(location_id %in% parent_loc_min_dates$location_id)]
  )
  reported_start_dates$reported_vaccinations <- 1
  
  # Drop the UK, Argentina, & Canada
  
  reported_start_dates <- reported_start_dates[!(location_id %in% hierarchy[parent_id %in% c(95, 101, 97, 81, 92, 86), location_id])]
  
  # Prep for shifting levels
  observed[, max_date := max(date), by = "location_id"]
  observed[, max_daily_reported := max(daily_reported_vaccinations[date != min_date]), by = "location_id"]
  last_reported_cumulative <- observed[date == max_date]
  
  # Merge with children
  
  parent_level_shift <- last_reported_cumulative[location_id %in% c(hierarchy[level == 3 & most_detailed == 0, location_id], 570)]
  
  # Drop US, UK, Canada, Germany, Italy, Spain, Brazil, China since more detailed info available
  parent_level_shift <- parent_level_shift[!(location_id %in% c(102, 95, 101, 81, 92, 86, 135, 6))]
  parent_level_shift <- merge(parent_level_shift, adult_population, by = "location_id")
  setnames(parent_level_shift, c("location_id","adult_population"), c("parent_id", "country_pop"))
  parent_level_shift <- merge(hierarchy[,c("location_id","parent_id")], parent_level_shift, by = "parent_id")
  parent_level_shift <- merge(parent_level_shift, adult_population, by = "location_id")
  parent_level_shift[, reported_vaccinations := adult_population / country_pop * reported_vaccinations]
  parent_level_shift[, max_daily_reported := adult_population / country_pop * max_daily_reported]
  
  # Keep only most detailed
  last_reported_cumulative <- last_reported_cumulative[location_id %in% hierarchy[most_detailed == 1, location_id]]
  
  ## Fill missing "fully_vaccinated"
  fill_fully_vaccinated <- observed_owid[!is.na(fully_vaccinated), lapply(.SD, function(x) median(x / reported_vaccinations)),
                                         by = "date",
                                         .SDcols = "fully_vaccinated"]
  setnames(fill_fully_vaccinated, "fully_vaccinated","mean_fully_vaccinated_prop")
  
  observed <- merge(observed, fill_fully_vaccinated, by = "date", all.x = T)
  observed[, last_observed_ratio := tail(ratio_reported_people[!is.na(ratio_reported_people)],1), by = "location_id"]
  
  # Test for Albania, Australia, UAE, Ethiopia, -Inf
  observed[, shift_full := fully_vaccinated - shift(fully_vaccinated), by = "location_id"]
  observed[, has_inf := ifelse(max(people_vaccinated) == 0, 1, 0), by = "location_id"]
  observed[, last_obs := ifelse(tail(shift_full, 1) == 0, 1, 0), by = "location_id"]
  fill_in_locations <- c(43, 71, 156, 179, 145,
                         
                         unique(observed[has_inf == 1]$location_id)
                         )
  
  # exclude China and all children from being filled - introduces outlier
  ignore_locs <- children_of_parents(6, hierarchy, "loc_ids", include_parent = TRUE)
  fill_in_locations <- fill_in_locations[!(fill_in_locations %in% ignore_locs)]
  
  # invert this
  observed[, last_observed_ratio := (1 - 1 / last_observed_ratio)]
  observed[is.na(last_observed_ratio), last_observed_ratio := mean_fully_vaccinated_prop]
  # observed[shift_full == 0 & location_id %in% fill_in_locations, fully_vaccinated := reported_vaccinations * mean_fully_vaccinated_prop]
  observed[shift_full == 0 & location_id %in% fill_in_locations, fully_vaccinated := reported_vaccinations * last_observed_ratio]
  observed[shift_full == 0 & location_id %in% fill_in_locations, people_vaccinated := reported_vaccinations - fully_vaccinated]
  observed[, shift_full := NULL]
  #
  observed[, last_observed_ratio := NULL]
  observed[, mean_fully_vaccinated_prop := NULL]
  
  observed[, daily_fully_vaccinated := fully_vaccinated - shift(fully_vaccinated, 1, fill = 0), by = "location_id"]
  observed[daily_fully_vaccinated < 0 | is.na(daily_fully_vaccinated), daily_fully_vaccinated := 0]
  observed[, daily_first_vaccinated := people_vaccinated - shift(people_vaccinated, 1, fill = 0), by = "location_id"]
  
  ## Add manual start date where known
  known_start_date <- model_inputs_data$load_manual_start_dates(model_inputs_root)
  
  # Something odd when obs < 3
  observed[, loc_rows := .N, by = "location_id"]
  observed <- observed[loc_rows > 2]
  observed[, loc_rows := NULL]
  
  observed <- rbind(
    observed,
    known_start_date[!(location_id %in% unique(observed$location_id))],
    fill = T
  )
  
  
  # This checks for outliers introduced if Chinese subnats are not included in ignore_locs above ~60 lines
  
  # pdf(file.path(vaccine_output_root, "process_observed_vax_china_provinces.pdf"), onefile = TRUE)
  # 
  # for (i in children_of_parents(6, hierarchy, "loc_ids", include_parent = T)) {
  #   
  #   x <- observed[location_id == i,]
  #   
  #   p <- x %>% tidyr::pivot_longer(cols = c("reported_vaccinations", "people_vaccinated", "fully_vaccinated", "boosters_administered")) %>%
  #     ggplot(aes(x=date, y=value, color=name)) +
  #     geom_line(na.rm = T) +
  #     geom_hline(data = adult_population[location_id==i], aes(yintercept = adult_population)) +
  #     ggtitle(x$location_id) +
  #     theme_minimal_hgrid()
  #   
  #   print(p)
  #   
  # }
  # 
  # dev.off()
  
  
  
  
  ############# Update: empirical delay between doses #####################
  # The empirical lags are no longer used in the uptake model but it appears the
  # pipeline will still break whithout this file written. Should see if this can indeed
  # be removed down the track
  .make_write_empirical_delay(observed = observed, delay_days = 7, vaccine_output_root, hierarchy)

  ## This is what the model will attempt to track
  observed[is.na(daily_first_vaccinated), daily_first_vaccinated := daily_reported_vaccinations - daily_fully_vaccinated]
  observed[, daily_people_vaccinated := people_vaccinated - shift(people_vaccinated), by = "location_id"]
  # observed <- observed[daily_reported_vaccinations >= 0 & daily_first_vaccinated >= 0 | manual_start == 1]
  observed <- observed[!(daily_reported_vaccinations < 0 | daily_first_vaccinated < 0) | manual_start == 1]
  
  # I don't know why this is here but it needs to go - Georgia state
  observed <- observed[!(date == "2021-05-07" & location_id == 533 & reported_vaccinations == 6838037)]
  observed <- observed[!(date == "2021-05-29" & location_id == 533 & reported_vaccinations == 159209)]
  
  # Something odd when obs < 3
  observed[, loc_rows := .N, by = "location_id"]
  observed <- observed[loc_rows > 2 | manual_start == 1]
  observed[, loc_rows := NULL]
  
  observed[, max_date := max(date), by = "location_id"]
  
  message(paste('Max date of observed vaccination coverage data:', max(observed$date[observed$observed == 1], na.rm=T)))
  
  # Write Output File  ---------------------------------------------------------
  
  ## Cleanup and Validation ----
  observed <- rbindlist( # refill for plotting
    lapply(split(observed, observed$location_id), function(x) {
      x$location_name <- .get_column_val(x$location_name)
      x$data_filename <- .get_column_val(x$data_filename)
      return(x)
    })
  )
  
  observed <- observed[!is.na(location_id)]
  
  .validate_key(dt = observed, 
                key_cols = c("location_id", "date"), 
                script_name = "process_observed_vaccinations.R", 
                dt_name =  "observed")
  
  vaccine_data$write_observed_vaccinations(observed, vaccine_output_root)
  
  
  ## Splice locations ----------------------------------------------------------
  
  if(length(model_parameters$splice_locs)) {
    
    splice_locs <- model_parameters$splice_locs
    
    # labels for message
    splice_loc_labels <- hierarchy[location_id %in% splice_locs, .(location_id, location_name)]
    splice_loc_labels[, labels := paste(location_id, ":", location_name)]
    splice_loc_labels <- splice_loc_labels[['labels']]
    splice_loc_labels <- paste0(splice_loc_labels, collapse = ", ")
    message("Splicing : ", splice_loc_labels)
    
    # splice all time series from last best version
    old_vaccine_root <- file.path(model_parameters$previous_best_path)
    new_raw <- fread(file.path(vaccine_output_root, "observed_data.csv"))
    old_raw <- fread(file.path(old_vaccine_root, "observed_data.csv"))
    new_raw <- new_raw[!location_id %in% splice_locs]
    new_raw <- rbind(new_raw, old_raw[location_id %in% splice_locs], fill = T) # columns may differ between versions
    # Overwrite file
    vaccine_data$write_observed_vaccinations(new_raw, vaccine_output_root =  .output_path)
    # clean-up
    rm(new_raw, old_raw, old_vaccine_root, splice_locs) 
    
  }
  
  ## Plots ----
  
  # Previous best overlapping current data time series plots
  .submit_job(script_path = file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, 'plot_observed_processed_data.R'),
              runtime = '15',
              archiveTF = F,
              args_list = list('--output_path' = .output_path,
                               '--previous_best_path' = .previous_best_path))
  
  # Previous best vs. current scatter plots
  message("Preparing data for scatters (scatter_plot_prep)")
  plot_dt <- .scatter_prep_observed(vaccine_output_root)
  
  plot_cols <- c("reported_vaccinations", "fully_vaccinated", 
                 "people_vaccinated", "boosters_administered", "booster_1", "booster_2")
  
  pdf(file.path(vaccine_output_root, 
                glue("observed_scatter_{.model_inputs_version}_v_{.previous_best_version}.pdf")),
      height=10, width=15)
  
  for (i in plot_cols){
    .scatter_plot_compare(DATASET = plot_dt, PERCENT_DIFF = 0.5, VARIABLE = i)
  }
  
  dev.off()
  
}


# -----------------------------------------------------------------------------#


# Get Observed Methods  --------------------------------------------------------

.process_observed_owid <- function(observed_owid, adult_population, hierarchy) {
  
  
  setnames(
    observed_owid,
    c("initially_vaccinated", "total_administered"),
    c("people_vaccinated" , "reported_vaccinations"))
  
  data_cols <- grep("vaccin", names(observed_owid), value = T) # for interpolation, boosters handled separately
  refill_cols <- grep("location|age|brand|filename", names(observed_owid), value = T)
  
  #observed_owid[is.na(fully_vaccinated), fully_vaccinated := reported_vaccinations - people_vaccinated]
  
  # Georgia state location_id is being used....Location
  observed_owid[location_id == 533, location_id := 35]
  
  # add a row for UAE (https://ourworldindata.org/covid-vaccinations?country=OWID_WRL)
  observed_owid <- rbind(observed_owid[!(location_id == 156 & date == "2021-04-20")],
                         data.table(location_id = 156,
                                    date = as.Date("2021-04-20"),
                                    people_vaccinated = 5080000,
                                    fully_vaccinated = 4700000),
                         fill = T)
  
  
  
  if (F) {
    
    x <- observed_owid[location_id == 133]
    plot(x$date, x$reported_vaccinations, main=unique(x$location_name))
    points(x$date, x$people_vaccinated, col='blue')
    points(x$date, x$fully_vaccinated, col='red')
    points(x$date, x$boosters_administered, col='green')
    
  }
  
  
  observed_owid <- do.call(
    rbind,
    lapply(split(observed_owid, by='location_id'), function(x) {
      
      # The current location
      x_loc <- .get_column_val(x$location_name)
      
      
      pakistan_and_subnats <- hierarchy[location_id == 165 | parent_id == 165]$location_name
      
      tryCatch( {
        
        # Location specific fixes
        if (x_loc == 'Turkmenistan') {
          
          t <- as.Date(range(observed_owid$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          
          x[x$date == min(observed_owid$date), c('people_vaccinated', 'fully_vaccinated', 'reported_vaccinations')] <- 0
          
          x$reported_vaccinations <- .do_sqrt_interp(x$reported_vaccinations)
          x$people_vaccinated <- .do_sqrt_interp(x$people_vaccinated)
          x$fully_vaccinated <- .do_sqrt_interp(x$fully_vaccinated)
          
          
        } else if (x_loc == 'Afghanistan') {
          
          # This will likely change. THe folowing fixe adjusts AFG data to match what is curenntly
          # shown by OWiD website and google plots of OWiD
          t <- as.Date(range(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          
          # Pin down early part of time series
          sel <- 1:which.min(x$fully_vaccinated)
          x$fully_vaccinated[sel] <- x$reported_vaccinations[sel] - x$people_vaccinated[sel]
          
          # Interp the rest
          x$reported_vaccinations <- zoo::na.approx(x$reported_vaccinations, na.rm=F)
          x$people_vaccinated <- zoo::na.approx(x$people_vaccinated, na.rm=F)
          x$fully_vaccinated <- zoo::na.approx(x$fully_vaccinated, na.rm=F)
          
          # Fully vax is shorter - must extend the end values
          x$fully_vaccinated <- .extend_end_values(x$fully_vaccinated)
          
        } else if (x_loc == 'Kuwait') {
          
          t <- as.Date(range(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          
          x$reported_vaccinations <- zoo::na.approx(x$reported_vaccinations, na.rm=F)
          
          sel <- which.min(x$people_vaccinated)
          x$people_vaccinated[1:sel] <- x$reported_vaccinations[1:sel]
          x$fully_vaccinated[1:sel] <- 0
          
          x$people_vaccinated <- zoo::na.approx(x$people_vaccinated, na.rm=F)
          x$fully_vaccinated <- zoo::na.approx(x$fully_vaccinated, na.rm=F)
          
          
        } else if (x_loc == 'Niue') {
          
          t <- as.Date(range(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          
          x$reported_vaccinations <- zoo::na.approx(x$reported_vaccinations, na.rm=F)
          
          # Use latest proportion of initial to fully from neighboring Fiji
          tmp <- observed_owid[location_name == 'Fiji']
          sel <- which(!is.na(tmp$fully_vaccinated))
          den <- tmp$people_vaccinated[sel] + tmp$fully_vaccinated[sel]
          prop_fully_vaccinated <- max(tmp$fully_vaccinated[sel]/den)
          
          x$people_vaccinated <- x$reported_vaccinations * (1 - prop_fully_vaccinated)
          x$fully_vaccinated <- x$reported_vaccinations * prop_fully_vaccinated
          
          
        } else if (x_loc %in% pakistan_and_subnats) {
          
          t <- as.Date(range(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          
          x$reported_vaccinations <- zoo::na.approx(x$reported_vaccinations, na.rm=F)
          x$people_vaccinated <- zoo::na.approx(x$people_vaccinated, na.rm=F)
          
          sel <- 1:which.min(x$fully_vaccinated)
          x$fully_vaccinated[sel] <- x$reported_vaccinations[sel] - x$people_vaccinated[sel]
          x$fully_vaccinated <- zoo::na.approx(x$fully_vaccinated, na.rm=F)
          
        } else if (x_loc %in% hierarchy[location_id %in% c(6, 354, 361), location_name]) { 
          # China, Hong Kong, Macao
          
          t <- as.Date(range(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          x$brand <- .get_column_val(x$brand)
          x$age_start <- .get_column_val(x$age_start)
          x$age_end <- .get_column_val(x$age_end)
          x$data_filename <- .get_column_val(x$data_filename)
          
          x$reported_vaccinations <- zoo::na.approx(x$reported_vaccinations, na.rm=F)
          x$people_vaccinated <- zoo::na.approx(x$people_vaccinated, na.rm=F)
          x$fully_vaccinated <- zoo::na.approx(x$fully_vaccinated, na.rm=F)
          x$boosters_administered <- zoo::na.approx(x$boosters_administered, na.rm=F)
          
          prop_people_vaccinated <- x$people_vaccinated/x$reported_vaccinations
          sel <- min(which(!is.na(prop_people_vaccinated)))
          prop_people_vaccinated <- prop_people_vaccinated[sel]
          x$people_vaccinated[1:(sel-1)] <- x$reported_vaccinations[1:(sel-1)] * prop_people_vaccinated
          
          prop_fully_vaccinated <- x$fully_vaccinated/x$reported_vaccinations
          sel <- min(which(!is.na(prop_fully_vaccinated)))
          prop_fully_vaccinated <- prop_fully_vaccinated[sel]
          x$fully_vaccinated[1:(sel-1)] <- x$reported_vaccinations[1:(sel-1)] * prop_fully_vaccinated
          
          # Build boosters_administered
          sel <- which(x$date < as.Date('2021-07-27')) #https://www.bloomberg.com/news/articles/2021-08-27/china-greenlights-covid-19-booster-shots-for-high-risk-residents
          x$boosters_administered[sel] <- 0
          x$boosters_administered <- .do_sqrt_interp(x$boosters_administered)
          prop_booster <- x$boosters_administered / x$reported_vaccinations
          sel <- max(which(!is.na(prop_booster)))
          last_obs_prop <- prop_booster[sel]
          delta <- mean(diff(prop_booster)[(sel-5):(sel-1)])
          
          if (any(is.na(x$boosters_administered))) {
            deltas <- rep(delta, sum(is.na(x$boosters_administered)))
            prop_booster[is.na(prop_booster)] <- last_obs_prop * (1 + cumsum(tidyr::replace_na(deltas, 0)))
            x$boosters_administered <- x$reported_vaccinations * prop_booster
          }
          
          # Build booster_1
          sel <- which(x$date < as.Date('2021-07-27')) #https://www.bloomberg.com/news/articles/2021-08-27/china-greenlights-covid-19-booster-shots-for-high-risk-residents
          x$booster_1[sel] <- 0
          x$booster_1 <- .do_sqrt_interp(x$booster_1)
          prop_booster <- x$booster_1 / x$reported_vaccinations
          sel <- max(which(!is.na(prop_booster)))
          last_obs_prop <- prop_booster[sel]
          delta <- mean(diff(prop_booster)[(sel-5):(sel-1)])
          
          if (any(is.na(x$booster_1))) {
            deltas <- rep(delta, sum(is.na(x$booster_1)))
            prop_booster[is.na(prop_booster)] <- last_obs_prop * (1 + cumsum(tidyr::replace_na(deltas, 0)))
            x$booster_1 <- x$reported_vaccinations * prop_booster
          }
          
          
        } else if (x_loc == 'Mexico') {
          
          t <- as.Date(range(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          
          x$reported_vaccinations <- zoo::na.approx(x$reported_vaccinations)
          x$people_vaccinated <- zoo::na.approx(x$people_vaccinated)
          
          sel <- which(x$date == min(x$date[!is.na(x$fully_vaccinated)]))
          x$fully_vaccinated[1:(sel-1)] <- 0
          # truncate tailing NA - zoo::na.approx will fail
          sel <- which(x$date == max(x$date[!is.na(x$fully_vaccinated)]))
          x <- x[1:sel] 
          
          x$fully_vaccinated <- zoo::na.approx(x$fully_vaccinated, na.rm=T)
          
        } else if (x_loc %in% c('Kiribati', 'Nicaragua', 'Burundi', 'Papua New Guinea', 'Nauru',
                                'Venezuela (Bolivarian Republic of)',
                                'Mauritius', 'Tokelau', 'Bhutan', 'Djibouti', 'Cook Islands')) {
          
          t <- c(as.Date("2020-12-16"), max(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          x$brand <- .get_column_val(x$brand)
          x$age_start <- .get_column_val(x$age_start)
          x$age_end <- .get_column_val(x$age_end)
          
          x[x$date == t[1], c('reported_vaccinations', 'people_vaccinated')] <- 0
          
          x$reported_vaccinations <- .do_sqrt_interp(x$reported_vaccinations)
          x$people_vaccinated <- .do_sqrt_interp(x$people_vaccinated)
          
          prop <- x$people_vaccinated/x$reported_vaccinations
          prop[!is.finite(prop)] <- 0
          x$fully_vaccinated <- x$reported_vaccinations*(1-prop)
          
          
        } else if (x_loc %in% c('Venezuela (Bolivarian Republic of)')) {
          
          x <- observed_owid[location_id == 133]
          
          t <- c(as.Date("2020-12-16"), max(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          x$brand <- .get_column_val(x$brand)
          x$age_start <- .get_column_val(x$age_start)
          x$age_end <- .get_column_val(x$age_end)
          
          x[x$date == t[1], c('people_vaccinated', 'fully_vaccinated')] <- 0
          sel <- 1:(min(which(!is.na(x$boosters_administered)))-1)
          x[sel, 'boosters_administered'] <- 0
          
          x$people_vaccinated <- .do_sqrt_interp(x$people_vaccinated)
          x$fully_vaccinated <- .do_sqrt_interp(x$fully_vaccinated)
          x$boosters_administered <- .do_sqrt_interp(x$boosters_administered)
          
          x$people_vaccinated <- pmax(x$people_vaccinated, x$fully_vaccinated)
          x$reported_vaccinations <- rowSums(cbind(x$people_vaccinated, x$fully_vaccinated, x$boosters_administered))
          
          plot(x$date, x$reported_vaccinations, main=unique(x$location_name))
          points(x$date, x$people_vaccinated, col='blue')
          points(x$date, x$fully_vaccinated, col='red')
          points(x$date, x$boosters_administered, col='green')
          
          
        } else if (x_loc %in% c('Romania', 'Bulgaria', 'Zambia')) {
          
          # THis fix deals with fully > initially vaccinated
          
          t <- c(as.Date("2020-12-16"), max(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          x$brand <- .get_column_val(x$brand)
          x$age_start <- .get_column_val(x$age_start)
          x$age_end <- .get_column_val(x$age_end)
          
          x[x$date == t[1], c('reported_vaccinations', 'people_vaccinated', 'fully_vaccinated')] <- 0
          
          x$reported_vaccinations <- .do_sqrt_interp(x$reported_vaccinations)
          x$people_vaccinated <- .do_sqrt_interp(x$people_vaccinated)
          x$fully_vaccinated <- .do_sqrt_interp(x$fully_vaccinated)
          
          
          prop <- x$fully_vaccinated/x$reported_vaccinations
          prop[!is.finite(prop)] <- 0
          x$people_vaccinated <- x$reported_vaccinations*(1-prop)
          
          tmp <- x$fully_vaccinated * (1/0.9)
          x$people_vaccinated <- pmax(x$people_vaccinated, tmp, na.rm=TRUE)
          
          x$people_vaccinated <- pmin(x$people_vaccinated, 
                                      adult_population$adult_population[adult_population$location_id == x$location_id[1]], 
                                      na.rm=FALSE)
          
          x$fully_vaccinated <- pmin(x$fully_vaccinated, 
                                     adult_population$adult_population[adult_population$location_id == x$location_id[1]], 
                                     na.rm=FALSE)
          
          x$reported_vaccinations <- x$people_vaccinated + x$fully_vaccinated
          
        } else if (x_loc %in% c('Philippines')) {
          
          # Knock out spike in fully vax
          x[date %in% as.Date('2021-11-15'):as.Date('2021-11-22'), c('reported_vaccinations', 'people_vaccinated', 'fully_vaccinated')] <- NA
          
          t <- c(as.Date("2020-12-16"), max(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          x$brand <- .get_column_val(x$brand)
          x$age_start <- .get_column_val(x$age_start)
          x$age_end <- .get_column_val(x$age_end)
          
          x[x$date == t[1], c('reported_vaccinations', 'people_vaccinated', 'fully_vaccinated')] <- 0
          
          x$reported_vaccinations <- .do_sqrt_interp(x$reported_vaccinations)
          x$people_vaccinated <- .do_sqrt_interp(x$people_vaccinated)
          x$fully_vaccinated <- .do_sqrt_interp(x$fully_vaccinated)
          
          sel <- which.max(x$date[!is.na(x$fully_vaccinated)])
          prop <- x$fully_vaccinated[sel] / x$reported_vaccinations[sel]
          
          sel <- is.na(x$fully_vaccinated)
          x$fully_vaccinated[sel] <- x$reported_vaccinations[sel] * prop
          
          sel <- which.max(x$date[!is.na(x$people_vaccinated)])
          prop <- x$people_vaccinated[sel] / x$reported_vaccinations[sel]
          
          sel <- is.na(x$people_vaccinated)
          x$people_vaccinated[sel] <- x$reported_vaccinations[sel] * prop
          
          x$reported_vaccinations <- x$people_vaccinated + x$fully_vaccinated
          
          
        } else if (x_loc == 'Denmark') {
          
          t <- c(as.Date("2020-12-16"), max(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          x$brand <- .get_column_val(x$brand)
          x$age_start <- .get_column_val(x$age_start)
          x$age_end <- .get_column_val(x$age_end)
          
          x[x$date == t[1], c('reported_vaccinations', 'people_vaccinated', 'fully_vaccinated')] <- 0
          x$reported_vaccinations <- .do_sqrt_interp(x$reported_vaccinations)
          x$people_vaccinated <- .do_sqrt_interp(x$people_vaccinated)
          x$fully_vaccinated <- .do_sqrt_interp(x$fully_vaccinated)
          
        } else if (x_loc == 'Vanuatu') {
          
          t <- as.Date(range(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x[, (data_cols) := lapply(.SD, .do_sqrt_interp), .SDcols = data_cols]
          x[, (refill_cols) := lapply(.SD, .get_column_val), .SDcols = refill_cols]
          
        } else if (x_loc == 'Austria') {
          
          
          # next three locations are cumulative in raw data, post processing produces cryptic negatives
          # these processing steps are unnecessary, not-harmful, and retained for later investigation as time allows
          
          t <- as.Date(range(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x[, (refill_cols) := lapply(.SD, .get_column_val), .SDcols = refill_cols]
          
          x$people_vaccinated <- floor(.make_cumulative(x$people_vaccinated))
          # x$fully_vaccinated looks like garbage, but scenario model takes care of it
          
        } else if (x_loc == 'Algeria') {
          
          t <- as.Date(range(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x[, (data_cols) := lapply(.SD, .do_sqrt_interp), .SDcols = data_cols]
          x[, (refill_cols) := lapply(.SD, .get_column_val), .SDcols = refill_cols]
          
        } else if (x_loc == 'Ethiopia') {
          
          t <- as.Date(range(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x[, (data_cols) := lapply(.SD, .do_sqrt_interp), .SDcols = data_cols]
          x[, (refill_cols) := lapply(.SD, .get_column_val), .SDcols = refill_cols]
          
        } else if (x_loc == 'Luxembourg') {
          # this replicates what ultimately ends up in the slow scenario, but preserves the shape of the raw people_vaccinated curve
          
          # OWID stopped reporting fully_vaccinated (J&J counting issue)
          # https://github.com/owid/covid-19-data/issues/1963
          x$fully_vaccinated <- x$people_vaccinated * 0.95
        
        } else if (x_loc == 'England') {
          
          # Separate booster_2 from booster_1 - Autumn booster campaign
          # https://www.england.nhs.uk/2022/11/nhs-protects-more-than-half-of-those-due-autumn-booster/
          # https://www.gov.uk/government/publications/covid-19-vaccination-autumn-booster-resources/a-guide-to-the-covid-19-autumn-booster
          
          # get boosters in daily space, and assign those after 2022-09-01 to booster_2 (booster_1 will not increase further)
          x[, b1_daily := c(0, diff(booster_1))]
          x[date < "2021-10-01" & is.na(b1_daily), b1_daily := 0]
          x$booster_2 <- as.numeric(x$booster_2)
          x[date >= "2022-09-01", booster_2 := cumsum(b1_daily)]
          last_booster_1_value <- x[date == "2022-08-31", booster_1]
          x[date >= "2022-09-01", booster_1 := last_booster_1_value]
          # clean up
          x[, b1_daily := NULL]
          
        }
        
      }, error=function(e){
        
        cat("ERROR :", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(e), "\n")
        
      })
      
      return(x)
    })
  )
  
  if (F) {
    
    for (i in c('Afghanistan', 'Niue', 'Kuwait', 'Philippines')) {
      
      x <- observed_owid[location_name == i]
      plot(x$date, x$reported_vaccinations, main=unique(x$location_name))
      points(x$date, x$people_vaccinated, col='blue')
      points(x$date, x$fully_vaccinated, col='red')
      points(x$date, x$boosters_administered, col='green')
      
    }
  }
  
  return(observed_owid)
}


.make_missing_china_subnats <- function(observed_owid, 
                                        adult_population, 
                                        hierarchy,
                                        vaccine_output_root) {
  
  message('Adding missing subnational data for China')
  
  vax_quantities <- colnames(observed_owid)[colnames(observed_owid) %like% 'vaccinated' | 
                                              colnames(observed_owid) %like% 'vaccinations' | 
                                              colnames(observed_owid) %like% 'booster']
  
  observed_chn <- fread('FILEPATH/china_vaccination_age.csv') # point estimates, age stratified
  if ('initially_vaccinated' %in% colnames(observed_chn)) setnames(observed_chn, "initially_vaccinated", "people_vaccinated")
  if ('total_administered' %in% colnames(observed_chn)) setnames(observed_chn, "total_administered", "reported_vaccinations")
  if ('additional_dose' %in% colnames(observed_chn)) setnames(observed_chn, "additional_dose", "booster_1")
  if ('second_additional_dose' %in% colnames(observed_chn)) setnames(observed_chn, "second_additional_dose", "booster_2")
  observed_chn <- observed_chn[age_start == 0 & age_end == 125,]
  observed_chn$date <- .convert_dates(observed_chn$date)
  observed_chn[, paste0("booster_", 1:2) := lapply(.SD, as.numeric), .SDcols = paste0("booster_", 1:2)]
  observed_chn[is.na(booster_1), booster_1 := boosters_administered]
  observed_chn[is.na(booster_2), booster_2 := 0]
  
  
  if (length(unique(observed_chn$date)) > 1) stop('CHINA CDC DATA HAVE MORE THAN ONE DATE -- MUST UPDATE DATA PROCESSING')
  
  if (!(44533 %in% observed_chn$location_id)) { # aggregate a Mainland China point estimate from provinces for one day in time
    
    tmp <- as.data.frame(observed_chn[1,])
    tmp$people_vaccinated <- sum(observed_chn$people_vaccinated)
    tmp$fully_vaccinated <- sum(observed_chn$fully_vaccinated)
    tmp$reported_vaccinations <- sum(observed_chn$reported_vaccinations)
    tmp$boosters_administered <- sum(observed_chn$boosters_administered)
    tmp$booster_1 <- sum(observed_chn$boosters_administered) # Temp fix without data
    tmp$population <- sum(observed_chn$population)
    tmp$location_id <- 44533
    tmp$location_name <- hierarchy[location_id==44533, location_name]
    
    observed_chn <- as.data.table(rbind(tmp, observed_chn))
    
  }
  
  china_provinces_cdc <- observed_chn[observed_chn$location_id == 44533] # borrow for plotting, date use
  x <- observed_owid[location_id == 6,] # borrow time series to create provinces
  
  # diagnostic plot
  if (F) {
    plot(x$date, x$reported_vaccinations, main=unique(x$location_name))
    abline(h=adult_population[location_id==6, adult_population])
    points(x$date, x$people_vaccinated, col='blue')
    points(x$date, x$fully_vaccinated, col='red')
    points(x$date, x$boosters_administered, col='green')
    # compare 44533 from CCDC to 6 from OWiD)
    points(china_provinces_cdc$date, china_provinces_cdc$people_vaccinated, col='blue', pch=2)
    points(china_provinces_cdc$date, china_provinces_cdc$fully_vaccinated, col='red', pch=2)
    points(china_provinces_cdc$date, china_provinces_cdc$boosters_administered, col='green', pch=2)
  }
  
  
  
  # Another way, if you like it better, is to use "Sys.setenv()" and set an environment variable.
  R.utils::setOption('include_china_mainland', FALSE)
  if (R.utils::getOption('include_china_mainland')){
    owid_pt <- x[date == china_provinces_cdc$date[1],]
    prop <- china_provinces_cdc[,..vax_quantities] / owid_pt[,..vax_quantities]
    x$people_vaccinated <- x$people_vaccinated * prop[, people_vaccinated]
    x$fully_vaccinated <- x$fully_vaccinated * prop[, fully_vaccinated]
    x$reported_vaccinations <- x$reported_vaccinations * prop[, reported_vaccinations]
    x$boosters_administered <- x$boosters_administered * prop[, boosters_administered]
    x$location_id <- 44533
    x$location_name <- hierarchy[location_id == 44533, location_name]
    
    # Borrow OWiD time series for China, and create a proportional series for each province 
    # Adjust time series based on proportions calculated from point estimate China CDC data
  } 
  china_province_list <- vector("list", nrow(observed_chn))
  
  for (i in 1:nrow(observed_chn)){
    x <- observed_owid[location_id == 6] # time series for provinces, based on proportions calculated from point estimate China CDC data
    owid_pt <- x[date == observed_chn$date[1],]
    prop <- observed_chn[i, ..vax_quantities] / owid_pt[location_id == 6, ..vax_quantities]
    x$people_vaccinated <- x$people_vaccinated * prop[, people_vaccinated]
    x$fully_vaccinated <- x$fully_vaccinated * prop[, fully_vaccinated]
    x$reported_vaccinations <- x$reported_vaccinations * prop[, reported_vaccinations]
    x$boosters_administered <- x$boosters_administered * prop[, boosters_administered]
    x$booster_1 <- x$booster_1 * prop[, booster_1]
    x$booster_2 <- x$booster_2 * prop[, booster_2]
    
    x$location_id <- observed_chn[i, location_id]
    x$location_name <- hierarchy[location_id == observed_chn[i, location_id], location_name]
    china_province_list[[i]] <- x
  }
  
  chn_provinces <- rbindlist(china_province_list)
  chn_provinces$data_filename <- "calculated"
  
  if(any(unique(chn_provinces$location_id) %in% unique(observed_owid$location_id))){
    stop("One of the Chinese chn_provinces is already in observed_owid")
  }
  observed_owid <- rbind(observed_owid, chn_provinces)
  
  # observed_owid <- rbind(observed_owid, x)
  
  # Diagnostic plots for China, Hong Kong, Macao, Anhui
  # par(mfrow=c(2,2)) # plotting subset of China subnats at different levels
  # for (i in c(44533, 354, 361, 491)) {
  # 
  # x <- observed_owid[location_id == i,]
  # plot(x$date, x$reported_vaccinations, main=unique(x$location_name))
  # abline(h=adult_population[location_id == i, adult_population])
  # points(x$date, x$people_vaccinated, col='blue')
  # points(x$date, x$fully_vaccinated, col='red')
  # points(x$date, x$boosters_administered, col='green')
  # 
  # }
  # par(mfrow=c(1,1))
  
  return(observed_owid)
  
}


.process_observed_cdc <- function(observed_cdc, adult_population, hierarchy) {
  
  observed_cdc[observed_cdc$location_name == 'NewHampshire', 'location_name'] <- 'New Hampshire'
  observed_cdc[observed_cdc$location_name == 'NewJersey', 'location_name'] <- 'New Jersey'
  
  # Drop Palau (untrustworthy)
  observed_cdc <- observed_cdc[!(location_id %in% c(380))]
  
  setnames(observed_cdc, c("total_administered","initially_vaccinated"), c("reported_vaccinations","people_vaccinated"))
  
  observed_cdc[, date := date - 3] # Shift states by 3 day lag (per INDIVIDUAL_NAME)
  
  #x <- observed_cdc[location_id == 552,]
  
  do.call(
    rbind,
    lapply(split(observed_cdc, by='location_id'), function(x) {
      
      x_loc <- .get_column_val(x$location_id)
      
      tryCatch( {
        
        if (x_loc %in% c(552, 553)) { # New Hampshire, New Jersey
          
          t <- as.Date(range(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          
          # Fill in reported vax
          x$reported_vaccinations <- zoo::na.approx(x$reported_vaccinations, na.rm=F)
          x$reported_vaccinations <- floor(.make_cumulative(x$reported_vaccinations))
          
          # Anchor initial and fully
          sel <- which(x$date == min(x$date[!is.na(x$reported_vaccinations)]))
          x$people_vaccinated[sel] <- x$fully_vaccinated[sel] <- 0
          
          x$people_vaccinated <- zoo::na.approx(x$people_vaccinated, na.rm=F)
          x$people_vaccinated <- floor(.make_cumulative(x$people_vaccinated))
          
          x$fully_vaccinated <- zoo::na.approx(x$fully_vaccinated, na.rm=F)
          x$fully_vaccinated <- floor(.make_cumulative(x$fully_vaccinated))
          
          
        } else if (x_loc %in% c(536, 540)) { # Illinois, Kentucky
          
          t <- as.Date(range(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          
          # Fill in reported vax
          x$reported_vaccinations <- zoo::na.approx(x$reported_vaccinations, na.rm=F)
          x$reported_vaccinations <- floor(.make_cumulative(x$reported_vaccinations))
          
          x[x$date >= '2021-10-19' , c('people_vaccinated', 'fully_vaccinated')] <- NA
          
          prop <- x$people_vaccinated/x$reported_vaccinations
          prop <- prop[!is.na(prop)]
          
          sel <- x$date < min(x$date[!is.na(x$people_vaccinated)])
          x$people_vaccinated[sel] <- x$reported_vaccinations[sel]*prop[1]
          
          sel <- x$date > max(x$date[!is.na(x$people_vaccinated)])
          x$people_vaccinated[sel] <- x$reported_vaccinations[sel]*prop[length(prop)]
          
          x$people_vaccinated <- zoo::na.approx(x$people_vaccinated, na.rm=F)
          x$people_vaccinated <- floor(.make_cumulative(x$people_vaccinated))
          
          sel <- is.na(x$fully_vaccinated)
          x$fully_vaccinated <- x$reported_vaccinations - x$people_vaccinated
          
        } else if (x_loc == 376) { # Northern Mariana Island
          
          # Data for Northern Mariana Islands missing, infer from Guam
          tmp <- observed_cdc[location_name == 'Guam']
          tmp <- merge(tmp, adult_population, by='location_id', all.x=TRUE)
          
          tmp$prop_reported <- tmp$reported_vaccinations / tmp$adult_population
          tmp$prop_boost <- tmp$boosters_administered / tmp$adult_population
          
          sel <- which(!is.na(tmp$fully_vaccinated))
          den <- tmp$people_vaccinated[sel] + tmp$fully_vaccinated[sel]
          prop_fully <- max(tmp$fully_vaccinated[sel]/den)
          
          
          # Take Guam proportions and out Northern Mariana Islands
          tmp$adult_population <- adult_population[location_id == 376, adult_population]
          tmp$reported_vaccinations <- tmp$adult_population * (tmp$prop_reported*0.65) # NMI lagging behind Guam
          tmp$boosters_administered <- tmp$adult_population * (tmp$prop_boost*0.65)
          tmp$people_vaccinated <- tmp$reported_vaccinations * (1 - prop_fully)
          tmp$fully_vaccinated <- tmp$reported_vaccinations * prop_fully
          tmp$location_id <- x_loc
          tmp$location_name <- hierarchy[location_id == x_loc, location_name]
          
          x <- tmp; rm(tmp)
          sel <- colnames(x)[colnames(x) %in% colnames(observed_cdc)]
          x <- x[,..sel]
          
          
        # } else if (x_loc == 571) { # West Virginia
        #   
        #   t <- as.Date(range(x$date))
        #   if (t[2] < as.Date('2022-01-03')) t[2] <- as.Date('2022-01-03')
        #   
        #   x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
        #   x$location_id <- .get_column_val(x$location_id)
        #   x$location_name <- .get_column_val(x$location_name)
        #   x$brand <- .get_column_val(x$brand)
        #   x$age_start <- .get_column_val(x$age_start)
        #   x$age_end <- .get_column_val(x$age_end)
        #   x$data_filename <- .get_column_val(x$data_filename)
        #   
        #   cut_date <- as.Date('2021-11-12')
        #   x[x$date > cut_date, c('people_vaccinated', 'fully_vaccinated', 'reported_vaccinations', 'boosters_administered')] <- NA
        #   
        #   point_est <- 1106807 # Last reported point estimate from https://dhhr.wv.gov/COVID-19/Pages/default.aspx
        #   point_est_date <- as.Date('2022-01-03') 
        #   x[x$date == point_est_date, 'people_vaccinated'] <- point_est
        #   
        #   # Infer other quantities
        #   x$people_vaccinated <- .do_sqrt_interp(x$people_vaccinated)
        #   
        #   sel <- is.na(x$reported_vaccinations)
        #   
        #   ratio <- x$reported_vaccinations/x$people_vaccinated
        #   x$reported_vaccinations[sel] <- x$people_vaccinated[sel] * ratio[x$date == cut_date]
        #   
        #   ratio <- x$fully_vaccinated/x$people_vaccinated
        #   x$fully_vaccinated[sel] <- x$people_vaccinated[sel] * ratio[x$date == cut_date]
        #   
        #   ratio <- x$boosters_administered/x$people_vaccinated
        #   x$boosters_administered[sel] <- x$people_vaccinated[sel] * ratio[x$date == cut_date]
          
        } else {
          
          #
          
        }
        
      }, error=function(e){
        
        cat("ERROR :", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(e), "\n")
        
      })
      
      return(x)
    })
  )
  
}


.process_observed_can <- function(observed_can) {
  setnames(observed_can, "initially_vaccinated", "people_vaccinated")
  
  
  observed_can[, reported_vaccinations := people_vaccinated + fully_vaccinated]
  
  # Booster Authorization dates
  # booster_1 
  # first authorized 2021-11-09 (Pfizer) https://www.canada.ca/en/health-canada/services/drugs-health-products/covid19-industry/drugs-vaccines-treatments/authorization/applications.html
  # booster_2 
  # News - start April 7, 2022? Off-label; https://www.canada.ca/content/dam/phac-aspc/documents/services/immunization/national-advisory-committee-on-immunization-naci/naci-guidance-second-booster-dose-covid-19-vaccines.pdf
  # earliest observed in data 2022-05-08, but non-zero
  
  booster_1_date <- "2021-11-09"
  booster_2_date <- "2022-04-07"
  
  # x <- split(observed_can, by = "location_id")[[1]]
  # plot(x$date, x$reported_vaccinations)
  
  observed_can <- do.call(
    rbind,
    lapply(split(observed_can, by='location_id'), function(x) {
      tryCatch(
        {   
          # Build full time series 
          
          t <- c(min(x$date), max(x$date))
          x <- merge(x, data.table(date = as.Date(t[1]:t[2])), all.y = T)
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          x$brand <- .get_column_val(x$brand)
          x$age_start <- .get_column_val(x$age_start)
          x$age_end <- .get_column_val(x$age_end)
          
          # # Interpret missing values
          
          x$reported_vaccinations <- .do_sqrt_interp(x$reported_vaccinations)
          x$people_vaccinated <- .do_sqrt_interp(x$people_vaccinated)
          x$fully_vaccinated <- .do_sqrt_interp(x$fully_vaccinated)
          
          # Build boosters
          
          x <- .build_boosters(x = x, booster_round = 1, interp_method = "sqrt", first_booster_date = booster_1_date)
          x <- .build_boosters(x, 2, "sqrt", booster_2_date)
        },
        error = function(e) {cat("Error :", conditionMessage(e), "\n")}
      )
      # rebuild boosters from components
      boosters_in_data <- names(x)[grep("booster_", names(x))]
      x[, boosters_administered := rowSums(.SD, na.rm = T), .SDcols = boosters_in_data]
    })
  )
  

  
  return(observed_can)
  
}


.process_observed_mex <- function(
    observed_mex, 
    observed_owid,
    vaccine_output_root,
    population, 
    model_parameters
) {
  # For ease of stepping in and starting from the top. 
  # observed_mex <- model_inputs_data$load_observed_vaccinations(model_inputs_root, "mexico", template="one")
  
  setnames(observed_mex, c("initially_vaccinated", "total_administered"), c("people_vaccinated", "reported_vaccinations"))
  
  # Add proportion of total population to subnationals
  observed_mex = merge(
    x = observed_mex,
    y = population[, .(location_id, adult_population)],
    by = c("location_id"),
    all.x = T,
    all.y = F
  )
  setnames(observed_mex, "adult_population", "population")
  
  sel <- which(observed_mex$location_id == 130)[1]
  observed_mex$population_prop <- observed_mex$population / observed_mex$population[sel]
  
  observed_mex <- observed_mex[!(location_id == 130),] # Leave out national data --- we get that from OWiD
  
  # Get proportion first dose from national level data from processed OWiD
  nat <- observed_owid[location_id == 130,]
  nat$prop_first <- nat$people_vaccinated / nat$reported_vaccinations
  
  nat = .build_booster_tails(
    observed = nat,
    max_booster_courses = model_parameters$booster_courses
  )
  
  # Will remove this section that does plotting once Mexico data are approved by everyone - commented out 2022-04-28
  pdf(file.path(vaccine_output_root, "Mexico_subnats_data_inferred_booster_tails_late_nat_splice.pdf"), onefile = TRUE)

  plot(nat$date, nat$reported_vaccinations, main=unique(nat$location_name))
  points(nat$date, nat$people_vaccinated, col='blue')
  points(nat$date, nat$fully_vaccinated, col='red')
  points(nat$date, nat$boosters_administered, col='yellow')
  points(nat$date, nat$booster_1, col='orange')
  points(nat$date, nat$booster_2, col='violet')
  legend(x='topleft',
         legend=c('Total reported vaccinations', 'At least one dose', 'Fully vaccinated', 'observed data', "boosters_administered", "booster_1", "booster_2"),
         col=c('black', 'blue', 'red', 'green', "yellow", "orange", "violet"),
         pch=1,
         bty='n')
  
  
  # Fill subnats from nat.
  boost_cols = c("boosters_administered")
  for (i in 1:model_parameters$booster_courses) {
    boost_cols = c(boost_cols, paste0("booster_", i))
  }
  
  observed_mex <- do.call(
    rbind.fill,
    lapply(split(observed_mex, by='location_id'), function(x, nat) {
      
      x_loc <- .get_column_val(x$location_id)
      
      tryCatch( {
        
        # Some dates are duplicated with no data (drop only duplicates without data)
        #x <- x[!(is.na(x$people_vaccinated) & is.na(x$percentage_pop_one_dose)),]
        x <- x[order(x$date),]
        
        # Knock out problem dates so that cleaning/interp code below will fill in
        if (x_loc == 4644) { # Baja California
          
          x <- .knock_out_rows_by_date(x, "2021-06-19", "2021-07-15")
          
        } else if (x_loc %in% c(4649, 4650, 4651, 4647, 4648, 4653, 4654, 4655)) { # Chiapas, Chihuahua, Mexico City, Coahuila, Colima, Guanajuato, Guerrero, Hidalgo
          
          x <- .knock_out_rows_by_date(x, "2021-07-11", "2021-07-12")
          
        } else if (x_loc == 4656) { # Jalisco
          
          x <- .knock_out_rows_by_date(x, "2021-07-13", "2021-07-14")
          
        } else if (x_loc == 4670) { # Tamaulipas
          
          x <- .knock_out_rows_by_date(x, "2021-06-25", "2021-07-29")
          
        } else if (x_loc == 4672) { # Veracruz de Ignacio de la Llave
          
          x <- .knock_out_rows_by_date(x, "2021-07-01", "2021-07-01")
          
        }
        
        
        # Infer subnat data from nat data.
        ## Find max date where we have national prop first dose and sub-national people vaccinated
        t_max <- min(max(nat$date), max(x$date))
        
        ## Fill any missing dates
        x <- merge(data.table(date=seq(min(nat$date), t_max, by=1)),
                   x[x$date <= t_max],
                   by='date',
                   all=TRUE)
        y <- copy(x) # Set aside observed data to plot.
        
        x <- x[x$date <= max(x$date[!is.na(x$people_vaccinated)]),]
        
        
        sel_nat_date_range = nat$date >= min(nat$date) & nat$date <= max(x$date)
        
        ## Start each subnational on Dec 24 (when vaccinations began in Mexico) with a proportion of national total based on pop size
        pop_prop = .get_column_val(x$population_prop)
        x$people_vaccinated[1] <- nat$people_vaccinated[1] * pop_prop
        ## Anchor people_vaccinated to first date in national data
        x$people_vaccinated[1:5] <- 0
        ## Interpolate with exponential increase not linear
        x$people_vaccinated <- sqrt(x$people_vaccinated)
        x$people_vaccinated <- zoo::na.approx(x$people_vaccinated, na.rm=FALSE)
        x$people_vaccinated <- x$people_vaccinated^2
        x$people_vaccinated <- floor(.make_cumulative(x$people_vaccinated))
        
        ## Infer total and fully
        x$prop_first <- nat$prop_first[sel_nat_date_range]
        x$fully_vaccinated <- (x$people_vaccinated * (1/x$prop_first)) * (1 - x$prop_first)
        x$fully_vaccinated <- floor(.make_cumulative(x$fully_vaccinated))
        x$reported_vaccinations <- x$people_vaccinated + x$fully_vaccinated
        
        ## Allocate boosters.
        for (boost_col in boost_cols) {
          x[, eval(boost_col) := nat[sel_nat_date_range][[boost_col]] * pop_prop]
        }
        
        
        # Splice in national data if missing latest subnat data.
        
        # If subnat series ends before national series, splice in proportional to population.
        max_nat_date = max(nat$date)
        max_sub_date = max(x$date)
        
        if (max_sub_date < max_nat_date) {
          # Extend the series.
          x <- merge(
            x = x,
            y = data.table(date = seq(min(x$date), max_nat_date, by = 1)),
            by ='date',
            all = TRUE
          )
          
          all_measures = c("people_vaccinated", "fully_vaccinated", "reported_vaccinations", boost_cols)
          
          # Merge parent values into empty series'.
          subnat_tail_dt = x[
            date > max_sub_date,
            names(x)[!(names(x) %in% all_measures)],
            with = F
          ]
          x = x[date <= max_sub_date]
          
          subnat_tail_dt = merge(
            x = subnat_tail_dt,
            y = nat[, c("date", all_measures) , with = F],
            by = c("date"),
            all.x = T,
            all.y = F
          )
          
          # Proportionally adjust and sync up to tail of existing data.
          # for (measure in setdiff(all_measures, c("boosters_administered", "booster_1", "booster_2"))) {
          for (measure in all_measures) {
            # Proportionally adjust.
            subnat_tail_dt[
              ,
              eval(measure) := pop_prop * get(measure)
            ]
            
            # Sync up to tail of existing data.
            ## Determine tail intercept adjustment.
            spike = 0
            
            data_in_tail = nrow(subnat_tail_dt[!is.na(get(measure))]) > 0
            data_in_head = nrow(x[!is.na(get(measure))]) > 0
            
            if (data_in_tail & data_in_head) {
              # Flush tail to head.
              min_tail_date = min(subnat_tail_dt[!is.na(get(measure)), date])
              
              spike = subnat_tail_dt[date == min_tail_date, get(measure)] -
                x[date == max_sub_date, get(measure)]
              
            } else if (data_in_tail & !data_in_head) {
              # No head to flush tail to, so set proportionally.
              min_tail_date = min(subnat_tail_dt[!is.na(get(measure)), date])
              
              day1 = subnat_tail_dt[date == min_tail_date, get(measure)]
              
              day1_fully = subnat_tail_dt[date == min_tail_date, fully_vaccinated]
              
              day1_prop_of_fully = nat[date == min_tail_date, get(measure)] /
                nat[date == min_tail_date, fully_vaccinated]
              
              spike = day1 - (day1_prop_of_fully * day1_fully)
            }
            
            ## Adjust tail intercept.
            subnat_tail_dt[, eval(measure) := get(measure) - spike]
          }
          
          # Tack back on.
          x = rbind(x, subnat_tail_dt)
          
          # Point to the correct data file for these observations.
          nat_file = .get_column_val(nat$data_filename)
          x[
            date %in% subnat_tail_dt$date,
            data_filename := .get_column_val(nat$data_filename)
          ]
        }
        
        
        # Fill in missing constants.
        
        fill_cols = c("nid", "location_id", "location_name", "brand", "age_start", "age_end", "population", "population_prop")
        
        for (fill_col in fill_cols) {
          # The column exists ...
          if (fill_col %in% names(x)) {
            # ... and has one unique non-NA value ...
            if (length(unique(x[!is.na(get(fill_col)), get(fill_col)])) == 1) {
              # ... so fill in the empty values.
              x[, eval(fill_col) := .get_column_val(x[[fill_col]])]
            }
          }
        }
        
        
        # Plot. ################################################################
        
        plot(x$date, x$reported_vaccinations, main=unique(x$location_name))
        points(x$date, x$people_vaccinated, col='blue')
        points(y$date, y$people_vaccinated, col='green', pch=2, cex=1.2)
        points(x$date, x$fully_vaccinated, col='red')
        points(x$date, x$boosters_administered, col='yellow')
        points(x$date, x$booster_1, col='orange')
        points(x$date, x$booster_2, col='violet')
        legend(x='topleft',
               legend=c('Total reported vaccinations', 'At least one dose', 'Fully vaccinated', 'observed data', "boosters_administered", "booster_1", "booster_2"),
               col=c('black', 'blue', 'red', 'green', "yellow", "orange", "violet"),
               pch=c(1,1,1,2),
               bty='n')
        
      }, error=function(e){
        
        cat("Warning :", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(e), "\n")
        
      })
      
      return(x)
      
    },
    nat = nat
    )
  )
  
  dev.off()
  # refill for plotting
  sel <- which(!is.na(unique(observed_mex$data_filename)))
  filenames <- paste(unique(observed_mex$data_filename)[sel], collapse = " | ")
  observed_mex$data_filename <- filenames
  
  return(data.table(observed_mex))
}


.process_observed_bra <- function(observed_bra) {
  
  setnames(observed_bra, 
           c("initially_vaccinated", "total_administered"), 
           c("people_vaccinated", "reported_vaccinations"))
  
  # Data looks great now, guard against future incomplete data
  
  if (nrow(observed_bra[!complete.cases(observed_bra),]) > 0){
  
    stop("process_observed_vaccinations: Brazil's data has missingness, revisit processing.")
  }
  
  return(observed_bra)
  
}


.process_observed_ita <- function(observed_ita, observed_owid, adult_population, model_parameters) {
  
  setnames(
    observed_ita,
    c("total_administered", "initially_vaccinated"),
    c("reported_vaccinations", "people_vaccinated"))
  
  # Get proportion first dose from national level data from processed OWiD
  nat <- observed_owid[location_id == 86,]
  nat$ratio <- nat$fully_vaccinated / nat$people_vaccinated
  
  #x <- observed_ita[location_id == 35510,]
  
  observed_ita <- do.call(
    rbind,
    lapply(split(observed_ita, by='location_id'), function(x) {
      
      tryCatch( {
        
        if (unique(x$location_id)[1] == 86) { 
          
          x <- nat
          
        } else { # Subnationals only

          # x <- split(observed_ita, by = 'location_id')[[2]] # Piemonte, 1 is Italy national
          
          # build full time series from start and end date
          t <- c(as.Date("2020-12-16"), max(x$date)) # what is this date?
          x <- merge(x, data.table(date = as.Date(t[1]:t[2])), all.y=T)
          # refill values
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          x$brand <- .get_column_val(x$brand)
          x$age_start <- .get_column_val(x$age_start)
          x$age_end <- .get_column_val(x$age_end)
          
          # start quantities at 0 (not NA)
          x[x$date == t[1], c('reported_vaccinations', 'people_vaccinated', 'fully_vaccinated')] <- 0
          
          # Fill NA, nonlinear
          x$reported_vaccinations <- .do_sqrt_interp(x$reported_vaccinations)
          x$people_vaccinated <- .do_sqrt_interp(x$people_vaccinated)
          
          # add national ratio of fully vaxed people (no raw data in)
          x <- merge(x, nat[,c('date', 'ratio')], by='date', all.x=TRUE)
          x$ratio <- .extend_end_values(na.approx(x$ratio, na.rm=FALSE))
          x$fully_vaccinated <- x$people_vaccinated * x$ratio
          
          # keep people vaccinated below adult population
          x$people_vaccinated <- pmin(x$people_vaccinated, 
                                      adult_population$adult_population[adult_population$location_id == x$location_id[1]], 
                                      na.rm=FALSE)
          
          x$fully_vaccinated <- pmin(x$fully_vaccinated, 
                                     adult_population$adult_population[adult_population$location_id == x$location_id[1]], 
                                     na.rm=FALSE)
          
          # Build boosters only for boosters present in data
          tryCatch(
            
            {for (i in 1:model_parameters$booster_courses) {x <- .build_boosters(x, i, "sqrt")}},
            error = function(e) {cat("Warning :", conditionMessage(e), "\n")}
            
          )
          
          # rebuild vaccinations and boosters from components
          boosters_in_data <- names(x)[grep("booster_", names(x))]
          x[, boosters_administered := rowSums(.SD, na.rm = T), .SDcols = boosters_in_data]
          
        }
        
      }, error=function(e){
        
        cat("Warning :", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(e), "\n")
        
      })
      
      return(x)
    })
  )
  
  observed_ita <- observed_ita[,ratio := NULL]
  return(observed_ita)
  
}



.process_observed_ind <- function(observed_ind) {
  
  setnames(
    observed_ind,
    c("total_administered","initially_vaccinated"),
    c("reported_vaccinations","people_vaccinated")
  )
  
  f <- function(x) sum(x, na.rm=T)
  nat <- as.data.table(aggregate(formula = people_vaccinated ~ date, data=observed_ind, FUN=f))
  nat$reported_vaccinations <- aggregate(formula = reported_vaccinations ~ date, data=observed_ind, FUN=f)[,2]
  nat$boosters_administered <- aggregate(formula = boosters_administered ~ date, data=observed_ind, FUN=f)[,2]
  nat$age_start <- 0
  nat$age_end <- 125
  nat$brand <- 'All'
  nat$location_id <- 163
  nat$location_name <- 'India'
  nat$data_filename <- 'india_vaccination.csv'
  
  observed_ind <- rbind(nat, observed_ind, fill=T)
  observed_ind$fully_vaccinated <- observed_ind$reported_vaccinations - (observed_ind$people_vaccinated + observed_ind$boosters_administered)
  
  observed_ind <- do.call(
    rbind,
    lapply(split(observed_ind, by='location_id'), function(x) {
      
      t <- c(as.Date("2020-12-16"), max(x$date))
      x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
      
      x[x$date <= as.Date("2020-12-16")+7, c('people_vaccinated', 'reported_vaccinations', 'boosters_administered', 'fully_vaccinated')] <- 0
      
      x$location_id <- .get_column_val(x$location_id)
      x$location_name <- .get_column_val(x$location_name)
      x$brand <- .get_column_val(x$brand)
      x$age_start <- .get_column_val(x$age_start)
      x$age_end <- .get_column_val(x$age_end)
      x
      
    })
  )
  
  observed_ind <- as.data.frame(observed_ind)
  for (i in c("reported_vaccinations","people_vaccinated","fully_vaccinated","boosters_administered")) {
    observed_ind[observed_ind$location_id == 163, i] <- .do_sqrt_interp(.remove_non_cumulative(observed_ind[observed_ind$location_id == 163, i]))
  } 
  
  #x <- observed_ind[observed_ind$location_id == 163,]
  #plot(x$date, x$reported_vaccinations)
  #points(x$date, x$people_vaccinated, col='blue')
  #points(x$date, x$fully_vaccinated, col='red')
  #points(x$date, x$boosters_administered, col='green')
  
  return(as.data.table(observed_ind))
}


.process_observed_ger <-function(observed_ger) {
  
  setnames(
    observed_ger,
    c("total_administered",   "initially_vaccinated"),
    c("reported_vaccinations","people_vaccinated")
  )
  
  if (!(is.numeric(observed_ger$reported_vaccinations) | is.integer(observed_ger$reported_vaccinations))) observed_ger$reported_vaccinations <- as.numeric(observed_ger$reported_vaccinations)
  
  observed_ger[date < "2021-01-16", fully_vaccinated := 0]
  observed_ger[date < "2021-01-16", people_vaccinated := reported_vaccinations]
  
  ger_max_date <- observed_ger[!is.na(reported_vaccinations), max(date)]
  ger_min_date <- observed_ger[!is.na(reported_vaccinations), min(date)]
  observed_ger <- observed_ger[date <= ger_max_date,]
  
  
  observed_ger <- do.call(
    rbind,
    lapply(split(observed_ger, by = 'location_id'), function(x) {
      tryCatch(
        {
          
          # build full time series from start and end date
          t <- c(as.Date(ger_min_date), max(x$date)) 
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          # refill values
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          x$brand <- .get_column_val(x$brand)
          x$age_start <- .get_column_val(x$age_start)
          x$age_end <- .get_column_val(x$age_end)
          # Fill NA, nonlinear
          x$reported_vaccinations <- .do_sqrt_interp(x$reported_vaccinations)
          x$people_vaccinated <- .do_sqrt_interp(x$people_vaccinated)
          x$fully_vaccinated <- .do_sqrt_interp(x$fully_vaccinated)
          
          # Large jump in vaccinations on June 06 due to reporting issue
          # Shifting observations from Apr 07 to Jun 04 to compensate
          # Data intake will re-extract and will hopefully fix it
          
          t <- as.Date("2021-06-06")
          obs_t <- x[date == t, people_vaccinated]
          bias <- (obs_t - (x[date == t + 1, people_vaccinated] - obs_t) * 2) - x[date == t - 2, people_vaccinated]
          x[date >= "2021-04-07" & date < t, people_vaccinated := people_vaccinated + round(bias / 2)]
          
          # Build boosters only for boosters present in data
          booster_1_date <- observed_ger[!is.na(booster_1), min(date)]
          booster_2_date <- observed_ger[!is.na(booster_2), min(date)]
          
          tryCatch(
            {
            x <- .build_boosters(x, 1, "sqrt", booster_1_date)
            x <- .build_boosters(x, 2, "sqrt", booster_2_date)
            },
            error = function(e) {cat("Warning :", conditionMessage(e), "\n")}
            
          )
          
          # rebuild boosters from components
          boosters_in_data <- names(x)[grep("booster_", names(x))]
          x[, boosters_administered := rowSums(.SD, na.rm = T), .SDcols = boosters_in_data]
          
        },
        error = function(e) {
          cat("Warning :", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(e), "\n")
        }
      )
      
      return(x)
    })
  )
  
  ## Fix unmatched in reported_vaccinations.
  #observed_ger[reported_vaccinations < people_vaccinated + fully_vaccinated, reported_vaccinations := people_vaccinated + fully_vaccinated]
  
  return(observed_ger)
}


.process_observed_esp <- function(observed_esp, adult_population) {
  # location_name:         Spain + subnational regions
  # date: epi-date.        Translated from “Fecha de la ultima vacuna registrada”
  # distributed_vaccines:  total number of doses distributed. Translated from “Dosis entregadas totales”
  # initiated_vaccines:    total number of people that have received at least one dose. Translated from “No Personas con al menos 1 dosis”
  # fully_vaccinated:      total number of people who have received all doses of the vaccine. Translated from “No. Personas con pauta completa”
  # doses_administered:    total number of doses administered. Translated from “Dosis administradas”
  
  setnames(observed_esp, "total_administered", "reported_vaccinations")
  setnames(observed_esp, "initially_vaccinated", "people_vaccinated")
  
  # Get rid of repeat dates but keep the one with most data
  t = as.Date("2021-07-12")
  tmp <- observed_esp[date == t & !is.na(people_vaccinated) & !is.na(fully_vaccinated)]
  sel <- which(observed_esp$date == t)
  observed_esp <- rbind(observed_esp[-sel,], tmp)
  
  
  observed_esp[, reported_vaccinations := as.numeric(reported_vaccinations)]
  observed_esp[, people_vaccinated := as.numeric(people_vaccinated)]
  observed_esp[, fully_vaccinated := as.numeric(fully_vaccinated)]
  
  observed_esp[, booster_1 := boosters_administered]
  
  
  # Replacing outlier week Feb 15 with the previous week
  # Not high priority, but should check if data intake has resolved the issues at these location-dates
  fill_cols <- c('people_vaccinated', 'fully_vaccinated', 'reported_vaccinations')
  tmp <- observed_esp[date == "2021-02-08", ..fill_cols]
  observed_esp[date == "2021-02-15", fill_cols] <- tmp
  
  tmp_locs <- c('Andalucia', 'Melilla', 'Ceuta')
  tmp <- observed_esp[location_name %in% tmp_locs & date == "2021-02-15", ..fill_cols]
  observed_esp[location_name %in% tmp_locs & date == "2021-02-16", fill_cols] <- tmp
  
  tmp_locs <- c('Ceuta')
  tmp <- observed_esp[location_name %in% tmp_locs & date == "2021-04-27", ..fill_cols]
  observed_esp[location_name %in% tmp_locs & date == "2021-05-02", fill_cols] <- tmp
  
  
  #x <- observed_esp[location_name == 'Ceuta']
  
  observed_esp <- do.call(
    rbind,
    lapply(split(observed_esp, by='location_id'), function(x) {
      
      tryCatch( {
        
        #tmp <- c(x$reported_vaccinations[1], diff(x$reported_vaccinations)) - c(x$fully_vaccinated[1], diff(x$fully_vaccinated))
        #tmp[tmp < 0] <- 0
        #x$people_vaccinated <- cumsum(tmp)
        
        t <- c(as.Date("2020-12-16"), max(x$date))
        x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
        x$location_id <- .get_column_val(x$location_id)
        x$location_name <- .get_column_val(x$location_name)
        x$brand <- .get_column_val(x$brand)
        x$age_start <- .get_column_val(x$age_start)
        x$age_end <- .get_column_val(x$age_end)
        
        x[x$date == t[1], c('reported_vaccinations', 'people_vaccinated', 'fully_vaccinated')] <- 0
        
        x$reported_vaccinations <- .do_sqrt_interp(x$reported_vaccinations)
        
        tmp_prop <- x$people_vaccinated / x$reported_vaccinations
        tmp_prop <- .extend_end_values(na.approx(tmp_prop, na.rm=F))
        x$people_vaccinated <- x$reported_vaccinations * tmp_prop
        
        tmp_prop <- x$fully_vaccinated / x$reported_vaccinations
        tmp_prop <- .extend_end_values(na.approx(tmp_prop, na.rm=F))
        x$fully_vaccinated <- x$reported_vaccinations * tmp_prop
        
        # BOOSTERS
        # First day of spain booster: age >70, age 60-69 (2021.12.14); Age 50-59, 40-49 (2021.12.23); Age 30-39,20-29 (2022.01.21); Age  12-19 (2022.03.11)
        # Build boosters only for boosters present in data
        # esp_booster_1_auth = as.Date("2021-12-14")
        esp_booster_1_date = as.Date("2021-10-05") # minimum observed in data
        
        tryCatch(
          
          x <- .build_boosters(x, 1, "sqrt", esp_booster_1_date),
          error = function(e) {cat("Warning :", conditionMessage(e), "\n")}
          
        )
        # rebuild vaccinations and boosters from components
        boosters_in_data <- names(x)[grep("booster_", names(x))]
        x[, boosters_administered := rowSums(.SD, na.rm = T), .SDcols = boosters_in_data]
        
        #tmp <- x$fully_vaccinated * (1/0.9)
        #x$people_vaccinated <- pmax(x$people_vaccinated, tmp, na.rm=TRUE)
        #
        #x$people_vaccinated <- pmin(x$people_vaccinated, 
        #                              adult_population$adult_population[adult_population$location_id == x$location_id[1]], 
        #                              na.rm=FALSE)
        #
        #x$fully_vaccinated <- pmin(x$fully_vaccinated, 
        #                             adult_population$adult_population[adult_population$location_id == x$location_id[1]], 
        #                             na.rm=FALSE)
        #
        #x$reported_vaccinations <- x$people_vaccinated + x$fully_vaccinated
        
        #plot(x$date, x$reported_vaccinations)
        #points(x$date, x$people_vaccinated, col = 'blue')
        #points(x$date, x$fully_vaccinated, col ='red')
        
        #plot(x$date, c(0, diff(x$people_vaccinated)), type='l')
        
      }, error=function(e){
        
        cat("Warning :", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(e), "\n")
        
      })
      
      return(x)
    })
  )
  
  
  observed_esp[is.na(reported_vaccinations), reported_vaccinations := people_vaccinated + fully_vaccinated]
  
  
  # Fix duplicate records by date and location_id.
  
  spain <- observed_esp[location_id=="92"]
  observed_esp <- observed_esp[location_id!="92"]
  spain <- spain[!duplicated(spain[,date]),]
  observed_esp <- rbind(observed_esp,spain)
  return(observed_esp)
}

.process_observed_ecdc <- function(observed_ecdc, observed_owid){
  
  owid <- copy(observed_owid)
  
  setnames(observed_ecdc, 
           c("initially_vaccinated", "total_administered"), 
           c("people_vaccinated", "reported_vaccinations"))
  
  # Filter locations with missing or more detailed second boosters
  all_zero_booster_2 <- observed_ecdc[
    !is.na(booster_2), .(max_booster_2 = max(booster_2)), by = location_id
  ][
    max_booster_2 == 0, location_id
  ]
  
  all_na_booster_2 <- observed_ecdc[
    , lapply(.SD, function (x) all(is.na(x))), by = location_id
  ][
    which(booster_2), location_id
  ]

  observed_ecdc <- observed_ecdc[!location_id %in% c(
    81, # Germany
    86, # Italy
    92, # Spain
    all_zero_booster_2,
    all_na_booster_2
  )]
  
  if (!all(unique(observed_ecdc$location_id) %in% unique(owid$location_id))) {
    stop(".process_observed_ecdc : not all ECDC locations in OWiD - check process_observed_vaccinations.R script.")
  }
  
  # Take main vaccines from OWiD, add ECDC boosters
  booster_cols <- grep("booster", names(observed_ecdc), value = T)
  owid[, (booster_cols) := NULL]
  owid <- owid[location_id %in% unique(observed_ecdc$location_id)]
  ecdc_cols <- c("location_id", "date", booster_cols)
  observed_ecdc <- merge(owid, observed_ecdc[, ..ecdc_cols], all.x = T, all.y = T)
  observed_ecdc[, date := as.Date(date)]
  # observed_ecdc[, location_name := NULL]
  # refill names
  # observed_ecdc_merge <- merge(observed_ecdc, hierarchy[, .(location_id, location_name)], by = "location_id", all.x = T)
  
  observed_ecdc <- do.call(
    rbind,
    lapply(split(observed_ecdc, by = "location_id"), function(x) {
      
      tryCatch( 
        {
          t <- as.Date(range(x$date)) # what is this date?
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T)
          # refill values
          x$location_id <- .get_column_val(x$location_id)
          x$location_name <- .get_column_val(x$location_name)
          x$brand <- .get_column_val(x$brand)
          x$age_start <- .get_column_val(x$age_start)
          x$age_end <- .get_column_val(x$age_end)
          
          # ECDC reporting seems weekly - remove nonreport days
          # create a daily column for each measure
          measure_cols <- grep("administered|vaccinated|booster", names(x), value = T)
          x[, paste0("daily_", measure_cols) := lapply(.SD, function(y) y-lag(y)), .SDcols = measure_cols]
          daily_cols <- grep("daily", names(x), value = T)
          # remove cumulative value where daily == 0
          col <- measure_cols[1]
          x <- data.frame(x)
          for (col in measure_cols){
            # x[,col]
            # x[,daily]
            daily <- paste0("daily_", col)
            idx_0 <- which(x[,daily]==0)
            x[idx_0 ,col] <- NA
          }
          x <- data.table(x)
          # remove daily cols
          x[, (daily_cols) := NULL]
          
          # Fill NA, nonlinear
          x$reported_vaccinations <- .do_sqrt_interp(x$reported_vaccinations)
          x$people_vaccinated <- .do_sqrt_interp(x$people_vaccinated)
          x$fully_vaccinated <- .do_sqrt_interp(x$fully_vaccinated)
          
          # Build boosters
          booster_1_date <- x[!is.na(booster_1), min(date)] 
          booster_2_date <- x[!is.na(booster_2), min(date)] 
        
          
          tryCatch(
            {
              x <- .build_boosters(x, 1, "sqrt", booster_1_date)
              x <- .build_boosters(x, 2, "sqrt", booster_2_date)
            },
            error = function(e) {cat("Warning :", conditionMessage(e), "\n")}
            
          )
          
          # rebuild vaccinations and boosters from components
          boosters_in_data <- names(x)[grep("booster_", names(x))]
          x[, boosters_administered := rowSums(.SD), .SDcols = boosters_in_data]
          
          
        }, error = function(e) {
        
        cat("Warning:", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(e), "\n")
        
      })
      
      return(x)
    })
  )
  
  return(observed_ecdc)
}

# Used for building booster courses, not main vaccines
# Less complete than all-age data - scale these proportionally up to all-age total boosters
.process_observed_cdc_age <- function(observed_cdc_age, observed_cdc){
  
  
  setnames(observed_cdc_age, # for indexing, ordering and clarity
           c("initially_vaccinated", "total_administered",    "additional_dose", "second_additional_dose"), 
           c("people_vaccinated",    "reported_vaccinations", "booster_1",       "booster_2"))
  
  observed_cdc_age[, date := date - 3] # Shift states by 3 day lag (per INDIVIDUAL_NAME)
  
  # Find all-age booster_2 from max of all other age groups (doesn't exist in raw data)
  
  
  # data should be unique by location_id, age_start, date
  # find max boosters by location and date
  # trim main data to all-age only (0-125)
  # summary(observed_cdc_age$booster_2) # all NA, as expected
  # replace old booster_2, ensure unique rows
  observed_cdc_age <- unique(observed_cdc_age, by = c("location_id", "age_start", "date"))
  max_idx <- observed_cdc_age[, .I[which.max(booster_2)], by = .(location_id, date)][,V1]
  max_booster_2 <- observed_cdc_age[max_idx, .(location_id, date, booster_2)]
  observed_cdc_age <- observed_cdc_age[age_start == 0]
  observed_cdc_age <- merge(observed_cdc_age, max_booster_2, by = c("location_id", "date"), all.x = T)
  observed_cdc_age[,booster_2.x := NULL]
  setnames(observed_cdc_age, "booster_2.y", "booster_2")
  observed_cdc_age <- unique(observed_cdc_age, by = c("location_id", "date"))
  
  # checkpoint <- copy(observed_cdc_age)
  # observed_cdc_age <- copy(checkpoint)
  
  # https://www.fda.gov/news-events/press-announcements/fda-authorizes-booster-dose-pfizer-biontech-covid-19-vaccine-certain-populations
  FDA_booster_1_EUA <- "2021-09-22"
  # https://www.fda.gov/news-events/press-announcements/coronavirus-covid-19-update-fda-authorizes-second-booster-dose-two-covid-19-vaccines-older-and
  FDA_booster_2_EUA <- "2022-03-29"
  
  # Expedient patch for West Virginia
  wv <- merge(
    observed_cdc_age[location_id==571, -c("booster_1"), with = F],
    observed_cdc[location_id==571, .(date, booster_1)],
    by = "date"
  )
  plot(wv$date, wv$booster_1)
  observed_cdc_age <- rbind(observed_cdc_age[!location_id %in% 571], wv)
  
  # Complete time series, build boosters
  observed_cdc_age <- do.call(
    rbind,
    lapply(split(observed_cdc_age, by = "location_id"), function(x) {
      
      tryCatch( {
        
        # build main time series
        t <- as.Date(range(x$date))
        x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T, by = 'date')
        # refill values
        x$location_id <- .get_column_val(x$location_id)
        x$location_name <- .get_column_val(x$location_name)
        x$brand <- .get_column_val(x$brand)
        x$age_start <- .get_column_val(x$age_start)
        x$age_end <- .get_column_val(x$age_end)
        x$source <- .get_column_val(x$source)
        x$data_filename <- .get_column_val(x$data_filename)
        
        # Build boosters only for boosters present in data
        tryCatch(
          {
          x <- .build_boosters(x, 1, "sqrt", FDA_booster_1_EUA)
          x <- .build_boosters(x, 2, "sqrt", FDA_booster_2_EUA)
          },
          
          error = function(e) {cat("Warning :", conditionMessage(e), "\n")}
          
        )
        
        # rebuild boosters from components
        booster_courses <- names(x)[grep("booster_", names(x))]
        x[, boosters_administered := rowSums(.SD, na.rm = T), .SDcols = booster_courses]
        
      }, error = function(e) {
        
        cat("Warning:", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(e), "\n")
        
      })
      
      return(x)
    })
  )
  
  # join booster courses onto main data
  new_cdc <- 
    merge(
      observed_cdc[,.(location_id, date, location_name, brand, age_start, age_end,
                      people_vaccinated, fully_vaccinated, reported_vaccinations, 
                      data_filename)], 
      observed_cdc_age[,.(location_id, date, boosters_administered, booster_1, 
                          booster_2)], 
      by = c("location_id", "date")
    )
  
  # filanames for dx plots
  filename1 <- .get_column_val(observed_cdc$data_filename)
  filename2 <- .get_column_val(observed_cdc_age$data_filename)
  new_cdc[, data_filename := paste(filename1, "|", filename2)]
  
  return(new_cdc)
  
}

#' Build South Africa's subnational time series for NPI project
#' 
#' Subnats do not have first 10 months of data 
#' - split proportionally from national
#' - scale to meet real data, interpolate full series
#' - aggregate total_administered and national values from subnat series
#'
#' @param observed_saf [data.table] New South Africa subnat data file
#' @param observed_owid [data.table] Contains old OWiD national data
#' @return [data.table] completed time series for all S.Africa locations
.process_observed_safr <- function(observed_saf, observed_owid, over5_population) {
  
  setnames(observed_saf, 
           c("initially_vaccinated", "total_administered"), 
           c("people_vaccinated", "reported_vaccinations"))
  
  hierarchy <- gbd_data$get_gbd_hierarchy()
  saf_subnat_locs <- children_of_parents(196 , hierarchy, "loc_ids", F)
  
  # rename data
  new_saf <- observed_saf
  old_owid <- observed_owid
  old_saf <- old_owid[location_name %like% "South Africa"]; rm(old_owid)
  
  # dates
  Dates_saf <- list(
    new_min = min(new_saf$date),
    new_max = max(new_saf$date),
    old_min = min(old_saf$date),
    old_max = max(old_saf$date)
  )
  
  SAF <- rbind(
    old_saf[date < Dates_saf$new_min],
    new_saf[date >= Dates_saf$new_min]
  )
  
  measure_cols <- grep("vaccin|booster", names(SAF), value = T)
  scale_cols <- grep("vaccin", names(SAF), value = T)
  refill_cols <- grep("location_|brand|age_|_population|pop_prop", names(SAF), value = T)
  
  # build population proportional time series for subnats
  SAF <- merge(SAF, over5_population, by = "location_id")
  SAF[, pop_prop := (adult_population / unique(over5_population[location_id == 196, adult_population]))] # S.Afr. National
  
  SAF <- 
    rbindlist(
      lapply(
        split(SAF, SAF$location_id), function(x) {
          
          # scale national series to subnat population proportion
          saf_nat <- SAF[date < Dates_saf$new_min & location_id == 196]
          saf_nat[, `:=` (pop_prop = unique(x$pop_prop), 
                          location_id = unique(x$location_id),
                          location_name = unique(x$location_name),
                          adult_population = unique(x$adult_population)
          )]
          saf_nat[, (measure_cols) := lapply(.SD, function(y) y * pop_prop), .SDcols = measure_cols]
          
          # combine and create full series
          x <- rbind(saf_nat, x)
          x <- x[order(x$date)]
          t <- as.Date(range(x$date))
          x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T, by = "date")
          x[, (refill_cols) := lapply(.SD, function(y) .get_column_val(y)), .SDcols = refill_cols]
          x[, (measure_cols) := lapply(.SD, function(y) .do_sqrt_interp(y)), .SDcols = measure_cols]
          x <- tidyr::fill(x, data_filename, .direction = "down")
          
          # correct mismatch in measure value on join date 
          # (scale split data to meet real subnat time series)
          Idx <- which(x$date == Dates_saf$new_min)
          
          x <- data.frame(x)
          
          for (col in scale_cols) {
            ratio <- x[Idx, col] / x[Idx - 1, col]
            
            for (i in 1:(Idx-1)) {
              x[i, col] <- x[i, col] * ratio
            }
          }
          
          x <- data.table(x)
          
        }
      )
    )
  
  
  # rebuild total_administered from parts
  subpart_cols <- measure_cols[! measure_cols == "reported_vaccinations"]
  totals <- rowSums(SAF[, ..subpart_cols], na.rm = T)
  SAF[, total_administered := totals]
  
  # rebuild national from subnats
  nat <- SAF[location_id %in% saf_subnat_locs]
  nat <- nat[, lapply(.SD, sum, na.rm=T), .SDcols = measure_cols, by = date]
  nat2 <- SAF[location_id == 196]
  nat2[, (measure_cols) := NULL]
  nat2 <- merge(nat2, nat, by = "date")
  SAF <- SAF[!location_id == 196]
  SAF <- rbind(nat2, SAF)
  
  # refill for plotting
  sel <- which(!is.na(unique(SAF$data_filename)))
  filenames <- paste(unique(SAF$data_filename)[sel], collapse = " | ")
  SAF$data_filename <- filenames

  # return  
  return(SAF)
  
}

.process_observed_aus <- function(observed_aus, observed_owid) {
  
  # location-specific file includes separation of first and second boosters
  
  x <- copy(observed_owid)
  x <- x[location_id == 71]
  refill_cols <- grep("location_|brand|age_|_population|pop_prop", names(x), value = T)
  measure_cols <- grep("vaccin|booster_", names(x), value = T)
  booster_cols <- grep("booster", names(x), value = T)
  filename_combined <- paste(
    unique(x$data_filename), 
    unique(observed_aus$data_filename), 
    sep = " | "
    )
  
  # merge to replace owid boosters with location-specific boosters
  observed_aus <- observed_aus[, .(location_id, date, booster_1, booster_2, data_filename)]
  x[, c(booster_cols) := NULL]
  x <- merge(x, observed_aus, by = c("location_id", "date"), all.x = T, all.y = T)
  x[, data_filename := paste(data_filename.x, data_filename.y, sep = " | ")]
  x[, c("data_filename.x", "data_filename.y") := NULL]
  
  # booster_2 is 0 before first data
  nans <- is.na(x$booster_2)
  last_nans <- min(which(!nans)) - 1
  x[1:last_nans, booster_2 := 0]
  
  # rebuild time series and interpolate
  t <- as.Date(range(x$date))
  x <- merge(x, data.table(date=as.Date(t[1]:t[2])), all.y=T, by = "date")
  x[, (refill_cols) := lapply(.SD, function(y) .get_column_val(y)), .SDcols = refill_cols]
  x[, (measure_cols) := lapply(.SD, function(y) .do_sqrt_interp(y)), .SDcols = measure_cols]
  x$data_filename <- filename_combined
  
  # rebuild total boosters
  x[, boosters_administered := booster_1 + booster_2]
  
  # replace owid data with newly built boosters
  observed_owid <- observed_owid[!location_id == 71]
  observed_owid <- rbind(observed_owid, x)
  # reorder
  observed_owid <- observed_owid[order(observed_owid$location_id, observed_owid$date)]
  
  return(observed_owid)
  
}

.fix_late_start_date_for_subs <- function(observed_data, start_date, location, hierarchy) {
  
  if (location == 'cdc') {
    observed_fix_late_date <- rbind( 
      observed_data,
      
      data.table(
        location_id = hierarchy[parent_id == 102, location_id],
        reported_vaccinations = 50,
        date = start_date
      ),
      fill = T
    )
  } else {
    observed_fix_late_date <- rbind( 
      observed_data,
      data.table(
        location_id = unique(observed_data$location_id),
        reported_vaccinations = 50,
        date = start_date
      ),
      fill = T
    )
  }
  return(observed_fix_late_date)
}


.process_observed_data <- function(observed) {
  observed <- observed[order(location_id, date)]
  observed[, observed := 1]
  # Change to observed = 0 if only 1 of three metrics available (have to guess at other two)
  observed[, observed := ifelse(is.na(reported_vaccinations) & is.na(fully_vaccinated), 0,
                                ifelse(is.na(reported_vaccinations) & is.na(people_vaccinated), 0,
                                       ifelse(is.na(fully_vaccinated) & is.na(people_vaccinated), 0, 1)))]
  
  
  observed <- observed[!is.na(location_id)]
  observed[, reported_vaccinations := as.numeric(reported_vaccinations)]
  # End up being negative daily, which is bad...
  
  observed[, daily_reported_vaccinations := reported_vaccinations - shift(reported_vaccinations, fill = 0), by = "location_id"]
  observed <- observed[daily_reported_vaccinations > 0]
  observed[is.na(daily_reported_vaccinations), daily_reported_vaccinations := reported_vaccinations]
  
  ## Fill missing dates (linear interpolation by na.approx)
  observed[, ratio_reported_people := reported_vaccinations / people_vaccinated]
  fill_dates <- data.table(expand.grid(date = seq(as.Date("2020-12-01"), max(observed$date), by = "1 day"),
                                       location_id = unique(observed$location_id)))
  observed <- merge(fill_dates, observed, by = c("location_id", "date"), all.x = T)
  observed[, min_date := min(date[!is.na(reported_vaccinations)]), by = "location_id"]
  observed[, max_date := max(date[!is.na(reported_vaccinations)]), by = "location_id"]
  observed <- observed[date >= min_date & date <= max_date]
  observed[daily_reported_vaccinations == 0, c("daily_reported_vaccinations","reported_vaccinations","fully_vaccinated") := NA]
  observed[,max_fully_vaccinated := max(fully_vaccinated, na.rm = T), by = "location_id"]
  observed[date == min_date & is.na(fully_vaccinated), fully_vaccinated := 0]
  
  observed[date == max_date & is.na(fully_vaccinated), fully_vaccinated := max_fully_vaccinated]
  observed[fully_vaccinated == "-Inf", fully_vaccinated := 0]
  
  observed[, max_people_vaccinated := max(people_vaccinated, na.rm = T), by = "location_id"]
  
  observed[, reported_vaccinations := na.approx(reported_vaccinations), by = "location_id"]
  observed[, fully_vaccinated := na.approx(fully_vaccinated), by = "location_id"]
  
  ## I want to fill people vaccinated, but ONLY after the first date of reported people vaccinated
  # # Do in two steps
  observed[, first_people_vaccinated_date := min(date[!is.na(people_vaccinated)]), by = "location_id"]
  observed[date == min_date & is.na(people_vaccinated), people_vaccinated := 0]
  observed[date == max_date & is.na(people_vaccinated), people_vaccinated := max_people_vaccinated]
  
  observed[, people_vaccinated := na.approx(people_vaccinated), by = "location_id"]
  # Now, after interpolated, reset values before first reported day to NA
  observed[people_vaccinated > reported_vaccinations, people_vaccinated := reported_vaccinations]
  
  observed[, max_fully_vaccinated := NULL]
  observed[, max_people_vaccinated := NULL]
  observed[, first_people_vaccinated_date := NULL]
}



.make_observed_children <- function(observed, adult_population, hierarchy) {
  
  ## Redistribute child locations
  
  obs_children <- observed[location_id %in% c(hierarchy[level == 3 & most_detailed == 0, location_id], 570, 44538)]
  
  # Drop since more detailed info available
  drop_parents <- c(
    102, # United States of America
    95, # United Kingdom
    101, # Canada
    81, # Germany
    92, # Spain
    86, # Italy
    163, # India
    135, # Brazil
    130, # Mexico
    6, # China
    196 # South Africa
  )
  obs_children <- obs_children[!(location_id %in% drop_parents)]
  obs_children <- merge(obs_children, adult_population, by = "location_id")
  setnames(obs_children, 
           c("location_id","adult_population"), 
           c("parent_id",  "country_pop"))
  obs_children <- merge(hierarchy[,c("location_id","parent_id")], obs_children, by = "parent_id", allow.cartesian = T)
  obs_children <- merge(obs_children, adult_population, by = "location_id")
  obs_children[, reported_vaccinations := adult_population / country_pop * reported_vaccinations]
  obs_children[, people_vaccinated := adult_population / country_pop * people_vaccinated]
  obs_children[, fully_vaccinated := adult_population / country_pop * fully_vaccinated]
  obs_children[, boosters_administered := adult_population / country_pop * boosters_administered]
  obs_children[, booster_1 := adult_population / country_pop * booster_1]
  obs_children[, booster_2 := adult_population / country_pop * booster_2]
  # Drop Hong Kong, Macao from children
  obs_children <- obs_children[!(location_id %in% c(361, 354))]
  obs_children[, observed := 0]
}


.make_write_empirical_delay <- function(observed,
                                        delay_days, # days to the empirical delay
                                        vaccine_output_root,
                                        hierarchy
) {
  emp_lag <- observed[date > max_date - delay_days]
  emp_lag <- emp_lag[, lapply(.SD, function(x) round(mean(x))),
                     by = "location_id",
                     .SDcols = "fully_vaccinated"]
  setnames(emp_lag, "fully_vaccinated", "tail_fully_vaccinated")
  
  o2 <- merge(observed, emp_lag, by = "location_id")
  o2 <- merge(o2, hierarchy, by = "location_id")
  
  o2[, difference := abs(people_vaccinated - tail_fully_vaccinated)]
  o2[, key_date := date[which.min(difference)], by = "location_id"]
  
  o2[, observed_lag := as.numeric(max_date - key_date)]
  
  lag_dt <- o2[date == max_date, c("location_id","observed_lag")]
  # INDIVIDUAL_NAME says no less than 4 weeks, no more than 12
  lag_dt[observed_lag < 28, observed_lag := 28]
  lag_dt[observed_lag > 84, observed_lag := 84]
  
  vaccine_data$write_empirical_lag_days(lag_dt, vaccine_output_root)
  
}

#' Build a booster time series, for a SINGLE LOCATION
#' 
#' Requires a full time series from the base data frame (merges on all.x)
#' Requires: data.table, zoo
#'
#' @param x [data table] REQUIRES columns: date, booster_round. One location of vaccine data.  
#' @param booster_round [integer] Function creates and replaces "booster_1", "booster_2", etc. column
#' @param interp_method [character] "linear" or "sqrt" - how to interpolate NA values
#' @param first_booster_date [character/date] REQUIRE "YYYY-MM-DD". First day boosters have non-zero value - function will force boosters on day prior = 0
#' Finds first non-zero date in data if NULL 
#' @param tail_only [logical] builds only the tail for booster_correction - three logical switches in the function
#'
#' @return [data table]
.build_boosters <- function(x, booster_round, interp_method, 
                            first_booster_date = NULL,
                            tail_only = FALSE) {
  
  # # scoping in
  # rm(x,b)
  # x <- copy(observed_owid[location_name %like% "Botswana"])
  # plot(x$date, x$booster_1)
  # plot(b$date, b$new_booster)
  # first_booster_date <- NULL
  # interp_method <- "sqrt"
  # booster_round <- 1
  # tail_only = T
  
  # setup checks
  require(zoo)
  require(data.table)
  valid_methods <- c("linear", "sqrt")
  b_course <- paste0("booster_", booster_round)
  
  if (any(is.null(interp_method), (!interp_method %in% valid_methods))) {
    stop (" : ", unique(x$location_name), " : ", 
          unique(x$location_id),
          "Invalid interpolation method, please choose: ", 
          paste(valid_methods, collapse = ", "))
  }
  
  if (all(is.na(x[[b_course]]))) {
    return(x) # return undisturbed data for binding
    (stop(unique(x$location_name), " : ", 
          unique(x$location_id), " : ",
          "Booster course ", "'", b_course, "'",
          " is entirely missing from data and was not created. "))
  }
  
  if (is.null(first_booster_date)) {
    first_booster_date <- 
      unique(
        min(x[!is.na(get(b_course)), date])
      )
  }
  
  # (2022-08-07 update: tail_only) build full time series from start and max date 
  if(!tail_only) {first_booster_date <- as.Date(first_booster_date) - 1} # if data starts close to 0 boosters on first recorded day
  b <- x[,.(date, new_booster = get(b_course))]
  t <- c(as.Date(first_booster_date), max(x$date)) 
  b <- merge(b, data.table(date=as.Date(t[1]:t[2])), all.y=T)
  if(!tail_only) {b$new_booster[1] <- 0} # (2022-08-07 update: tail_only)
  
  b$new_booster <- switch(interp_method,
                          "linear"  = zoo::na.approx(b$new_booster),
                          "sqrt"    = .do_sqrt_interp(b$new_booster))
  
  x <- merge(x, b, by ="date", all.x = TRUE, all.y = TRUE)
  
  # refill values
  x$location_id <- .get_column_val(x$location_id)
  if(!tail_only){ # (2022-08-07 update: tail_only)
    x$location_name <- .get_column_val(x$location_name)
    x$brand <- .get_column_val(x$brand)
    x$age_start <- .get_column_val(x$age_start)
    x$age_end <- .get_column_val(x$age_end)
  }
   # (2022-08-07 update: tail_only)
  if(!tail_only) {x[date < first_booster_date, "new_booster"] <- 0} # fill zeros only if building full series
  x[[b_course]] <- NULL
  setnames(x, "new_booster", b_course)
  
  return(x)
  
}


#' Build booster course time series tail for locations with sparse data
#' 
#' Goal is to improve booster correction script, and prevent locations from
#' being added/dropped week over week as the booster point estimate finder may
#' grab or exclude locations with sparse data at random
#'
#' @param observed [data.table] REQUIRES columns: "location_id", at least one
#'   "booster_x", where x is an integer.  Observed vaccine data after main
#'   processing
#' @param max_booster_courses [integer] how many booster courses does the
#'   pipeline expect? (set in model parameters)
#'
#' @return [data.table] processed data with complete tail time series of booster
#'   courses, which won't start at 0, but is sufficient for booster correction
#'   for now
.build_booster_tails <- function(observed, max_booster_courses) {
  rbindlist(use.names = T,
            l = lapply(
              split(observed, observed$location_id), function(x) {
                tryCatch(
                  {
                    for (i in 1:max_booster_courses) {
                      x <- .build_boosters(x, i, "sqrt", tail_only = T)  
                    }
                    # rebuild boosters from components, replace tally only if components are present, do not introduce 0s
                    boosters_in_data <- names(x)[grep("booster_+\\d$", names(x))]
                    b <- x[, ..boosters_in_data]
                    sel <- which(rowSums(b, na.rm = T)>0)
                    x[sel, boosters_administered := rowSums(.SD, na.rm = T), .SDcols = boosters_in_data]
                  },
                  error = function(e) {cat("Error : ", unique(x$location_id), " : ", conditionMessage(e), "\n")}
                )
              }
            )
  )
}

