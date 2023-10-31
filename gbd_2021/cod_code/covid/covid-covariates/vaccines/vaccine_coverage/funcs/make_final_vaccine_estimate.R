
splice <- function(new, old, splice_locs) {
  data <- rbind(
    new[!location_name %in% splice_locs, ],
    old[location_name %in% splice_locs, ],
    fill = T
  )
  return(data)
}

.plotting_function <- function(data_table, y_plot, y1_line, y2_line, title) {
  plot <- ggplot(data_table, aes(x = as.Date(date), y = data_table[[y_plot]], col = current_version, lty = "any vaccinated")) +
    geom_line() +
    geom_line(aes(y = data_table[[y1_line]], lty = "immune")) +
    geom_line(aes(y = data[[y2_line]], lty = "disease only")) +
    scale_y_continuous("Cumulative people", labels = comma) +
    ggtitle(title) +
    theme_bw() +
    xlab("") +
    scale_color_discrete("") +
    scale_linetype("") +
    theme(legend.position = "bottom") +
    guides(color = guide_legend(nrow = 3), lty = guide_legend(nrow = 3))

  if (title == "Other adults") {
    grid.arrange(p, q, r, nrow = 1, top = unique(compare_ref[location_id == loc_id, location_name]))
  }
  return(plot)
}


make_final_vaccine_estimate <- function(vaccine_output_root) {
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  model_inputs_path <- model_parameters$model_inputs_path
  previous_best_path <- model_parameters$previous_best_path

  adults_o65 <- model_parameters$adults_o65
  include_o12 <- model_parameters$include_o12
  include_o5 <- model_parameters$include_o5
  long_range <- model_parameters$long_range
  save_counties <- model_parameters$save_counties
  cdc_scenarios <- model_parameters$cdc_scenarios

  ## -----------------------------------------------------------

  ## Read location hierarchy for QC and population to split if necessary
  hierarchy <- gbd_data$get_covid_modeling_hierarchy()
  eu_locs <- gbd_data$get_other_european_union_hierarchy()

  ########################################################################
  ## Read in vaccine hesitancy, vaccine availability

  accept_ts <- vaccine_data$load_time_point_vaccine_hesitancy(vaccine_output_root, "default")
  accept_ts <- accept_ts[!duplicated(accept_ts[, c("location_id", "smooth_combined_yes")])] # Why could there be duplicates?

  available_dt <- vaccine_data$load_final_doses(vaccine_output_root)

  adult_population <- model_inputs_data$load_adult_population(model_inputs_path, model_parameters)

  ## Read in Essential Workers, assume they have same acceptance as general population
  essential <- static_data$load_essential_workers()

  if (!(44533 %in% essential$location_id)) {
    
    china_and_subnats <- hierarchy[parent_id == 6, location_id]
    china_pop <- adult_population[location_id == 6, adult_population]
    china_pop_other <- sum(adult_population[location_id %in% china_and_subnats[!(china_and_subnats == 44533)], adult_population])
    p <- (china_pop - china_pop_other) / china_pop

    tmp <- as.data.frame(essential[location_id == 6, ])
    sel <- !(colnames(tmp) %in% c("location_id", "location_name"))
    for (i in colnames(tmp)[sel]) tmp[, i] <- tmp[, i] * p
    tmp$location_id <- 44533
    tmp$location_name <- hierarchy[location_id == 44533, location_name]
    essential <- rbind(essential, as.data.table(tmp))
  }


  ## Vaccine efficacy table mainly needed for J&J one dose efficacy
  ve_table <- vaccine_data$load_vaccine_efficacy(vaccine_output_root)
  one_dose_ve <- ve_table[company == "Janssen"]$efficacy

  eua <- .load_emergency_use_authorizations(model_inputs_root = model_inputs_path, use_data_intake = TRUE)


  #-----------------------------------------------------------------------------
  # Use 5+ population for the following locations
  write.csv(eua[eua$age_group == "5-11" & !is.na(eua$date), ], file = file.path(vaccine_output_root, "five_plus_locations.csv"))
  o5_locs <- eua[eua$age_group == "5-11" & !is.na(eua$date), "location_id"]
  o5_locs <- c(o5_locs, 156)
  over5_population <- model_inputs_data$load_o5_population(model_inputs_path)
  setnames(over5_population, "over5_population", "adult_population")
  sel <- which(adult_population$location_id %in% o5_locs)
  adult_population <- rbind(adult_population[-sel, ], over5_population[sel, ])
  adult_population <- adult_population[order(location_id), ]
  #-----------------------------------------------------------------------------

  o65_population <- model_inputs_data$load_o65_population(model_inputs_path)


  ## Merge all those files together
  dt <- merge(available_dt[date >= "2020-12-01"],
    accept_ts[, c("location_id", "smooth_combined_yes")],
    by = "location_id", all.x = T
  )

  dt <- merge(dt, adult_population, by = "location_id")
  dt <- merge(dt, o65_population, by = "location_id")
  dt[, under65_population := adult_population - over65_population]

  dt <- merge(dt, essential[, c(
    "location_id", "essential", "essential_jobtype", "count_gbd_health_workers"
  )], by = "location_id", all.x = T)
  dt[, date := as.Date(date)]

  ## Calculate the final doses by location
  dt[, weighted_doses_per_course := ifelse(weighted_doses_per_course == 0, 2, weighted_doses_per_course)]
  dt[, pct_2_dose := weighted_doses_per_course / 2]


  ## Grab observed data
  # observed_root <- if (long_range) previous_best_path else vaccine_output_root
  observed <- vaccine_data$load_observed_vaccinations(vaccine_output_root)
  lag_dt <- vaccine_data$load_empirical_lag_days(vaccine_output_root)
  lag_dt <- lag_dt[!duplicated(lag_dt), ]

  last_reported_cumulative <- observed[date == max_date]
  last_reported_cumulative <- last_reported_cumulative[location_id %in% hierarchy[most_detailed == 1, location_id]]

  ## Merge observed with dt, adjust available doses.
  dt <- merge(dt, observed[, c(
    "location_id", "date", "daily_reported_vaccinations", "daily_first_vaccinated", "reported_vaccinations",
    "daily_fully_vaccinated", "fully_vaccinated", "people_vaccinated", "daily_people_vaccinated"
  )],
  by = c("date", "location_id"), all.x = T
  )

  dt <- merge(dt, lag_dt, by = "location_id", all.x = T, allow.cartesian = TRUE)
  dt[is.na(observed_lag), observed_lag := 28]

  dt[, pct_vaccinated := people_vaccinated / adult_population]
  # Don't want to allow this to vary with time, accounted for here:
  ## Account for reported % vaccinated
  dt[, any_vaccinated_pct := max(pct_vaccinated, na.rm = T), by = "location_id"]
  dt[!is.finite(any_vaccinated_pct), any_vaccinated_pct := 0]

  dt[, would_be_vaccinated := ifelse(
    is.na(any_vaccinated_pct), smooth_combined_yes,
    ifelse(any_vaccinated_pct > smooth_combined_yes, any_vaccinated_pct, smooth_combined_yes)
  )]

  #########################################################################
  ## Build vaccine coverage scenarios. Only "slow_scale_up" is being used
  ## regularly in the SEIR model but "slow_no_hesitancy", "fast_scale_up",
  ## and "fast_elderly" are all expected in covariates_prep and it will
  ## break (I believe) without them. The CDC scenarios are bespoke,
  ## do not need to be saved regularly.
  #########################################################################
  scale_up_duration <- 90
  lag_days <- 28

  checkpoint <- copy(dt)

  
  slow_scale_up <- make_scenario_priority(
    dt = checkpoint,
    observed = observed,
    model_inputs_path = model_inputs_path,
    last_reported_cumulative = last_reported_cumulative,
    one_dose_ve = one_dose_ve,
    hierarchy = hierarchy,
    priority = "mixed", # Equally splits vaccine between elderly and essential workers
    loss_followup = 0.1, # Proportion lost between doses 1 and 2
    essential_name = "essential", # Name of essential workers column
    scale_up_duration = 60, # How long does the scale up period take from first dose level to maximum?
    max_doses_per_day = 1500000, # What is the maximum number of doses per day?
    distribute_type = "none", # Should "residual" doses not accounted for be distributed?
    lag_days_partial = 14, # Lag between first dose and partial immunity
    start_level = 1, # Proportion of the doses available on day 1 that can be delivered
    partial_1_pct = 0.6, # Proportion of overall VE afforded after 1 dose
    shift_date = T, # Mainly for testing, should available dose date be shifted?
    shift_level = T, # Mainly for testing, should delivery capacity be shifted?
    cascade_spline = T, # Scale up based on cascade spline?
    max_pct_threshold = 0.99
  )


  # Check
  tmp <- slow_scale_up[location_id == 44533]
  plot(tmp$date, tmp$reported_vaccinations, main = tmp$location_name[1])
  points(tmp$date, tmp$people_vaccinated, col = "blue")
  points(tmp$date, tmp$fully_vaccinated, col = "red")
  abline(h = tmp$adult_population, col = "black")
  lines(tmp$date, tmp$cumulative_all_vaccinated, col = "blue")
  lines(tmp$date, tmp$cumulative_all_fully_vaccinated, col = "red")
  lines(tmp$date, tmp$cumulative_all_effective, col = "green")


  checkpoint2 <- copy(slow_scale_up)

  if (TRUE) {
    observed_chn <- fread("FILEPATH/china_vaccination_age.csv")
    if ("initially_vaccinated" %in% colnames(observed_chn)) setnames(observed_chn, "initially_vaccinated", "people_vaccinated")
    if ("total_administered" %in% colnames(observed_chn)) setnames(observed_chn, "total_administered", "reported_vaccinations")
    observed_chn$date <- .convert_dates(observed_chn$date)

    dups <- duplicated(observed_chn)
    if (any(dups)) observed_chn <- observed_chn[!dups, ]
    
    chn_provinces <- c(44533, hierarchy[parent_id == 44533, location_id])

    for (loc in chn_provinces) {


      # Sticking to March 19 point est to avoid breaking in future if/when more data are added
      pt_est_date <- as.Date("2022-03-19")
      sel <- observed_chn$date == pt_est_date &
        observed_chn$age_start >= 1 & observed_chn$age_start < 64 &
        observed_chn$age_end >= 1 & observed_chn$age_end <= 64

      obs_adult <- aggregate(people_vaccinated ~ date, data = observed_chn[sel, ], FUN = sum)
      obs_adult <- obs_adult$people_vaccinated
      tmp_adult_pop <- aggregate(population ~ date, data = observed_chn[sel, ], FUN = sum)
      tmp_adult_pop <- tmp_adult_pop$population

      # The age data are derived from GBD 22 pop sizes, but need to keep things in GBD 19 pop sizes, so scaling down to keep all in GBD 19 land
      obs_adult <- adult_population[location_id == loc, adult_population] * (obs_adult / tmp_adult_pop)
      tmp_adult_pop <- adult_population[location_id == loc, adult_population]

      sel <- observed_chn$date == pt_est_date &
        observed_chn$age_start >= 65 & observed_chn$age_start < 125 &
        observed_chn$age_end >= 65 & observed_chn$age_end <= 125

      obs_elderly <- aggregate(people_vaccinated ~ date, data = observed_chn[sel, ], FUN = sum)
      obs_elderly <- obs_elderly$people_vaccinated
      tmp_elderly_pop <- aggregate(population ~ date, data = observed_chn[sel, ], FUN = sum)
      tmp_elderly_pop <- tmp_elderly_pop$population

      # The age data are derived from GBD 22 pop sizes, but need to keep things in GBD 19 pop sizes, so scaling down to keep all in GBD 19 land
      obs_elderly <- o65_population[location_id == loc, over65_population] * (obs_elderly / tmp_elderly_pop)
      tmp_elderly_pop <- o65_population[location_id == loc, over65_population]

      # Grab vaccine model estimates on point est date for China subnat(s)
      tmp <- slow_scale_up[location_id == loc, ]
      tmp$cumulative_lr_vaccinated <- cumsum(tmp$lr_vaccinated)
      tmp$cumulative_hr_vaccinated <- cumsum(tmp$hr_vaccinated)
      tmp <- tmp[date == pt_est_date, ]

      scalar_adult <- obs_adult / tmp$cumulative_lr_vaccinated
      scalar_elderly <- obs_elderly / tmp$cumulative_hr_vaccinated

      tmp <- as.data.frame(slow_scale_up[location_id == loc, ])

      quants_lr <- c(
        "lr_vaccinated",
        "lr_effective_variant",
        "lr_effective_protected_variant",
        "lr_effective_wildtype",
        "lr_effective_protected_wildtype"
      )

      for (i in quants_lr) tmp[, i] <- tmp[, i] * scalar_adult



      tmp$cumulative_lr_vaccinated <- cumsum(tmp[, quants_lr[1]])
      # tmp$cumulative_lr_vaccinated <- pmin(tmp$cumulative_lr_vaccinated, tmp_adult_pop)
      tmp$cumulative_lr_effective <- cumsum(rowSums(tmp[, quants_lr[-1]]))

      quants_hr <- c(
        "hr_vaccinated",
        "hr_effective_variant",
        "hr_effective_protected_variant",
        "hr_effective_wildtype",
        "hr_effective_protected_wildtype"
      )

      for (i in quants_hr) tmp[, i] <- tmp[, i] * scalar_elderly
      tmp$cumulative_hr_vaccinated <- cumsum(tmp[, quants_hr[1]])
      # tmp$cumulative_hr_vaccinated <- pmin(tmp$cumulative_hr_vaccinated, tmp_elderly_pop)
      tmp$cumulative_hr_effective <- cumsum(rowSums(tmp[, quants_hr[-1]]))

      orig <- as.data.frame(slow_scale_up[location_id == loc, ])

      orig$cumulative_lr_vaccinated <- cumsum(orig[, quants_lr[1]])
      orig$cumulative_lr_effective <- cumsum(rowSums(orig[, quants_lr[-1]]))
      orig$cumulative_hr_vaccinated <- cumsum(orig[, quants_hr[1]])
      orig$cumulative_hr_effective <- cumsum(rowSums(orig[, quants_hr[-1]]))


      slow_scale_up <- slow_scale_up[!(location_id == loc), ]
      slow_scale_up <- rbind(slow_scale_up, tmp, fill = T)

    }
  }

  message("NOTE: the following locations have observations where fully_vaccinated > initially_vaccinated:")
  sel <- slow_scale_up$cumulative_all_vaccinated < slow_scale_up$cumulative_all_fully_vaccinated
  hierarchy[location_id %in% slow_scale_up[sel, location_id], .(location_id, location_name)]


  .check_time_point_vax_willing_USA(
    version_output_path = .output_path,
    vaccine_model_output = slow_scale_up,
    hierarchy = hierarchy
  )


  ## -----------------------------------------------------------------------
  # Adding 5-11 vaccinations as post-hoc scenario for now

  if (model_parameters$child_vaccination_scenario) {

    # Currently for USA only
    # 3wk dosing period, 3mo scale up, willingness from KFF survey
    slow_scale_up <- add_child_vaccinations(
      vaccine_output_root = .output_path,
      model_output_12_plus = slow_scale_up,
      eua_date = "2021-11-03", # Date of Emergency Use Authorization in US
      eua_date_lag = 7, # Number of days after EUA that vaccinations begin
      delivery_schedule = 365 / 4, # Default is delivery rate required to vaccinated all children in state in 6 months
      dosing_period = 21, # Number of days between first and second dose
      prop_parents_willing = 0.45, # Proportion of parents willing to vaccinate their children reported by KFF
      use_hesitancy = FALSE, # If TRUE and prop_parents_willing = NULL, then willingness in children is set to adult vaccinated + willing in each state
      lag_days_partial = 7, # Number of days until partial efficacy after first dose
      lag_days_full = 14, # Number days until full efficacy after second dose
      loss_followup = 0.1, # Proportion of people that do not follow up to get second dose
      efficacy_partial = 0.6, # Efficacy after one dose
      scenario_suffix = NULL
    ) 

  }


  ## -------------------------------------------------------------------------

  print("Saving scenarios!")


  ## -------------------------------------------------------------------------
  # Splice any problem locations
  # The files that go into SEIR are the **slow_scenario_vaccine_coverage.csv** file. Other files are (presumably) required
  # in the SEIR covariates prep and include *fast_scenario_vaccine_coverage.csv* and *fast_elderly_scenario_vaccine_coverage.csv*.

  message("Splicing locations:")

  # Splice by name
  # splice_loc_name <- character(0)
  splice_loc_name <- c("Côte d'Ivoire", "Turkmenistan", "Niger") # , 'Assam', 'Gujarat', 'Kerala', 'Denmark', 'West Virginia')

  # Splice by ID
  # splice_loc_id <- integer(0)
  splice_loc_id <- c(43863)
  
  splice_loc_id <- c(163, hierarchy[parent_id == 163]$location_id) # India and subnats
  # splice_loc_id <- c(95, hierarchy[parent_id == 95]$location_id) # UK and subnats
  # splice_loc_id <- c(86, hierarchy[parent_id == 86]$location_id) # Italy and subnats
  # splice_loc_id <- c(splice_loc_id, 130, hierarchy[parent_id == 130]$location_id) # Mexico and subnats
  # splice_loc_id <- c(splice_loc_id, 4667) # Sinaloa


  sel <- which(hierarchy$location_name %in% splice_loc_name | hierarchy$location_id %in% splice_loc_id)
  tmp <- hierarchy[sel, .(location_id, location_name)]
  print(tmp)

  splice <- function(new, old, splice_locs) {
    data <- rbind(new[!location_id %in% splice_locs, ],
      old[location_id %in% splice_locs, ],
      fill = T
    )
    return(data)
  }

  slow_scale_up <- splice(
    new = slow_scale_up,
    old = vaccine_data$load_scenario_forecast(file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, .previous_best_version), "slow"),
    splice_locs = tmp$location_id
  )


  if (model_parameters$set_past_to_reference) {

    # Resolve historical vaccine coverage
    slow_scale_up <- set_past_to_reference(
      objective_output = slow_scale_up,
      reference_path = model_parameters$reference_path,
      stop_date = NULL,
      t_gap = 7
    )
  }

  # Cap all vaccination quantities so that cumulative sums do not surpass adult population
  slow_scale_up <- duct_tape(object = slow_scale_up)




  ## ----------------------------------------------------------------------
  ## Save versions of just US counties ##
  if (save_counties == T) {
    message("Copying model data and metadata to counties directory")
    system(paste0(SYSTEM_COMMAND))

    message("Splitting output to counties")
    county_slow <- split_us_counties(slow_scale_up, model_parameters)

    message("Saving counties version")
    vaccine_data$write_scenario_forecast(county_slow, .county_path, scenario = "slow")
  }

  ## Save version with all GBD locations
  gbd_locs <- split_gbd_locations(model = slow_scale_up, model_parameters = model_parameters)
  slow_scale_up <- rbind(slow_scale_up, gbd_locs, fill = T)

  ## IFR/IHR calculation requires people vaccinated, protected from disease but not immune
  slow_scale_up <- make_protected_not_immune(slow_scale_up)

  vaccine_data$write_scenario_forecast(slow_scale_up, vaccine_output_root, scenario = "slow")



  ## -----------------------------------------------------------------------
  scenario_list <- list(slow_scale_up)
  scenario_names <- c("slow")

  ## -----------------------------------------------------------------------
  ## Submit QC jobs

  message("Submitting plotting jobs")
  .submit_plot_job(
    plot_script_path = file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "vaccination_lines_check.R"),
    current_version = .output_version
  )

  .submit_plot_job(
    plot_script_path = file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "line_plots_scenarios_compare.R"),
    current_version = .output_version,
    compare_version = .previous_best_version
  )

  plot_script <- file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "final_diagnostic_line_plots.R")
  for (sname in scenario_names) {
    job_name <- paste0("vaccine_plots_", sname)

    .submit_job(
      script_path = plot_script,
      job_name = job_name,
      mem = "10G",
      archiveTF = TRUE,
      threads = "2",
      runtime = "20",
      Partition = "d.q",
      Account = "proj_covid",
      args_list = list(
        "--version " = vaccine_output_root,
        "--scenario_name " = sname
      )
    )
  }

  ## Submit job for maps of people vaccinated ##
  plot_script <- file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "people_vaccinated_maps.R")
  for (sname in c("slow", "fast")) {
    job_name <- paste0("vaccine_maps_", sname)

    .submit_job(
      script_path = plot_script,
      job_name = job_name,
      mem = "10G",
      archiveTF = TRUE,
      threads = "2",
      runtime = "20",
      Partition = "d.q",
      Account = "proj_covid",
      args_list = list(
        "--version " = vaccine_output_root,
        "--scenario_name " = sname
      )
    )
  }

  ## Scatter doses per day with previous best.
  scatter_vax_dates <- function(previous_path, scenario, cumulative, dates, diff = 500000, log = T, relative = 0.25) {
    old <- vaccine_data$load_scenario_forecast(previous_path, scenario)
    old[, date := as.Date(date)]

    if (scenario == "slow") {
      new <- slow_scale_up
    } else if (scenario == "fast") {
      new <- fast_scale_up
    } else {
      new <- fast_elderly
    }

    column_names_for_new <- c(
      "location_id", "date", "cumulative_elderly_vaccinated",
      "cumulative_essential_vaccinated", "cumulative_adults_vaccinated",
      "cumulative_all_vaccinated", "cumulative_all_effective"
    )
    column_names_for_old <- c(column_names_for_new, "location_name")

    compare_ref <- merge(old[, ..column_names_for_old], new[, ..column_names_for_new], by = c("date", "location_id"))
    compare_ref[, xaxis := get(paste0(cumulative, ".x"))]
    compare_ref[, yaxis := get(paste0(cumulative, ".y"))]
    compare_ref[, difference := abs(yaxis - xaxis)]
    compare_ref[, rel_difference := difference / xaxis]
    compare_ref <- compare_ref[!(location_id %in% unique(gbd_locs$location_id))]

    if (log == T) {
      compare_ref[, xaxis := log10(xaxis)]
      compare_ref[, yaxis := log10(yaxis)]
    }

    p <- ggplot(
      compare_ref[date %in% as.Date(dates)],
      aes(x = xaxis, y = yaxis)
    ) +
      geom_point() +
      facet_wrap(~date, scale = "free") +
      theme_bw() +
      geom_abline(intercept = 0, slope = 1) +
      scale_x_continuous("Previous best", labels = comma) +
      scale_y_continuous("New model", labels = comma) +
      ggtitle(cumulative) +
      geom_text_repel(data = compare_ref[rel_difference > relative & date %in% as.Date(dates)], aes(label = location_name))
    print(p)
  }

  ## line plot doses per day with previous best.
  line_model_compare <- function(previous_path, prev_scenario, scenario) {
    old <- vaccine_data$load_scenario_forecast(previous_path, scenario)
    old[, date := as.Date(date)]
    old[, current_version := "Current Best"]
    old[, cumulative_elderly_effective_protected := cumulative_elderly_effective_protected_variant + cumulative_elderly_effective_protected_wildtype]
    old[, cumulative_essential_effective_protected := cumulative_essential_effective_protected_variant + cumulative_essential_effective_protected_wildtype]
    old[, cumulative_adults_effective_protected := cumulative_adults_effective_protected_variant + cumulative_adults_effective_protected_wildtype]
    #
    if (scenario == "slow") {
      new <- slow_scale_up
    } else if (scenario == "fast") {
      new <- fast_scale_up
    } else {
      new <- fast_elderly
    }
    new[, current_version := "New"]
    new[, cumulative_elderly_effective_protected :=
      cumulative_elderly_effective_protected_variant + cumulative_elderly_effective_protected_wildtype]
    new[, cumulative_essential_effective_protected :=
      cumulative_essential_effective_protected_variant + cumulative_essential_effective_protected_wildtype]
    new[, cumulative_adults_effective_protected :=
      cumulative_adults_effective_protected_variant + cumulative_adults_effective_protected_wildtype]

    compare_ref <- rbind(old, new, fill = T)
    for (loc_id in unique(compare_ref$location_id)) {
      data <- compare_ref[location_id == loc_id]
      p <- .plotting_function(
        data, "cumulative_elderly_vaccinated", "cumulative_elderly_effective",
        "cumulative_elderly_effective_protected", "Elderly"
      )
      q <- .plotting_function(
        data, "cumulative_essential_vaccinated", "cumulative_essential_effective",
        "cumulative_essential_effective_protected", "Essential"
      )
      r <- .plotting_function(
        data, "cumulative_adults_vaccinated", "cumulative_adults_effective",
        "cumulative_adults_effective_protected", "Other adults"
      )
    }
  }

  print("Making scatters")
  pdf(file.path(vaccine_output_root, "cumulative_doses_scatter_versions_reference.pdf"), height = 12, width = 12)
  scatter_vax_dates(previous_best_path, "slow", "cumulative_all_vaccinated", c("2021-03-01", "2021-07-01", "2021-05-01", "2021-09-01", "2021-12-01", as.character(Sys.Date())), diff = 750000)
  scatter_vax_dates(previous_best_path, "slow", "cumulative_all_vaccinated", c("2020-12-31", "2021-03-01", "2021-04-01", "2021-09-01", "2021-12-01", as.character(Sys.Date())), relative = 0.5)
  scatter_vax_dates(previous_best_path, "slow", "cumulative_elderly_vaccinated", c("2020-12-31", "2021-03-01", "2021-04-01", "2021-09-01", "2021-12-01", as.character(Sys.Date())), diff = 750000)
  scatter_vax_dates(previous_best_path, "slow", "cumulative_essential_vaccinated", c("2020-12-31", "2021-03-01", "2021-04-01", "2021-09-01", "2021-12-01", as.character(Sys.Date())), diff = 750000)
  scatter_vax_dates(previous_best_path, "slow", "cumulative_adults_vaccinated", c("2020-12-31", "2021-03-01", "2021-04-01", "2021-09-01", "2021-12-01", as.character(Sys.Date())), diff = 750000)
  dev.off()

  current <- tail(unlist(strsplit(vaccine_output_root, "/")), n = 1)
  previous_best <- tail(unlist(strsplit(previous_best_path, "/")), n = 1)
  pdf(file.path(vaccine_output_root, glue("cumulative_effective_scatter_{current}_{previous_best}.pdf")), height = 12, width = 12)
  scatter_vax_dates(previous_best_path, "slow", "cumulative_all_effective", c("2020-12-31", "2021-02-01", "2021-03-01", "2021-04-01", "2021-12-01", as.character(Sys.Date())), relative = 0.5)
  scatter_vax_dates(previous_best_path, "slow", "cumulative_all_effective", c("2021-05-01", "2021-06-01", "2021-07-01", "2021-08-01", "2021-12-01", as.character(Sys.Date())), relative = 0.5)
  dev.off()
}
