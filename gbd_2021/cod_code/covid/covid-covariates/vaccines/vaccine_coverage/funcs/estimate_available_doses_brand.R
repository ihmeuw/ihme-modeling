
estimate_available_doses_brand <- function(vaccine_output_root) {

  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  model_inputs_root <- model_parameters$model_inputs_path
  gavi_doses <- model_parameters$gavi_dose_scenario
  end_date <- as.Date(model_parameters$projection_end_date)

  ##------------------------------------------------------------------------
  ## Location hierarchies, population
  population <- model_inputs_data$load_total_population(model_inputs_root)
  hierarchy <- gbd_data$get_covid_modeling_hierarchy()

  ##------------------------------------------------------------------------
  message('Making purchase candidates')

  pc_list <- make_purchase_candidates(data_root = model_inputs_root,
                                      output_root = vaccine_output_root,
                                      total_population = population)

  purchase_candidates <- pc_list$purchase_candidates
  covid_modeling_hierarchy <- pc_list$covid_modeling_hierarchy
  eu_block <- pc_list$eu_block

  ##------------------------------------------------------------------------------------------------
  ## Great! Now by manufacturer and candidate, we have probability and number of doses by quarter.
  ## We also have the "purchase_blocks" which is the number of doses secured by multilateral or national
  ## purchasing agreements. This is in an object called "purchase_candidates" which will be used
  ## a great deal from here forward.
  ##-------------------------------------------------------------------------------
  # A couple updates needed before days and doses established
  purchase_candidates <- pre_stockpile_modifications(purchase_candidates, hierarchy)
  purchase_candidates <- make_stockpile(purchase_candidates, hierarchy)
  purchase_candidates <- post_stockpile_modifications(purchase_candidates)

  ##-------------------------------------------------------------------------
  ## Update COVAX AMC locations
  # need to do this here cause it's used in flat file modifications for some reason.

  #message(paste('Processing GAVI scenario data | Scenario =', gavi_doses))
  #gavi_out <- process_gavi_allocations(model_inputs_root, gavi_doses, hierarchy)
  #gavi_out <- process_new_gavi_allocations(DATA_ROOTS$DATA_INTAKE_GAVI_ROOT, gavi_doses, hierarchy)

  ## Make changes from CSV (ideal version for this type of modification)
  # Will also make gavi modifications
  message('Flat file modifications')

  dose_updates <- model_inputs_data$load_dose_update_sheet(model_inputs_root)

  # If including gavi senario doses, do not include manual updates of gavi source doses from flat file
  if (.model_parameters$use_gavi) dose_updates <- dose_updates[!(location %in% c('COVAX', 'COVAX AMC')),]


  
  purchase_candidates <- flat_file_modifications(purchase_candidates=purchase_candidates,
                                                 dose_update_sheet=dose_updates,
                                                 gavi_candidates=NULL # set to NULL to switch off
  )

  ##-------------------------------------------------------------------------
  # Increase doses by 20% for Pfizer (more doses per vial than expected - how does this happen?)
  purchase_candidates[company == "Pfizer/BioNTech", daily_probable_doses := daily_probable_doses / 0.8]
  purchase_candidates[company == "Pfizer/BioNTech", stockpile := stockpile / 0.8]

  # Effective doses
  purchase_candidates[, daily_effective_probable_doses := daily_probable_doses * efficacy]
  purchase_candidates[, stockpile_effective := stockpile * efficacy]

  # Assume initial doses will all be sent to purchase block locations
  purchase_candidates[, purchase_block_quarterly_doses := ifelse(date == expected_ready,
                                                                 proportion_all_purchase * quarterly_probable_doses,
                                                                 proportion_annual_capacity * quarterly_probable_doses)]

  purchase_candidates[, purchase_block_secured_daily_doses := ifelse(date == expected_ready,
                                                                     proportion_all_purchase * daily_probable_doses,
                                                                     proportion_annual_capacity * daily_probable_doses)]

  # Figure out the proportion of the stockpile to that location (I think this is daily)
  purchase_candidates[, location_stockpile := proportion_all_purchase * purchase_block_pop * stockpile]

  # Find quarterly doses in each country based on population size
  purchase_candidates[, location_quarterly_doses := purchase_block_pop * purchase_block_quarterly_doses]

  # Add doses later for the US (July 1)
  purchase_candidates[location == "United States of America" &
                        company %in% c("Pfizer/BioNTech","Moderna") &
                        date == as.Date("2021-07-01"),
                      c("purchase_block_secured_daily_doses") := purchase_block_secured_daily_doses + 100000000 / effective_days]

  purchase_candidates[location == "United States of America" &
                        company %in% c("Pfizer/BioNTech","Moderna") &
                        date == as.Date("2021-01-01"),
                      c("purchase_block_secured_daily_doses") := purchase_block_secured_daily_doses + 20000000 / effective_days]

  purchase_candidates[, location_secured_daily_doses := purchase_block_pop * purchase_block_secured_daily_doses]

  ##--------------------------------------------------------------
  # Remove doses that have been secured in the stockpile
  message('Various stockpile modifications')

  purchase_candidates[, total_doses_from_manufacturer := sum(location_secured_daily_doses * effective_days), by=c("location_id","company")]
  purchase_candidates[, stockpile_length := ifelse(candidate == "BNT-162", 17,
                                                   ifelse(company == "Moderna", 10, 30))]
  purchase_candidates[, total_location_stockpile := sum(location_stockpile * stockpile_length), by = c("location_id","company")]
  purchase_candidates[, num_quarters := length(candidate)-1, by=c("location","location_id","company")]
  purchase_candidates[, remove_stockpile_from_daily := total_location_stockpile / num_quarters / effective_days]
  purchase_candidates[, remove_stockpile_from_daily := ifelse(location_stockpile > 0, 0, remove_stockpile_from_daily)]

  purchase_candidates[, remove_stockpile_from_daily := ifelse(candidate %in% c("BNT-162","Moderna","NVX CoV-2373"), 0, remove_stockpile_from_daily)]

  # Push extra stockpile doses in the EU back
  eu_az_candidates <- purchase_candidates[location == "European Commission" & company == "AstraZeneca"]
  eu_az_candidates[, april1 := location_secured_daily_doses[date == "2021-04-01"], by = "location_id"]
  eu_az_candidates[, location_secured_daily_doses := ifelse(date == "2021-04-01", location_secured_daily_doses / 2,
                                                            ifelse(date > "2021-04-01", location_secured_daily_doses + april1 / 4 * 91 / 92,
                                                                   location_secured_daily_doses))]
  eu_az_candidates[, april1 := NULL]

  purchase_candidates <- rbind(purchase_candidates[!(location == "European Commission" & company == "AstraZeneca")],
                               eu_az_candidates)

  purchase_candidates[, daily_remove_stockpile := location_secured_daily_doses - remove_stockpile_from_daily]
  # Don't allow to be negative, the implication of this is that a location
  # gets all its secured doses as part of the stockpile.
  purchase_candidates[daily_remove_stockpile < 0, daily_remove_stockpile := 0]

  purchase_candidates[, total_secured_location := sum(daily_remove_stockpile * effective_days), by = c("location_id","company")]
  purchase_candidates[, total_secured_location := total_secured_location + total_location_stockpile]

  purchase_candidates[, location_secured_daily_doses := daily_remove_stockpile]
  purchase_candidates[, location_secured_quarter_doses := location_secured_daily_doses * effective_days]
  purchase_candidates[, location_quarter_stockpile := location_stockpile * stockpile_length]
  purchase_candidates[, review_total := location_secured_quarter_doses + location_quarter_stockpile]

  ##############################################################################################################################
  ##############################################################################################################################
  ## Load and merge GAVI scenarios

  if (.model_parameters$use_gavi) {

    message(paste('Processing GAVI scenario data | Scenario =', gavi_doses))

    if (gavi_doses %in% c('more', 'less')) { # Previous GAVI scenarios (1.3B versus 1.8B)

      gavi_out <- process_gavi_allocations(model_inputs_root, gavi_doses, hierarchy)

    } else if (gavi_doses %in% c('low', 'medium', 'high')) {

      gavi_out <- process_new_gavi_allocations(data_root=DATA_ROOTS$DATA_INTAKE_GAVI_ROOT,
                                               gavi_dose_scenario=gavi_doses,
                                               location_hierarchy=hierarchy)

    } else stop('Unrecognized GAVI scenario')

    # Add quantities used downstream
    gavi_dates <- sort(unique(gavi_out$date))
    gavi_effective_days <- c(92, 183, 364)

    for (i in seq_along(gavi_dates)) {
      sel <- which(gavi_out$date == gavi_dates[i])
      gavi_out[sel, effective_date := date - gavi_effective_days[i]]
      gavi_out[sel, effective_days := gavi_effective_days[i]]
      gavi_out[sel, location_secured_daily_doses := named_doses / effective_days]
    }

    gavi_out[is.na(location_secured_daily_doses), location_secured_daily_doses := 0]

    # Ensure that efficacy exists for all entries
    effectiveness <- vaccine_data$load_vaccine_efficacy(vaccine_output_root)

    gavi_out <- do.call(
      rbind,
      lapply(split(gavi_out, by='merge_name'), function(x) {

        tryCatch( {

          x$efficacy <- effectiveness[effectiveness$merge_name %in% x$merge_name, 'efficacy']
          x$variant_efficacy <- effectiveness[effectiveness$merge_name %in% x$merge_name, 'variant_efficacy']
          x$prop_protected_not_infectious <- effectiveness[effectiveness$merge_name %in% x$merge_name, 'prop_protected_not_infectious']

        }, error=function(e){

          cat("Error :", unique(x$merge_name), ":", conditionMessage(e), "\n")

        })

        return(x)

      })
    )

    gavi_out$manufacturer <- gavi_out$company
    gavi_out$wastage <- .get_column_val(purchase_candidates$wastage)
    head(gavi_out)



    # Remove all COVAX entries after start of Gavi scenarios
    sel <- which(purchase_candidates$location %in% c('COVAX', 'COVAX AMC') &
                   purchase_candidates$effective_date >= min(gavi_out$effective_date))


    # Append to purchase_candidates
    purchase_candidates <- rbind(purchase_candidates[-sel,], gavi_out, fill=TRUE)
    #purchase_candidates <- rbind(purchase_candidates, gavi_out, fill=TRUE)

  }
  #message(glue('Merging {gavi_doses} GAVI scenario with purchase candidates'))
  # This has to be merged first to not duplicate previously defined purchase agreements?
  #purchase_candidates <- merge(purchase_candidates,
  #                             gavi_out[,c("location_id","date","company","named_doses","gavi_scenario")],
  #                             by=c("location_id","date","company"),
  #                             all=TRUE)
  #
  # Now add details to merged gavi rows
  #gavi_out <- purchase_candidates[!is.na(gavi_scenario),]


  #purchase_candidates <- rbind(purchase_candidates[is.na(gavi_scenario),], gavi_out)

  ##############################################################################################################################
  ##############################################################################################################################


  # WHat is this?? So we assume that Pfizer doses arrive immediately but others arrive over time?? (This is for COVAX coming in from flat file I think)
  #purchase_candidates[location_id %in% unique(gavi_out$location_id) & location == "COVAX AMC" & company != "Pfizer/BioNTech",
  #                    location_secured_daily_doses := named_doses / effective_days]

  # ## Doses are swapped from purchase candidates to exactly match doses provided by Gavi, however we need to add
  # ## 200m doses in 2021 and 300m in 2022 https://www.theguardian.com/us-news/2021/jun/09/us-biden-vaccines-pfizer-global-plan
  # ## Need to add 100m in Q3 and Q4

  # purchase_candidates[location_id %in% unique(gavi_out$location_id) & date >= "2021-07-01" & company == "Pfizer/BioNTech" & location == "COVAX AMC",
  #                     location_secured_daily_doses := location_secured_daily_doses + (100000000 * purchase_block_pop / effective_days)] # Add the 100m,
  # split by pop
  ## Now, add a new row for Q1 in 2022
  #gavi_2022_q1 <- purchase_candidates[location_id %in% unique(gavi_out$location_id) &
  #                                      location == "COVAX AMC" &
  #                                      date == "2021-10-01" &
  #                                      company == "Pfizer/BioNTech"]
  #gavi_2022_q1[, c("date", "effective_date") := as.Date("2022-01-01")]
  #gavi_2022_q1[, effective_days := 90]
  #gavi_2022_q1[, location_secured_daily_doses := (300000000 * purchase_block_pop / effective_days)]
  #purchase_candidates <- rbind(purchase_candidates, gavi_2022_q1)

  #purchase_candidates[location_id %in% unique(gavi_out$location_id) &
  #                      location == "COVAX AMC" &
  #                      is.na(location_secured_daily_doses),
  #                    location_secured_daily_doses := 0]


  ##--------------------------------------------------------------
  # Find quarterly pre-purchased doses, aggregated by company
  company_all_secured <- purchase_candidates[, lapply(.SD, function(x) sum(x, na.rm=T)), by=c("company","candidate","quarter"),
                                             .SDcols = c("location_quarterly_doses")]

  company_all_secured <- merge(company_all_secured, unique(purchase_candidates[,c("company","candidate","quarter","probable_doses")]),
                               by = c("company","candidate","quarter"))
  # Find doses unallocated
  company_all_secured[, residual_quarterly_doses := probable_doses - location_quarterly_doses]

  # Merge residual back on, reallocated residual to pre-purchase blocks.
  purchase_candidates <- merge(purchase_candidates, company_all_secured[,c("quarter","residual_quarterly_doses","company","candidate")],
                               by = c("quarter","company","candidate"), all.x=T)
  purchase_candidates[, daily_residual_doses := residual_quarterly_doses / quarter_days * effective_quarter]
  purchase_candidates[, purchased_location_daily_residual := ifelse(date == expected_ready,
                                                                    proportion_all_purchase * daily_residual_doses * purchase_block_pop,
                                                                    proportion_annual_capacity * daily_residual_doses * purchase_block_pop)]


  # Calculate number of doses that will lead to protected only and protected and not infectious
  # Use daily!
  purchase_candidates[, doses_protected := location_quarterly_doses * efficacy]
  purchase_candidates[, secured_daily_doses_protected := location_secured_daily_doses * efficacy]

  # Summary counts before wastage
  purchase_candidates <- purchase_candidates[!is.na(location_id),]

  purchase_candidates[, sum_location_secured_daily_doses := location_secured_daily_doses * effective_days + location_stockpile * stockpile_length]
  purchase_candidates[candidate == "BNT-162", sum_location_secured_daily_doses := sum_location_secured_daily_doses - total_location_stockpile / (num_quarters + 1)]
  if (.model_parameters$use_gavi) purchase_candidates[location == 'GAVI', sum_location_secured_daily_doses := location_secured_daily_doses * effective_days]

  #purchase_candidates[is.na(sum_location_secured_daily_doses), sum_location_secured_daily_doses := location_secured_daily_doses * effective_days]
  purchase_candidates[sum_location_secured_daily_doses < 0, sum_location_secured_daily_doses := 0]

  purchase_candidates[, secured_effective_daily_doses := efficacy * location_secured_daily_doses]
  purchase_candidates[, secured_variant_effective_daily_doses := variant_efficacy * location_secured_daily_doses]
  purchase_candidates[, effective_protected_daily_doses := prop_protected_not_infectious * location_secured_daily_doses]
  purchase_candidates[, effective_residual_secured_daily_doses := efficacy * purchased_location_daily_residual]

  # Account for wastage
  purchase_candidates[, location_secured_daily_doses := location_secured_daily_doses * (1 - wastage)]
  purchase_candidates[, purchased_location_daily_residual := purchased_location_daily_residual * (1 - wastage)]
  purchase_candidates[, secured_effective_daily_doses := secured_effective_daily_doses * (1 - wastage)]
  purchase_candidates[, secured_variant_effective_daily_doses := secured_variant_effective_daily_doses * (1 - wastage)]
  purchase_candidates[, effective_protected_daily_doses := effective_protected_daily_doses * (1 - wastage)]
  purchase_candidates[, effective_residual_secured_daily_doses := effective_residual_secured_daily_doses * (1 - wastage)]

  purchase_candidates[, full_course := location_secured_daily_doses / ifelse(manufacturer == "Janssen", 1, 2)]

  # Save that file
  purchase_candidates <- purchase_candidates[!is.na(date)]
  purchase_candidates <- purchase_candidates[order(location_id, effective_date)]
  purchase_candidates[, cumulative_daily := cumsum(location_secured_daily_doses)]

  setcolorder(purchase_candidates, order(colnames(purchase_candidates)) )
  vaccine_data$write_purchase_candidates(purchase_candidates, vaccine_output_root)
  message(paste('Writing purchase candidates to file:', file.path(vaccine_output_root, 'purchase_candidates.csv')))

  ## I often want to review a change here, so uncomment to run for a specific location
  # ggplot(purchase_candidates[location_id == loc & !is.na(date)], aes(x=factor(effective_date), y=sum_location_secured_daily_doses, fill = manufacturer)) +
  #   geom_bar(stat="identity", col = "black") + xlab("Start date doses available") + scale_y_continuous("Doses", labels = comma) +
  #   scale_fill_discrete("") +
  #   ggtitle(unique(purchase_candidates[location_id == loc, location_name]),
  #           subtitle = paste0(format(round(sum(purchase_candidates[location_id == loc]$sum_location_secured_daily_doses), 0), big.mark = ","),
  #                             " total probable secured doses")) +
  #   theme_bw() +
  #   facet_wrap(~location) + theme(axis.text.x = element_text(angle = 90, hjust = 1))

  ## What a mess! I can't figure out how to do this in a prettier, more intuitive way, so brute force here to get daily doses when
  ## start dates are not the same.
  # Need to track AstraZeneca separately
  message('Getting start dates and making daily doses')
  purchase_candidates[, az_doses := ifelse(company == "AstraZeneca", location_secured_daily_doses, 0)]

  purchase_candidates_old <- purchase_candidates
  purchase_candidates <- purchase_candidates_old

  UComp <- unique(purchase_candidates$company)
  UComp_no_space <- gsub(" ", "", UComp, fixed = TRUE)


  ##### ADDED
  comp_dose_names <- paste(UComp_no_space,"doses", sep="_")
  comp_effective_names <- paste(UComp_no_space,"effective", sep="_")
  comp_effect_names <- paste(UComp_no_space,"effect", sep="_")
  comp_protective_names <- paste(UComp_no_space,"protective", sep="_")
  comp_protect_names <- paste(UComp_no_space,"protect", sep="_")
  comp_variant_effective_names <- paste(UComp_no_space,"variant_effective", sep="_")
  comp_variant_effect_names <- paste(UComp_no_space,"variant_effect", sep="_")
  comp_residual_effective_names <- paste(UComp_no_space,"residual_effective", sep="_")
  comp_residual_effect_names <- paste(UComp_no_space,"residual_effect", sep="_")

  for (comp_num in 1:length(UComp)){
    purchase_candidates[, (comp_dose_names[comp_num]) := ifelse(company == UComp[comp_num], location_secured_daily_doses, 0)]
    purchase_candidates[, (comp_effective_names[comp_num]) := ifelse(company == UComp[comp_num], secured_effective_daily_doses, 0)]
    purchase_candidates[, (comp_protective_names[comp_num]) := ifelse(company == UComp[comp_num], effective_protected_daily_doses, 0)]
    purchase_candidates[, (comp_variant_effective_names[comp_num]) := ifelse(company == UComp[comp_num], secured_variant_effective_daily_doses, 0)]
    purchase_candidates[, (comp_residual_effective_names[comp_num]) := ifelse(company == UComp[comp_num], effective_residual_secured_daily_doses, 0)]
  }
  ##### STOP ADDED

  ##### CHANGED
  cmp <- purchase_candidates[, lapply(.SD, function(x) sum(x)), by=c("date","effective_date","location_id"),
                             .SDcols = c("location_secured_daily_doses", "location_stockpile","purchased_location_daily_residual",
                                         "secured_effective_daily_doses","secured_variant_effective_daily_doses",
                                         "effective_protected_daily_doses", "az_doses",
                                         "effective_residual_secured_daily_doses", "full_course",
                                         comp_dose_names, comp_effective_names, comp_protective_names, comp_variant_effective_names, comp_residual_effective_names)] # "residual_secured_daily_doses"

  tres <- purchase_candidates[, lapply(.SD, function(x) sum(x, na.rm=T)), by=c("date","effective_date"),
                              .SDcols = c("purchased_location_daily_residual","effective_residual_secured_daily_doses")]
  setnames(tres, c("purchased_location_daily_residual","effective_residual_secured_daily_doses"), c("global_daily_residual","effective_global_daily_residual"))

  cmp <- merge(cmp, tres, by=c("date","effective_date"))
  cmp <- cmp[order(location_id, effective_date)]
  cmp[, weighted_doses_per_course := location_secured_daily_doses / full_course]

  ## This is so ugly. What I am trying to do is make a column with daily doses by type
  ## The reason it is hard is because not all vaccines start on the same date. For example,
  ## most of the time we have daily doses defined by quarterly capacity, so the number of
  ## doses starts on the first date of a quarter. However, I also want to account for vaccines
  ## that become available in the middle of a quarter (like April 8 for example). So, I am
  ## mainly having trouble getting the time series of doses to sum when encountering a new
  ## date. That is what I am getting around here in a very awkward way.

  ##### CHANGED
  key_dates <- as.Date(c("2021-01-01","2021-04-01","2021-07-01","2021-10-01","2022-01-01"))

  cmp[, doses := ifelse(effective_date %in% key_dates, location_secured_daily_doses,
                        location_secured_daily_doses + shift(location_secured_daily_doses, fill = 0)), by="location_id"]
  cmp[, pldr := ifelse(effective_date %in% key_dates, purchased_location_daily_residual,
                       purchased_location_daily_residual + shift(purchased_location_daily_residual, fill = 0)), by="location_id"]
  cmp[, ldd := ifelse(effective_date %in% key_dates, secured_effective_daily_doses,
                      secured_effective_daily_doses + shift(secured_effective_daily_doses, fill = 0)), by="location_id"]
  # Add variant
  cmp[, vdd := ifelse(effective_date %in% key_dates, secured_variant_effective_daily_doses,
                      secured_variant_effective_daily_doses + shift(secured_variant_effective_daily_doses, fill = 0)), by="location_id"]
  # Add protected (not contagious)
  cmp[, pnc := ifelse(effective_date %in% key_dates, effective_protected_daily_doses,
                      effective_protected_daily_doses + shift(effective_protected_daily_doses, fill = 0)), by="location_id"]

  cmp[, er := ifelse(effective_date %in% key_dates, effective_residual_secured_daily_doses,
                     effective_residual_secured_daily_doses + shift(effective_residual_secured_daily_doses, fill = 0)), by="location_id"]
  cmp[, gdr := ifelse(effective_date %in% key_dates, global_daily_residual,
                      global_daily_residual + shift(global_daily_residual, fill = 0)), by="location_id"]
  cmp[, egdr := ifelse(effective_date %in% key_dates, effective_global_daily_residual,
                       effective_global_daily_residual + shift(effective_global_daily_residual, fill = 0)), by="location_id"]
  cmp[, fc := ifelse(effective_date %in% key_dates, full_course,
                     full_course + shift(full_course, fill = 0)), by="location_id"]
  cmp[, lsp := ifelse(effective_date %in% c(as.Date("2021-02-01"), key_dates), location_stockpile,
                      location_stockpile + shift(location_stockpile, fill = 0)), by="location_id"]
  # AstraZeneca doses
  cmp[, az := ifelse(effective_date %in% c(as.Date("2021-02-01"), key_dates), az_doses,
                     az_doses + shift(az_doses, fill = 0)), by="location_id"]

  #### ADDED
  for (comp_num in 1:length(UComp)){
    cmp[, (UComp_no_space[comp_num]) := ifelse(effective_date %in% c(as.Date("2021-02-01"), key_dates), get(comp_dose_names[comp_num]),
                                               get(comp_dose_names[comp_num]) + shift(get(comp_dose_names[comp_num]), fill = 0)), by="location_id"]
    cmp[, (comp_effect_names[comp_num]) := ifelse(effective_date %in% c(as.Date("2021-02-01"), key_dates), get(comp_effective_names[comp_num]),
                                                  get(comp_effective_names[comp_num]) + shift(get(comp_effective_names[comp_num]), fill = 0)), by="location_id"]
    cmp[, (comp_protect_names[comp_num]) := ifelse(effective_date %in% c(as.Date("2021-02-01"), key_dates), get(comp_protective_names[comp_num]),
                                                   get(comp_protective_names[comp_num]) + shift(get(comp_protective_names[comp_num]), fill = 0)), by="location_id"]
    cmp[, (comp_variant_effect_names[comp_num]) := ifelse(effective_date %in% c(as.Date("2021-02-01"), key_dates), get(comp_variant_effective_names[comp_num]),
                                                          get(comp_variant_effective_names[comp_num]) + shift(get(comp_variant_effective_names[comp_num]), fill = 0)), by="location_id"]
    cmp[, (comp_residual_effect_names[comp_num]) := ifelse(effective_date %in% c(as.Date("2021-02-01"), key_dates), get(comp_residual_effective_names[comp_num]),
                                                           get(comp_residual_effective_names[comp_num]) + shift(get(comp_residual_effective_names[comp_num]), fill = 0)), by="location_id"]
  }

  cmp[,doses] / rowSums(cmp[,..UComp_no_space])
  ##### STOP ADDED

  cmp[is.na(doses), doses := location_secured_daily_doses]
  cmp[is.na(pldr), pldr := purchased_location_daily_residual]
  cmp[is.na(ldd), ldd := secured_effective_daily_doses]
  cmp[is.na(vdd), vdd := secured_variant_effective_daily_doses]
  cmp[is.na(pnc), pnc := effective_protected_daily_doses]
  cmp[is.na(er), er := effective_residual_secured_daily_doses]
  cmp[is.na(gdr), gdr := global_daily_residual]
  cmp[is.na(egdr), egdr := effective_global_daily_residual]
  cmp[is.na(fc), fc := full_course]
  cmp[is.na(lsp), lsp := location_stockpile]
  cmp[is.na(az), az := az_doses]

  ##### ADDED
  # No idea if I need a few more "gets" in here...
  for (comp_num in 1:length(UComp)){
    cmp[is.na(UComp_no_space[comp_num]),  (UComp_no_space[comp_num]) := get(comp_dose_names[comp_num])]
    cmp[is.na(comp_effect_names[comp_num]),  (comp_effect_names[comp_num]) := get(comp_effective_names[comp_num])]
    cmp[is.na(comp_protect_names[comp_num]),  (comp_protect_names[comp_num]) := get(comp_protective_names[comp_num])]
    cmp[is.na(comp_variant_effect_names[comp_num]),  (comp_variant_effect_names[comp_num]) := get(comp_variant_effective_names[comp_num])]
    cmp[is.na(comp_residual_effect_names[comp_num]),  (comp_residual_effect_names[comp_num]) := get(comp_residual_effective_names[comp_num])]
  }
  ##### STOP ADDED



  cmp <- cmp[!is.na(location_id)]
  cmp[, date := effective_date]

  ## I don't know why but the Marshall Islands have 0s. Replace with Guam
  cmp_mshl_isl <- cmp[location_id == 349] # Guam
  cmp_mshl_isl$location_id <- 24

  cmp <- rbind(cmp[location_id != 24],
               cmp_mshl_isl)

  # Create a long location-date dataframe for the final results
  message('Collating final long-form data')
  final_dt <- data.table(expand.grid(date = seq(as.Date("2020-01-01"), end_date, 1), location_id = unique(covid_modeling_hierarchy$location_id)))

  final_dt <- merge(final_dt, cmp, by=c("location_id","date"), all.x=T)
  final_dt <- final_dt[order(location_id, date)]

  final_dt[, secured_daily_doses := ifelse(date == "2020-01-01", 0, doses)]
  final_dt[, secured_daily_doses := na.locf(secured_daily_doses), by = "location_id"]

  final_dt[, az_doses := ifelse(date == "2020-01-01", 0, az)]
  final_dt[, az_doses := na.locf(az_doses), by = "location_id"]

  ##### ADDED
  for (comp_num in 1:length(UComp)){
    final_dt[, (comp_dose_names[comp_num]) := ifelse(date == "2020-01-01", 0, get(UComp_no_space[comp_num]))]
    final_dt[, (comp_dose_names[comp_num]) := na.locf(get(comp_dose_names[comp_num])), by = "location_id"]
    #
    final_dt[, (comp_effective_names[comp_num]) := ifelse(date == "2020-01-01", 0, get(comp_effect_names[comp_num]))]
    final_dt[, (comp_effective_names[comp_num]) := na.locf(get(comp_effective_names[comp_num])), by = "location_id"]
    #
    final_dt[, (comp_protective_names[comp_num]) := ifelse(date == "2020-01-01", 0, get(comp_protect_names[comp_num]))]
    final_dt[, (comp_protective_names[comp_num]) := na.locf(get(comp_protective_names[comp_num])), by = "location_id"]
    #
    final_dt[, (comp_variant_effective_names[comp_num]) := ifelse(date == "2020-01-01", 0, get(comp_variant_effect_names[comp_num]))]
    final_dt[, (comp_variant_effective_names[comp_num]) := na.locf(get(comp_variant_effective_names[comp_num])), by = "location_id"]
    #
    final_dt[, (comp_residual_effective_names[comp_num]) := ifelse(date == "2020-01-01", 0, get(comp_residual_effect_names[comp_num]))]
    final_dt[, (comp_residual_effective_names[comp_num]) := na.locf(get(comp_residual_effective_names[comp_num])), by = "location_id"]
  }
  ##### STOP ADDED

  # Round about way to add stockpile doses
  final_dt[, effective_date := as.Date(effective_date)]
  final_dt[, effective_date := fifelse(date == "2020-01-01", as.Date("2020-01-01"), effective_date)]
  final_dt[, effective_date := na.locf(effective_date), by = "location_id"]
  final_dt[, daily_stockpile_doses := ifelse(date == "2020-01-01", 0, lsp)]
  final_dt[, daily_stockpile_doses := na.locf(daily_stockpile_doses), by = "location_id"]
  final_dt[, daily_stockpile_doses := ifelse(as.Date(date) <= as.Date(effective_date) + 29, daily_stockpile_doses, 0)]

  final_dt[, weighted_doses_per_course := ifelse(date == "2020-01-01", 0, fc)]
  final_dt[, weighted_doses_per_course := na.locf(weighted_doses_per_course), by = "location_id"]
  final_dt[, weighted_doses_per_course := secured_daily_doses / weighted_doses_per_course]

  final_dt[, secured_effective_daily_doses := ifelse(date == "2020-01-01", 0, ldd)]
  final_dt[, secured_effective_daily_doses := na.locf(secured_effective_daily_doses), by = "location_id"]

  final_dt[, secured_variant_effective_daily_doses := ifelse(date == "2020-01-01", 0, vdd)]
  final_dt[, secured_variant_effective_daily_doses := na.locf(secured_variant_effective_daily_doses), by = "location_id"]
  final_dt[, effective_protected_daily_doses := ifelse(date == "2020-01-01", 0, pnc)]
  final_dt[, effective_protected_daily_doses := na.locf(effective_protected_daily_doses), by = "location_id"]

  final_dt[, residual_secured_daily_doses := ifelse(date == "2020-01-01", 0, pldr)]
  final_dt[, residual_secured_daily_doses := na.locf(residual_secured_daily_doses), by = "location_id"]

  final_dt[, effective_daily_residual := ifelse(date == "2020-01-01", 0, er)]
  final_dt[, effective_daily_residual := na.locf(effective_daily_residual), by = "location_id"]

  final_dt[, global_daily_residual := ifelse(date == "2020-01-01", 0, gdr)]
  final_dt[, global_daily_residual := na.locf(global_daily_residual), by = "location_id"]

  final_dt[, global_effective_daily_residual := ifelse(date == "2020-01-01", 0, egdr)]
  final_dt[, global_effective_daily_residual := na.locf(global_effective_daily_residual), by = "location_id"]

  # Make an effective stockpile doses
  final_dt[, effective_stockpile_doses := daily_stockpile_doses * (secured_effective_daily_doses / secured_daily_doses)]

  ## We only expect Pfizer, Moderna to be available in 2020
  # (except in Russia, China)
  
  final_dt[!(location_id %in% c(62, hierarchy[parent_id == 6, location_id])),
           secured_daily_doses := ifelse(date < "2021-01-01", 0, secured_daily_doses)]
  final_dt[!(location_id %in% c(62, hierarchy[parent_id == 6, location_id])),
           secured_effective_daily_doses := ifelse(date < "2021-01-01", 0, secured_effective_daily_doses)]

  ##### CHANGED BELOW
  tmp_final_dt <- final_dt
  final_dt <- tmp_final_dt
  # Keep only the ones of interest
  col_names_to_keep <- c("location_id","date","secured_daily_doses","daily_stockpile_doses","effective_stockpile_doses",
                         "weighted_doses_per_course","secured_effective_daily_doses", "secured_variant_effective_daily_doses",
                         "effective_protected_daily_doses", "az_doses",
                         "residual_secured_daily_doses","effective_daily_residual",
                         "global_daily_residual","global_effective_daily_residual",
                         comp_dose_names,
                         comp_effective_names,
                         comp_protective_names,
                         comp_variant_effective_names,
                         comp_residual_effective_names)
  final_dt <- final_dt[, ..col_names_to_keep]

  # # Keep only the ones of interest
  # final_dt <- final_dt[, c("location_id","date","secured_daily_doses","daily_stockpile_doses","effective_stockpile_doses",
  #                          "weighted_doses_per_course","secured_effective_daily_doses", "secured_variant_effective_daily_doses",
  #                          "effective_protected_daily_doses", "az_doses",
  #                          "residual_secured_daily_doses","effective_daily_residual",
  #                          "global_daily_residual","global_effective_daily_residual")]

  ##### END CHANGED BELOW

  final_dt[, weighted_efficacy_location := secured_effective_daily_doses / secured_daily_doses]
  final_dt[, weighted_variant_efficacy_location := secured_variant_effective_daily_doses / secured_daily_doses]
  final_dt[, weighted_protected_location := effective_protected_daily_doses / secured_daily_doses]

  final_dt[, weighted_efficacy_residual := effective_daily_residual / residual_secured_daily_doses]
  final_dt[, weighted_efficacy_global := global_effective_daily_residual / global_daily_residual]

  final_dt <- merge(final_dt, hierarchy[,c("location_id","parent_id","location_name","level","most_detailed")], by = "location_id")

  # Add a column for "residual doses" where they are distributed to each location
  final_dt <- merge(final_dt, population, by = "location_id")
  global_pop <- sum(population$population)
  final_dt[, fraction_global_population := population / global_pop]

  final_dt[, global_daily_residual_location := fraction_global_population * global_daily_residual]
  final_dt[, global_effective_daily_residual_location := fraction_global_population * global_effective_daily_residual]

  ## Aggregate some locations
  # Aggregate Washington
  
  wa_loc_ids <- hierarchy[parent_id == 570, location_id]
  wa_aggs_date <- final_dt[location_id %in% wa_loc_ids, lapply(.SD, function(x) sum(x)), by = c("parent_id","date"),
                           .SDcols = c("secured_daily_doses","daily_stockpile_doses","effective_stockpile_doses","secured_effective_daily_doses",
                                       "residual_secured_daily_doses","effective_daily_residual", "secured_variant_effective_daily_doses",
                                       "effective_protected_daily_doses", "az_doses",
                                       "global_daily_residual","global_effective_daily_residual","global_daily_residual_location",
                                       "global_effective_daily_residual_location","population")]
  setnames(wa_aggs_date, "parent_id", "location_id")
  wa_aggs_date[, weighted_efficacy_location := secured_effective_daily_doses / secured_daily_doses]
  wa_aggs_date[, weighted_variant_efficacy_location := secured_variant_effective_daily_doses / secured_daily_doses]
  wa_aggs_date[, weighted_protected_location := effective_protected_daily_doses / secured_daily_doses]
  wa_aggs_date[, weighted_efficacy_residual := effective_daily_residual / residual_secured_daily_doses]
  wa_aggs_date[, weighted_efficacy_global := global_effective_daily_residual / global_daily_residual]

  wa_aggs_date <- merge(wa_aggs_date,
                        hierarchy[,c("location_id","location_name","parent_id","region_name","level","most_detailed")],
                        by = "location_id")

  final_dt <- rbind(final_dt[location_id != 570], wa_aggs_date, fill = T)

  # Aggregate Nationals
  
  parent_loc_ids <- hierarchy[level == 3 & most_detailed == 0, location_id]
  parent_aggs_date <- final_dt[parent_id %in% parent_loc_ids, lapply(.SD, function(x) sum(x)), by = c("parent_id","date"),
                               .SDcols = c("secured_daily_doses","daily_stockpile_doses","effective_stockpile_doses","secured_effective_daily_doses",
                                           "residual_secured_daily_doses","effective_daily_residual", "secured_variant_effective_daily_doses",
                                           "effective_protected_daily_doses", "az_doses",
                                           "global_daily_residual","global_effective_daily_residual","global_daily_residual_location",
                                           "global_effective_daily_residual_location","population")]
  setnames(parent_aggs_date, "parent_id", "location_id")
  parent_aggs_date[, weighted_efficacy_location := secured_effective_daily_doses / secured_daily_doses]
  parent_aggs_date[, weighted_variant_efficacy_location := secured_variant_effective_daily_doses / secured_daily_doses]
  parent_aggs_date[, weighted_protected_location := effective_protected_daily_doses / secured_daily_doses]
  parent_aggs_date[, weighted_efficacy_residual := effective_daily_residual / residual_secured_daily_doses]
  parent_aggs_date[, weighted_efficacy_global := global_effective_daily_residual / global_daily_residual]

  parent_aggs_date <- merge(parent_aggs_date,
                            hierarchy[,c("location_id","location_name","parent_id","region_name","level","most_detailed")],
                            by = "location_id")

  final_dt <- rbind(final_dt, parent_aggs_date, fill = T)

  # Should the stockpile doses be added?
  final_dt[, without_stockpile_daily_doses := secured_daily_doses]
  final_dt[, without_stockpile_effective_daily_doses := secured_effective_daily_doses]
  # Find pct that are AZ
  final_dt[, pct_az := az_doses / secured_daily_doses]
  final_dt[, secured_daily_doses := secured_daily_doses + daily_stockpile_doses]
  final_dt[, secured_effective_daily_doses := secured_effective_daily_doses + effective_stockpile_doses]
  final_dt[, weighted_efficacy_location := secured_effective_daily_doses / secured_daily_doses]
  final_dt[, proportion_protected := effective_protected_daily_doses / secured_daily_doses]

  #final_dt[, weighted_variant_efficacy_location := secured_variant_effective_daily_doses / secured_daily_doses]
  final_dt[, weight_ve_variant_fill :=
             head(weighted_variant_efficacy_location[is.finite(weighted_variant_efficacy_location)],1), by = "location_id"]
  final_dt[is.infinite(weighted_variant_efficacy_location), weighted_variant_efficacy_location := weight_ve_variant_fill]

  final_dt[, weight_protection_fill :=
             head(weighted_protected_location[is.finite(weighted_protected_location)],1), by = "location_id"]
  final_dt[is.infinite(weighted_protected_location), weighted_protected_location := weight_protection_fill]

  final_dt <- final_dt[order(location_id, date)]
  final_dt[, cumulative_doses := cumsum(secured_daily_doses), by = "location_id"]

  final_dt <- final_dt[date >= "2020-12-01"]

  ## Track AZ doses in the EU:
  # Set a pause in AZ for some locations (email from INDIVIDUAL_NAME, 3/15/2021)
  pause_az_names <- c("Estonia","Latvia","Lithuania","Luxembourg","France","Ireland","Bulgaria","Denmark","Norway",
                      "Netherlands","Democratic Republic of the Congo","Thailand","Romania","Iceland")
  pause_az_ids <- hierarchy[location_name %in% pause_az_names, location_id]
  pause_az_ids <- c(pause_az_ids, hierarchy[parent_id %in% c(81, 86), location_id])
  final_dt[location_id %in% pause_az_ids & date > "2021-03-15" & date < "2021-04-01", secured_daily_doses := secured_daily_doses * (1 - pct_az)]
  final_dt[location_id %in% pause_az_ids & date > "2021-03-15" & date < "2021-04-01", secured_effective_daily_doses := secured_effective_daily_doses * (1 - pct_az) * 0.74]

  # Remove AZ doses outside EU (will be used for high-risk efficacy)
  final_dt[, az_doses := ifelse(location_id %in% unique(eu_block$location_id), az_doses, NA)]

  ## Patch for J&J hold in US (4/13/2021) and in EU (4/19/2021) https://www.nytimes.com/live/2021/04/13/world/johnson-vaccine-blood-clots
  
  jj_hold_locs <- c(570, hierarchy[parent_id == 102, location_id], eu_block$location_id)
  # merge with population and proportion of US
  jj_dt <- purchase_candidates[location_id %in% jj_hold_locs
                               & company == "Janssen" & date == "2021-04-01",
                               c("location_id","location_secured_daily_doses","secured_effective_daily_doses")]
  jj_dt <- jj_dt[, lapply(.SD, function(x) sum(x)),
                 .SDcols = c("location_secured_daily_doses","secured_effective_daily_doses"),
                 by = "location_id"]
  setnames(jj_dt, c("location_secured_daily_doses","secured_effective_daily_doses"), c("jj_doses","effective_jj_doses"))
  final_dt <- merge(final_dt, jj_dt, by = "location_id", all.x = T)
  final_dt[location_id %in% jj_hold_locs & date >= "2021-04-13" & date < "2021-04-27",
           weighted_doses_per_course := 2]
  final_dt[location_id %in% jj_hold_locs & date >= "2021-04-13" & date < "2021-04-27",
           secured_daily_doses := secured_daily_doses - jj_doses]
  final_dt[location_id %in% jj_hold_locs & date >= "2021-04-13" & date < "2021-04-27",
           secured_effective_daily_doses := secured_effective_daily_doses - effective_jj_doses]
  final_dt[location_id %in% jj_hold_locs & date >= "2021-04-13" & date < "2021-04-27",
           weighted_efficacy_location := secured_effective_daily_doses / secured_daily_doses]

  final_dt[location_id %in% jj_hold_locs & date >= "2021-04-13" & date < "2021-04-27",
           weighted_variant_efficacy_location := 0.707]
  final_dt[location_id %in% jj_hold_locs & date >= "2021-04-13" & date < "2021-04-27",
           weighted_protected_location := 0.855]

  final_dt[, c("jj_doses","effective_jj_doses") := NULL]

  #-----------------------------------------------------------------------------
  # Assign zero doses to Turkmenistan per Steves request
  # Not sure where to do this

  #final_dt[location_name == 'Turkmenistan', secured_daily_doses := 0]
  #final_dt[location_name == 'Turkmenistan', daily_stockpile_doses := 0]
  #final_dt[location_name == 'Turkmenistan', effective_stockpile_doses := 0]
  #final_dt[location_name == 'Turkmenistan', secured_effective_daily_doses := 0]
  #final_dt[location_name == 'Turkmenistan', secured_variant_effective_daily_doses := 0]
  #final_dt[location_name == 'Turkmenistan', effective_protected_daily_doses := 0]
  #final_dt[location_name == 'Turkmenistan', residual_secured_daily_doses := 0]
  #final_dt[location_name == 'Turkmenistan', effective_daily_residual := 0]
  #final_dt[location_name == 'Turkmenistan', global_daily_residual_location := 0]
  #final_dt[location_name == 'Turkmenistan', global_effective_daily_residual_location := 0]
  #final_dt[location_name == 'Turkmenistan', without_stockpile_daily_doses := 0]
  #final_dt[location_name == 'Turkmenistan', global_effective_daily_residual_location := 0]

  # Are there any duplicate dates?
  dedup <- sum(duplicated(unique(final_dt[ , c("location_id","date")])))

  if (sum(dedup > 0)) {
    message("There are duplicate date/location_ids somewhere in the dataset!")
    final_dt[dedup,]
    stop()
  }

  # Sweep through columns and clean up negatives, infinites, and NaNs
  final_dt <- .put_columns_in_bounds(final_dt)

  ##-------------------------------------------------------------------------
  message(paste('Writing available doses to file:', file.path(vaccine_output_root, 'final_doses_by_location_brand.csv')))

  ##### CHANGED BELOW
  col_names_to_keep_2 <- c(
    "location_id", "date", "secured_daily_doses", "secured_effective_daily_doses", "az_doses",
    "weighted_efficacy_location", "weighted_variant_efficacy_location", "weighted_protected_location",
    "cumulative_doses", "parent_id", "location_name", "global_daily_residual_location",
    "global_effective_daily_residual_location", "weighted_doses_per_course",
    comp_dose_names,
    comp_effective_names,
    comp_protective_names,
    comp_variant_effective_names,
    comp_residual_effective_names
  )

  final_dt <- final_dt[, ..col_names_to_keep_2]

  # final_dt <- final_dt[, c(
  #   "location_id", "date", "secured_daily_doses", "secured_effective_daily_doses", "az_doses",
  #   "weighted_efficacy_location", "weighted_variant_efficacy_location", "weighted_protected_location",
  #   "cumulative_doses", "parent_id", "location_name", "global_daily_residual_location",
  #   "global_effective_daily_residual_location", "weighted_doses_per_course"
  # )]
  ##### END CHANGED BELOW

  vaccine_data$write_final_doses(final_dt, vaccine_output_root, filename='final_doses_by_location_brand.csv')

}

submit_supply_plot <- function(vaccine_output_root) {
  ##-------------------------------------------------------------------------
  ## Submit job for diagnostic plots ##
  
  .submit_job(
    script_path = file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "plot_supply.R"),
    job_name = paste0("vaccine_supply_plots"),
    mem = "20G",
    archiveTF = TRUE,
    threads = "6",
    runtime = "40",
    Partition = "d.q",
    Account = "proj_covid",
    args_list = list("--version " = vaccine_output_root)
  )

}
