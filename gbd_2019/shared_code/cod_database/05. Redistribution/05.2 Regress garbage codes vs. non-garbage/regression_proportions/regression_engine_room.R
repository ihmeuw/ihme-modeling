# Regression engine -------------------------------------------------------

#' Combines fixed effect Beta for prop garbage term and 
#' random slope betas for prop garbage by country, region, sex
check_slope_significance <- function(model, predictions) {
  predictions <- copy(predictions)
  # overall_slope_beta <- fixef(model)[grep("prop_garbage", names(fixef(model)), value = T)][[1]] # [[ at end drops name
  
  # tack on random slope betas (for prop garbage) to predictions
  for (level in names(ranef(model))) {
    dt <- as.data.table(ranef(model)[[level]], keep.rownames = level)[, -"(Intercept)"]
    if (level == "country") dt[, country := as.integer(country)] # country col is actually location_id, other 2 are names
    
    # rename prop_grabage col something more descriptive and concise: rslope_beta_{level}
    dt <- setnames(dt, names(dt), c(names(dt)[1], paste0("rslope_beta_", gsub("_.*$", "", names(dt)[1]))))
    predictions <- merge(predictions, dt, by = level, all.x = T)
  }
  
  # add all slope betas together
  # predictions[, total_slope_beta := mapply(sum, overall_slope_beta, rslope_beta_country, rslope_beta_region, rslope_beta_super, na.rm = T)]
  predictions[, total_rslope_beta := mapply(sum, rslope_beta_country, rslope_beta_region, rslope_beta_super, na.rm = T)]
  
  # pull together standard error for fixed effects and random slopes
  # overall_slope_std_dev <- summary(model)$coefficients[grep("prop_garbage", names(fixef(model)), value = T), "Std. Error"]
  random_slope_std_devs <- as.data.table(VarCorr(model))
  random_slope_std_devs <- random_slope_std_devs[grepl("prop_garbage", var1)]
  
  # overall_var <- overall_slope_std_dev ^ 2
  country_var <- random_slope_std_devs[grp == "country", vcov]
  region_var <- random_slope_std_devs[grp == "region_name", vcov]
  super_var <- random_slope_std_devs[grp == "super_region_name", vcov]
  
  # reliance on (semi) global variables is tight, yo^^
  compute_std_dev <- function(rslope_beta_country, rslope_beta_region, rslope_beta_super) {
    
    # allow of locs that DONT have all these (id regions dont have country variance)
    if (is.na(rslope_beta_country)) country_var <- NA
    if (is.na(rslope_beta_region)) region_var <- NA
    if (is.na(rslope_beta_super)) super_var <- NA
    
    return(sqrt(sum(country_var, region_var, super_var, na.rm = T)))
  }
  
  predictions[, total_rslope_std_dev := mapply(compute_std_dev, rslope_beta_country, rslope_beta_region, rslope_beta_super)]
  
  # compute upper and lowers
  predictions[, slope_lower := total_rslope_beta - 1.96 * total_rslope_std_dev]
  predictions[, slope_upper := total_rslope_beta + 1.96 * total_rslope_std_dev]
  predictions[, is_significant := ifelse(slope_upper < 0, 1, 0)]
  
  # first, need to give levels depending on location
  predictions <- merge(predictions, loc_data[, c("location_id", "level")], by.x = "country", by.y = "location_id")
  
  # WARN if we have non-significance at a global level
  # if (overall_slope_upper >= 0) {
  #   warning(paste0("Global estimates are NON-significant. Setting all proportions to 0 for ", predictions$model_group[1]))
  #   predictions[, prop_target := 0]
  #   return(predictions)
  # }
  
  # set all locations where we DONT have data to non-significant regardless of technical total slope beta
  # doing this because we want to trust region/super/global aggregates more in these cases
  predictions[level == 3 & is.na(rslope_beta_country), `:=`(is_significant = 0, no_data = 1)]
  predictions[level == 2 & is.na(rslope_beta_region), `:=`(is_significant = 0, no_data = 1)]
  predictions[level == 1 & is.na(rslope_beta_super), `:=`(is_significant = 0, no_data = 1)]
  

  # Go from most detailed location type (country) up, marking prop_target for replacement for LOCS WITH NO DATA with the prop_target
  # value of the location one level below. Countries -> regions -> super regions -> global
  # predictions[, replaced_by := NA_character_]
  level_id <- 2
  for (location_level in c("region", "super_region", "global")) {
    predictions_map <- predictions[level == level_id, c(location_level, "age", "sex_id", "year_window", "prop_target", "is_significant", "no_data", "level"), with = F]
    predictions_map <- setnames(predictions_map, c("prop_target", "is_significant", "no_data"), c("prop_target_replace", "is_significant_replace", "no_data_replace"))
    predictions <- merge(predictions, predictions_map[, -"level"], by = c(location_level, "age", "sex_id", "year_window"), all.x = T)
    
    # replace if no data for that loc at that level
    predictions[level == 3 & no_data == 1, replaced_by := location_level]
    # predictions[level == 3 & no_data == 1, prop_target := prop_target_replace]
    predictions[level == 3 & no_data == 1, is_significant := is_significant_replace]
    predictions[level == 3 & no_data == 1, no_data := no_data_replace]
    predictions[, `:=`(prop_target_replace = NULL, is_significant_replace = NULL, no_data_replace = NULL)]
    
    # handle non-significant global props replacing country/region/super props
    if (location_level == "global" && nrow(predictions[level == 0 & !is_significant]) > 0) {
      predictions[level == 3 & no_data == 1 & !is_significant, replaced_by :=  "global (non-sig)"]
    }
    
    level_id <- level_id - 1
  }
  
  
  return(predictions)
}


regression_engine <- function(model_formula, input_dt, square_dt, random_effects=FALSE) {
  
  model_predictions <- data.table()
  models <- list()
  model_summaries <- list()
  model_groups <- unique(input_dt$model_group)
  
  
  # STEP 1: run a regression on the input data restricted by stars, max garbage (for non-diabetes)
  # OR testing treating diabetes differently, going back to original data restrictions
  if (launch_set$shared_package_id == 2614) {
    MAX_PROP_GARBAGE <- 0.50
    MAX_PROP_CODED_TYPE_1 <- 0.30
    
    country_years <- input_dt[age_group_id >= 15, list(deaths_garbage = sum(deaths_garbage), deaths_target = sum(deaths_target)), by = c("country", "year_id", "cause_id", "sex_id")]
    country_years <- data.table::dcast(country_years, country + year_id + sex_id + deaths_garbage ~
                                         cause_id, value.var = "deaths_target", fun.aggregate = mean)
    setnames(country_years, c("975", "976"), c("deaths_type1", "deaths_type2"))
    
    country_years[, prop_garbage := deaths_garbage / (deaths_garbage + deaths_type1 + deaths_type2)]
    country_years[, prop_type1_of_coded := deaths_type1 / (deaths_type1 + deaths_type2)]
    
    # grab where we have less than 50% garbage and of deaths directly coded, less than 30% go to type 1
    country_years <- country_years[prop_garbage < MAX_PROP_GARBAGE & prop_type1_of_coded < MAX_PROP_CODED_TYPE_1]
    country_years[, valid_country_year_sex := paste(country, year_id, sex_id, sep = "_")]
    valid_country_year_sex <- country_years$valid_country_year_sex
    
    step_1_input_dt <- input_dt[paste(country, year_id, sex_id, sep = "_") %in% valid_country_year_sex]
  } else {
    step_1_input_dt <- limit_stars(input_dt, stars_lower_limit = LOWER_LIMIT_STARS)
    step_1_input_dt <- step_1_input_dt[prop_garbage < MAX_PROP_GARBAGE]
  }
  
  # STEP 1.5: THIS IS A HACK
  if (launch_set$shared_package_id == 9) {
    input_dt <- limit_stars(input_dt, stars_lower_limit = 3)
  }
  
  for (run_model_group in model_groups) {
    run_model_formula <- model_formula
    
    # in case we filtered to only 1 age group due to cause restrictions,
    # drop age dummies. mad this isnt done by lmer automatically
    if (length(unique(input_dt[model_group == run_model_group, age])) == 1) {
      run_model_formula <- gsub("\\+ ?age", "", run_model_formula)
    }
    
    cat_ln(paste0("Model group: ", run_model_group, " - ", run_model_formula))
    
    if (random_effects) {
      model <- lmer(run_model_formula, data = step_1_input_dt[model_group == run_model_group, ]
      )
    } else {
      model <- lm(run_model_formula, data = step_1_input_dt[model_group == run_model_group, ]
      )
    }
    models[[run_model_group]] <- model
    model_summaries[[run_model_group]] <- list(summary(model), anova(model))
    
    # STEP 2: Return to input dt with ALL data (not restricted by stars, garbage) and predict everywhere.
    # Compute residuals
    target_col_name <- gsub(" *~.*$", "", model_formula)
    
    input_dt[model_group == run_model_group, pred := predict(model, newdata=input_dt[model_group == run_model_group], allow.new.levels=TRUE)]
    input_dt[model_group == run_model_group, resid := get(target_col_name) - pred]
    
    # STEP 3: Predict for all locations using restricted model
    pred_df <- square_dt[model_group == run_model_group, ]
    pred_df[, logit_prop_target := predict(model, newdata=pred_df, allow.new.levels=TRUE)]
    
    # STEP 4: run a lm on residuals vs logit proportion garbage by age to adjust for proportion garbage
    for (age_group in unique(input_dt[model_group == run_model_group, age])) {
      resid_mod <- lm(resid ~ logit_prop_garbage, data = input_dt[model_group == run_model_group & age == age_group])
      pred_df[age == age_group, beta_resid := resid_mod$coefficients[["logit_prop_garbage"]]]
    }
    
    # STEP 5: apply adjustment on redistribution estimates
    # FORMULA: y @ GC = 0 for age - y @ GC for given country/year/sex/age to get the CHANGE in the residuals in a world with 0 garbage
    # aka y intercept (adjusting for being in logit space) - actual y given a %GC
    # BETA (resid mod, for age X) * LOGIT(0 garbage) - BETA (resid mod, for age X) * LOGIT(% GC in that loc/year/sex/age)
    pred_df[, logit_garbage_adjustment := beta_resid * (logit(LOWER_OFFSET) - logit_prop_garbage)]
    pred_df[, logit_prop_target := logit_prop_target + logit_garbage_adjustment]
    
    # COMMENTING OUT FOR NOW
    # if (get_year_setting(model_formula) == "year_window") {
    #   pred_df <- unique(pred_df[, -c("prop_garbage", "logit_prop_garbage", "year_id")])
    # }
    
    pred_df[, prop_target := invlogit(logit_prop_target)]
    pred_df[, formula := run_model_formula]
    
    # FOR NOW: only apply significance check for cancer
    if (!launch_set$shared_package_id %in% c(15, 2614)) {
      pred_df <- check_slope_significance(model, pred_df) 
    }
    
    if (any(is.na(pred_df$prop_target))) {
      stop(paste0(sum(is.na(pred_df$prop_target))), " NAs in pred_df for ", run_model_group)
    }
    
    model_predictions <- rbind(model_predictions, pred_df)
  }
  
  if (get_year_setting(model_formula) == "year_window") {
    # take an average across all years within the year window
    model_predictions[, prop_target := mean(prop_target), by = c("country", "year_window", "age", "sex_id", "cause_id")]
  }
  
  model_predictions[, prop_target_scaled := prop_target / sum(prop_target), by = c("country", "year_id", "age", "sex_id")]
  model_predictions[, location_id := country]
  model_predictions[, level := 3]
  
  return(list("model_predictions" = model_predictions, "model_summaries" = model_summaries, "models" = models))
}


# Regression wrapper and post-processing ----------------------------------

# Helper functions to read in all-cause mortality and collapse by our larger age groups
prep_all_cause_mortality <- function(age_map, formula = launch_set$formula, decomp_step = DECOMP_STEP, download_new = FALSE) {
  base_path <- paste0(REGDIR, "FILEPATH")
  age_groups_ids <- c(2:20, 30:32, 235)
  sex_ids <- 1:2
  
  if (download_new) {
    all_cause_mort <- get_envelope(age_group = age_groups_ids, location_id = "all", year_id = "all", sex_id = sex_ids,
                                   gbd_round_id = 6, decomp_step = DECOMP_STEP)
    readr::write_csv(all_cause_mort, paste0(base_path, "all_cause_mortality_", gsub("-", "_", Sys.Date()), ".csv"))
  } else { # read in most recent version from /flat_inputs/
    mort_version <- sort(list.files(paste0(REGDIR, "ICD_cod/flat_inputs/"), pattern = "all_cause_mortality"), decreasing = TRUE)[1]
    warning(paste("Reading in all-cause mortality flat file:", mort_version))
    
    all_cause_mort <- fread(paste0(base_path, mort_version))
  }
  
  # clean up, clean up, everybody do your share :music_note:
  # merge on ages and collapse on small age_group_ids
  all_cause_mort <- all_cause_mort[year_id >= 1980]
  all_cause_mort <- merge(all_cause_mort, age_map, by = "age_group_id")
  all_cause_mort <- all_cause_mort[, list(all_deaths = sum(mean)), by = c("location_id", "age", "sex_id", "year_id")]
  
  # merge on region, super, global
  locs <- loc_data[, c("location_id", "region_id", "super_region_id", "level")]
  setnames(locs, c("region_id", "super_region_id"), c("region", "super_region"))
  locs[, global := 1]
  
  all_cause_mort <- merge(all_cause_mort, locs, by = "location_id", all.x = TRUE)
  all_cause_mort <- all_cause_mort[level == 3]
  all_cause_mort$level = NULL
  
  # create region, super region, and global totals
  base_cols <- c("location_id", "region", "super_region", "global", "year_id", "age", "sex_id")
  
  
  # need to deal with year-specificity as it changes how aggregation works
  prediction <- gsub(" ?~.*", "", formula)
  if (grepl("all_years", prediction)) { # not year specific
    # collapse on year
    base_cols <- base_cols[!base_cols %in% "year_id"]
    all_cause_mort <- all_cause_mort[, list(all_deaths = sum(all_deaths)), by = base_cols]
  } else if (grepl("year_window", prediction)) {
    # create and collapse on time window
    base_cols <- base_cols[!base_cols %in% "year_id"]
    base_cols <- c(base_cols, "year_window")
    
    time_window_cut <- 2004
    start <- min(all_cause_mort$year_id)
    end <- 2017 # unfortunate hack
    warning("Assuming year_window ends at 2017")
    
    all_cause_mort[, year_window := ifelse(year_id <= time_window_cut, 
                                           paste0(start, " - ", time_window_cut),
                                           paste0(time_window_cut + 1, " - ", end))]
    all_cause_mort <- all_cause_mort[, list(all_deaths = sum(all_deaths)), by = base_cols]
    
  } # else IS year-specific and we can move forward w/o special case
  
  region_cols <- base_cols[!base_cols %in% "location_id"]
  super_cols <- region_cols[!region_cols %in% "region"]
  global_cols <- super_cols[!super_cols %in% "super_region"]
  
  region_deaths <- all_cause_mort[, list(all_deaths_region = sum(all_deaths)), by = region_cols]
  super_deaths <- all_cause_mort[, list(all_deaths_super = sum(all_deaths)), by = super_cols]
  global_deaths <- all_cause_mort[, list(all_deaths_global = sum(all_deaths)), by = global_cols]
  
  all_cause_mort <- merge(all_cause_mort, region_deaths, by = region_cols)
  all_cause_mort <- merge(all_cause_mort, super_deaths, by = super_cols)
  all_cause_mort <- merge(all_cause_mort, global_deaths, by = global_cols)
  
  return(all_cause_mort)
}


regression_results <- function(outdir, launch_set, input_dt, square_dt, forced_predictions,
                               random_effects = FALSE, write = FALSE) {
  results <- regression_engine(launch_set$formula, input_dt, square_dt, random_effects=random_effects)
  
  master_predictions <- list()
  
  models <- results$models
  master_summaries <- results$model_summaries
  master_predictions <- rbind(master_predictions, results$model_predictions)
  
  if (launch_set$shared_package_id %in% c(4, 9, 2614)) {
    master_predictions <- rbind(master_predictions, forced_predictions, fill=TRUE)
  }
  
  ### begin region/super region aggregation. SUBJECT TO CHANGE
  #' `note`: as of 2/15/19 we are switching to weighting by ALL CAUSE MORTALITY
  #' so aggregate_prop_garbage() does more than we want
  input_deaths <- aggregate_prop_garbage(paste0(indir, "/data/_input_data_id_", launch_set$data_id, ".csv"))
  
  # NEED fresh input_dt bc initial one is altered
  input_dt <- read_input_dt(paste0(indir, "/data/_input_data_id_", launch_set$data_id, ".csv"))
  all_cause_mort <- prep_all_cause_mortality(age_map = unique(input_dt[, c("age", "age_group_id")]), decomp_step = DECOMP_STEP)
  master_predictions <- merge(master_predictions, all_cause_mort, by = c("location_id", "region", "super_region", "global",
                                                                         "age", "sex_id", get_year_setting(launch_set$formula)), all.x = T)
  
  master_predictions[, region_weight := all_deaths / all_deaths_region]
  master_predictions[, super_weight := all_deaths / all_deaths_super]
  master_predictions[, global_weight := all_deaths / all_deaths_global]
  
  # REG EX: extract column name for prop garbage based on what it is in formula
  # this grabs first word after ~, drops all else
  year_col_name <- get_year_setting(launch_set$formula)
  
  base_cols <- c("country", "region", "super_region", "global", year_col_name, "age", "sex_id", "cause_id")
  input_deaths <- input_deaths[, c(base_cols, "deaths_target", "deaths_garbage"), with = F]
  
  region_cols <- base_cols[!base_cols %in% "country"]
  super_cols <- region_cols[!region_cols %in% "region"]
  global_cols <- super_cols[!super_cols %in% "super_region"]
  
  input_deaths[, cause_id := as.character(cause_id)]
  master_predictions[, cause_id := as.character(cause_id)]
  master_predictions <- merge(master_predictions, input_deaths, all.x = T, by = base_cols)
  
  # calculate reg/super props
  master_predictions[, prop_target_region_weighted := prop_target_scaled * region_weight]
  master_predictions[, prop_target_super_weighted := prop_target_scaled * super_weight]
  master_predictions[, prop_target_global_weighted := prop_target_scaled * global_weight]
  
  regs <- master_predictions[, list(prop_target = sum(prop_target_region_weighted)), by = region_cols]
  super <- master_predictions[, list(prop_target = sum(prop_target_super_weighted)), by = super_cols]
  global <- master_predictions[, list(prop_target = sum(prop_target_global_weighted)), by = global_cols]
  
  # corrects for too high proportions if using year windows or all-years
  regs[, prop_target_scaled := prop_target / sum(prop_target), by = eval(region_cols[!region_cols %in% "cause_id"])]
  regs <- regs[is.nan(prop_target_scaled), prop_target_scaled := NA]
  regs[, `:=`(location_id = region, level = 2, level_name = "Region")]
  
  super[, prop_target_scaled := prop_target / sum(prop_target), by = eval(super_cols[!super_cols %in% "cause_id"])]
  super <- super[is.nan(prop_target_scaled), prop_target_scaled := NA]
  super[, `:=`(location_id = super_region, level = 1, level_name = "Super region")]
  
  global[, prop_target_scaled := prop_target / sum(prop_target), by = eval(global_cols[!global_cols %in% "cause_id"])]
  global <- global[is.nan(prop_target_scaled), prop_target_scaled := NA]
  global[, `:=`(location_id = global, level = 0, level_name = "Global")]
  
  # right now aggregated proportions are scaled to 1 for year_windows, NOT year_ids
  # like the country-level results. Fixing this here
  if (year_col_name == "year_window") {
    regs[, year_id := ifelse(year_window == "1980 - 2004", 2000, 2010)]
    super[, year_id := ifelse(year_window == "1980 - 2004", 2000, 2010)]
    global[, year_id := ifelse(year_window == "1980 - 2004", 2000, 2010)]
  }
  
  master_predictions <- rbind(master_predictions, regs, fill = T)
  master_predictions <- rbind(master_predictions, super, fill = T)
  master_predictions <- rbind(master_predictions, global, fill = T)
  master_predictions[, `:=`(region_weight = NULL, prop_target_region_weighted = NULL,
                            super_weight = NULL, prop_target_super_weighted = NULL,
                            global_weight = NULL, prop_target_global_weighted = NULL,
                            all_deaths = NULL, all_deaths_region = NULL,
                            all_deaths_super = NULL, all_deaths_global = NULL)]
  
  # For predictions marked to be replaced, replace w/ whatever it says (region, super, global, global non-sig?)
  if ("replaced_by" %in% names(master_predictions)) {
    level_id <- 2
    for (location_level in c("region", "super_region", "global")) {
      predictions_map <- master_predictions[level == level_id, c(location_level, "age", "sex_id", get_year_setting(launch_set$formula), "cause_id","prop_target_scaled", "level"), with = F]
      predictions_map <- setnames(predictions_map, c("prop_target_scaled"), c("prop_target_replace"))
      master_predictions <- merge(master_predictions, predictions_map[, -"level"], by = c(location_level, "age", "sex_id", "cause_id", get_year_setting(launch_set$formula)), all.x = T)
      
      # replace if no data for that loc at that level
      master_predictions[level == 3 & replaced_by == location_level, prop_target_scaled := prop_target_replace]
      master_predictions[, `:=`(prop_target_replace = NULL)]
      
      level_id <- level_id - 1
    }
    
    # THIS IS VERY IMPORTANT:
    # while I haven't figured out exactly why, as of 3/19/19 replacing country props w/ reg/super/global NEEDS to be rescaled after
    # somewhere my assumptions are false but I haven't figured out exactly where. This should do the trick tho
    master_predictions[level == 3, prop_target_scaled := prop_target_scaled / sum(prop_target_scaled), by = c("country", "year_id", "age", "sex_id")]
  }
  
  
  ###
  
  print(master_summaries)
  
  if (write) {
    writing <- pretty_print(master_predictions)
    readr::write_csv(writing, path = paste0(outdir, "/_master_predictions.csv"))
    saveRDS(models, file = paste0(indir, "/", launch_set$regression_launch_set_id, "/model_objects.RDS"))
    
    coefs <- data.frame()
    for (model_group in names(models)) {
      model_coefs <- as.data.frame(coef(summary(models[[model_group]])))
      model_coefs$parameter <- row.names(model_coefs)
      model_coefs$model_group <- model_group
      
      coefs <- rbind(coefs, model_coefs)
    }
    map <- fread(paste0(REGDIR, "/ICD_cod/model_groups_map.csv"))
    coefs  <- merge(coefs, map, by = "model_group")
    if (has_random_effects(launch_set$formula)) {
      # ARTIFACT FROM USING BASE LMER
      col_order <- c("model_group", "description", "parameter", "Estimate", "Std. Error", "t value", "Pr(>|t|)")
    } else {
      col_order <- c("model_group", "description", "parameter", "Estimate", "Std. Error", "t value", "Pr(>|t|)")
    }
    
    readr::write_csv(coefs[, col_order], path = paste0(outdir, "/_coefficient_table.csv"))
    
    plot_redististribution_props_for(regression_launch_set_id = launch_set$regression_launch_set_id)
    plot_reg_fits_for(regression_launch_set_id = launch_set$regression_launch_set_id)
    plot_residuals_for(regression_launch_set_id = launch_set$regression_launch_set_id)
    plot_time_trend_for(regression_launch_set_id = launch_set$regression_launch_set_id)
    plot_garbage_relation_for(regression_launch_set_id = launch_set$regression_launch_set_id)
  }
}


pretty_print <- function(dt) {
  dt <- as.data.table(dt)
  
  # add formula if missing
  dt[, formula := launch_set$formula]
  
  # Tack on location, region and super region name if not already there
  if (!"location_name" %in% names(dt) || any(is.na(dt$location_name))) {
    dt[, `:=`(location_name = NULL, region_name = NULL, super_region_name = NULL, level = NULL)]
    locs <- loc_data[, c("location_id", "location_name", "region_name", "super_region_name", "level")]
    dt <- merge(dt, locs, by = "location_id", all.x = T)
    
    # fix issue where region and super region name are the same for NAME
    dt[region_name == "North Africa and Middle East" & level == 2, location_name := "North Africa and Middle East (region)"]
    dt[region_name == "South Asia" & level == 2, location_name := "South Asia (region)"]
  }
  
  # Add on sex names
  dt[, sex := ifelse(sex_id == 1, "Male", "Female")]
  
  # Add on full age ranges
  grab_age_range <- function(age_ids) {
    # assume age_ids are input as ordered, comma-separated string
    start <- gsub(",.*$", "", age_ids)
    end <- gsub("^.*, ", "", age_ids)
    
    if (!exists("ages")) {
      source_functions(get_ids = T)
      ages <<- get_ids("age_group")
    }
    
    start <- ages[age_group_id == start, age_group_name]
    end <- ages[age_group_id == end, age_group_name]
    
    # convert these age groups into single age values based on start/end
    if (start == "Early Neonatal") start <- "0"
    else start <- gsub(" .*$", "", start) # grab first age (ex 10 in '10 to 14')
    
    if (end == "95 plus") end <- "+"
    else end <- gsub("^.* ", " - ", end) # grab 2nd age (ex 14)
    
    return(paste0(start, end))
  }
  
  input_dt <- read_input_dt(paste0(indir, "/data/_input_data_id_", launch_set$data_id, ".csv"))
  ids_in_ages <- input_dt[, .(age_group_ids = paste(sort(unique(age_group_id)), collapse = ", ")), by = c("age")]
  ids_in_ages[, age_full := mapply(grab_age_range, age_group_ids)]
  dt <- merge(dt, ids_in_ages, by = "age")
  
  # Map model_groups -> descriptive names
  dt[is.na(model_group), model_group := paste(cause_id, sex_id, sep = "_")]
  map <- fread(paste0(REGDIR, "/ICD_cod/model_groups_map.csv"))
  
  # if the model group does NOT already exist in the map we have 
  # (ie we're dealing wiht a new cause) then
  # add it and save changes
  if (length(setdiff(dt$model_group, map$model_group)) > 0) {
    source_functions(get_cause_metadata = T)
    cause_data <- get_cause_metadata(cause_set_id = 4) # 4 == gbd estimation, for GC redist
    cause_data <- cause_data[cause_id %in% unique(dt$cause_id), c("cause_id", "cause_name")]
    
    # cross the cause data with both sexes so we can match up cause-sex specific model groups
    to_add <- cross(cause_data, data.table(sex_id = c(1, 2)))
    to_add[, model_group := paste(cause_id, sex_id, sep = "_")]
    to_add[, description := paste(cause_name, ifelse(sex_id == 1, "Male", "Female"), sep = ", ")]
    to_add <- to_add[, c("model_group", "cause_name", "description")]
    
    map <- rbind(map, to_add)
    map <- unique(map, by = "model_group") # in case any new model groups overlap with existing
    readr::write_csv(map, paste0(REGDIR, "ICD_cod/model_groups_map.csv"))
  }
  
  dt <- merge(dt, map, by = "model_group", all.x = TRUE)
  
  if (nrow(dt) == 0) {
    warning(paste0("No prediction rows exist. Check that the model groups for shared package ", 
                   launch_set$shared_package_name," have been added to model_groups_map.csv"))
  }
  
  # deal with missing years for aggregate (region, super) estimates 
  # if using year window, all years. Also drop duplicate data
  prediction <- gsub(" ?~.*", "", launch_set$formula)
  if (grepl("all_years", prediction)) { # not year specific
    dt[is.na(year_id), year_id := 2010] # for easy viewing
    dt <- dt[year_id == 2010]
  } else if (grepl("year_window", prediction)) {
    dt[is.na(year_id), year_id := ifelse(year_window == "1980 - 2004", 2000, 2010)]
    dt <- dt[year_id %in% c(2000, 2010)]
  }
  
  return(dt)
}

