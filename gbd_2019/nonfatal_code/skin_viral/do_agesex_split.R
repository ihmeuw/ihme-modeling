#'####################################`INTRO`##########################################
#' @purpose: Age and sex splitting and crosswalk 
#'
#'####################################`INTRO`##########################################

library("ihme", lib.loc = "FILEPATH")
setup()

library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')
library('magrittr')

ihme::source_functions(get_model_results = T, get_demographics = T, get_location_metadata = T,
                       get_ids = T, get_population = T, interpolate = T, get_draws = T, get_cause_metadata = T)


# source custom functions
source(paste0("FILEPATH", "/functions_agesex_split.R"))


BASE_DIR <- "FILEPATH"
BUNDLES <- as.data.table(openxlsx::read.xlsx("FILEPATH"))

# Functions ---------------------------------------------------------------


do_agesex_split <- function(bun_id, me_id, measure_name, bun_path, save_dir, dt = NULL) {
  cat(paste0(Sys.time(), " Applying age-sex splitting for bundle id ", bun_id, " (", measure_name, ")\n"))
  age_map <- fread("FILEPATH")
  
  dir.create("FILEPATH", showWarnings = F, recursive = T)
  dir.create("FILEPATH", showWarnings = F, recursive = T)
  
  # Load and prep bundle data -----------------------------------------------
  if (!is.null(dt)) {
    cat(paste0("Using data passed in via 'dt' parameter\n"))
    original_data <- copy(dt)
  } else {
    original_data <- fread(bun_path)
  }
  
  bun_data <- pull_bundle_data(measure_name = measure_name, bun_id = bun_id, bun_data = original_data)
  
  # subset data into an aggregate dataset and a fully-specified dataset
  data <- divide_data(input_data = bun_data)
  good_data <- data[need_split == 0]
  aggregate <- fsetdiff(data, good_data, all = TRUE)
  
  if (nrow(aggregate) == 0) { 
    cat(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split\n"))
    write.csv(original_data, 
              file = "FILEPATH",
              row.names = FALSE)
    return(NULL)
  }
  
  # expand the aggregate dataset into its constituent age and sexes
  expanded <- expand_test_data(agg.test = aggregate)
  
  # merge populations and age group ids onto the expanded dataset
  if ("age_group_id" %in% names(expanded) == TRUE) {
    expanded$age_group_id <- NULL
  }
  
  cat(paste0(Sys.time()), "Loading populations... ")
  expanded <- add_pops(expanded, age_map = age_map)
  cat("DONE\n")
  
  if (nrow(expanded[is.na(population)]) > 0) {
    stop(paste0("Population merge failed: ", nrow(expanded[is.na(population)]), " rows have NA population."))
  }
  
  
  # label each row with the closest dismod estimation year for matching to dismod model results
  expanded[, est_year_id := year_id - year_id %% 5]
  expanded[est_year_id < 1990, est_year_id := 1990]
  expanded[year_id == 2017, est_year_id := 2017]
  
  #' Pull draw data for each age-sex-yr for every location in the current aggregated test data 
  #' needed to be split.
  cat(paste0(Sys.time()), "Pulling DisMod results... ")
  weight_draws <- pull_model_weights(me_id, measure_name, expanded = expanded)
  cat("DONE\n")
  
  #' Append draws to the aggregated dataset
  cat(paste0(Sys.time()), "Applying age and sex splits\n")
  draws <- merge(expanded, weight_draws, by = c("age_group_id", "sex_id", "location_id", "est_year_id"), 
                 all.x=TRUE)
  
  #' Take all the columns labeled "draw" and melt into one column, row from expanded now has 1000 
  #' rows with same data with a unique draw. Draw ID for the equation
  draws <- melt.data.table(draws, id.vars = names(draws)[!grepl("draw", names(draws))], measure.vars = patterns("draw"),
                           variable.name = "draw.id", value.name = "model.result")
  
  #' Removed sampling from the original data point because too unstable, and not sure that it's necessary
  
  
  # Apply age-sex splitting -------------------------------------------------
  #' This is the numerator of the equation, the total number of cases for a specific age-sex-loc-yr
  #' based on the modeled prevalence
  draws[, numerator := model.result * population]
  
  #' This is the denominator, the sum of all the numerators by both draw and split ID. The number of cases in the aggregated age/sex 
  #' group.
  #' The number of terms to be summed should be equal to the number of age/sex groups present in the original aggregated data point
  draws[, denominator := sum(numerator), by = .(split.id, draw.id)]
  
  #' Calculate the actual estimate of the split point from the input data (mean) and a unique draw from the modelled 
  #' prevalence (model.result)
  draws[, estimate := mean * model.result / denominator * pop.sum]
  draws[, sample_size_new := sample_size * population / pop.sum]
  
  # If the numerator and denominator is zero, set the estimate to zero
  draws[numerator == 0 & denominator == 0, estimate := 0]
  
  # Collapsing the draws by the expansion ID, by taking the mean, SD, and quantiles of the 1000 draws of each calculated split point
  #' Each expand ID has 1000 draws associated with it, so just take summary statistics by expand ID
  final <- draws[, .(mean.est = mean(estimate),
                     sd.est = sd(estimate),
                     upr.est = quantile(estimate, .975),
                     lwr.est = quantile(estimate, .025),
                     sample_size_new = unique(sample_size_new),
                     cases.est = mean(numerator),
                     agg.cases = mean(denominator)), by = expand.id] %>% merge(expanded, by = "expand.id")
  final[, se.est := sd.est]
  final[, agg.sample.size := sample_size]
  final[, sample_size := sample_size_new]
  final[,sample_size_new:=NULL]
  
  #' Set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
  final[mean==0, mean.est := 0]
  final[, case_weight := cases.est / agg.cases]
  final$agg.cases <- NULL
  
  setnames(final, c("mean", "standard_error", "cases"), c("agg.mean", "agg.std.error", "agg.cases"))
  setnames(final, c("mean.est", "se.est"), c("mean", "standard_error"))
  setnames(final, 'seq','parent_seq')
  final[, seq := integer()] 
  
  # Save off just the split data for troubleshooting/diagnostics ------------
  split_data <- final[, c('nid','parent_seq','age_start','age_end','sex_id','mean',
                          'standard_error','case_weight','sample_size',
                          'agg_age_start','agg_age_end','agg_sex_id', 'agg.mean',
                          'agg.std.error','agg.cases','agg.sample.size',
                          'population','pop.sum',
                          'age_group_id','age_demographer','n.age','n.sex',
                          'location_id','year_start','year_end','est_year_id')]
  split_data <- split_data[order(nid)]
  
  write.csv(split_data, 
            file = "FILEPATH",
            row.names = FALSE)
  
  
  
  
  # Append split data back onto fully-specified data and save ---------------
  good_data <- good_data[,names(bun_data),with = FALSE]
  good_data[, parent_seq := integer()]
  
  final[, sex := ifelse(sex_id == 1, "Male", "Female")]
  final[, `:=` (lower = lwr.est, upper = upr.est,
                cases = NA, effective_sample_size = NA)]

  
  final <- final[,names(good_data),with = FALSE]
  
  # add back in non-age split data 'MEASURE_NAME' (prev probs) data along
  # with split data AND data of other measures
  #full_bundle <- rbind(good_data, final, other_measure_data, fill = TRUE)
  full_bundle <- rbind(good_data, final, fill = TRUE)
  
  out_file <- "FILEPATH"
  cat(paste0(Sys.time(), " Saving results to ", out_file, "\n"))
  
  write.csv(full_bundle, 
            file = out_file,
            row.names = FALSE)
  
  cat(paste0(Sys.time()), "FINISHED\n****************************\n")
  return(full_bundle)
}

# Function ----------------------------------------------------------------

#' Automatic handling of version mr brt models
#'
#' @param model_label_prefix 
#' @param description description of the model run. be clear!
#' @param path path to the crosswalk versioning file to edit
#'
#' @return tacks on addtional line with mr brt model metadata on end of file at PATH
record_mr_brt_run <- function(model_label_prefix, model_description, path = "FILEPATH") {
  versions <- fread(path)
  
  if (nrow(versions[version_name == model_label_prefix & description == model_description]) == 0) {
    new_row <- data.table(version_name = model_label_prefix,  
                          description = description, 
                          results_comment = NA_character_, 
                          is_comparable = 1, is_best = 0, 
                          date = as.character(Sys.Date()))
    versions <- rbind(versions, new_row)
    
    readr::write_csv(versions, path)
  } else {
    warning(paste0("A MR-BRT run was already recorded for the given version name and description ( ", 
                   model_label_prefix, ": ", model_description, "). Not updating versioning csv."))
  }
}


generate_crosswalk_coefs <- function(bundle_data, model_label_prefix, model_description, match_version) {
  bundle_id <- bundle_data$bundle_id
  me_id <- bundle_data$me_id
  
  bundle_path <- get_data_path(base_dir = paste0(BASE_DIR, "cached_bundles/"), bundle_id = bundle_id)
  cat_ln(paste0(Sys.time(), " Using file at ", bundle_path, "\n"))
  
  if (!SKIP_AGE_SEX_SPLIT) {
    dt <- data.table()
    
    for (measure in c("prevalence", "incidence")) {
      measure_dt <- do_agesex_split(bun_id = bundle_id, me_id = me_id, measure_name = measure,
                                    bun_path = bundle_path, save_dir = AGE_SEX_SAVE_DIR)
      
      dt <- rbind(dt, measure_dt, fill = TRUE)
    }
    
    # save age and sex split data for ALL measures
    readr::write_csv(dt, "FILEPATH")
  }
  
  # log matching
  dir.create("FILEPATH", showWarnings = F)
  sink("FILEPATH")
  
  cat_ln("Default demographics used to match:")
  for (demographic in DEMOGRAPHIC_COLS) {
    cat_ln("    -", demographic)
  }
  
  cat_ln("Default year range extension:", YEAR_OVERLAP)
  cat_ln("Default age range extension:", AGE_OVERLAP, "\n")
  
  # extract ref, alts cols and parameter overrides, if available
  ref_col <- bundle_data$reference_col
  alt_cols <- strsplit(bundle_data$alternate_cols, split = ", *")[[1]]
  demographic_cols <- strsplit(bundle_data$demographics, split = ", *")[[1]]
  year_overlap <- bundle_data$year_overlap
  age_overlap <- bundle_data$age_overlap
  
  if (any(is.na(demographic_cols))) {
    demographic_cols <- DEMOGRAPHIC_COLS
  }
  
  if (is.na(year_overlap)) {
    year_overlap <- YEAR_OVERLAP
  }
  
  if (is.na(age_overlap)) {
    age_overlap <- AGE_OVERLAP
  }
  
  cat_ln(paste0(Sys.time(), " Generating ratio matches for ", bundle_data$acause, " (", bundle_id, ")\n"))
  generate_ratios(bundle_id, ref_col = ref_col, alt_cols = alt_cols,
                  demographic_cols = demographic_cols, crosswalks = NA,
                  year_overlap = year_overlap, age_overlap = age_overlap)
  
  
  print(warnings())
  sink()
  
  # Final step: run MR-BRT
  cat_ln("***************************************************************")
  cat_ln(paste0(Sys.time()), "Running MR-BRT model + forest plotting for", bundle_data$acause)
  record_mr_brt_run(model_label_prefix = model_label_prefix, model_description = model_description)
  res <- run_mr_brt_for(bundle_data = bundle_data, model_label_prefix = model_label_prefix, match_version = match_version)
  
  # unpack results
  ratios <- res$ratios
  fit <- res$fit
  
  predict_mr_brt_for(ratios, fit = fit)
  plot_mr_brt_betas(bundle_data, mr_brt_dir = "FILEPATH")
}

# Run ---------------------------------------------------------------------

model_label_prefix <- "LABEL"
description <- "DESCRIPTION"


for (i in 3){#1:nrow(BUNDLES)) {
  bundle_data <- BUNDLES[i, ]
  
  tryCatch({
    generate_crosswalk_coefs(bundle_data, model_label_prefix = model_label_prefix, model_description = description, match_version = NA)
  }, error = function(e) { 
    cat(paste0("Crosswalk beta generation for ", bundle_data$acause, " (bundle ", bundle_data$bundle_id, ") failed:\n", e, "\n"))
    warning(paste0("Crosswalk beta generation for ", bundle_data$acause, " (bundle ", bundle_data$bundle_id, ") failed:\n", e, "\n")) 
  })
  
}

