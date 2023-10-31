library(data.table)
library(dplyr)

source("FILEPATH/get_draws.R")
source("FILEPATH/save_results_risk.R")

#' For GBD 2020, check if values vary across prior years. If not, copy
#' the values to the new years 2020, 2021, 2022.
#' 
#' @note This function mutates df
#' @return Dataframe with new years filled, or NULL if there is variation by year
year_fill <- function(df) {
  by_cols <- setdiff(names(df), "year_id")
  df[, mean_over_yrs := mean(draw_0), by=c(by_cols[!by_cols %like% "draw_"])]
  if (nrow(df[near(draw_0, mean_over_yrs)]) == nrow(df)) {
    # values do not vary across years
    df[,mean_over_yrs := NULL]
    new_years <- df[year_id == 2019, ][, list(year_id=c(2020, 2021, 2022)), by=by_cols]
    df <- rbind(df, new_years)
    return(df)
  } else {
    # values vary, so we can't backfill
    return(NULL)
  }
}

#' For GBD 2020, copy draws for age groups 4 and 5 to the new under-5
#' age groups, then drop age groups 4 and 5 from the estimates.
#' 
#' @note This function mutates df
age_fill <- function(df) {
  by_cols <- setdiff(names(df), "age_group_id")
  post_neo <- df[age_group_id == 4, ][, list(age_group_id=c(388, 389)), by=by_cols]
  one_to_four <- df[age_group_id == 5, ][, list(age_group_id=c(238, 34)), by=by_cols]
  df <- rbind(df[!age_group_id %in% c(4, 5), ], post_neo, one_to_four)
  return(df)
}

#' Save draws for the given decomp_step
save <- function(dt, data_dir, out_file, rid, description, rf_source, decomp_step) {
  save_results_risk(input_dir = data_dir,
                    input_file_pattern = out_file,
                    modelable_entity_id = unique(dt[rei_id==rid,modelable_entity_id]),
                    description = description,
                    risk_type = rf_source,
                    sex_id = unique(dt$sex_id),
                    measure_id = unique(dt$measure_id),
                    gbd_round_id = 7,
                    decomp_step = decomp_step,
                    mark_best = TRUE,
                    db_env = "prod")
}

# Setup
# Input files downloaded from RF Toolbox
do_save <- TRUE
skipped_reis <- character()
steps <- c("step2")
rf_source <- "tmrel" # rr or tmrel
base_dir <- "FILEPATH"
data_dir <- paste0(base_dir, rf_source, "/")
if (rf_source == "rr") {
  in_file <- "rr_09Apr20.csv"
  rf_name <- "relative risks"
} else {
  in_file <- "tmrel_15Apr20.csv"
  rf_name <- "TMRELs"
}
rf_data <- fread(paste0(base_dir, in_file))
message("Read ", rf_source, " input file")

# Keep only rows that have a best_model_version.
# No need to spend time on second hand smoke since it varies by year
rf_data <- rf_data[!is.na(best_model_version) & rei_id != 100]

for (rid in rf_data$rei_id) {
  rei <- rf_data[rei_id==rid, rei]
  message("Getting draws for ", rid, " (", rei, ")")
  dt <- get_draws("rei_id", rid, source=rf_source, status="best",
                  gbd_round_id=6, decomp_step="step4")

  # Copy year if no variation
  dt <- year_fill(dt)
  if (is.null(dt)) {
    display_name <- paste0(rid, ": ", rei)
    message("Values vary across years: ", display_name)
    skipped_reis <- c(skipped_reis, display_name)
    next
  }
  message("Filled years")

  # Copy age - expand Post Neonatal and age 1-4 to new age groups
  dt <- age_fill(dt)
  message("Expanded age groups")

  # Save draws to disk
  out_file <- paste0(rid, ".csv")
  write.csv(dt, paste0(data_dir, out_file), row.names=F, na="")

  # Upload to current round
  description <- paste0("Copy of GBD 2019 ", rf_name, " (model_version_id ",
                        unique(rf_data[rei_id==rid, best_model_version]),")")
  message("Running save_results_risk for ", rid)
  if (do_save) {
    for (decomp_step in steps) {
      save(dt, data_dir, out_file, rid, description, rf_source, decomp_step)
      message("Saved results for ", decomp_step)
    }
  }
}

message("Skipped reis: ", paste(skipped_reis, sep=", "))
