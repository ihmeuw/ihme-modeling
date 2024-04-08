
#' @title Load summaries and data
#' @description Load summaries and data and prepare data.table for diagnostics
#'
#' @param run_ids \[`character()`\]\cr
#'   Vector of run IDs to load in. If the vector is named, the names will be
#'   used to label the "model_type" column. If the vector is un-named, the
#'   run comment from the run configs will be used.
#' @param loc_id \[`integer(1)`\] or NULL \cr
#'   Single location ID to read in. Defaults NULL and will pull all available
#'   locations.
#' @param load_model_type \[`character(1)`\]\cr
#'   The name of "model_type" column in summaries file to be read in.
#'   Default to "regmod" but this could also be, for example, "poisson".
#' @param gbd_year \[`integer(1)`\]\cr
#'  GBD year to be passed into `demInternal::ids_names` for location metadata.
#' @param base_dir \[`character(1)`\]\cr
#'  Base directory for COVID em estimates. Example: ".../covid_em_estimate/".
#'  This is a config variable.
#'
load_summaries_and_data <- function(run_ids,
                                    loc_id = NULL,
                                    load_model_type = NULL,
                                    gbd_year = 2020,
                                    base_dir) {

  # set ID cols
  id_cols <- c("time_start", "time_unit", "year_start",
               "sex", "age_start", "age_end", "location_id")

  # Load in results ---------------------------------------------------------

  # expected deaths
  dt <- rbindlist(lapply(run_ids, function(run_id) {

    print(run_id)

    config_temp <- config::get(
      file = paste0(base_dir, "/", run_id, "/covid_em_detailed.yml"),
      use_parent = FALSE
    )

    # use run comment to label run if input run IDs are un-named
    if (is.null(names(run_id))) {
      mt <- config_temp$run_comment
    } else {
      mt <- names(run_id)
    }

    # load in data for one run
    folder <- fs::path(base_dir, run_id, "/outputs/summaries/")
    if (!is.null(loc_id)) {
      loc_list <- loc_id
    } else {
      loc_list <- fread(fs::path(base_dir, run_id, "/inputs/process_locations.csv"))
      if ("is_estimate_1" %in% names(loc_list)) {
        loc_list <- loc_list[(is_estimate_1), location_id]
      } else {
        loc_list <- loc_list[(is_estimate), location_id]
      }
    }
    if (length(loc_list) > 1) {
      temp <- assertable::import_files(
        filenames = c(paste0(c(2010:2022), ".csv"), paste0(loc_list, ".csv")),
        folder = folder,
        warn_only = T
      )
    } else {
      temp <- fread(paste0(folder, "/", loc_list, ".csv"))
    }

    # format
    setnames(temp, "mean", "death_rate_excess")
    setnames(temp, "q2.5", "death_rate_excess_lower")
    setnames(temp, "q97.5", "death_rate_excess_upper")
    if (!is.null(load_model_type)) temp <- temp[model_type %like% load_model_type]
    if (nrow(temp) == 0) {
      stop(paste0("No results for run ", run_id, " and load model type ",
                  load_model_type, "."))
    }
    if (!is.null(names(run_id))) temp[, model_type := mt]
    temp[, run_id := run_id]
    return(temp)

  }))


  # Deal with locations -----------------------------------------------------

  # merge on location info
  dt <- demInternal::ids_names(
    dt,
    id_cols = c("location_id"),
    extra_output_cols = c("ihme_loc_id"),
    gbd_year = gbd_year,
    warn_only = T
  )
  dt[location_id == 60412,
     `:=` (location_name = "Wuhan",
           ihme_loc_id = "CHN_60412")
  ]
  dt[location_id == 432,
     `:=` (location_name = "England and Wales",
           ihme_loc_id = "GBR_432")
  ]
  dt[location_id == 99999,
     `:=` (location_name = "Provincia autonoma di Bolzano + Provincia autonoma di Trento",
           ihme_loc_id = "ITA_99999")
  ]



  # Load in data ------------------------------------------------------------

  # observed deaths, covid deaths, population, covariates
  data <- rbindlist(lapply(run_ids, function(run) {

    # load in data for one run
    data_files <- paste0(dt[run_id == run, unique(ihme_loc_id)], ".csv")
    temp <- assertable::import_files(
      filenames = data_files,
      folder = fs::path(base_dir, run, "/inputs/data_all_cause_not_offset/")
    )

    # format and return
    setnames(temp, "deaths", "deaths_observed")
    temp[, run_id := run]
    return(temp)
  }))

  # combine
  dt <- merge(dt, data, by = c(id_cols, "run_id"), all.x = T)

  # remove age_name and recreate it
  dt[, age_name := NULL]
  dt <- hierarchyUtils::gen_name(dt, "age")


  # Calculate extra metrics -------------------------------------------------

  # double-check column type
  numeric_cols <- c(names(dt)[names(dt) %like% "death"], "population")
  dt[, (numeric_cols) := lapply(.SD, as.numeric), .SDcols = numeric_cols]

  # calculate other variables
  dt[, deaths_excess := death_rate_excess * population]
  dt[, deaths_excess_lower := death_rate_excess_lower * population]
  dt[, deaths_excess_upper := death_rate_excess_upper * population]

  dt[, deaths_expected := deaths_observed - deaths_excess]
  dt[, deaths_expected_lower := deaths_observed - deaths_excess_upper]
  dt[, deaths_expected_upper := deaths_observed - deaths_excess_lower]

  # calculate rates
  dt[, death_rate_observed := deaths_observed / population]
  dt[, death_rate_expected := deaths_expected / population]
  dt[, death_rate_expected_lower := deaths_expected_lower / population]
  dt[, death_rate_expected_upper := deaths_expected_upper / population]
  dt[, death_rate_covid := deaths_covid / population]

  return(dt)

}
