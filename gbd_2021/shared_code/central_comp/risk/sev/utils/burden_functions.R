# Helper functions related to YLL, YLD, etc burden
library(data.table)

source("FILEPATH/get_draws.R")

#--FUNCTIONS -------------------------------------------------------------------
#' Pull YLLs long by draws for a list of causes for a location across a set of years.
#'
#' @param cause_ids cause ids to pull YLLs for
#' @param location_id location id to pull YLLs for
#' @param year_ids years to pull YLLs for
#' @param gbd_round_id GBD round of CodCorrect YLLs are sourced from
#' @param decomp_step decomp step of CodCorrect YLLs are sourced from
#' @param codcorrect_version CodCorrect version
#' @param n_draws number of draws to pull. Downsamples if less than 1000
#'
pull_ylls <- function(cause_ids, location_id, year_ids, codcorrect_version, n_draws,
                      gbd_round_id, decomp_step = "step3") {
  message("Reading YLLs from CoDCorrect v", codcorrect_version)
  yll <- get_draws(gbd_id_type = "cause_id", gbd_id = cause_ids, location_id = location_id,
                   measure_id = 4, year_id = year_ids, gbd_round_id = gbd_round_id,
                   decomp_step = decomp_step, source = "codcorrect", version_id = codcorrect_version,
                   n_draws = n_draws, downsample = TRUE)
  if (!(all(year_ids %in% yll$year_id)))
    stop("CoDCorrect v", codcorrect_version, " is missing draws for year IDs: ",
         paste(setdiff(year_ids, unique(yll$year_id)), collapse = ", "))
  yll <- melt(yll, id.vars = c("cause_id", "location_id", "year_id", "age_group_id", "sex_id"),
              measure.vars = paste0("draw_", 0:(n_draws - 1)),
              variable.name = "draw", value.name = "yll")
  yll[, draw := as.numeric(gsub("draw_", "", draw))]

  return(yll)
}

#' Pull YLDs long by draws for a list of causes for a location across a set of years.
#'
#' @param cause_ids cause ids to pull YLLs for
#' @param location_id location id to pull YLLs for
#' @param year_ids years to pull YLLs for
#' @param gbd_round_id GBD round of CodCorrect YLLs are sourced from
#' @param decomp_step decomp step of CodCorrect YLLs are sourced from
#' @param como_version COMO version
#' @param n_draws number of draws to pull. Downsamples if less than 1000
#'
pull_ylds <- function(cause_ids, location_id, year_ids, como_version, n_draws,
                      gbd_round_id, decomp_step = "iterative") {
  message("Reading YLDs from COMO v", como_version)
  # Pull YLDs, which will be in rate space
  yld <- get_draws(gbd_id_type = "cause_id", gbd_id = cause_ids, location_id = location_id,
                   measure_id = 3, year_id = year_ids, gbd_round_id = gbd_round_id,
                   decomp_step = decomp_step, source = "como", version_id = como_version,
                   n_draws = n_draws, downsample = TRUE)
  if (!(all(year_ids %in% yld$year_id)))
    stop("COMO v", como_version, " is missing draws for year IDs: ",
         paste(setdiff(year_ids, unique(yld$year_id)), collapse = ", "))
  yld <- melt(yld, id.vars = c("cause_id", "location_id", "year_id", "age_group_id", "sex_id"),
              measure.vars = paste0("draw_", 0:(n_draws - 1)),
              variable.name = "draw", value.name = "yld")
  yld[, draw := as.numeric(gsub("draw_", "", draw))]

  # Covert rates to counts by multiplying by population
  population <- fread(paste0(out_dir, "/population_35.csv"))
  yld <- merge(yld, population, by = c("location_id", "year_id", "age_group_id", "sex_id"))
  yld[, yld := yld * population]
  yld[, population := NULL]

  return(yld)
}

#' Given subcause PAFs, pull YLLs, YLDs, or DALYs for a given year or years and
#' aggregate PAFs to parent cause.
#'
#' @return data.table of parent cause PAFs
aggregate_paf_to_parent_cause <- function(
  dt, parent_cause_id, measure, location_id, year_id, gbd_round_id, n_draws,
  codcorrect_version = NULL, como_version = NULL
) {
  # for LBW/SG outcomes (cause_id 1061) use custom cause set.
  # in all other cases, use computation cause set.
  cause_set_id <- ifelse(parent_cause_id == 1061, 20, 2)
  cause_meta <- get_cause_metadata(cause_set_id = cause_set_id, gbd_round_id = gbd_round_id) %>%
    .[(parent_id == parent_cause_id & most_detailed == 1), ]

  # Pull YLLs and/or YLDs, and calculate DALYs (YLLs + YLDs) if needed, filling in 0s for age-restrictions
  cause_dt <- data.table()
  if (measure %in% c("yll", "daly")) {
    yll <- pull_ylls(cause_ids = cause_meta$cause_id, location_id = location_id,
                     year_ids = year_id, n_draws = n_draws, gbd_round_id = gbd_round_id,
                     codcorrect_version = codcorrect_version)
    setnames(yll, "yll", "cause_burden")
    cause_dt <- rbind(cause_dt, yll)
  }
  if (measure %in% c("yld", "daly")) {
    yld <- pull_ylds(cause_ids = cause_meta$cause_id, location_id = location_id,
                     year_ids = year_id, n_draws = n_draws, gbd_round_id = gbd_round_id,
                     como_version = como_version)
    setnames(yld, "yld", "cause_burden")
    cause_dt <- rbind(cause_dt, yld)
  }
  if (measure == "daly") {
    cause_dt <- cause_dt[
      , .(cause_burden = sum(cause_burden)),
      by = c("cause_id", "location_id", "year_id", "age_group_id", "sex_id", "draw")
    ]
  }

  # Merge cause burden onto PAF dataframe
  if (length(year_id) == 1 & !all(unique(dt$year_id) %in% year_id)) {
    cause_dt <- cause_dt[, .(year_id = unique(dt$year_id)), by = setdiff(names(cause_dt), "year_id")]
  }
  parent_paf <- merge(
    dt[cause_id %in% cause_meta$cause_id], cause_dt,
    by = c("cause_id", "location_id", "year_id", "age_group_id", "sex_id", "draw"),
    all.y = TRUE
  )

  # Aggregate PAFs to parent cause, weighting by cause burden
  parent_paf[is.na(paf), paf := 0]
  return(
    parent_paf[
      , .(cause_id = parent_cause_id, paf = sum(paf * cause_burden) / sum(cause_burden)),
      by = c("location_id", "year_id", "age_group_id", "sex_id", "draw")
    ]
  )
}
