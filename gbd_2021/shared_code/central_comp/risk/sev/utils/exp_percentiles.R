# Functions for finding min and max exposure for a risk at given exposure percentiles

#' Assemble a data.table containing population, exposure mean, and exposure sd by demographic
#'
#' @param rei_id rei ID to calculate exposure min/max for
#' @param gbd_round_id GBD round to run for
#' @param decomp_step decomp step to run for
#' @param rei_meta list of risk metadata
#' @param pop_path optional path to a cached df containing population estimates
#'
#' @return data.table with columns location_id, year_id, age_group_id, sex_id, exp_mean,
#'     exp_sd, population
get_exposure_dt <- function(rei_id, gbd_round_id, decomp_step, rei_meta, pop_path = NULL) {

    demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)

    # get MEs for risk
    mes <- get_rei_mes(rei_id, gbd_round_id, decomp_step)

    #-- PULL EXP MEAN, EXP SD, AND POPULATION --------------------------------------
    exp_dt <- get_model_results("epi",
                                gbd_id = mes[draw_type  == "exposure"]$modelable_entity_id,
                                location_id = demo$location_id, year_id = demo$year_id,
                                age_group_id = demo$age_group_id, sex_id = demo$sex_id,
                                gbd_round_id = gbd_round_id, decomp_step = decomp_step)
    setnames(exp_dt, "mean", "exp_mean")
    exp_sd_dt <- get_model_results("epi",
                                   gbd_id = mes[draw_type  == "exposure_sd"]$modelable_entity_id,
                                   location_id = demo$location_id, year_id = demo$year_id,
                                   age_group_id = demo$age_group_id, sex_id = demo$sex_id,
                                   gbd_round_id = gbd_round_id, decomp_step = decomp_step)
    setnames(exp_sd_dt, "mean", "exp_sd")
    if (!is.null(pop_path)) {
        pop <- fread(pop_path)
    } else {
        pop <- get_population(location_id = demo$location_id, year_id = demo$year_id,
                              age_group_id = demo$age_group_id, sex_id = demo$sex_id,
                              gbd_round_id = gbd_round_id, decomp_step = decomp_step)
    }
    dt <- merge(exp_dt[, .(location_id, year_id, age_group_id, sex_id, exp_mean)],
                exp_sd_dt[, .(location_id, year_id, age_group_id, sex_id, exp_sd)],
                by = c("location_id", "year_id", "age_group_id", "sex_id"))
    dt <- merge(dt,
                pop[, .(location_id, year_id, age_group_id, sex_id, population)],
                by = c("location_id", "year_id", "age_group_id", "sex_id"))

    #-- FILTER TO RELEVANT AGES AND SEXES ------------------------------------------
    age_spans <- get_age_spans(gbd_round_id = gbd_round_id)
    age_min <- min(age_spans[
        age_group_id %in% c(rei_meta$yll_age_group_id_start, rei_meta$yld_age_group_id_start)
    ]$age_group_years_start)
    age_max <- max(age_spans[
        age_group_id %in% c(rei_meta$yll_age_group_id_end, rei_meta$yld_age_group_id_end)
    ]$age_group_years_start)
    age_range <- age_spans[age_group_years_start >= age_min & age_group_years_start <= age_max, ]$age_group_id
    rm(age_min, age_max)
    dt <- dt[age_group_id %in% age_range, ]

    if (is.na(rei_meta$female)) {
        dt <- dt[sex_id == 1, ] # only keep males
    } else if (is.na(rei_meta$male)) {
        dt <- dt[sex_id == 2, ] # only keep females
    }

    return(dt)
}

#' Return quantiles from either a normal or log normal exposure distribution
#'
#' @param dt data.table containing exposure mean, sd and population
#' @param dist exposure distribution type, i.e. "ensemble" or "normal"
#' @param percentile_interval number defining symmetrical lower and upper percentiles to return
#'     from the exposure distribution. (ex. 0.01 means we want the 1st and 99th percentiles).
#' @param median_threshold if provided, use the median exposure above the threshold instead of
#'     percentiles. Relevant for SEVs when risk-cause has an absolute risk curve. Not relevant
#'     for PAFs. Default is NULL
#'
#' @return list of two exposure values representing the lower and upper percentiles of exposure
#'     from the given distribution
log_normal <- function(dt, dist, percentile_interval, median_threshold) {
    set.seed(124535)

    if (dist == "normal") {
        sample <- rnorm(dt$population, dt$exp_mean, dt$exp_sd)
    } else {
        mu <- log(dt$exp_mean/sqrt(1 + (dt$exp_sd^2/(dt$exp_mean^2))))
        sdlog <- sqrt(log(1 + (dt$exp_sd^2/dt$exp_mean^2)))
        sample <- rlnorm(dt$population, mu, sdlog)
    }

    # if a threshold is provided, we take the median exposure above the threshold as the
    # exposure max instead of using percentiles
    if (!is.null(median_threshold)) {
        median_exposure <- median(sample[sample > median_threshold])
        percentiles <- list(median_exposure, median_exposure)
    } else {
        percentiles <- quantile(sample[sample > 0], c(percentile_interval, 1 - percentile_interval), na.rm = T)
    }
    return(percentiles)
}

#' Given a risk and an interval defining upper and lower percentiles, find the exposure value
#' at those percentiles. Return a df containing exposure lower and upper for each relevant
#' age group. Depending on metadata, some risks will have age-specific exposure values while
#' others will have the same all-age values for every age group.
#'
#' @param rei_id rei ID to calculate exposure min/max for
#' @param gbd_round_id GBD round to run for
#' @param decomp_step decomp step to run for
#' @param rei_meta list of risk metadata pulled using get_rei_meta() from PAF calculator
#' @param pop_path optional path to a cached df containing population estimates for estimation
#'     years and most-detailed locations, ages, and sexes. If not provided, will pull using
#'     get_population
#' @param percentile_interval number defining symmetrical lower and upper percentiles to return
#'     from the exposure distribution. (ex. 0.01 means we want the 1st and 99th percentiles).
#'     Default is 0.01, the percentiles used for GBD 2020 PAF calculation
#' @param median_threshold if provided, use the median exposure above the threshold instead of
#'     percentiles. Relevant for SEVs when risk-cause has an absolute risk curve. Not relevant
#'     for PAFs. Default is NULL
#'
#' @return data.table with columns rei_id, age_group_id, lower, upper
get_exp_percentiles <- function(rei_id, gbd_round_id, decomp_step, rei_meta, pop_path = NULL,
                                percentile_interval = 0.01, median_threshold = NULL) {
    # get exposure and population
    dt <- get_exposure_dt(rei_id, gbd_round_id, decomp_step, rei_meta, pop_path)

    #-- FIND MAX/MIN ---------------------------------------------------------------
    max_min <- data.table(rei_id = rei_id, age_group_id = unique(dt$age_group_id))
    if (!is.na(rei_meta$age_specific_exp)) {
        # risk uses percentiles specific to each age group
        for (age in unique(dt$age_group_id)) {
            percentiles <- log_normal(dt = dt[age_group_id==age], dist = rei_meta$exposure_type,
                                      percentile_interval = percentile_interval,
                                      median_threshold = median_threshold)
            max_min[age_group_id==age, `:=` (lower = percentiles[[1]], upper = percentiles[[2]])]
        }
    } else {
        # risk uses the same all-ages percentiles for each age group
        percentiles <- log_normal(dt = dt, dist = rei_meta$exposure_type,
                                  percentile_interval = percentile_interval,
                                  median_threshold = median_threshold)
        max_min[, `:=` (lower = percentiles[[1]], upper = percentiles[[2]])]
    }

    return(max_min)
}
