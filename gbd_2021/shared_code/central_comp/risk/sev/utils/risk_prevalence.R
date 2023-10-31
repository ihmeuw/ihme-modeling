#' For a given continuous risk and demographic(s), pull risk exposure and exposure SD,
#' generate a distribution of exposure and calculate prevalence above or below some
#' threshold within that distribution.
#' 
#' Implemented for FPG and SBP.
#'
#' @return data.table of prevalence above/below exposure threshold for the given risk
#' and demographics
get_risk_prevalence <- function(
    rei_id, rei, location_id, year_id, gbd_round_id, decomp_step, n_draws
) {
    # load needed libraries, some from PAF code
    source("FILEPATH/get_draws.R")
    source("FILEPATH/data.R")
    source("FILEPATH/db.R")
    source("FILEPATH/edensity.R")
    
    # Confirm REI has implementation in this function
    if (!(rei %in% c("metab_fpg", "metab_sbp"))) {
        stop(paste0("Function not implemented for rei '", rei,"'."))
    }
    
    # pull exposure and exposure SD
    exposure_dt <- merge(
        get_exp(
            rei_id = rei_id, location_id = location_id, year_id = year_id, sex_id = c(1, 2),
            gbd_round_id = gbd_round_id, decomp_step = decomp_step, n_draws = n_draws
        ),
        get_exp_sd(
            rei_id = rei_id, location_id = location_id, year_id = year_id, sex_id = c(1, 2),
            gbd_round_id = gbd_round_id, decomp_step = decomp_step, n_draws = n_draws
        ),
        by = c("location_id", "year_id", "age_group_id", "sex_id", "draw")
    )

    # Add on exposure threshold
    # For FPG: diabetic > 7 mmol/L FPG
    # For SBP: HTN HD exposure level > 140 mmHg
    rei_metadata <- get_rei_meta(rei_id = rei_id, gbd_round_id = gbd_round_id)
    if (rei == "metab_fpg") {
        exposure_dt[, threshold := 7]
    } else if (rei == "metab_sbp") {
        exposure_dt[, threshold := 140]
    }

    # calculate risk prevalence ABOVE or BELOW threshold
    ensemble_weights <- fread(paste0("FILEPATH/_weights/", rei, ".csv"))
    calc_prevalence <- function(i) {
        weights <- ensemble_weights[
            age_group_id == exposure_dt[i, ]$age_group_id & sex_id == exposure_dt[i, ]$sex_id,
        ][, c("location_id", "year_id", "age_group_id", "sex_id") := NULL][1, ]
        exposure_threshold <- exposure_dt[i, ]$threshold
        dens <- get_edensity(weights, exposure_dt[i, ]$exp_mean, exposure_dt[i, ]$exp_sd)
        if (!rei_metadata$inv_exp) {
            # Area above threshold
            return(pracma::trapz(dens$x[dens$x > exposure_threshold], dens$fx[dens$x > exposure_threshold]))
        } else {
            # Area under threshold
            return(pracma::trapz(dens$x[dens$x < exposure_threshold], dens$fx[dens$x < exposure_threshold]))
        }
        
    }
    calc_prevalencec <- compiler::cmpfun(calc_prevalence)
    prevs <- mclapply(1:nrow(exposure_dt), calc_prevalencec, mc.cores = 2)
    prevalence_dt <- cbind(exposure_dt, prevalence = unlist(prevs))
    return(prevalence_dt[, .(location_id, year_id, age_group_id, sex_id, draw, prevalence)])
}

#' Pull iron deficiency prevalence, defined as the sum of moderate and severe anemia prevalence.
#' 
#' If all the years requested are estimation years, pull estimates directly from get_draws.
#' If not, we interpolate from the minimum given year - 5 to the maximum given year + 5
#' (rounded to the closest estimation years).
#' 
#' @return data.table of prevalence of moderate or severe anemia for given risk demographics
#' in rate - space
get_iron_deficiency_prevalance <- function(location_id, year_id, gbd_round_id, decomp_step, n_draws,
                                           df_utils) {
    source("FILEPATH/get_draws.R")
    source("FILEPATH/get_demographics.R")
    source("FILEPATH/interpolate.R")
    
    # Get estimation years: if year_id is one of them, call get_draws. Otherwise, interpolate
    estimation_year_ids <- get_demographics("epi", gbd_round_id = gbd_round_id)$year_id

    # Pull moderate and severe anemia prevalence draws
    mod_anemia_me_id <- 1622
    sev_anemia_me_id <- 1623
    if (all(year_id %in% estimation_year_ids)) {
        anemia <- get_draws("modelable_entity_id", gbd_id = c(mod_anemia_me_id, sev_anemia_me_id),
                            source = "epi", measure_id = 5, metric_id = 3, location_id = location_id,
                            year_id = year_id, gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                            n_draws = n_draws, downsample = n_draws != 1000)
    } else {
        # Interpolate +/- 5 years from min and max of given years, one ME at a time
        # Here we calculate the closest estimation years to min(year_id) - 5 and max(year_id) + 5 
        estimation_years <- data.table(estimation_year_ids = estimation_year_ids, val = estimation_year_ids)
        setattr(estimation_years, "sorted", "estimation_year_ids") 
        reporting_year_start <- estimation_years[J(min(year_id) - 5), roll = "nearest"]$val
        reporting_year_end <- estimation_years[J(max(year_id) + 5), roll = "nearest"]$val
        message(paste("Interpolating anemia prevalence between", reporting_year_start, "and", reporting_year_end))
        
        mod_anemia <- interpolate(
            gbd_id_type = "modelable_entity_id",
            gbd_id = mod_anemia_me_id,
            source = "epi",
            measure_id = 5,
            location_id = location_id,
            gbd_round_id = gbd_round_id,
            decomp_step = decomp_step,
            reporting_year_start = reporting_year_start,
            reporting_year_end = reporting_year_end,
        )
        sev_anemia <- interpolate(
            gbd_id_type = "modelable_entity_id",
            gbd_id = sev_anemia_me_id,
            source = "epi",
            measure_id = 5,
            location_id = location_id,
            gbd_round_id = gbd_round_id,
            decomp_step = decomp_step,
            reporting_year_start = reporting_year_start,
            reporting_year_end = reporting_year_end,
        )
        
        # Subset to only the year requested
        year <- year_id
        anemia <- rbind(mod_anemia[year_id %in% year], sev_anemia[year_id %in% year])
        
        # Downsample draws if necessary using sample method as get_draws
        anemia <- downsample(dt=anemia, n_draws=n_draws, df_utils=df_utils)
    }
    
    # Sum moderate and severe prevalence rates together
    draw_cols <- paste0("draw_", 0:(n_draws - 1))
    anemia <- anemia[, lapply(.SD, sum), by = c("age_group_id", "location_id", "sex_id", "year_id"),
                     .SDcols = draw_cols]
    
    # Reshape long and return
    anemia <- melt(anemia, id.vars = c("age_group_id", "sex_id", "location_id", "year_id"),
                   variable.name = "draw", value.name = "prevalence")
    anemia[, draw := as.integer(gsub("draw_", "", draw))]
    
    return(anemia)
}
