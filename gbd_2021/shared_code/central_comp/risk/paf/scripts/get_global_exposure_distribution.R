get_global_exposure_distribution <- function(rei_id, gbd_round_id=7, decomp_step="iterative") {
    source("FILEPATH/get_rei_metadata.R")
    source("FILEPATH/get_model_results.R")
    source("FILEPATH/get_demographics.R")
    source("FILEPATH/get_population.R")
    source("FILEPATH/get_age_metadata.R")
    setwd("FILEPATH")
    source("./utils/db.R")

    set.seed(124535)

    #-- Load metadata for given risk  ------------------------------------------
    demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
    age_meta <- get_age_metadata(age_group_set_id = 19, gbd_round_id = gbd_round_id)

    # get risk info
    rei_meta <- get_rei_meta(rei_id=rei_id, gbd_round_id=gbd_round_id)
    mes <- get_rei_mes(rei_id, gbd_round_id, decomp_step)
    age_min <- min(rei_meta$yll_age_group_id_start, rei_meta$yld_age_group_id_start)
    age_max <- max(rei_meta$yll_age_group_id_end, rei_meta$yld_age_group_id_end)
    ages <- age_meta[age_group_years_start >= age_meta[age_group_id == age_min]$age_group_years_start &
                         age_group_years_start <= age_meta[age_group_id == age_max]$age_group_years_start, ]$age_group_id

    #-- Pull exposure mean, exposure sd, and population ------------------------
    exp_dt <- get_model_results("epi",
                                gbd_id = mes[draw_type  == "exposure"]$modelable_entity_id,
                                location_id = demo$location_id, year_id = demo$year_id,
                                age_group_id = ages, sex_id = demo$sex_id,
                                gbd_round_id = gbd_round_id, decomp_step = decomp_step)
    setnames(exp_dt, "mean", "exp_mean")
    exp_sd_dt <- get_model_results("epi",
                                   gbd_id = mes[draw_type  == "exposure_sd"]$modelable_entity_id,
                                   location_id = demo$location_id, year_id = demo$year_id,
                                   age_group_id = ages, sex_id = demo$sex_id,
                                   gbd_round_id = gbd_round_id, decomp_step = decomp_step)
    setnames(exp_sd_dt, "mean", "exp_sd")
    pop <- get_population(location_id = demo$location_id, year_id = demo$year_id,
                          age_group_id = ages, sex_id = demo$sex_id,
                          gbd_round_id = gbd_round_id, decomp_step = decomp_step)
    dt <- merge(exp_dt[, .(location_id, year_id, age_group_id, sex_id, exp_mean)],
                exp_sd_dt[, .(location_id, year_id, age_group_id, sex_id, exp_sd)],
                by = c("location_id", "year_id", "age_group_id", "sex_id"))
    dt <- merge(dt,
                pop[, .(location_id, year_id, age_group_id, sex_id, population)],
                by = c("location_id", "year_id", "age_group_id", "sex_id"))
    if (is.na(rei_meta$female)) {
      dt <- dt[sex_id == 1, ] # only keep males
    } else if (is.na(rei_meta$male)) {
      dt <- dt[sex_id == 2, ] # only keep females
    }

    # -- Sample exposure distribution ------------------------------------------
    cut_vals <- seq(.01, .99, .98/999)
    if (rei_meta$exposure_type == "normal") {
        sample <- rnorm(dt$population, dt$exp_mean, dt$exp_sd)
        percentiles <- quantile(sample[sample > 0], cut_vals, na.rm = T) %>% unname
    } else if (rei_meta$exposure_type == "lognormal") {
        mu <- log(dt$exp_mean/sqrt(1 + (dt$exp_sd^2/(dt$exp_mean^2))))
        sdlog <- sqrt(log(1 + (dt$exp_sd^2/dt$exp_mean^2)))
        sample <- rlnorm(dt$population, mu, sdlog)
        percentiles <- quantile(sample[sample > 0], cut_vals, na.rm = T) %>% unname
    } else if (rei_meta$exposure_type == "ensemble") {
        mu <- log(dt$exp_mean/sqrt(1 + (dt$exp_sd^2/(dt$exp_mean^2))))
        sdlog <- sqrt(log(1 + (dt$exp_sd^2/dt$exp_mean^2)))
        sample <- rlnorm(dt$population, mu, sdlog)
        percentiles <- quantile(sample[sample > 0], cut_vals, na.rm = T) %>% unname
    } else {
        stop("Distribution not implemented")
    }
    return(percentiles)
}
