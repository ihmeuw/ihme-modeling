# library(assertable)
# test <- import_hiv_draws("180531_numbat", 6, "2C", 2017, c(2:20, 30:32, 235), loc_map = setDT(get_locations(level = "all")))
# test2 <- import_hiv_draws("180531_numbat", 523, "2A", 2017, c(2:20, 30:32, 235), loc_map = setDT(get_locations(level = "all")))
import_hiv_draws <- function(hiv_run_name, loc_id, hiv_group, gbd_year, hiv_ages, loc_map, pop_dt, start_year = 1950, sim_values = c(0:999)) {
  spec_sim_dir <- "FILEPATH + hiv_run_name"
  stgpr_dir <- "FILEPATH"

  loc_ihme <- loc_map[location_id == loc_id, ihme_loc_id]

  ## Spectrum Results (if Group 1 [GEN], Group 2B [CON incomplete VR], or Group 2C [CON no data])
  ## Note that non-HIV deaths are only used for Group 1 and aren't accurate for ENN/LNN/PNN (they represent under-1 deaths, need to be split by envelope)
  if(hiv_group %in% c("1A","1B","2B","2C")) {
    spec_draws <- data.table(assertable::import_files(paste0(loc_ihme, "_ART_deaths.csv"), folder=spec_sim_dir))
    setnames(spec_draws,c("year_id", "run_num", "hiv_deaths"), c("year", "sim", "mx_spec_hiv"))
    spec_draws[, sim := sim - 1] # Format sims to be in the same number-space
    spec_draws[, non_hiv_deaths_prop := NULL]
  }

  ## ST-GPR Results (if Group 2A [CON complete VR])
  ## For these, we trust their VR systems so we take straight GPR results
  ## Note that currently, how we implement this with random draws, we underestimate uncertainty by not accounting for covariance in age/time
  if(hiv_group == "2A") {
    spec_draws <- import_files("gpr_results_2017_extended.csv", folder = stgpr_dir)

    spec_draws <- spec_draws[location_id == loc_id & !is.na(gpr_mean), list(year_id, age_group_id, sex_id, gpr_mean, gpr_var)]

    assertthat::assert_that(nrow(spec_draws) > 0)
    assertable::assert_values(spec_draws, colnames(spec_draws), "not_na", quiet = T)

    ## Generate 1000 draws by location/year/age/sex
    ## Need to use Delta Method to transform into real space before making draws
    spec_draws[gpr_mean == 0, zero := 1]
    spec_draws[gpr_mean != 0, gpr_var := ((1/gpr_mean)^2)*gpr_var]
    spec_draws[gpr_mean != 0, gpr_sd := sqrt(gpr_var)]
    spec_draws[gpr_var == 0, gpr_sd := 0]
    spec_draws[gpr_mean != 0, gpr_mean := log(gpr_mean)]

    ## Create 1000 normal sims around the logged mean/sd
    sims <- spec_draws[, list(gpr_mean, gpr_sd)]
    setnames(sims, c("mean", "sd"))
    sims <- data.table(plyr::mdply(sims, rnorm, n=1000))

    ## Combine and reshape the results, then back-transform
    spec_draws <- cbind(spec_draws, sims)
    spec_draws[,c("mean", "sd", "gpr_mean", "gpr_var", "gpr_sd"):=NULL]
    spec_draws <- melt(spec_draws, id.vars = c("year_id", "age_group_id", "sex_id", "zero"), variable.name = "sim")
    spec_draws[,sim := as.numeric(gsub("V", "", sim)) - 1]
    spec_draws[,mx_spec_hiv := exp(value) / 100] # Convert to real numbers then divide by 100 since the death rate is in rate per capita * 100
    spec_draws[zero == 1, mx_spec_hiv := 0]

    spec_draws <- spec_draws[, list(year_id, age_group_id, sex_id, sim, mx_spec_hiv)]
    spec_draws[, non_hiv_deaths := 99999] # Does not matter, HIV ratios are only used for under-15 Group 1
    setnames(spec_draws, "year_id", "year")
  }

  ## For years before where HIV has data, fill it in with 0 HIV deaths
  min_spec_year <- min(spec_draws[, year])
  if (min_spec_year > start_year) {
    age_group_ids <- unique(spec_draws[, age_group_id])
    sex_ids <- unique(spec_draws[, sex_id])
    sims <- unique(spec_draws[, sim])
    new_years <- start_year : (min_spec_year-1)

    new_rows <- expand.grid(sex_id = sex_ids, year = new_years, age_group_id = age_group_ids, sim = sims, mx_spec_hiv = 0, non_hiv_deaths = 2)
    spec_draws <- rbindlist(list(new_rows, spec_draws), use.names = T)
  }

  setnames(spec_draws, c("sim", "year"), c("draw", "year_id"))

  ## Extract under-1 results
  hiv_deaths_u1 <- spec_draws[age_group_id %in% c(2:4)]
  hiv_deaths_u1 <- merge(hiv_deaths_u1, pop_dt, by = c("year_id", "sex_id", "age_group_id"))
  hiv_deaths_u1[, mx_spec_hiv := mx_spec_hiv * mean_population]
  hiv_deaths_u1[, non_hiv_deaths := non_hiv_deaths * mean_population]

  hiv_deaths_u1 <- hiv_deaths_u1[age_group_id %in% c(2:4),
                                 .(mx_spec_hiv = sum(mx_spec_hiv),
                                   non_hiv_deaths = sum(non_hiv_deaths),
                                   mean_population = sum(mean_population)),
                                 by = c("year_id", "sex_id", "draw")]
  hiv_deaths_u1[, mx_spec_hiv := mx_spec_hiv / mean_population]
  hiv_deaths_u1[, non_hiv_deaths := non_hiv_deaths / mean_population]
  hiv_deaths_u1[, c("mean_population") := NULL]
  hiv_deaths_u1[, age_group_id := 28]

  spec_draws <- rbindlist(list(spec_draws, hiv_deaths_u1), use.names = T)
  spec_draws[, hiv_free_ratio := non_hiv_deaths / (non_hiv_deaths + mx_spec_hiv)]

  ## Value and squareness assertions
  spec_draws[is.na(hiv_free_ratio), hiv_free_ratio := 1]
  assertable::assert_values(spec_draws, "hiv_free_ratio", quiet = T)
  assertable::assert_values(spec_draws, "mx_spec_hiv", "gte", 0, quiet = T)

  id_vars <- list(year_id = start_year:gbd_year, draw = sim_values, age_group_id = hiv_ages, sex_id = c(1:2))
  assertable::assert_ids(spec_draws, id_vars, quiet = T)
  assertable::assert_values(spec_draws[age_group_id %in% c(2,3)], "mx_spec_hiv", "equal", 0, quiet = T)

  spec_draws[, location_id := loc_id]

  return(spec_draws)

}
