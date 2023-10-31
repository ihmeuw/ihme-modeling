gen_detailed_u5_srs_deaths_pop <- function(data,
                                           run_id_vrp,
                                           path_deaths_pop,
                                           year_ids,
                                           loc_ids) {

  dt_vrp <- mortdb::get_mort_outputs(
    "death number empirical", "data",
    run_id = run_id_vrp,
    location_ids = loc_ids,
    year_ids = year_ids,
    age_group_ids = c(28, 5),
    sex_ids = 1:2,
    demographic_metadata = TRUE
  )

  dt_srs_pop_deaths <- setDT(readRDS(path_deaths_pop))

  dt_vrp_prep <- dt_vrp[
    detailed_source == "SRS",
    .(
      ihme_loc_id,
      year = year_id,
      sex = tolower(sex),
      age = age_group_years_start,
      age_end = age_group_years_end - 1,
      deaths = mean
    )
  ]

  dt_srs_pop_deaths_prep <- dt_srs_pop_deaths[
    year_id %in% year_ids & age_end <= 5 & sex_id != 3,
    .(
      ihme_loc_id,
      year = year_id,
      sex = ifelse(sex_id == 1, "male", "female"),
      age = age_start,
      age_end = age_end - 1,
      population = pop,
      deaths
    )
  ]

  dt_srs_pop_deaths_prep[
    dt_vrp_prep,
    deaths := i.deaths,
    on = .(ihme_loc_id, year, sex, age, age_end)
  ]

  dt_srs_pop_deaths_prep <- melt(
    dt_srs_pop_deaths_prep,
    measure.vars = c("population", "deaths"),
    variable.name = "measure",
    variable.factor = FALSE
  )

  dt_srs_pop_deaths_prep[, source_type := "SRS"]

  data[
    dt_srs_pop_deaths_prep,
    value := i.value,
    on = .(ihme_loc_id, year, sex, age, age_end, source_type, measure)
  ]

  invisible(data)

}
