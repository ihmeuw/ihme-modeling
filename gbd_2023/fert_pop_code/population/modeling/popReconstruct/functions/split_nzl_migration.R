split_nzl_migration <- function(dt_mig, dt_pop, age_map) {

  age_cols <- c("age_group_years_start", "age_group_years_end")
  age_groups_mig <- unique(dt_mig, by = age_cols)[, ..age_cols]

  dt_pop[
    age_groups_mig,
    abr_age_start := i.age_group_years_start,
    on = .(age_group_years_start >= age_group_years_start)
  ]

  dt_pop[location_id == 72, loc_type := "national"]
  dt_pop[location_id != 72, loc_type := "subnational"]
  dt_pop[, prop := pop / sum(pop), by = .(loc_type, year_id, sex_id, abr_age_start)]

  dt_mig_split <- dt_mig[
    dt_pop,
    .(
      age_group_id = i.age_group_id,
      location_id = i.location_id,
      year_id,
      nid = x.nid,
      underlying_nid = x.underlying_nid,
      sex_id,
      mean = x.mean * i.prop,
      age_group_years_start = i.age_group_years_start
    ),
    on = .(year_id, sex_id, age_group_years_start = abr_age_start)
  ]

  setorder(dt_mig_split, year_id, sex_id, location_id, age_group_years_start)

  dt_mig_split[
    age_map,
    age_group_years_end := i.age_group_years_end,
    on = "age_group_id"
  ]

  stopifnot(!anyNA(dt_mig_split[, -c("nid", "underlying_nid")]))
  return(dt_mig_split)

}
