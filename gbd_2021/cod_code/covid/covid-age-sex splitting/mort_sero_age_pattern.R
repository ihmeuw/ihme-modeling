prep_mort_sero_age_pattern <- function(pop, mort_age_pattern_path, sero_age_pattern_path, age_table, hierarchy) {
  
  prep_parent_child <- function(hierarchy, locs, dt) {
    parent_child <- rbindlist(lapply(locs, function(l) {
      path_to_top <- rev(as.integer(
        strsplit(
          hierarchy[location_id == l]$path_to_top_parent,
          ","
        )[[1]]
      ))
      p <- path_to_top[min(which(path_to_top %in% dt$location_id))]
      data.table(location_id = l, parent_id = p)
    }))
    return(parent_child[])
  }
  
  # Prep mortality age pattern to align with GBD age groups (split under-5)
  mort_age_pattern <- fread(mort_age_pattern_path)
  
  #fill in missing mort age patterns with parent mort age pattern
  missing_mort_age_locs <- setdiff(hierarchy[most_detailed==1]$location_id, unique(mort_age_pattern$location_id))
  if (length(missing_mort_age_locs)>0){
    p_c <- prep_parent_child(hierarchy, missing_mort_age_locs, mort_age_pattern)
    add_mort_age <- merge(p_c, setnames(copy(mort_age_pattern), "location_id", "parent_id"))[, parent_id := NULL]
    mort_age_pattern <- rbind(mort_age_pattern, add_mort_age)
  }
  
  # Add superregion
  uniq_mrids <- unique(mort_age_pattern$mr_id)
  uniq_srids <- uniq_mrids[grepl("super_region_name", uniq_mrids)]
  df_agepattern_mort_tmp <- mort_age_pattern %>%
    left_join(hierarchy[, c("location_id", "super_region_name")], by = "location_id") %>%
    filter(gsub("super_region_name__", "", mr_id) == super_region_name) %>%
    select(age_start, age_end, MRprob, super_region_name) %>%
    filter(!duplicated(.)) %>%
    rename(super_MRprob = MRprob)
  mort_age_pattern <- mort_age_pattern %>%
    select(location_id, age_start, age_end, MRprob) %>%
    left_join(hierarchy[, c("location_id", "super_region_name", "location_type")], by = "location_id") %>%
    filter(!location_type %in% c('global', 'superregion', 'region')) %>%
    left_join(df_agepattern_mort_tmp, by = c("super_region_name", "age_start", "age_end")) %>%
    as.data.table()
  
  # Split the under-5's
  u5_props <- age_table[age_group_years_start < 5]
  u5_props[, prop := prop.table(age_group_years_end - age_group_years_start)]
  u5_MRprobs <- as.data.table(
    tidyr::crossing(
      mort_age_pattern[age_start == 0, .(location_id, MRprob, super_MRprob)], 
      u5_props[,.(age_group_id, prop)]
    )
  )
  u5_MRprobs[, MRprob := MRprob * prop]
  u5_MRprobs[, super_MRprob := super_MRprob * prop]
  setnames(mort_age_pattern, "age_start", "age_group_years_start")
  mort_age_pattern <- merge(
    mort_age_pattern, 
    age_table[, .(age_group_years_start, age_group_id)]
  )
  mort_age_dt <- rbind(
    mort_age_pattern[age_group_years_start >= 5, .(location_id, age_group_id, MRprob, super_MRprob)],
    u5_MRprobs[, .(location_id, age_group_id, MRprob, super_MRprob)]
  )
  
  # Merge male pop prob and mort age pattern
  missing_pop_locs <- setdiff(hierarchy[most_detailed == 1]$location_id, unique(pop$location_id))
  if (length(missing_pop_locs)>0){
    p_c <- prep_parent_child(hierarchy, missing_pop_locs, pop)
    add_pop <- merge(p_c, setnames(copy(pop), "location_id", "parent_id"))[, parent_id := NULL]
    pop <- rbind(pop, add_pop)
  }

  # Prep male population proportion
  cast_pop <- dcast.data.table(pop, location_id + age_group_id ~ sex_id, value.var = "population")
  cast_pop[, male_pop_prop := `1` / `3`]
  cast_pop[, age_pop_prop := `3` / sum(`3`), by = location_id]
  pop_mort_props_dt <- merge(
    cast_pop[, .(location_id, age_group_id, male_pop_prop, age_pop_prop)],
    mort_age_dt,
    by = c("location_id", "age_group_id")
  )
  pop_mort_props_dt[, weight := MRprob * age_pop_prop]
  pop_mort_props_dt[, age_mort_prop := weight / sum(weight), by = location_id]
  pop_mort_props_dt[, super_weight := super_MRprob * age_pop_prop]
  pop_mort_props_dt[, super_age_mort_prop := super_weight / sum(super_weight), by = location_id]
  pop_mort_props_dt[, c("weight", "super_weight") := NULL]

  # Seroprevalence age pattern
  sero_age_pattern <- fread(sero_age_pattern_path)
  u5_sero_probs <- as.data.table(
    tidyr::crossing(
      sero_age_pattern[age_group_start == 0, .(seroprev)], 
      u5_props[,.(age_group_id, prop)]
    )
  )
  u5_sero_probs[, seroprev := seroprev * prop]
  setnames(sero_age_pattern, "age_group_start", "age_group_years_start")
  sero_age_pattern <- merge(
    sero_age_pattern, 
    age_table[, .(age_group_years_start, age_group_id)]
  )
  sero_age_dt <- rbind(
    sero_age_pattern[age_group_years_start >= 5, .(age_group_id, seroprev)],
    u5_sero_probs[, .(age_group_id, seroprev)]
  )
  pop_mort_sero_props_dt <- merge(pop_mort_props_dt, sero_age_dt, by = "age_group_id")
  pop_mort_sero_props_dt[, weight := seroprev * age_pop_prop]
  pop_mort_sero_props_dt[, age_sero_prop := weight / sum(weight), by = location_id]
  pop_mort_sero_props_dt[, weight := NULL]
  pop_mort_sero_props_dt <- pop_mort_sero_props_dt[order(age_group_id)]
  
  return(pop_mort_sero_props_dt)
}