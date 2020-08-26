
# FUNCTIONS ---------------------------------------------------------------

location_split <- function(raw_dt, map){
  map_dt <- copy(map)
  dt <- copy(raw_dt)
  map_dt[, id := .GRP, by = c("nid", "parent_loc")]
  split_dt <- rbindlist(lapply(1:length(map_dt[, unique(id)]), function(x) split_loc(num = x, split_dt = dt, map_dt = map_dt)), use.names = T)
  merge_dt <- unique(map_dt[, .(nid, location_id = parent_loc, merge = 1)])
  merged_dt <- merge(dt, merge_dt, by = c("location_id", "nid"), all.x = T)
  nosplit_dt <- merged_dt[is.na(merge)]
  nosplit_dt[, merge := NULL]
  split_dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  split_dt[, seq := NA]
  total_dt <- rbind(split_dt, nosplit_dt, use.names = T, fill = T)
  return(total_dt)
}

split_loc <- function(num, split_dt, map_dt){
  instruct <- copy(map_dt[id == num])
  pop_split <- instruct[, unique(no_pop_split)]
  nid <- map_dt[id == num, unique(nid)]
  parent_loc <- map_dt[id == num, unique(parent_loc)]
  child_locs <- map_dt[id == num, unique(child_loc)]
  if (is.na(pop_split)){
    final_dt <- split_weight(manip_dt = split_dt, n = nid, p = parent_loc, cs = child_locs)
  } else if (pop_split == 1){
    final_dt <- split_even(manip_dt = split_dt, n = nid, p = parent_loc, cs = child_locs)
  }
  return(final_dt)
}

split_even <- function(manip_dt, n, p, cs){
  dt <- copy(manip_dt[nid == n & location_id == p])
  merge_dt <- copy(loc_dt[location_id %in% cs, .(location_id, location_name)])
  merge_dt[, `:=` (merge = 1, num = length(cs))]
  dt[, `:=` (merge = 1, location_id = NULL, location_name = NULL)]
  split_dt <- merge(dt, merge_dt, by = "merge", allow.cartesian = T)
  split_dt[, sample_size := sample_size / num]
  z <- qnorm(0.975)
  split_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  split_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  split_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  split_dt[, `:=` (lower = NA, upper = NA, cases = sample_size * mean, effective_sample_size = NA, uncertainty_type_value = NA,
                   note_modeler = paste0(note_modeler, " | location split using by number of locs (", num, ")"), 
                   step2_location_year = "new location-split loc", ihme_loc_id = NULL)]
  split_dt[, c("merge", "num") := NULL]
  return(split_dt)
}

split_weight <- function(manip_dt, n, p, cs){
  dt <- copy(manip_dt[nid == n & location_id == p])
  dt[, midyear := round((year_start + year_end) / 2, 0)]
  pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                           year_ids = dt[, unique(midyear)], location_ids = cs)
  pops[age_group_years_end == 125, age_group_years_end := 99]
  split_dt <- rbindlist(parallel::mclapply(1:nrow(dt), function(x) split_row(row_id = x, data_dt = dt, pop_dt = pops, cs = cs), mc.cores = "NUM"))
  return(split_dt)
}

split_row <- function(row_id, data_dt, pop_dt, cs){
  row_dt <- copy(data_dt)
  row <- row_dt[row_id]
  row[, `:=` (merge = 1, location_id = NULL, location_name = NULL)]
  pops_sub <- pop_dt[location_id %in% cs & year_id == row[, midyear] & sex == row[, sex] &
                       age_group_years_start >= row[, age_start] & age_group_years_end <= row[, age_end]]
  agg <- copy(pops_sub)
  agg <- pops_sub[, pop_sum := sum(population), by = c("location_id")]
  agg <- unique(agg, by = "location_id")
  agg[, `:=` (merge = 1, proportion = pop_sum/sum(pop_sum))]
  agg <- agg[, .(location_name, location_id, proportion, merge)]
  split <- merge(row, agg, by = "merge", allow.cartesian = T)
  split[, sample_size := sample_size * proportion]
  z <- qnorm(0.975)
  split[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  split[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  split[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  split[, `:=` (lower = NA, upper = NA, cases = sample_size * mean, effective_sample_size = NA, uncertainty_type_value = NA, 
                note_modeler = paste0(note_modeler, " | location split using population weights: ", round(proportion, 2)),
                step2_location_year = "new location-split loc", ihme_loc_id = NULL)]
  split[, c("midyear", "proportion", "merge") := NULL]
  return(split)
}

# SPLIT LOCS --------------------------------------------------------------

split_map <- as.data.table(read.xlsx(paste0("FILEPATH")))
xwalk_split_dt <- location_split(raw_dt = xwalked_dt, map = split_map)

