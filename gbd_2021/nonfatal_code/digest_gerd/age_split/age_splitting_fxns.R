dis_age_split <- function (intERsplit, age, gbd_round_id = 6, age_pattern_loc = 1, measure = 5, age_pattern_mod_ent, step) {

  dt <- copy(intERsplit)
  dt[ , fxn_id := paste0(seq, crosswalk_parent_seq, sex)]
  
  print("subsetting and formatting data")
  
  # Select broad-age data, excluding those points all above age 80
  sub_dt <- dt[age_end-age_start > 24 & age_start < 80, ]
  # Exclude 0s
  sub_dt <- sub_dt[mean!=0 & cases !=0, ]
  
  ggplot(sub_dt, aes(age_start, mean, color = factor(nid))) + geom_point()
  ggsave(paste0(out_path, "/broadage_PRE_dis_pattern", date, ".pdf"), width=20, height=10, limitsize=FALSE)
  
  # In-function IDs
  sub_dt[sex=="Female", sex_id := 2]
  sub_dt[sex=="Male", sex_id :=1]
  sub_dt[, year_id := year_start]
  
  # Round age-groups
  round_age <- copy(sub_dt)
  round_age[, age_start := age_start - age_start %%5]
  round_age[, age_end := age_end - age_end %%5 + 4]
  
  # Divide the age-range in the datum into 5-year groups and set the bottom of the range
  round_age[, n.age:=(age_end+1 - age_start)/5]
  round_age[, age_start_floor:=age_start]
  table(round_age$specificity)
  
  ggplot(round_age, aes(age_start, mean, color = factor(nid))) + geom_point()
  ggsave(paste0(out_path, "/broadage_sufficientcases", date, ".pdf"), width=20, height=10, limitsize=FALSE)
  
  # Make new rows for age-split data
  expanded <- rep(round_age$fxn_id, round_age$n.age) %>% data.table("fxn_id" = .)
  list_rows_to_split <- unique(expanded$fxn_id)  #save a list of the unique ids of the input rows that are now being split
  
  split <- merge(expanded, round_age, by="fxn_id", all=T)
  table(split$nid)
  print(unique(split$fxn_id))
  
  split[,age.rep:= 1:.N - 1, by =.(fxn_id)]
  split[,age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4 ]
  table(split$fxn_id, split$sex)
  table(split$nid, split$sex)
  
  print("merging age meta-data")
  # Merge age meta-data onto new rows for age-splits
  all_age_tot <- merge(split, age, by = c("age_start", "age_end"), all.x = T)
  table(all_age_tot$age_group_id, useNA = "always")
  table(all_age_tot[is.na(age_group_id), ]$age_start)
  
  # Assign age_group_id if the merge wasn't straightforward
  all_age_tot[age_start ==95, age_group_id := 235]
  table(all_age_tot$age_group_id, useNA = "always")
  
  # Make lists of years and locations for which we will need population information
  ppln_locations <- unique(all_age_tot$location_id)
  ppln_years <- unique(all_age_tot$year_id)
  ppln_ages <- unique(all_age_tot$age_group_id)
  draw_years <- as.integer(paste0(trunc(ppln_years/10), 5))
  
  # Get global draws of the best model (specified by modelable entity ID, GBD round, decomp step) you picked, for the measures you picked, for females and males and finest GBD ages, for all the years you need for splits
  draws <- paste0("draw_", 0:999)
  print("getting draws")
  age_pattern <- as.data.table(
    get_draws(gbd_id_type = "modelable_entity_id", gbd_id = age_pattern_mod_ent,
              measure_id = measure, location_id = 1, source = "epi",
              status = "best", sex_id = c(1,2), gbd_round_id = gbd_round_id, decomp_step = step,
              age_group_id = c(1:20,30,31,32,235), year_id = draw_years)  )
  # Only keep draws for the ages we actually need to split into (called all from the database because it is a more efficient call for getting aggregates)
  age_pattern <- age_pattern[age_group_id %in% ppln_ages, ]
  print(age_pattern[1:3, c(1,2,995:1008)])
  
  # Summarize draws for each yasl that we are splitting into
  print("summarizing draws")
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, point_est_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(year_id, age_group_id, sex_id, measure_id, point_est_dis, se_dismod)]
  table(age_pattern$year_id)
  
  if (measure == 5) {
    all_age_tot[measure=="prevalence", measure_id := 5]
  } else if (measure == 6) {
    all_age_tot[measure=="incidence", measure_id := 6]
  }
  
  all_age_tot[ , year_id := as.integer(paste0(trunc(year_id/10), 5))]
  all_age_wpattern <- merge(all_age_tot, age_pattern, by = c("year_id", "age_group_id", "sex_id", "measure_id"))
  table(all_age_wpattern$nid)
  all_age_wpattern[ , year_id := year_start]
  
  # Get population estimates for the yasls we're splitting data into
  print("getting population estimates")
  population <- as.data.table(get_population(location_id = ppln_locations, year_id = ppln_years, sex_id = c(1, 2), decomp_step = step,
                                             age_group_id = ppln_ages))
  print(unique(population$year_id))
  print(unique(population$age_group_id))
  print(unique(population$sex_id))
  print(unique(population$location_id))
  
  population <- population[, .(year_id, age_group_id, sex_id, location_id,  population)]
  
  # Merge population onto the table of data-points requiring external age-split, replicated for the GBD age-groups that cover the originally reported age-group, as prepared above
  all_age_wpop <- merge(all_age_wpattern, population, by = c("year_id", "age_group_id", "sex_id", "location_id"))
  table(all_age_wpop$nid)
  
  # Adjust actual data columns for the new split rows
  print("adjusting data")
  now_split <- all_age_wpop[, total_pop := sum(population), by = "fxn_id"]
  now_split[, sample_size := (population / total_pop) * sample_size]
  now_split[, cases_dis := sample_size * point_est_dis]
  now_split[, total_cases_dis := sum(cases_dis), by = "fxn_id"]
  now_split[, total_sample_size := sum(sample_size), by = "fxn_id"]
  now_split[, all_age_est := total_cases_dis/total_sample_size]
  now_split[, ratio := mean / all_age_est]
  now_split[, mean := ratio * point_est_dis]
  
  ggplot(now_split, aes(age_start, mean, color = factor(nid))) + geom_point()
  ggsave(paste0(out_path, "/POST_external_age_split", date, ".pdf"), width=20, height=10, limitsize=FALSE)
  
  now_split[, `:=` (input_type = "split", age_issue = 1, specificity = paste0(specificity, ", age"), note_modeler = paste(note_modeler, "| broad-age row split using age-pattern from dismod ME ", age_pattern_mod_ent, " round ", gbd_round_id, " step ", step))]
  blank_vars <- c("lower", "upper", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", "standard_error", "cases")
  now_split[, (blank_vars) := NA]
  drop_vars <- c("year_id", "age_group_id", "sex_id", "measure_id", "n.age", "age_start_floor", "drop", "age.rep", "age_group_years_start", "age_group_years_end", "age_group_weight_value", "point_est_dis", "se_dismod", "population", "total_pop", "cases_dis", "total_cases_dis", "total_sample_size", "all_age_est", "ratio")
  now_split[ , (drop_vars) := NULL]
  
  not_split <- dt[!(fxn_id %in% list_rows_to_split), ]
  dt2 <- rbind(not_split, now_split, use.names = TRUE)
  
  dt2[ , fxn_id:=NULL]
  
  return(dt2)

}