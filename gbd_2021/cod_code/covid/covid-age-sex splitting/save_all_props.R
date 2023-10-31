save_props <- function(dt, output_dir) {
  
  #deaths using mort-age pattern and age-weights
  death_prop_dt <- copy(dt$pop_mort_sero_props)
  death_prop_dt[, age_rate:=age_mort_prop]
  death_prop_dt <- merge(death_prop_dt, dt$deaths_rr, by=c('age_group_id'), all.x=T, allow.cartesian=T) #age-specific, global mort RR
  death_prop_dt[, male_death_prop := (rr / (1 + rr))]
  death_prop_dt[, male_rate := age_rate * male_death_prop * male_pop_prop]
  death_prop_dt[, female_rate := age_rate * (1 - male_pop_prop) * (1 - male_death_prop)]
  melt_d_prop_dt <- melt.data.table(death_prop_dt[, .(location_id, age_group_id, male_rate, female_rate)], id.vars = c("location_id", "age_group_id"))
  melt_d_prop_dt[, sex_id := ifelse(grepl("female", variable), 2, 1)]
  melt_d_prop_dt[, prop := prop.table(value), by = location_id]
  out_death_prop_dt <- melt_d_prop_dt[, .(location_id, age_group_id, sex_id, prop)][order(location_id, age_group_id, sex_id)]
  #save sex-specific only for viz tools
  props_death_sex <- out_death_prop_dt[, .(prop=sum(prop)), by=.(location_id, sex_id)]
  props_death_sex[, measure:='deaths']
  write.csv(out_death_prop_dt, file.path(output_dir, "props/age_sex_death_props.csv"), row.names = F)
  write.csv(props_death_sex, file.path(output_dir, 'props/sex_death_props.csv'), row.names=F)
  
  #sero props using only serology age pattern
  sero_prop_dt <- copy(dt$pop_mort_sero_props)
  sero_prop_dt[, age_rate:=age_sero_prop]
  rr <- 1
  sero_prop_dt[, male_sero_prop := (rr / (1 + rr))]
  sero_prop_dt[, male_rate := age_rate * male_sero_prop * male_pop_prop]
  sero_prop_dt[, female_rate := age_rate * (1 - male_pop_prop) * (1 - male_sero_prop)]
  melt_s_prop_dt <- melt.data.table(sero_prop_dt[, .(location_id, age_group_id, male_rate, female_rate)], id.vars = c("location_id", "age_group_id"))
  melt_s_prop_dt[, sex_id := ifelse(grepl("female", variable), 2, 1)]
  melt_s_prop_dt[, prop := prop.table(value), by = location_id]
  out_sero_prop_dt <- melt_s_prop_dt[, .(location_id, age_group_id, sex_id, prop)][order(location_id, age_group_id, sex_id)]
  #save sex-specific only for viz tools
  props_sero_sex <- out_sero_prop_dt[, .(prop=sum(prop)), by=.(location_id, sex_id)]
  props_sero_sex[, measure:='infections']
  
  #sero props using mort and IFR
  sero_prop_dt2 <- copy(dt$pop_mort_sero_props)
  sero_prop_dt2[, age_rate:=age_mort_prop]
  ifr_age <- copy(dt$ifr_age)
  setnames(ifr_age, 'rates', 'ifr')
  sero_prop_dt2 <- merge(sero_prop_dt2, ifr_age, by=c('age_group_id', 'location_id'), all.x=T, allow.cartesian=T)
  sero_prop_dt2[, age_rate:=age_rate/ifr]
  sero_prop_dt2[, male_sero_prop := (rr / (1 + rr))]
  sero_prop_dt2[, male_rate := age_rate * male_sero_prop * male_pop_prop]
  sero_prop_dt2[, female_rate := age_rate * (1 - male_pop_prop) * (1 - male_sero_prop)]
  melt_s_prop_dt2 <- melt.data.table(sero_prop_dt2[, .(location_id, age_group_id, male_rate, female_rate)], id.vars = c("location_id", "age_group_id"))
  melt_s_prop_dt2[, sex_id := ifelse(grepl("female", variable), 2, 1)]
  melt_s_prop_dt2[, prop := prop.table(value), by = location_id]
  out_sero_prop_dt2 <- melt_s_prop_dt2[, .(location_id, age_group_id, sex_id, prop)][order(location_id, age_group_id, sex_id)]
  
  #save sex-specific ensemble of two approaches for viz tools
  props_sero_sex2 <- out_sero_prop_dt2[, .(prop=sum(prop)), by=.(location_id, sex_id)]
  props_sero_sex2[, measure:='infections2']
  props_sero_ensemble <- rbind(props_sero_sex, props_sero_sex2)
  props_sero_ensemble <- props_sero_ensemble[, .(prop=mean(prop)), by=.(location_id, sex_id)][, measure:='infections']
  write.csv(props_sero_ensemble, file.path(output_dir, 'props/sex_infecs_props.csv'), row.names=F)
  
  #hosp props using sero and IHR
  hosp_prop_dt <- copy(dt$pop_mort_sero_props)
  hosp_prop_dt[, age_rate:=age_sero_prop]
  ihr_age <- copy(dt$ihr_age)
  setnames(ihr_age, 'rates', 'ihr')
  hosp_prop_dt <- merge(hosp_prop_dt, ihr_age, by=c('age_group_id'), all.x=T, allow.cartesian=T)
  hosp_prop_dt <- merge(hosp_prop_dt, dt$hosps_rr, by=c('age_group_id'), all.x=T, allow.cartesian=T) 
  hosp_prop_dt[, age_rate:=age_rate*ihr]
  hosp_prop_dt[, male_hosp_prop := (hosp_rr / (1 + hosp_rr))]
  hosp_prop_dt[, male_rate := age_rate * male_hosp_prop * male_pop_prop]
  hosp_prop_dt[, female_rate := age_rate * (1 - male_pop_prop) * (1 - male_hosp_prop)]
  melt_h_prop_dt <- melt.data.table(hosp_prop_dt[, .(location_id, age_group_id, male_rate, female_rate)], id.vars = c("location_id", "age_group_id"))
  melt_h_prop_dt[, sex_id := ifelse(grepl("female", variable), 2, 1)]
  melt_h_prop_dt[, prop := prop.table(value), by = location_id]
  out_hosp_prop_dt <- melt_h_prop_dt[, .(location_id, age_group_id, sex_id, prop)][order(location_id, age_group_id, sex_id)]
  #save sex-specific only for viz tools
  props_hosp_sex <- out_hosp_prop_dt[, .(prop=sum(prop)), by=.(location_id, sex_id)]
  props_hosp_sex[, measure:='hospitalizations']
  write.csv(props_hosp_sex, file.path(output_dir, 'props/sex_hosps_props.csv'), row.names=F)
  
  #save all together
  all_props <- rbind(props_death_sex, props_sero_ensemble, props_hosp_sex, fill=T)
  write.csv(all_props, file.path(output_dir, 'props/sex_infecs_hosps_deaths_props.csv'), row.names=F)
  
}
