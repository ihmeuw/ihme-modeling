prep_gbd_age_rates <- function(rates_age_pattern_path, age_table) {
  
  # Prep age-specific rates for GBD age groups
  rates_in <- fread(rates_age_pattern_path)
  if("ifr" %in% names(rates_in)) {
    setnames(rates_in, "ifr", "rates")
  }
  if("hir" %in% names(rates_in)) {
    setnames(rates_in, "hir", "rates")
  }
  if('rr' %in% names(rates_in)) {
    setnames(rates_in, 'rr', 'rates')
  }
  rates_u5_dt <- rates_in[age_group_start < 5]
  rates_5plus_dt <- rates_in[age_group_start >= 5]
  setnames(rates_5plus_dt, "age_group_start", "age_group_years_start")
  rates_5plus_dt <- merge(
    rates_5plus_dt, 
    age_table[, .(age_group_years_start, age_group_id)]
  )
  u5_ages <- age_table[age_group_years_start < 5]$age_group_id
  u5_rates <- rbindlist(lapply(u5_ages, function(a) {
    temp_dt <- copy(rates_u5_dt)
    temp_dt[, age_group_id := a]
    return(temp_dt)
  }))
  
  if (rates_age_pattern_path %like% 'ifr'){ #loc specific IFR
    rates_dt <- rbind(
      u5_rates[, .(location_id, age_group_id, rates)],
      rates_5plus_dt[, .(location_id, age_group_id, rates)],
      use.names = T
    )
  } else {
    rates_dt <- rbind(
      u5_rates[, .(age_group_id, rates)], 
      rates_5plus_dt[, .(age_group_id, rates)], 
      use.names = T
    )
  }
  rates_dt <- rates_dt[order(age_group_id)]

  return(rates_dt)
}
