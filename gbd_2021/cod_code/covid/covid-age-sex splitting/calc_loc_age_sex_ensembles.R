calc_loc_age_sex <- function(loc_id, dt, output_dir, rake_hosp, fhs, apply_scalars) {
  
  age_order <- dt$pop_mort_sero_props[location_id == loc_id]$age_group_id
  pop_prop <- dt$pop_mort_sero_props[location_id == loc_id]$male_pop_prop
  
  if(length(pop_prop) == 0) {
    stop("Missing population")
  }
  
  daily_infecs <- dt$daily_infecs[location_id == loc_id]
  daily_infecs[is.na(daily_infecs)] <- 0
  daily_deaths <- dt$daily_deaths[location_id == loc_id]
  daily_deaths[is.na(daily_deaths)] <- 0
  daily_hosps <- dt$daily_hosps[location_id == loc_id]
  daily_hosps[is.na(daily_hosps)] <- 0
  
  if (fhs==T){
    date_order <- seq(as.Date('2019-11-29'), as.Date('2023-12-31'), by = "1 day")
  } else {
    date_order <- seq(as.Date('2019-11-29'), as.Date('2023-01-01'), by = "1 day")
  }
  
  # Fill the date backwards to 12/01/2019
  min_dt <- daily_deaths[date == min(date)]
  min_dt[, paste0("draw_", 0:99) := 0]
  min_d <- unique(min_dt$date)
  if (as.Date(min_d) > as.Date("2019-11-29")){
  fill_d <- seq(as.Date("2019-11-29"), (as.Date(min_d) - 1), by = "1 day")
  fill_dt <- rbindlist(lapply(fill_d, function(d) {
      copy(min_dt)[, date := d]
    }))
    daily_deaths[, date := as.Date(date)]
    daily_deaths <- rbind(daily_deaths[date %in% date_order], fill_dt)
    daily_deaths <- daily_deaths[order(date)]
  }
  
  # Save reported deaths post zero-fill but pre application of EM scalars
  reported_covid_prod <- daily_deaths[year(date) %in% c(2020, 2021)][, year_id := year(date)][, .(location_id, year_id, draw_0)]
  reported_covid_prod <- reported_covid_prod[, .(reported_covid_prod = sum(draw_0)), by='year_id']
  
  if (apply_scalars == T){
    
    print('applying scalars')
    em_scalars <- dt$em_scalars
    em_scalars[, draw:=paste0('draw_', draw-1)]
    em_scalars <- em_scalars[!is.na(year_id)]
    loc_scalars <- em_scalars[location_id==loc_id]
    if (loc_id %in% c(133,131,39,40,189)){ #if all NA and non-zero covid, replace with regional scalar
      p_id <- gbd_hierarchy[location_id==loc_id]$parent_id
      loc_scalars <- em_scalars[location_id==p_id]
      loc_scalars[, location_id:=loc_id]
    }
    loc_scalars <- dcast.data.table(loc_scalars, location_id+year_id~draw, value.var='scalar')

    # Make daily
    em_2020 <- loc_scalars[year_id==2020]
    fill_d <- seq(as.Date("2019-11-29"), as.Date('2020-12-31'), by = "1 day")
    fill_2020 <- rbindlist(lapply(fill_d, function(d) {
      copy(em_2020)[, date := d]
    }))
    fill_2020[, date:=as.Date(date)]
    
    em_2021 <- loc_scalars[year_id==2021]
    if (fhs==T){
      fill_d <- seq(as.Date("2021-01-01"), as.Date('2023-12-31'), by = "1 day")
    } else {
      fill_d <- seq(as.Date("2021-01-01"), as.Date('2023-01-01'), by = "1 day")
    }
    fill_2021 <- rbindlist(lapply(fill_d, function(d) {
      copy(em_2021)[, date := d]
    }))
    fill_2021[, date:=as.Date(date)]
    
    loc_scalars <- rbind(fill_2020, fill_2021)
    loc_scalars <- loc_scalars[order(date)]
    draw_names <- grep("draw", names(loc_scalars), value = T)
    
    # Apply scalars
    scaled_deaths <- daily_deaths[, draw_names, with = F] * loc_scalars[, draw_names, with = F]
    
    daily_deaths <- cbind(
      daily_deaths[, .(location_id, date)],
      scaled_deaths
    )
    
    # Scale total deaths to match EM analysis (using ratio of reported deaths)
    
    # Demographics team reported deaths
    reported_covid <- em_scalars[location_id==loc_id & draw=='draw_0', .(year_id, reported_covid_demog = deaths_reported_covid)]
    
    #if the location is missing from the demographics file (china subnats), assume they used the same as prod
    if (is.na(sum(reported_covid$reported_covid_demog))){
      reported_covid <- data.table(year_id=c(2020,2021), reported_covid_demog=reported_covid_prod$reported_covid_prod)
    }
    
    # COVID production reported deaths
    # saved above (pre application of EM scalars)
    
    # Address 0s and NaNs
    reported_covid <- merge(reported_covid, reported_covid_prod, by='year_id')
    reported_covid[(reported_covid_prod==0 | reported_covid_demog==0) & abs(reported_covid_prod - reported_covid_demog) < 1,
                   c('reported_covid_prod', 'reported_covid_demog') := 1]
    reported_covid[reported_covid_demog==0 & abs(reported_covid_prod - reported_covid_demog) >= 1, reported_covid_demog:=1]
    reported_covid[reported_covid_prod==0 & abs(reported_covid_prod - reported_covid_demog) >= 1, reported_covid_prod:=1]
    
    # Calculate ratio
    reported_covid <- reported_covid[, ratio := reported_covid_demog/reported_covid_prod]
    reported_covid <- reported_covid[, .(year_id, ratio)]
    
    # Add extra years
    extra_rows <- data.table(year_id = c(2022, 2023, 2019), ratio=c(1,1,1))
    reported_covid <- rbind(reported_covid, extra_rows)
    
    # Make the adjustment to all draws of daily deaths
    daily_deaths[, year_id := year(date)]
    daily_deaths <- merge(daily_deaths, reported_covid, by='year_id')
    daily_deaths[, (draw_names) := lapply(.SD, function(x)
      x * daily_deaths[['ratio']]), .SDcols = draw_names]
    daily_deaths <- daily_deaths[, c('location_id', 'date', paste0('draw_',0:99)), with=F]
    
  } else {
    print('not applying scalars')
  }
  
  #save annual 2020 and 2021 deaths
  daily_deaths2 <- copy(daily_deaths)[, year_id:=year(date)]
  annual_deaths <- daily_deaths2[, lapply(.SD, sum),
                                      by = .(location_id, year_id),
                                      .SDcols = paste0("draw_", 0:99)]
  fwrite(annual_deaths, file.path(output_dir, 'annual_deaths', paste0(loc_id, '.csv')))
  
  
  # Daily Infections
  min_dt <- daily_infecs[date == min(date)]
  min_dt[, paste0("draw_", 0:99) := 0]
  min_d <- unique(min_dt$date)
  if (as.Date(min_d) > as.Date("2019-11-29")){
    fill_d <- seq(as.Date("2019-11-29"), (as.Date(min_d) - 1), by = "1 day")
    fill_dt <- rbindlist(lapply(fill_d, function(d) {
      copy(min_dt)[, date := d]
    }))
    daily_infecs[, date := as.Date(date)]
    daily_infecs <- rbind(daily_infecs[date %in% date_order], fill_dt)
    daily_infecs <- daily_infecs[order(date)]
  }
  
  sero_age <- dt$pop_mort_sero_props[location_id == loc_id]$age_sero_prop
  mort_age <- dt$pop_mort_sero_props[location_id == loc_id]$age_mort_prop
  ihr_age <- dt$ihr_age
  ifr_age <- dt$ifr_age[location_id == loc_id]
  
  deaths_rr <- dt$deaths_rr
  infecs_rr <- dt$infecs_rr
  hosps_rr <- dt$hosps_rr

  draw_names <- grep("draw", names(daily_infecs), value = T)
  
  draw_names_1000 <- paste0('draw_', 0:999)
  #Resample inputs up to 1000 (for FHS)
  if(length(draw_names) < 1000) {
    daily_infecs <- resample(daily_infecs, draw_names, 1000)
    daily_hosps <- resample(daily_hosps, draw_names, 1000)
    daily_deaths <- resample(daily_deaths, draw_names, 1000)
    draw_names <- paste0('draw_', 0:999)
  }
  
  #calculate infec2death
  loc_dt <- calc_age_sex(dt, daily_infecs, daily_deaths, daily_hosps, sero_age, mort_age, age_order, pop_prop, 
                         ihr_age, ifr_age, deaths_rr, infecs_rr, hosps_rr, date_order, draw_names, infec2death=T)
  loc_dt[, location_id:=loc_id]
  loc_dt[, infec2death:=T]

  # Format
  setcolorder(
    loc_dt,
    c("location_id", "date", "age_group_id", "sex_id", "measure_name", "metric_id")
  )
  loc_dt <- loc_dt[order(location_id, date, age_group_id, sex_id, measure_name)]
  loc_dt[, year_id := year(date)]
  
  # Calculate annual
  out_dt <- loc_dt[, lapply(.SD, sum),
                   by = .(location_id, age_group_id, sex_id, measure_name, metric_id, year_id),
                   .SDcols = draw_names]
  out_dt[, infec2death:=T]

  
  #Now, death2infec
  loc_dt2 <- calc_age_sex(dt, daily_infecs, daily_deaths, daily_hosps, sero_age, mort_age, age_order, pop_prop, 
                         ihr_age, ifr_age, deaths_rr, infecs_rr, hosps_rr, date_order, draw_names, infec2death=F)
  loc_dt2[, location_id:=loc_id]
  loc_dt2[, infec2death:=F]
  
  # Format
  setcolorder(
    loc_dt2,
    c("location_id", "date", "age_group_id", "sex_id", "measure_name", "metric_id")
  )
  loc_dt2 <- loc_dt2[order(location_id, date, age_group_id, sex_id, measure_name)]
  loc_dt2[, year_id := year(date)]
  
  # Calculate annual
  out_dt2<- loc_dt2[, lapply(.SD, sum),
                   by = .(location_id, age_group_id, sex_id, measure_name, metric_id, year_id),
                   .SDcols = draw_names]
  out_dt2[, infec2death:=F]


  #calculate ensemble (mean) of infections approaches and rake 
  infecs1 <- loc_dt[measure_name=='infections']
  infecs2 <- loc_dt2[measure_name=='infections']
  draw_means <- (infecs1[, draw_names, with = F] + infecs2[, draw_names, with = F])/2
  meta_data <- infecs1[, .(location_id, age_group_id, sex_id, measure_name, metric_id, year_id, date)]
  infecs_ensemble <- cbind(meta_data, draw_means)
  
  #rake infecs
  collapse_infecs <- infecs_ensemble[, lapply(.SD, sum), by = .(date), .SDcols = draw_names]
  collapse_infecs <- collapse_infecs[, lapply(.SD, function(x) ifelse(x==0, x+0.0000000001, x)), .SDcols=draw_names]
  raking_factors <- as.matrix(daily_infecs[, draw_names, with = F] / collapse_infecs[, draw_names, with = F])
  raking_factors[is.nan(raking_factors)] <- 0
  infecs_ensemble <- rbindlist(lapply(1:length(unique(infecs_ensemble$date)), function(i) {
    date_dt <- infecs_ensemble[date == unique(infecs_ensemble$date)[i]]
    r_factor <- raking_factors[i, ]
    date_dt[, draw_names] <- as.data.table(t(apply(date_dt[, draw_names, with = F], 1, r_factor, FUN = '*')))
    return(date_dt)
  })) 
  
  
  # Calculate annual
  annual_infecs_ensemble <- infecs_ensemble[, lapply(.SD, sum),
                    by = .(location_id, age_group_id, sex_id, measure_name, metric_id, year_id),
                    .SDcols = draw_names]
  
  #save final draws
  hosps <- loc_dt[measure_name %in% c('hospitalizations', 'icu')][, method_name:='Infections to deaths']
  deaths <- loc_dt2[measure_name %in% c('deaths')][, method_name:='Deaths to infections']
  infecs <- infecs_ensemble[measure_name == 'infections'][, method_name:='Ensemble']
  all_dt <- rbind(hosps, deaths, infecs, fill=T)
  all_dt[measure_name=='infections', measure_id:=6]
  all_dt[measure_name=='deaths', measure_id:=1]
  saveRDS(all_dt, file.path(output_dir, 'daily', 'final', paste0(loc_id, '.rds')))
  
  annual <- all_dt[, lapply(.SD, sum),
                   by = .(location_id, age_group_id, sex_id, measure_name, metric_id, year_id, measure_id, method_name),
                   .SDcols = draw_names]
  
  saveRDS(annual, file.path(output_dir, 'annual', 'final', paste0(loc_id, '.rds')))

}

calc_age_sex <- function(dt, daily_infecs, daily_deaths, daily_hosps, sero_age, mort_age, age_order, pop_prop, 
                         ihr_age, ifr_age, deaths_rr, infecs_rr, hosps_rr, date_order, draw_names, infec2death) {
  
  if (infec2death){
    # Infections
    infecs_mat <- as.matrix(daily_infecs[, draw_names, with = F])
    age_infecs <- outer(sero_age, infecs_mat) # Age by day by draw infecs
    male_infecs <- apply(age_infecs, c(2, 3), sex_split_outcome, infecs_rr, pop_prop)
    female_infecs <- age_infecs - male_infecs
    age_date_dt <- data.table(
      age_group_id = rep(age_order, length(date_order)),
      date = rep(date_order, each = length(age_order))
    )
    loc_infecs_dt <- rbind(
      cbind(
        age_date_dt,
        sex_id = 1,
        matrix(male_infecs, ncol = dim(male_infecs)[3], 
               dimnames = list(NULL, draw_names))
      ),
      cbind(
        age_date_dt,
        sex_id = 2,
        matrix(female_infecs, ncol = dim(female_infecs)[3], 
               dimnames = list(NULL, draw_names))
      )
    )
    loc_infecs_dt[, c("measure_name", "metric_id"):= .("infections", 1)]

    # Hospitalizations - Multiply cases by hosp:case ratio and then split by sex
    both_hosps <- age_infecs
    both_hosps[] <- apply(both_hosps, c(2, 3), ihr_age$rates, FUN = '*')
    male_hosps <- apply(both_hosps, c(2, 3), sex_split_outcome, hosps_rr$hosp_rr, pop_prop)
    female_hosps <- both_hosps - male_hosps
    loc_hosps_dt <- rbind(
      cbind(
        age_date_dt,
        sex_id = 1,
        matrix(male_hosps, ncol = dim(male_hosps)[3], 
               dimnames = list(NULL, draw_names))
      ),
      cbind(
        age_date_dt,
        sex_id = 2,
        matrix(female_hosps, ncol = dim(female_hosps)[3], 
               dimnames = list(NULL, draw_names))
      )
    )
    loc_hosps_dt[, c("measure_name", "metric_id", "date"):= .("hospitalizations", 1, date + dt$durations$hosp)]
    
    # ICU - Multiply hosps by ICU prop
    loc_icu_mat <- t(apply(loc_hosps_dt[, draw_names, with = F], 1, dt$icu_prop[draw %in% draw_names]$proportion, FUN = '*'))
    loc_icu_dt <- cbind(loc_hosps_dt[, .(age_group_id, date, sex_id)], loc_icu_mat)
    loc_icu_dt[, c("measure_name", "metric_id", "date"):= .("icu", 1, date + dt$durations$icu)]
    
    # Fill the hosp date backwards to 12/01/2019
    min_dt <- loc_hosps_dt[date == min(date)]
    min_d <- unique(min_dt$date)
    if (as.Date(min_d) > as.Date("2019-11-29")){
      fill_d <- seq(as.Date("2019-11-29"), (as.Date(min_d) - 1), by = "1 day")
      fill_dt <- rbindlist(lapply(fill_d, function(d) {
        copy(min_dt)[, date := d]
      }))
      loc_hosps_dt <- rbind(loc_hosps_dt[date %in% date_order], fill_dt)
      loc_hosps_dt <- loc_hosps_dt[order(date)]
    }
    
    # Rake hospital admissions
    if (rake_hosp==T){
      collapse_hosps <- loc_hosps_dt[, lapply(.SD, sum), by = .(date), .SDcols = draw_names]
      collapse_hosps <- collapse_hosps[, lapply(.SD, function(x) ifelse(x==0, x+0.0000000001, x)), .SDcols=draw_names]
      raking_factors <- as.matrix(daily_hosps[, draw_names, with = F] / collapse_hosps[, draw_names, with = F])
      raking_factors[is.nan(raking_factors)] <- 0
      loc_hosps_dt <- rbindlist(lapply(1:length(unique(loc_hosps_dt$date)), function(i) {
        date_dt <- loc_hosps_dt[date == unique(loc_hosps_dt$date)[i]]
        r_factor <- raking_factors[i, ]
        date_dt[, draw_names] <- as.data.table(t(apply(date_dt[, draw_names, with = F], 1, r_factor, FUN = '*')))
        return(date_dt)
      }))
    }

    # Fill the icu date backwards to 12/01/2019
    min_dt <- loc_icu_dt[date == min(date)]
    min_d <- unique(min_dt$date)
    if (as.Date(min_d) > as.Date("2019-11-29")){
      fill_d <- seq(as.Date("2019-11-29"), (as.Date(min_d) - 1), by = "1 day")
      fill_dt <- rbindlist(lapply(fill_d, function(d) {
        copy(min_dt)[, date := d]
      }))
      loc_icu_dt <- rbind(loc_icu_dt[date %in% date_order], fill_dt)
      loc_icu_dt <- loc_icu_dt[order(date)]
    }

    loc_dt <- rbindlist(list(loc_infecs_dt, loc_hosps_dt, loc_icu_dt))
    
    # Deaths - Multiply cases by death:case ratio and then split by sex
    both_deaths <- age_infecs
    both_deaths[] <- apply(both_deaths, 3, ifr_age$rates, FUN = '*')
    male_deaths <- apply(both_deaths, c(2, 3), sex_split_outcome, deaths_rr$rr, pop_prop)
    female_deaths <- both_deaths - male_deaths
    loc_deaths_dt <- rbind(
      cbind(
        age_date_dt,
        sex_id = 1,
        matrix(male_deaths, ncol = dim(male_deaths)[3], 
               dimnames = list(NULL, draw_names))
      ),
      cbind(
        age_date_dt,
        sex_id = 2,
        matrix(female_deaths, ncol = dim(female_deaths)[3], 
               dimnames = list(NULL, draw_names))
      )
    )
    loc_deaths_dt[, c("measure_name", "metric_id", "date") := .("deaths", 1, date + dt$infec_death_lag)]
    
    # End lagged age/sex-specific daily deaths at last date estimated for total daily deaths
    loc_deaths_dt <- loc_deaths_dt[date <= max(daily_deaths$date)]
    
    # Fill the date backwards to 12/01/2019
    min_dt <- loc_deaths_dt[date == min(date)]
    min_d <- unique(min_dt$date)
    if (as.Date(min_d) > as.Date("2019-11-29")){
      fill_d <- seq(as.Date("2019-11-29"), (as.Date(min_d) - 1), by = "1 day")
      fill_dt <- rbindlist(lapply(fill_d, function(d) {
        copy(min_dt)[, date := d]
      }))
      loc_deaths_dt <- rbind(loc_deaths_dt[date %in% date_order], fill_dt)
      loc_deaths_dt <- loc_deaths_dt[order(date)]
    }
    
    #rake deaths
    collapse_deaths <- loc_deaths_dt[, lapply(.SD, sum), by = .(date), .SDcols = draw_names]
    collapse_deaths <- collapse_deaths[, lapply(.SD, function(x) ifelse(x==0, x+0.0000000001, x)), .SDcols=draw_names]
    raking_factors <- as.matrix(daily_deaths[, draw_names, with = F] / collapse_deaths[, draw_names, with = F])
    raking_factors[is.nan(raking_factors)] <- 0
    loc_deaths_dt <- rbindlist(lapply(1:length(unique(loc_deaths_dt$date)), function(i) {
      date_dt <- loc_deaths_dt[date == unique(loc_deaths_dt$date)[i]]
      r_factor <- raking_factors[i, ]
      date_dt[, draw_names] <- as.data.table(t(apply(date_dt[, draw_names, with = F], 1, r_factor, FUN = '*')))
      return(date_dt)
    }))
    
    loc_dt <- rbindlist(list(loc_dt, loc_deaths_dt))
    
  } else {
    
    # Deaths
    deaths_mat <- as.matrix(daily_deaths[, draw_names, with = F])
    age_deaths <- outer(mort_age, deaths_mat) # Age by day by draw deaths
    male_deaths <- apply(age_deaths, c(2, 3), sex_split_outcome, deaths_rr$rr, pop_prop)
    female_deaths <- age_deaths - male_deaths
    age_date_dt <- data.table(
      age_group_id = rep(age_order, length(date_order)),
      date = rep(date_order, each = length(age_order))
    )
    loc_deaths_dt <- rbind(
      cbind(
        age_date_dt,
        sex_id = 1,
        matrix(male_deaths, ncol = dim(male_deaths)[3], 
               dimnames = list(NULL, draw_names))
      ),
      cbind(
        age_date_dt,
        sex_id = 2,
        matrix(female_deaths, ncol = dim(female_deaths)[3], 
               dimnames = list(NULL, draw_names))
      )
    )
    loc_deaths_dt[, c("measure_name", "metric_id"):= .('deaths', 1)]
    
    # Infecs - Divide deaths by death:case ratio and then split by sex
    both_infecs <- age_deaths
    both_infecs[] <- apply(both_infecs, 3, ifr_age$rates, FUN = '/')
    male_infecs <- apply(both_infecs, c(2, 3), sex_split_outcome, infecs_rr, pop_prop)
    female_infecs <- both_infecs - male_infecs
    loc_infecs_dt <- rbind(
      cbind(
        age_date_dt,
        sex_id = 1,
        matrix(male_infecs, ncol = dim(male_infecs)[3], 
               dimnames = list(NULL, draw_names))
      ),
      cbind(
        age_date_dt,
        sex_id = 2,
        matrix(female_infecs, ncol = dim(female_infecs)[3], 
               dimnames = list(NULL, draw_names))
      )
    )
    loc_infecs_dt[, c("measure_name", "metric_id", "date"):= .('infections', 1, date - dt$infec_death_lag)]
    
    # Fill the date forwards to 2023/01/01
    max_dt <- loc_infecs_dt[date == max(date)]
    max_d <- unique(max_dt$date)
    if (as.Date(max_d) < as.Date("2023-01-01")){
      fill_d <- seq((as.Date(max_d)+1), as.Date('2023-01-01'), by = "1 day")
      fill_dt <- rbindlist(lapply(fill_d, function(d) {
        copy(max_dt)[, date := d]
      }))
      loc_infecs_dt <- rbind(loc_infecs_dt[date %in% date_order], fill_dt)
      loc_infecs_dt <- loc_infecs_dt[order(date)]
    }
    
    #rake infecs
    collapse_infecs <- loc_infecs_dt[, lapply(.SD, sum), by = .(date), .SDcols = draw_names]
    collapse_infecs <- collapse_infecs[, lapply(.SD, function(x) ifelse(x==0, x+0.0000000001, x)), .SDcols=draw_names]
    raking_factors <- as.matrix(daily_infecs[, draw_names, with = F] / collapse_infecs[, draw_names, with = F])
    raking_factors[is.nan(raking_factors)] <- 0
    loc_infecs_dt <- rbindlist(lapply(1:length(unique(loc_infecs_dt$date)), function(i) {
      date_dt <- loc_infecs_dt[date == unique(loc_infecs_dt$date)[i]]
      r_factor <- raking_factors[i, ]
      date_dt[, draw_names] <- as.data.table(t(apply(date_dt[, draw_names, with = F], 1, r_factor, FUN = '*')))
      return(date_dt)
    }))
    
    loc_dt <- rbind(loc_deaths_dt, loc_infecs_dt)
  }
  
  return(loc_dt)
}

resample <- function(loc_dt, draw_names, n_draws) {
  samp_draws <- sample(draw_names, n_draws, replace = T)
  samp_dt <- loc_dt[, samp_draws, with = F]
  names(samp_dt) <- paste0("draw_", 0:(n_draws-1))
  loc_dt <- cbind(loc_dt[, setdiff(names(loc_dt), draw_names), with = F], samp_dt)
  return(loc_dt)
}
