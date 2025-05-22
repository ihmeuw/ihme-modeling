#' @import data.table

aim.dir <- paste0(root,"FILEPATH")
## To aid in aligning with age group storage in eppasm
age.list <- c('15-24', '15-24', '15-24', '25-34', '25-34', '35-44', '35-44', '45+', '45+')
extend.years <- function(dt, years){
  dt <- as.data.table(dt)
  if('year_id' %in% names(dt)){setnames(dt, 'year_id', 'year')}
  dt <- dt[as.integer(year) %in% as.integer(years)]
  while(max(dt$year) < max(as.integer(years))){
    dt.ext <- dt[year == max(dt$year)]
    dt.ext[, year := year + 1]
    dt <- rbind(dt, dt.ext, use.names = T)
  }  
  return(dt)
}

extrapolate_years <- function(dt, end_year, years_to_average = 5, trans_vars, id_vars = c('location_id', 'age_group_id', 'sex_id')){
  
  # Check to make sure year or year_id are not present in id_vars
  if ("year_id" %in% id_vars) stop("Years cannot be used to identify variables in the data set.")
  
  # But we do need years somewhere!
  if(!("year_id" %in% names(dt))) stop("The data table must contain a year_id variable")
  
  
  # We'll only need years between start and end years
  max_year <- max(dt$year_id)
  start_year <- max_year - years_to_average
  dt$year_id <- as.numeric(dt$year_id)
  
  all <- lapply(trans_vars,function(value_var){
    print(value_var)
    
    output <- dt[between(year_id, start_year, max_year), c(value_var,id_vars,"year_id"),with=FALSE]
    dt_var <- dt[,.SD, .SDcols=c(id_vars, value_var, 'year_id')]
    
    # Key on id_vars, and order by year
    setkeyv(output, id_vars)
    output <- output[order(year_id), .SD, by=id_vars]
    # Find rate of change using shift
    output[, roc := log(get(value_var) / shift(get(value_var), type='lag')), by=id_vars]
    
    # Find average of roc by group, to join on later
    group_change <- output[, .(mean_change = mean(roc, na.rm=T)), by=id_vars]
    
    # For forecasting, we'll cast it wide and add columns.
    form <- as.formula(paste0(paste(id_vars, collapse="+"), "~ year_id"))
    output_wide <- data.table::dcast(output, form, value.var = value_var)

    # Merge averaged ROC onto output
    
    output_wide <- output_wide[group_change, on=id_vars]
    
    # Forecasting part
    
    for (yr in c((max_year+1):end_year)) {
      interval <- yr - max_year
      output_wide[, as.character(yr) := lapply(.SD, function(x) exp(interval*mean_change)*x), .SDcols= as.character(max_year)]
    }
    
    # Melt back to long
    yr_cols <- grep("^[0-9]{4}$", names(output_wide), value=T)
    output <- melt(output_wide, id.vars = id_vars, measure.vars = yr_cols, variable.name = "year_id", value.name = value_var)
    
    
    # Coerce year_id back to integer, and bind back to original table
    output[, year_id := as.integer(levels(year_id))[year_id]]
    output <- output[between(year_id, max_year + 1, end_year)]
    # Drop unused columns - output will have only id_vars, value_var, and year_id
    dt_var <- dt_var[, .SD, .SDcols=c(id_vars, value_var, 'year_id')]
    
    ##Replace infinate values with 0, e.g. male births is always 0
    output[is.nan(get(value_var)),paste(value_var) := 0]
    
    # Rbind and sort before returning
    dt_var <- rbind(dt_var, output, use.names=T)
    setkeyv(dt_var, c(id_vars, 'year_id'))
    return(dt_var)
    
  })
  
  
  dt <- Reduce(merge,all)
  
  
  
  
  return(dt)
  
}

append.ciba.incrr <- function(dt, loc, run.name){
  ciba.incrr.prior <- fread(paste0('FILEPATH/', loc, '_SPU_inc_draws.csv'))
  ciba.incrr.prior <- melt(ciba.incrr.prior, id.vars = c('year', 'single.age', 'sex'))
  ciba.incrr.prior[, age := single.age - (single.age %% 5) ]
  ciba.incrr.prior[, single.age := NULL]
  ciba.incrr.prior <- ciba.incrr.prior[age >= 15,.(inc = mean(value)), by = c('year', 'sex', 'age', 'variable')]
  
  ##15-24:25+ ratio over time
  pop <- fread(paste0('FILEPATH/' , gbdyear, '/', run.name, '/population/', loc, '.csv'))
  pop[, age := (age_group_id - 5) * 5]
  pop[, sex := ifelse(sex_id == 1, 'male', 'female')]
  setnames(pop, 'year_id', 'year')
  ciba <- merge(ciba.incrr.prior, pop[,.(sex, age, year, population)], by = c('age', 'sex', 'year'))
  ciba[age < 25, group := 'young']
  ciba[age >= 25 & age < 50, group := 'ref']
  ciba <- ciba[!is.na(group)]
  ciba <- ciba[,.(inc = weighted.mean(inc, w = population)), by = c('group', 'sex', 'year', 'variable')]
  ciba <- ciba[,.(inc = mean(inc)), by = c('group', 'sex', 'year')]
  ciba <- dcast.data.table(ciba, sex + year ~ group, value.var = 'inc')
  ciba[, rr := young / ref]
  ciba <- ciba[year >= 1980]
  ciba[, year := year - 1979]
  fit.f <- lm(rr~year, data = ciba[sex == 'female'])
  fit.m <- lm(rr~year, data = ciba[sex == 'male'])
  attr(dt, 'specfp')$f15to24_ratio <- fit.f$coefficients
  attr(dt, 'specfp')$m15to24_ratio <- fit.m$coefficients
  
  sex.incrr.prior <- copy(ciba.incrr.prior)
  ## crude mean, could be improved
  sex.incrr.prior <- sex.incrr.prior[,.(inc = mean(inc)), by = c('year', 'sex')]
  sex.incrr.prior <- dcast.data.table(sex.incrr.prior, year ~ sex, value.var = 'inc')
  sex.incrr.prior[, rr := ifelse(male == 0, 0, female/male)]
  sex.incrr.prior <- extend.years(sex.incrr.prior, years = start.year:stop.year)
  sex.incrr.prior <- sex.incrr.prior$rr
  names(sex.incrr.prior) <- start.year:stop.year
  attr(dt, 'specfp')$incrr_sex <- sex.incrr.prior
  ref.inc <- ciba.incrr.prior[age == 25]
  ref.inc[, age := NULL]
  setnames(ref.inc, 'inc', 'ref')
  ciba.incrr.prior <- merge(ciba.incrr.prior, ref.inc, by = c('sex', 'year', 'variable'))
  ciba.incrr.prior[, rr := ifelse(ref == 0, 0, inc / ref)]
  ciba.incrr.prior <- ciba.incrr.prior[,.(rr = weighted.mean(rr, w = inc)), by = c('age', 'variable')]
  ciba.incrr.prior <- ciba.incrr.prior[,.(sd = sqrt(var(rr)), rr = mean(rr)), by = c('age')]
  old.age <- ciba.incrr.prior[age == 65]
  ciba.incrr.prior[age >= 70, rr := old.age[, rr] * 0.5]
  ciba.incrr.prior[age >= 70, sd := old.age[, sd]]
  ciba.incrr.prior[, rr := log(rr)]
  attr(dt, 'specfp')$ciba_incrr_prior <- data.frame(ciba.incrr.prior)
  
  attr(dt, 'specfp')$fitincrr <- 'cibaincrr'
  
  return(dt)
}

append.diagn <- function(dt, loc, run.name){
  diagn.dt <- fread(paste0('FILEPATH/', gbdyear, '/', run.name, '/fit_data/', loc, '.csv'))
  diagn.dt <- diagn.dt[ihme_loc_id == loc & model == 'Case Report',.(sex, mean, year)]
  diagn.mat <- dcast(diagn.dt, sex ~year, value.var = 'mean')
  diagn.mat[, sex := NULL]
  diagn.mat <- as.matrix(diagn.mat)
  attr(dt, 'eppd')$diagnoses <- diagn.mat
  return(dt)
}

append.vr <- function(dt, loc, run.name){
  years <- start.year:stop.year
  cod.dt <- fread(paste0('FILEPATH/', gbdyear, '/', run.name, '/fit_data/', loc, '.csv'))
  cod.dt <- cod.dt[year > 1980 & model == 'VR' & age_group_id >= 8 & metric == 'Count' & age_group_id != 22,.(mean, age, sex, year, age_group_id)]
  cod.dt[age_group_id >= 30, age := '80']
  cod.dt <- cod.dt[,.(mean = sum(mean)), by = c('age', 'sex', 'year')]
  cod.dt[, age := as.numeric(age)]
  backfill <- expand.grid(year = 1971:1980, sex = c('male', 'female'), age = seq(15, 80, 5), mean = 0)
  cod.dt <- rbind(backfill, cod.dt, use.names = T)
  fill <- expand.grid(year = 1971:2019, sex = c('male', 'female'), age = seq(15, 80, 5))
  cod.dt <- merge(cod.dt, fill, by = c('year', 'sex', 'age'), all = T)
  vr.deaths <- data.table::dcast(cod.dt, year + age ~ sex, value.var = 'mean')
  vr.deaths <- vr.deaths[order(year, age)]
  vr <- array(0, c(14, 2, length(years) - 1))
  for(j in 1:(length(years) - 1)){
    vr[,,j] <- as.matrix(vr.deaths[year == years[j + 1], .(male,female)])
  }
  dimnames(vr) <- list(age = seq(15, 80, 5), sex = c('Male', 'Female'), year = years[-1])
  
  eppd <- list(vr = vr, 
               country = loc.table[ihme_loc_id == loc, location_name],
               region = loc,
               projset_id = 0)
  attr(dt, 'eppd') <- eppd
  return(dt)
  
}

append.deaths <- function(dt, loc, run.name, start.year, stop.year){
  years <- start.year:stop.year
  deaths <- fread('FILEPATH/gpr_results.csv')
  deaths <- deaths[location_id == loc.table[ihme_loc_id == loc, location_id] & age_group_id >= 8,.(year_id, age_group_id, sex_id, gpr_mean, gpr_var)]
  ## Generate 1000 draws by location/year/age/sex
  ## Need to use Delta Method to transform into real space before making draws
  deaths[gpr_mean == 0,zero := 1]
  deaths[gpr_mean != 0,gpr_var := ((1/gpr_mean)^2)*gpr_var]
  deaths[gpr_mean != 0,gpr_sd := sqrt(gpr_var)]
  deaths[gpr_var == 0, gpr_sd := 0]
  deaths[gpr_mean != 0,gpr_mean := log(gpr_mean)]
  
  ## Take a draw from the logged mean/sd
  sims <- deaths[,list(gpr_mean,gpr_sd)]
  setnames(sims,c("mean","sd"))
  sims <- data.table(mdply(sims,rnorm,n=1))
  
  ## Combine and reshape the results, then back-transform
  deaths <- cbind(deaths,sims)
  setnames(deaths, 'V1', 'value')
  deaths[,c("mean","sd","gpr_mean","gpr_var","gpr_sd"):=NULL]
  deaths[,value:=exp(value)/100] # Convert to real numbers then divide by 100 since the death rate is in rate per capita * 100
  deaths[zero==1,value:=0]
  deaths <- deaths[,list(year_id,age_group_id,sex_id,value)]
  
  pop <- fread(paste0('FILEPATH/', gbdyear, '/', run.name, '/population/', loc, '.csv'))
  pop.80plus <- fread(paste0('FILEPATH', gbdyear, '/', run.name, '/population_splits/', loc, '.csv'))
  pop.80plus <- pop.80plus[age_group_id %in% c(30, 31, 32, 235)]
  pop <- rbind(pop, pop.80plus, use.names = T)
  deaths <- merge(deaths, pop, by = c('year_id', 'age_group_id', 'sex_id'))
  deaths <- deaths[,.( value = value * population, year_id, sex_id, age_group_id)]
  deaths[age_group_id >= 30, age_group_id := 21]
  deaths <- deaths[,.(value = sum(value)), by = c('year_id', 'age_group_id', 'sex_id')]
  
  ## single-age-specific
  age.map <- fread(paste0('FILEPATH/', gbdyear, '/', run.name, '/age_map.csv'))
  deaths <- merge(deaths, age.map[,.(age_group_id, age_group_name_short)], by = 'age_group_id')
  vr.deaths <- copy(deaths)
  vr.deaths[,age_group_id := NULL]
  backfill <- expand.grid(year_id = 1971:1980, sex_id = 1:2, age_group_name_short = seq(15, 80, 5), value = 0)
  vr.deaths <- rbind(vr.deaths, backfill, use.names = T)
  vr.deaths <- data.table::dcast(vr.deaths, year_id + age_group_name_short ~ sex_id, value.var = 'value')
  vr.deaths <- vr.deaths[order(year_id, age_group_name_short)]
  vr <- array(0, c(14, 2, length(years) - 1))
  for(j in 1:(length(years) - 1)){
    vr[,,j] <- as.matrix(vr.deaths[year_id == years[j + 1], c('1','2')])
  }
  dimnames(vr) <- list(age = seq(15, 80, 5), sex = c('Male', 'Female'), year = years[-1])
  
  age.expand <- data.table(age = 15:80)
  age.expand[,age_group_name_short := as.character(age - age %% 5)]
  deaths <- merge(age.expand, deaths, by = 'age_group_name_short', allow.cartesian = TRUE)
  ## assume deaths evenly split across 5-year age group
  deaths[age!=80, value := value/5]
  deaths[,c('age_group_id', 'age_group_name_short') := NULL]
  
  backfill <- expand.grid(year_id = 1971:1980, sex_id = 1:2, age = 15:80, value = 0)
  deaths <- rbind(backfill, deaths)
  deaths <- deaths[order(year_id, age)]
  deaths <- data.table::dcast(deaths, year_id + age ~ sex_id, value.var = 'value')
  
  ## rep 10x for spectrum timesteps (if we're removing deaths directly from population...)
  deaths_dt <- array(0, c(66,2,(length(years) -1) * 10))
  for(j in 1:(length(years) - 1)){
    for(k in 1:10){
      deaths_dt[,,((j - 1) * 10) + k] <- as.matrix(deaths[year_id == years[j+1], c('1', '2')]) / 10
    }
  }
  
  eppd <- list(vr = vr, 
               country = loc.table[ihme_loc_id == loc, location_name],
               region = loc,
               projset_id = 0)
  attr(dt, 'eppd') <- eppd
  attr(dt, 'specfp')$deaths_dt <- deaths_dt
  return(dt)
}

convert_paed_cd4 <- function(dt, agegr){
  if(agegr == 'u5'){
    dt[CD4 == 'GT30', cat := 1]
    dt[CD4 == '26to30', cat := 2]
    dt[CD4 == '21to25', cat := 3]
    dt[CD4 == '16to20', cat := 4]
    dt[CD4 == '11to15', cat := 5]
    dt[CD4 == '5to10', cat := 6]
    dt[CD4 == 'LT5', cat := 7]  
  }else{
    dt[CD4 == 'GT1000CD4', cat := 1]
    dt[CD4 == '750to999CD4', cat := 2]
    dt[CD4 == '500to749CD4', cat := 3]
    dt[CD4 == '350to499CD4', cat := 4]
    dt[CD4 == '200to349CD4', cat := 5]
    dt[CD4 == 'LT200CD4', cat := 6]
  }
  dt[, CD4 := NULL]
  return(dt)
}

sub.paeds <- function(dt, loc, k, start.year = 1970, stop.year = stop.year, gbdyear = gbdyear, run.name = run.name){
  
    fp_root <- fp_root
    artdist <- fread(artdist)
    artelig <- fread(artelig)
    percbf <- fread(percbf)
    mort.art <- fread(mort.art)
    prog <-  fread(prog)
    mort.offart <-  fread(mort.offart)
    dropout <- fread(dropout)
    
    
  ##these are the only ones that are extrapolated
    art <- fread(art)
    pmtct <- fread(pmtct)


  
  dir <- paste0('FILEPATH/', gbdyear, '/', run.name, '/')
  years <- start.year:stop.year
  pop <- fread(paste0(dir, 'population_single_age/', loc, '.csv'))
  setnames(pop, old = 'year_id', new= 'year')
  #pop <- extend.years(pop, years)
  pop[age_group_id == 28, age := 0]
  pop[age_group_id == 238, age:= 1]
  pop[age_group_id == 21, age := 80]
  pop[is.na(age), age := age_group_id - 48]
  pop <- pop[order(age)]
  ped.pop <- pop[age <= 14]
  ped.pop[, sex := ifelse(sex_id == 1, 'Male', 'Female')]
  ped.pop <- data.table::dcast(ped.pop[,.(age, sex, year, population)], age + year ~ sex, value.var = 'population')
  ped.basepop <- as.matrix(ped.pop[year == start.year,.(Male, Female)])
  rownames(ped.basepop) <- 0:14
  attr(dt, 'specfp')$paedbasepop <- ped.basepop
  
  attr(dt, 'specfp')$paedtargetpop <- array(0, c(15, 2, length(years)))
  for(i in 1:length(years)){
    pop.year <- as.matrix(ped.pop[year == as.integer(years[i]),.(Male, Female)])
    rownames(pop.year) <- 0:14
    attr(dt, 'specfp')$paedtargetpop[,,i] <- pop.year
  }
  
  progu5 <- prog[age %in% 0:4]
  progu5 <- convert_paed_cd4(progu5, 'u5')
  progu5 <- data.table::dcast(progu5, cat + sex ~ age, value.var = 'value')
  progu5[,sex := ifelse(sex == 'male', 'Male', 'Female')]
  attr(dt, 'specfp')$prog_u5 <- array(0, c(6, 5, 2))
  dimnames(attr(dt, 'specfp')$prog_u5) <- list(cat = 1:6, age = 0:4, sex = c('Male', 'Female'))
  for(c.sex in c('Male', 'Female')){
    prog.mat <- progu5[sex == c.sex]
    prog.mat <- as.matrix(prog.mat[,c('cat', 'sex') := NULL])
    rownames(prog.mat) <- 1:6
    attr(dt, 'specfp')$prog_u5[,,c.sex] <- prog.mat
  }
  progu15 <- prog[age %in% 5:15]
  progu15 <- convert_paed_cd4(progu15, 'u15')
  progu15 <- data.table::dcast(progu15, cat + sex ~ age, value.var = 'value')
  progu15[,sex := ifelse(sex == 'male', 'Male', 'Female')]
  attr(dt, 'specfp')$prog_u15 <- array(0, c(5, 10, 2))
  dimnames(attr(dt, 'specfp')$prog_u15) <- list(cat = 1:5, age = 5:14, sex = c('Male', 'Female'))
  for(c.sex in c('Male', 'Female')){
    prog.mat <- progu15[sex == c.sex]
    prog.mat <- as.matrix(prog.mat[,c('cat', 'sex') := NULL])
    rownames(prog.mat) <- 1:5
    attr(dt, 'specfp')$prog_u15[,,c.sex] <- prog.mat
  }
  

  mort.art[category == 'LT6Mo', artdur := 'ART0MOS']
  mort.art[category == '6to12Mo', artdur := 'ART6MOS']
  mort.art[category == 'GT12Mo', artdur := 'ART1YR']
  mort.art[,category := NULL]
  mortu5 <- mort.art[age %in% 0:4]
  mortu5 <- convert_paed_cd4(mortu5, 'u5')
  mortu5 <- data.table::dcast(mortu5, age + sex + artdur ~ cat, value.var = 'value')
  mortu5[,sex := ifelse(sex == 'male', 'Male', 'Female')]
  attr(dt, 'specfp')$art_mort_u5 <- array(0, c(3, 7, 5, 2))
  dimnames(attr(dt, 'specfp')$art_mort_u5) <- list(artdur = c('ART0MOS', 'ART6MOS', 'ART1YR'), cat = 1:7, age = paste0(0:4), sex = c('Male', 'Female'))
  for(c.age in paste0(0:4)){
    for(c.sex in c('Male', 'Female')){
      mort.mat <- mortu5[sex == c.sex & age == c.age]
      mort.mat <- as.matrix(mort.mat[,c('artdur', 'sex', 'age') := NULL])
      attr(dt, 'specfp')$art_mort_u5[,,c.age,c.sex] <- mort.mat
    }
  }
  mortu15 <- mort.art[age %in% 5:14]
  mortu15 <- convert_paed_cd4(mortu15, 'u15')
  mortu15 <- data.table::dcast(mortu15, age + sex + artdur ~ cat, value.var = 'value')
  mortu15[,sex := ifelse(sex == 'male', 'Male', 'Female')]
  attr(dt, 'specfp')$art_mort_u15 <- array(0, c(3, 6, 10, 2))
  dimnames(attr(dt, 'specfp')$art_mort_u15) <- list(artdur = c('ART0MOS', 'ART6MOS', 'ART1YR'), cat = 1:6, age = paste0(5:14), sex = c('Male', 'Female'))
  for(c.age in paste0(5:14)){
    for(c.sex in c('Male', 'Female')){
      mort.mat <- mortu15[sex == c.sex & age == c.age]
      mort.mat <- as.matrix(mort.mat[,c('artdur', 'sex', 'age') := NULL])
      attr(dt, 'specfp')$art_mort_u15[,,c.age,c.sex] <- mort.mat
    }
  }
  mortu5 <- mort.offart[age %in% 0:4]
  mortu5 <- convert_paed_cd4(mortu5, 'u5')
  mortu5 <- data.table::dcast(mortu5, age + birth_category ~ cat, value.var = 'value')
  attr(dt, 'specfp')$cd4_mort_u5 <- array(0, c(4, 7, 5, 2))
  dimnames(attr(dt, 'specfp')$cd4_mort_u5) <- list(birth_category = c("BF0", "BF12", "BF7", "perinatal"), cat = 1:7, age = paste0(0:4), sex = c('Male', 'Female'))
  for(c.sex in c('Male', 'Female')){
    for(c.age in paste0(0:4)){
      mort.mat <- mortu5[age == c.age]
      mort.mat <- as.matrix(mort.mat[,c('birth_category', 'age') := NULL])
      attr(dt, 'specfp')$cd4_mort_u5[,,c.age, c.sex] <- mort.mat
    }
  }
  mortu15 <- mort.offart[age %in% 5:14]
  mortu15 <- convert_paed_cd4(mortu15, 'u15')
  mortu15 <- data.table::dcast(mortu15, age + birth_category ~ cat, value.var = 'value')
  attr(dt, 'specfp')$cd4_mort_u15 <- array(0, c(4, 6, 10, 2))
  dimnames(attr(dt, 'specfp')$cd4_mort_u15) <- list(birth_category = c("BF0", "BF12", "BF7", "perinatal"), cat = 1:6, age = paste0(5:14), sex = c('Male', 'Female'))
  for(c.sex in c('Male', 'Female')){
    for(c.age in paste0(5:14)){
      mort.mat <- mortu15[age == c.age]
      mort.mat <- as.matrix(mort.mat[,c('birth_category', 'age') := NULL])
      attr(dt, 'specfp')$cd4_mort_u15[,,c.age, c.sex] <- mort.mat
    }
  }
  

    
   
  years <- start.year:stop.year
  art <- extend.years(art, years)
  setnames(art, old = 'year', new = 'year_id')
  
  if('ART_Cov_pct' %in% colnames(art)){
    setnames(art, old = 'ART_Cov_pct', new = 'ART_cov_pct')
    
  }

  setnames(art, old = 'year_id', new = 'year')
  

  if(min(art$year) > start.year){
    backfill <- data.table(year = start.year:(min(art$year) - 1))
    backfill <- backfill[, names(art)[!names(art) == 'year'] := 0]
    art <- rbind(art, backfill, use.names = T)
  }
  art <- art[order(year)]
  art[is.na(art)] <- 0
  art[,art_isperc := ifelse(ART_cov_pct > 0, TRUE, FALSE)]
  art[,cotrim_isperc := ifelse(Cotrim_cov_pct > 0, TRUE, FALSE)]
  artpaed <- ifelse(art$art_isperc, art[,ART_cov_pct], art[,ART_cov_num])
  names(artpaed) <- art$year
  attr(dt, 'specfp')$artpaed_num <- artpaed
  art_isperc <- art[,art_isperc]
  names(art_isperc) <- art$year
  attr(dt, 'specfp')$artpaed_isperc <- art_isperc
  cotrim <-  ifelse(art$cotrim_isperc, art[,Cotrim_cov_pct], art[,Cotrim_cov_num])
  names(cotrim) <- art$year
  attr(dt, 'specfp')$cotrim_num <- cotrim
  cotrim_isperc <- art[,cotrim_isperc]
  names(cotrim_isperc) <- art$year
  attr(dt, 'specfp')$cotrim_isperc <- cotrim_isperc
  
  
  artdist <- artdist[year %in% years]
  artdist <- extend.years(artdist, years)
  artdist <- data.table::dcast(artdist, year~age)
  artdist[, year := NULL]
  artdist <- as.matrix(artdist)
  rownames(artdist) <- years
  colnames(artdist) <- 0:14
  attr(dt, 'specfp')$paed_artdist <- artdist
  

  

  artelig <- artelig[year %in% years]
  artelig <- extend.years(artelig, years)  
  if(min(artelig$year) > start.year){
    backfill <- data.table(year = start.year:(min(artelig$year) - 1))
    backfill <- backfill[, names(artelig)[!names(artelig) == 'year'] := 0]
    artelig <- rbind(artelig, backfill, use.names = T)
  }
  artelig <- artelig[order(year)]
  artelig[age == 'LT11mos', age_start := 0]
  artelig[age == '12to35mos', age_start := 1]
  artelig[age == '35to59mos', age_start := 3]
  artelig[age == 'GT5yrs', age_start := 5]
  artelig[, age := NULL]
  attr(dt, 'specfp')$paed_arteligibility <- data.frame(artelig)
  
  
  infdist <- c(0.6, 0.12, 0.1, 0.09, 0.05, 0.03, 0.01)
  names(infdist) <- 1:7
  attr(dt, 'specfp')$paed_distnewinf <- infdist
  
  
 
  
 
  

  pmtct <- pmtct[year %in% years]
  pmtct <- extend.years(pmtct, years)
  if(min(pmtct$year) > start.year){
    backfill <- data.table(year = start.year:(min(pmtct$year) - 1))
    backfill <- backfill[, names(pmtct)[!names(pmtct) == 'year'] := 0]
    pmtct <- rbind(pmtct, backfill, use.names = T)
  }
  pmtct <- pmtct[order(year)]
  pmtct_num <- data.table(year = years)
  pmtct_isperc <- data.table(year = years)
  for(var in c('tripleARTdurPreg', 'tripleARTbefPreg', 'singleDoseNevir', 'prenat_optionB', 'prenat_optionA', 'postnat_optionB', 'postnat_optionA', 'dualARV')){
    pmtct.var <- pmtct[,c('year', paste0(var, '_num'), paste0(var, '_pct')), with = F]
    vector <- ifelse(pmtct.var[,get(paste0(var, '_pct'))] > 0, pmtct.var[,get(paste0(var, '_pct'))], pmtct.var[,get(paste0(var, '_num'))])
    pmtct_num[,paste0(var) := vector]
    vector <- ifelse(pmtct.var[,get(paste0(var, '_pct'))] > 0, TRUE, FALSE)
    pmtct_isperc[,paste0(var) := vector]
  }
  attr(dt, 'specfp')$pmtct_num <- data.frame(pmtct_num)
  attr(dt, 'specfp')$pmtct_isperc <- data.frame(pmtct_isperc)
  

  attr(dt, 'specfp')$pmtct_dropout <- data.frame(dropout)
  
  attr(dt, 'specfp')$perc_bf_on_art <- percbf[,on_arv]
  attr(dt, 'specfp')$perc_bf_off_art <- percbf[,no_arv]
  mtctrans <- fread(paste0('FILEPATH/', gbdyear, '/paeds/PMTCT_transmission_rts_2016.csv'))
  attr(dt, 'specfp')$MTCtrans <- data.frame(mtctrans)
  childCTXeffect <- fread(paste0('FILEPATH/', gbdyear, '/paeds/Child_treatment_effects_cotrim.csv'))
  childCTXeffect <- childCTXeffect[year_fm_start %in% 1:5,.(hivmort_reduction = mean(hivmort_reduction)), by = 'ART_status']
  attr(dt, 'specfp')$childCTXeffect <- childCTXeffect
  
  ## Survival
  surv <- fread(paste0('FILEPATH/',loc, "_life_tables.csv"))
  surv <- melt(surv, id.vars = c('sex', 'year', 'age'))
  surv[, variable := as.integer(gsub('px', '', variable))]
  surv <- surv[variable == k]
  surv[,variable := NULL]
  surv[sex == 'male', sex := 'Male']
  surv[sex == 'female', sex := 'Female']
  surv <- data.table::dcast(surv, year + age ~ sex)
  surv = extend.years(surv, years)
  surv <- surv[age %in% 0:14]
  attr(dt, 'specfp')$paed_Sx <- array(0, c(15, 2, length(years)))
  for(i in 1:length(years)){
    sx.year = as.matrix(surv[year == as.integer(years[i]), .(Male, Female)])
    rownames(sx.year) <- 0:14
    attr(dt, 'specfp')$paed_Sx[,,i] <- sx.year
  }
  
  ## Migration
  dir <- paste0('FILEPATH/', gbdyear, '/', run.name, '/')
  mig <- fread(paste0(dir, 'migration/', loc, '.csv'))
  setnames(mig, 'sex', 'sex_id')
  mig <- mig[age %in% 0:14]
  mig <- mig[sex_id %in% c(1,2)]
  mig[, sex := ifelse(sex_id == 1, 'Male', 'Female')]
  mig <- data.table::dcast(mig[,.(year, age, value, sex)], year + age ~ sex)
  mig <- extend.years(mig, years)
  attr(dt, 'specfp')$paed_mig <- array(0, c(15, 2, length(years)))
  for(i in 1:length(years)){
    mig.year = as.matrix(mig[year == as.integer(years[i]), .(Male, Female)])
    rownames(mig.year) <- 0:14
    attr(dt, 'specfp')$paed_mig[,,i] <- mig.year
  }  
  
  return(dt)
}

sub.pop.params.specfp <- function(fp, loc, k){
  
  ## Population
  years <- start.year:stop.year
  pop <- fread(population_single_age)
  pop <- extend.years(pop, years)
  pop[age_group_id == 28, age := 0]
  pop[age_group_id == 21, age := 80]
  pop[is.na(age), age := age_group_id - 48]
  pop <- pop[order(age)]
  pop <- pop[age %in% 15:80]
  pop[, sex := ifelse(sex_id == 1, 'Male', 'Female')]
  pop <- data.table::dcast(pop[,.(age, sex, year, population)], age + year ~ sex, value.var = 'population')
  fp$targetpop <- array(0, c(66, 2, length(years)))
  dimnames(fp$targetpop) <- list(paste0(15:80), c('Male', 'Female'), years)
  fp$basepop <- array(0, c(66, 2))
  dimnames(fp$basepop) <- list(paste0(15:80), c('Male', 'Female'))
  for(i in 1:length(years)){
    pop.year <- as.matrix(pop[year == as.integer(years[i]),.(Male, Female)])
    rownames(pop.year) <- 15:80
    fp$targetpop[,,i] <- pop.year
    if(i == 1){
      fp$basepop <- pop.year
    }
  }
  
  ## Survival
  surv <- fread(paste0('FILEPATH/',loc, "_life_tables.csv"))
  surv <- melt(surv, id.vars = c('sex', 'year', 'age'))
  surv[, variable := as.integer(gsub('px', '', variable))]
  surv <- surv[variable == k]
  surv[,variable := NULL]
  surv[sex == 'male', sex := 'Male']
  surv[sex == 'female', sex := 'Female']
  print(surv)
  surv <- data.table::dcast(surv, year + age ~ sex)
  surv = extend.years(surv, years)
  surv <- surv[age %in% 15:80]
  fp$Sx <- array(0, c(66, 2, length(years)))
  dimnames(fp$Sx) <- list(paste0(15:80), c('Male', 'Female'), years)
  for(i in 1:length(years)){
    sx.year = as.matrix(surv[year == as.integer(years[i]), .(Male, Female)])
    rownames(sx.year) <- 15:80
    fp$Sx[,,i] <- sx.year
  }
  

  ## ASFR
  asfr <- fread(ASFR)
  asfr <- extend.years(asfr, years)
  ## Copy 5-year asfr
  for(c.age in 15:49){
    if(!c.age %in% asfr$age){
      asfr.ext <- asfr[age == (c.age - c.age%%5)]
      asfr.ext[, age := c.age]
      asfr <- rbind(asfr, asfr.ext)
    }
  }
  asfr <- as.matrix(data.table::dcast.data.table(asfr, age~year))
  asfr <- asfr[,2:(length(years) + 1)]
  rownames(asfr) <- 15:49
  fp$asfr <- array(0, c(35, length(years)))
  dimnames(fp$asfr) <- list(age = paste0(15:49), year = paste0(years))
  fp$asfr <- asfr
  
  ## Births
  births <- fread(births)
  births <- births$population
  names(births) <- start.year:stop.year
  fp$births <- births
  
  ## SRB
  srb <- fread(SRB)
  srb.mat <- array(0, c(2, length(years)))
  srb.mat[1,] <- srb$male_srb[1:length(years)]
  srb.mat[2,] <- srb$female_srb[1:length(years)]
  colnames(srb.mat) <- years
  fp$srb <- srb.mat
  
  ## Migration
  mig <- fread(migration)
  setnames(mig, 'sex', 'sex_id')
  mig <- mig[sex_id %in% c(1,2),]
  mig[, sex := ifelse(sex_id == 1, 'Male', 'Female')]
  mig <- data.table::dcast(mig[,.(year, age, value, sex)], year + age ~ sex)
  mig <- extend.years(mig, years)
  fp$netmigr <- array(0, c(66, 2, length(years)))
  dimnames(fp$netmigr) <- list(paste0(15:80), c('Male', 'Female'), paste0(years))
  for(i in 1:length(years)){
    mig.year = as.matrix(mig[year == as.integer(years[i]) & age %in% 15:80, .(Male, Female)])
    rownames(mig.year) <- 15:80
    fp$netmigr[,,i] <- mig.year
  }
  return(fp)
}


sub.prev <- function(loc, dt){
  gen.pop.dict <- c("General Population", "General population", "GP", "GENERAL POPULATION", "GEN. POPL.", "General population(Low Risk)", "Remaining Pop")
  if(length(dt) == 1) {
    gen.pop.i <- 1
  } else {
    gen.pop.i <- which(names(dt) %in% gen.pop.dict)
  }
  if(grepl('KEN', loc)){
    surv.path <- "FILEPATH/prev_surveys.csv"
  }else{
    if(grepl('IND', loc)){
      surv.path <- "FILEPATH/prev_surveys.csv"
      
    }else{
      surv.path <- prev_surveys
      
    
  }}
  data4 <- fread(surv.path)[iso3 == loc]
  
  if(loc=="STP"){
    data4 = data4[!sex_id==3]
    data4 = data4[!is.na(loc_year)]
    data4 = data4[!(year==2008 & age_year=="15-49")]
  }
  
  data4[,c("iso3", "int_year", "nid") := NULL]
  
  if(nrow(data4) > 0) {
    data4[,used:=TRUE]
    data4[prev==0,used:=FALSE]
    data4[,W.hhs:=qnorm(prev)]
    data4[,v.hhs:=2*pi*exp(W.hhs^2)*se^2]
    data4[,sd.W.hhs := sqrt(v.hhs)]
    data4[,idx := year - (attr(dt, 'specfp')$ss$time_epi_start-1.5)]
    data4[, agegr := '15-49']
    data4[, sex := 'both']
    data4[, deff := 2]
    data4[, deff_approx := 2]

    if(grepl("ZAF", loc)) {
      drop.years <- unique(c(data4$year,data4$year+1,data4$year-1))
      data4 <- rbind(data4,attr(dt, 'eppd')$hhs[! attr(dt, 'eppd')$hhs$year %in% drop.years, ], fill = TRUE)
    } 
    data4 <- data4[order(data4$year),]

    if(!length(dt)){
      attr(dt, 'eppd')$hhs <- as.data.frame(data4[, .(year, sex, agegr, n, prev, se, used, deff, deff_approx)])
    } else{
      attr(dt[[gen.pop.i]], 'eppd')$hhs <- as.data.frame(data4[, .(year, sex, agegr, n, prev, se, used, deff, deff_approx)])
    }
  } else { 
    print(paste0("No surveys for ",loc))
  }
  return(dt)
}

## This function appends region-specific fertility rate ratio parameters, provided by Jeff Eaton (June 2019)
add_frr_noage_fp <- function(obj){
  path <- 'FILEPATH/'
  frr_country <- read.csv(paste0(path, 'frr_country_sex12m.csv'), stringsAsFactors = FALSE) %>%
    mutate(sex12m_z = (sex12m - 40) / 15)
  frr_age <- read.csv(paste0(path, 'frr_age.csv'),stringsAsFactors = FALSE)
  frr_cd4cat <- read.csv(paste0(path, 'frr_cd4.csv'), stringsAsFactors = FALSE)
  
  frr_art_age <- read.csv(paste0(path, 'frr_art.csv'), stringsAsFactors = FALSE)
  frr_15to19sex12m <- read.csv(paste0(path, "frr_15to19sex12m_z.csv"),header = FALSE) %>% as.numeric
  
  
  country_name <- attr(obj, "country")
  
  region_name <- filter(frr_country, country == country_name)$region
  
  if(length(region_name) != 1){
    region_name <- 'East'
    country_name <- 'Malawi'
  }
  
  frr_beta_country <- filter(frr_country, country == country_name)$frr_country
  if(length(frr_beta_country) != 1)
    frr_beta_country <- 1.0
  
  frr_beta_cd4 <- frr_cd4cat$est
  frr_beta_age <- filter(frr_age, region == region_name)$est * frr_beta_country
  
  sex12m_z <- filter(frr_country, country == country_name)$sex12m_z
  frr_15to19 <- frr_beta_age[1] * frr_15to19sex12m ^ sex12m_z
  
  frr_beta_art_age <- frr_art_age$est * frr_beta_country
  
  ## Construct FRR parameter inputs
  frr_cd4 <- array(NA, c(7, 8, length(start.year:stop.year)))
  frr_cd4[] <- outer(frr_beta_cd4, c(frr_15to19, frr_15to19, frr_beta_age[2:7]), "*")
  
  frr_art <- array(NA, c(3, 7, 8, length(start.year:stop.year)))
  frr_art[1,,,] <- frr_cd4
  frr_art[2:3,,,] <- rep(frr_beta_art_age[c(1, 1:7)], each = 2*7)
  
  attr(obj, "specfp")$frr_cd4 <- frr_cd4
  attr(obj, "specfp")$frr_art <- frr_art
  
  obj
}

sub.prev.granular <- function(dt, loc, test.sub_prev_granular){
  ## TODO: Add this to cache prev
  ##make sure that this only keeps sex 3 for 15-49
  if(grepl('KEN', loc)){
    age.prev.dt <- fread(prev_surveys)
    age.prev.dt <- age.prev.dt[sex_id == 3,]
  }else{
    if(grepl('IND', loc)){
      prev_surveys <- "FILEPATH/prev_surveys_ind.csv"
      age.prev.dt <- fread(prev_surveys)

      }else{
        age.prev.dt <- fread(prev_surveys)
        
       }
    }
  if('use' %in% colnames(age.prev.dt) & loc != 'CPV'){
    age.prev.dt <- age.prev.dt[use == TRUE]
  }else{
    print('CPV testing in sub.prev.granular')
    age.prev.dt <- age.prev.dt[use == FALSE]
    
  }
  
  if (loc == "ETH_44858"){
    age.prev.dt <- age.prev.dt[iso3 == "ETH_95069"]
  }else{
    age.prev.dt <- age.prev.dt[iso3 == loc]
  }
  
    
  if(loc == 'AGO'){
    age.prev.dt[year == 2016, year := 2015]
  }
  ## outliering this survey
  if(grepl('CMR', loc)){
    age.prev.dt <- age.prev.dt[year != 2018,]
  }
  age.prev.dt[,loc_year := paste0(iso3, '_', year)]
  

  copied_site_years <- intersect(age.prev.dt[age_year %in% c("15"), unique(loc_year)], age.prev.dt[age_year %in% c("15-49"), unique(loc_year)])
  age.prev.dt <- age.prev.dt[loc_year %in% copied_site_years & (age_year %in% c('15-49', '15-64')), age_year := 'drop']
  age.prev.dt <- age.prev.dt[age_year != 'drop',]
  # 


  ##this step makes it so that it is just all ages (15 - 64)
  age.prev.dt <- age.prev.dt[age_year %in% 15:59 | age_year == '15-49' | age_year == '15-64']
  age.prev.dt[!age_year %in% c('15-49', '15-64'), agegr := paste0(age_year, '-', as.numeric(age_year)+4)]
  age.prev.dt[age_year == '15-49', agegr := '15-49']
  age.prev.dt[age_year == '15-64', agegr := '15-64']
  age.prev.dt[sex_id == 1, sex := 'male']
  age.prev.dt[sex_id == 2, sex := 'female']
  age.prev.dt[sex_id == 3, sex := 'both']
  age.prev.dt[,c('used','deff', 'deff_approx') := list(TRUE,2, 2)]
  age.prev.dt <- age.prev.dt[,.(year, sex, agegr, n, prev, se, used, deff, deff_approx)]
  age.prev.dt$n <- as.numeric(age.prev.dt$n)
  gen.pop.dict <- c("General Population", "General population", "GP", "GENERAL POPULATION", "GEN. POPL.", "General population(Low Risk)", "Remaining Pop")
 
   if(length(dt) == 1) {
    gen.pop.i <- 1
  } else {
    gen.pop.i <- which(names(dt) %in% gen.pop.dict)
  }
  if(!length(dt)){
    attr(dt, 'eppd')$hhs <- as.data.frame(age.prev.dt)
  } else{
    attr(dt[[gen.pop.i]], 'eppd')$hhs <- as.data.frame(age.prev.dt)
  }
  return(dt)
}



sub.off.art <- function(dt, loc, k) {
  # Off-ART Mortality
  mortnoart <- fread(paste0(aim.dir, "transition_parameters/HIVmort_noART/current_draws_combined/",loc,"_mortality_par_draws.csv"))
  mortnoart <- mortnoart[draw==k,]
  mortnoart[age == '45-100', age := '45+']
  mortnoart[age == '15-25', age := '15-24']
  mortnoart[age == '25-35', age := '25-34']
  mortnoart[age == '35-45', age := '35-44']
  mortnoart[,risk:=-1*log(1-mort)/0.1]
  mortnoart[,prob:=1-exp(-1*risk)]
  cd4_cats <- unique(mortnoart[,cd4])
  cd4_vars <- data.table(cd4=cd4_cats)
  
  mortnoart <- mortnoart[cd4=="GT500CD4", cat := 1]
  mortnoart <- mortnoart[cd4=="350to500CD4", cat := 2]
  mortnoart <- mortnoart[cd4=="250to349CD4", cat := 3]
  mortnoart <- mortnoart[cd4=="200to249CD4", cat := 4]
  mortnoart <- mortnoart[cd4=="100to199CD4", cat := 5]
  mortnoart <- mortnoart[cd4=="50to99CD4", cat := 6] 
  mortnoart <- mortnoart[cd4=="LT50CD4", cat := 7] 
  mortnoart[,risk:=-1*log(1-prob)]
  mortnoart <- mortnoart[,.(age,risk,cat)]
  age.list <- c('15-24', '15-24', '15-24', '25-34', '25-34', '35-44', '35-44', '45+', '45+')
  age.index <- list(a = '15-24', b = '15-24', c = '15-24', d = '25-34', e = '25-34', f = '35-44', g = '35-44', h = '45+', i = '45+')
  replace <- array(0, c(7, length(age.list), 2))
  dimnames(replace) <- list(cd4stage = paste0(1:7), agecat = age.list, sex = c('Male', 'Female'))
  for(c.sex in c('Male', 'Female')){
    for(c.ageindex in 1:length(age.list)){
      for(c.cd4 in paste0(1:7)){
        replace[c.cd4, c.ageindex, c.sex] = as.numeric(mortnoart[age == age.index[[c.ageindex]] & cat == c.cd4, risk])
      }
    }
  }  
  if(!length(dt)){
    attr(dt, 'specfp')$cd4_mort <- replace
  } else{
    for (n in names(dt)) {
      for(i in 1:7){
        attr(dt[[n]], 'specfp')$cd4_mort <- replace
      }
    }
  }
  return(dt)
}

sub.on.art <- function(dt, loc, k) {
mortart <- fread(mortart)
  
  mortart <- melt(mortart, 
                  id = c("durationart", "cd4_category", "age", "sex","cd4_lower",
                         "cd4_upper"))
  
  mortart[value > 1, value := 0.9999]
  
  setnames(mortart, c("variable","value","cd4_category"),c("draw","mort","cd4"))
  
  mortart <- mortart[draw==paste0('mort',k),]
  mortart[,sex := as.character(sex)]
  mortart[, sex := ifelse(sex == 1, 'Male', 'Female')]
  mortart[age == '45-55', age := '45+']
  mortart[age == '15-25', age := '15-24']
  mortart[age == '25-35', age := '25-34']
  mortart[age == '35-45', age := '35-44']
  mortart <- mortart[age != '55-100']
  cd4_cats <- unique(mortart[,cd4])
  durat_cats <- unique(mortart[,durationart])
  cd4_vars <- expand.grid(durationart=durat_cats, cd4=cd4_cats)
  mortart <- mortart[cd4=="ARTGT500CD4", cat := 1]
  mortart <- mortart[cd4=="ART350to500CD4", cat := 2]
  mortart <- mortart[cd4=="ART250to349CD4", cat := 3]
  mortart <- mortart[cd4=="ART200to249CD4", cat := 4]
  mortart <- mortart[cd4=="ART100to199CD4", cat := 5]
  mortart <- mortart[cd4=="ART50to99CD4", cat := 6] 
  mortart <- mortart[cd4=="ARTLT50CD4", cat := 7]
  mortart <- mortart[,.(durationart, age, sex, mort, cat)]
  mortart[,risk:=-1*log(1-mort)]
  mortart[, mort := NULL]
  mortart[durationart == '6to12Mo', artdur := 'ART6MOS']
  mortart[durationart == 'GT12Mo', artdur := 'ART1YR']
  mortart[durationart == 'LT6Mo', artdur := 'ART0MOS']
  mortart[, durationart := NULL]
  setnames(mortart, c('age', 'cat'), c('agecat', 'cd4stage'))
  
  age.index <- list(a = '15-24', b = '15-24', c = '15-24', d = '25-34', e = '25-34', f = '35-44', g = '35-44', h = '45+', i = '45+')
  replace <- array(0, c(3, 7, length(age.list), 2))
  dimnames(replace) <- list(artdur = c('ART0MOS', 'ART6MOS', 'ART1YR'), cd4stage = paste0(1:7), agecat = age.list, sex = c('Male', 'Female'))
  
  for(c.sex in c('Male', 'Female')){
    for(c.ageindex in 1:length(age.list)){
      for(c.dur in c('ART0MOS', 'ART6MOS', 'ART1YR')){
        for(c.cd4 in paste0(1:7)){
          replace[c.dur, c.cd4, c.ageindex, c.sex] = 
            as.numeric(mortart[sex == c.sex & artdur == c.dur & agecat == age.index[[c.ageindex]] & cd4stage == c.cd4, risk])
        }
      }
    }
  }
  if(!length(dt)){
    attr(dt, 'specfp')$art_mort <- replace
  } else{
    for(n in names(dt)){
      attr(dt[[n]], 'specfp')$art_mort <- replace
    }
  }
  return(dt)
}

sub.cd4.prog <- function(dt, loc, k){
  progdata <- fread(paste0(aim.dir, "transition_parameters/DurationCD4cats/current_draws_combined/", loc, "_progression_par_draws.csv"))
  progdata <- progdata[order(age,cd4,draw)]
  progdata_read <- progdata[,c("age","cd4","draw","prog"), with=F]
  progdata_read <- progdata_read[,lambda:=1/prog]
  
  progdata <- progdata_read[draw==k,]
  progdata[,risk:=-1*log(1-prog)/0.1]
  progdata[,prob:=1-exp(-1*risk)]
  
  progdata[age == '45-100', age := '45+']
  progdata[age == '15-25', age := '15-24']
  progdata[age == '25-35', age := '25-34']
  progdata[age == '35-45', age := '35-44']
  progdata <- progdata[cd4=="GT500CD4", cat := 1]
  progdata <- progdata[cd4=="350to500CD4", cat := 2]
  progdata <- progdata[cd4=="250to349CD4", cat := 3]
  progdata <- progdata[cd4=="200to249CD4", cat := 4]
  progdata <- progdata[cd4=="100to199CD4", cat := 5]
  progdata <- progdata[cd4=="50to99CD4", cat := 6] 
  progdata[,risk:=-1*log(1-prob)]
  
  age.index <- list(a = '15-24', b = '15-24', c = '15-24', d = '25-34', e = '25-34', f = '35-44', g = '35-44', h = '45+', i = '45+')
  replace <- array(0, c(6, length(age.list), 2))
  dimnames(replace) <- list(cd4stage = paste0(1:6), agecat = age.list, sex = c('Male', 'Female'))
  for(c.sex in c('Male', 'Female')){
    for(c.ageindex in 1:length(age.list)){
      for(c.cd4 in paste0(1:6)){
        replace[c.cd4, c.ageindex, c.sex] = as.numeric(progdata[age == age.index[[c.ageindex]] & cat == c.cd4, risk])
      }
    }
  }  
  if(!length(dt)){
    attr(dt, 'specfp')$cd4_prog <- replace
  } else{
    for (n in names(dt)) {
      attr(dt[[n]], 'specfp')$cd4_prog <- replace
    }	
  }
  return(dt)
}

geo_adj <- function(loc, dt, i, uncertainty) {
    print("using LBD Adjustment")
    
    ##Bring in the matched data - reading in as CSV rather then fread because the latter seems to add quotations when there are escape characters, which messes up the matching
    # Read in LBD matched data ---------------------------------------
    anc.dt.all <- read.csv(paste0('FILEPATH/', loc, '_ANC_matched.csv'))  
    if(grepl("KEN",loc)){
      anc.dt.all <- anc.dt.all[which(anc.dt.all$subpop == attr(dt,"eppd")$ancsitedat$subpop[1]),]
    }
    if(grepl("SOM",loc)){
      anc.dt.all <- anc.dt.all[anc.dt.all$subpop %in% "Remaining females",]
    }
    setnames(anc.dt.all, old = 'prev', new = 'mean')
    setnames(anc.dt.all, old = 'clinic', new = 'site')
    anc.dt.all <- as.data.table(anc.dt.all)
    anc.dt.all  <- anc.dt.all[,c( "site","year_id","mean","site_pred","adm0_mean","adm0_lower", "adm0_upper","subpop","high_risk")]
    
    # Read in ancsitedat from the PJNZ ---------------------------------------
    eppd <- attr(dt, "eppd")

  
    if(uncertainty){
      #Choose 1 from 1000 draws of uncertainty using adm0_mean bounds
      for(row in 1:nrow(anc.dt.all)){
        if(!is.na(anc.dt.all[row,adm0_mean])){
          set.seed(j)

          lower <- anc.dt.all[row,adm0_lower]
          upper <- anc.dt.all[row,adm0_upper]
          replace <- sample(runif(1000,lower,upper),1)
          anc.dt.all[row,adm0_mean := replace]
          
        } else {
          next
        }
      }
    }
    
    # Generate offset ---------------------------------------
    ##Generate the local:national offest term as the probit difference between national and predicted site prevalence - by subpopulation to avoid duplicates
    all.anc <- data.table()
    for(subpop2 in unique(anc.dt.all$subpop)){
        
        if(subpop2 %in% unique(eppd$ancsitedat$subpop)){
          anc.dt <- anc.dt.all[subpop == subpop2] 
          site.dat <- eppd$ancsitedat[eppd$ancsitedat$subpop==subpop2,]
          site.dat <- as.data.table(site.dat)
        } else {
          anc.dt <- anc.dt.all
          site.dat <- eppd$ancsitedat
          site.dat <- as.data.table(site.dat)
        }
        
        anc.dt  <- anc.dt[,offset := qnorm(adm0_mean)-qnorm(site_pred)]
        site.dat[,adm0_lower := NULL]
        site.dat[,adm0_upper := NULL]
        site.dat[,adm0_mean := NULL]
        site.dat[,site_pred:=NULL]
        site.dat[,offset := NULL]
        anc.dt[,site := as.character(site)]
        anc.dt[,mean := NULL]
        if(loc == 'SEN'){
          anc.dt[,offset := 0]
        }
  

        if(loc == 'ZWE' | loc == 'MOZ' | loc == 'BEN' | loc == 'ETH_44859' | loc == "KEN_35626" | loc == 'LSO' | loc == 'MWI' |
           loc == 'NAM' | loc == 'NGA_25332' | loc == 'SOM' | loc == 'SWZ' | loc == 'TZA' | loc == 'ZMB' | grepl('KEN', loc) | grepl('ETH', loc)){
          anc.dt[,high_risk := FALSE]
          anc.dt[,high_risk := unique(site.dat[,high_risk])]
        }
        if(!'high_risk' %in% colnames(site.dat)){
          site.dat[subpop %in% c('Urban', 'Rural'), high_risk := FALSE]
          site.dat[,high_risk := as.logical(high_risk)]
        }
        if('year' %in% colnames(site.dat)){
          setnames(site.dat, 'year', 'year_id')
        }
        anc.dt <- merge(site.dat, anc.dt, by.x = c('site',  "year_id" ,"subpop" ,"high_risk" ),
              by.y = c('site',  'year_id',  'subpop', 'high_risk'))

        #replace NAs with averages
        replace <- anc.dt[is.na(offset) & year_id >= 2000,]
        anc.dt <- data.table(anc.dt)
        for(site_x in replace$site){
          if(!all(is.na(anc.dt[site == site_x,offset]))){
            anc.dt[is.na(offset) & site == site_x, site_pred := mean(anc.dt[site == site_x, site_pred], na.rm = T)]
            anc.dt[is.na(offset) & site == site_x, adm0_mean := mean(anc.dt[site == site_x, adm0_mean], na.rm = T)]
            anc.dt[is.na(offset) & site == site_x, adm0_lower := mean(anc.dt[site == site_x, adm0_lower], na.rm = T)]
            anc.dt[is.na(offset) & site == site_x, adm0_upper := mean(anc.dt[site == site_x, adm0_upper], na.rm = T)]
            anc.dt[is.na(offset) & site == site_x, offset := qnorm(adm0_mean)-qnorm(site_pred)]
            if(any(is.nan(anc.dt[site == site_x,offset]))){
              anc.dt[is.nan(offset) & site == site_x, offset := mean(anc.dt[site == site_x, offset], nan.rm = T)]
              
            }
            
          }else{
            years_to_fill <- unique(anc.dt[is.na(offset) & site == site_x,year_id])
            for(year_x in years_to_fill){
              anc.dt[is.na(offset) & year_id == year_x & site == site_x, site_pred := mean(anc.dt[year_id == year_x, site_pred], na.rm = T)]
              anc.dt[is.na(offset) & year_id == year_x & site == site_x, adm0_mean := mean(anc.dt[year_id == year_x, adm0_mean], na.rm = T)]
              anc.dt[is.na(offset) & year_id == year_x & site == site_x, adm0_lower := mean(anc.dt[year_id == year_x, adm0_lower], na.rm = T)]
              anc.dt[is.na(offset) & year_id == year_x & site == site_x, adm0_upper := mean(anc.dt[year_id == year_x, adm0_upper], na.rm = T)]
              anc.dt[is.na(offset) & year_id == year_x & site == site_x,offset := qnorm(adm0_mean)-qnorm(site_pred)]
              if(any(is.nan(anc.dt[site == site_x,offset]))){
                anc.dt[is.nan(offset) & year_id == year_x & site == site_x, offset := mean(anc.dt[year_id == year_x, offset], na.rm = T)]
                
              }
            }
          }
        }
        
        #copy over post 2000 data to pre 2000 data
        if(any(is.na(anc.dt[year_id < 2000,offset]))){
          min_offset <- c()
          for(site.x in unique(anc.dt$site)){
            df <- anc.dt[site == site.x,.(year_id, offset)]
            df <- df[order(year_id),]
            min_offset[site.x] <- df$offset[!is.na(df$offset)][1]
            
          }
          min_offset <- data.table(min_offset)
          min_offset[,site := unique(anc.dt$site)]
          pre_2000 <- anc.dt[year_id < 2000 & is.na(offset)]
          pre_2000[,offset:= NULL]
          pre_2000 <- merge(pre_2000, min_offset, by = 'site')
          setnames(pre_2000, 'min_offset', 'offset')
          anc.dt <- rbind(anc.dt[year_id >= 2000], anc.dt[year_id < 2000 & !is.na(offset),], pre_2000)
        }
  
        
        temp.dat <- anc.dt
        if(loc != 'SEN'){
          temp.dat[,subpop := subpop2]
          
        }
    
        ##Note that the temp.dat should not exceed the number of rows that were originally in the eppd objsect
        all.anc <- rbind(all.anc,temp.dat,fill = TRUE)
        print(subpop2)
        
      
      
    }
  
    if(any(colnames(all.anc) == 'clinic')){
      setnames(all.anc, 'clinic', 'site')
      
    }
     nrow(all.anc) == nrow(eppd$ancsitedat)
     any(is.na(all.anc[,'offset']))

     

     all.anc[is.na(offset), offset := 0]
     all.anc[offset > 0.15, offset := 0.15]
     all.anc[offset < -0.15, offset := -0.15]
     all.anc[,c('site_pred','adm0_mean','adm0_lower','adm0_upper','high_risk') := NULL]
     
     ##This corrects a mistake in the file generation - should be corrected in the initial generation
     if(loc=="NGA_25332"){
       all.anc[,high_risk := FALSE]
     }
      
     setnames(all.anc, 'year_id', 'year')
     eppd$ancsitedat <- unique(as.data.frame(all.anc[,.(site, subpop, year, used, prev, n, type, agegr, age, agspan, offset)]))

     
     attr(dt, "eppd") <- eppd

    return(dt)
}

geo_adj_old <- function(loc, dt, i, uncertainty) {
  # Make adjustments to ANC coming from PJNZ files ** add more **
  ## Prep EPP data
  # Choose subpopulation for substitution
  print("using LBD Adjustment")
  
  ##Bring in the matched data - reading in as CSV rather then fread because the latter seems to add quotations when there are escape characters, which messes up the matching
  anc.dt.all <- read.csv(paste0('/share/hiv/data/lbd_anc/', loc, '_ANC_matched.csv')) %>% data.table()
  anc.dt.all  <- anc.dt.all[,c( "clinic","year_id","mean","site_pred","adm0_mean","adm0_lower", "adm0_upper","subpop","high_risk")]
  setnames(anc.dt.all,c("clinic","year_id"),c("site","year"))
  eppd <- attr(dt, "eppd")
  
  if(uncertainty){
    #Choose 1 from 1000 draws of uncertainty using adm0_mean bounds
    for(row in 1:nrow(anc.dt.all)){
      if(!is.na(anc.dt.all[row,adm0_mean])){
        set.seed(i)
        lower <- anc.dt.all[row,adm0_lower]
        upper <- anc.dt.all[row,adm0_upper]
        replace <- sample(runif(1000,lower,upper),1)
        anc.dt.all[row,adm0_mean := replace]
        
      } else {
        next
      }
    }
  }
  
  
  ##Generate the local:national offest term as the probit difference between national and predicted site prevalence - by subpopulation to avoid duplicates
  all.anc <- list()
  for(subpop2 in unique(anc.dt.all$subpop)){
    
    if(subpop2 %in% unique(eppd$ancsitedat$subpop)){
      anc.dt <- anc.dt.all[subpop == subpop2] 
      site.dat <- eppd$ancsitedat[eppd$ancsitedat$subpop==subpop2,] %>% data.table()
    } else {
      anc.dt <- anc.dt.all
      site.dat <- eppd$ancsitedat %>% data.table() 

    }
    setnames(site.dat, 'year_id', 'year')
    site.dat[,offset := NULL]
    anc.dt[,subpop := NULL]
    
    merge_on <- intersect(colnames(anc.dt[,.(site,year,site_pred,high_risk)]), colnames(site.dat))
    
    anc.dt  <- anc.dt[,offset := qnorm(adm0_mean)-qnorm(site_pred)]
    if(loc == 'GAB'){
      site.x <- gsub(" (%)", '', as.character(anc.dt[,site]))
      site.x <- gsub(" \\(%)", '', site.x)
      anc.dt[,site := site.x]
      
      
    }
    #Copy year 2000 or otherwise earliest year to fill in  early years where GBD has data but LBD does not  
    post.2000 <- anc.dt[year >=2000]
    min.dt <- post.2000[year == min(year),.(offset), by = 'site']
    
    if(subpop2 %in% unique(eppd$ancsitedat$subpop)){
      temp.dat <- merge(site.dat,anc.dt[,.(site,subpop,year,site_pred,adm0_mean,adm0_lower,adm0_upper,offset,high_risk)], by=c("site","subpop","year"),all.x=TRUE)
    } else {
      anc.dt <- anc.dt[,.(site, year, offset)]
      temp.dat <- merge(site.dat,anc.dt, by=c('site', 'year'),all.x=TRUE)
      
    }
    #Duplicate issue with 'pseudo sites' in Mozambique
    if(loc == "MOZ"){
      min.dt <- unique(min.dt)
    }
    
    merge.dt <- copy(temp.dat[year < 2000 & !is.na(prev)])[,offset := NULL]
    merge.dt <- merge(merge.dt, min.dt, by = 'site')
    
    temp.dat <- temp.dat[year >= 2000 & !is.na(prev)]
    temp.dat <- rbind(temp.dat, merge.dt, use.names = T)
    
    all.anc <- rbind(all.anc,temp.dat)
    
    
  }
  
  nrow(all.anc) == nrow(eppd$ancsitedat)
  all.anc[is.na(offset), offset := 0]
  all.anc[offset > 0.15, offset := 0.15]
  all.anc[offset < -0.15, offset := -0.15]
  all.anc[is.na(high_risk),high_risk := FALSE]
  
  ##This corrects a mistake in the file generation - should be corrected in the initial generation
  if(loc=="NGA_25332"){
    all.anc[,high_risk := FALSE]
  }
  
  all.anc <- all.anc[!high_risk==TRUE]
  all.anc[,c('site_pred','adm0_mean','adm0_lower','adm0_upper','high_risk') := NULL]
  
  all.anc <- as.data.frame(all.anc)
  
  
  eppd$ancsitedat <- all.anc
  
  attr(dt, "eppd") <- eppd
  
  return(dt)
}

  
  
  sub.art <- function(dt, loc, use.recent.unaids = FALSE, pct = F) {
       temp.loc <- loc
      

    art.dt <- fread(art.dt)
    
    if(pct){
      if(grepl("ZAF",temp.loc) ){
        print("Using Tembisa for ZAF")
        art.dt = fread(tem_art)
        art.dt[,ART_cov_num := 0]
        
      }
      
    }else{
      if(grepl("ZAF",temp.loc) ){
        print("Using Tembisa for ZAF")
        art.dt = fread(tem_art)
        art.dt[,ART_cov_pct := 0]
        
      }
      
    }

    art.dt[is.na(art.dt)] <- 0
    ##Need this to be logical later
    art.dt[, type := ifelse(ART_cov_pct > 0, TRUE, FALSE)]	
    
    #years <- epp.input$epp.art$year
    years <- as.integer(attr(attr(dt,"specfp")$art15plus_isperc,"dimnames")$year)
    years <- c(1970:stop.year)
    
    if(max(years) > max(art.dt$year)) {
      max.dt <- copy(art.dt[year == max(year)])
      missing.years <- setdiff(years, art.dt$year)
      add.dt <- rbindlist(lapply(missing.years, function(cyear) {
        copy.dt <- copy(max.dt)
        copy.dt[, year := cyear]
      }))
      art.dt <- rbind(art.dt, add.dt)
    }
    
    art.dt <- unique(art.dt)
    if(grepl('ZAF', temp.loc)){
      if(dim( attr(dt, 'specfp')$art15plus_isperc)[2] < attr(dt, 'specfp')$SIM_YEARS){
        diff <- dim( attr(dt, 'specfp')$art15plus_isperc)[2] - attr(dt, 'specfp')$SIM_YEARS
        while(diff != 0){
          attr(dt, 'specfp')$art15plus_isperc <-  abind::abind( attr(dt, 'specfp')$art15plus_isperc,  attr(dt, 'specfp')$art15plus_isperc[,ncol( attr(dt, 'specfp')$art15plus_isperc)])
          diff <- dim( attr(dt, 'specfp')$art15plus_isperc)[2] - attr(dt, 'specfp')$SIM_YEARS
          
        }
      }
      if(dim( attr(dt, 'specfp')$art15plus_num)[2] < attr(dt, 'specfp')$SIM_YEARS){
        diff <- dim( attr(dt, 'specfp')$art15plus_num)[2] - attr(dt, 'specfp')$SIM_YEARS
        while(diff != 0){
          attr(dt, 'specfp')$art15plus_num <-  abind::abind( attr(dt, 'specfp')$art15plus_num,  attr(dt, 'specfp')$art15plus_num[,ncol( attr(dt, 'specfp')$art15plus_num) - 1])
          diff <- dim( attr(dt, 'specfp')$art15plus_num)[2] - attr(dt, 'specfp')$SIM_YEARS
          
        }
      }
    }
    attr(dt,"specfp")$art15plus_isperc <- attr(dt,"specfp")$art15plus_isperc[,1:length(years)]
    attr(dt,"specfp")$art15plus_num <- attr(dt,"specfp")$art15plus_num[,1:length(years)]
    
    
    attr(dt,"specfp")$art15plus_isperc[1,] <- art.dt[year %in% years & sex == 1, type]
    attr(dt,"specfp")$art15plus_isperc[2,] <- art.dt[year %in% years & sex == 2, type]
    
    art.dt[, ART_cov_val := ifelse(ART_cov_pct > 0, ART_cov_pct, ART_cov_num)]
    attr(dt,"specfp")$art15plus_num[1,] <- art.dt[year %in% years & sex == 1, ART_cov_val]
    attr(dt,"specfp")$art15plus_num[2,] <- art.dt[year %in% years & sex == 2, ART_cov_val]
    
    return(dt)
  }
  
  sub.sexincrr <- function(dt, loc, i){
    if(!grepl('IND', loc)){
      rr.dt <- fread(paste0('FILEPATH/', substr(loc, 1, 3), '.csv'))
    }else{
      rr.dt <- fread(paste0('FILEPATH/', loc, '.csv'))
    }
    rr <- rr.dt[draw == i, FtoM_inc_ratio]
    final.rr <- rr[length(rr)]
    if(length(rr) < length(start.year:stop.year)){
      rr <- c(rr, rep(final.rr, length(start.year:stop.year) - length(rr)))
    }else{
      dt_length <-length(start.year:stop.year)
      rr <- rr[(length(rr) - dt_length):length(rr)]
    }
    names(rr) <- start.year:stop.year
    attr(dt, 'specfp')$incrr_sex <- rr
    return(dt)
  }
  
  #We use a different prior for MDG or else the curve goes to 0. It is worth rethinking this strategy for all no-survey locations.
  #We use a different prior for no-survey locs (except PNG which has enough ANC data)
  sub.anc.prior <- function(dt,loc){

   if(loc %in%  c("SDN","SSD","SOM","GNB","MDG","PNG", "COM")){
      ancbias.pr.mean <<- 0.15
      ancbias.pr.sd <<- 0.001
    }else {
      ancbias.pr.mean <<- 0.15
      ancbias.pr.sd <<- 1
    }
    return(dt)
  }
  
  
  
