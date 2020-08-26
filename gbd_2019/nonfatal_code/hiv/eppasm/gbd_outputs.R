#' Model outputs for GBD
#'
#' @param mod simulation model output
#' @param fp model fixed parameters
#'
#' @return A data.frame
#'
#' @examples
#' library(eppasm)
#'
#' pjnz <- system.file("extdata/testpjnz", "Botswana2018.PJNZ", package="eppasm")
#' fp <- prepare_directincid(pjnz)
#' mod <- simmod(fp, "R")
#'
#' gbdout <- get_gbd_outputs(mod, fp)
#' write.csv(gbdout, "gbd-output-run1.csv", row.names=FALSE)
#'
#' @export

gbd_sim_mod <-  function(fit, rwproj=fit$fp$eppmod == "rspline", VERSION = 'C'){
  ## We only need 1 draw, so let's save time and subset to that now
  rand.draw <- round(runif(1, min = 1, max = 3000))
  if(!(exists('group', where = fit$fp) & fit$fp$group == '2')){
    fit$param <- lapply(seq_len(nrow(fit$resample)), function(ii) fnCreateParam(fit$resample[ii,], fit$fp))
    
    if(rwproj){
      if(exists("eppmod", where=fit$fp) && fit$fp$eppmod == "rtrend")
        stop("Random-walk projection is only used with r-spline model")
      
      dt <- if(inherits(fit$fp, "eppfp")) fit$fp$dt else 1.0/fit$fp$ss$hiv_steps_per_year
      
      lastdata.idx <- as.integer(max(fit$likdat$ancsite.dat$df$yidx,
                                     fit$likdat$hhs.dat$yidx,
                                     fit$likdat$ancrtcens.dat$yidx,
                                     fit$likdat$hhsincid.dat$idx,
                                     fit$likdat$sibmx.dat$idx))
      
      fit$rvec.spline <- sapply(fit$param, "[[", "rvec")
      firstidx <- which(fit$fp$proj.steps == fit$fp$tsEpidemicStart)
      lastidx <- (lastdata.idx-1)/dt+1
      
      ## replace rvec with random-walk simulated rvec
      fit$param <- lapply(fit$param, function(par){par$rvec <- epp:::sim_rvec_rwproj(par$rvec, firstidx, lastidx, dt); par})
    }
    fit$fp$incrr_age <- fit$fp$incrr_age[,,1:fit$fp$SIM_YEARS]
    fp.list <- lapply(fit$param, function(par) update(fit$fp, list=par, keep.attr = FALSE))
    fp.draw <- fp.list[[rand.draw]]
  }else{
    fp.draw <- fit$fp
    theta <- fit$resample[rand.draw,]
    fp.draw$cd4_mort_adjust <- exp(theta[1])
    incrr_nparam <- getnparam_incrr(fp.draw)
    paramcurr <- 1
    if(incrr_nparam > 0){
      fp.draw$incrr_sex = fp.draw$incrr_sex[1:fp.draw$SIM_YEARS]
      fp.draw$incrr_age = fp.draw$incrr_age[,,1:fp.draw$SIM_YEARS]
      param <- list()
      param <- transf_incrr(theta[paramcurr + 1:incrr_nparam], param, fp.draw)
      paramcurr <- paramcurr + incrr_nparam
      fp.draw <- update(fp.draw, list = param)
    }
    nparam_eppmod <- get_nparam_eppmod(fp.draw)
    nparam_diagn <- 0
    fp.draw <- create_param_csavr(theta[paramcurr + 1:(nparam_eppmod + nparam_diagn)], fp.draw)
    
    
  }
  mod <- simmod(fp.draw, VERSION = VERSION)
  attr(mod, 'theta') <- fit$resample[rand.draw,]
  return(mod)
}

get_gbd_outputs <- function(mod, fp, paediatric = FALSE) {

  mod <- mod_dimnames(mod, fp$ss, paediatric)
  hp1 <- hivpop_singleage(mod, fp$ss)
  
  pop <- as.data.frame.table(apply(mod, c(1, 2, 4), sum), responseName = "pop")
  hiv_deaths <- as.data.frame.table(attr(mod, "hivdeaths"),
                                    responseName = "hiv_deaths")
  non_hiv_deaths <- as.data.frame.table(attr(mod, "natdeaths"),
                                        responseName = "non_hiv_deaths")
  new_hiv <- as.data.frame.table(attr(mod, "infections"),
                                 responseName = "new_hiv")
  pop_neg <- as.data.frame.table(mod[,,fp$ss$hivn.idx,])
  setnames(pop_neg, 'Freq', 'pop_neg')
  
  total_births <- as.data.frame.table(get_births(mod, fp), responseName = "total_births")
  total_births$sex <- "female"
  pregprev <- as.data.frame.table(get_pregprev(mod, fp, hp1), responseName = "pregprev")
  pregprev$sex <- "female"
  
  pop_art <- as.data.frame.table(colSums(hp1$artpop1,,2), responseName = "pop_art")
  setnames(pop_art, 'agegr', 'age')

  hivpop_daly <- as.data.frame.table(get_daly_hivpop(hp1$hivpop), responseName = "value")
  hivpop_daly <- data.table::dcast(hivpop_daly, ... ~ cd4daly)
  setnames(hivpop_daly, 'agegr', 'age')

  v <- pop
  v <- merge(v, hiv_deaths, all.x=TRUE)
  v <- merge(v, non_hiv_deaths, all.x=TRUE)
  v <- merge(v, new_hiv, all.x=TRUE)
  v <- merge(v, pop_neg, all.x=TRUE)
  v <- merge(v, total_births, all.x=TRUE)
  v <- merge(v, pregprev, all.x=TRUE)
  v$hiv_births <- v$total_births * v$pregprev  # number of births to HIV positive women
  v$pregprev <- NULL
  v$birth_prev <- 0                         # HIV prevalence among newborns
  v <- merge(v, pop_art, all.x=TRUE)
  v <- merge(v, hivpop_daly, all.x=TRUE)

  v <- data.table(v)
  for(var in c('total_births', 'hiv_births')){
    v[is.na(get(var)), (var) := 0.0]
  }
  
  v[,year := as.numeric(levels(year))[year]]
  v[,sex := as.character(sex)]
  v[,age := as.numeric(levels(age))[age]]
  if(paediatric){
    hivdeathsu15 <- de_factor(as.data.frame.table(attr(mod, 'hivdeathsu15'), responseName = 'hiv_deaths'))
    deathsu15 <- de_factor(as.data.frame.table(attr(mod, 'deathsu15'), responseName = 'non_hiv_deaths'))
    
    artpopu5 <- as.data.frame.table(apply(attr(mod, 'artpopu5'), 2:4, sum), responseName = 'pop_art')
    artpopu15 <- as.data.frame.table(apply(attr(mod, 'artpopu15'), 2:4, sum), responseName = 'pop_art')
    artpop <- rbindlist(lapply(list(artpopu5, artpopu15), de_factor ))
    
    ## divide into approx adult cd4 groups
    ## taken from spectrum
    hivpop <- array(0, c(3, 15, 2, fp$ss$PROJ_YEARS))
    hivpop[3,1:5,,] <- apply(attr(mod, 'hivpopu5')[6:7,,,], 2:4, sum) + (0.111 * attr(mod, 'hivpopu5')[5,,,])
    hivpop[2,1:5,,] <- (0.8888 * attr(mod, 'hivpopu5')[5,,,]) + (0.2307 * attr(mod, 'hivpopu5')[4,,,])
    hivpop[1,1:5,,] <- (0.7692 * attr(mod, 'hivpopu5')[4,,,]) + apply(attr(mod, 'hivpopu5')[1:3,,,], 2:4, sum)
    hivpop[3,6:15,,] <- attr(mod, 'hivpopu15')[6,,,]
    hivpop[2,6:15,,] <- attr(mod, 'hivpopu15')[5,,,]
    hivpop[1,6:15,,] <- apply(attr(mod, 'hivpopu15')[1:4,,,], 2:4, sum)
    dimnames(hivpop) <- list(cd4daly = c('pop_gt350', 'pop_200to350', 'pop_lt200'), age = 0:14, sex = c('male', 'female'), year = fp$ss$proj_start:(fp$ss$proj_start+fp$ss$PROJ_YEARS - 1))
    hivpop <- data.table(as.data.frame.table(hivpop, responseName = 'value'))
    hivpop <- de_factor(dcast.data.table(hivpop, age + sex + year ~ cd4daly, value.var = 'value'))
    
    popu5 <- as.data.frame.table(apply(attr(mod, 'popu5'), c(1, 2, 4), sum), responseName = 'pop')
    popu15 <- as.data.frame.table(apply(attr(mod, 'popu15'), c(1, 2, 4), sum), responseName = 'pop')
    pop <- rbindlist(lapply(list(popu5, popu15), de_factor ))
    popneg <- array(0, c(15, 2, fp$ss$PROJ_YEARS))
    popneg[1:5,,] <- attr(mod, 'popu5')[,,fp$ss$hivn.idx,]
    popneg[6:15,,] <- attr(mod, 'popu15')[,,fp$ss$hivn.idx,]
    dimnames(popneg )<- list(age = 0:14, sex = c('male', 'female'), year = fp$ss$proj_start:(fp$ss$proj_start+fp$ss$PROJ_YEARS - 1))
    popneg <- de_factor(as.data.frame.table(popneg, responseName = 'pop_neg'))
    
    inf <- de_factor(as.data.frame.table(attr(mod, 'infectionsu15'), responseName = 'new_hiv'))
    birthprev <- as.data.frame.table(attr(mod, 'birthprev'), responseName = 'birth_prev')
    birthprev$age <- 0
    birthprev <- data.table(rbind(birthprev, expand.grid(list(age = 1:14, sex = c('male', 'female'), year = fp$ss$proj_start:(fp$ss$proj_start+fp$ss$PROJ_YEARS - 1), birth_prev = 0))))
    birthprev[,year:=as.numeric(levels(year))[year] ]
    birthprev[,sex := as.character(sex)]
    
    vu15 <- merge(pop, hivdeathsu15)
    vu15 <- merge(vu15, deathsu15)
    vu15 <- merge(vu15, inf)
    vu15 <- merge(vu15, popneg)
    vu15$total_births <- 0
    vu15$hiv_births <- 0
    vu15 <- merge(vu15, birthprev)
    vu15 <- merge(vu15, artpop)
    vu15 <- merge(vu15, hivpop)
    v <- rbind(v, vu15, use.names = T)
    
    
  }

  v <- v[order(year, sex, age)]
  
  return(v)
}

## split under 1 into enn, lnn, pnn
get_under1_splits <- function(mod, fp){
  yrlbl <- get_proj_years(fp$ss)
  dimnames(attr(mod, 'under1incidence')) <- list(transmission = c('perinatal', 'BF0', 'BF6'), year = yrlbl)
  under1.inc <- data.table(as.data.frame.table(attr(mod, 'under1incidence'), responseName = 'new_infections'))
  prop.inc <- under1.inc[,total_infections := sum(new_infections), by ='year']
  prop.inc[, prop := ifelse(total_infections == 0, 0, new_infections/total_infections)]
  
  split.dt <- data.table(year = yrlbl)
  split.dt[, enn := prop.inc[transmission == 'perinatal', prop] + (prop.inc[transmission == 'BF0', prop] * (1/26))]
  ## split first 6 months of BF incidence
  split.dt[, lnn := prop.inc[transmission == 'BF0', prop] * (3/26)]
  split.dt[, pnn := (prop.inc[transmission == 'BF0', prop] * (22/26)) + prop.inc[transmission == 'BF6', prop]]
  return(split.dt)
}

## Quick convert factors
de_factor <- function(dt){
  dt <- data.table(dt)
  dt[,year := as.numeric(levels(year))[year]]
  dt[,sex := as.character(sex)]
  dt[,age := as.numeric(levels(age))[age]]
  return(dt)
}

#' Births by single age
#' 
get_births <- function(mod, fp){

  py <- fp$ss$PROJ_YEARS
  fertpop <- apply(mod[fp$ss$p.fert.idx, fp$ss$f.idx, , ] +
                   mod[fp$ss$p.fert.idx, fp$ss$f.idx, , c(1, 1:(py-1))],
                   c(1, 3), sum) / 2
  fertpop * fp$asfr
}


#' HIV prevalence among pregant women by single age
#' 
get_pregprev <- function(mod, fp, hp1){

  ss <- fp$ss
  py <- fp$ss$PROJ_YEARS
  expand_idx <- rep(fp$ss$h.fert.idx, fp$ss$h.ag.span[fp$ss$h.fert.idx])
  hivn_w <- mod[ss$p.fert.idx, ss$f.idx, ss$hivn.idx,  ]
  hivp_w <- colSums(hp1$hivpop[ , ss$p.fert.idx, ss$f.idx, ] * fp$frr_cd4[ , expand_idx, 1:py])
  art_w <- colSums(hp1$artpop[ , , ss$p.fert.idx, ss$f.idx, ] * fp$frr_art[ , , expand_idx, 1:py],,2)
  denom_w <- hivn_w + hivp_w + art_w
  pregprev_a <- 1 - (hivn_w + hivn_w[ , c(1, 1:(py-1))]) / (denom_w + denom_w[ , c(1, 1:(py-1))])

  pregprev_a
}


get_daly_hivpop <- function(hivpop1){
  idx <- rep(c("pop_gt350", "pop_200to350", "pop_lt200"), c(2, 2, 3))
  v <- apply(hivpop1, 2:4, fastmatch::ctapply, idx, sum)
  names(dimnames(v))[1] <- "cd4daly"
  v
}

## Under-5 splits using GBD 2017 methods

split_u5_gbd2017 <- function(dt){
  pop <- fread(paste0('/ihme/hiv/epp_input/gbd19/', run.name, "/population_splits/", loc, '.csv'))
  u5.pop <- pop[age_group_id <= 5]
  u5.pop[,pop_total := sum(population), by = c('sex_id', 'year_id')]
  u5.pop[,pop_prop := population/sum(population), by = c('sex_id', 'year_id')]
  ## props for incidence
  u5.pop[age_group_id != 5, pop_prop_inc := pop_prop]
  u5.pop[age_group_id == 5, pop_prop_inc := 0]  
  u5.pop[,pop_prop_inc := pop_prop_inc/sum(pop_prop_inc), by = c('year_id', 'sex_id')]
  ## props for death
  u5.pop[age_group_id!=2 & age_group_id != 3,pop_prop_death:=pop_prop]
  u5.pop[age_group_id==2 | age_group_id == 3,pop_prop_death:=0]
  u5.pop[,pop_prop_death:=pop_prop_death/sum(pop_prop_death), by=list(sex_id,year_id)]
  setnames(u5.pop, 'year_id', 'year')
  u5.pop[,sex := ifelse(sex_id == 1, 'male', 'female')]
  u5.pop <- u5.pop[,list(sex,year,age_group_id,pop_total,pop_prop,pop_prop_inc,pop_prop_death)]

  spec_u5 <- merge(dt,u5.pop,by=c("year","sex"), allow.cartesian=T)
  # Split all variables that can be split by population without age restrictions for 1-4 age cat
  pop_weight_all <- function(x) return(x*spec_u5[['pop_prop']])
  all_age_vars <- c("pop_neg","non_hiv_deaths","pop_lt200","pop_200to350","pop_gt350","pop_art","pop")
  spec_u5[,(all_age_vars) := lapply(.SD,pop_weight_all),.SDcols=all_age_vars] 
  
  # Split incidence into NN categories only, and enforce 0 for 1-4
  pop_weight_inc <- function(x) return(x*spec_u5[['pop_prop_inc']])
  inc_vars <- c("new_hiv")
  spec_u5[,(inc_vars) := lapply(.SD,pop_weight_inc),.SDcols=inc_vars] 
  
  # Split deaths into ENN and 1-4 age categories
  pop_weight_death <- function(x) return(x*spec_u5[['pop_prop_death']])
  death_vars <- c("hiv_deaths")
  spec_u5[,(death_vars) := lapply(.SD,pop_weight_death),.SDcols=death_vars] 
  
  spec_u5[,c("pop_total","pop_prop","pop_prop_inc","pop_prop_death", "age") :=NULL]
  spec_u5[age_group_id == 2, age := 'enn']
  spec_u5[age_group_id == 3, age := 'lnn']
  spec_u5[age_group_id == 4, age := 'pnn']
  spec_u5[age_group_id == 5, age := '1']
  spec_u5[, age_group_id := NULL]
  return(spec_u5)
  
}

split_u1 <- function(dt, loc, run.name){
  pop <- data.table("FILEPATH"))
  u1.pop <- pop[age_group_id < 5]
  u1.pop[,pop_total := sum(population), by = c('sex_id', 'year_id')]
  u1.pop[,pop_prop := population/sum(population), by = c('sex_id', 'year_id')]
  ## props for death
  u1.pop[age_group_id!=2 & age_group_id != 3,pop_prop_death:= 1]
  u1.pop[age_group_id==2 | age_group_id == 3,pop_prop_death:=0]
  setnames(u1.pop, 'year_id', 'year')
  u1.pop[,sex := ifelse(sex_id == 1, 'male', 'female')]
  u1.pop <- u1.pop[,list(sex,year,age_group_id,pop_total,pop_prop,pop_prop_death)]
  
  spec_u1 <- merge(dt,u1.pop,by=c("year","sex"), allow.cartesian=T)
  # Split all variables that can be split by population without age restrictions for 1-4 age cat
  pop_weight_all <- function(x) return(x*spec_u1[['pop_prop']])
  all_age_vars <- c("pop_neg","non_hiv_deaths","pop_lt200","pop_200to350","pop_gt350","pop_art","pop")
  spec_u1[,(all_age_vars) := lapply(.SD,pop_weight_all),.SDcols=all_age_vars] 
  
  # Split deaths into ENN and 1-4 age categories
  pop_weight_death <- function(x) return(x*spec_u1[['pop_prop_death']])
  death_vars <- c("hiv_deaths")
  spec_u1[,(death_vars) := lapply(.SD,pop_weight_death),.SDcols=death_vars] 
  
  spec_u1[,c("pop_total","pop_prop","pop_prop_death", "age") :=NULL]
  spec_u1[age_group_id == 2, age := 'enn']
  spec_u1[age_group_id == 3, age := 'lnn']
  spec_u1[age_group_id == 4, age := 'pnn']
  spec_u1[, age_group_id := NULL]
  
  ## pull in incidence proportions from eppasm
  split.dt <- fread("FILEPATH")
  split.dt <- melt(split.dt, id.vars = c('year', 'run_num'))
  setnames(split.dt, 'variable', 'age')
  spec_u1 <- merge(spec_u1, split.dt, by = c('year', 'run_num', 'age'))
  pop_weight_inc <- function(x) return(x*spec_u1[['value']])
  inc_vars <- c("new_hiv")
  spec_u1[,(inc_vars) := lapply(.SD,pop_weight_inc),.SDcols=inc_vars] 
  spec_u1[, value := NULL]
  
  return(spec_u1)
  
}

get_summary <- function(output, loc, run.name, paediatric = FALSE){
  ## create gbd age groups
  output[age >= 5,age_gbd :=  age - age%%5]
  output[age %in% 1:4, age_gbd := 1]
  output[age == 0, age_gbd := 0 ]
  output <- output[,.(pop = sum(pop), hiv_deaths = sum(hiv_deaths), non_hiv_deaths = sum(non_hiv_deaths), new_hiv = sum(new_hiv), pop_neg = sum(pop_neg),
                      total_births = sum(total_births), hiv_births = sum(hiv_births), birth_prev = sum(birth_prev),
                      pop_art = sum(pop_art), pop_gt350 = sum(pop_gt350), pop_200to350 = sum(pop_200to350), pop_lt200 = sum(pop_lt200)), by = c('age_gbd', 'sex', 'year', 'run_num')]
  setnames(output, 'age_gbd', 'age')
  if(paediatric){
    output.u1 <- split_u1(output[age == 0], loc, run.name)
    output <- output[age != 0]
    output <- rbind(output, output.u1, use.names = T)    
  }
  output[, hivpop := pop_art + pop_gt350 + pop_200to350 + pop_lt200]
  output[,c('pop_gt350', 'pop_200to350', 'pop_lt200', 'birth_prev', 'pop_neg', 'hiv_births', 'total_births') := NULL]
  output.count <- melt(output, id.vars = c('age', 'sex', 'year', 'pop', 'run_num'))
  

  age.map <- fread("FILEPATH")
  age.map[age_group_name_short == 'All', age_group_name_short := 'All']
  if(!paediatric){
    age.spec <- age.map[age_group_id %in% 8:21,.(age_group_id, age = age_group_name_short)]
    age.spec[, age := as.integer(age)]
  }else{
    age.spec <- age.map[age_group_id %in% c(2:21),.(age_group_id, age = age_group_name_short)]
  }
  output.count <- merge(output.count, age.spec, by = 'age')
  output.count[, age := NULL]
  
  # Collapse to both sex
  both.sex.dt <- output.count[,.(value = sum(value), pop = sum(pop)), by = c('year', 'variable', 'age_group_id', 'run_num')]
  both.sex.dt[, sex := 'both']
  all.sex.dt <- rbind(output.count, both.sex.dt)
  
  # Collapse to all-ages and adults
  all.age.dt <- all.sex.dt[,.(value = sum(value), pop = sum(pop)), by = c('year', 'variable', 'sex','run_num')]
  all.age.dt[, age_group_id := 22]
  
  adult.dt <- all.sex.dt[age_group_id %in% 8:14, .(value = sum(value), pop = sum(pop)), by = c('year', 'variable', 'sex', 'run_num')]
  adult.dt[, age_group_id := 24]
  
  age.dt <- rbindlist(list(all.sex.dt, all.age.dt, adult.dt), use.names = T)
  
  output.rate <- copy(age.dt)
  ## Denominator for ART pop is HIV+ pop
  art.pop <- output.rate[variable == 'hivpop']
  art.pop[, pop := NULL]
  setnames(art.pop, 'value', 'pop')
  art.pop[, variable := 'pop_art']
  art.merge <- merge(art.pop, output.rate[variable == 'pop_art',.(age_group_id, sex, year, variable, value, run_num)], by = c('age_group_id', 'sex', 'year', 'variable', 'run_num'))
  output.rate <- output.rate[!variable == 'pop_art']
  output.rate <- rbind(output.rate, art.merge, use.names = T)
  output.rate[, rate := ifelse(pop == 0, 0, value/pop)]
  output.rate[, value := NULL]
  
  ## Bind together
  setnames(output.rate, 'rate', 'value')
  output.rate[, metric := 'Rate']
  age.dt[, metric := 'Count']
  out.dt <- rbind(output.rate, age.dt, use.names = T)
  out.dt[variable == 'hiv_deaths', variable := 'Deaths']
  out.dt[variable == 'pop_art', variable := 'ART']
  out.dt[variable == 'non_hiv_deaths', variable := 'Background']
  out.dt[variable == 'new_hiv', variable := 'Incidence']
  out.dt[variable == 'hivpop', variable := 'Prevalence']
  setnames(out.dt, 'variable', 'measure')
  age.map <- rbind(age.map[,.(age_group_id, age_group_name_short)], data.table(age_group_id = 24, age_group_name_short = '15 to 49'))
  out.dt <- merge(out.dt, age.map[,.(age_group_id, age = age_group_name_short)], by = 'age_group_id')
  out.dt <- out.dt[,.(mean = mean(value), lower = quantile(value, 0.025), upper = quantile(value, 0.975)), by = c('age_group_id', 'sex', 'year', 'measure', 'metric', 'age')]
  return(out.dt)
  }

## Get data from eppd object, save for future plotting
save_data <- function(loc, eppd, run.name){
  age.map <-  fread("FILEPATH")
  if(nrow(eppd$hhs) > 0){
    prevdata <- data.table(eppd$hhs)
    prevdata <- prevdata[,.(sex, agegr, type = 'point', model = 'Household Survey', indicator = 'Prevalence', mean = prev, upper = prev + (1.96 * se), lower = ifelse(prev - (1.96 * se) < 0, 0, prev - (1.96 * se)), year)]
    if(exists('agegr', where = prevdata)){
      prevdata.agg <- prevdata[agegr == '15-49']
      prevdata.agg[, age_group_id := 24]
      prevdata.agg[, age := '15-49']
      prevdata <- prevdata[!agegr == '15-49']
      prevdata[,age:=as.character(sapply(strsplit(agegr, "-"), "[[", 1))]
      prevdata <- merge(prevdata, age.map[,.(age = age_group_name_short, age_group_id)],  by = 'age')
      prevdata <- rbind(prevdata, prevdata.agg, use.names = T)
      prevdata[, agegr  := NULL]
    }
  }else{
    prevdata <- NULL
  }
  if(exists("ancsitedat",where=eppd)){
  ancdata <- data.table(eppd$ancsitedat)
  ancdata <- ancdata[type=="ancss",.(sex = 'female', age = agegr, type = 'point', model = 'ANC Site', indicator = 'Prevalence', mean = prev, upper = NA, lower = NA, year, age_group_id = 24)]
  } else {
    ancdata <- NULL
  }
  output <- rbind(prevdata, ancdata, use.names = T)
  output[, metric := 'Rate']
  output[, ihme_loc_id := loc]
  path <- "FILEPATH"
  dir.create("FILEPATH", recursive = TRUE, showWarnings = FALSE)
  write.csv(output, path, row.names = F)
  return(output)
}
