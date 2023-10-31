

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"J:/","/home/j/")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "H:/", "/homes/"), user, "/hiv_gbd2019/")

## Packages
library(data.table); library(haven); library(parallel); library(reshape); library(ggplot2); library(dplyr)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) > 0) {
  loc <- args[1]
  run.name <- args[2]
  ncores <- as.numeric(args[3])
  ens.version <- args[4]
} else {

  loc <- "PAK_53621"
  run.name <- "190630_rhino_combined"
  ncores <- 2
  ens.version <- 277
}


#ncores <- 40
id.vars <- c("year_id", "age_group_id", "run_num", "sex_id", "location_id")
value.vars <- c('hiv_deaths', 'pop_hiv', 'new_hiv', 'pop_art', 'suscept_pop', 'non_hiv_deaths', 'spec_deaths', 'population', 'scaled_new_hiv')
all.vars <- c(id.vars, value.vars)

### Functions
library(mortdb, lib = "/home/j/WORK/02_mortality/shared/r")
source(paste0(root, "/Project/Mortality/shared/functions/get_locations.r"))

source(paste0("/share/cc_resources/libraries/current/r/get_population.R"))

find.children <- function(loc) {
  parent.loc <- loc.table[ihme_loc_id==loc, location_id]
  most.detailed <- loc.table[location_id==parent.loc, most_detailed]
  if(loc.table[ihme_loc_id == loc, level] < 3){
    most.detailed <- 0
  }
  if(most.detailed==0){
    parent.level <- loc.table[location_id==parent.loc, level]
    child.list <- loc.table[parent_id==parent.loc & location_id!=parent_id & level==parent.level + 1, ihme_loc_id]
  } else {
    child.list <- loc.table[location_id==parent.loc, ihme_loc_id]
  }
  return(child.list)
}

### Paths
spec.dir <- paste0("/ihme/hiv/spectrum_prepped/art_draws/", run.name, "/")
scalar.dir <- paste0("/ihme/hiv/post_spectrum_scal/", run.name, "/")
dir.create(scalar.dir, recursive = T, showWarnings = F)

ens.dir <- paste0("/share/mortality/hiv_adjust/", ens.version, "/hiv_output/")
out.dir <- paste0("/ihme/hiv/spectrum_prepped/aggregates/", run.name, "/")
dir.create(out.dir, recursive = T, showWarnings = F)

### Tables
loc.table <- data.table(get_locations(hiv_metadata = T, level = 'all', gbd_year=2019))

location.id <- loc.table[ihme_loc_id == loc, location_id]
region.scalars <- fread('/ihme/hiv/spectrum_prepped/region_scalars.csv')

### Code
children <- find.children(loc)


combined.dt <- rbindlist(
  mclapply(children, 
           function(child) {
             in.path <- paste0(out.dir, child, ".csv")
             if(!file.exists(in.path) | child == loc) {
               # Grab population
               loc.id <- loc.table[ihme_loc_id == child, location_id]
               gbd.pop <- get_population(decomp_step="step4", 
                                         year_id = -1, location_id = loc.id, 
                                         sex_id = -1, age_group_id = -1, gbd_round_id = 6)
               gbd.pop[,location_id := as.integer(location_id)]
               gbd.pop[, age_group_id := as.integer(age_group_id)]
               gbd.pop[, sex_id := as.integer(sex_id)]
               gbd.pop[, year_id := as.integer(year_id)]
               # Read in Spectrum data, drop deaths, and add up pop_hiv
               spec.dt <- read.csv(paste0(spec.dir, child, "_ART_data.csv")) %>% data.table()
               spec.dt[, location_id := loc.id]
               setnames(spec.dt, "hiv_deaths", "spec_deaths")
               spec.dt[, pop_hiv := pop_lt200 + pop_200to350 + pop_gt350 + pop_art]
               spec.dt[, c("pop_neg", "pop_lt200", "pop_200to350", "pop_gt350") := NULL]
               
               # Merge and convert rates to counts
               pop.dt <- merge(spec.dt, gbd.pop, by = c("location_id", "age_group_id", "sex_id", "year_id"))
               iter.vars <- c("new_hiv", "suscept_pop", "pop_hiv", "pop_art", "spec_deaths", "non_hiv_deaths")
               for(var in iter.vars) {
                 pop.dt[, (var) := get(var) * population]
               }
               
               # Read in Ensemble data
               ens.data <- read.csv(paste0(ens.dir, "hiv_death_", child, ".csv")) %>% data.table()
               setnames(ens.data, "sim", "run_num")
               ens.data[, run_num := run_num + 1]
               
               all.dt <- merge(pop.dt, ens.data, by = c("location_id", "age_group_id", "sex_id", "year_id", "run_num"))
               all.dt[, scaled_new_hiv := new_hiv]
               all.dt <- all.dt[, all.vars, with = F]
             } else {
               all.dt <- fread(in.path)[, hiv_background := NULL]
               if(!("scaled_new_hiv" %in% names(all.dt))) {
                 all.dt[, scaled_new_hiv := new_hiv]
               }
               all.dt <- all.dt[, all.vars, with = F]
               
             }
           }, 
           mc.cores = ncores
  ),
  use.names = T)


combined.dt[, location_id := location.id]
collapsed.dt <- combined.dt[, lapply(.SD, sum), by = id.vars, .SDcols = value.vars]
collapsed.dt[, hiv_background := pop_hiv * (non_hiv_deaths / population)]

### prep case report data
if(loc %in% c('PAK_53620', 'PAK_53621')){
  high_income.inc.dt <- fread("/ihme/hiv/data/case_notifications/data_49.csv")
  
}else{
  high_income.inc.dt <- fread("/ihme/hiv/data/case_notifications/combined_high_income_cases_decomp_FINAL.csv")
  
}
#high_income.inc.dt <- fread("/ihme/hiv/results_comparison/comparison_data/high_income_incidence_extractions/combined_high_income_cases.csv")
high_income.inc.dt <- high_income.inc.dt[year!="cumulative"]
high_income.inc.dt <- high_income.inc.dt[!(grepl(" or earlier", year)| grepl("1985-", year))]
high_income.inc.dt <- high_income.inc.dt[ihme_loc_id==loc]
high_income.inc.dt[, c("year", "cases"):= .(as.numeric(year), as.numeric(cases))]
high_income.inc.dt[, age_group_id := ifelse(is.na(age_group_id), 22, age_group_id)]

setnames(high_income.inc.dt, c("year"), c("year_id"))

## Outlier
high_income.inc.dt <- high_income.inc.dt[!(ihme_loc_id == 'NLD' & year_id == 2016)]
#Only include continuous years
high_income.inc.dt <- high_income.inc.dt[!(ihme_loc_id=="SGP" & year_id < 2007)]

## FOr Taiwan, we combined both sexes to be able to use more years and outlier early years
if(loc == "TWN"){
  high_income.inc.dt[,cases_sum := sum(cases), by = c("year_id")]
  high_income.inc.dt[,sex_id := 3]
  high_income.inc.dt = high_income.inc.dt[!year_id %in% c(2008,2009)]
  high_income.inc.dt[,cases := cases_sum]
  high_income.inc.dt = unique(high_income.inc.dt)
}

#Check for duplicate year-sex combos  and take the higher value
if(nrow(high_income.inc.dt) > nrow(unique(high_income.inc.dt[,c("year_id", "sex_id","age_group_id")]))){
  ##Write out the duplicats
  idx <- duplicated(high_income.inc.dt[,c("year_id", "sex_id")]) 
  write.csv(high_income.inc.dt[idx, ],paste0("/share/hiv/data/case_notifications/duplicates/",loc,".csv"),row.names =FALSE)
  
  high_income.inc.dt[,new.cases := max(cases),by=c("year_id", "sex_id")]
  high_income.inc.dt <- high_income.inc.dt[cases==new.cases]
  
  #Randomly disgard remaining duplicates, which would occur if the same age-sex combo had the same value
  idx <- duplicated(high_income.inc.dt[,c("year_id", "sex_id")]) 
  high_income.inc.dt <- high_income.inc.dt[!idx, ] 
  
}



# Calculate adjusted incidence
if(loc %in% loc.table[group == "1A" & most_detailed == 1, ihme_loc_id]) {
  sum.dt <- collapsed.dt[, lapply(.SD, sum), by = .(year_id, run_num, sex_id), .SDcols = c("pop_hiv", "new_hiv", "hiv_deaths", "hiv_background", "spec_deaths")]
  mean.dt <- sum.dt[,lapply(.SD, mean), by=.(year_id, sex_id), .SDcols = c("pop_hiv", "new_hiv", "hiv_deaths", "hiv_background", "spec_deaths")]
  mean.dt[, new_hiv_calc := new_hiv + hiv_deaths - spec_deaths]
  mean.dt[, inc_scalar := ifelse(new_hiv == 0, 0, new_hiv_calc / new_hiv)]
  mean.dt[inc_scalar < 0, inc_scalar := 0]
  
  ## save the scalar 
  scalar.output <- mean.dt[,.(year_id, sex_id, inc_scalar)]
  scalar.output[,ihme_loc_id:= loc]
  write.csv(scalar.output, paste0(scalar.dir, loc, "_scalar.csv"), row.names=F)
  ##
  scale.dt <- merge(collapsed.dt, mean.dt[, .(year_id, sex_id, inc_scalar)], by = c("year_id", "sex_id"))
  scale.dt[, scaled_new_hiv := new_hiv * inc_scalar]
  scale.dt[, inc_scalar := NULL]	
  

  #2019 exception for Taiwan because of collaborator feedback
} else if ((loc %in% unique(high_income.inc.dt$ihme_loc_id) & loc.table[ihme_loc_id==loc, super_region_name] == "High-income" & loc.table[ihme_loc_id==loc, level]==3) |
           loc == "TWN"){

  if (length(unique(high_income.inc.dt$source))>1){
    if (loc %in% c("FRA")) {
      high_income.inc.dt <- high_income.inc.dt[source=="ECDC"] 
    } else {
      high_income.inc.dt <- high_income.inc.dt[source!="ECDC"] }
  }
  
  if(all(c(1, 2) %in% unique(high_income.inc.dt$sex_id))) {
    high_income.inc.dt <- copy(high_income.inc.dt[sex_id %in% c(1,2)])
    both <- F
  } else {
    high_income.inc.dt <- copy(high_income.inc.dt[sex_id == 3])
    both <- T
  }
  ### prep mean level spectrum data
  ## all age + both sex
  if(both) {
    sum.dt <- collapsed.dt[, lapply(.SD, sum), by = .(year_id, run_num), .SDcols = "new_hiv"]
    sum.dt[, sex_id := 3]
  } else {
    sum.dt <- collapsed.dt[, lapply(.SD, sum), by = .(year_id, run_num, sex_id), .SDcols = "new_hiv"]			
  }
  mean.dt <- sum.dt[,lapply(.SD, mean), by=c("sex_id", "year_id")]
  mean.dt[,c("age_group_id", "run_num"):= .(22, NULL)]
  ## combine data
  combined.dt <- merge(mean.dt, high_income.inc.dt[,.(year_id, sex_id, age_group_id, cases)], by=c("year_id", "sex_id", "age_group_id"), all.x=T)
  ## add lag in the case report data
  combined.dt[, cases:=shift(cases, type="lead", 5), by = .(sex_id)]
  ## calculate scalar
  combined.dt[, inc_scalar:= cases/new_hiv]

  if(both) {
    male.dt <- copy(combined.dt)[, sex_id := 1]
    female.dt <- copy(combined.dt)[, sex_id := 2]
    combined.dt <- rbind(male.dt, female.dt)
  }
  scalar.dt <- copy(combined.dt)[,.(year_id, sex_id, inc_scalar)]
  last.year <- max(combined.dt[!is.na(cases), year_id])
  first.year <- min(combined.dt[!is.na(cases), year_id])
  for(sex in 1:2) {
    last.scalar <- scalar.dt[year_id == last.year & sex_id == sex, inc_scalar]
    scalar.dt[year_id > last.year & sex_id == sex, inc_scalar := last.scalar]
    
    first.scalar <- scalar.dt[year_id == first.year & sex_id == sex, inc_scalar]
    scalar.dt[year_id < first.year & sex_id == sex, inc_scalar := ifelse(loc == 'NLD', 1, first.scalar)]
  }
  scalar.dt[is.na(inc_scalar), inc_scalar:=1]
  
  scalar.dt[inc_scalar < 1, inc_scalar:=1] ## only scalar up
  ## save the scalar 
  scalar.output <- copy(scalar.dt)
  scalar.output[,ihme_loc_id:= loc]
  write.csv(scalar.output, paste0(scalar.dir, loc, "_scalar.csv"), row.names=F)
  #####
  scale.dt <- merge(collapsed.dt, scalar.dt, by=c("year_id", "sex_id"), all.x=T)
  scale.dt[, scaled_new_hiv:=new_hiv*inc_scalar]
  
  all.locs <- grep(paste0(loc, "_"), loc.table$ihme_loc_id, value = T)
  if(length(all.locs) > 0 & length(unique(scale.dt$inc_scalar)) != 1) {
    mclapply(all.locs, function(child) {
      child.dt <- fread(paste0(out.dir, child, ".csv"))
      merge.dt <- merge(child.dt, scale.dt[, .(year_id, age_group_id, run_num, sex_id, inc_scalar)], by = c("year_id", "age_group_id", "run_num", "sex_id"))
      merge.dt[, scaled_new_hiv := inc_scalar * new_hiv]
      merge.dt[, inc_scalar := NULL]
      write.csv(merge.dt, paste0(out.dir, child, ".csv"), row.names = F)
    }, mc.cores = ncores)			
  }
  scale.dt[, inc_scalar := NULL]
  
  #Exception to bring in new case reports for Pakistan child epidemic 
} else if(loc %in% c("PAK_53621","PAK_53620")){

## create draws and bind to data
  create_draws = high_income.inc.dt[,.(year_id, sex_id, age_group_id, cases)]
  draws = apply(create_draws, 1, function(x) abs(rnorm(1000,x[4],2)))
  create_draws$variable = paste0("V",1:13)
  draws = as.data.frame(draws)
  draws = melt(draws)
  create_draws = merge(create_draws, draws, allow.cartesian = TRUE)
  create_draws[,run_num := rep(1:1000,13)]
  create_draws[,c('variable','id') := NULL]

  ## bind back to data
  scale.dt <- merge(collapsed.dt,  create_draws[,.(year_id,sex_id,age_group_id,run_num,value)], by=c("year_id", "sex_id","age_group_id","run_num"),all.x=TRUE)
  scale.dt <- scale.dt[,scaled_new_hiv := ifelse(!is.na(value) & value > new_hiv, value, new_hiv)]
  scale.dt[,value := NULL]

}  else {
  scale.dt <- copy(collapsed.dt)
}


# Apply regional scalars 
scale.vars <- setdiff(c(value.vars, "scaled_new_hiv"), "population")
if(location.id %in% unique(region.scalars$location_id)) {
  ## Extend region scalars to most recent year, if necessary
  if(!max(scale.dt$year_id) %in%  unique(region.scalars$year_id)){
    
    region.scalars.ext <- data.table()
    
    for(yeardiff in seq(max(region.scalars$year_id)+1,max(scale.dt$year_id))){
      region.scalars.ext.base <- region.scalars[year_id == max(region.scalars$year_id)]
      extension <- region.scalars.ext.base[, year_id := yeardiff]
      region.scalars.ext <- rbind(region.scalars.ext,extension)
    }
    
    region.scalars <- rbind(region.scalars, region.scalars.ext, use.names = T)
  }
  
  merged.dt <- merge(scale.dt, region.scalars[,.(location_id, year_id, scaling_factor = mean)], by = c("location_id", "year_id"), all.x = T)
  for(var in scale.vars) {
    merged.dt[, (var) := get(var) * scaling_factor]
  }
  merged.dt[, scaling_factor := NULL]
  out.dt <- copy(merged.dt)
} else {
  out.dt <- copy(scale.dt)
}

write.csv(out.dt, paste0(out.dir, loc, ".csv"), row.names = F)




### End




























