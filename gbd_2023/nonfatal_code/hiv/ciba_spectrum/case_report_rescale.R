################################################################################
## Purpose: 
## Date created: 
## Date modified:
## Author: USERNAME, USERNAME@uw.edu
## Run instructions: 
## Notes:
################################################################################

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- "ADRESS"
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0("ADRESS/hiv_gbd/")

## Packages
library(data.table); library(haven); library(parallel); library(reshape); library(ggplot2); library(dplyr)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) > 0) {
  loc <- args[1]
  run.name <- args[2]
  ncores <- as.numeric(args[3])
  gbd_year = args[6]
} else {
  loc <- "USA_533"
  run.name <- "240514_platypus"
  ncores <- 2
  gbd_year = 2023
}


#ncores <- 40
id.vars <- c("year_id", "age_group_id", "run_num", "sex_id", "location_id")
value.vars <- c('hiv_deaths', 'pop_hiv', 'new_hiv', 'pop_art', 'suscept_pop', 'non_hiv_deaths', 'population', 'scaled_new_hiv')
all.vars <- c(id.vars, value.vars)

### Functions
library(mortdb, lib ="FILEPATH")
source(paste0("FILEPATH/get_population.R"))

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
spec.dir <- paste0("FILEPATH/", run.name, "/")
scalar.dir <- paste0("FILEPATH/", run.name, "/") 
dir.create(scalar.dir, recursive = T, showWarnings = F)

out.dir <- paste0("FILEPATH/", run.name, "/1_28_2025/")
dir.create(out.dir, recursive = T, showWarnings = F)

### Tables
loc.table <- data.table(get_locations(hiv_metadata = T, level = 'all', gbd_year=2023))
location.id <- loc.table[ihme_loc_id == loc, location_id]

### Code
children <- find.children(loc)
if(length(children)==0){
  children <- loc
}
child = children
combined.dt <- rbindlist(
  mclapply(children, 
           function(child) {
             
             # Grab population
             loc.id <- loc.table[ihme_loc_id == child, location_id]
             gbd.pop <- get_population(release_id = 16, 
                                       year_id = -1, location_id = loc.id, 
                                       sex_id = -1, age_group_id = -1)
             gbd.pop[, location_id := as.integer(location_id)]
             gbd.pop[, age_group_id := as.integer(age_group_id)]
             gbd.pop[, sex_id := as.integer(sex_id)]
             gbd.pop[, year_id := as.integer(year_id)]
             
             # Read in Spectrum data, drop deaths, and add up pop_hiv
             spec.dt <- read.csv(paste0(spec.dir, child, "_ART_data.csv")) %>% data.table()
             spec.dt[, location_id := loc.id]
             spec.dt[, pop_hiv := pop_lt200 + pop_200to350 + pop_gt350 + pop_art]
             spec.dt[, c("pop_neg", "pop_lt200", "pop_200to350", "pop_gt350") := NULL]
             
             # Merge with population and convert rates to counts
             pop.dt <- merge(spec.dt, gbd.pop, by = c("location_id", "age_group_id", "sex_id", "year_id"))
             iter.vars <- c("new_hiv", "suscept_pop", "pop_hiv", "pop_art", "hiv_deaths", "non_hiv_deaths")
             for(var in iter.vars) {
               pop.dt[, (var) := get(var) * population]
             }
             all.dt <- pop.dt
             all.dt[, scaled_new_hiv := new_hiv]
             all.dt <- all.dt[, all.vars, with = F]
             all.dt <- all.dt[year_id>=1990]
             all.dt[, hiv_background := pop_hiv * (non_hiv_deaths / population)]
             
           }, 
           mc.cores = ncores
  ),
  use.names = T)

for (c.loc in unique(combined.dt$location_id)) {
  loc.name <- loc.table[location_id==loc, ihme_loc_id]
  write.csv(combined.dt[location_id==loc], paste0(out.dir, loc.name, ".csv"), row.names = F)
}
combined.dt[, location_id := location.id]
combined.dt <- combined.dt[year_id>=1990]
collapsed.dt <- combined.dt[,lapply(.SD, sum), by = id.vars, .SDcols = value.vars]
collapsed.dt[, hiv_background := pop_hiv * (non_hiv_deaths / population)]

### prep case report data
four_stars <- fread("FILEPATH/cod_decomp4_stars_2021.csv")
four_stars <- four_stars[stars >= 4]


high_income.inc.dt <- fread("FILEPATH/case_report_compilation.csv")
high_income.inc.dt <- high_income.inc.dt[year_start!="cumulative"]
high_income.inc.dt <- high_income.inc.dt[!(grepl(" or earlier", year_start)| grepl("1985-", year_start))]
high_income.inc.dt <- high_income.inc.dt[ihme_loc_id==loc]
high_income.inc.dt <- high_income.inc.dt[cases != '<96>',]
high_income.inc.dt <- high_income.inc.dt[cases != '\x96',]
high_income.inc.dt[, c("year_start", "cases"):= .(as.numeric(year_start), as.numeric(cases))]
high_income.inc.dt[, age_group_id := ifelse(is.na(age_group_id), 22, age_group_id)]
setnames(high_income.inc.dt, c("year_start"), c("year_id"))
high_income.inc.dt[,year_id := as.numeric(year_id)]

# Remove Pakistan & its subnationals 
## Exception to bring in new case reports for Pakistan child epidemic (numbers from collaborator Dr Mir)
## Already implemented in apply_age_splits.R
if(grepl("PAK", loc)){ high_income.inc.dt <- NULL }

# Calculate adjusted incidence for >=4 star locations with available case report
if (loc %in% unique(high_income.inc.dt$ihme_loc_id) & loc %in% unique(four_stars$ihme_loc_id) & loc.table[ihme_loc_id==loc, level]==3){
  ## adjust Group 2A and 2B locations using case report
  if (length(unique(high_income.inc.dt$source))>1){
    if (loc %in% c("FRA")) {
      high_income.inc.dt <- high_income.inc.dt[source=="ECDC"] 
    } else {
      if(!loc %in% c('RUS')){
        high_income.inc.dt <- high_income.inc.dt[source!="ECDC"] }
      
    }
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
    scalar.dt[year_id < first.year & sex_id == sex, inc_scalar := first.scalar]
  }
  scalar.dt[is.na(inc_scalar), inc_scalar:=1]
  scalar.dt[inc_scalar < 1, inc_scalar:=1] ## only scalar up
  ## save the scalar 
  scalar.output <- copy(scalar.dt)
  scalar.output[,ihme_loc_id:= loc]
  write.csv(scalar.output, paste0(scalar.dir, loc, "_scalar_case_report.csv"), row.names=F)
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
  write.csv(scale.dt, paste0(out.dir, loc, ".csv"), row.names = F)
}

### End



