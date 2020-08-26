### Setup
rm(list=ls())
windows <- Sys.info()[1][["sysname"]]=="Windows"
root <- ifelse(windows,"J:/","/home/j/")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- "FILEPATH"

### Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
  loc <- args[1]
  run.name <- args[2]
  spec.name <- args[3]
} else {
  loc <- "KEN_35650"
  run.name <- "190630_rhino2"
  spec.name <- "190630_rhino"
}
fill.draw <- T
fill.na <- T

### Paths
eppasm_dir <- paste0("FILEPATH", run.name, '/')
out_dir <- paste0("FILEPATH",spec.name)
dir.create(out_dir, recursive = T, showWarnings = F)
out_dir_death <- paste0("FILEPATH",spec.name)
dir.create(out_dir_death, recursive = T, showWarnings = F)
out_dir_birth <- paste0("FILEPATH",spec.name, '/')
dir.create(out_dir_birth, recursive = T, showWarnings = F)

### Functions
library(mortdb, lib = "/home/j/WORK/02_mortality/shared/r")
source(paste0("/share/cc_resources/libraries/current/r/get_population.R"))
setwd(paste0(ifelse(windows, "H:", paste0("/ihme/homes/", user)), "/eppasm/"))
devtools::load_all()
setwd(code.dir)
devtools::load_all()


## Libraries etc.
library(data.table); library(foreign); library(assertable)
age_map <- data.table(fread("FILEPATH"))
age_map <- age_map[(age_group_id>=2 & age_group_id <=21) ,list(age_group_id,age=age_group_name_short)]

## Create a map to scramble draws from Spectrum
locations <- data.table(get_locations(hiv_metadata = T, gbd_year=2019))
loc_id <- locations[ihme_loc_id==loc,location_id]




##################################################################################################################
## Import and Format Data


## Bring in EPPASM Draws

spec_draw <- data.table(fread(paste0(eppasm_dir,"/compiled/",loc,".csv"), blank.lines.skip = T))
spec_draw[age >= 5,age_gbd :=  age - age%%5]
spec_draw[age %in% 1:4, age_gbd := 1]
spec_draw[age == 0, age_gbd := 0 ]
spec_draw <- spec_draw[,.(pop = sum(pop), hiv_deaths = sum(hiv_deaths), non_hiv_deaths = sum(non_hiv_deaths), new_hiv = sum(new_hiv), pop_neg = sum(pop_neg),
                    total_births = sum(total_births), hiv_births = sum(hiv_births), birth_prev = sum(birth_prev),
                    pop_art = sum(pop_art), pop_gt350 = sum(pop_gt350), pop_200to350 = sum(pop_200to350), pop_lt200 = sum(pop_lt200)), by = c('age_gbd', 'sex', 'year', 'run_num')]
setnames(spec_draw, 'age_gbd', 'age')
output.u1 <- split_u1(spec_draw[age == 0], loc, run.name)
spec_draw <- spec_draw[age != 0]
spec_draw <- rbind(spec_draw, output.u1, use.names = T)    

# Fill in missing draws
if(fill.draw) {
  have.draws <- unique(spec_draw$run_num)
  need.draws <- setdiff(1:1000, have.draws)
  for(draw in need.draws) {
    replace.draw <- sample(have.draws, 1)
    replace.dt <- spec_draw[run_num == replace.draw]
    replace.dt[, run_num := draw]
    spec_draw <- rbind(spec_draw, replace.dt)
  }
}

# Fill in NA's
na.present <- any(is.na(spec_draw))
if(fill.na & na.present) {
  na.draws <- unique(spec_draw[rowSums(is.na(spec_draw)) > 0, run_num])
  remain.draws <- setdiff(1:1000, na.draws)
  for(draw in na.draws) {
    replace.draw <- sample(remain.draws, 1)
    replace.dt <- copy(spec_draw[run_num == replace.draw])
    replace.dt[, run_num := draw]
    spec_draw <- rbind(spec_draw[run_num != draw], replace.dt)
  }
}

spec_draw[sex=="male",sex_id:=1]
spec_draw[sex=="female",sex_id:=2] 
spec_draw[,age:=as.character(age)]
spec_draw <- merge(spec_draw,age_map,by="age")
spec_draw[,age:=NULL]
## vestigial column
spec_draw[, suscept_pop := pop_neg]


## Calculate birth prevalence rate
birth_pop <- get_population(age_group_id=164, location_id=loc_id, year_id=1970:2019, sex_id=1:2, gbd_round_id = 6, decomp_step = 'step4')
setnames(birth_pop, c("year_id", "population"), c("year", "gbd_pop"))
birth_dt <- copy(spec_draw)
birth_dt <- birth_dt[,.(age_group_id = 164, birth_prev = sum(birth_prev), total_births = sum(total_births)), by = c('year', 'run_num', 'sex_id')]
birth_dt[, total_births := sum(total_births)/2, by = c('year', 'run_num')]
birth_dt[, birth_prev_rate := ifelse(total_births == 0, 0, birth_prev/total_births)]
## Scale to GBD pop
birth_dt <- merge(birth_dt, birth_pop[,.(year, gbd_pop, sex_id)], by = c('year', 'sex_id'))
birth_dt[, birth_prev_count := birth_prev_rate * gbd_pop]
write.csv(birth_dt[,.(year, sex_id, run_num, age_group_id, birth_prev_rate, birth_prev_count)], paste0(out_dir_birth, loc, '.csv'), row.names = F)
spec_draw[,birth_prev := NULL]

## Save over-80 Spectrum pops for splitting
spec_o80 <- data.table(spec_draw[age_group_id==21,])
spec_o80[, age_group_id := NULL]

# Get raw proportions for splitting populations and other general ones
pop <- fread(paste0("FILEPATH", run.name, '/population_splits/', loc, '.csv'))
o80_pop <- pop[age_group_id %in% c(30:32, 235),]
o80_pop[,pop_total:=sum(population), by=list(sex_id,year_id)]
o80_pop[,pop_prop:=population/pop_total, by=list(sex_id,year_id)]
# Create proportions for incidence 
o80_pop[,pop_prop_inc:=pop_prop]
# Create proportions for death 
o80_pop[,pop_prop_death:=pop_prop]
o80_pop <- o80_pop[,list(location_id,sex_id,year = year_id,age_group_id,pop_total,pop_prop,pop_prop_inc,pop_prop_death)]

## Split over-80 proportionally by age-group populations
spec_o80 <- merge(spec_o80,o80_pop,by=c("year","sex_id"),allow.cartesian=T) # m:m merge -- expand pops by draw, spec by age groups

# Split all variables that can be split by population
pop_weight_all <- function(x) return(x*spec_o80[['pop_prop']])
all_age_vars <- c("suscept_pop","pop_neg","non_hiv_deaths","pop_lt200","pop_200to350","pop_gt350","pop_art","pop")
spec_o80[,(all_age_vars) := lapply(.SD,pop_weight_all),.SDcols=all_age_vars] 

# Split incidence
pop_weight_inc <- function(x) return(x*spec_o80[['pop_prop_inc']])
inc_vars <- c("new_hiv")
spec_o80[,(inc_vars) := lapply(.SD,pop_weight_inc),.SDcols=inc_vars] 

# Split deaths 
pop_weight_death <- function(x) return(x*spec_o80[['pop_prop_death']])
death_vars <- c("hiv_deaths")
spec_o80[,(death_vars) := lapply(.SD,pop_weight_death),.SDcols=death_vars] 

spec_o80[,c("pop_total","pop_prop","pop_prop_inc","pop_prop_death","location_id") :=NULL]


##################################################################################################################
## Convert from Spectrum to rate space
convert_to_rate <- function(x) { 
  rate <- ifelse(spec_combined[['spec_pop']] != 0,
                 x/spec_combined[['spec_pop']], 0)
  return(rate)
} 

convert_to_rate_2 <- function(x) {
  rate <- ifelse(spec_combined[['spec_pop']] != 0,
                 x/((spec_combined[['spec_pop']]+spec_combined[['spec_pop_last']])/2), 0)  
  return(rate)
} 

shift_to_midyear <- function(x) {
  ## TODO we need to revisit repeating the last year
  x_last <- x[length(x)]
  x_lag <- c(shift(x, type = "lead")[-length(x)], x_last)
  out_x <- (x + x_lag) / 2
  return(out_x)
}

# Recombine with spec_draw
spec_combined <- rbindlist(list(spec_draw[!age_group_id == 21,],spec_o80),use.names=T) # Drop 0-5 and 80+, replace with spec_u5 and spec_o80 
spec_combined[, non_hiv_deaths_prop := non_hiv_deaths / (non_hiv_deaths + hiv_deaths)]
spec_combined[is.na(non_hiv_deaths_prop), non_hiv_deaths_prop := 1]

### get the mid year spec_pop for incidence and death
setnames(spec_combined, 'pop', 'spec_pop')
spec_combined_last <- spec_combined[,.(year, sex_id, run_num, age_group_id, spec_pop)]
spec_combined_last[, year:= year+1]
spec_combined_0 <- spec_combined[year==min(spec_combined$year),.(year, sex_id, run_num, age_group_id, suscept_pop)]
setnames(spec_combined_0, "suscept_pop", "spec_pop")
spec_combined_last <- rbind(spec_combined_last, spec_combined_0)
setnames(spec_combined_last, "spec_pop", "spec_pop_last")

spec_combined <- merge(spec_combined, spec_combined_last, by=c("year", "sex_id", "run_num", "age_group_id"))

## Shift HIV incidence and deaths
## EPPASM output infections and deaths are midyear-to-midyear
## We want incidence and deaths to be calendar year, and populations to be midyear
convert_vars <- c('non_hiv_deaths', 'hiv_deaths', 'new_hiv', 'hiv_births', 'total_births')
spec_combined[,(convert_vars) := lapply(.SD,shift_to_midyear),.SDcols=convert_vars, by=c("sex_id", "run_num", "age_group_id")] 
spec_combined[,(convert_vars) := lapply(.SD,convert_to_rate),.SDcols=convert_vars] 
print(head(spec_combined))

convert_vars2 <- c("suscept_pop","pop_neg","pop_lt200","pop_200to350","pop_gt350","pop_art")
spec_combined[,(convert_vars2) := lapply(.SD,convert_to_rate),.SDcols=convert_vars2] 
print(spec_combined)


##################################################################################################################
## Format and Output
setnames(spec_combined,"year","year_id")

##This is a fluke in gbd2019 that we get negative values - need to figure out how it could happen
if(loc == "KEN_44796" | loc == "KEN_35646"){
  spec_combined[run_num==880 & year_id==2019 & sex_id==2 & age_group_id==13,pop_lt200:=0]
}

print(head(spec_combined))
assert_values(spec_combined, names(spec_combined), "gte", 0)
assert_values(spec_combined, colnames(spec_combined), "not_na")

write.csv(spec_combined[,list(sex_id,year_id,age_group_id,run_num,hiv_deaths,non_hiv_deaths, non_hiv_deaths_prop)],paste0(out_dir_death,"/",loc,"_ART_deaths.csv"),row.names=F)


write.csv(spec_combined[,list(sex_id,year_id,run_num,hiv_deaths, non_hiv_deaths, new_hiv,hiv_births,suscept_pop,total_births,pop_neg,pop_lt200,pop_200to350,pop_gt350,pop_art,age_group_id)],paste0(out_dir,"/",loc,"_ART_data.csv"),row.names=F)


