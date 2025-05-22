## Set up settings
rm(list=ls())
Sys.umask(mode = "0002")

if (Sys.info()[1]=="Windows") {
  root <- "FILEPATH" 
  user <- Sys.getenv("USERNAME")
} else {
  root <- "FILEPATH"
  user <- Sys.getenv("USER")
}

### Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
  iso3 <- args[1]
  spec_name <- args[2]
} else {
  iso3 <- "PAK_53620"
  spec_name <- "240304_platypus"
}
fill.draw <- T
fill.na <- T


### Paths
# Searches for Spectrum results from first to last, useful if you ran spectrum multiple times and need to compile down to one

directories <- list(
  paste0("FILEPATH/",spec_name,"/compiled/stage_1")
)

for(directory in directories) {
  if(file.exists(paste0(directory,"/",iso3,"_ART_data.csv"))) {
    print(paste0("File is in ",directory))
    results_dir <- directory
    break
  }
}

spec_dir <- paste0(results_dir)

# spec_name <- "240514_platypus"
out_dir <- paste0("FILEPATH/art_draws/",spec_name)
dir.create(out_dir, recursive = T, showWarnings = F)
out_dir_death <- paste0("FILEPATH/death_draws/",spec_name)
dir.create(out_dir_death, recursive = T, showWarnings = F)
out_dir_birth <- paste0("FILEPATH/birth_prev/",spec_name, '/')
dir.create(out_dir_birth, recursive = T, showWarnings = F)

### Functions
library(mortdb, lib = "FILEPATH")
source("FILEPATH/get_population.R")
invisible(sapply(list.files("FILEPATH/", full.names = T), source))


## Libraries etc.
library(data.table); library(foreign); library(assertable)
age_map <- data.table(fread("FILEPATH/age_map.csv"))
age_map <- age_map[(age_group_id>=6 & age_group_id <=21) | age_group_id == 28,list(age_group_id,age=age_group_name_short)]

## Create a map to scramble draws from Spectrum
locations <- get_location_metadata(location_set_id = 35, release_id = 16)
loc_id <- locations[ihme_loc_id==iso3,location_id]

##################################################################################################################
## Import and Format Data

## Function to fill in missing year/sex/age/draw combinations for Spectrum draws
fill_years <- function(data) {
  if(grepl('20200805', spec_name)){
    years <- seq(1970,2030)
    
  }else{
    years <- seq(1970,2024)
    
  }
  sexes <- c("male","female")
  ages <- seq(0,80,5)
  run_num <- seq(1,1000)
  draw_count <- length(sexes) * length(ages) * length(run_num) # How many draws do we expect per year?
  template <- data.table(expand.grid(year=years,sex=sexes,age=ages,run_num=run_num,stringsAsFactors=F))
  data <- merge(data,template,by=c("year","sex","age","run_num"),all=T)
  
  # Get number of missing observations in each yer
  # We expect 34,000 rows per year -- if a year has a different number, this means not all draws are present or something else strange has happened
  data_miss_count <- data[is.na(hiv_deaths),.N,by=c("year", "run_num")]
  miss_years <- unique(data_miss_count[N==draw_count,year])
  data[year %in% unique(data_miss_count$year)&run_num %in% unique(data_miss_count$run_num),c("hiv_deaths","new_hiv","hiv_births","total_births","suscept_pop","pop_lt200","pop_200to350","pop_gt350","pop_art", "birth_prev"):=0]
  data[year %in% unique(data_miss_count$year)&run_num %in% unique(data_miss_count$run_num),c("non_hiv_deaths","pop_neg"):=1] # To prevent divide by 0 issues later on

  assert_ids(data, id_vars = list(year = years, sex = sexes, age = ages, run_num = run_num))
  return(data.table(data))
}

## Bring in Spectrum Draws
spec_draw <- data.table(fread(paste0(spec_dir,"/",iso3,"_ART_data.csv")))
spec_draw[, V1 := NULL]
spec_draw <- unique(spec_draw)

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
if(iso3 == "PHL") {
  have.draws <- unique(spec_draw[year==1989&sex=="female"& age ==60]$run_num)
  need.draws <- setdiff(1:1000, have.draws)
  spec_draw <- spec_draw[!(run_num %in% need.draws)]
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

spec_draw <- fill_years(spec_draw)
spec_draw[sex=="male",sex_id:=1]
spec_draw[sex=="female",sex_id:=2] 
spec_draw[,age:=as.character(age)]
spec_draw <- merge(spec_draw,age_map,by="age")
spec_draw[,age:=NULL]
spec_draw[,spec_pop:=pop_neg+pop_lt200+pop_200to350+pop_gt350+pop_art]
spec_draw[spec_pop==0,pop_neg:=1] # Sometimes spectrum reports no pop total. Assume pop_neg of 1 and spec_pop of 1
spec_draw[spec_pop==0,spec_pop:=1] 

## Make under-5 proportions to split pops, inc, prev, etc.
pop <- get_population(age_group_id = -1, location_id = loc_id, year_id = -1, sex_id = c(1,2),
                      location_set_id = 79, release_id = 9)[, 
                      .(location_id, sex_id, year_id, age_group_id, population)]
  pop.2022 <- pop[year_id ==2022]
  pop.2022[, year_id := 2023]
  pop <- rbind(pop, pop.2022)
  pop.2022[, year_id := 2024]
  pop <- rbind(pop, pop.2022)
setnames(pop, c("year_id", "population"), c("year", "gbd_pop"))
birth_pop <- get_population(age_group_id=164, location_id=loc_id, 
                            year_id=1970:2024, sex_id=1:2, release_id = 9)
pop.2022 <- birth_pop[year_id ==2022]
pop.2022[, year_id := 2023]
birth_pop <- rbind(birth_pop, pop.2022)
pop.2022[, year_id := 2024]
birth_pop <- rbind(birth_pop, pop.2022)

setnames(birth_pop, c("year_id", "population"), c("year", "gbd_pop"))

## Calculate birth prevalence rate
birth_dt <- copy(spec_draw)
birth_dt <- birth_dt[,.(age_group_id = 164, birth_prev = sum(birth_prev), total_births = sum(total_births)), by = c('year', 'run_num', 'sex_id')]
birth_dt[, total_births := sum(total_births)/2, by = c('year', 'run_num')]
birth_dt[, birth_prev_rate := ifelse(total_births == 0, 0, birth_prev/total_births)]
spec_draw[,birth_prev := NULL]
setnames(birth_dt, 'birth_prev', 'birth_prev_count')
id.vars = list(year = c(1970:2024), sex_id = c(1,2), age_group_id = 164, run_num = c(1:1000))
missing = assertable::assert_ids(birth_dt, id_vars = id.vars, warn_only = F)
write.csv(birth_dt[,.(year, sex_id, run_num, age_group_id, birth_prev_rate, birth_prev_count)], paste0(out_dir_birth, iso3, '.csv'), row.names = F)

## Save under-5 Spectrum pops for splitting
spec_u5 <- data.table(spec_draw[age_group_id==28,])
spec_u5[,c("age_group_id"):=NULL]

# Get raw proportions for splitting populations and other general ones
u5_pop <- pop[age_group_id %in% c(2,3,388,389,238,34),]
u5_pop[,pop_total:=sum(gbd_pop), by=list(sex_id,year)]
u5_pop[,pop_prop:=gbd_pop/sum(gbd_pop), by=list(sex_id,year)]

# Create proportions for incidence (no 1-4 age group because no incidence after 12 months)
u5_pop[!(age_group_id %in% c(34,238)),pop_prop_inc:=pop_prop]
u5_pop[age_group_id %in% c(34,238),pop_prop_inc:=0]
u5_pop[,pop_prop_inc:=pop_prop_inc/sum(pop_prop_inc), by=list(sex_id,year)]

# Create proportions for death (no ENN or LNN deaths)
u5_pop[age_group_id!=2 & age_group_id != 3,pop_prop_death:=pop_prop]
u5_pop[age_group_id==2 | age_group_id == 3,pop_prop_death:=0]
u5_pop[,pop_prop_death:=pop_prop_death/sum(pop_prop_death), by=list(sex_id,year)]

u5_pop <- u5_pop[,list(location_id,sex_id,year,age_group_id,pop_total,pop_prop,pop_prop_inc,pop_prop_death)]

#################

## Save over-80 Spectrum pops for splitting
spec_o80 <- data.table(spec_draw[age_group_id==21,])
spec_o80[,c("age_group_id"):=NULL]

# Get raw proportions for splitting populations and other general ones
o80_pop <- pop[age_group_id %in% c(30:32, 235),]
o80_pop[,pop_total:=sum(gbd_pop), by=list(sex_id,year)]
o80_pop[,pop_prop:=gbd_pop/sum(gbd_pop), by=list(sex_id,year)]

# Create proportions for incidence (no 1-4 age group because no incidence after 12 months)
o80_pop[,pop_prop_inc:=pop_prop]

# Create proportions for death (no ENN or LNN deaths)
o80_pop[,pop_prop_death:=pop_prop]

o80_pop <- o80_pop[,list(location_id,sex_id,year,age_group_id,pop_total,pop_prop,pop_prop_inc,pop_prop_death)]

##################################################################################################################
## Split under-5 proportionally by age-group populations
spec_u5 <- merge(spec_u5,u5_pop,by=c("year","sex_id"), allow.cartesian=T) # m:m merge -- expand pops by draw, spec by age groups

# Split all variables that can be split by population without age restrictions for 1-4 age cat
pop_weight_all <- function(x) return(x*spec_u5[['pop_prop']])
all_age_vars <- c("suscept_pop","pop_neg","non_hiv_deaths","pop_lt200","pop_200to350","pop_gt350","pop_art","spec_pop")
spec_u5[,(all_age_vars) := lapply(.SD,pop_weight_all),.SDcols=all_age_vars]

# Split incidence into NN categories only, and enforce 0 for 1-4
pop_weight_inc <- function(x) return(x*spec_u5[['pop_prop_inc']])
inc_vars <- c("new_hiv")
spec_u5[,(inc_vars) := lapply(.SD,pop_weight_inc),.SDcols=inc_vars]

# Split deaths into ENN and 1-4 age categories
pop_weight_death <- function(x) return(x*spec_u5[['pop_prop_death']])
death_vars <- c("hiv_deaths")
spec_u5[,(death_vars) := lapply(.SD,pop_weight_death),.SDcols=death_vars]

spec_u5[,c("pop_total","pop_prop","pop_prop_inc","pop_prop_death","location_id") :=NULL]


#########
## Split over-80 proportionally by age-group populations
spec_o80 <- merge(spec_o80,o80_pop,by=c("year","sex_id"),allow.cartesian=T) # m:m merge -- expand pops by draw, spec by age groups

# Split all variables that can be split by population
pop_weight_all <- function(x) return(x*spec_o80[['pop_prop']])
all_age_vars <- c("suscept_pop","pop_neg","non_hiv_deaths","pop_lt200","pop_200to350","pop_gt350","pop_art","spec_pop")
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
  x_lag <- c(0, data.table::shift(x, type = "lag")[-1])
  out_x <- (x + x_lag) / 2
  return(out_x)
}

# Recombine with spec_draw
spec_combined <- rbindlist(list(spec_draw[!(age_group_id %in% c(21, 28)),],spec_u5, spec_o80),use.names=T) # Drop 0-5 and 80+, replace with spec_u5 and spec_o80
spec_combined[, non_hiv_deaths_prop := non_hiv_deaths / (non_hiv_deaths + hiv_deaths)]
spec_combined[is.na(non_hiv_deaths_prop), non_hiv_deaths_prop:=1]

### get the mid year spec_pop for incidence and death
spec_combined_last <- spec_combined[,.(year, sex_id, run_num, age_group_id, spec_pop)]
spec_combined_last[, year:= year+1]
spec_combined_0 <- spec_combined[year==min(spec_combined$year),.(year, sex_id, run_num, age_group_id, suscept_pop)]
setnames(spec_combined_0, "suscept_pop", "spec_pop")
spec_combined_last <- rbind(spec_combined_last, spec_combined_0)
setnames(spec_combined_last, "spec_pop", "spec_pop_last")

spec_combined <- merge(spec_combined, spec_combined_last, by=c("year", "sex_id", "run_num", "age_group_id"))

convert_vars <- c("suscept_pop","pop_neg","pop_lt200","pop_200to350","pop_gt350","pop_art")
spec_combined[,(convert_vars) := lapply(.SD,convert_to_rate),.SDcols=convert_vars]
spec_combined[,(convert_vars) := lapply(.SD,shift_to_midyear),.SDcols=convert_vars, by=c("sex_id", "run_num", "age_group_id")]
convert_vars2  <- c(inc_vars,death_vars,"non_hiv_deaths", "hiv_births","total_births")
spec_combined[,(convert_vars2) := lapply(.SD,convert_to_rate_2),.SDcols=convert_vars2]
print(head(spec_combined))
print(head(spec_u5))
# 
# 
# ##################################################################################################################
# ## Format and Output
setnames(spec_combined,"year","year_id")

# Output deaths as a separate csv to make read-time easier on 01c step
# Note that non-HIV deaths in NN granular groups is NOT usable in current format -- needs to be collapsed to under-1 because deaths were simply pop-weighted.
# Used in 02_lt_gen in ensemble code, where it is collapsed to under-1

print(head(spec_combined))
print(head(spec_combined[,non_hiv_deaths]))
assert_values(spec_combined, names(spec_combined), "gte", 0)
assert_values(spec_combined, colnames(spec_combined), "not_na")

id.vars = list(year_id = c(1970:2024), sex_id = c(1,2), age_group_id = c(2,3,6:20, 30:32, 34, 235, 238, 388, 389), run_num = c(1:1000))
missing = assertable::assert_ids(spec_combined, id_vars = id.vars, warn_only = F)

write.csv(spec_combined[,list(sex_id,year_id,age_group_id,run_num,hiv_deaths,non_hiv_deaths, non_hiv_deaths_prop)],paste0(out_dir_death,"/",iso3,"_ART_deaths.csv"),row.names=F)


### Rescale based on PAK incidence outbreak
if(iso3 %in% c("PAK_53620", "PAK_53621")){
  PAK.fixed <- fread("FILEPATH/incidence_draws_for_epi_viz.csv")
  PAK.fixed <- PAK.fixed[sex_id %in% c(1,2)&metric_id==3]
  PAK.fixed[,c("val", "lower", "upper","metric_id","cause_id","measure_id") := NULL]
  PAK.fixed <- melt(PAK.fixed, id.vars = c("location_id", "age_group_id", "sex_id", "year_id"))
  PAK.fixed<- as.data.table(PAK.fixed)
  PAK.fixed[, variable := as.numeric(gsub("draw_", "", variable))]
  setnames(PAK.fixed, "variable", "run_num")
  PAK.fixed <- PAK.fixed[location_id==loc_id]
  PAK.fixed[, location_id:= NULL]
  spec_combined <- merge(spec_combined, PAK.fixed, by=c("year_id", "sex_id","age_group_id","run_num"), all.x =T)
  spec_combined[,new_hiv := ifelse(!is.na(value) & value > new_hiv, value, new_hiv)]
  spec_combined[, value:= NULL]
}
write.csv(spec_combined[,list(sex_id,year_id,run_num,hiv_deaths, non_hiv_deaths, new_hiv,hiv_births,suscept_pop,total_births,pop_neg, pop_lt200,pop_200to350,pop_gt350,pop_art,age_group_id)],paste0(out_dir,"/",iso3,"_ART_data.csv"),row.names=F)
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
