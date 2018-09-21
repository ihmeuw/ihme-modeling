################################################################################
## Purpose: Prepare RR of mortality based on spectrum mortablity for life table
## Date modified:
## Run instructions: Be launched by  
################################################################################

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"FILEPATH")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "FILEPATH")

## Packages
library(data.table); library(foreign)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
	loc <- args[1]
	run.name <- args[2]
} else {
	loc <- ""
	run.name <- ""
}


### Output  
output.path <- paste0("FILEPATH")
curr.path <- "FILEPATH"
dir.create(output.path, recursive = T, showWarnings = F)


### GBD pop
loc.table <- get_locations()
location.id <- loc.table[ihme_loc_id==loc, location_id]
age.table <- data.table(get_age_map(type="all"))


######
spectrum.dt <- fread(spectrum.path)
spectrum.dt[, population:= pop_neg+pop_lt200+pop_200to350+pop_gt350+pop_art]
spectrum.dt[, c("location_id"):=.(location.id)]
setnames(spectrum.dt, "run_num", "sim")
merged.dt <- spectrum.dt[, .(location_id, sim, year, sex, age, hiv_deaths, suscept_pop, population)]

### Aggregate under 1
merged.dt.under1 <- merged.dt[age==0]
merged.dt.under1[, age:=1]

### Calculate total deaths 
total.dt <- merged.dt[, .(total_deaths = sum(hiv_deaths)), by = .(year, sim)]
mean.total <- total.dt[, .(total_deaths = mean(total_deaths)), by = .(year)]

####
merged.dt <- rbind(merged.dt, merged.dt.under1)
### use the end of last year pop as the start of current year pop
merged.dt_last <- copy(merged.dt)
merged.dt_last[, c("year", "hiv_deaths"):= .(year+1, NULL)]
setnames(merged.dt_last, "population", "population_last")
## for HIV starting year, use the suscept_pop as the starting year pop
merged.dt_year1 <- merged.dt_last[year==min(merged.dt_last$year)] 
merged.dt_year1[, c("population_last", "year"):= .(suscept_pop, min(merged.dt$year))]

merged.dt_last <- rbind(merged.dt_year1, merged.dt_last)
###
merged.dt <- merge(merged.dt, merged.dt_last, by=c("location_id", "year", "sex",  "sim", "age"))
merged.dt[, mort_rate:= hiv_deaths/((population+population_last)/2)]
merged.dt[, c("hiv_deaths", "population", "suscept_pop.x", "suscept_pop.y", "population_last"):=NULL]
merged.dt <- merge(merged.dt, mean.total, by = "year")
### Reference group 40-44
reference.dt <- merged.dt[age==40,.(location_id, year, sex, sim, mort_rate)]
setnames(reference.dt, "mort_rate", "mort_rate_ref")
ref.merged.dt <- merge(merged.dt, reference.dt, by=c("location_id", "year", "sex",  "sim"))
ref.merged.dt [, hivrr := mort_rate/mort_rate_ref]
ref.merged.dt[is.na(hivrr)]$hivrr <- 1
ref.merged.dt[is.infinite(hivrr)]$hivrr <- 1
ref.merged.dt[total_deaths < 100]$hivrr <- 1
ref.merged.dt[, total_deaths := NULL]
combined.dt <- ref.merged.dt
combined.dt <- merge(combined.dt, loc.table[ihme_loc_id==loc, .(ihme_loc_id, location_id)], by="location_id")
combined.dt[, c("mort_rate", "mort_rate_ref"):= NULL]
combined.dt[, c("location_id", "sim"):=.(NULL, sim-1)]
#### save the files ########
write.csv(combined.dt, paste0(output.path, loc, "FILEPATH"), row.names = F)
write.csv(combined.dt, paste0(curr.path, loc, "FILEPATH"), row.names = F)

