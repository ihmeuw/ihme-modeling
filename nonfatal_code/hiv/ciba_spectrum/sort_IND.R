################################################################################
## Purpose: Identify IND spectrum output between EPP/CIBA incidence input
## Run instructions: 
## Notes:
# Always use EPP incidence: Andrha PrUSER, Karnataka, Maharastra, Manipur, Telangana, Tamil Nadu and Uttar PrUSER. 
# For the rest of locations choose EPP or CIBA based on which produces higher mean adult prevalence in 2005 (the year of the survey).
################################################################################

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"FILEPATH")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "FILEPATH")

## Packages
library(data.table); library(haven); library(parallel); library(reshape)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) > 0) {
	loc <- args[1]
	run.name.epp <- args[2]
	run.name.ciba <- args[3]
} else {
	loc <- ""
	run.name.epp <- ""
	run.name.ciba <- ""
}
ncores <- 2
id.vars <- c("year_id", "age_group_id", "run_num", "sex_id", "location_id")
value.vars <- c('hiv_deaths', 'pop_hiv', 'new_hiv', 'pop_art', 'suscept_pop')
all.vars <- c(id.vars, value.vars)
date <- substr(gsub("-", "", Sys.Date()),3,8)


find.children <- function(loc) {
	parent.loc <- loc.table[ihme_loc_id==loc, location_id]
	most.detailed <- loc.table[location_id==parent.loc, most_detailed]
	hiv.only <- loc.table[location_id==parent.loc, hiv_only]
	if(most.detailed==0){
		if(hiv.only == 1) {
			child.list <- loc.table[location_id==parent.loc, ihme_loc_id]
		} else {
			parent.level <- loc.table[location_id==parent.loc, level]
			child.list <- loc.table[parent_id==parent.loc & location_id!=parent_id & level==parent.level + 1 & hiv_only==0, ihme_loc_id]
		}
	} else {
		child.list <- loc.table[location_id==parent.loc, ihme_loc_id]
	}
	return(child.list)
}

### Tables
loc.table <- get_locations()
age.table <- fread(paste0(FILEPATH))
sex.table <- fread(paste0(FILEPATH))
location.id <- loc.table[ihme_loc_id == loc, location_id]

### Code
# Read in Spectrum data, drop deaths, and add up pop_hiv
spec.dt.epp <- fread(paste0(spec.dir.epp, loc, "FILEPATH"))
spec.dt.epp[,source:= "EPP"]
parent_id <- gsub("IND_", "", loc)
if (!parent_id %in% c("4841", "4856", "4860", "4861", "4870", "4871", "4873")) { # These locations have small uncertainty prevalence survey
	spec.dt.ciba <- fread(paste0(spec.dir.ciba, loc, "FILEPATH"))
	spec.dt.ciba[, source:= "CIBA"]
} else { 
	spec.dt.ciba <- NULL
}

spec.dt <- rbind(spec.dt.epp, spec.dt.ciba)
spec.dt[, c("hiv_deaths", "location_id") := .(NULL, location.id)]
spec.dt[, pop_hiv := pop_lt200 + pop_200to350 + pop_gt350 + pop_art]
spec.dt[, population := pop_neg + pop_lt200 + pop_200to350 + pop_gt350 + pop_art]
spec.dt[, c("pop_neg", "pop_lt200", "pop_200to350", "pop_gt350") := NULL]

### Adult prev rate
pop.dt.adult <- spec.dt[age %in% seq(15, 45, 5), .(location_id, age, sex, year, run_num, pop_hiv, population, source)]	
pop.dt.adult.both.sex <- pop.dt.adult[, .(pop_hiv = sum(pop_hiv), population = sum(population)), by=c("location_id", "year", "run_num", "source")]
pop.dt.adult.both.sex[, prev_rate := pop_hiv/population]
pop.dt.adult.mean <- pop.dt.adult.both.sex[, lapply(.SD, mean), by=c("location_id", "year", "source")]
prev.2005 <- pop.dt.adult.mean[year==2005, ]
source.index <- prev.2005[prev_rate==max(prev_rate), source]

if(source.index == "CIBA") {
	out.dt <- fread(paste0(spec.dir.ciba, loc, "FILEPATH"))
} else {
	out.dt <- fread(paste0(spec.dir.epp, loc, "FILEPATH"))
}

write.csv(out.dt, paste0(out.dir, loc, "FILEPATH"), row.names = F)

### End