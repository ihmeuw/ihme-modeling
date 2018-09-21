## Purpose: Adjust Group 2C incidence based on ratio from Group 2A,2B ciba process
## Randomly sample location from the same region, randomly same draw from selected location


### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"FILEPATH")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "FILEPATH")

### Packages #####
library(data.table)
library(reshape2)
library(ggplot2)
# library(tempdisagg)
library(foreign)
library(parallel)

# Get arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) == 0) {
	loc <- "COM"
	run.folder <- ""
	out.folder <- ""
	group_infor <- "2A"
} else {
	# Location
	loc <- args[1]
	# Run folder name
	run.folder <- args[2]
	# Group information 
	group_infor <- args[3]
}
set.seed()

# Fill single ages
# Convenient for data.table
extend.ages <- function(a.vec) {
  return(min(a.vec):max(a.vec))
}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
### Tables
loc.table <- get_locations()
age.table <- data.table(get_age_map(type="all"))
#start.time <- proc.time()

### Code
loc.id <- loc.table[ihme_loc_id ==  loc, location_id]
## Read in the spectrum Stage 1 draw level data ##
in.dir <- paste0('FILEPATH')
setwd(in.dir)

# Read in all necessary locations for current location
# (Subnationals if adjusting nationally, for instance)
inc.data <- data.table()
for (tmp.loc in loc) {
	tmp.inc.data <- fread(paste0(tmp.loc,'FILEPATH'))
	tmp.inc.data[,total.pop := pop_neg+pop_lt200+pop_200to350+pop_gt350+pop_art]
	tmp.inc.data <- tmp.inc.data[age >= 15,.(run_num, year, age, sex, new_hiv, suscept_pop, hiv_deaths, total.pop)]
	if (tmp.loc != loc) {
		tmp.inc.data[,new_hiv := 0]
		tmp.inc.data[,suscept_pop := 0]
	}
	tmp.inc.data[,loc := tmp.loc]
	inc.data <- rbind(inc.data, tmp.inc.data)
}
# Aggregate to GBD location
inc.data <- inc.data[,.(new_hiv=sum(new_hiv), suscept_pop=sum(suscept_pop), hiv_deaths=sum(hiv_deaths), total.pop=sum(total.pop)), by=.(run_num, year, age, sex)]

# Get number of draws in input
n.draws <- length(inc.data[,unique(run_num)])

# Get Spectrum population for use in population scaling
spec.pop.dt <- inc.data[,.(total.pop=mean(total.pop)), by=.(year, age, sex)]

# Create structure for single age data
single.age.structure <- inc.data[,.(single.age = extend.ages(age)),by=.(year,sex,run_num)]
single.age.structure[,age:=single.age-(single.age%%5)]

# Replicate observations for single-ages and divide HIV deaths by five
merged.inc.data <- merge(inc.data, single.age.structure, by=c('year', 'age', 'sex', 'run_num'), all.y=T)
merged.inc.data[,single.d := hiv_deaths/5]
merged.inc.data[age==80, single.d := hiv_deaths]

# Divide new infections by five
merged.inc.data[,single.cases := new_hiv/5]
merged.inc.data[age==80, single.cases := new_hiv]

# Divide susceptible population by five
merged.inc.data[,single.pop := suscept_pop/5]
merged.inc.data[age==80, single.pop := suscept_pop]

# Convert new infections to wide format (year, age, and sex long, draws wide)
wide.inc.data <- data.table(dcast(merged.inc.data[,.(year, single.age, sex, run_num, single.cases)], year+single.age+sex~run_num, value.var=c('single.cases')))
setnames(wide.inc.data, as.character(1:n.draws), paste0('single.cases_',1:n.draws))

# Convert susceptible population to wide format
### Single age  
long.pop.data.age <- merged.inc.data[single.age>=15 ,.(single.pop = sum(single.pop)), by=.(year, single.age, run_num, sex)]
wide.pop.data.age <- data.table(dcast(long.pop.data.age[,.(year, single.age, sex, run_num, single.pop)], year+single.age+sex~run_num, value.var=c('single.pop')))
setnames(wide.pop.data.age, as.character(1:n.draws), paste0('single.pop_',1:n.draws))

### calculate the input incidence percentage 
inc.pop.data <- merge(wide.inc.data, wide.pop.data.age, by=c('year', 'single.age', 'sex'))
inc.pct.data <- inc.pop.data[,.(year, single.age, sex)]
# Convert to rate per susceptible person and multiply by 100
alloc.col(inc.pct.data, 1003)
for (i in 1:n.draws) {
  set(inc.pct.data,j=paste0('draw_input_',i),value=100*inc.pop.data[[paste0('single.cases_',i)]]/inc.pop.data[[paste0('single.pop_',i)]])
}

########## Sample the ratio #########
if(grepl("IDN", loc)) {
	inc.ratio.dir <- 'FILEPATH'	
} else {
	inc.ratio.dir <- 'FILEPATH'	
}
file.list <- list.files(inc.ratio.dir)
loc.list <- gsub("FILEPATH","", file.list)
region <- loc.table[ihme_loc_id==loc, region_name]
super_region <- loc.table[ihme_loc_id==loc, super_region_name]
# loc.pool <- loc.list
loc.pool <- intersect(loc.list, loc.table[super_region_name==super_region, ihme_loc_id])

if (length(loc.pool) == 0 ) {
	loc.pool <- intersect(loc.list, loc.table[super_region_name!= "High-income", ihme_loc_id])
}  
# if (length(loc.pool) == 0) {
# 	loc.pool <- loc.list
# }
source.table <- data.table(loc = sample(loc.pool, 1000, replace = T), draw = sample(1:1000, 1000, replace = T))

i <- 1
inc.adjut.data <- copy(inc.pct.data[,.(year, single.age, sex)])
alloc.col(inc.adjut.data, 1003)
for (loc.select in unique(source.table$loc)) {
	# loc.select <- unique(source.table$loc)[1]
	### read in ratio
	inc.ratio.data <- fread(paste0(inc.ratio.dir, loc.select, "FILEPATH"))
	merged.ratio.data <- merge(inc.pct.data[, .(year, single.age, sex)], inc.ratio.data, by = c("year", "single.age", "sex"), all.x = T)
	for(j in names(merged.ratio.data)) {
    set(merged.ratio.data,which(is.na(merged.ratio.data[[j]])),j,1)
	}

	source.draws <- source.table[loc == loc.select, draw]
	for(draw_select in source.draws) {
		# draw_select <- source.draws[1]
		set(inc.adjut.data,j=paste0('draw',i),value=inc.pct.data[[paste0('draw_input_',i)]]*merged.ratio.data[[paste0('ratio_',draw_select)]])
		i <- i + 1
	}
}

## Save the adjusted incidence data ###
## save it to run_name folder
out.dir_run <- paste0("FILEPATH")
dir.create(out.dir_run, showWarnings=FALSE, recursive=TRUE)
write.csv(inc.adjut.data, paste0(out.dir_run, loc,'FILEPATH'), row.names=F)
## save it to current_run folder
out.dir <- paste0("FILEPATH")
dir.create(out.dir, showWarnings=FALSE, recursive=TRUE)
write.csv(inc.adjut.data, paste0(out.dir, loc,'FILEPATH'), row.names=F)