## Purpose: Prepare for GBD2016 CIBA run
## Use ensemble data for Group1 and ST-GPR data for Other groups


######################
# Adjust estimated HIV incidence using assumptions about duration
# AKA Cohort Incidence Bias Adjustmnet (CIBA)
#
# 
# Adjusts incidence from a Spectrum run using preexisting duration data
# to fit Spectrum to ST-GPR  mortality data.
#
######################
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

# Get arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) == 0) {
	loc <- "ZAF_485"
	run.folder <- ""
	group_infor <- "1A" #"2A"
} else {
	# Location
	loc <- args[1]
	# Run folder name
	run.folder <- args[2]
	# Group information 
	group_infor <- args[3]
}


### Functions 

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
if (grepl("MDA", loc)) {
	loc.pa <- loc.table[ihme_loc_id==loc, parent_id]
	loc.children <- loc.table[parent_id==loc.pa, ihme_loc_id]
} else {loc.children <- loc}
###
inc.data <- data.table()
for (tmp.loc in loc.children) {
	tmp.inc.data <- fread(paste0(tmp.loc,'FILEPATH'))
	tmp.inc.data[,total.pop := pop_neg+pop_lt200+pop_200to350+pop_gt350+pop_art]
	tmp.inc.data <- tmp.inc.data[age >= 15,.(run_num, year, age, sex, new_hiv, suscept_pop, hiv_deaths, total.pop)]
	if (tmp.loc != loc) {
		tmp.inc.data[,new_hiv := 0]
		tmp.inc.data[,suscept_pop := 0]
	}
	tmp.inc.data[,loc_id := tmp.loc]
	inc.data <- rbind(inc.data, tmp.inc.data)
}
#### get the spectrum_pop prop for gbd_pop split later
if (grepl("MDA", loc)) {
spec.pop.dt.all <- inc.data[, .(total.pop=sum(total.pop)), by=.(run_num, year, age, sex)]
spec.pop.dt.loc <- inc.data[loc_id==loc, .(loc.pop=sum(total.pop)), by=.(run_num, year, age, sex)]
spec.pop.dt.prop <- merge(spec.pop.dt.loc, spec.pop.dt.all, by=c("run_num", "year", "age", "sex"))
spec.pop.dt.prop.mean <- spec.pop.dt.prop[,lapply(.SD, mean), by=.(year, age, sex)]
spec.pop.dt.prop.mean[, pop_prop:= loc.pop/total.pop]
spec.pop.dt.prop.mean[, sex_id:=ifelse(sex=="male", 1, 2)]
spec.pop.dt.prop.mean[, year_id:= year]
}
# Aggregate to GBD location
inc.data <- inc.data[loc_id==loc,.(new_hiv=sum(new_hiv), suscept_pop=sum(suscept_pop), hiv_deaths=sum(hiv_deaths), total.pop=sum(total.pop)), by=.(run_num, year, age, sex)]

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

# Get GBD population
if (grepl("MDA", loc)) {
	pop.data <- get_population(age_group_id = c(2:20, 30:32,235), location_id = loc.pa, year_id = -1, sex_id = c(1,2), location_set_id = 79)
	pop.data <- merge(pop.data, age.table[,.(age_group_id, age_group_years_start)], by="age_group_id", all.x=T)
	setnames(pop.data, "age_group_years_start", "age")
	pop.data.temp <- merge(pop.data, spec.pop.dt.prop.mean[,.(age, sex_id, year_id, pop_prop)], by=c("age", "year_id", "sex_id")) 
	pop.data.temp[, population:=population*pop_prop]
	pop.data <- pop.data.temp[, .SD, .SDcols=c(colnames(pop.data))]
} else {
	pop.data <- get_population(age_group_id = c(2:20, 30:32,235), location_id = loc.id, year_id = -1, sex_id = c(1,2), location_set_id = 79)
}
pop.data[, process_version_map_id:=NULL]
pop.data <- merge(pop.data, loc.table[,.(location_id, ihme_loc_id)], by="location_id", all.x=T)
pop.data <- pop.data[,.(pop=sum(population)), by=.(ihme_loc_id, year_id, sex_id, age_group_id, location_id)]

##### For Group1 countries only #####
if (group_infor %in% c("1A", "1B")) {
	ens.version <- get_proc_version(model_name="death number", model_type="estimate", run_id="recent")

	cod.data.raw <- fread(paste0("FILEPATH"))  ## use the up-to-date ensemble data file
	cod.pop.merged <- merge(cod.data.raw,pop.data, by=c('location_id', 'year_id','age_group_id','sex_id', 'ihme_loc_id'))

	# Get counts of ensemble deaths
	# Ensembled data are at count level already, no need to calculate
	cod.pop.merged[,deaths := mean]    
} else {

###### For non-Group1 countries ##########
	# Get GPR mortality results (per 100 people, might be worth changing in the long term)
	cod.data <- fread('FILEPATH')  ## update to time

	if(grepl("IND", loc)) {
		loc.id <- loc.table[ihme_loc_id == loc, location_id]
		children <- loc.table[parent_id == loc.id, location_id]
		subset.cod <- cod.data[location_id %in% children]
		subpop.data <- get_population(age_group_id = c(2:20, 30:32,235), location_id = children, year_id = -1, sex_id = c(1,2), location_set_id = 79)
		subset.cod <- merge(subset.cod, subpop.data, by=c("location_id", "year_id", "age_group_id", "sex_id"))
		subset.cod[, deaths:= gpr_mean*population]
		cod.data <- subset.cod[, .(deaths = sum(deaths), population = sum(population)), by = .(year_id, age_group_id, sex_id)]
		cod.data[, gpr_mean:= deaths/population]
		cod.data[, location_id := loc.id]
		cod.data[, c("deaths", "population"):=NULL]
	}

	cod.pop.merged <- merge(cod.data,pop.data, by=c('location_id', 'year_id','age_group_id','sex_id'))

	# Get counts of GPR deaths
	cod.pop.merged[,deaths := pop*gpr_mean/100]
 }
##################

cod.pop.merged[age_group_id <= 5, age_group_id := 1]  ### group all under 5 together
cod.pop.merged[age_group_id > 20, age_group_id := 21]  ### group all 80+ together
cod.pop.merged <- merge(cod.pop.merged, age.table[,.(age_group_id, age_group_years_start)], by="age_group_id", all.x=T)
setnames(cod.pop.merged, "age_group_years_start", "age")
cod.pop.merged[, c("ihme_loc_id", "location_id"):=.(loc, loc.id)]

# Aggregate counts to same location as Spectrum reuslts and recalculate rate
cod.pop.collapsed <- cod.pop.merged[ihme_loc_id %in% loc,.(deaths=sum(deaths), pop=sum(pop)), by=.(ihme_loc_id, year_id, age, sex_id)]

cod.pop.collapsed[, mort:=deaths/pop]
setnames(cod.pop.collapsed, "year_id", "year")
cod.pop.collapsed[, sex:=ifelse(sex_id==1, "male", "female")]

# Calculate deaths with _Spectrum_ population, not GBD
spec.cod.dt <- merge(spec.pop.dt, cod.pop.collapsed, by=c('year', 'age', 'sex'))
spec.cod.dt[,hiv_deaths := total.pop*mort]


# Get to correct location
cod.data <- spec.cod.dt[ihme_loc_id %in% loc,.(deaths=sum(hiv_deaths)), by=.(year, age, sex)]


# Get single-age structure for GPR data
single.age.structure <- cod.data[,.(single.age = extend.ages(age)),by=.(year,sex)]
single.age.structure[,age:=single.age-(single.age%%5)]

# Convert GPR deaths to single ages
merged.cod.data <- merge(cod.data, single.age.structure, by=c('year', 'age', 'sex'))
merged.cod.data[,single.cod := deaths/5]
merged.cod.data[age==80, single.cod := deaths]

# Get year extent of CoD data
cod.years <- min(cod.data[,year]):max(cod.data[,year])

# Identify years to use in adjustment
obs.years <- pmax(min(cod.years), 1990):max(cod.years)

# Merge both death datasets together and calculate the ratio
merged.deaths <- merge(merged.cod.data, merged.inc.data[year %in% cod.years], by=c('year','single.age','sex'))
merged.deaths <- merged.deaths[year %in% obs.years]
merged.deaths[,r:=single.cod/single.d]
merged.deaths[single.d==0,r:=1]

# Convert ratios to wide format (wide on draws)
wide.deaths <- data.table(dcast(merged.deaths[,.(year, single.age, sex, r, run_num)], year+single.age+sex ~ run_num, value.var=c('r')))
setnames(wide.deaths, as.character(1:n.draws), paste0('r_',1:n.draws))

######### Read in spectrum duration run data #############
duration.path <- paste0("FILEPATH")

# Get data from duration Spectrum run
duration.data <- fread(paste0(duration.path, loc,'FILEPATH'))

wide.reshaped.temp <- data.table()

rho.fun <- function(run_id) {	
	tmp.data <- duration.data[run_num==run_id]
	tmp.data2 <- cbind(tmp.data[, c(1:4), with=F], do.call("cbind", lapply(tmp.data[, -c(1:4), with=F], as.numeric)))
	in.data <- melt(tmp.data2, id.vars=c('year','age','sex', "run_num"))

# Aggregate to correct location
in.data <- in.data[year %in% obs.years,.(value=sum(value)), by=.(year, age, sex, run_num, variable)]


# Identify years of infection
in.data[,variable:=as.character(variable)]
split.names <- t(sapply(strsplit(in.data[,variable], '_'), function(x) {c(x[1],x[2])}))
in.data[,metric := split.names[,1]]
in.data[,inf_year := as.integer(split.names[,2])]

# Restrict to deaths
in.data <- in.data[metric=='d']
in.data[,variable:=NULL]
in.data[,inf_year:=as.numeric(inf_year)]
setnames(in.data, 'age', 'single.age')

# Calculate time since infection and age at infection for a given year-age-infection combination

## YEAR: chronological year
## INF_YEAR: year of infection
## AGE: current age
in.data[,inf.dist := year-inf_year]
in.data[,inf.age := single.age - inf.dist]

# Restrict to observation years
in.data <- in.data[year %in% cod.years]

# Calculate total HIV deaths for each infection year-age-sex cohort and merge back on to full dataset
cohort.d <- in.data[,.(total.d = sum(value)), by=.(inf_year, inf.age, sex)]
in.data <- merge(in.data, cohort.d, by=c('inf_year', 'inf.age', 'sex'))

# Calculate rho, the cohort-specifc share of observed HIV deaths that occur in a particular year
# Used to weight "r" calculated previously
in.data[,rho := value/total.d]
in.data[total.d == 0, rho := 0]

# Identify last year
max.year <- in.data[,max(inf_year)]

#### change duration data to wide format
wide.reshaped <- data.table(dcast(in.data[,.( inf_year, inf.age, year, single.age, sex, rho, run_num)], inf_year+inf.age+year+single.age+sex ~ run_num, value.var=c('rho')))
setnames(wide.reshaped, as.character(run_id), paste0('rho_',run_id))

if (run_id==1) {
		wide.reshaped.temp <- wide.reshaped
} else {
		wide.reshaped.temp <- wide.reshaped[,6,with=F]		
}
}

wide.reshaped.all.list <- lapply(1:n.draws, rho.fun)
wide.reshaped.all <- do.call("cbind", wide.reshaped.all.list) 
wide.reshaped.all <- data.table(wide.reshaped.all)

# Convert rho to wide format by cohort
wide.rho.data <- merge(wide.reshaped.all, wide.deaths, by=c('year', 'single.age', 'sex'))
wide.combined.data <- wide.rho.data[,.(year, single.age, sex, inf_year, inf.age)]
alloc.col(wide.combined.data,1005)

# Multiply each "r" by the appropriate "rho"
r.cols <- paste0('r_', 1:n.draws)
for (i in 1:n.draws) {
  set(wide.combined.data,j=paste0('combined.r_',i),value=wide.rho.data[[paste0('r_',i)]] * wide.rho.data[[paste0('rho_',i)]])
}

# Aggregate weighted r's
combined.r.dt <- wide.combined.data[,lapply(.SD, sum, na.rm=T),by=.(inf_year, inf.age, sex), .SDcols=paste0('combined.r_',1:n.draws)]

# Once we aggregate we can think of inf_year and inf_age as
# chronological year and age because we only have one observation per cohort
setnames(combined.r.dt, c('inf_year', 'inf.age'), c('year', 'single.age'))

# Identify last year
max.year <- inc.data[,max(year)]
# Restrict to appropriate years
combined.r.dt <- combined.r.dt[year < max.year]
# Extend second to last year to adjust actual last year
penult.r <- combined.r.dt[year==max.year-1]
penult.r[,year:=max.year]
combined.r.dt <- rbind(combined.r.dt, penult.r)

# Merge adjusted r and original new cases from Spectrum
adj.data <- merge(wide.inc.data, combined.r.dt, by=c('year', 'single.age', 'sex'))

# Adjust cohort-specific incidence using adjusted r
alloc.col(adj.data, 3003)
for (i in 1:n.draws) {
  set(adj.data,j=paste0('adj.cases_',i),value=adj.data[[paste0('single.cases_',i)]]*adj.data[[paste0('combined.r_',i)]])
}

## Single-age  ##
#adj.data[, age:=single.age-(single.age%%5)]
agg.data <- adj.data[ ,lapply(.SD, sum, na.rm=T),by=.(year, single.age, sex), .SDcols=paste0('adj.cases_',1:n.draws)]
agg.data <- merge(agg.data, wide.pop.data.age, by=c('year', 'single.age', 'sex'))
out.data <- agg.data[,.(year, single.age, sex)]
# Convert to rate per susceptible person and multiply by 100
alloc.col(out.data, 1003)
for (i in 1:n.draws) {
  value.temp <- 100*agg.data[[paste0('adj.cases_',i)]]/agg.data[[paste0('single.pop_',i)]]
  value.temp[is.nan(value.temp)] <- 0
  value.temp[is.infinite(value.temp)] <- 0
  set(out.data,j=paste0('draw',i),value=value.temp)
}

#print(proc.time()-start.time)
## Save the new incidence data ###
## save it to run_name folder
out.dir_run <- paste0("FILEPATH")
dir.create(out.dir_run, showWarnings=FALSE, recursive=TRUE)
write.csv(out.data, paste0(out.dir_run, loc,'FILEPATH'), row.names=F)
## save it to current_run folder

### Ciba data preparation end 


