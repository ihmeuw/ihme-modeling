

# Adjust estimated HIV incidence using assumptions about duration
# AKA Cohort Incidence Bias Adjustmnet (CIBA)
#

######################
### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"J:/","/home/j/")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "H:/", "/homes/"), user, "/hiv_gbd2019/")

### Packages #####
library(data.table)
library(reshape2)
library(foreign)
library(dplyr)


# Get arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) == 0) {
  loc <- "VNM"
  run.folder <- "191206_inputs_testing"
  group_infor <- "2B" #"2A"
} else {
  # Location
  loc <- args[1]
  # Run folder name
  run.folder <- args[2]
  # Group information 
  group_infor <- args[3]
}

### Functions 
library(mortdb, lib = "/home/j/WORK/02_mortality/shared/r")
source(paste0(root, "Project/Mortality/shared/functions/get_locations.r"))
source(paste0(root, "Project/Mortality/shared/functions/get_age_map.r"))
source(paste0("/share/cc_resources/libraries/current/r/get_population.R"))
source(paste0(root, "Project/Mortality/shared/functions/get_proc_version.R"))

# Fill single ages

# Convenient for data.table
extend.ages <- function(a.vec) {
  return(min(a.vec):max(a.vec))
}

### Tables
loc.table <- data.table(get_locations(hiv_metadata = T))
age.table <- data.table(get_age_map(type="all"))
#start.time <- proc.time()

### Code
loc.id <- loc.table[ihme_loc_id ==  loc, location_id]
## Read in the spectrum Stage 1 draw level data ##
in.dir <- paste0('/ihme/hiv/spectrum_draws/',run.folder,'/compiled/stage_1')
setwd(in.dir)

# Read in all necessary locations for current location
stage1.loc <- loc
if(grepl('GBR', loc) & loc.table[ihme_loc_id == loc, level] == 6){
  stage1.loc <- paste0('GBR_', loc.table[ihme_loc_id == loc, parent_id])
}
###
tmp.inc.data <- fread(paste0(stage1.loc,'_ART_data.csv'))
tmp.inc.data[sex == 1, sex := 'male']
tmp.inc.data[sex == 2, sex := 'female']
tmp.inc.data[,total.pop := pop_neg+pop_lt200+pop_200to350+pop_gt350+pop_art]
inc.data <- tmp.inc.data[,.(run_num, year, age, sex, new_hiv, suscept_pop, hiv_deaths, total.pop)]

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
long.pop.data.age <- merged.inc.data[,.(single.pop = sum(single.pop)), by=.(year, single.age, run_num, sex)]
wide.pop.data.age <- data.table(dcast(long.pop.data.age[,.(year, single.age, sex, run_num, single.pop)], year+single.age+sex~run_num, value.var=c('single.pop')))
setnames(wide.pop.data.age, as.character(1:n.draws), paste0('single.pop_',1:n.draws))

pop.data <- get_population(age_group_id = c(2:20, 30:32,235), 
                           location_id = loc.id, year_id = -1, 
                           sex_id = c(1,2), location_set_id = 79,
                           decomp_step = "step4")
pop.data[, process_version_map_id:=NULL]
pop.data <- merge(pop.data, loc.table[,.(location_id, ihme_loc_id)], by="location_id", all.x=T)
pop.data <- pop.data[,.(pop=sum(population)), by=.(ihme_loc_id, year_id, sex_id, age_group_id, location_id)]

##### For Group1 countries only #####
if (group_infor %in% c("1A", "1B")) {
  ens.version <- get_proc_version(model_name="death number", model_type="estimate", run_id="recent")
  
  cod.data.raw <- fread(paste0("/home/j/WORK/04_epi/01_database/02_data/hiv/requests/GBD15_ensemble_data_all_locations_mean_v", ens.version, ".csv"))  ## use the up-to-date ensemble data file
  cod.pop.merged <- merge(cod.data.raw,pop.data, by=c('location_id', 'year_id','age_group_id','sex_id', 'ihme_loc_id'))
  
  # Get counts of ensemble deaths
  # Ensembled data are at count level already, no need to calculate
  cod.pop.merged[,deaths := mean]    
} else {
  
  ###### For non-Group1 countries ##########
  # Get GPR mortality results (per 100 people, might be worth changing in the long term)
  cod.data <- fread('/ihme/hiv/st_gpr/spectrum_gpr_results.csv')  ## update to time
  

  if(grepl('CHN', loc)){
    cod.data <- fread('/ihme/hiv/st_gpr/spectrum_gpr_results_20190723_china_2019scalars.csv') 
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
#obs.years <- pmax(min(cod.years), 1990):2019

# Merge both death datasets together and calculate the ratio
merged.deaths <- merge(merged.cod.data, merged.inc.data[year %in% cod.years], by=c('year','single.age','sex'))
merged.deaths <- merged.deaths[year %in% obs.years]
merged.deaths[,r:=single.cod/single.d]


#####################

merged.deaths[single.d==0,r:=1]



# Convert ratios to wide format (wide on draws)
wide.deaths <- data.table(dcast(merged.deaths[,.(year, single.age, sex, r, run_num)], year+single.age+sex ~ run_num, value.var=c('r')))
setnames(wide.deaths, as.character(1:n.draws), paste0('r_',1:n.draws))

######### Read in spectrum duration run data #############
duration.path <- paste0("/ihme/hiv/spectrum_draws/", run.folder, "/compiled/stage_1/")

# Get data from duration Spectrum run
duration.data <- fread(paste0(duration.path, stage1.loc,'_cohort.csv'))
duration.data[, sex := as.character(sex)]
duration.data[sex == '1', sex := 'male']
duration.data[sex == '2', sex := 'female']


wide.reshaped.temp <- data.table()

rho.fun <- function(run_id) {	
  print(run_id)
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
  wide.reshaped <- data.table(dcast(in.data[,.( inf_year, inf.age, year, single.age, sex, rho, run_num)],
                                    inf_year+inf.age+year+single.age+sex ~ run_num, value.var=c('rho')))
  setnames(wide.reshaped, as.character(run_id), paste0('rho_',run_id))
  
  if (run_id==1) {
    wide.reshaped.temp <- wide.reshaped
  } else {
    wide.reshaped.temp <- wide.reshaped[,6,with=F]		
  }
}


wide.reshaped.all.list <- mclapply(1:n.draws, rho.fun, mc.cores = 2)
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
combined.r.dt <- wide.combined.data[,lapply(.SD, sum, na.rm=T),by=.(inf_year, inf.age, sex), 
                                    .SDcols=paste0('combined.r_',1:n.draws)]

# Once we aggregate we can think of inf_year and inf_age as
# chronological year and age because we only have one observation per cohort
setnames(combined.r.dt, c('inf_year', 'inf.age'), c('year', 'single.age'))

# Identify last year
max.year <- inc.data[,max(year)]

# Restrict to appropriate years
combined.r.dt <- combined.r.dt[year < max.year]

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
out.dir_run <- paste0("/ihme/hiv/ciba_temp/", run.folder, "_adj/")
dir.create(out.dir_run, showWarnings=FALSE, recursive=TRUE)
write.csv(out.data, paste0(out.dir_run, loc,'_SPU_inc_draws.csv'), row.names=F)



### Ciba data preparation end 


if(!grepl("IND", loc)) {
  ######## Calculate net adjustment ratio for use in places without VR
  ### calculate the input incidence percentage 
  inc.pop.data <- merge(wide.inc.data, wide.pop.data.age, by=c('year', 'single.age', 'sex'))
  inc.pct.data <- inc.pop.data[,.(year, single.age, sex)]
  # Convert to rate per susceptible person and multiply by 100
  alloc.col(inc.pct.data, 1003)
  for (i in 1:n.draws) {
    set(inc.pct.data,j=paste0('draw_input_',i),value=100*inc.pop.data[[paste0('single.cases_',i)]]/inc.pop.data[[paste0('single.pop_',i)]])
  }
  
  ####
  inc.in.out.data <- merge(inc.pct.data, out.data, by=c('year', 'single.age', 'sex'))
  inc.ratio.data <- inc.in.out.data[,.(year, single.age, sex)]
  # Convert to rate per susceptible person and multiply by 100
  alloc.col(inc.ratio.data, 1003)
  for (i in 1:n.draws) {
    ratio.temp <- inc.in.out.data[[paste0('draw',i)]]/inc.in.out.data[[paste0('draw_input_',i)]] 	
    ratio.temp[is.nan(ratio.temp)] <- 1
    ratio.temp[is.infinite(ratio.temp)] <- 1
    ratio.temp[ratio.temp==0] <- 1
    set(inc.ratio.data,j=paste0('ratio_',i),value=ratio.temp)  ## set all NA and 0 as 1 for ratio:no change from input inc
  }
  
  out.ratio.dir_run <- paste0('/ihme/hiv/ciba_temp/', run.folder, '_ratios/')
  dir.create(out.ratio.dir_run, showWarnings=FALSE, recursive=TRUE)
  write.csv(inc.ratio.data, paste0(out.ratio.dir_run, loc,'_inc_ratios.csv'), row.names=F)	
  if(grepl("IDN", loc)) {
    out.ratio.IDN.dir <- '/ihme/hiv/ciba_temp/current_run_IDN_ratios/'
    dir.create(out.ratio.IDN.dir, showWarnings=FALSE, recursive=TRUE)
    write.csv(inc.ratio.data, paste0(out.ratio.IDN.dir, loc,'_inc_ratios.csv'), row.names=F)	
  }
  out.ratio.dir <- '/ihme/hiv/ciba_temp/current_run_ratios/'
  dir.create(out.ratio.dir, showWarnings=FALSE, recursive=TRUE)
  write.csv(inc.ratio.data, paste0(out.ratio.dir, loc,'_inc_ratios.csv'), row.names=F)	
  
}

