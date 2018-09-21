
############################################################################################################
## Purpose: generate weighted average of met demand for contraception among women in specified age range 
###########################################################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH" 
  h <- "FILEPATH"
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library(data.table)
library(magrittr)
library(parallel)
library(matrixStats,lib.loc=file.path(h,"FILEPATH"))

# specify model, ages, and years to take average across (requires 1000 draws of met demand and of modern contraceptive use)
# as well as number of cores to parallelize across
demand_model <- 24056 #24056 18272
mod_model <- 24046 #24046 18271 
all_ages <- seq(8,14)
all_years <- seq(1990,2016)
ages <- all_ages
years <- all_years
cores.provided <- 10
first_run <- F # only True if first time running this st-gpr model (must be compiled together and saved)
adolescent <- F # if True, will restrict aggregation to age group ids 8 and 9, otherwise includes 8-14
global <- T # toggle for whether to generate global aggregate as well
version <- "" # if generating new results to compare to old ones, rather than overwrite them, replace with a number

# full dataset outpaths/inpaths
demand_results <- file.path(j,paste0("FILEPATH"))
mod_results <- file.path(j,paste0("FILEPATH"))

#  subset for adolescent analysis
type <- ""
if (adolescent) {
  type <- "_adolescent"
  ages <- c(8,9)
  years <- seq(1990,2015,5)
}

# output paths
weights_output <- file.path(j,paste0("FILEPATH",type,version,".csv"))
final_output <- file.path(j,paste0("FILEPATH",type,version,".csv"))
final_output_draws <- file.path(j,paste0("FILEPATH",type,version,".csv"))
global_output <- file.path(j,paste0("FILEPATH",type,version,".csv"))
global_output_draws <- file.path(j,paste0("FILEPATH",type,version,".csv"))
map_output_root <- file.path(j,paste0("FILEPATH",type,version,"_map"))

#**********************************************************
# READ IN AND SAVE ST-GPR RESULTS
#**********************************************************

# read in ST-GPR age-specific results for met demand and modern contraceptive prevalence, compile, and write to FILEPATH
if (first_run) {
  files <- list.files(file.path("FILEPATH"))
  demand <- rbindlist(lapply(file.path("FILEPATH",files),fread))
  demand <- demand[age_group_id %in% all_ages & year_id %in% all_years, -c("measure_id"), with = F]
  demand[,sex_id := 2]
  demand <- unique(demand)
  write.csv(demand,demand_results,row.names=F)
  
  files <- list.files(file.path("FILEPATH"))
  mod <- rbindlist(lapply(file.path("FILEPATH",files),fread))
  mod <- mod[age_group_id %in% all_ages & year_id %in% all_years, -c("measure_id"), with = F]
  mod[,sex_id := 2]
  mod <- unique(mod)
  write.csv(mod,mod_results,row.names=F)
}

#**********************************************************
# GENERATE AGGREGATES
#**********************************************************

# read in populations and locations
source(file.path(j,"FILEPATH"))
source(file.path(j,"FILEPATH"))
locations <- get_location_metadata(gbd_round_id = 4, location_set_id = 22)
locs <- copy(locations[, c("location_id","ihme_loc_id"), with=F])
pops <- get_population(location_set_version_id = 149, year_id = years, sex_id = 2, location_id = -1, 
                       age_group_id = ages) %>% data.table
pops <- pops[,-c("process_version_map_id"),with=F]

# read in data sets, subset, and merge together along with population estimates
demand <- fread(demand_results)
mod <- fread(mod_results)
demand <- demand[age_group_id %in% ages & year_id %in% years]
mod <- mod[age_group_id %in% ages & year_id %in% years]
draws <- paste0("draw_",seq(0,999))
id.vars <- c("location_id","year_id","age_group_id","sex_id")
setnames(demand,draws,paste0("demand_",seq(0,999)))
setnames(mod,draws,paste0("mod_",seq(0,999)))
df <- merge(demand, mod,by=id.vars)
df <- merge(df, pops,by=id.vars,all.x=T)
df <- merge(df, locs, by = id.vars[1])

# generate skeleton of id.vars
weights <- copy(df[,c(id.vars,"ihme_loc_id"),with=F])
# calculate number of people with need in each age group
weights[, (paste0("need_",seq(0,999))) := mclapply(seq(0,999),function(x) {df[,(get(paste0("mod_",x))/get(paste0("demand_",x)))*population]},mc.cores = cores.provided)]
# get total need across all age groups (by location)
weights[, (paste0("needtotal_",seq(0,999))) := mclapply(seq(0,999),function(x) {rep(weights[,sum(get(paste0("need_",x))),by=c(id.vars[1:2])]$V1,each=length(ages))},mc.cores = cores.provided)]

if (global == F) {
  # weight is the proportion of total need that an age groups makes up
  weights[, (paste0("weight_",seq(0,999))) := mclapply(seq(0,999),function(x) {get(paste0("need_",x))/get(paste0("needtotal_",x))},mc.cores = cores.provided)]
  # multiply each draw of met demand by its corresponding weight
  df[,(paste0("demand_",seq(0,999))) := lapply(seq(0,999),function(x) {df[,get(paste0("demand_",x))]*weights[,get(paste0("weight_",x))]})]
  # generate aggregate-age met demand by summing draws*weight by location-year
  df[,(paste0("demand_",seq(0,999))) := mclapply(seq(0,999),function(x) {rep(df[,sum(get(paste0("demand_",x))),by=c(id.vars[1:2])]$V1,each=length(ages))},mc.cores = cores.provided)]
  
  # write weight outputs
  write.csv(weights[,c("ihme_loc_id",id.vars,paste0("weight_",seq(0,999))),with=F],weights_output,row.names=F)
} else {  
  # get total need across all age groups and locations (need to drop subnationals for global to avoid double-counting locations)
  weights[!grepl("_",ihme_loc_id), (paste0("needglobal_",seq(0,999))) := mclapply(seq(0,999),function(x) {rep(weights[!grepl("_",ihme_loc_id),sum(get(paste0("need_",x))),by=c(id.vars[2])]$V1,each=length(ages))},mc.cores = cores.provided)]
  # weight is the proportion of total (or global) need that an location-age group makes up
  weights[, (paste0("weight_",seq(0,999))) := mclapply(seq(0,999),function(x) {get(paste0("need_",x))/get(paste0("needtotal_",x))},mc.cores = cores.provided)]
  weights[!grepl("_",ihme_loc_id), (paste0("weightglobal_",seq(0,999))) := mclapply(seq(0,999),function(x) {get(paste0("need_",x))/get(paste0("needglobal_",x))},mc.cores = cores.provided)]
  # multiply each draw of met demand by its corresponding weight
  df[!grepl("_",ihme_loc_id),(paste0("demandglobal_",seq(0,999))) := lapply(seq(0,999),function(x) {df[!grepl("_",ihme_loc_id),get(paste0("demand_",x))]*weights[!grepl("_",ihme_loc_id),get(paste0("weightglobal_",x))]})]
  df[,(paste0("demand_",seq(0,999))) := lapply(seq(0,999),function(x) {df[,get(paste0("demand_",x))]*weights[,get(paste0("weight_",x))]})]
  # generate aggregate-age met demand by summing draws*weight by location-year (or year for global)
  df[,(paste0("demand_",seq(0,999))) := mclapply(seq(0,999),function(x) {rep(df[,sum(get(paste0("demand_",x))),by=c(id.vars[1:2])]$V1,each=length(ages))},mc.cores = cores.provided)]
  df[!grepl("_",ihme_loc_id),(paste0("demandglobal_",seq(0,999))) := mclapply(seq(0,999),function(x) {rep(df[!grepl("_",ihme_loc_id),sum(get(paste0("demandglobal_",x))),by=c(id.vars[2])]$V1,each=length(ages))},mc.cores = cores.provided)]
  
  # write weight outputs
  write.csv(weights[,c("ihme_loc_id",id.vars,paste0("weight_",seq(0,999)),paste0("weightglobal_",seq(0,999))),with=F],weights_output,row.names=F)
  
  # subset global dataset and generate mean and CI
  global <- df[location_id == 6 & age_group_id == ages[1]]
  global[,age_group_id := 22]
  global[,location_id := 1]
  global[,ihme_loc_id := "G"]
  global[,met_demand_global := rowMeans(global[,c(paste0("demandglobal_",seq(0,999))),with=F])]
  global[,lower_global := lapply(seq(1,nrow(global)), function(x) quantile(as.vector(t(global[x,c(paste0("demandglobal_",seq(0,999))),with=F])),0.025))]
  global[,lower_global := as.vector(unlist(lower_global))]
  global[,upper_global := lapply(seq(1,nrow(global)), function(x) quantile(as.vector(t(global[x,c(paste0("demandglobal_",seq(0,999))),with=F])),0.975))]
  global[,upper_global := as.vector(unlist(upper_global))]
  
  # write global outputs
  write.csv(global[,c("ihme_loc_id",id.vars,paste0("demandglobal_",seq(0,999))),with=F],global_output_draws,row.names=F)
  write.csv(global[,c("ihme_loc_id",id.vars,"met_demand_global","lower_global","upper_global"),with=F],global_output,row.names=F)
}

# copy entire output for interactive error-checking
all_df <- copy(df)

# remove redundant rows
df <- df[age_group_id == ages[1]]
df[,age_group_id := 22]

# calculate mean and CI
df[,met_demand := rowMeans(df[,c(paste0("demand_",seq(0,999))),with=F])]
df[,lower := lapply(seq(1,nrow(df)), function(x) quantile(as.vector(t(df[x,c(paste0("demand_",seq(0,999))),with=F])),0.025))]
df[,lower := as.vector(unlist(lower))]
df[,upper := lapply(seq(1,nrow(df)), function(x) quantile(as.vector(t(df[x,c(paste0("demand_",seq(0,999))),with=F])),0.975))]
df[,upper := as.vector(unlist(upper))]

# write location-specific outputs
write.csv(df[,c("ihme_loc_id",id.vars,paste0("demand_",seq(0,999))),with=F],final_output_draws,row.names=F)
write.csv(df[,c("ihme_loc_id",id.vars,"met_demand","lower","upper"),with=F],final_output,row.names=F)

#**********************************************************
# MAP RESULTS
#**********************************************************

file.path(j,"FILEPATH") %>% source

map.data <- df[,list(ihme_loc_id,year_id,met_demand)]
setnames(map.data,"met_demand","mapvar")
  
# SET SPECIFICATIONS BASED ON TYPE OF NEED BEING MODELED
datamax <- 1
def_long <- "Met demand among " # "Unmet demand among " # "Unmet need as a proportion of all "
def_short <- "Met demand " # "Unmet demand " # "unmet need "
age_text <- "15-49"
if (adolescent) age_text <- "15-24"

map.data90 <- map.data[year_id == 1990,list(ihme_loc_id,mapvar)]
map.data95 <- map.data[year_id == 1995,list(ihme_loc_id,mapvar)]
map.data00 <- map.data[year_id == 2000,list(ihme_loc_id,mapvar)]
map.data05 <- map.data[year_id == 2005,list(ihme_loc_id,mapvar)]
map.data10 <- map.data[year_id == 2010,list(ihme_loc_id,mapvar)]
map.data15 <- map.data[year_id == 2015,list(ihme_loc_id,mapvar)]

gbd_map(map.data90, limits = seq(0, datamax, .1), col.reverse = F, na.color = "#D3D3D3",
        title = paste0(def_long,"women ",age_text," in 1990"),
        legend.title = paste0(def_short,"for contraception"),
        fname = paste0(map_output_root,"1990.pdf"))

gbd_map(map.data95, limits = seq(0, datamax, .1), col.reverse = F, na.color = "#D3D3D3",
        title = paste0(def_long,"women ",age_text," in 1995"),
        legend.title = paste0(def_short,"for contraception"),
        fname = paste0(map_output_root,"1995.pdf"))

gbd_map(map.data00, limits = seq(0, datamax, .1), col.reverse = F, na.color = "#D3D3D3",
        title = paste0(def_long,"women ",age_text," in 2000"),
        legend.title = paste0(def_short,"for contraception"),
        fname = paste0(map_output_root,"2000.pdf"))

gbd_map(map.data05, limits = seq(0, datamax, .1), col.reverse = F, na.color = "#D3D3D3",
        title = paste0(def_long,"women ",age_text," in 2005"),
        legend.title = paste0(def_short,"for contraception"),
        fname = paste0(map_output_root,"2005.pdf"))

gbd_map(map.data10, limits = seq(0, datamax, .1), col.reverse = F, na.color = "#D3D3D3",
        title = paste0(def_long,"women ",age_text," in 2010"),
        legend.title = paste0(def_short,"for contraception"),
        fname = paste0(map_output_root,"2010.pdf"))

gbd_map(map.data15, limits = seq(0, datamax, .1), col.reverse = F, na.color = "#D3D3D3",
        title = paste0(def_long,"women ",age_text," in 2015"),
        legend.title = paste0(def_short,"for contraception"),
        fname = paste0(map_output_root,"2015.pdf"))
