
############################################################################################################
## Purpose: generate weighted average of modern contraceptive usage among women in specified age range 
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

# full dataset outpath/inpath
mod_results <- file.path(j,paste0("FILEPATH"))

#  subset for adolescent analysis
type <- ""
if (adolescent) {
  type <- "_adolescent"
  ages <- c(8,9)
  years <- seq(1990,2015,5)
}

# output paths
weights_output <- file.path(j,paste0("FILEPATH"))
final_output <- file.path(j,paste0("FILEPATH"))
final_output_draws <- file.path(j,paste0("FILEPATH"))
global_output <- file.path(j,paste0("FILEPATH"))
global_output_draws <- file.path(j,paste0("FILEPATH"))
map_output_root <- file.path(j,paste0("FILEPATH"))


#**********************************************************
# READ IN AND SAVE ST-GPR RESULTS
#**********************************************************

# read in ST-GPR age-specific results for met demand and modern contraceptive prevalence, compile, and write to FILEPATH
if (first_run) {
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

# read in dataset, subset, and merge together along with population estimates
mod <- fread(mod_results)
mod <- mod[age_group_id %in% ages & year_id %in% years]
draws <- paste0("draw_",seq(0,999))
id.vars <- c("location_id","year_id","age_group_id","sex_id")
df <- merge(mod, pops,by=id.vars,all.x=T)

df[,poptotal := sum(population),by=c(id.vars[1:2])]
df[,weight := population/poptotal]
# df[,sum(weight),by=c(id.vars[1:2])]
df[,(paste0("draw_",seq(0,999))) := mclapply(seq(0,999),function(x) {df[,get(paste0("draw_",x))*weight]},mc.cores = cores.provided)]
df[,(paste0("draw_",seq(0,999))) := mclapply(seq(0,999),function(x) {rep(df[,sum(get(paste0("draw_",x))),by=c(id.vars[1:2])]$V1,each=length(ages))},mc.cores = cores.provided)]

df <- df[age_group_id == ages[1]]
df <- df[,age_group_id := 22] 

# calculate mean and CI
df[,mod_contra := rowMeans(df[,c(paste0("draw_",seq(0,999))),with=F])]
df[,lower := lapply(seq(1,nrow(df)), function(x) quantile(as.vector(t(df[x,c(paste0("draw_",seq(0,999))),with=F])),0.025))]
df[,lower := as.vector(unlist(lower))]
df[,upper := lapply(seq(1,nrow(df)), function(x) quantile(as.vector(t(df[x,c(paste0("draw_",seq(0,999))),with=F])),0.975))]
df[,upper := as.vector(unlist(upper))]

# add on ihme_loc_ids
df <- merge(df, locs, by = id.vars[1])

write.csv(df[,c("ihme_loc_id",id.vars,"mod_contra","lower","upper"),with=F],final_output,row.names=F)

#**********************************************************
# MAPPING RESULTS
#**********************************************************

file.path(j,"FILEPATH") %>% source

map.data <- df[,list(ihme_loc_id,year_id,met_demand)]
setnames(map.data,"met_demand","mapvar")

# SET SPECIFICATIONS BASED ON TYPE OF CONTRACEPTION INDICATOR BEING MODELED
datamax <- 1
def_long <- "Modern contraceptive prevalence among " # "Unmet demand among " # "Unmet need as a proportion of all "
def_short <- "mCPR " # "Unmet demand " # "unmet need "

map.data90 <- map.data[year_id == 1990,list(ihme_loc_id,mapvar)]
map.data95 <- map.data[year_id == 1995,list(ihme_loc_id,mapvar)]
map.data00 <- map.data[year_id == 2000,list(ihme_loc_id,mapvar)]
map.data05 <- map.data[year_id == 2005,list(ihme_loc_id,mapvar)]
map.data10 <- map.data[year_id == 2010,list(ihme_loc_id,mapvar)]
map.data15 <- map.data[year_id == 2015,list(ihme_loc_id,mapvar)]

gbd_map(map.data90, limits = seq(0, datamax, .1), col.reverse = F, na.color = "#D3D3D3",
        title = paste0(def_long,"women ages 15-24 in 1990"),
        legend.title = def_short,
        fname = paste0(map_output_root,"1990.pdf"))

gbd_map(map.data95, limits = seq(0, datamax, .1), col.reverse = F, na.color = "#D3D3D3",
        title = paste0(def_long,"women ages 15-24 in 1995"),
        legend.title = def_short,
        fname = paste0(map_output_root,"1995.pdf"))

gbd_map(map.data00, limits = seq(0, datamax, .1), col.reverse = F, na.color = "#D3D3D3",
        title = paste0(def_long,"women ages 15-24 in 2000"),
        legend.title = def_short,
        fname = paste0(map_output_root,"2000.pdf"))

gbd_map(map.data05, limits = seq(0, datamax, .1), col.reverse = F, na.color = "#D3D3D3",
        title = paste0(def_long,"women ages 15-24 in 2005"),
        legend.title = def_short,
        fname = paste0(map_output_root,"2005.pdf"))

gbd_map(map.data10, limits = seq(0, datamax, .1), col.reverse = F, na.color = "#D3D3D3",
        title = paste0(def_long,"women ages 15-24 in 2010"),
        legend.title = def_short,
        fname = paste0(map_output_root,"2010.pdf"))

gbd_map(map.data15, limits = seq(0, datamax, .1), col.reverse = F, na.color = "#D3D3D3",
        title = paste0(def_long,"women ages 15-24 in 2015"),
        legend.title = def_short,
        fname = paste0(map_output_root,"2015.pdf"))
