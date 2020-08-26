###########################################################
### Project: Anemia
### Purpose: Generate "Normal" Hemoglobin Covariate
###########################################################

###################
### Setting up ####
###################
rm(list=ls())
pacman::p_load(data.table, dplyr, ggplot2, stats, boot, actuar)

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH", user)
}

# Source Functions
source("FILEPATH/get_draws.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_population.R")
source("FILEPATH/utility.r")
source("FILEPATH/collapse_point.R")

# Args
code.dir <- paste0("FILEPATH")
age_ids <- c(2:20, 30:32, 235)
age_map <- data.table(age_group_id = c(2:20,30:32,235),age = c(1:23))
ages <- get_ids("age_group")
years <- c(1980:2019)
thresholds <- paste0("FILEPATH/anemia_thresholds.csv")
locs_meta <- fread("FILEPATH/locs.csv")

# Distribution Functions
source(paste0(code.dir, "/DistList_mv2P.R"))
XMAX = 220
# Load ensemble weights 
w = c(0,0.4,0,0.6,0)
# Ensemble function
qmgumbel_mod <- function(p, alpha, scale) {
  XMAX - (qgumbel(p, alpha, scale))
}

ens_mv2prev <- function(q, mn, vr, w){
  x = q
  ##parameters
  params_gamma = gamma_mv2p(mn, vr)
  params_mgumbel = mgumbel_mv2p(mn, vr)
  
  ##weighting
  prev = sum(
    w[2] * qgamma(x, params_gamma$shape,params_gamma$rate), 
    w[4] * qmgumbel_mod(1-x, params_mgumbel$alpha, params_mgumbel$scale)
  )
  prev
}

#############################################
############### PULL AND PLOT ###############
#############################################

means <- get_draws(location_id = c(1), gbd_id = 10487, measure_id = 19, year_id = years, age_group_id=age_ids, gbd_id_type = "modelable_entity_id", source="epi", status = 'best', gbd_round_id = 6, decomp_step='step2')
stdev <- get_draws(location_id = c(1), gbd_id = 10488, measure_id = 19, year_id = years, age_group_id=age_ids, gbd_id_type = "modelable_entity_id", source="epi", status = 'best', gbd_round_id = 6, decomp_step='step2')

means[, c("modelable_entity_id", "model_version_id", "metric_id") := NULL]
stdev[, c("modelable_entity_id", "model_version_id", "metric_id") := NULL]

means <- collapse_point(means)[,.(age_group_id,location_id,sex_id,year_id,mean)]
stdev <- collapse_point(stdev)[,.(age_group_id,location_id,sex_id,year_id,mean)]
setnames(stdev,"mean","stdev")

df <- merge(means, stdev, by = c("location_id","age_group_id","sex_id","year_id"))
df[, variance := stdev ^ 2]
df[, sex := ifelse(sex_id==1,"Male","Female")]
df <- merge(df,age_map,by="age_group_id",all.x=T)
df <- merge(df,ages,by="age_group_id",all.x=T)
df[age_group_name=="Early Neonatal", age_group_name := "ENN"]
df[age_group_name=="Late Neonatal", age_group_name := "LNN"]
df[age_group_name=="Post Neonatal", age_group_name := "PNN"]
age_names <- unique(df$age_group_name)

df[, pct50 := ens_mv2prev(.50, mean, variance, w = w), by = 1:nrow(df)]


# Pop aggregate
pops <- get_population(age_group_id=age_ids, location_id=1, year_id=years, sex_id=c(1,2), decomp_step='step4')
pops[,run_id := NULL]

df <- merge(df,pops,by=c("location_id","age_group_id","year_id","sex_id"),all.x=TRUE)
df[, mean := weighted.mean(pct50,w=population), by=c("age_group_id","sex_id")]
df <- unique(df[,.(age_group_id,sex_id,mean)])
setnames(df,"mean","mean_value")

# Make square
loc_ids <- get_location_metadata(22)[,c("location_id")][[1]]
dt <- data.table(expand.grid(location_id=loc_ids, sex_id=c(1,2), year_id=c(1980:2019), age_group_id=age_ids))
dt <- merge(dt,df,by=c("age_group_id","sex_id"),all.x=TRUE)
dt[,covariate_name_short := "normal_hemoglobin"]

# Write out
write.csv(dt,"FILEPATH/normal_hemoglobin.csv",row.names=F)

## END