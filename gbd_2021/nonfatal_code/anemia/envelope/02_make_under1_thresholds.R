###################################################################
### Project: Anemia
### Purpose: Generate under-1 anemia thresholds based on normal Hgb
###################################################################

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
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_population.R")
source("FILEPATH/utility.r")
source("FILEPATH/collapse_point.R")

# Args
code.dir <- paste0("FILEPATH")
age_ids <- c(2:3, 388, 389, 238, 34, 6:20, 30:32, 235)
age_map <- data.table(age_group_id = c(2:3, 388, 389, 238, 34, 6:20, 30:32, 235),
                      age = c(1:25))
ages <- get_ids("age_group")
years <- c(1980:2022)
thresholds <- fread("FILEPATH/anemia_thresholds.csv")
loc_meta <- get_location_metadata(location_set_id=9,gbd_round_id=7)
loc_meta <- loc_meta[is_estimate == 1 & most_detailed == 1]
locs <- loc_meta$location_id

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

means <- get_model_results("epi",10487,age_group_id=age_ids,location_id=locs,year_id=years,sex_id=c(1,2),model_version_id=500480,decomp_step="iterative")
stdev <- get_model_results("epi",10488,age_group_id=age_ids,location_id=locs,year_id=years,sex_id=c(1,2),model_version_id=500969,decomp_step="iterative")

means[, c("crosswalk_version_id", "bundle_id","model_version_id", "measure_id","measure","lower","upper") := NULL]
stdev[, c("crosswalk_version_id", "bundle_id","model_version_id", "measure_id","measure","lower","upper") := NULL]
setnames(stdev,"mean","stdev")

df <- merge(means, stdev, by = c("location_id","age_group_id","sex_id","year_id"))
df[, variance := stdev ^ 2]
df[, sex := ifelse(sex_id==1,"Male","Female")]
df <- merge(df,age_map,by="age_group_id",all.x=T)
df <- merge(df,ages,by="age_group_id",all.x=T)
df[age_group_name=="Early Neonatal", age_group_name := "ENN"]
df[age_group_name=="Late Neonatal", age_group_name := "LNN"]
age_names <- unique(df$age_group_name)

df[, pct50 := ens_mv2prev(.50, mean, variance, w = w), by = 1:nrow(df)]

# Pop aggregate
pops <- get_population(age_group_id=age_ids, location_id=locs, year_id=years, sex_id=c(1,2), gbd_round_id=7, decomp_step='iterative')
pops[,run_id := NULL]

df <- df[age_group_id %in% c(2,3,388,389)]
df <- merge(df,pops,by=c("location_id","age_group_id","year_id","sex_id"),all.x=TRUE)
df[, mean := weighted.mean(pct50,w=population), by=c("age_group_id")]
df <- unique(df[,.(age_group_id,mean)])

# Make ratio
df[, ratio := mean/df[age_group_id==389,mean]]

# Adjust thresholds
save <- thresholds[!(age_group_id %in% c(2,3,388,389))]
thresholds <- thresholds[age_group_id %in% c(2,3,388,389)]
thresholds <- merge(thresholds,df[,.(age_group_id,ratio)],by="age_group_id",all.x=TRUE)

thresholds[, hgb_lower_mild := round(thresholds[age_group_id==389,hgb_lower_mild] * ratio/5)*5]
thresholds[, hgb_upper_mild := round(thresholds[age_group_id==389,hgb_upper_mild] * ratio/5)*5]
thresholds[, hgb_lower_moderate := round(thresholds[age_group_id==389,hgb_lower_moderate] * ratio/5)*5]
thresholds[, hgb_upper_moderate := round(thresholds[age_group_id==389,hgb_upper_moderate] * ratio/5)*5]
thresholds[, hgb_lower_severe := round(thresholds[age_group_id==389,hgb_lower_severe] * ratio/5)*5]
thresholds[, hgb_upper_severe := round(thresholds[age_group_id==389,hgb_upper_severe] * ratio/5)*5]
thresholds[, hgb_lower_anemic := 0]
thresholds[, hgb_upper_anemic := hgb_upper_mild]
thresholds[, ratio := NULL]

# append back >1 thresholds
thresholds <- rbind(thresholds,save)

# Write out
write.csv(thresholds,"FILEPATH/anemia_thresholds.csv",row.names=F)
## END