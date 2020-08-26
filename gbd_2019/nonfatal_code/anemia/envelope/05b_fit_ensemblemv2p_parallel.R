###########################################################
### Project: Anemia
### Purpose: Parallelized jobs for distribution fitting
###########################################################


#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
j <- "FILEPATH"
h <-"FILEPATH"
  
arg <- commandArgs()[-(1:3)]  


# load packages
pacman::p_load(data.table,actuar)

print("CHECKING FUNCTION EXISTS")
exists("pgumbel", mode = "function")


# set parameters from input arguments
code.dir <- arg[1]
output_root <- arg[2]
loc <- as.numeric(arg[3])
draws.required <- as.numeric(arg[4])
distribution <- arg[5]
print(paste("code.dir", code.dir))
print(paste("output_root", output_root))
print(paste("loc", loc))

#***********************************************************************************************************************

ages <- c(2:20, 30:32, 235)
years <- c(1990:2019)
thresholds <- paste0("FILEPATH/anemia_thresholds.csv")
test = F

#Distribution Functions
source(paste0(code.dir, "/DistList_mv2P.R"))
XMAX = 220
#Load ensemble weights - in future this will be automated but this was from dx optimization 
w = c(0,0.4,0,0.6,0)

#----MODEL-------------------------------------------------------------------------------------------------------------

###################
### LOAD DRAWS ####
###################
id_vars <- c("measure_id", "location_id", "year_id", "age_group_id", "sex_id")
by_vars <- c("measure_id", "location_id", "year_id", "age_group_id", "sex_id", "draw")
id_vars.p <- id_vars[id_vars != "measure_id"]

# Cluster - get_draws
if(Sys.info()["sysname"] == "Linux") {
  #Mean/stdev 
  source(paste0("FILEPATH/get_draws.R"))
  print("LOADING DRAWS FOR MEAN/STDEV")
  means <- get_draws(location_id = loc, gbd_id = 10487, measure_id = 19, year_id = years, age_group_id=ages, gbd_id_type = "modelable_entity_id", source="epi", status = 'best', decomp_step='step4', gbd_round_id=6)
  stdev <- get_draws(location_id = loc, gbd_id = 10488, measure_id = 19, year_id = years, age_group_id=ages, gbd_id_type = "modelable_entity_id", source="epi", status = 'best', decomp_step='step4', gbd_round_id=6)
  #age-specific fertility
  print("LOADING ASFR COVARIATE ESTIMATES")
  source("FILEPATH/get_covariate_estimates.R")
  asfr <- get_covariate_estimates(13,location_id=loc,decomp_step='step4',gbd_round_id=6)
  }


means[, c("modelable_entity_id", "model_version_id", "metric_id") := NULL]
stdev[, c("modelable_entity_id", "model_version_id", "metric_id") := NULL]

means.l <- melt(means, id.vars = id_vars, variable.name = "draw", value.name = "mean")
stdev.l <- melt(stdev, id.vars = id_vars, variable.name = "draw", value.name = "stdev")

df <- merge(means.l, stdev.l, by = by_vars)
df[, variance := stdev ^ 2]


### CALCULATE PREGNANCY RATE 

#Age-spec-preg-prev = (ASFR + stillbirth) * 46/52
setnames(asfr, "mean_value", "asfr")

#stillbirths 
still <- get_covariate_estimates(2267,location_id=loc,decomp_step='step4',gbd_round_id=6)[,.(location_id,year_id,mean_value)]
setnames(still,"mean_value","sbr_mean")

#Merge - stillbirths are only location-year specific 
df.p <- merge(asfr, still, by = c("location_id", "year_id"))
#Stillbirth_mean is still births per live birth
df.p[, prev_pregnant := (asfr + (sbr_mean * asfr)) * 46/52  ]
print(max(df.p$prev_pregnant))
#if(max(df.p$prev_pregnant) > 0.5) stop("PREGNANCY PREV OVER 50% - CHECK MATH?") 
if(max(df.p[sex_id == 1]$prev_pregnant) > 0) stop("PREGNANT MALES - CHECK MATH?") 

#Subset to pregnant 
df.p <- df.p[prev_pregnant > 0 & year_id %in% years & age_group_id %in% ages]
#Anemia threshold for <15 does NOT depend on pregnancy
df.p <- df.p[age_group_id >= 8]

#Pregnant prev 
preg_prev <- copy(df.p)
preg_prev <- preg_prev[, c(id_vars.p, "prev_pregnant"), with = FALSE]

#Merge to mean df
df.p <- df.p[, c("location_id", "year_id", "age_group_id", "sex_id", "prev_pregnant"), with = FALSE]
df.p <- merge(df, df.p, by = c("location_id", "year_id", "age_group_id", "sex_id"))

df[, pregnant := 0]
df.p[, pregnant := 1]

#Calc pregnant mean & stdev 
df.p[, mean := mean*0.919325]
df.p[, variance := variance * (1.032920188 ^ 2)]
df.p[, stdev := sqrt(variance)]

df.preg <- copy(df.p)
df.preg[, prev_pregnant := NULL]

df <- rbind(df, df.preg)

#MAP THRESHOLDS
thresh <- fread(thresholds)
pre <- nrow(df)
df <- merge(df, thresh, by = c("age_group_id", "sex_id", "pregnant"))
post <- nrow(df)
if (pre != post) stop("NOT ALL OBSERVATIONS MERGED TO THRESHOLD MAP")

if(test == T){
  df <- df[draw %in% c("draw_1", "draw_2")]
}
if(test == F){
  print("Using all draws")
}

###FULL VERSION FOR ALL 5 DIST
ens_mv2prev <- function(q, mn, vr, w){
  x = q
  ##parameters
  params_weibull = weibull_mv2p(mn, vr)
  params_gamma = gamma_mv2p(mn, vr)
  params_mirgamma = mirgamma_mv2p(mn, vr)
  params_mgumbel = mgumbel_mv2p(mn, vr)
  params_mirlnorm <- mirlnorm_mv2p(mn, vr)
  
  ##weighting
  prev = sum(
    w[1] * pweibull(x, params_weibull$shape,params_weibull$scale),
    w[2] * pgamma(x, params_gamma$shape,params_gamma$rate), 
    w[3] * pmirgamma(x, params_mirgamma$shape,params_mirgamma$rate),
    w[4] * pmgumbel(x,params_mgumbel$alpha,params_mgumbel$scale, lower.tail = T),
    w[5] * pmirlnorm(x,params_mirlnorm$meanlog,params_mirlnorm$sdlog))
  prev
  
}

###ABBREVIATED - FOR JUST GAMMA AND MGUMBEL
ens_mv2prev <- function(q, mn, vr, w){
  x = q
  ##parameters
  #params_weibull = weibull_mv2p(mn, vr)
  params_gamma = gamma_mv2p(mn, vr)
  #params_mirgamma = mirgamma_mv2p(mn, vr)
  params_mgumbel = mgumbel_mv2p(mn, vr)
  #params_mirlnorm <- mirlnorm_mv2p(mn, vr)
  
  ##weighting
  prev = sum(
    w[2] * pgamma(x, params_gamma$shape,params_gamma$rate), 
    w[4] * pmgumbel(x,params_mgumbel$alpha,params_mgumbel$scale, lower.tail = T)
  )
  prev
  
}

### CALCULATE PREVALENCE 
print("CALCULATING MILD")
df[, mild := ens_mv2prev(hgb_upper_mild, mean, variance, w = w) - ens_mv2prev(hgb_lower_mild, mean, variance, w = w)
   , by = 1:nrow(df)]
print("CALCULATING MOD")
df[, moderate := ens_mv2prev(hgb_upper_moderate, mean, variance, w = w) - ens_mv2prev(hgb_lower_moderate, mean, variance, w = w)
   , by = 1:nrow(df)]
print("CALCULATING SEV")
df[, severe := ens_mv2prev(hgb_upper_severe, mean, variance, w = w) - ens_mv2prev(hgb_lower_severe, mean, variance, w = w)
   , by = 1:nrow(df)]
#Anemic is the sum
df[, anemic := mild + moderate + severe]

sevs <- c("mild", "moderate", "severe", "anemic")
###PREGNANCY ADJUSTMENT
df.p <- df[pregnant == 1]
df.p <- merge(df.p, preg_prev, by = id_vars.p, all = TRUE)
df.p <- df.p[, c(by_vars, sevs, "prev_pregnant"), with = FALSE]
lapply(sevs, function(s) setnames(df.p, s, paste(s, "preg", sep = "_")))


df <- df[pregnant == 0]

nrow(df)
df <- merge(df, df.p, by = by_vars, all = TRUE)
nrow(df)


#weighted sum
test <- df[!is.na(prev_pregnant)]
lapply(sevs, function(sev) df[ !is.na(prev_pregnant) , c(sev) := get(sev) * (1 - prev_pregnant) + get(paste0(sev, "_preg")) * prev_pregnant ]) 
test2 <- df[!is.na(prev_pregnant)]


#Everything is prevalence (means/stdev entered as continuous)
df[,measure_id := 5]

#RESHAPE
mild <- df[,c(by_vars, "mild"), with = F]
mild <- dcast(mild, ... ~ draw, value.var = "mild", drop = T, fill = NA)
moderate <- df[,c(by_vars, "moderate"), with = F]
moderate <- dcast(moderate, ... ~ draw, value.var = "moderate", drop = T, fill = NA)
severe <- df[,c(by_vars, "severe"), with = F]
severe <- dcast(severe, ... ~ draw, value.var = "severe", drop = T, fill = NA)
anemic <- df[,c(by_vars, "anemic"), with = F]
anemic <- dcast(anemic, ... ~ draw, value.var = "anemic", drop = T, fill = NA)

#SAVE
map.meid <- c(10489, 10490, 10491, 10507)
map.sev <- c("mild", "moderate", "severe", "anemic")
map <- cbind(map.meid, map.sev)

meids <- unique(map.meid)
for(meid in meids){
  print(paste("SAVING", meid))
  
  output_df <- get(map[map.meid == meid, 2])
  output_dir <- paste0(output_root, "/", meid)
  filename <- paste0(loc, ".csv")
  dir.create(file.path(output_dir), showWarnings = FALSE)
  write.csv(output_df, paste0(output_dir, "/", filename), row.names = F)
  
}  
