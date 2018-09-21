


#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())


# load packages
pacman::p_load(data.table, actuar)

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
years <- c(1990, 1995, 2000, 2005, 2010, 2016)
thresholds <- paste0(j, FILEPATH)
test = F

#Distribution Functions
source(paste0(code.dir, "/DistList_mv2P.R"))
XMAX = 220
#Load ensemble weights 
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
    source(paste0(j,"FILEPATH"))
    print("LOADING DRAWS FOR MEAN/STDEV")
    means <- get_draws(location_ids = loc, gbd_id = 10487, measure_ids = 19, year_ids = years, age_group_ids=ages, gbd_id_field = "modelable_entity_id", source="epi")
    stdev <- get_draws(location_ids = loc, gbd_id = 10488, measure_ids = 19, year_ids = years, age_group_ids=ages, gbd_id_field = "modelable_entity_id", source="epi")
  #age-specific fertility
    source(paste0(j, "FILEPATH"))
    cov <- get_covariate_estimates(covariate_name_short = "ASFR") 
    asfr <- cov[location_id == loc]
  
  
means[, c("modelable_entity_id", "model_version_id") := NULL]
stdev[, c("modelable_entity_id", "model_version_id") := NULL]


means.l <- melt(means, id.vars = id_vars, variable.name = "draw", value.name = "mean")
stdev.l <- melt(stdev, id.vars = id_vars, variable.name = "draw", value.name = "stdev")

df <- merge(means.l, stdev.l, by = by_vars) # not yet draw level 
df[, variance := stdev ^ 2]




### CALCULATE PREGNANCY RATE 
  
  #Age-spec-preg-prev = (ASFR + stillbirth) * 46/52
  setnames(asfr, "mean_value", "asfr")
  asfr[, asfr_se := (upper_value - lower_value) / (2 * 1.96)]
  
  #stillbirths 
  still <- fread(paste0(j, FILEPATH))
  
  #Merge - stillbirths are only location-year specific 
  df.p <- merge(asfr, still, by = c("location_id", "year_id"))
  #Stillbirth_mean is still births per live birth
  df.p[, prev_pregnant := (asfr + (sbr_mean * asfr)) * 46/52  ]
 
  #Subset to pregnant 
  df.p <- df.p[prev_pregnant > 0 & year_id %in% years & age_group_id %in% ages]
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
  df.p[, mean := mean - (-19.874 + 0.2416 * mean)]
  df.p[, stdev := stdev - 0.2532]
  df.p[, variance := stdev ^ 2]
  
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


