library(tidyverse)
library(data.table)
library(RMySQL)
default <- 64
run <- 25

cf_vers <- fread(FILEPATH)
cf_vers <- cf_vers[cf_version > 5]
cf_vers[, cf := NULL]
myconn <- RMySQL::dbConnect(CONN)

# Query for estimate IDs that we actually release
bundles <- dbGetQuery(myconn, QUERY)

dbDisconnect(myconn) 


bundles <- merge(bundles, cf_vers, by='bundle_id', all=TRUE) %>% as.data.table()
bundles[is.na(cf_version), cf_version := default]
bundles[, index := 1]

cf <- data.table(cf = c('cf1','cf2','cf3'), index = c(1,1,1))
bundles <- merge(bundles, cf, by='index', allow.cartesian=TRUE)
bundles[,index := NULL]

bundles[, cf_ests := FILEPATH]
bundles[, draws := FILEPATH]

cf_estimates <- lapply(bundles$cf_ests, function(f){
  if(file.exists(f)){
    df <- fread(f)
    df <- df[,c('age_start','age_group_id','age_midpoint','sex_id','bundle_id','cf_type','cf_mean','cf_median','upper','lower')]
  }else{
    df <- NA
  }
  return(df)
})
cf_estimates <- cf_estimates[!is.na(cf_estimates)]
cf_estimates <- rbindlist(cf_estimates)
fwrite(cf_estimates,FILEPATH)

cf_draws <- lapply(bundles$draws, function(f){
  if(file.exists(f)){
    df <- fread(f)
  }else{
    df <- NA
  }
  return(df)
})
cf_draws <- cf_draws[!is.na(cf_draws)]
cf_draws <- rbindlist(cf_draws, fill = TRUE)
cf_draws[,index.x := NULL][, index.y := NULL]

cf_draws <- split(cf_draws, by=c('age_group_id','sex_id'))
lapply(names(cf_draws), function(x){
  name <- str_replace(x,'\\.','_')
  fwrite(cf_draws[[x]],FILEPATH)
})
