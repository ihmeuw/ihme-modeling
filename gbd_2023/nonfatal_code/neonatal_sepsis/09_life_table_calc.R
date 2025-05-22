########################################################################################################################
## Project: Neonatal Sepsis
## Purpose: Run Life-Table Calculation to Produce Prevalence at 28 days for long-term impairment
########################################################################################################################

# Set up environment
rm(list=ls())
os <- .Platform$OS.type
if (os=="Windows") {
  j<- "FILEPATH"
  h <-"FILEPATH"
  k <- "FILEPATH"
} else {
  j<- "FILEPATH/"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH", user)
  k <- "FILEPATH"
}
set.seed(112358)

code_dir <- "FILEPATH"
setwd(code_dir)
pacman::p_load(data.table, openxlsx, tidyr, ggplot2, plotly)
source(paste0(k,"FILEPATH/get_population.R"))
source(paste0(k,"FILEPATH/get_covariate_estimates.R"))
source(paste0(k,"FILEPATH/get_location_metadata.R"))
source(paste0(k,"FILEPATH/get_crosswalk_version.R"))
source(paste0(k,"FILEPATH/get_draws.R"))
source(paste0(k,"FILEPATH/get_demographics.R"))
source(paste0(k,"FILEPATH/get_envelope.R"))
source(paste0(k,"FILEPATH/get_life_table.R"))
source("FILEPATH/utility.r")

## Args
loc_id <- "all"
gbd_round <- 7

### Define Functions

## Get Best Run ID
best_run_id <- function(me) {
  df <- fread("FILEPATH/neonatal_sepsis_run_log.csv")
  run_id <- df[me_name==me & is_best==1]$run_id
  if (length(run_id) > 1) stop("More than 1 run_id")
  return(run_id)
}
## Read Draws
read_draws <- function(run_id, loc_id) { 
  path <- paste0("FILEPATH")
  files <- list.files(path, full.names=TRUE)
  if (length(files)==0) stop("No draws")
  if (loc_id != "all"){ 
    df <- fread(paste0(path,"/",loc_id,".csv"))
  } else{
    df <- mclapply(files, fread, mc.cores=10) %>% rbindlist(., use.names=TRUE)
  }
  key <- c("location_id", "year_id", "age_group_id", "sex_id")
  setkeyv(df, cols=key)
  df <- unique(df)
  df <- df[year_id >= 1990]
  return(df)
}
## Melt Draws
melt_draws <- function(df){
  id_vars <- names(df)[!names(df) %like% "draw_"]
  df <- melt(df,id.vars=id_vars, variable.name="draw", value.name="inc")
  return(df)
}
## Save Draws
save_draws <- function(df, path) {
  unlink(path, recursive=TRUE)
  dir.create(path, showWarnings=FALSE)
  location_ids <- unique(df$location_id)
  mclapply(location_ids, function(x) {
    write.csv(df[location_id==x], paste0(path, "/", x, ".csv"), row.names=FALSE)
  }, mc.cores=10)
}
## Merge Population
merge_pops <- function(df, gbd_round=7){
  # pull live births and neonatal population
  births <- get_covariate_estimates(1106,location_id=unique(df$location_id), sex_id=c(1,2), year_id=unique(df$year_id),
                                    gbd_round_id=gbd_round, decomp_step="iterative")[,.(location_id, year_id, sex_id, mean_value)]
  setnames(births,"mean_value","births")
  pops <- get_population(location_id=unique(df$location_id), year_id=unique(df$year_id), age_group_id=unique(df$age_group_id),
                         sex_id=unique(df$sex_id), gbd_round_id=gbd_round, decomp_step="iterative")[,-"run_id"]
  
  # merge populations to data
  df <- merge(df,births,by=c("location_id","year_id","sex_id"),all.x=TRUE)
  if (nrow(df[is.na(births)])>0) stop("Some rows missing birth population!")
  df <- merge(df,pops,by=c("location_id","year_id","age_group_id","sex_id"),all.x=TRUE)
  if (nrow(df[is.na(population)])>0) stop("Some rows missing neonatal population!")
  return(df)
}
## Merge Mortality
merge_mortality <- function(df, loc_id, gbd_round=7){
  # pull all cause mortality rate and ax
  message("Pulling all-cause mortality and ax")
  mort <- get_envelope(age_group_id=unique(df$age_group_id), location_id=unique(df$location_id), year_id=unique(df$year_id), 
                       sex_id=unique(df$sex_id), gbd_round_id=gbd_round, with_shock=1, with_hiv=1, decomp_step="iterative")[,-c("run_id","upper","lower")]
  setnames(mort,"mean","envelope")
  
  ax <- get_life_table(location_id = unique(df$location_id), gbd_round_id = gbd_round, decomp_step = 'iterative', 
                       with_hiv = 1, with_shock = 1, year_id = unique(df$year_id), life_table_parameter_id = 2,
                       age_group_id = unique(df$age_group_id), sex_id = unique(df$sex_id))[,-c("life_table_parameter_id","run_id")]
  setnames(ax,"mean","ax")
  
  #  merge to data frame
  df <- merge(df, mort, by=c("location_id","year_id","age_group_id","sex_id"), all.x=TRUE)
  df <- merge(df, ax, by=c("location_id","year_id","age_group_id","sex_id"), all.x=TRUE)
  if (nrow(df[is.na(envelope)])>0) stop("Some rows missing envelope deaths!")
  if (nrow(df[is.na(ax)])>0) stop("Some rows missing life table ax!")
  
  # pull EMR draws
  message("Pulling draws of EMR")
  if (loc_id != "all"){
    emr <- fread(paste0("FILEPATH/",loc_id,".csv"))
    if (nrow(emr)==0) stop(paste0("No EMR draws in folder for location id ",loc_id))
  } else {
    path <- paste0("FILEPATH")
    files <- list.files(path, full.names=TRUE)
    if (length(files)==0) stop("No draws")
    emr <- mclapply(files, fread, mc.cores=10) %>% rbindlist(., use.names=TRUE)
  }
  
  # reshape and merge to df
  emr <- melt(emr, id.vars=c("location_id","year_id","age_group_id","sex_id"), variable.name="draw", value.name="emr")
  df <- merge(df, emr, by=c("location_id","year_id","age_group_id","sex_id","draw"), all.x=TRUE)
  if (nrow(df[is.na(emr)])>0) stop("Some rows missing EMR data after merging!")
  
  return(df)
}
## Calculate Prevalence
calc_lifetable <- function(df){
  
  # sample draws of remission - mean of 40 and SD of 5.1 (corresponds to upper/lower of 30-50)
  message("Simulating draws of remission")
  rems <- data.table(draw=paste0("draw_",0:999),
                     remrt=rnorm(1000,mean=40,sd=5.1))
  df <- merge(df,rems,by="draw",all.x=TRUE)
  
  # calculate early neonatal 
  message("Life table calculation - ENN")
  enn <- df[age_group_id==2]
  enn[, incratio_enn := 1 - exp(-inc*((7/365)-0))]
  enn[, inc_enn := (births/52) * incratio_enn]
  enn[, remratio_enn := 1 - exp(-remrt*((7/365)-0))]
  enn[, remission_enn := inc_enn*remratio_enn]
  enn[, deaths_enn := inc_enn*emr]
  enn[, survivor_enn := inc_enn - deaths_enn - remission_enn]
  enn[, prev_enn := inc*(((7/365) - 0)/2)]
  enn[, pop_7days := (births/52) - survivor_enn - (envelope/52)]
  
  # calculate late neonatal
  message("Life table calculation - LNN")
  lnn <- df[age_group_id==3]
  lnn <- merge(lnn, enn[,.(location_id,year_id,sex_id,draw,inc_enn,deaths_enn,survivor_enn,pop_7days)], 
               by=c("location_id","year_id","sex_id","draw"), all.x=TRUE)
  lnn[, incratio_lnn := 1 - exp(-inc*((4/52) - (7/365)))]
  lnn[, inc_lnn := pop_7days * 3 * incratio_lnn]
  lnn[, remratio_lnn := 1 - exp(-remrt*((4/52)-(7/365)))]
  lnn[, remission_lnn := inc_lnn*remratio_lnn]
  lnn[, deaths_lnn := inc_lnn*emr]
  lnn[, survivor_lnn := inc_lnn - deaths_lnn - remission_lnn]
  lnn[survivor_lnn < 0, survivor_lnn:=0] 
  lnn[, prev_lnn := inc * (((4/52) - (7/365))/2)]
  lnn[, pop_28days := (pop_7days*3) + (survivor_enn*3) - (envelope*(3/52))]
  
  # calculate 28 days
  message("Life table calculation - 28 days")
  df_28 <- lnn[,.(location_id,year_id,age_group_id,sex_id,draw,survivor_enn,survivor_lnn,pop_28days)]
  df_28[, prev_28 := (survivor_enn + survivor_lnn)/pop_28days]
  
  # Cast wide
  message("Life table complete! Formatting and writing out draws")
  enn <- enn[,.(location_id,year_id,age_group_id,sex_id,draw,prev_enn)]
  enn <- dcast(enn, location_id+year_id+age_group_id+sex_id~draw)
  
  lnn <- lnn[,.(location_id,year_id,age_group_id,sex_id,draw,prev_lnn)]
  lnn <- dcast(lnn, location_id+year_id+age_group_id+sex_id~draw)
  
  df_28 <- df_28[,.(location_id,year_id,sex_id,draw,prev_28)]
  df_28 <- dcast(df_28, location_id+year_id+sex_id~draw)
  
  # Write out
  save_draws(enn, "FILEPATH")
  save_draws(lnn, "FILEPATH")
  save_draws(df_28, "FILEPATH")
  
  message("Draws successfully saved to FILEPATH")
}

## Run
run_id <- best_run_id("neonatal_sepsis")
df <- read_draws(run_id, loc_id)
df <- melt_draws(df)
df <- merge_pops(df)
df <- merge_mortality(df, loc_id)
calc_lifetable(df)

## END