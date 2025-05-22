# NTDS: Onchocerciasis
# Author: Cathleen Keller, 05.31.2024
# Purpose: Calculate ratios between geospatial between GBD estimates for 
# years in GBD without data (2014-2024)
#
### ======================= BOILERPLATE ======================= ###

rm(list = ls())
code_root <- "FILEPATH"
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common /ihme/ IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPATH")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
}

# Source relevant libraries
library(data.table)
library(dplyr)
source("/FILEPATH/get_demographics.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_population.R")
source(paste0(code_root, "/FILEPATH/sbatch.R"))
code_dir   <- paste0(code_root, "/FILEPATH/")
shell  <- "/FILEPATH.sh"
params_dir <- paste0(data_root, "/ADDRESS/params")
run_file <- fread(paste0(params_dir, '/run_file.csv'))
run_folder_path <- run_file[nrow(run_file), run_folder_path]
draws_dir <- paste0(run_folder_path, "/draws")
interms_dir <- paste0(run_folder_path, "/interms")
date <- Sys.Date()

mean_ui <- function(x){
  y <- select(x, starts_with("draw"))
  y$mean_gbd <- apply(y, 1, mean)
  y$lower_gbd <- apply(y, 1, function(x) quantile(x, c(.025)))
  y$upper_gbd <- apply(y, 1, function(x) quantile(x, c(.975)))
  
  w <- y[, c('mean_gbd', 'lower_gbd', 'upper_gbd')]
  
  z <- select(x, -contains("draw"))
  
  v <- cbind(z, w)
  v <- as.data.table(v)
  return(v)
  
}

draws <- paste0('draw_',0:999)
years <- 1990:2024
outcome <- 'NAME'
meid <- ADDRESS

#############################################################################################
###                                   Geospatial                                          ###
#############################################################################################
# metadata
demo <- get_demographics(gbd_team = 'epi',release_id = ADDRESS)
ages <- demo$age_group_id

gbd_locs <- as.numeric(gsub('.csv', '', list.files(paste0(interms_dir, '/', meid))))
loc_meta <- get_location_metadata(location_set_id = ID, release_id = ADDRESS)[level==3] %>% select(location_name,location_id)
pop <- get_population(age_group_id = c(22,ages), sex_id = c(1,2,3), location_id = gbd_locs, year_id = years, release_id = ADDRESS)

#' [Aggregate GBD estimates to all-age-sex by loc/year]

data <- NULL
for (loc in gbd_locs){
  
  df <- fread(paste0(interms_dir, '/', meid, '/', loc, ".csv"))

  df_pop <- left_join(df, pop, by = c('location_id','age_group_id','sex_id','year_id'))
  
  df_pop[, paste0("cases_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * population), by = c("age_group_id", "location_id", "sex_id", "year_id")]
  df_pop[, paste0("total_cases_", 0:999) := lapply(0:999, function(x) sum(get(paste0("cases_", x)))), by = c("location_id", "year_id")]
  
  df_pop[, paste0("draw_",0:999) := NULL]
  df_pop[, paste0("cases_",0:999) := NULL]
  df_pop[, c('run_id','population') := NULL]
  df_pop$age_group_id <- 22
  df_pop$sex_id <- 3
  df_pop <- distinct(df_pop)
  
  df_all <- left_join(df_pop, pop, by = c('location_id','age_group_id','sex_id','year_id'))
  df_all[, paste0("draw_", 0:999) := lapply(0:999, function(x) get(paste0("total_cases_", x)) / population), by = c("age_group_id", "location_id", "sex_id", "year_id")]
  df_all[, paste0("total_cases_",0:999) := NULL]

  data <- rbind(df_all,data)
}

temp_gbd <- mean_ui(data)

#' [Run regression of geospatial estimates]
df <- fread(paste0(params_dir, '/FILEPATH.csv'))

df_loc <- left_join(df, loc_meta, by = 'location_name')
yrs_missing <- 2019:2024

# expand dataset 
geo_allyrs <- NULL
for (loc in gbd_locs){
  for (yr in yrs_missing){
    
    loc_to_add <- subset(df_loc, location_id == loc)
    
    new_yr <- subset(loc_to_add, year_id == 2018)
    new_yr$year_id <- yr
    
    geo_allyrs <- rbind(geo_allyrs,new_yr)
    
  }
}


df_geo <- rbind(df_loc, geo_allyrs)
df_geo <- subset(df_geo, year_id > 2012)

temp_geo <- NULL
for (loc in gbd_locs){
  
  geo <- subset(df_geo, location_id == loc)
  if (sum(geo[year_id %in% 2013:2018, mean]) == 0) {
    
    geo[, 'log_mean_geo' := 0]

  } else {

    geo_log_lm <- lm(log(mean) ~ year_id, data = geo[year_id %in% 2013:2018])
    geo <- geo[, 'log_mean_geo' := predict(geo_log_lm, geo)]   # predictions in log space
    
    geo$log_mean_geo <- ifelse(geo$year_id %in% 2013:2018, log(geo$mean), geo$log_mean_geo)
    
  }
  temp_geo <- rbind(temp_geo,geo)
}

temp_geo$orig_geo_predict <- exp(temp_geo$log_mean_geo)   # predictions out of log space - used for vetting

# save copy
write.csv(temp_geo, paste0(interms_dir, '/FILEPATH.csv'), row.names = FALSE)

#' [Recalculate geospatial estimates using log difference of predictions; calculate ratios]

ratio_df <- NULL
for (loc in gbd_locs){
    #' [combine]
    df1 <- subset(temp_geo, location_id == loc) %>% select(location_id,year_id,sex_id,log_mean_geo)
    df2 <- subset(temp_gbd, location_id == loc) %>% select(location_id,year_id,sex_id,population,mean_gbd)

    df2 <- subset(df2, year_id >= 2013)
    df1 <- subset(df1, year_id %in% df2$year_id)

    #' [calculate log difference of means]
    df3 <- left_join(df1,df2,by = c('location_id','sex_id','year_id'))
    
    df3$log_diff_2013 <- log(df3[year_id == 2013]$mean_gbd) - df3[year_id == 2013]$log_mean_geo
    
    df3$log_mean_adj_geo <- df3$log_mean_geo + df3$log_diff_2013
    df3$mean_adj_geo <- exp(df3$log_mean_adj_geo)
    
    ratio_df <- rbind(ratio_df,df3)
    
}  


ratio_df_adj <- copy(ratio_df)
ratio_df_adj$mean_gbd <- ifelse(ratio_df_adj$mean_gbd < 0.0001, 0.0001, ratio_df_adj$mean_gbd)
ratio_df_adj$mean_adj_geo <- ifelse(ratio_df_adj$mean_adj_geo < 0.0001, 0.0001, ratio_df_adj$mean_adj_geo)
    
ratio_df_adj$ratio <- ratio_df_adj$mean_adj_geo/ratio_df_adj$mean_gbd

ratio_df_adj <- ratio_df_adj[, c('location_id','year_id','ratio','mean_gbd','mean_adj_geo')]
write.csv(ratio_df_adj, paste0(params_dir, '/FILEPATH.csv'), row.names = FALSE)

