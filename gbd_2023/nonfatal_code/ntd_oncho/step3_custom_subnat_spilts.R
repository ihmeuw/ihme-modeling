# NTDS: Onchocerciasis
# Purpose: custom subnational split/adjustment for nigeria - population weight, americas, yemen - custom, ethiopia - par, sudans 
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <- "FILEPATH"
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common   IO Paths
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

library(data.table)

params_dir <- paste0(data_root, "FILEPATH")
run_file <- fread(paste0(params_dir, 'FILEPATH'))
run_folder_path <- run_file[nrow(run_file), run_folder_path]
draws_dir <- paste0(run_folder_path, "FILEPATH")
interms_dir <- paste0(run_folder_path, "FILEPATH")

# Source relevant libraries
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_best_model_versions.R")
source("FILEPATH/get_draws.R")
source(paste0(code_root, "FILEPATH/processing.R"))
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")
library(data.table)
library(foreign)
library(maptools) 
library(rgeos) 
library(rgdal)
require(raster)
library(sp)
library(stats)

release_id <- ADDRESS

meids <- c(ADDRESS)
study_dems <- get_demographics(gbd_team = "ADDRESS", release_id = release_id)
gbdyears <- study_dems$year_id # get gbd years for this year
gbdsexes <- study_dems$sex_id # get sex_ids needed
gbdages <- study_dems$age_group_id # get age groups needed

### ======================= MAIN ======================= ###

#'[ Nigeria ]
# (PrevN * PopN) * (PopS / PopN) * (1 / PopS) = PrevS
locs <- get_location_metadata(location_set_id=ADDRESS,
                              release_id=release_id)
nig_sn <- locs[parent_id == ADDRESS, location_id]

for (meid in meids){
  nigeria_draws <- fread(paste0(draws_dir, 'FILEPATH'))
  for (sn in nig_sn){
    nigeria_draws[,location_id := sn]
    nigeria_draws[, metric_id := 3]
    fwrite(nigeria_draws, paste0(draws_dir, 'FILEPATH'))
  }
}

#'[ Americas]
# vector only locations need to overwrite for Americas estimates that need to be run
america_locs <-  c(IDS)

for (meid in meids) {
  cat(paste0("\n on meid ", meid, " for the Americas\n"))

  draws <- get_draws(gbd_id_type = "model_id", gbd_id = ID, source = "ADDRESS", measure_id = 5, 
                          release_id = ADDRESS, location_id = america_locs, decomp_step = 'ADDRESS')
  # Add extra years
  years_to_add <- c(2023,2024)
  years_df <- copy(draws_2019)

  if (length(years_to_add) > 0) {

    draws_df <- NULL
    for (year_to_add in years_to_add){
      new_rows <- copy(years_df[year_id == 2019])
      new_rows[, year_id:=year_to_add]
      draws_df <- rbind(draws_df, new_rows)
    }
  }

  draws_df <- rbind(draws_df,draws)
  for (loc in america_locs) {

    draw <- draws_df[location_id == loc]
    draw <- draw[modelable_entity_id == meid]
    draw[, metric_id := 3L]
    draw <- subset(draw, year_id %in% gbdyears)

    # Output
    fwrite(draw, paste0(draws_dir, "/", meid, "/", loc, ".csv"))
  }
}

#'[ Sudans]
### Get Population at Risk Estimates for Sudan Pre Abu Hamed Focus Elimination

# get population for Sudan and South Sudan (all-age-sex)
pop <- get_population(location_id = c(IDS), sex_id = 3, age_group_id = 22, year_id = 2000:2018, release_id = release_id)

geo_data <- fread(paste0(params_dir, '/FILEPATH.csv'))

geo_data <- subset(geo_data, location_name %in% c('Sudan', 'South Sudan'))
#geo_data$location_id <- ifelse(geo_data$location_name == 'Sudan', 435, 522)
geo_data <- left_join(geo_data, pop, by = c('location_id','age_group_id','sex_id','year_id'))

geo_data$cases <- geo_data$geo_mean * geo_data$population
geo_data[, total_cases := sum(cases), by = "year_id"]
geo_data[, prop := cases/total_cases]

par <- subset(geo_data, select = c('year_id','location_id','prop'))
years_to_add <- setdiff(gbdyears, unique(par$year_id))
locs <- c(IDS)

temp <- NULL
for (year in years_to_add){
  for (loc in locs){
    
    par_temp <- data.table()
    par_temp$year_id <- year
    par_temp$location_id <- loc
    
    if (year < 2000){
      
      par_temp$prop <- par[year_id == 2000 & location_id == loc, prop]
      
    } else {
      
      par_temp$prop <- par[year_id == 2018 & location_id == loc, prop]
    }
    temp <- rbind(temp, par_temp)
  }
}

par <- rbind(temp, par)
pop <- get_population(location_id = c(IDS), sex_id = gbdsexes, age_group_id = gbdages, year_id = gbdyears, release_id = release_id)

for(loc in locs) {
  for(meid in meids) {    
    temp_draws <- fread(paste0(draws_dir, '/' ,  meid, "/FILEPATH.csv"))
    
    if (loc == ID) {
      temp_draws[, location_id := ID]
    }
    
    # merge proportions
    temp_draws <- merge(temp_draws, par, by = c("location_id", "year_id"))
    
    # merge national population
    temp_draws <- merge(temp_draws, pop, by = c("sex_id", "age_group_id", "year_id", "location_id"))  
    setnames(temp_draws, "population", "national_population")
    
    temp_draws <- merge(temp_draws, pop[location_id == ID,], by = c("sex_id", "age_group_id", "year_id")) 
    setnames(temp_draws, "location_id.x", "location_id")
    temp_draws[, location_id.y := NULL]
    
    draw_names <- grep("draw", names(temp_draws), value = TRUE)
    test <- temp_draws[, (draw_names) := lapply(.SD, function(x){(x * population * prop)/national_population}), .SDcols = draw_names]
    test <- test[, c('population','national_population','run_id.y','run_id.x','prop') := NULL]
    
    fwrite(test, paste0(draws_dir, '/', meid, "/", loc, ".csv"))
    message(sprintf("Wrote data for %d %i", loc, meid))
  }
}


#'[ Ethiopia ]
### Proportions of subnationals to split national cases
# load data - file updated to account for splits of SNNPR
remo <- fread("/FILEPATH.csv")

# create cleaned REMO dataset
remo_clean <- data.table(location = remo$DIVISION1,
                         cases = remo$TOT_NOD,
                         sample.size = remo$TOT_EXAM)

remo_clean[, mean := cases/sample.size]

# standardize location spellings
remo_clean <- remo_clean[location == "AMHARA", location := "Amhara"]
remo_clean <- remo_clean[location == "OROMIYA", location := "Oromiya"]
remo_clean <- remo_clean[location == "BENISHANGUL", location := "Benishangul"]
remo_clean <- remo_clean[location == "GAMBELLA", location := "Gambella"]

# subset by region
remo_amhara <- remo_clean[location == "Amhara",] # 0.1045
remo_oromiya <- remo_clean[location == "Oromiya",] # 0.187
remo_benishangul <- remo_clean[location == "Benishangul",] # 0.1526
remo_gambella <- remo_clean[location == "Gambella",] # 0.1813
remo_tigray <- remo_clean[location == "Tigray",] # 0.0521
remo_southwest <- remo_clean[location == "South West",] 

# population weighted averages now
amhara_mean <- weighted.mean(remo_amhara$mean, remo_amhara$sample.size)
oromiya_mean <- weighted.mean(remo_oromiya$mean, remo_oromiya$sample.size)
benishangul_mean <- weighted.mean(remo_benishangul$mean, remo_benishangul$sample.size)
gambella_mean <- weighted.mean(remo_gambella$mean, remo_gambella$sample.size)
tigray_mean <- weighted.mean(remo_tigray$mean, remo_tigray$sample.size)
southwest_mean <- weighted.mean(remo_southwest$mean, remo_southwest$sample.size)

### Need population at risk for each subnational -> proportions in case space

df_all_subnats <- NULL
for (meid in meids){
  
  print(paste0('working on ', meid))
  eth_end_locs <- c(ADDRESS)

  if (meid == ADDRESS){
    ages_end <- setdiff(gbdages, c(ADDRESS))
    ages_nonend <- c(ADDRESS)
    
  } else if (meid == ADDRESS) {
    ages_end <- setdiff(gbdages, c(ADDRESS))
    ages_nonend <- c(ADDRESS)
    
  } else {
    ages_end <- setdiff(gbdages, c(ADDRESS))
    ages_nonend <- c(ADDRESS)
    
  }

    #get population for ETH national
    nat_eth_pop <- get_population(location_id = ID, sex_id = gbdsexes, age_group_id = gbdages, year_id = gbdyears, release_id = release_id)
    nat_eth_pop <- nat_eth_pop[, c('sex_id','age_group_id','year_id','population')]

    # subnational proportions by age/sex
    eth_subnat_pop <- get_population(location_id = c(ADDRESS), sex_id = gbdsexes, age_group_id = gbdages, year_id = gbdyears, release_id = release_id)
    eth_subnat_pop$population <- ifelse(eth_subnat_pop$age_group_id %in% ages_end, eth_subnat_pop$population, 0)
    eth_subnat_pop[, total_pop := sum(population), by = c('year_id','location_id')]
    eth_subnat_pop[, prop_byagesex := population / total_pop]

    eth_subnat_pop <- eth_subnat_pop[, .(age_group_id, sex_id, year_id, location_id, prop_byagesex, population)]  #proportions of population by age/sex within loc/year


    final_sum_pop <- get_population(location_id = c(ADDRESS), sex_id = 3, age_group_id = 22, 
                                year_id = gbdyears, release_id = release_id) %>% select(location_id,year_id,population)

    ### Convert to cases (PAR * mean)
    library(tidyr)

    df <- data.frame(matrix(ncol = 3, nrow = 5))
    colnames(df) <-c("year_id","location_id","remo_mean")
    df[,2] <- c(ADDRESS)
    df[1,3] <- amhara_mean
    df[2,3] <- oromiya_mean
    df[3,3] <- gambella_mean
    df[4,3] <- benishangul_mean
    df[5,3] <- southwest_mean

    df_expand <- df %>% group_by(location_id, remo_mean) %>% expand(year_id = gbdyears) 
    proportion_table <- left_join(df_expand,final_sum_pop, by = c('location_id','year_id'))
    proportion_table$cases <- proportion_table$remo_mean * proportion_table$population
    proportion_table <- as.data.table(proportion_table)
    setnames(proportion_table, 'population','subnat_pop')
    proportion_table[, total_cases := sum(cases), by = c('year_id')]
    proportion_table$prop <- proportion_table$cases / proportion_table$total_cases
    proportion_table[, c('remo_mean','cases','total_cases') := NULL]

    # Now split all-cases of Ethiopia
    temp_draws <- fread(paste0(draws_dir, '/', meid, "/FILEPATH.csv"))
    temp_draws <- merge(temp_draws, nat_eth_pop, by = c("sex_id", "age_group_id", "year_id"))

    draw_names <- paste0("draw_", 0:999)
    test <- temp_draws[, (draw_names) := lapply(.SD, function(x){(x * population)}), .SDcols = draw_names]
    test[, paste0("draw_", 0:999) := lapply(0:999, function(x) sum(get(paste0("draw_", x)))), by = c("location_id", "year_id")]
    test <- test[, c('age_group_id','sex_id','population','location_id') := NULL]
    test <- distinct(test)

    temp_draws <- merge(test, proportion_table, by = c("year_id"))
    eth_prop_subnat <- temp_draws[, (draw_names) := lapply(.SD, function(x){(x * prop)}), .SDcols = draw_names]  # this gives all-age-sex cases by loc/year within subnational, now split
    eth_prop_subnat[, c('subnat_pop','prop') := NULL]

 
    check <- merge(eth_prop_subnat, eth_subnat_pop, by = c("year_id","location_id"))
    check <- check[, (draw_names) := lapply(.SD, function(x){(x * prop_byagesex)/population}), .SDcols = draw_names]  # this gives all-age-sex cases by loc/year within subnational, now split

    check_nas <- subset(check, age_group_id %in% ages_nonend)
    df_to_bind <- setdiff(check,check_nas)
    
    check_nas <- check_nas[, paste0('draw_',0:999) := 0]
    
    df_all <- rbind(check_nas,df_to_bind)

    
    for (loc in eth_end_locs){
  
  final_df <- subset(df_all, location_id == loc)
  final_df <- final_df[, model_version_id := NULL]
  final_df <- final_df[, c('run_id','population','total_pop','propbyagesex') := NULL]
  final_df[, modelable_entity_id := meid]
  final_df[, measure_id := 5]
  
  final_df[, metric_id := 3]
  fwrite(final_df, paste0(draws_dir, '/', meid, "/", loc, ".csv"))
  }

}

#'[ Yemen ]

######################################
# 2. CONVERT YEMEN TO BINOMIAL DISTRIBUTION FROM FOLLOWING CITATIONS (30,000 data point)
######################################

#######################################################################################
# MoH Yemen, 1991 reported 30,000 infected with Onchocerciasis
# sOURCE: http://apps.who.int/iris/bitstream/10665/37346/1/WHO_TRS_852.pdf

# WE KNOW STARTING IN 2000 THE AIM WAS TO TREAT APPROXIMATELY 300,000 PEOPLE 4 TIMES A YEAR
# SOURCE: https://www.cartercenter.org/resources/pdfs/news/health_publications/selendy-waterandsanitationrelateddiseases-chapt11.pdf

# CSSW ONCHO CONTROL PROGRAM IN YEMEM REPORTS AGE-ISH/SEX SPLIT DATA IN 2014
# MALE < 15 = 557 CASES
# MALE > 15 = 839 CASES
# FEMALE < 15 = 405 CASES
# FEMALE > 15 = 765 CASES
#######################################################################################

###########################################################
# BUILD GBD ESTIMATE SKELETON
###########################################################
# set-up GBD skeleton

gbdages <- study_dems$age_group_id # gbd ages
gbdyears <- study_dems$year_id # gbd years
gbdsexes <- study_dems$sex_id # gbd sexes

###############################################################
# AGE/SEX SPLIT NATIONAL MF PREVALENCE CASES
###############################################################


datalist <- NULL # initialize list to store draws for each year

for(a in 1:length(gbdyears)) {
  print(a)
  
  year <- gbdyears[a]
  population <-get_population(age_group_id = 22, year_id = year, sex_id = 3, location_id = ADDRESS, release_id = release_id) # get national all-age population for year
  
  # set up variables for binomial proportion confidence interval
  cases <- 30000 # number of successes
  pop <- round(population$population) # number of trials
  prob <- cases/pop # hypothesized probability of success
  
  # sampling binomial distribution
  case_draws <- rbinom(1000, pop, prob)
  
  # get global prevalence by age/sex to inform splits
  
  if (year > 2019){
    year_temp <- 2019
  } else {
    year_temp <- year
  }
  
  # get old prevalence from last
  global_prev_draws <- get_draws(gbd_id_type = "model_id", gbd_id = ADDRESS, measure_id = 5,
                                 location_id = 1,  year_id = year_temp,
                                 sex_id = c(1,2), source = "ADDRESS", status="best", release_id = release_id, version_id = "ADDRESS")
  
  draws_temp <- global_prev_draws
  
  # get proportions now
  draw_names <- grep("draw", names(global_prev_draws), value = TRUE)
  
  # try taking mean of draws
  draw_means <- as.data.table(rowMeans(global_prev_draws[, ..draw_names]))
  
  draws_temp <- draws_temp[, (draw_names) := draw_means$V1]
  
  
  # split cases by proportions
  draws_temp <- as.data.table(mapply(`*`, draws_temp[, ..draw_names], case_draws))
  
  # for each draw col get scalar which is sum cases / sum (current est cases)
  scalars <- draws_temp[, lapply(.SD, sum, na.rm=TRUE), .SDcols = draw_names]
  scalars <- case_draws / scalars
  
  
  # scale now to correct case count
  draws_temp <- as.data.table(mapply(`*`, draws_temp[, ..draw_names], scalars))
  
  # convert to prevalence now by dividing cases by total population in that year
  draws_temp <- as.data.table(mapply(`/`, draws_temp[, ..draw_names], population$population))
  
  info <- data.table(model_id= global_prev_draws$model_id,
                     measure_id = global_prev_draws$measure_id,
                     age_group_id = global_prev_draws$age_group_id,
                     year_id = year,
                     location_id = ADDRESS,
                     sex_id = global_prev_draws$sex_id)
  
  # add back other column names needed
  draws_temp <- dplyr::bind_cols(info, draws_temp)
  
  # append to previous year....etc.
  datalist[[a]] <- draws_temp # add it to your list
  
}

nat_draws <- do.call(rbind, datalist)


nat_draws[, metric_id := 3]
fwrite(nat_draws, paste0(draws_dir, "FILEPATH"))

# Yemen has ROD most severe form with a lot of itching so for now mapping

nat_draws[, model_id:= 'ADDRESS']
fwrite(nat_draws, paste0(draws_dir, "FILEPATH"))
