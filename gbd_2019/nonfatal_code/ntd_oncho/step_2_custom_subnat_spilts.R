
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

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
  location_id <- 214
}

params_dir <- paste0(data_root, "FILEPATH")
run_file <- fread(paste0(params_dir, 'FILEPATH'))
run_folder_path <- run_file[nrow(run_file), run_folder_path]
draws_dir <- paste0(run_folder_path, "FILEPATH")
interms_dir <- paste0(run_folder_path, "FILEPATH")

# Source relevant libraries
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source(paste0(code_root, "FILEPATH"))
library(data.table)
library(foreign)
library(maptools) 
library(rgeos) 
library(rgdal) 
require(raster)
library(sp)
library(stats)
library(data.table) 

gbd_round_id <- ADDRESS
decomp_step <- 'step1'

meids <- c(ADDRESS)
study_dems <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
old_dems <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id-1)

gbdyears <- study_dems$year_id 
gbdsexes <- study_dems$sex_id 
gbdages <- study_dems$age_group_id
old_ages <- old_dems$age_group_id

### ======================= MAIN ======================= ###

#'[ Nigeria ]

# (PrevN * PopN) * (PopS / PopN) * (1 / PopS) = PrevS

locs <- get_location_metadata(location_set_id=35,
                              gbd_round_id=gbd_round_id,
                              decomp_step=decomp_step)
nig_sn <- locs[parent_id == 214, location_id]

for (meid in meids){
  nigeria_draws <- fread(paste0(draws_dir, '/', meid, "FILEPATH"))
  for (sn in nig_sn){
    nigeria_draws[,location_id := sn]
    fwrite(nigeria_draws, paste0(draws_dir, '/', meid, "/", sn ,".csv"))
  }
}


america_locs <-  c(130, 133, 135, 125, 122, 128)

for (meid in meids) {
  cat(paste0("\n on meid ", meid, "FILEPATH\n"))
  # get_draws from gbd2019
  draws_2019 <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = meid, source = "epi", measure_id = 5, 
                          gbd_round_id = gbd_round_id-1, location_id = america_locs, decomp_step = 'iterative')
  # Add extra years
  rows_2020 <- copy(draws_2019[year_id == 2019])
  rows_2020[, year_id := 2020] 
  rows_2021 <- copy(rows_2020)
  rows_2021[, year_id := 2021] 
  rows_2022 <- copy(rows_2020)
  rows_2022[, year_id := 2022] 
  draws_2022 <- rbind(draws_2019, rows_2020, rows_2021, rows_2022)
  
  # Add GBD 2020 age groups
  new_age_groups <- c(388, 389, 238, 34)
  
  zero_draws <- fread(paste0(draws_dir, '/', meid, "/", 200, ".csv"))
  zero_draws <- zero_draws[age_group_id %in% new_age_groups]
  
  draws_2019[, measure_id := 5]
  draws_2019[, modelable_entity_id := meid]

  # order columns
  draw_names <- grep("draw", names(draws_2019), value = TRUE)
  other_cols <- c("modelable_entity_id", "measure_id", "location_id", "year_id", "age_group_id", "sex_id")
  setcolorder(draws_2019, append(other_cols, draw_names))
  
  zero_draws[, measure_id := draws_2019$measure_id[1]]
  zero_draws[, metric_id := 3L]
  zero_draws[, modelable_entity_id := meid]
  zero_draws[, outvar:= NULL]
  
  for (loc in america_locs) {
    
    draw <- draws_2019[location_id == loc]
    draw <- draw[modelable_entity_id == meid]
    draw[, metric_id := 3L]
    draw <- rbind(draw, zero_draws)
    draw[, location_id := loc]
    
    # Output
    fwrite(draw, paste0(draws_dir, "/", meid, "/", loc, ".csv"))
  }
}



############################################################################
# Get Population at Risk Estimates for Sudan Pre Abu Hamed Focus Elimination
############################################################################



# load in maps
sudan_endem_pre_abu <- shapefile("FILEPATH")
years <- c(1990, 1995, 2000, 2005, 2010)
sudan_par_pre_elim <- data.table()

for (a in 1:length(years)) {
  year <- years[a]
  tot_pop <- raster(paste0("FILEPATH", year,"FILEPATH"))
  
  # Extract data from  raster for locations in eth_endem locations (spatialPolygon) and sum values
  sum_population <- extract(tot_pop, sudan_endem_pre_abu, df=TRUE)
  sum_population <- data.table(sum_population)
  sum_population <- na.omit(sum_population) # get rid of NA values 
  final_sum_pop <- sum_population[,.(population_risk = sum(eval(as.name((paste0("worldpop_total_1y_", year, "_00_00")))))),by=ID] # sum of values for each ID (each subnational location)
  
  final_sum_pop <- data.table(location = "Sudan",
                              year_id = year,
                              population_risk = final_sum_pop$population_risk)
  
  sudan_par_pre_elim <- dplyr::bind_rows(sudan_par_pre_elim, final_sum_pop)
  
}


#############################################################################
# Get Population at Risk Estimates for Sudan Post Abu Hamed Focus Elimination
#############################################################################


# load in maps
sudan_endem_post_abu <- shapefile("FILEPATH")
tot_pop <- raster("FILEPATH")

# Extract data from  raster for locations in sudan_endem_post_abu locations (spatialPolygon) and sum values
sum_population <- extract(tot_pop, sudan_endem_post_abu, df=TRUE)
sum_population <- data.table(sum_population)
sum_population <- na.omit(sum_population) # get rid of NA values 
sudan_par_post_elim <- sum_population[,.(population_risk = sum(worldpop_total_1y_2017_00_00)),by=ID] # sum of values for each ID (each location)

extend_years <- gbdyears[gbdyears > 2013]
sudan_par_post_elim <- data.table(location = "Sudan",
                                  year_id = extend_years, 
                                  population_risk = sudan_par_post_elim$population_risk)

#############################################################################
# Get Population at Risk Estimates for South Sudan
#############################################################################
# load in maps
south_sudan_endem <- shapefile("FILEPATH")
years <- gbdyears
south_sudan_par <- data.table()

for (a in 1:length(years)) {
  year <- years[a]
  tot_pop <- raster(paste0("FILEPATH", year,"FILEPATH"))
  
  # Extract data from  raster for locations in eth_endem locations (spatialPolygon) and sum values
  sum_population <- extract(tot_pop, south_sudan_endem, df=TRUE)
  sum_population <- data.table(sum_population)
  sum_population <- na.omit(sum_population) # get rid of NA values 
  final_sum_pop <- sum_population[,.(population_risk = sum(eval(as.name((paste0("worldpop_total_1y_", year, "_00_00")))))),by=ID] # sum of values for each ID (each subnational location)
  
  final_sum_pop <- data.table(location = "South Sudan",
                              year_id = year,
                              population_risk = final_sum_pop$population_risk)
  
  south_sudan_par <- dplyr::bind_rows(south_sudan_par, final_sum_pop)
  
}


par <- dplyr::bind_rows(sudan_par_pre_elim, sudan_par_post_elim)
par <- dplyr::bind_rows(par, south_sudan_par)

####################################################
# Now Calculate proportions and format
####################################################
par[, total := sum(population_risk), by = "year_id"]
par[, prop := population_risk/total]
par[, population_risk := NULL]
par[, total := NULL]

par[location == "Sudan", location_id := 522]
par[location == "South Sudan", location_id := 435]

##########################################################################
# NOW SPLIT DRAWS 
##########################################################################
# set-up GBD skeleton
gbdages <- study_dems$age_group_id # gbd ages
gbdyears <- study_dems$year_id # gbd years
gbdsexes <- study_dems$sex_id # gbd sexes 

# get population for Sudan and South Sudan
pop <- get_population(location_id = c(522, 435), sex_id = gbdsexes, age_group_id = gbdages, year_id = gbdyears, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
pop <- pop[, run_id := NA]


locations <- c(522, 435)

for(a in 1:length(locations)) {
  for(b in 1:length(meids)) {
    
    loc <- locations[a]
    meid <- meids[b]
    
    # get draws
    temp_draws <- fread(paste0(draws_dir, '/' ,  meid, "FILEPATH"))
    
    
    if (loc == 522) {
      temp_draws[, location_id := 522]
    }
    
    # merge proportions
    temp_draws <- merge(temp_draws, par, by = c("location_id", "year_id"))
    
    # merge national population
    temp_draws <- merge(temp_draws, pop, by = c("sex_id", "age_group_id", "year_id", "location_id"))  
    setnames(temp_draws, "population", "national_population")
    
    
    
    temp_draws <- merge(temp_draws, pop[location_id == 435,], by = c("sex_id", "age_group_id", "year_id")) 
    setnames(temp_draws, "location_id.x", "location_id")
    temp_draws[, location_id.y := NULL]
    
    
    draw_names <- grep("draw", names(temp_draws), value = TRUE)
    test <- temp_draws[, (draw_names) := lapply(.SD, function(x){(x * population * prop)/national_population}), .SDcols = draw_names]
    
    # format structure of output
    test <- test[, population := NULL]
    test <- test[, national_population := NULL]
    test <- test[, run_id.y := NULL]
    
    test <- test[, run_id.x := NULL]
    test <- test[, prop := NULL]
    test <- test[, metric_id := NULL]
    test <- test[, location := NULL]
    
    test[, modelable_entity_id := meid]
    test[, measure_id := 5]
    
    #order modelable_entity_id  measure_id location_id year_id age_group_id sex_id  draw*
    other_cols <- c("modelable_entity_id", "measure_id", "location_id", "year_id", "age_group_id", "sex_id", "outvar")
    setcolorder(test, append(other_cols, draw_names))

    # Output
    fwrite(test, paste0(draws_dir, '/', meid, "/", loc, ".csv"))
    message(sprintf("Wrote data for %d %i", loc, meid))
  }
}

###############################################
# GET PROPORTIONS NOW TO SPLIT NATIONAL CASES
###############################################
# load data
remo <- fread("FILEPATH")

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
remo_snnpr <- remo_clean[location == "SNNPR",] # 0.1712

# population weighted averages 
amhara_mean <- weighted.mean(remo_amhara$mean, remo_amhara$sample.size)
oromiya_mean <- weighted.mean(remo_oromiya$mean, remo_oromiya$sample.size)
benishangul_mean <- weighted.mean(remo_benishangul$mean, remo_benishangul$sample.size)
gambella_mean <- weighted.mean(remo_gambella$mean, remo_gambella$sample.size)
tigray_mean <- weighted.mean(remo_tigray$mean, remo_tigray$sample.size)
SNNPR_mean <- weighted.mean(remo_snnpr$mean, remo_snnpr$sample.size)

########################################################

########################################################.

# load in maps
eth_endem <- shapefile("FILEPATH")
tot_pop_2015 <- raster("FILEPATH")

# Extract data from  raster for locations in eth_endem locations (spatialPolygon) and sum values
sum_population <- extract(tot_pop_2015, eth_endem, df=TRUE)
sum_population <- data.table(sum_population)
sum_population <- na.omit(sum_population) # get rid of NA values
final_sum_pop <- sum_population[,.(population_risk = sum(worldpop_total_1y_2015_00_00)),by=ID] # sum of values for each ID (each ETH subnational location)

id_match <- data.table(location = eth_endem$ADM1_NAME,
                       ID = 1:5)

final_sum_pop <- merge(final_sum_pop, id_match, by = "ID")

############################################################################
#  MULTIPLY MEAN AND POPULATION AT RISK TO CONVERT TO CASE SPACE
############################################################################

case_amhara <- amhara_mean * final_sum_pop[location == "Amhara",]$population_risk
case_oromiya <- oromiya_mean * final_sum_pop[location == "Oromia",]$population_risk
case_benishangul <- benishangul_mean * final_sum_pop[location == "Beneshangul Gumu",]$population_risk
case_gambella <- gambella_mean * final_sum_pop[location == "Gambela",]$population_risk
case_tigray <- tigray_mean * 0 # according to endemicity map from 2015 article there is no population at risk in Ethiopia
case_SNNPR <- SNNPR_mean * final_sum_pop[location == "SNNPR",]$population_risk

############################################################################
# GET PROPORTIONS
############################################################################
total_cases <- case_amhara + case_benishangul + case_gambella + case_oromiya + case_SNNPR + case_tigray
prop_amhara <- case_amhara/total_cases
prop_oromiya <- case_oromiya/total_cases
prop_benishangul <- case_benishangul/total_cases
prop_gambella <- case_gambella/total_cases
prop_SNNPR <- case_SNNPR/total_cases

#############################################################
# SPLIT DRAWS ...
#############################################################
# set-up GBD skeleton
gbdages <- study_dems$age_group_id # gbd ages
gbdyears <- study_dems$year_id # gbd years
gbdsexes <- study_dems$sex_id # gbd sexes

# get list of subnational location ids for ETH
locs <- get_location_metadata(location_set_id=35,
                             gbd_round_id=gbd_round_id,
                             decomp_step=decomp_step)
locs <- locs[ihme_loc_id %like% "ETH",]

# get population for ETH subnationals
eth_pop <- get_population(location_id = locs$location_id, sex_id = gbdsexes, age_group_id = gbdages, year_id = gbdyears, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
eth_pop <- eth_pop[, run_id := NA]

#get population for ETH national
nat_eth_pop <- get_population(location_id = 179, sex_id = gbdsexes, age_group_id = gbdages, year_id = gbdyears, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
nat_eth_pop <- data.table(sex_id = nat_eth_pop$sex_id,
                          age_group_id = nat_eth_pop$age_group_id,
                          year_id = nat_eth_pop$year_id,
                          population = nat_eth_pop$population)

# create data.table with proportions
location <- c("Addis Ababa", "Afar", "Amhara", "Benishangul-Gumuz", "Dire Dawa", "Gambella", "Harari", "Oromia", "Somali", "SNNP", "Tigray")
location_id <- c(44861, 44853, 44854, 44857, 44862, 44860, 44859, 44855, 44856, 44858, 44852)
proportions <- c(0, 0, prop_amhara, prop_benishangul, 0, prop_gambella, 0, prop_oromiya, 0, prop_SNNPR, 0)

proportion_table <- data.table(location_id = location_id,
                               proport = proportions)

locations <- locs[ihme_loc_id %like% "ETH_",]$location_id
meids <- c(ADDRESS)

for(a in 1:length(location_id)) {
  for(b in 1:length(meids)) {
    loc <- locations[a]
    meid <- meids[b]
    cat(paste0("Writing location id ", loc, " for meid ", meid, " -- ", Sys.time(), "\n"))
    # get draws
    temp_draws <- fread(paste0(draws_dir, '/', meid, "FILEPATH"))
    temp_draws <- temp_draws[, location_id := loc]
    
    # merge subnational populations
    temp_draws <- merge(temp_draws, eth_pop, by = c("sex_id", "age_group_id", "year_id", "location_id"))
    setnames(temp_draws, "population", "subnational_population")
    
    # merge national populations
    temp_draws <- merge(temp_draws, nat_eth_pop, by = c("sex_id", "age_group_id", "year_id"))
    setnames(temp_draws, "population", "national_population")
    
    # merge proportions
    temp_draws <- merge(temp_draws, proportion_table, by = "location_id")
    
    draw_names <- grep("draw", names(temp_draws), value = TRUE)
    test <- temp_draws[, (draw_names) := lapply(.SD, function(x){(x * national_population * proport)/subnational_population}), .SDcols = draw_names]
    
    # format structure of output
    test <- test[, model_version_id := NULL]
    test <- test[, subnational_population := NULL]
    test <- test[, run_id := NULL]
    test <- test[, national_population := NULL]
    test <- test[, proport := NULL]
    test <- test[, metric_id := NULL]
    test[, modelable_entity_id := meid]
    test[, measure_id := 5]
    
    
    other_cols <- c("modelable_entity_id", "measure_id", "location_id", "year_id", "age_group_id", "sex_id")
    setcolorder(test, append(other_cols, draw_names))
    
    
    
    # Output
    fwrite(test, paste0(draws_dir, '/', meid, "/", loc, ".csv"))
  }
}

#'[ Yemen ]

######################################
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
  population <-get_population(age_group_id = 22, year_id = year, sex_id = 3, location_id = 157, decomp_step = decomp_step, gbd_round_id = gbd_round_id) # get national all-age population for year
  
  # set up variables for binomial proportion confidence interval
  cases <- 30000 
  pop <- round(population$population) 
  prob <- cases/pop 
  
  # sampling binomial distribution
  case_draws <- rbinom(1000, pop, prob)
  
  # get global prevalence by age/sex to inform splits
  
  if (year > 2019){
    year_temp <- 2019
  } else {
    year_temp <- year
  }
  

  global_prev_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = ADDRESS, measure_id = 5,
                                 location_id = 1,  year_id = year_temp,
                                 sex_id = c(1,2), source = "epi", status="best", gbd_round_id = 6, decomp_step = "iterative", version_id = ADDRESS)
  
  draws_temp <- global_prev_draws
  
  
  draw_names <- grep("draw", names(global_prev_draws), value = TRUE)
  
  
  draw_means <- as.data.table(rowMeans(global_prev_draws[, ..draw_names]))
  
  draws_temp <- draws_temp[, (draw_names) := draw_means$V1]
  
  
  # split cases by proportions
  draws_temp <- as.data.table(mapply(`*`, draws_temp[, ..draw_names], case_draws))
  
  scalars <- draws_temp[, lapply(.SD, sum, na.rm=TRUE), .SDcols = draw_names]
  scalars <- case_draws / scalars
  
  
   draws_temp <- as.data.table(mapply(`*`, draws_temp[, ..draw_names], scalars))
  
  # convert to prevalence  by dividing cases by total population in that year
  draws_temp <- as.data.table(mapply(`/`, draws_temp[, ..draw_names], population$population))
  
  info <- data.table(modelable_entity_id = global_prev_draws$modelable_entity_id,
                     measure_id = global_prev_draws$measure_id,
                     age_group_id = global_prev_draws$age_group_id,
                     year_id = global_prev_draws$year_id,
                     location_id = 157,
                     sex_id = global_prev_draws$sex_id)
  
  # add back other column names needed
  draws_temp <- dplyr::bind_cols(info, draws_temp)
  datalist[[a]] <- draws_temp 
  
}

nat_draws <- do.call(rbind, datalist)

# Add extra years
rows_2020 <- copy(nat_draws [year_id == 2019])
rows_2020[, year_id := 2020] 
rows_2021 <- copy(rows_2020)
rows_2021[, year_id := 2021] 
rows_2022 <- copy(rows_2020)
rows_2022[, year_id := 2022] 
draws_2022 <- rbind(nat_draws , rows_2020, rows_2021, rows_2022)

fwrite(nat_draws, paste0(draws_dir, "FILEPATH"))

nat_draws[, modelable_entity_id := ADDRESS]
fwrite(nat_draws, paste0(draws_dir, "FILEPATH"))