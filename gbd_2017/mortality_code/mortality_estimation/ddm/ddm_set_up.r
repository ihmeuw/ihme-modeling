  # Author: 
  # Date: 09/07/17


rm(list=ls())
library(data.table); library(haven); library(readstata13); library(assertable); library(DBI); library(readr); library(plyr); library(mortdb, lib = "FILEPATH"); library(mortcore, lib = "FILEPATH")

if (Sys.info()[1] == "Linux") {
  root <- "/home/j/"
  username <- Sys.getenv("USER")
  before_kids <- commandArgs(trailingOnly = T)[1]
  ddm <- as.numeric(commandArgs(trailingOnly = T)[2])
  after_kids <- as.numeric(commandArgs(trailingOnly = T)[3])
  input_prep <- as.numeric(commandArgs(trailingOnly = T)[4])
  pre_5q0 <- as.numeric(commandArgs(trailingOnly = T)[5])
  post_5q0 <- as.numeric(commandArgs(trailingOnly = T)[6])
  gen_new <- as.numeric(commandArgs(trailingOnly = T)[7])
} else {
  root <- "J:/"
}

gbd_year <- 2017
project_flag <- "proj_mortenvelope"

source(paste0(root, "FILEPATH/get_population.R"))
source(paste0(root, "FILEPATH/get_ids.R"))


####################################################################
# Generate new run version_id and proper parent-child relationships
####################################################################
if (pre_5q0 == 1) {
  if (gen_new == 1) {
    child_version_est <- gen_new_version(model_name = "ddm",
                                         model_type = "estimate",
                                         comment = "GBD2017 - re-submission loop 8 (2nd attempt)")
    child_version_data <- gen_new_version(model_name = "ddm",
                                          model_type = "data",
                                          comment = "GBD2017 - re-submission loop 8 (2nd attempt)")
    estimate_parent_list = list()
    estimate_parent_list[['ddm data']] <- child_version_data
    gen_parent_child(parent_runs = estimate_parent_list, child_process = "ddm estimate", child_id = child_version_est)

      ## Create proper directories
    main_dir <- paste0("FILEPATH", child_version_est)

    dir.create(main_dir, showWarnings = F)
    dir.create(file.path(main_dir, "data"), showWarnings = F)
    dir.create(file.path(paste0(main_dir, "/data/"), "temp"), showWarnings = F)
    dir.create(file.path(main_dir, "inputs"), showWarnings = F)
    dir.create(file.path(main_dir, "diagnostics"), showWarnings = F)
    dir.create(file.path(paste0(main_dir, "/diagnostics/"), "dropped_deaths"), showWarnings = F)
    dir.create(file.path(paste0(main_dir, "/diagnostics/"), "dropped_pop"), showWarnings = F)
    dir.create(file.path(main_dir, "graphs"), showWarnings = F)
    dir.create(file.path(main_dir, "archive"), showWarnings = F)
    dir.create(file.path(main_dir, "upload"), showWarnings = F)
    dir.create(file.path("FILEPATH", as.character(child_version_est)), showWarnings = F)
    dir.create(file.path("FILEPATH", paste0("v", as.character(child_version_est))), showWarnings = F)
    dir.create(file.path(paste0("FILEPATH",child_version_est), "diagnostics"), showWarnings = F)
  }
  if (gen_new != 1){
    child_version_est <- get_proc_version(model_name = "ddm", model_type = "estimate", run_id = "recent", gbd_year = gbd_year)
  }

  parent_list <- get_best_versions(c("population estimate", "death number empirical data", "population empirical data"), gbd_year = gbd_year)

  pre_5q0_process_table <- data.table(process_name = names(parent_list), run_id = unlist(parent_list))
  write_csv(pre_5q0_process_table, paste0("FILEPATH", child_version_est, "FILEPATH"))
  
  prep_empirical <- 1
}

if(post_5q0 == 1){
  child_version_est <- get_proc_version(model_name = "ddm", model_type = "estimate", run_id = "recent", gbd_year = gbd_year)

  best_5q0_version_id <- get_proc_version(model_name = "5q0", model_type = "estimate", gbd_year = gbd_year, run_id = 'best')
  proc_lineage <- data.table(get_proc_lineage(model_name = "5q0", model_type = "estimate", run_id = best_5q0_version_id))
  data_5q0_version_id <- proc_lineage[parent_process_name == "5q0 data", parent_run_id]
  parent_list <- list()
  parent_list[['5q0 data']] <- data_5q0_version_id
  parent_list[['5q0 estimate']] <- best_5q0_version_id

  pre_5q0_process_table <- fread(paste0("FILEPATH", child_version_est, "FILEPATH"))
  process_table_5q0 <- data.table(process_name = names(parent_list), run_id = unlist(parent_list))
  process_table <- data.table(rbind(pre_5q0_process_table, process_table_5q0))
  write_csv(process_table, paste0("FILEPATH", child_version_est, "FILEPATH"))

  prep_empirical <- 0
}

## Set proper directories
main_dir <- paste0("FILEPATH", child_version_est)
inputs_outdir <- paste0(main_dir, "/inputs/")
source_outdir <- paste0("FILEPATH", child_version_est, "/data/")
code_dir <- paste0("FILEPATH", username, "/adult-mortality/ddm/")
function_dir <- paste0(code_dir, "functions")
r_shell <- "FILEPATH/FILEPATH.sh"


#####################################################################################
####################### Pull necessary data from mortality DB #######################
#####################################################################################


# Set up location metadata
ap_old <- data.table(get_locations(gbd_type = 'ap_old', level = 'estimate', gbd_year = gbd_year))
ap_old <- ap_old[ihme_loc_id %in% c("IND_44849", "XSU", "XYG")]

locations <- data.table(get_locations(gbd_year = gbd_year))
locations <- locations[!(grepl("KEN_", ihme_loc_id) & level == 4)]
locations <- rbind(locations, ap_old)
write_csv(locations, paste0(main_dir, "FILEPATH", gbd_year, ".csv"))
locations <- locations[order(ihme_loc_id),]
locations[, group := seq(0, 149, by =1)]
locs <- unique(locations$ihme_loc_id)
groups <- unique(locations$group)


##################################
# Empirical VR and population data
##################################

if (prep_empirical == 1){
  ##############
  # Population
  ##############


  pop_version_id <- parent_list[['population empirical data']]
  population <- get_mort_outputs(model_name = "population empirical", model_type = "data", location_set_id = 82, gbd_year = gbd_year, run_id = pop_version_id)
  population <- population[outlier == 0, list(location_id, year = year_id, precise_year, sex_id, age_group_id, source_type_id, pop_source = detailed_source, pop_nid = nid, underlying_pop_nid = underlying_nid, mean)]
  population <- merge(population, locations[, list(location_id, ihme_loc_id, country = location_name)], by = 'location_id', all.x =T)
  population <- population[!is.na(ihme_loc_id)]
  population[, year := as.double(year)]
  population[, precise_year := as.character(precise_year)]

  population[source_type_id == 1, source_type := "VR"]
  population[source_type_id == 2, source_type := "SRS"]
  population[source_type_id == 3, source_type := "DSP"]
  population[source_type_id == 5, source_type := "CENSUS"] 

  population[source_type_id == 34, source_type := "MOH survey"]

  ## Split out SSPC-DC
  population[source_type_id == 36, source_type := "SSPC"]
  population[source_type_id == 37, source_type := "DC"]

  population[source_type_id == 38, source_type := "FFPS"]
  population[source_type_id == 39, source_type := "SUPAS"]
  population[source_type_id == 40, source_type := "SUSENAS"]
  population[source_type_id == 16, source_type := "SURVEY"]
  population[source_type_id == 41, source_type := "HOUSEHOLD_DLHS"]
  population[source_type_id == 42, source_type := "HOUSEHOLD"]
  population[source_type_id == 43, source_type := "HOUSEHOLD_HHC"]

  population[source_type_id == 50, source_type := "SSPC-DC"]
  population[source_type_id == 52, source_type := "pop_registry"]
  population[source_type_id == 5 & ihme_loc_id== "IDN" & year == 1999 , source_type := "SURVEY"]
  population[pop_source == "CONAPO_FROM_KATE_LOFGREN_NOT_USABLE", source_type := "NOT_USABLE_MODELED_MEX"]
  population[pop_source == "CHN_PROV_CENSUS_OLD", source_type := "CENSUS_OLD"]
  population[pop_source == "NOT_USABALLS", source_type := "CENSUS_NOT_USABLE"]


  population[sex_id == 1, sex_name := "male"]
  population[sex_id == 2, sex_name := "female"]
  population[sex_id == 3, sex_name := "both"]
  setnames(population, "sex_name", "sex")


  population[source_type == "SURVEY" & ihme_loc_id== "ZAF" & year == 2007, year := 2006.69]

  population[, c('sex_id', 'source_type_id') := NULL]
  population[, pop_footnote := ""]

  pre_scaled_pop <- copy(population)
  pre_scaled_pop <- pre_scaled_pop[, list(ihme_loc_id, country, sex, year, source_type, pop_source, pop_footnote, pop_nid, underlying_pop_nid, mean, age_group_id)]
  write_csv(population, paste0(main_dir, "FILEPATH"))
  check_files("d00_compiled_population_pre_scaled.csv", paste0(main_dir, "/inputs"), continual = T)

  qsub("ddm_scalars", paste0(code_dir, "compute_scalars.r"), pass = list(child_version_est, gbd_year), slots = 3, mem= 6, submit=T, proj = project_flag, shell = r_shell)

  ages <- data.table(get_age_map('all'))
  ages[, age_group_name := gsub("years", "", age_group_name)]
  ages[, age_group_name := gsub("-", "to", age_group_name)]
  ages[, start := as.character(age_group_years_start)]
  ages[grepl(" ", age_group_name), age_gap := gsub(" ", "", age_group_name)]
  ages[!grepl(" ", age_group_name), age_gap := paste0(start, "to", start)]
  ages[, age_gap := paste0("@", age_gap)]
  ages[age_group_id == 283, age_gap := "@UNK"]
  ages[age_group_id == 22, age_gap := "@TOT"]
  ages[age_group_id == 24, age_gap := "@15to49"]
  ages[age_group_id == 26, age_gap := "@70plus"]
  ages[age_group_id == 29, age_gap := "@15plus"]
  ages <- ages[!age_group_id %in% c(308, 42, 27, 236, 164, 161)]
  ages[age_group_id == 1, age_gap := "@0to4"]
  ages[age_group_id == 28, age_gap := "@0to0"]
  ages <-ages[, list(age_group_id, age_gap, age_group_name)]

  population <- merge(population, ages, by = 'age_group_id', all.x = T)
  population[, age_group_id := NULL]
  population[, age_group_name := NULL]

  pop_dups_ages <- population[duplicated(population[, list(ihme_loc_id, year, sex, source_type, age_gap)])]
  if (nrow(pop_dups_ages) > 0) stop("Population data not unique by location-year-sex-source-age")

  population <- dcast.data.table(population, ihme_loc_id + location_id + country + year + precise_year + sex + source_type + pop_source + pop_footnote + pop_nid + underlying_pop_nid ~ age_gap, value.var = "mean", fill = NA_real_)

  setnames(population, grep("@", colnames(population)), gsub("@", "DATUM", grep("@", names(population), value = T)))

  check_files("correction_factors.csv", paste0(main_dir, "/inputs"), continual = T)
  Sys.sleep(60)
  scalars <- fread(paste0(main_dir, "/FILEPATH/correction_factors.csv"))
  scalars <- scalars[!grepl("CEN|VR", source_type)]
  scalars[, year := as.double(year)]
  scalars[ihme_loc_id=="ZAF" & source_type == "SURVEY" & year == 2006, year := 2006.69]
  scalars <- scalars[, list(ihme_loc_id, year, sex, source_type, correction_factor)]

  population <- merge(population, scalars, by = c('ihme_loc_id', 'year', 'sex', 'source_type'), all.x = T)
  if (nrow(population[!is.na(correction_factor)]) != nrow(scalars)) stop("Please check your merge. There are either more/less correction factors applied to the empirical population data.")

  population[is.na(correction_factor), correction_factor := 1]
  for (var in grep("DATUM", names(population), value =T)){
    population[, (var) := get(var) * correction_factor]
  }

  population[, correction_factor := NULL]

  china_dsp_locs <- unique(population[(source_type == "DSP" & grepl("CHN", ihme_loc_id)), list(ihme_loc_id, source_type)])
  loc_source_list <- china_dsp_locs$source_type
  names(loc_source_list) <- china_dsp_locs$ihme_loc_id
  loc_source_list <- as.list(loc_source_list)

  for (i in 1:length(loc_source_list)){
    source <- loc_source_list[[i]]
    loc <- names(loc_source_list)[i]
    tmp_data <- population[ihme_loc_id == loc & source_type == source]
    tmp_data_years <- sort(unique(tmp_data$year))
    
    min_year <- min(tmp_data_years)
    max_year <- max(tmp_data_years)
    
    use_years <- c()
    next_year <- tmp_data_years[1]
    for (year in tmp_data_years){
      if (year != next_year){
        next 
      }
      i <- match(year, tmp_data_years)
      i_plus <- i + 1
      if (i_plus > length(tmp_data_years)) break
      for (i_plus in i_plus:length(tmp_data_years)){
        current_year <- tmp_data_years[i]
        next_year <- tmp_data_years[i_plus]
        year_diff <- next_year - current_year
        if (year_diff == 5) {
          use_years <- c(use_years, next_year)
          break
        } 
        else if (year_diff != 5 & year_diff < 5){ 
          next
        }
        else if (year_diff != 5 & year_diff > 5) {
          use_years <- c(use_years, next_year)
          break
        }
      }
    }
    use_years <- c(tmp_data_years[1], use_years, max_year)
    population[ihme_loc_id == loc & source_type == source & (year %in% use_years), dsp_use_ddm := 1]
  }
  population[is.na(dsp_use_ddm), dsp_use_ddm := 0]

  pop_dups <- population[duplicated(population[, list(ihme_loc_id, year, sex, source_type)])]
  if (nrow(pop_dups) > 0) stop("Population data not unique by location-year-sex-source")

  population <- population[!(ihme_loc_id == "BRA_4776" & year %in% c(1950, 1960, 1970) & source_type == "CENSUS")]

  population <- population[!(ihme_loc_id == "JOR" & year == 1961 & source_type == "CENSUS")]

  save.dta13(population, paste0(main_dir, "FILEPATH"))
  check_files("d00_compiled_population.dta", paste0(main_dir, "/FILEPATH"), continual = T)


  ##########
  # Deaths
  ##########
  empirical_deaths_version_id <- parent_list[['death number empirical data']]
  deaths <- data.table(get_mort_outputs(model_name = "death number empirical", model_type = "data", gbd_year = gbd_year, location_set_id = 82, run_id = empirical_deaths_version_id))
  deaths <- deaths[outlier == 0, list(ihme_loc_id, location_id, year = year_id, sex = sex_id, age_group_id, source_type_id, deaths_source = detailed_source, deaths_nid = nid, deaths_underlying_nid = underlying_nid, mean)]
  deaths <- deaths[!location_id %in% c(420, 428)]
  deaths <- unique(deaths)
  deaths[, year := as.double(year)]


  ages <- data.table(get_age_map('all'))
  ages[, age_group_name := gsub("years", "", age_group_name)]
  ages[, age_group_name := gsub("-", "to", age_group_name)]
  ages[, start := as.character(age_group_years_start)]
  ages[grepl(" ", age_group_name), age_gap := gsub(" ", "", age_group_name)]
  ages[!grepl(" ", age_group_name), age_gap := paste0(start, "to", start)]
  ages[, age_gap := paste0("@", age_gap)]
  ages[age_group_id == 283, age_gap := "@UNK"]
  ages[age_group_id == 22, age_gap := "@TOT"]
  ages[age_group_id == 24, age_gap := "@15to49"]
  ages[age_group_id == 161, age_gap := "@0to0"]
  ages[age_group_id == 26, age_gap := "@70plus"]
  ages[age_group_id == 29, age_gap := "@15plus"]
  ages[age_group_id == 1, age_gap := "@0to4"]
  ages[age_group_id == 28, age_gap := "@0to0"]
  ages <-ages[, list(age_group_id, age_gap, age_group_name)]
  deaths <- merge(deaths, ages, by ='age_group_id', all.x = T)
  deaths[, age_group_id := NULL]

  deaths <- dcast.data.table(deaths, ihme_loc_id + location_id + year + sex + source_type_id + deaths_source + deaths_nid + deaths_underlying_nid ~ age_gap, value.var = "mean")
  setnames(deaths, grep("@", colnames(deaths)), gsub("@", "DATUM", grep("@", names(deaths), value = T)))

  deaths <- merge(deaths, locations[, list(location_id, country = location_name)], by = c('location_id'), all.x = T)
  deaths[, location_id := NULL]
  deaths[, year := as.double(year)]

  deaths[source_type_id == 1, source_type := "VR"]
  deaths[source_type_id == 2, source_type := "SRS"]
  deaths[source_type_id == 3, source_type := "DSP"]
  deaths[source_type_id == 5, source_type := "CENSUS"]
  deaths[source_type_id == 34, source_type := "MOH survey"]
  deaths[source_type_id == 36, source_type := "SSPC"]
  deaths[source_type_id == 37, source_type := "DC"]
  deaths[source_type_id == 38, source_type := "FFPS"]
  deaths[source_type_id == 39, source_type := "SUPAS"]
  deaths[source_type_id == 40, source_type := "SUSENAS"]
  deaths[source_type_id == 41, source_type := "HOUSEHOLD_DLHS"]
  deaths[source_type_id == 42, source_type := "HOUSEHOLD"]
  deaths[source_type_id == 43, source_type := "HOUSEHOLD_HHC"]
  deaths[source_type_id == 55, source_type := "MCCD"]
  deaths[source_type_id == 56, source_type := "CR"]


  deaths[source_type_id %in% c(20, 21, 22, 23), source_type := "DSP"]

  deaths[source_type_id %in% c(44, 45, 46, 47, 48, 49), source_type := "SRS"]

  deaths[source_type_id %in% c(24, 32, 33) & ihme_loc_id == "KOR", source_type := "VR"]

  deaths[source_type_id %in% c(30, 31) & grepl("ZAF", ihme_loc_id), source_type := "VR"]

  deaths[source_type_id %in% c(28, 29) & grepl("MEX", ihme_loc_id), source_type := "VR"]

  deaths[source_type_id == 1 & ihme_loc_id == "ZAF" & deaths_source == "stats_south_africa", source_type := "VR-SSA"]

  deaths[ihme_loc_id == "CMR" & year == 1976 & source_type_id == 42, source_type := "HOUSEHOLD"]

  deaths[source_type_id == 16, source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "BFA" & deaths_source == "OECD" & year == 1960, source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "BGD" & deaths_source == "OECD" & year %in% c(1962, 1963, 1964, 1965), source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "HTI" & deaths_source == "DYB" & year == 1972, source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "IDN" & deaths_source == "DYB" & year == 1964, source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "ISR" & deaths_source == "OECD" & year %in% c(1948, 1949), source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "KHM" & deaths_source == "DYB" & year %in% c(1959), source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "LBR" & deaths_source == "DYB" & year %in% c(1970), source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "MWI" & deaths_source == "DYB" & year %in% c(1971), source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "PAK" & deaths_source == "OECD" & year %in% c(1962, 1963, 1964, 1965), source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "SAU" & year %in% c(2006, 2016), source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "TGO" & year %in% c(1961), source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "TUR" & year %in% c(1967, 1989), source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "TZA" & year %in% c(1973), source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "ZAF" & year %in% c(2006), source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "GHA" & year %in% c(2010), source_type := "SURVEY"]
  deaths[source_type_id == 16 & ihme_loc_id == "SSD" & year %in% c(2008), source_type := "SURVEY"]
  deaths[source_type_id == 35 & ihme_loc_id == "BWA", source_type := "VR"]

  deaths[source_type_id == 16 & ihme_loc_id == "ZAF", year := 2006.69]
  deaths[, source_type_id := NULL]

  deaths <- deaths[!(ihme_loc_id == "COG" & year == 2009)]
  deaths <- deaths[!(ihme_loc_id == "MMR" & year %in% c(2005, 2010))]
  deaths <- deaths[!(ihme_loc_id == "MDV" & year %in% c(1975, 1976))]
  deaths <- deaths[!(ihme_loc_id == "OMN" & year %in% c(2001, 2003, 2004))]
  deaths <- deaths[!(ihme_loc_id == "TON" & year == 1990)]
  deaths <- deaths[!(ihme_loc_id == "NER" & year == 2011)]

  former_yugoslavia_location_ids <- c(44, 46, 49, 50, 53, 55)
  former_yugoslavia_ihme_loc_ids <- unique(locations[location_id %in% former_yugoslavia_location_ids, ihme_loc_id])
  deaths <- deaths[!(deaths_nid == "140201" & year >= 1992 & year <= 1995 & ihme_loc_id %in% former_yugoslavia_ihme_loc_ids)]

  deaths <- deaths[!(ihme_loc_id %in% c("EST", "RUS", "LTU", "LVA", "UKR") & source_type == "VR" & year == 1958)]

  deaths[, deaths_footnote := ""]

  deaths[sex == 1, sex_name := "male"]
  deaths[sex == 2, sex_name := "female"]
  deaths[sex == 3, sex_name := "both"]
  deaths[, sex := NULL]
  setnames(deaths, "sex_name", "sex")

  deaths[, deaths_nid := as.character(deaths_nid)]
  deaths[, deaths_underlying_nid := as.character(deaths_underlying_nid)]
  deaths[is.na(deaths_underlying_nid), deaths_underlying_nid := ""]

  assert_values(deaths, c('ihme_loc_id', 'country', 'sex', 'year', 'source_type', 'deaths_source'), 'not_na')
  assert_values(deaths, c('deaths_nid'), 'not_na', warn_only = T)

  deaths[, dup := 0]
  deaths[duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)]) |
           duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)], fromLast = T), dup := 1]
  deaths <- deaths[dup == 0 | (dup == 1 & !(deaths_source %in% c("WHO_causesofdeath", "DYB")))]
  deaths_dups <- deaths[duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)]) |
                          duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)], fromLast = T)]
  if (nrow(deaths_dups) != 0) stop("Your deaths data contains duplicates")
  deaths[, dup := NULL]

  tmp <- copy(deaths)
  tmp[, n := seq(.N), by = c('ihme_loc_id', 'year', 'source_type', 'deaths_source')]
  tmp[, max := max(n), by = c('ihme_loc_id', 'year', 'source_type','deaths_source')]
  both_sex_deaths <- unique(tmp[max ==2])
  both_sex_deaths[, c("n", "max") := NULL]
  both_sex_deaths[, sex := NULL]
  datum_cols <- grep("^DATUM", names(both_sex_deaths), value = T)
  both_sex_deaths <- both_sex_deaths[, lapply(.SD, function(x)replace(x, which(is.na(x)), -999)), .SDcols = datum_cols, by = c('ihme_loc_id', 'country', 'year', 'source_type', 'deaths_source', 'deaths_footnote', 'deaths_nid', 'deaths_underlying_nid')]

  both_sex_deaths <- both_sex_deaths[, lapply(.SD, sum, na.rm = T), .SDcols = grep("^DAT", names(both_sex_deaths), value = T), by = c('ihme_loc_id', 'country', 'year', 'source_type', 'deaths_source', 'deaths_footnote', 'deaths_nid', 'deaths_underlying_nid')]
  both_sex_deaths <- both_sex_deaths[, lapply(.SD, function(x) replace(x, which(x<0), NA)), .SDcols = datum_cols, by = c('ihme_loc_id', 'country', 'year', 'source_type', 'deaths_source', 'deaths_footnote', 'deaths_nid', 'deaths_underlying_nid')]
  both_sex_deaths[, sex := "both"]
  both_sex_deaths <- both_sex_deaths[,c('ihme_loc_id', 'country', 'sex', 'year', 'source_type', 'deaths_source', 'deaths_footnote', grep("^DATUM", names(both_sex_deaths), value = T), 'deaths_nid', 'deaths_underlying_nid'), with = F]

  deaths <- data.table(rbind(deaths, both_sex_deaths))

  deaths <- merge(deaths, scalars, by = c('ihme_loc_id', 'year', 'sex', 'source_type'), all.x = T)
  deaths[is.na(correction_factor), hh_scaled := 0]
  deaths[!is.na(correction_factor), hh_scaled := 1]
  deaths[is.na(correction_factor), correction_factor := 1]
  deaths[ihme_loc_id %in% c("BGD", "PAK") & source_type == "SRS", hh_scaled := 1]
  deaths[source_type %in% c("MCCD", "CR"), hh_scaled := 1]
  for (var in grep("DATUM", names(deaths), value =T)){
    deaths[, (var) := get(var) * correction_factor]
  }
  deaths[, c('correction_factor') := NULL]

  deaths_dups <- deaths[duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)])]
  if (nrow(deaths_dups) > 0) stop("Deaths data not unique by location-year-sex-source")

  save.dta13(deaths, paste0("FILEPATH", "d00_compiled_deaths_v", child_version_est,".dta"))
  save.dta13(deaths, paste0(source_outdir, "d00_compiled_deaths.dta"))
  check_files("d00_compiled_deaths.dta", paste0(main_dir, "/FILEPATH"), continual = T)

  qsub("ddm_nids", paste0(code_dir, "FILEPATH/ddm_prep_nids.do"), pass = list(child_version_est), slots = 2, mem= 4, submit=T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))
  Sys.sleep(60)

  qsub("ddm_hyperparameters", paste0(code_dir, "set_hyperparameters.r"), pass = list(child_version_est), slots = 1, mem= 2, submit=T, proj = project_flag, shell = r_shell)
  qsub("ddm_vr_both_sex_only", paste0(code_dir, "vr_both_sexes_only.r"), hold = "ddm_nids", pass = list(child_version_est), slots = 2, mem= 4, submit=T, proj = project_flag, shell = r_shell)
  qsub("ddm_sex_ratio", paste0(code_dir, "sex_ratio_prep.r"), hold = "ddm_vr_both_sex_only", pass = list(child_version_est), slots = 2, mem= 4, submit=T, proj = project_flag, shell = r_shell)
  check_files("vr_both_sex_only.csv", paste0(main_dir, "/FILEPATH"), continual = T)
  check_files("sex_ratios.csv", paste0(main_dir, "/FILEPATH"), continual = T)
}

if (input_prep == 1){
  if (post_5q0 == 1){
    qsub("ddm_input_prep", paste0(code_dir, "prep_input_data.r"), pass = list(child_version_est, post_5q0, gbd_year, parent_list[['5q0 data']], parent_list[['5q0 estimate']], inputs_outdir), slots = 2, mem= 4, submit=T, proj = project_flag, shell = r_shell)
  } else {
    qsub("ddm_input_prep", paste0(code_dir, "prep_input_data.r"), pass = list(child_version_est, post_5q0, gbd_year, 0, 0, inputs_outdir), slots = 2, mem= 4, submit=T, proj = project_flag, shell = r_shell)
  }
}


##############################################################################################
## Begin running format code
##############################################################################################
setwd(paste0("FILEPATH", username, "FILEPATH"))

if (pre_5q0 == 1){
  unlink(paste0("FILEPATH", child_version_est, "FILEPATH"))
  for (i in groups){
    qsub(paste0("ddm01_", i), paste0(code_dir, "c01_format_population_and_deaths.do"), pass = list(i, child_version_est, gbd_year), slots = 5, mem= 10, submit=T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))
  }

  # Check that all c01 files have completed
  for (loc in locs){
    print(loc)
    check_files(paste0("d01_formatted_population_", loc, ".dta"), paste0(main_dir, "FILEPATH"), continual = T)
    check_files(paste0("d01_formatted_deaths_", loc, ".dta"), paste0(main_dir, "FILEPATH"), continual = T)
  }
  qsub("ddm_compile_formatted_population", paste0(code_dir, "compile_files.do"), pass = list(child_version_est, "d01_formatted_population"), slots = 2, mem= 4, submit=T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))
  qsub("ddm_compile_formatted_deaths", paste0(code_dir, "compile_files.do"), pass = list(child_version_est, "d01_formatted_deaths"), slots = 2, mem= 4, submit=T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))
  
  check_files(paste0("d01_formatted_population.dta"), paste0(main_dir, "/FILEPATH"), continual = T)
  
  check_files(paste0("gbd_population_estimates.dta"), paste0(main_dir, "/FILEPATH"), continual = T)
  qsub("ddm_09", paste0(code_dir, "c09_compile_denominators.do"), hold = "ddm_input_prep", pass = list(child_version_est, gbd_year), slots= 2, mem= 4, submit= T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))
  check_files("d09_denominators.dta", paste0(main_dir, "/data"), continual= T)

  check_files(paste0("d01_formatted_deaths.dta"), paste0(main_dir, "/FILEPATH"), continual = T)
}

if (post_5q0 == 1){
  if(before_kids == 1){
    unlink(paste0("FILEPATH", child_version_est, "FILEPATH/d02*"))
    unlink(paste0("FILEPATH", child_version_est, "FILEPATH/d03*"))
    # c01
    for (i in groups){
      qsub(paste0("ddm02_", i), paste0(code_dir, "c02_reshape_population_and_deaths.do"), pass = list(i, child_version_est, gbd_year), slots = 5, mem= 10, submit=T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))
      qsub(paste0("ddm03_", i), paste0(code_dir, "c03_combine_population_and_deaths.do"), hold = paste0("ddm02_", i), pass = list(i, child_version_est, gbd_year), slots = 5, mem= 10, submit=T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))
    }
    
    for (loc in locs){
      print(loc)
      check_files(paste0("d02_reshaped_population_", loc, ".dta"), paste0(main_dir, "FILEPATH"), continual = T)
      check_files(paste0("d02_reshaped_deaths_", loc, ".dta"), paste0(main_dir, "FILEPATH"), continual = T)
    }
    Sys.sleep(300)
    qsub("ddm_compile_reshaped_population", paste0(code_dir, "compile_files.do"), pass = list(child_version_est, "d02_reshaped_population"), slots = 2, mem= 4, submit=T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))
    qsub("ddm_compile_reshaped_deaths", paste0(code_dir, "compile_files.do"), pass = list(child_version_est, "d02_reshaped_deaths"), slots = 2, mem= 4, submit=T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))

    for (loc in locs){
      print(loc)
      check_files(paste0("d03_combined_population_and_deaths_", loc, ".dta"), paste0(main_dir, "FILEPATH"), continual = T)
    }
    Sys.sleep(300)
    qsub("ddm_compile_d03", paste0(code_dir, "compile_files.do"), pass = list(child_version_est, "d03_combined_population_and_deaths"), slots = 2, mem= 4, submit=T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))

    ##############################################################################################
    # Check if files saved
    ##############################################################################################
    check_files(paste0("d02_reshaped_population.dta"), paste0(main_dir, "/FILEPATH"), continual = T)
    check_files(paste0("d02_reshaped_deaths.dta"), paste0(main_dir, "/FILEPATH"), continual = T)
    check_files(paste0("d03_combined_population_and_deaths.dta"), paste0(main_dir, "/FILEPATH"), continual = T)
  }

  ##############################################################################################
  ## Launch DDM steps
  ##############################################################################################
  if (ddm == 1) {qsub("ddm_04", paste0(code_dir, "c04_apply_ddm.do"), pass = list(child_version_est, function_dir), slots = 2, mem= 4, submit=T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))}
  check_files("d04_raw_ddm.dta", paste0(main_dir, "/FILEPATH"), continual = T)

  ##############################################################################################
  ## Launch after kids steps
  ##############################################################################################
  if (after_kids == 1) {
    qsub("ddm_05", paste0(code_dir, "c05_format_ddm.do"), pass = list(child_version_est, gbd_year), slots = 2, mem= 4, submit=T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))
    check_files("d05_formatted_ddm.dta", paste0(main_dir, "/FILEPATH"), continual = T)

    check_files(paste0("raw.5q0.unadjusted.dta"), paste0(main_dir, "/FILEPATH"), continual = T)
    check_files(paste0("estimated_5q0_noshocks.dta"), paste0(main_dir, "/FILEPATH"), continual = T)

    qsub("ddm_06", paste0(code_dir, "c06_calculate_child_completeness.do"), pass = list(child_version_est, gbd_year), slots = 2, mem= 4, submit=T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))
    check_files("d06_child_completeness.dta", paste0(main_dir, "/FILEPATH"), continual = T)

    qsub("ddm_07", paste0(code_dir, "c07_combine_child_and_adult_completeness.do"), pass = list(child_version_est), slots= 2, mem= 4, submit=T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))
    check_files("d07_child_and_adult_completeness.dta", paste0(main_dir, "/FILEPATH"), continual = T)

    check_files(paste0("hyperparameters.csv"), paste0(main_dir, "/FILEPATH"), continual = T)
    qsub("ddm_08", paste0(code_dir, "c08_smooth_ddm.r"), pass = list(child_version_est, gbd_year), slots= 15, mem = 30, submit=T, proj = project_flag, shell = r_shell)
    check_files("d08_smoothed_completeness.dta", paste0(main_dir, "/FILEPATH"), continual= T)

    check_files(paste0("raw.45q15.dta"), paste0(main_dir, "/FILEPATH"), continual = T)
    qsub("ddm_10", paste0(code_dir, "c10_calculate_45q15.do"), pass = list(child_version_est), slots= 2, mem= 4, submit= T, proj = project_flag, shell = paste0(code_dir, "run_all.sh"))
    check_files("d10_45q15.dta", paste0(main_dir, "/FILEPATH"), continual= T)
  }
}


# DONE