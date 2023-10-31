###########################################################
### Project: Age Split Headaches
### Purpose: GBD 2019 Nonfatal Estimation
###########################################################

## SET-UP
rm(list=ls())

pacman::p_load(data.table, ggplot2, boot)
library(openxlsx, lib.loc = paste0("FILEPATH"))

## SET OBJECTS
date <- gsub("-", "_", Sys.Date())
step <- "step1"

## SOURCE FUNCTION
source(paste0("FILEPATH", "get_bundle_data.R"))
source(paste0("FILEPATH", "upload_bundle_data.R"))
source(paste0("FILEPATH", "get_draws.R"))
source(paste0("FILEPATH", "get_population.R"))
source(paste0("FILEPATH", "get_location_metadata.R"))
source(paste0("FILEPATH", "age_table.R"))

## USER FUNCTION
col_order <- function(data.table){
  dt <- copy(data.table)
  epi_order <- fread(paste0("FILEPATH", "upload_order.csv"), header = F)
  epi_order <- tolower(as.character(epi_order[, V1]))
  epi_order <- epi_order[!epi_order == ""]
  names_dt <- tolower(names(dt))
  setnames(dt, names(dt), names_dt)
  for (name in epi_order){
    if (name %in% names(dt) == F){
      dt[, c(name) := ""]
    }
  }
  extra_cols <- setdiff(names(dt), tolower(epi_order))
  new_epiorder <- c(epi_order, extra_cols)
  setcolorder(dt, new_epiorder)
  return(dt)
}

age_split <- function(name){
  
  ## GET DATA
  print(paste0("age splitting ", name))
  print("getting data")
  bundle_id <- map[bundle_name == name, bundle_id]
  dt <- get_bundle_data(bundle_id = bundle_id, step)
  gbd_id <- map[bundle_name == name, meid]
  age <- c(2:20, 30:32, 235)
  
  all_age <- copy(dt)
  all_age <- col_order(all_age)
  original_names <- names(all_age)
  
  ## FORMAT DATA
  print(paste0("formatting data"))
  all_age <- all_age[measure %in% c("prevalence", "incidence"),]
  all_age <- all_age[!group_review==0 | is.na(group_review),] ##don't use group_review 0
  all_age <- all_age[is_outlier==0,] ##don't age split outliered data
  all_age <- all_age[(age_end-age_start)>20,]
  all_age <- all_age[!mean ==0, ] ##don't split points with zero prevalence
  all_age[, sex_id := sex]
  all_age[sex_id=="Both", sex_id :=3]
  all_age[sex_id=="Female", sex_id := 2]
  all_age[sex_id=="Male", sex_id :=1]
  all_age[, sex_id := as.integer(sex_id)]
  all_age[measure == "prevalence", measure_id := 5]
  all_age[measure == "incidence", measure_id := 6]
  all_age[, year_id := year_start] ##so that can merge on year later
  
  ## CALC CASES AND SAMPLE SIZE
  all_age[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  all_age[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  all_age[is.na(cases), cases := sample_size * mean]
  all_age <- all_age[!cases==0,] ##don't want to split points with zero cases
  all_age_original <- copy(all_age)
  
  ## ROUND AGE GROUPS
  all_age_round <- copy(all_age)
  all_age_round[, age_start := age_start - age_start %%5]
  all_age_round[, age_end := age_end - age_end %%5 + 4]
  all_age_round <- all_age_round[age_end > 99, age_end := 99]
  
  ## EXPAND FOR AGE  
  all_age_round[, n.age:=(age_end+1 - age_start)/5]
  all_age_round[, age_start_floor:=age_start]
  all_age_round[, drop := cases/n.age] 
  all_age_round <- all_age_round[!drop<1,]
  if (nrow(all_age_round) != 0) {
    seqs_to_split <- all_age_round[, unique(seq)]
    all_age_parents <- all_age_original[seq %in% seqs_to_split] ##keep copy of parents to attach on later
    expanded <- rep(all_age_round$seq, all_age_round$n.age) %>% data.table("seq" = .) 
    split <- merge(expanded, all_age_round, by="seq", all=T)
    split[, age.rep:= 1:.N - 1, by =.(seq)]
    split[, age_start:= age_start+age.rep*5]
    split[, age_end :=  age_start + 4 ]
    
    ## GET SUPER REGIONS
    print("getting super regions")
    super_region <- get_location_metadata(location_set_id = 22)
    super_region <- super_region[, .(location_id, super_region_id)]
    split <- merge(split, super_region, by = "location_id")
    super_regions <- unique(split$super_region_id) ##get super regions for dismod results
    
    ## GET AGE GROUPS
    all_age_total <- merge(split, ages, by = c("age_start", "age_end"), all.x = T)
    
    ## CREATE AGE GROUP ID 1
    all_age_total[age_start == 0 & age_end == 4, age_group_id := 1]
    
    ##GET LOCS AND POPS
    pop_locs <- unique(all_age_total$location_id)
    pop_years <- unique(all_age_total$year_id)
    
    ## GET AGE PATTERN
    locations <- super_regions
    print("getting age pattern")
    draws <- paste0("draw_", 0:999)
    age_pattern <- as.data.table(get_draws(gbd_id_type = "modelable_entity_id", gbd_id = gbd_id, 
                                           measure_id = c(5, 6), location_id = locations, source = "epi",
                                           status = "best", sex_id = c(1,2), gbd_round_id = 6,
                                           age_group_id = age, year_id = 2017, decomp_step = step)) ##imposing age pattern 
    us_population <- as.data.table(get_population(location_id = locations, year_id = 2017, sex_id = c(1, 2), 
                                                  age_group_id = age, decomp_step = step))
    us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]
    age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
    age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
    age_pattern[, (draws) := NULL]
    age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]
    
    print("formatting age pattern")
    ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
    age_1 <- copy(age_pattern)
    age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5), ]
    se <- copy(age_1)
    se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)]
    age_1 <- merge(age_1, us_population, by = c("age_group_id", "sex_id", "location_id"))
    age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
    age_1[, frac_pop := population / total_pop]
    age_1[, weight_rate := rate_dis * frac_pop]
    age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
    age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
    age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
    age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
    age_1[, age_group_id := 1]
    age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5)]
    age_pattern <- rbind(age_pattern, age_1)
    
    ## CASES AND SAMPLE SIZE
    age_pattern[measure_id == 5, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
    age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
    age_pattern[, cases_us := sample_size_us * rate_dis]
    age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
    age_pattern[is.nan(cases_us), cases_us := 0]
    
    ## GET SEX ID 3
    sex_3 <- copy(age_pattern)
    sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
    sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
    sex_3[, rate_dis := cases_us/sample_size_us]
    sex_3[measure_id == 5, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
    sex_3[measure_id == 6, se_dismod := sqrt(cases_us)/sample_size_us]
    sex_3[is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0 can't calculate rate and standard error, but should both be 0
    sex_3[is.nan(se_dismod), se_dismod := 0]
    sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
    sex_3[, sex_id := 3]
    age_pattern <- rbind(age_pattern, sex_3)
    
    age_pattern[, super_region_id := location_id]
    age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
    
    ## MERGE AGE PATTERN
    age_pattern1 <- copy(age_pattern)
    all_age_total <- merge(all_age_total, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
    
    ## GET POPULATION INFO
    print("getting populations for age structure")
    populations <- as.data.table(get_population(location_id = pop_locs, year_id = pop_years,
                                                sex_id = c(1, 2, 3), age_group_id = age, decomp_step = step))
    age_1 <- copy(populations) ##create age group id 1 by collapsing lower age groups
    age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5)]
    age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
    age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
    age_1[, age_group_id := 1]
    populations <- populations[!age_group_id %in% c(2, 3, 4, 5)]
    populations <- rbind(populations, age_1)  ##add age group id 1 back on
    all_age_total <- merge(all_age_total, populations, by = c("location_id", "sex_id", "year_id", "age_group_id"))
    
    #####CALCULATE AGE SPLIT POINTS#######################################################################
    ## CREATE NEW POINTS
    print("creating new age split points")
    all_age_total[, total_pop := sum(population), by = "seq"]
    all_age_total[, sample_size := (population / total_pop) * sample_size]
    all_age_total[, cases_dis := sample_size * rate_dis]
    all_age_total[, total_cases_dis := sum(cases_dis), by = "seq"]
    all_age_total[, total_sample_size := sum(sample_size), by = "seq"]
    all_age_total[, all_age_rate := total_cases_dis/total_sample_size]
    all_age_total[, ratio := mean / all_age_rate]
    all_age_total[, mean := ratio * rate_dis]
    ######################################################################################################
    
    ## FIX OVER 1
    mean1 <- all_age_total[mean >= 1, .(nid, location_id, sex, year_id, measure, year_start, year_end)]
    mean1 <- unique(mean1, by = c("nid", "location_id", "sex", "year_id", "measure", "year_start", "year_end"))
    mean1[, flag := 1]
    all_age_total <- merge(all_age_total, mean1, by = c("nid", "location_id", "sex", "year_id", "measure", "year_start", "year_end"), all.x = T)
    all_age_total[flag == 1, `:=` (is_outlier = 1, note_modeler = paste0(note_modeler, " | outliered because age series has means over 1"))]
    all_age_total[, flag := NULL]
    all_age_total[mean>=1, mean := 1]
    
    ## FORMATTING
    all_age_total[, group := 1]
    all_age_total[, specificity := paste0(specificity, ", age-split child")]
    all_age_total[, group_review := 1]
    blank_vars <- c("lower", "upper", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", "seq", "standard_error", "cases")
    all_age_total[, blank_vars := "", with = F]
    all_age_total <- col_order(all_age_total)
  
    
    ## ADD PARENTS
    all_age_parents <- col_order(all_age_parents)
    all_age_parents <- merge(all_age_parents, mean1, by = c("nid", "location_id", "sex", "year_id", "measure", "year_start", "year_end"), all.x = T)
    all_age_parents[flag == 1, group_review := 1]
    all_age_parents[is.na(flag), group_review := 0]
    all_age_parents[flag == 1, note_modeler := paste0(note_modeler, " | added back in because age split contained values over 1")]
    all_age_parents[, flag := NULL]
    all_age_parents[, group := 1]
    all_age_parents[, specificity := paste0(specificity, ", age-split parent")]
    all_age_total[, setdiff(names(all_age_total), names(all_age_parents)) := NULL] ## make columns the same
    total <- rbind(all_age_parents, all_age_total)
    total <- total[, c(original_names), with = F] ## get rid of extra columns
    total[, note_modeler := paste0(note_modeler, " | age split using the age pattern from super region age pattern on ", date)]
  } else {
    total <- data.table(matrix(ncol = 81, nrow = 0))
    colnames(total) <- original_names
  }
  ## BREAK IF NO ROWS
  if (nrow(total) == 0){
    print(paste0("nothing in bundle ", name, " to age-sex split"))
  } else {
    print(paste0("age split ", length(unique(all_age_total$nid)), " nids"))
  }
  return(total)
}

apply_outliers <- function(name){
  print(paste0("applying outliers for ", name))
  dt <- get(name)
  to_outlier <- outlier_dt[bundle == name, .(nid, sex, note)]
  dt[, nid := as.integer(nid)]
  dt <- merge(dt, to_outlier, by = c("nid", "sex"), all.x = T)
  dt[!is.na(note) & grepl("parent", specificity), `:=` (group_review = 1,
                                                        note_modeler = paste0(note_modeler, " | ", note))]
  dt[!is.na(note) & grepl("child", specificity), `:=` (is_outlier = 1,
                                                      note_modeler = paste0(note_modeler, " | ", note))]
  dt[, note := NULL]
  return(dt)
}

upload_data <- function(name){
  print(paste0("uploading for ", name))
  dt <- get(name)
  cause <- map[bundle_name == name, acause]
  id <- map[bundle_name == name, bundle_id]
  write.xlsx(dt, paste0("FILEPATH"), sheetName = "extraction")
  upload_bundle_data(bundle_id = id, filepath = paste0("FILEPATH"), decomp_step = step)
}

upload_data_decomp1 <- function(name){
  print(paste0("uploading for ", name))
  dt <- get(name)
  dt <- merge(dt, loc_data, by = "location_id", all.x = T)
  dt <- dt[(parent_id %in% c(86, 214, 165, 16, 51)),] # | (location_id %in% c(86, 214, 165, 16, 51))
  dt[, parent_id := NULL]
  dt <- col_order(dt)
  cause <- map[bundle_name == name, acause]
  id <- map[bundle_name == name, bundle_id]
  write.xlsx(dt, paste0("FILEPATH"), sheetName = "extraction")
  upload_bundle_data(bundle_id = id, filepath = "FILEPATH", decomp_step = step)
}

## GET MAP
map <- fread(paste0("FILEPATH", "bundle_map.csv"))

## AGE SPLIT
split <- lapply(map[adjust == "adjusted", bundle_name], age_split)
for (i in 1:length(split)){
  name <- map[adjust == "adjusted", bundle_name][i]
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(split[i]))
}

## APPLY OUTLIERS
outlier_dt <- fread(paste0("FILEPATH", "agesplit_outliers.csv"))
outlier_bundles <- outlier_dt[, unique(bundle)]
outliered <- lapply(outlier_bundles, apply_outliers)
for (i in 1:length(outliered)){
  name <- outlier_bundles[i]
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(outliered[i]))
}

# ## UPLOAD
# lapply(map[adjust == "adjusted", bundle_name], upload_data)

## UPLOAD - DECOMP 1
source("FILEPATH")
loc_data <- as.data.table(get_location_metadata(location_set_id = 35, gbd_round_id = 6))
loc_data <- loc_data[, c("location_id", "parent_id")]
lapply(map[adjust == "adjusted", bundle_name], upload_data_decomp1)
