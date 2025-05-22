###########################################################
#AGE SPLITTING CODE FOR NONFATAL MODELS
###########################################################

#SETUP------------------------------------------------------------------------------------------------------
date <- gsub("-", "_", Sys.Date())
pkg_lib <- "FILEPATH"

##SOURCE FUNCTIONS-------------------------------------------------------------------------------------------
source("FILEPATH")
source_shared_functions(functions = c("get_draws", "get_population", "get_location_metadata",
                                      "get_age_metadata", "get_ids", "get_crosswalk_version",
                                      "save_crosswalk_version", "get_best_model_versions"))
pacman::p_load(data.table, openxlsx, ggplot2, magrittr, tidyr)


##ARGS & DIRS--------------------------------------------------------------------------------------------------
cause_name <- "endometriosis"
bundle_id <- OBJECT
draws <- paste0("draw_", 0:999)

temp_dir <- "FILEPATH"
xwalk_temp <- "FILEPATH"
out_dir <- "FILEPATH"

#get age tables
ages <- get_ages()

#PREP CROSSWALKED AND/OR SEX_SPLIT DT TO BE AGE_SPLIT--------------------------------------------------------------------------------------------------
database <- F
filepath <- T
memory <- F

prep_for_split <- function(bvid, to_as_fpath, dt_in_memory){
  if (database == T){
    dt = get_bundle_version(bundle_version_id = bvid, fetch = "all", export = FALSE, transform = TRUE)
  } else if (filepath == TRUE) {
    dt <-  data.table(read.xlsx(to_as_fpath))
  } else if (memory == TRUE){
    dt <- copy(dt_in_memory)
  }

  print(paste0("There are ",nrow(dt[measure == "prevalence"]), " prevalence rows in the dt."))
  print(paste0("# of 'Both' sex rows: ", nrow(dt[sex == "Both" & measure == "prevalence"])))
  print(paste0("# of rows that need to be age-split: ", nrow(dt[(age_end - age_start > 5) & measure == "prevalence"])))

  dt[measure %in% c("prevalence", "proportion") & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  dt[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := as.numeric(sample_size) * as.numeric(mean)]
  #set it up so that seqs will line up correctly
  dt[ ,age_seq := seq]

  dt[is.na(age_seq), age_seq := crosswalk_parent_seq]
  dt[, id_seq := 1:nrow(dt)]
  return(dt)
}

for_AS_fpath <- paste0("FILEPATH")
print(for_AS_fpath)
prepped_dt <- prep_for_split(bvid = bundle_version, to_as_fpath = for_AS_fpath, dt_in_memory = full_bv_xwalked)


#SPECIFY LAST COUPLE OF PARAMETERS------------------------------------------------------------------------------------------------------------------------
dt_to_agesplit  <- copy(prepped_dt)
age                 <- c(2:20, 30:32, 235)

#BEGIN TO RUN THE FUNCTION---------------------------------------------------------------------------------------------------------------------------------
age_split <- function(split_meid, year_id = 2010, age,
                          location_pattern_id, measures = c("prevalence", "incidence"), measure_ids = c(5,6)){

  print(paste0("getting data"))
  all_age <- copy(dt_to_agesplit)

  ## FORMAT DATA
  print(paste0("formatting data"))
  all_age <- all_age[measure %in% measures,]

  tri_cols <- c("group", "specificity", "group_review")
  if (tri_cols[1] %in% names(all_age)){
    all_age <- all_age[group_review %in% c(1,NA),] 
  } else {
    all_age[ ,`:=` (group = as.numeric(), specificity = "", group_review = as.numeric())]
  }

  all_age <- all_age[(age_end-age_start)>5,]
  all_age <- all_age[!mean ==0, ] ##don't split points with zero prevalence
  all_age[sex=="Female", sex_id := 2]
  all_age[sex=="Male", sex_id :=1]
  all_age[, sex_id := as.integer(sex_id)]
  all_age[measure == "proportion", measure_id := 18]
  all_age[measure == "prevalence", measure_id := 5]
  all_age[measure == "incidence", measure_id := 6]
  all_age[, year_id := year_start] 

  ## CALC CASES AND SAMPLE SIZE
  print(paste0("CALC CASES AND SAMPLE SIZE"))
  all_age <- all_age[cases!=0,] ##don't want to split points with zero cases
  all_age_original <- copy(all_age)

  ## ROUND AGE GROUPS
  print(paste0("ROUND AGE GROUPS"))
  all_age_round <- copy(all_age)
  all_age_round[, age_start := age_start - age_start %%5]
  all_age_round[, age_end := age_end - age_end %%5 + 4]
  all_age_round <- all_age_round[age_end > 99, age_end := 99]

  ## EXPAND FOR AGE
  print(paste0("EXPAND FOR AGE"))
  all_age_round[, n_age:=(age_end+1 - age_start)/5]
  all_age_round[, age_start_floor:=age_start]
  all_age_round[ ,cases := as.numeric(cases)]
  all_age_round[, drop := cases/n_age] ##drop the data points if cases/n_age is less than 1 
  all_age_round <- all_age_round[!drop<1,]
  seqs_to_split <- all_age_round[, unique(id_seq)] #changed from seq to id seq
  all_age_parents <- all_age_original[id_seq %in% seqs_to_split] 
  expanded <- rep(all_age_round$id_seq, all_age_round$n_age) %>% data.table("id_seq" = .) #duplicate rows by the number of groups for the given age
  split <- merge(expanded, all_age_round, by="id_seq", all=T)
  split[,age.rep:= (1:.N - 1), by =.(id_seq)]
  split[,age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4 ]

  ## GET SUPER REGION INFO
  print("getting super regions")
  super_region <- get_location_metadata() 
  super_region <- super_region[, .(location_id, super_region_id)]
  split <- merge(split, super_region, by = "location_id")
  super_regions <- unique(split$super_region_id) ##get super regions for dismod results

  ## GET AGE GROUPS #THIS IS WHERE YOU REMERGE
  all_age_total <- merge(split, ages, by = c("age_start", "age_end"), all.x = T)
  all_age_total <- data.table(all_age_total)

  ## CREATE AGE GROUP ID 1
  all_age_total[age_start == 0 & age_end == 4, age_group_id := 1] 
  all_age_total <- all_age_total[age_group_id %in% age] 

  ##GET LOCS AND POPS
  pop_locs <- unique(all_age_total$location_id)
  pop_years <- unique(all_age_total$year_id)

  ## GET AGE PATTERN
  print("GET AGE PATTERN")

  locations <- location_pattern_id
  print("getting age pattern")
  draws <- paste0("draw_", 0:999)
  print(split_meid)

  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = split_meid,
                           measure_id = measure_ids, location_id = locations, source = "epi",
                           status = "best", sex_id = unique(all_age$sex_id),
                           age_group_id = age, year_id = 2010) ##imposing age pattern

  global_population <- as.data.table(get_population(location_id = locations, year_id = 2010, sex_id = 2,
                                                age_group_id = age))
  global_population <- global_population[, .(age_group_id, sex_id, population, location_id)]
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]

  print("formatting age pattern")

  ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
  age_1 <- copy(age_pattern)
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5,6)]
  se <- copy(age_1)
  se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] 
  age_1 <- merge(age_1, global_population, by = c("age_group_id", "sex_id", "location_id"))
  age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
  age_1[, frac_pop := population / total_pop]
  age_1[, weight_rate := rate_dis * frac_pop]
  age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
  age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
  age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
  age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
  age_1[, age_group_id := 1]
  age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5,6)]
  age_pattern <- rbind(age_pattern, age_1)

  ## CASES AND SAMPLE SIZE
  age_pattern[measure_id %in% c(5, 18), sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
  age_pattern[is.nan(cases_us), cases_us := 0]

  ## GET SEX ID 3
  sex_3 <- copy(age_pattern)
  sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, rate_dis := cases_us/sample_size_us]
  sex_3[measure_id %in% c(5, 18), se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
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
  all_age_total <- merge(all_age_total, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))

  ## GET POPULATION INFO
  print("getting populations for age structure")
  populations <- as.data.table(get_population(location_id = pop_locs, year_id = pop_years,
                                              sex_id =unique(all_age$sex_id), age_group_id = age))
  age_1 <- copy(populations) 
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5,6)]
  age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
  age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
  age_1[, age_group_id := 1]
  populations <- populations[!age_group_id %in% c(2, 3, 4, 5,6)]
  populations <- rbind(populations, age_1)  
  total_age <- merge(all_age_total, populations, by = c("location_id", "sex_id", "year_id", "age_group_id"))

  #####CALCULATE AGE SPLIT POINTS#######################################################################
  ## CREATE NEW POINTS
  print("creating new age split points")
  total_age[, total_pop := sum(population), by = "id_seq"]
  total_age[, sample_size := (as.numeric(population) / as.numeric(total_pop)) * as.numeric(sample_size)]
  total_age[, cases_dis := sample_size * rate_dis]
  total_age[, total_cases_dis := sum(cases_dis), by = "id_seq"]
  total_age[, total_sample_size := sum(sample_size), by = "id_seq"]
  total_age[, all_age_rate := total_cases_dis/total_sample_size]
  total_age[, ratio := mean / all_age_rate]
  total_age[, mean := ratio * rate_dis]
  total_age[, cases := sample_size * mean] 

  ######################################################################################################
  ## FORMATTING
  total_age[, specificity := paste0(specificity, ", age-split child")]
  total_age[ ,specificity := "age-split child"]
  total_age[, group := 1]
  total_age[, group_review := 1]

  blank_vars <- c("lower", "upper", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", "standard_error", "cases") 
  total_age[ ,(blank_vars) := NA] #this will not work properly without parenthesis
  total_age[, crosswalk_parent_seq := age_seq]
  total_age[ , seq := NA]

  ## ADD PARENTS
  all_age_parents[, crosswalk_parent_seq := NA]
  all_age_parents[ ,measure_id := NULL]
  all_age_parents[, group_review := 0]
  all_age_parents[, group := 1]
  all_age_parents[, specificity := paste0(specificity, ", age-split parent")]
  all_age_parents[ ,specificity := "age-split parent"]

  #MORE FORMATTING
  total_age[, setdiff(names(total_age), names(all_age_parents)) := NULL] ## make columns the same
  total <- rbind(all_age_parents, total_age)
  total <- total[mean > 1, group_review := 0]
  total$sex_id <- NULL
  total$year_id <- NULL

  #RBIND TO THE NON AGE-SPLIT DATA POINTS
  non_split <- dt_to_agesplit[!(id_seq %in% seqs_to_split)]
  all_points <- rbind(non_split, total, fill = TRUE)
  print("finished.")
  return(all_points)
}
agesplit_data <- age_split(split_meid = split_meid, year_id = 2010, age, location_pattern_id = location_pattern_id, measures = c("prevalence", "incidence"), measure_ids = c(5,6))


agesplit_data

