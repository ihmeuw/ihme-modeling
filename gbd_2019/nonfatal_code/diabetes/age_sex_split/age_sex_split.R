###########################################################
### Project: GBD Nonfatal Estimation
### Purpose: Age-Sex Split and Age Split
### Description: These functions will take the data from a
###  bundle and either age-split or age-sex-split and will
###  write xlsx files to the bundle structure
### Notes: make sure to change the location you keep the
###  age table code in
###########################################################

##########################################################
## ARGUMENTS FOR AGE-SPLIT
## bundle = what bundle you are using
## gbd_id = the meid of the thing you are modeling
## acause = the short name (acause)
## age = what ages your cause applies to (ex. c(9:20, 30:33, 235))
## region_pattern = T/F do you want to use super_region age
##  pattern or pattern from one country
## location_pattern_id = if not using region pattern,
##  location id of the country whose pattern you want to use
##########################################################

##########################################################
## ARGUMENTS FOR AGE-SEX-SPLIT
## bundle = what bundle you are using
## acause = the short name (acause)
##########################################################

## SET-UP
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

pacman::p_load(data.table, ggplot2, magrittr, openxlsx)

## OBJECTS
temp_dir <- paste0(j_root, "FILEPATH")
repo_dir <- paste0(h_root, "FILEPATH")
functions_dir <- paste0(j_root, "FILEPATH")
age_table_code <- paste0(repo_dir, "age_table.R")
date <- Sys.Date()
date <- gsub("-", "_", date)

## SOURCE FUNCTIONS
source(paste0(functions_dir, "get_draws.R"))
source(paste0(functions_dir, "get_population.R"))
source(paste0(functions_dir, "get_location_metadata.R"))
source(paste0(functions_dir, "get_epi_data.R"))
source(age_table_code)

## USER FUNCTIONS
col_order <- function(dt){
  epi_order <- fread(paste0(temp_dir, "upload_order.csv"), header = F)
  epi_order <- tolower(as.character(epi_order[!V1 == "", V1]))
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
bundle<-bundle_id
all_age <- get_epi_data(bundle_id = bundle)

age_split(bundle = bundle_id, acause="cause_name", region_pattern = F, location_pattern_id = 1,age=c(8:20, 30:32, 235))

age_split <- function(bundle, gbd_id, acause, age, region_pattern, location_pattern_id){

  print(paste0("getting data"))
  all_age <- get_epi_data(bundle_id = bundle)
  all_age[, `:=` (mean = as.numeric(mean), sample_size = as.numeric(sample_size), cases = as.numeric(cases),
                  age_start = as.numeric(age_start), age_end = as.numeric(age_end), year_start = as.numeric(year_start))]
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
  all_age_round[, drop := cases/n.age] ##drop the data points if cases/n.age is less than 1
  all_age_round <- all_age_round[!drop<1,]
  seqs_to_split <- all_age_round[, unique(seq)]
  all_age_parents <- all_age_original[seq %in% seqs_to_split] ##keep copy of parents to attach on later
  expanded <- rep(all_age_round$seq, all_age_round$n.age) %>% data.table("seq" = .)
  split <- merge(expanded, all_age_round, by="seq", all=T)
  split[,age.rep:= 1:.N - 1, by =.(seq)]
  split[,age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4 ]

  ## GET SUPER REGION INFO
  if (region_pattern == T){
    print("getting super regions")
    super_region <- get_location_metadata(location_set_id = 22)
    super_region <- super_region[, .(location_id, super_region_id)]
    split <- merge(split, super_region, by = "location_id")
    super_regions <- unique(split$super_region_id) ##get super regions for dismod results
  }

  ## GET AGE GROUPS
  all_age_total <- merge(split, age, by = c("age_start", "age_end"), all.x = T)

  ## CREATE AGE GROUP ID 1
  all_age_total[age_start == 0 & age_end == 4, age_group_id := 1]
  all_age_total <- all_age_total[age_group_id %in% age | age_group_id ==1] ##don't keep where age group id isn't estimated for cause

  ##GET LOCS AND POPS
  pop_locs <- unique(all_age_total$location_id)
  pop_years <- unique(all_age_total$year_id)

  ## GET AGE PATTERN
  if (region_pattern == T) {
    locations <- super_regions
  } else {
    locations <- location_pattern_id
  }
  print("getting age pattern")
  draws <- paste0("draw_", 0:999)
  age_pattern <- as.data.table(get_draws(gbd_id_type = "modelable_entity_id", gbd_id = gbd_id,
                                         measure_id = c(5, 6), location_id = locations, source = "epi",
                                         status = "best", sex_id = c(1,2), gbd_round_id = 5,
                                         age_group_id = age, year_id = 2017)) ##imposing age pattern
  us_population <- as.data.table(get_population(location_id = locations, year_id = 2017, sex_id = c(1, 2),
                                                age_group_id = age))
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
  se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] ##just use standard error from 1-4 age group (as per Theo)
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

  if (region_pattern == T) {
    age_pattern1 <- copy(age_pattern)
    all_age_total <- merge(all_age_total, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
  } else {
    age_pattern1 <- copy(age_pattern)
    all_age_total <- merge(all_age_total, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
  }

  ## GET POPULATION INFO
  print("getting populations for age structure")
  populations <- as.data.table(get_population(location_id = pop_locs, year_id = pop_years,
                                              sex_id = c(1, 2, 3), age_group_id = age))
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

  ## FORMATTING
  all_age_total[, group := 1]
  all_age_total[, specificity := paste0(specificity, ", age-split child")]
  all_age_total[, group_review := 1]
  blank_vars <- c("lower", "upper", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", "seq", "standard_error", "cases")
  all_age_total[, blank_vars := NULL, with = F]
  all_age_total <- col_order(all_age_total)

  ## ADD PARENTS
  all_age_parents <- col_order(all_age_parents)
  invisible(all_age_parents[, group_review := 0])
  invisible(all_age_parents[, group := 1])
  invisible(all_age_parents[, specificity := paste0(specificity, ", age-split parent")])
  all_age_total[, setdiff(names(all_age_total), names(all_age_parents)) := NULL] ## make columns the same
  total <- rbind(all_age_parents, all_age_total)
  total <- total[, c(original_names), with = F] ## get rid of extra columns
  if (region_pattern ==T) {
    total[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern", date)]
  } else {
    total[, note_modeler := paste0(note_modeler, "| age split using the age pattern from location id ", location_pattern_id, " ", date)]
  }

  ## BREAK IF NO ROWS
  if (nrow(total) == 0){
    print("nothing in bundle to age-sex split")
    break
  }

  write.xlsx(total, "FILEPATH", sheetName = "extraction")
}


age_sex_split <- function(bundle, acause){

  ## LOAD DATA
  print("getting data")
  df <- get_epi_data(bundle_id = bundle)
  df <- df[!group_review == 0]
  df <- df[!is_outlier == 1]
  df[, seq := as.character(seq)]
  df[, note_modeler := as.character(note_modeler)]

  df_split <- copy(df)

  ## SUBSET DATA
  df_split[is.na(sample_size), note_modeler := paste0(note_modeler, " | sample size back calculated to age sex split")]
  df_split[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  df_split[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  df_split[is.na(cases), cases := sample_size * mean]
  df_split <- df_split[!is.na(cases),]
  df_split[, split := length(specificity[specificity == "age,sex"]), by = list(nid, group, measure, year_start, year_end)]
  df_split <- df_split[specificity %in% c("age", "sex") & split == 0,]

  ## CALC CASES MALE/FEMALE
  df_split[, cases_total:= sum(cases), by = list(nid, group, specificity, measure)]
  df_split[, prop_cases := round(cases / cases_total, digits = 3)]

  ## CALC PROP MALE/FEMALE
  df_split[, ss_total:= sum(sample_size), by = list(nid, group, specificity, measure)]
  df_split[, prop_ss := round(sample_size / ss_total, digits = 3)]

  ## CALC SE
  df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  df_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]

  ## RATIO
  df_split[, ratio := round(prop_cases / prop_ss, digits = 3)]
  df_split[, se_ratio:= round(sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) ), digits = 3)]
  df_ratio <- df_split[specificity == "sex", list(nid, location_id, group, sex, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end)]

  ## CREATE NEW OBSERVATIONS
  print("creating new observations")
  age.sex <- copy(df_split[specificity == "age"])
  age.sex[,specificity := "age,sex"]
  age.sex[,seq := ""]
  age.sex[,c("ratio", "se_ratio", "split",  "cases_total",  "prop_cases",  "ss_total",	"prop_ss",	"se_cases",	"se_ss") := NULL]
  male <- copy(age.sex[, sex := "Male"])
  female <- copy(age.sex[, sex := "Female"])

  age.sex <- rbind(male, female)
  age.sex <- merge(age.sex, df_ratio, by = c("nid", "location_id", "group", "sex", "measure", "year_start", "year_end"))

  ## CALC MEANS
  age.sex[, standard_error := round(sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2), digits = 4)]
  age.sex[, mean := mean * ratio]
  age.sex[, cases := round(cases * prop_cases, digits = 0)]
  age.sex[, sample_size := round(sample_size * prop_ss, digits = 0)]
  age.sex[,note_modeler := paste(note_modeler, "| age,sex split using sex ratio", round(ratio, digits = 2))]
  age.sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss") := NULL]
  age.sex[, c("lower", "upper", "uncertainty_type_value") := ""]

  ## CREATE UNIQUE GROUP TO GET PARENTS
  age.sex.m <- age.sex[,c("nid","group", "measure"), with=F]
  age.sex.m <- unique(age.sex.m, by=c("nid","group", "measure"))

  ## GET PARENTS
  parent <- merge(age.sex.m, df, by= c("nid", "group", "measure"))
  parent[specificity == "age" | specificity == "sex", group_review:=0]
  parent[, note_modeler := paste0(note_modeler, " | parent data, has been age-sex split")]

  ## FINAL DATA
  total <- rbind(parent, age.sex)
  total <- col_order(total)

  ## BREAK IF NO ROWS
  if (nrow(total) == 0){
    print("nothing in bundle to age-sex split")
    break
  }

  ## SAVE DATA
  write.xlsx(total, "FILEPATH", row.names=F, showNA=F, sheetName="extraction")
}



