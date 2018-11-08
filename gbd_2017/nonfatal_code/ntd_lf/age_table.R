###########################################################
### Date: 10/27/2017
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

library(data.table)
library(openxlsx, lib.loc = paste0(j_root, "FILEPATH"))

## OBJECTS
temp_dir <- paste0(j_root, "FILEPATH")
functions_dir <- paste0(j_root, "FILEPATH")
age_table_code <- paste0(temp_dir, "age_table.R") ## SWITCH TO WHERE YOU KEEP THIS CODE
date <- Sys.Date()
date <- gsub("-", "_", date)

## SOURCE FUNCTIONS
source(paste0(functions_dir, "get_draws.R"))
source(paste0(functions_dir, "get_population.R"))
source(paste0(functions_dir, "get_location_metadata.R"))
source(paste0(functions_dir, "get_epi_data.R"))
source(paste0(functions_dir, "upload_epi_data.R"))
#source(age_table_code)

## USER FUNCTIONS
col_order <- function(dt){
  epi_order <- fread(paste0(temp_dir, "upload_order.csv"), header = F)
  epi_order <- tolower(as.character(epi_order[, V1]))
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

age_split <- function(bundle, gbd_id, acause, age, region_pattern, location_pattern_id){

  # manual exploration
  bundle <- 61
  gbd_id <- 354
  acause <- "ntd_lf"
  age <- c(5:21, 30:32, 235)
  region_pattern <- TRUE

  ages <- fread(paste0(temp_dir, "age_table.csv"), header = T)


  print(paste0("getting data"))
  all_age <- get_epi_data(bundle_id = bundle)
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
  all_age[sex_id=="Both", sex_id :="3"]
  all_age[sex_id=="Female", sex_id := "2"]
  all_age[sex_id=="Male", sex_id :="1"]
  all_age[, sex_id := as.integer(sex_id)]
  all_age[measure == "prevalence", measure_id := 5]
  all_age[measure == "incidence", measure_id := 6]
  all_age[, year_id := year_start] ##so that can merge on year later

  ## CALC CASES AND SAMPLE SIZE
  all_age[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  all_age[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  all_age[is.na(cases), cases := sample_size * mean]
  all_age <- all_age[!cases==0,] ##don't want to split points with zero cases

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
  all_age_parents <- copy(all_age_round) ##keep copy of parents to attach on later

  ## do this for pre, post, and unknown MDA status
  # 1=pre, 2=post, 3=na
  expanded.1 <- rep(all_age_round[mda_binary == 0]$seq, all_age_round[mda_binary == 0]$n.age) %>% data.table("seq" = .)
  expanded.2 <- rep(all_age_round[mda_binary == 1]$seq, all_age_round[mda_binary == 1]$n.age) %>% data.table("seq" = .)
  expanded.3 <- rep(all_age_round[is.na(mda_binary)]$seq, all_age_round[is.na(mda_binary)]$n.age) %>% data.table("seq" = .)
  split.1 <- merge(expanded.1, all_age_round[mda_binary == 0], by="seq", all=T)
  split.2 <- merge(expanded.2, all_age_round[mda_binary == 1], by="seq", all=T)
  split.3 <- merge(expanded.3, all_age_round[is.na(mda_binary)], by="seq", all=T)
  split.list <- list(split.1, split.2, split.3)
  split.list <- lapply(split.list, function(x) {
                  x[,age.rep:= 1:.N - 1, by =.(seq)]
                  x[,age_start:= age_start+age.rep*5]
                  x[, age_end :=  age_start + 4 ]
                } )


  ## GET SUPER REGION INFO
  if (region_pattern == T){
    print("getting super regions")
    super_region <- get_location_metadata(location_set_id = 22)
    super_region <- super_region[, .(location_id, super_region_id)]
    split.list <- lapply(c(1:3), function(x) {
      split.list[[x]] <- merge(split.list[[x]], super_region, by = "location_id")
    })
    super_regions <- unique(c(unique(split.list[[1]]$super_region_id), unique(split.list[[2]]$super_region_id), unique(split.list[[3]]$super_region_id)))
  }

  ## GET AGE GROUPS
  all_age_total <- list()
  all_age_total <- lapply(c(1:3), function(x) {
    all_age_total[[x]] <- merge(split.list[[x]], ages, by = c("age_start", "age_end"), all.x = T)
  })

  ## CREATE AGE GROUP ID 1
  all_age_total <- lapply(c(1:3), function(x) {
    all_age_total[[x]][age_start == 0 & age_end == 4, age_group_id := 1]
    all_age_total[[x]] <-  all_age_total[[x]][age_group_id %in% age | age_group_id ==1]
  })

  ##GET LOCS AND POPS
  pop_locs <- unique(c(unique(all_age_total[[1]]$location_id), unique(all_age_total[[2]]$location_id), unique(all_age_total[[3]]$location_id)))
  pop_years <- unique(c(unique(all_age_total[[1]]$year_id), unique(all_age_total[[2]]$year_id), unique(all_age_total[[3]]$year_id)))

  ## GET AGE PATTERN
  if (region_pattern == T) {
    locations <- super_regions
  } else {
    locations <- location_pattern_id
  }
  print("getting age pattern")
  draws <- paste0("draw_", 0:999)
  # do this for pre, post, and unknown MDA
  # changed the year to 2010 because time shouldn't matter for pre- and post- due to 40 year time window,
  # and we want 2010 for unknown because it's in the middle of MDA implementation (started aroun 2005-2006)
  age_pattern.1 <- as.data.table(get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 19679,
                                         measure_id = 5, location_id = locations, source = "epi",
                                         sex_id = c(1,2), gbd_round_id = 5,
                                         age_group_id = age, year_id = 2010)) ##imposing age pattern
  age_pattern.2 <- as.data.table(get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 19818,
                                             measure_id = 5, location_id = locations, source = "epi",
                                             sex_id = c(1,2), gbd_round_id = 5,
                                             age_group_id = age, year_id = 2010)) ##imposing age pattern, version_id should be 252173
  age_pattern.3 <- as.data.table(get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 1491,
                                             measure_id = 5, location_id = locations, source = "epi",
                                             sex_id = c(1,2), gbd_round_id = 5,
                                             version_id = 246179, age_group_id = age, year_id = 2010)) ##imposing age pattern, version_id should be 246179

  us_population <- as.data.table(get_population(location_id = locations, year_id = 2010, sex_id = c(1, 2),
                                                age_group_id = age))
  us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]
  age_pattern <- list(age_pattern.1, age_pattern.2, age_pattern.3)
  age_pattern <- lapply(c(1:3), function(x) {
    age_pattern[[x]][, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
    age_pattern[[x]][, rate_dis := rowMeans(.SD), .SDcols = draws]
    age_pattern[[x]][, (draws) := NULL]
    age_pattern[[x]] <- age_pattern[[x]][ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]
  })

  print("formatting age pattern")
  ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
  age_1 <- copy(age_pattern)

  age_1 <- lapply(c(1:3), function(x) {
    age_1[[x]] <- age_1[[x]][age_group_id %in% c(2, 3, 4, 5), ]
  })

  se <- copy(age_1)
  se <- lapply(c(1:3), function(x) {
    se[[x]] <- se[[x]][age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] ##just use standard error from 1-4 age group
  })

  age_1 <- lapply(c(1:3), function(x) {
    age_1[[x]] <- merge(age_1[[x]], us_population, by = c("age_group_id", "sex_id", "location_id"))
    age_1[[x]][, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
    age_1[[x]][, frac_pop := population / total_pop]
    age_1[[x]][, weight_rate := rate_dis * frac_pop]
    age_1[[x]][, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
    age_1[[x]] <- unique(age_1[[x]], by = c("sex_id", "measure_id", "location_id"))
    age_1[[x]] <- age_1[[x]][, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
    age_1[[x]] <- merge(age_1[[x]], se[[x]], by = c("sex_id", "measure_id", "location_id"))
    age_1[[x]][, age_group_id := 1]
  })

  age_pattern <- lapply(c(1:3), function(x) {
    age_pattern[[x]] <- age_pattern[[x]][!age_group_id %in% c(2,3,4,5)]
    age_pattern[[x]] <- rbind(age_pattern[[x]], age_1[[x]])
  })

  ## CASES AND SAMPLE SIZE
  age_pattern <- lapply(c(1:3), function(x) {
      age_pattern[[x]][measure_id == 5, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
      age_pattern[[x]][measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
      age_pattern[[x]][, cases_us := sample_size_us * rate_dis]
      age_pattern[[x]][is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
      age_pattern[[x]][is.nan(cases_us), cases_us := 0]
  })

  ## GET SEX ID 3
  sex_3 <- copy(age_pattern)
  sex_3 <- lapply(c(1:3), function(x) {
    sex_3[[x]][, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
    sex_3[[x]][, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
    sex_3[[x]][, rate_dis := cases_us/sample_size_us]
    sex_3[[x]][measure_id == 5, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
    sex_3[[x]][measure_id == 6, se_dismod := sqrt(cases_us)/sample_size_us]
    sex_3[[x]][is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0 can't calculate rate and standard error, but should both be 0
    sex_3[[x]][is.nan(se_dismod), se_dismod := 0]
    sex_3[[x]] <- unique(sex_3[[x]], by = c("age_group_id", "measure_id", "location_id"))
    sex_3[[x]][, sex_id := 3]
  })

  age_pattern <- lapply(c(1:3), function(x) {
    age_pattern[[x]] <- rbind(age_pattern[[x]], sex_3[[x]])
    age_pattern[[x]][, super_region_id := location_id]
    age_pattern[[x]] <- age_pattern[[x]][ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
  })

  ## MERGE AGE PATTERN
  age_pattern1 <- copy(age_pattern)

  if (region_pattern == T) {
    all_age_total <- lapply(c(1:3), function(x) {
      all_age_total[[x]] <- merge(all_age_total[[x]], age_pattern1[[x]], by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
    })
  } else {
    all_age_total <- lapply(c(1:3), function(x) {
      all_age_total[[x]] <- merge(all_age_total[[x]], age_pattern1[[x]], by = c("sex_id", "age_group_id", "measure_id"))
    })
  }

  ## GET POPULATION INFO
  print("getting populations for age structure")
  populations <- as.data.table(get_population(location_id = pop_locs, year_id = pop_years,
                                              sex_id = c(1, 2, 3), age_group_id = age))
  age_1 <- lapply(c(1:3), function(x) {
    age_1[[x]] <- copy(populations)
  })
  pop <- list()
  pop <- lapply(c(1:3), function(x) {
    pop[[x]] <- populations[!age_group_id %in% c(2, 3, 4, 5)]
  })

  age_1 <- lapply(c(1:3), function(x) {
    age_1[[x]] <- age_1[[x]][age_group_id %in% c(2, 3, 4, 5)]
    age_1[[x]][, population := sum(population), by = c("location_id", "year_id", "sex_id")]
    age_1[[x]] <- unique(age_1[[x]], by = c("location_id", "year_id", "sex_id"))
    age_1[[x]][, age_group_id := 1]
  })

  pop <- lapply(c(1:3), function(x) {
    pop[[x]] <- rbind(pop[[x]], age_1[[x]])
  })

  all_age_total <- lapply(c(1:3), function(x) {
    all_age_total[[x]] <- merge(all_age_total[[x]], pop[[x]], by = c("location_id", "sex_id", "year_id", "age_group_id"))
  })

  #####CALCULATE AGE SPLIT POINTS#######################################################################
  ## CREATE NEW POINTS
  print("creating new age split points")
  all_age_total <- lapply(c(1:3), function(x) {
    all_age_total[[x]][, total_pop := sum(population), by = "seq"]
    all_age_total[[x]][, sample_size := (population / total_pop) * sample_size]
    all_age_total[[x]][, cases_dis := sample_size * rate_dis]
    all_age_total[[x]][, total_cases_dis := sum(cases_dis), by = "seq"]
    all_age_total[[x]][, total_sample_size := sum(sample_size), by = "seq"]
    all_age_total[[x]][, all_age_rate := total_cases_dis/total_sample_size]
    all_age_total[[x]][, ratio := mean / all_age_rate]
    all_age_total[[x]][, mean := ratio * rate_dis]
  })

  ######################################################################################################

  ## FORMATTING
  blank_vars <- c("lower", "upper", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", "seq", "standard_error", "cases")
  all_age_total <- lapply(c(1:3), function(x) {
    all_age_total[[x]][, group := 1]
    all_age_total[[x]][, specificity := paste0(specificity, ", age-split child")]
    all_age_total[[x]][, group_review := 1]
    all_age_total[[x]][, (blank_vars) := NULL]
    all_age_total[[x]] <- col_order(all_age_total[[x]])
  })

  ## ADD PARENTS
  all_age_parents <- col_order(all_age_parents)
  invisible(all_age_parents[, group_review := 0])
  invisible(all_age_parents[, group := 1])
  invisible(all_age_parents[, specificity := paste0(specificity, ", age-split parent")])
  all_age_total <- lapply(c(1:3), function(x) {
    all_age_total[[x]][, setdiff(names(all_age_total[[x]]), names(all_age_parents)) := NULL] ## make columns the same
  })
  total <- copy(all_age_total)
  all_total <- rbind(total[[1]], total[[2]], total[[3]])
  all_total <- rbind(all_total, all_age_parents)
  all_total <- all_total[, c(original_names), with = F]

  if (region_pattern ==T) {
    all_total <- all_total[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern", date)]
  } else {
    all_total <- all_total[, note_modeler := paste0(note_modeler, "| age split using the age pattern from location id ", location_pattern_id, " ", date)]
  }

  ## BREAK IF NO ROWS
  for(i in c(1:3)) {
    if (nrow(total[[i]]) == 0) {
      print("nothing in bundle to age-sex split")
      break
    }
  }

  write.xlsx(all_total, paste0(j_root, FILEPATH, "_age_split.xlsx"),
             sheetName = "extraction")
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
  df_ratio <- df_split[specificity == "sex", list(nid, group, sex, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end)]

  ## CREATE NEW OBSERVATIONS
  print("creating new observations")
  age.sex <- copy(df_split[specificity == "age"])
  age.sex[,specificity := "age,sex"]
  age.sex[,seq := ""]
  age.sex[,c("ratio", "se_ratio", "split",  "cases_total",  "prop_cases",  "ss_total",	"prop_ss",	"se_cases",	"se_ss") := NULL]
  male <- copy(age.sex[, sex := "Male"])
  female <- copy(age.sex[, sex := "Female"])

  age.sex <- rbind(male, female)
  age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "sex", "measure", "year_start", "year_end"))

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
  write.xlsx(total, paste0(j_root, FILEPATH, "_age_sex_split.xlsx"),
             row.names=F, showNA=F, sheetName="extraction")
}
