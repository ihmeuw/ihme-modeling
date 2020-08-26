##########################################################################
### Author: NAME
### Date: 03/22/2019
### Project: GBD Nonfatal Estimation
### Purpose: CROSSWALKING GBD 2019
### Last edited: USERNAME, September 2019
##########################################################################
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
}
library(pacman, lib.loc = paste0(j_root, "FILEPATH")) 
pacman::p_load(data.table, openxlsx, ggplot2, Hmisc, msm) 

# if the above doesn't work use this: 
library(data.table, lib.loc = paste0(j_root, "FILEPATH")) 
library(openxlsx, lib.loc = paste0(j_root, "FILEPATH")) 
library(ggplot2, lib.loc = paste0(j_root, "FILEPATH")) 
library(Hmisc, lib.loc = paste0(j_root, "FILEPATH")) 
library(msm, lib.loc = paste0(j_root, "FILEPATH"))

# continue as per script
library(ini, lib.loc = paste0(j_root, "FILEPATH"))
library(slackr, lib.loc = paste0(j_root, "FILEPATH")) 
library(mortdb, lib = "FILEPATH") 

date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------
dem_dir <- paste0(j_root, "FILEPATH")
decomp2_dir <- paste0(j_root, "FILEPATH")
decomp4_dir <- paste0(j_root, "FILEPATH")
functions_dir <- paste0(j_root, "FILEPATH")
mort_functions_dir <- paste0(j_root, "FILEPATH")
mrbrt_helper_dir <- paste0(j_root, "FILEPATH")
mrbrt_dir <- paste0("FILEPATH") # edit per cause
cv_drop <- c("cv_marketscan_inp_2000")
draws <- paste0("draw_", 0:999)

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_location_metadata.R"))
source(paste0(functions_dir, "get_age_metadata.R"))
source(paste0(functions_dir, "get_population.R"))
source(paste0(mort_functions_dir, "get_mort_outputs.R"))
source(paste0(mort_functions_dir, "db_init.R"))
source(paste0(mort_functions_dir, "get_proc_version.R"))
source(paste0(mort_functions_dir, "get_locations.R"))
source(paste0(mort_functions_dir, "get_gbd_round.R"))
source(paste0(mort_functions_dir, "get_age_map.R"))

functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
            "load_mr_brt_preds_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}

# GET METADATA ------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 22)
age_dt <- get_age_metadata(12)
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]

# DATA PROCESSING FUNCTIONS -----------------------------------------------


dt_marketscan<-read.csv(file=paste0("FILEPATH", "FILEPATH", ".csv"), sep=",", header=TRUE)
dt<-as.data.table(dt_marketscan)
dim(dt_marketscan)
dt<-as.data.table(dt_marketscan)
is.data.table(dt) 
dim(dt)  
length(unique(dt$nid)) 
table(dt_marketscan$measure)
dt<-subset(dt_marketscan, dt_marketscan$measure=="prevalence" | dt_marketscan$measure=="incidence")
table(dt$measure)
dim(dt) 

aggregate_marketscan <- function(mark_dt){
  dt <- copy(mark_dt)
  marketscan_dt <- copy(dt[cv_marketscan == 1])
  by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
  marketscan_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
  marketscan_dt[, `:=` (location_id = 102, location_name = "United States", mean = cases/sample_size,
                        lower = NA, upper = NA)]
  z <- qnorm(0.975)
  marketscan_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  marketscan_dt <- unique(marketscan_dt, by = by_vars)
  full_dt <- rbind(dt, marketscan_dt)
  return(full_dt)
}


## FILL OUT MEAN/CASES/SAMPLE SIZE
# only works if cases or sample size are missing, if both are missing then cases and sample size can not be calculated
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size), sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}


get_definitions <- function(ref_dt){
  dt <- copy(ref_dt)
  cvs <- names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop]
  cvs <- cvs[!cvs %in% cv_drop]
  dt[, definition := ""]
  for (cv in cvs){
    dt[get(cv) == 1, definition := paste0(definition, "_", cv)]
  }
  dt[definition == "", definition := "reference"]
  return(list(dt, cvs))
}


subnat_to_nat <- function(subnat_dt){
  ## subnat_dt<-dem_dt ## 
  dt <- copy(subnat_dt)
  dt<-subnat_dt
  dt[, ihme_loc_id := NULL]
  dt <- merge(dt, loc_dt[, .(location_id, level, parent_id, location_type, ihme_loc_id)], by = "location_id")
  dt[, loc_match := location_id]
  dt[level >= 4, loc_match := parent_id]
  dt[level == 6, loc_match := 4749] ## PAIR UTLAS TO ENGLAND
  dt[level == 5 & grepl("GBR", ihme_loc_id), loc_match := 4749] ## PAIR ENGLAND REGIONS TO ENGLAND
  dt[level == 5 & location_type %in% c("urbanicity", "admin2"), loc_match := 163] ## PAIR INDIA URBAN/RURAL TO INDIA
  dt[level == 5 & grepl("CHN", ihme_loc_id), loc_match := 6]
  dt[level == 5 & grepl("KEN", ihme_loc_id), loc_match := 180]
  return(dt)
}

calc_year <- function(year_dt){
  dt <- copy(year_dt)
  dt[, year_match := (year_start+year_end)/2]
  return(dt)
}

## GET UNIQUE "AGE SERIES" FOR THE PURPOSE OF AGGREGATING
get_age_combos <- function(agematch_dt){
  dt <- copy(agematch_dt)
  by_vars <- c("nid", "location_id","year_match", "sex", "measure", names(dem_dt)[grepl("^cv_", names(dem_dt)) & !names(dem_dt) %in% cv_drop])
  dt[, age_n := .GRP, by = by_vars]
  small_dt <- copy(dt[, c(by_vars, "age_start", "age_end","age_n"), with = F])
  return(list(dt, small_dt))
}

## HELPER FUNCTION TO ROUND TO NEAREST AGE START/AGE END IN GBD-LAND
get_closest_age <- function(i, var, start = T, dt){
  age <- dt[i, get(var)]
  if (start == T){
    age_dt[, age_group_years_start][which.min(abs(age_dt[, age_group_years_start] - age))]
  } else if (start == F){
    age_dt[, age_group_years_end][which.min(abs(age_dt[, age_group_years_end] - age))]
  }
}

## ACTUALLY AGGREGATE!
aggregation <- function(start_match, end_match, dt = pull_dt){
  agg <- nrow(dt[start_match == age_start & end_match == age_end]) == 0 ## FLAGS TO AGGREGATE EACH SIDE OF THE RATIO
  agg1 <- nrow(dt[start_match == age_start2 & end_match == age_end2]) == 0
  z <- qnorm(0.975)
  if (agg == T){ ## AGGREGATE FIRST SIDE
    row_dt <- unique(dt[age_start >= start_match & age_end <= end_match], by = c("age_start", "age_end"))
    row_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size))]
    row_dt[, `:=` (mean = cases/sample_size, age_start = min(age_start), age_end = max(age_end))]
    row_dt[measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    row_dt[measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    row_dt[measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
    row_dt <- dplyr::select(row_dt, names(row_dt)[!grepl("2$", names(row_dt))])
    row_dt <- unique(row_dt, by = "nid")
  } else { ## OR LEAVE THE SAME
    row_dt <- unique(dt[age_start == start_match & age_end == end_match], by = c("age_start", "age_end"))
    row_dt <- dplyr::select(row_dt, names(row_dt)[!grepl("2$", names(row_dt))])
  }
  if (agg1 == T){ ## AGGREGATE SECOND SIDE
    row_dt2 <- unique(dt[age_start2 >= start_match & age_end2 <= end_match], by = c("age_start2", "age_end2"))
    row_dt2[, `:=` (cases2 = sum(cases2), sample_size2 = sum(sample_size2))]
    row_dt2[, `:=` (mean2 = cases2/sample_size2, age_start2 = min(age_start2), age_end2 = max(age_end2))]
    row_dt2[measure == "prevalence", standard_error2 := sqrt(mean2*(1-mean2)/sample_size2 + z^2/(4*sample_size2^2))]
    row_dt2[measure == "incidence" & cases2 < 5, standard_error2 := ((5-mean2*sample_size2)/sample_size2+mean2*sample_size2*sqrt(5/sample_size2^2))/5]
    row_dt2[measure == "incidence" & cases2 >= 5, standard_error2 := sqrt(mean2/sample_size2)]
    row_dt2 <- dplyr::select(row_dt2, names(row_dt2)[grepl("2$", names(row_dt2))])
    row_dt2 <- unique(row_dt2, by = "nid2")
  } else { ## OR LEAVE THE SAME
    row_dt2 <- unique(dt[age_start2 == start_match & age_end2 == end_match], by = c("age_start2", "age_end2"))
    row_dt2 <- dplyr::select(row_dt2, names(row_dt2)[grepl("2$", names(row_dt2))])
  }
  new_row <- cbind(row_dt, row_dt2) ## BUT THE SIDES BACK TOGETHER
  return(new_row)
}

## SET UP AGGREGATION
aggregate_tomatch <- function(match_dt, id){
  print(id)
  pull_dt <- copy(match_dt[agg_id == id])
  start_matches <- pull_dt[age_start_match == T, age_start] ## GET ALL START MATCHES 
  allend_matches <- pull_dt[age_end_match == T, unique(age_end)] ## GET ALL END MATCHES
  end_matches <- c() ## MAKE SURE ACTUAL END MATCHES MAP 1:1 WITH START MATCHES
  for (x in 1:length(start_matches)){
    match <- allend_matches[allend_matches > start_matches[x]]
    match <- match[which.min(match-start_matches[x])]
    end_matches <- c(end_matches, match)
  }
  aggregated <- rbindlist(lapply(1:length(start_matches), function(x) aggregation(start_match = start_matches[x],  end_match = end_matches[x], dt = pull_dt)))
  return(aggregated)
}

## FULL FUNCTION TO GET MATCHES
get_matches_marketscan <- function(n, pair_dt, year_span = 5, age_span = 5){ 
  dt <- copy(pair_dt)
  pair <- pairs[,n]
  print(pair)
  keep_vars <- c("nid", "location_id", "sex", "year_start", "year_end", "measure", "age_start", "age_end",
                 "mean", "standard_error", "cases", "sample_size", "loc_match", "year_match", "age_n")
  dt1 <- copy(dt[definition == pair[1]])
  dt1 <- dplyr::select(dt1, keep_vars)
  dt2 <- copy(dt[definition == pair[2]])
  dt2 <- dplyr::select(dt2, keep_vars)
  setnames(dt2, keep_vars[!keep_vars  == "loc_match"], paste0(keep_vars[!keep_vars  == "loc_match"], "2"))
  matched <- merge(dt1, dt2, by = "loc_match", allow.cartesian = T) ## INITIAL CARTISIAN MERGE ONLY MATCHING ON LOCATION
  matched <- matched[sex == sex2 & measure == measure2 &
                       between(year_match, year_match2 - year_span/2, year_match2 + year_span/2)] ## FILETER OUT SEX, MEASURE, YEAR
  ###
  matched[, age_match := (between(age_start, age_start2 - age_span/2, age_start2 + age_span/2) & between(age_end, age_end2 - age_span/2, age_end2 + age_span/2))]
  unmatched <- copy(matched[age_match == F])
  ## AGE ROUNDING
  unmatched$age_start <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_start", dt = unmatched))
  unmatched$age_end <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_end", start = F, dt = unmatched))
  unmatched$age_start2 <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_start2", dt = unmatched))
  unmatched$age_end2 <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_end2", start = F, dt = unmatched))
  
  ## FIND WHERE IT IS POSSIBLE TO AGGREGATE
  unmatched[, age_start_match := age_start == age_start2]
  unmatched[, age_end_match := age_end == age_end2]
  unmatched[, sum_start := sum(age_start_match), by = c("age_n", "age_n2")]
  unmatched[, sum_end := sum(age_end_match), by = c("age_n", "age_n2")]
  agg_matches <- copy(unmatched[sum_start > 0 & sum_end > 0 & !is.na(cases) & !is.na(sample_size) & !is.na(cases2) & !is.na(sample_size2)])
  agg_matches[, agg_id := .GRP, by = c("age_n", "age_n2")]
  
  ## AGGREGATE IF THERE ARE PLACES TO AGGREGATE OTHERWISE DO NOTHING
  if (nrow(agg_matches) > 0){
    aggregated <- rbindlist(lapply(1:agg_matches[, max(agg_id)], function(x) aggregate_tomatch(match_dt = agg_matches, id = x)))
    aggregated[, c("age_start_match", "age_end_match", "sum_start", "sum_end", "agg_id") := NULL]
    final_match <- rbind(matched[age_match == T], aggregated)
  } else {
    final_match <- copy(matched[age_match == T])
  }
  
  final_match[, c("age_match") := NULL]
  final_match[, `:=` (def = pair[1], def2 = pair[2])] ## LABEL WITH DEFINITIONS
  return(final_match)
}

get_matches_recall_lifetime <- function(n, pair_dt, year_span = 5, age_span = 5){ 
  dt <- copy(pair_dt)
  pair <- pairs[,n]
  print(pair)
  keep_vars <- c("nid", "location_id", "sex", "year_start", "year_end", "measure", "age_start", "age_end",
                 "mean", "standard_error", "cases", "sample_size", "loc_match", "year_match", "age_n")
  dt1 <- copy(dt[definition == pair[1]])
  dt1 <- dplyr::select(dt1, keep_vars)
  dt2 <- copy(dt[definition == pair[2]])
  dt2 <- dplyr::select(dt2, keep_vars)
  setnames(dt2, keep_vars[!keep_vars  == "loc_match"], paste0(keep_vars[!keep_vars  == "loc_match"], "2"))
  matched <- merge(dt1, dt2, by = "loc_match", allow.cartesian = T) ## INITIAL CARTISIAN MERGE ONLY MATCHING ON LOCATION
  matched <- matched[sex == sex2 & measure == measure2 &
                       between(year_match, year_match2 - year_span/2, year_match2 + year_span/2)] ## FILETER OUT SEX, MEASURE, YEAR
  ###  if (pair[1] == "_cv_marketscan" | pair[2] == "_cv_marketscan"){ ## EXACT LOCS FOR MARKETSCAN BUT CURRENTLY ALL ARE EXACT LOCS
  ###  matched <- matched[location_id == location_id2]
  ###  }
  matched[, age_match := (between(age_start, age_start2 - age_span/2, age_start2 + age_span/2) & between(age_end, age_end2 - age_span/2, age_end2 + age_span/2))]
  unmatched <- copy(matched[age_match == F])
  ## AGE ROUNDING
  unmatched$age_start <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_start", dt = unmatched))
  unmatched$age_end <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_end", start = F, dt = unmatched))
  unmatched$age_start2 <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_start2", dt = unmatched))
  unmatched$age_end2 <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_end2", start = F, dt = unmatched))
  
  ## FIND WHERE IT IS POSSIBLE TO AGGREGATE
  unmatched[, age_start_match := age_start == age_start2]
  unmatched[, age_end_match := age_end == age_end2]
  unmatched[, sum_start := sum(age_start_match), by = c("age_n", "age_n2")]
  unmatched[, sum_end := sum(age_end_match), by = c("age_n", "age_n2")]
  agg_matches <- copy(unmatched[sum_start > 0 & sum_end > 0 & !is.na(cases) & !is.na(sample_size) & !is.na(cases2) & !is.na(sample_size2)])
  agg_matches[, agg_id := .GRP, by = c("age_n", "age_n2")]
  
  ## AGGREGATE IF THERE ARE PLACES TO AGGREGATE OTHERWISE DO NOTHING
  if (nrow(agg_matches) > 0){
    aggregated <- rbindlist(lapply(c(1:4, 6:7, 9:14, 18, 20, 21, 24, 26:31, 33:34, 36:46, 49, 52, 53:max(agg_matches$agg_id)), function(x) aggregate_tomatch(match_dt = agg_matches, id = x)))
    aggregated[, c("age_start_match", "age_end_match", "sum_start", "sum_end", "agg_id") := NULL]
    final_match <- rbind(matched[age_match == T], aggregated)
  } else {
    final_match <- copy(matched[age_match == T])
  }
  final_match[, c("age_match") := NULL]
  final_match[, `:=` (def = pair[1], def2 = pair[2])] ## LABEL WITH DEFINITIONS
  return(final_match)
}

get_matches_marketscan_2000 <- function(n, pair_dt, year_span = 20, age_span = 5){ 
  dt <- copy(pair_dt)
  pair <- pairs[,n]
  print(pair)
  keep_vars <- c("nid", "location_id", "sex", "year_start", "year_end", "measure", "age_start", "age_end",
                 "mean", "standard_error", "cases", "sample_size", "loc_match", "year_match", "age_n")
  dt1 <- copy(dt[definition == pair[1]])
  dt1 <- dplyr::select(dt1, keep_vars)
  dt2 <- copy(dt[definition == pair[2]])
  dt2 <- dplyr::select(dt2, keep_vars)
  setnames(dt2, keep_vars[!keep_vars  == "loc_match"], paste0(keep_vars[!keep_vars  == "loc_match"], "2"))
  matched <- merge(dt1, dt2, by = "loc_match", allow.cartesian = T) ## INITIAL CARTISIAN MERGE ONLY MATCHING ON LOCATION
  matched <- matched[sex == sex2 & measure == measure2 &
                       between(year_match, year_match2 - year_span/2, year_match2 + year_span/2)] ## FILETER OUT SEX, MEASURE, YEAR
  ###  if (pair[1] == "_cv_marketscan" | pair[2] == "_cv_marketscan"){ ## EXACT LOCS FOR MARKETSCAN BUT CURRENTLY ALL ARE EXACT LOCS
  ###  matched <- matched[location_id == location_id2]
  ###  }
  matched[, age_match := (between(age_start, age_start2 - age_span/2, age_start2 + age_span/2) & between(age_end, age_end2 - age_span/2, age_end2 + age_span/2))]
  unmatched <- copy(matched[age_match == F])
  ## AGE ROUNDING
  unmatched$age_start <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_start", dt = unmatched))
  unmatched$age_end <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_end", start = F, dt = unmatched))
  unmatched$age_start2 <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_start2", dt = unmatched))
  unmatched$age_end2 <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_end2", start = F, dt = unmatched))
  
  ## FIND WHERE IT IS POSSIBLE TO AGGREGATE
  unmatched[, age_start_match := age_start == age_start2]
  unmatched[, age_end_match := age_end == age_end2]
  unmatched[, sum_start := sum(age_start_match), by = c("age_n", "age_n2")]
  unmatched[, sum_end := sum(age_end_match), by = c("age_n", "age_n2")]
  agg_matches <- copy(unmatched[sum_start > 0 & sum_end > 0 & !is.na(cases) & !is.na(sample_size) & !is.na(cases2) & !is.na(sample_size2)])
  agg_matches[, agg_id := .GRP, by = c("age_n", "age_n2")]
  
  ## AGGREGATE IF THERE ARE PLACES TO AGGREGATE OTHERWISE DO NOTHING
  if (nrow(agg_matches) > 0){
    aggregated <- rbindlist(lapply(1:agg_matches[, max(agg_id)], function(x) aggregate_tomatch(match_dt = agg_matches, id = x)))
    aggregated[, c("age_start_match", "age_end_match", "sum_start", "sum_end", "agg_id") := NULL]
    final_match <- rbind(matched[age_match == T], aggregated)
  } else {
    final_match <- copy(matched[age_match == T])
  }
  
  final_match[, c("age_match") := NULL]
  final_match[, `:=` (def = pair[1], def2 = pair[2])] ## LABEL WITH DEFINITIONS
  return(final_match)
}
create_ratios <- function(ratio_dt){
  dt <- copy(ratio_dt)
  dt[, ratio := mean2/mean]
  dt[, ratio_se := sqrt((mean2^2 / mean^2) * (standard_error2^2/mean2^2 + standard_error^2/mean^2))]
  dt[, log_ratio := log(ratio)]
  dt$log_rse <- sapply(1:nrow(dt), function(i) {
    mean_i <- dt[i, "ratio"]
    se_i <- dt[i, "ratio_se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  return(dt)
}

create_mrbrtdt <- function(match_dt, vars = cvs){
  dt <- copy(match_dt)
  vars <- gsub("^cv_", "", vars)
  for (var in vars){
    dt[, (var) := as.numeric(grepl(var, def2)) - as.numeric(grepl(var, def))]
    if (nrow(dt[!get(var) == 0]) == 0){
      dt[, c(var) := NULL]
      vars <- vars[!vars == var]
    }
  }
  loc_map <- copy(loc_dt[, .(location_ascii_name, location_id)])
  dt <- merge(dt, loc_map, by = "location_id")
  setnames(loc_map, c("location_id", "location_ascii_name"), c("location_id2", "location_ascii_name2"))
  dt <- merge(dt, loc_map, by = "location_id2")
  dt[, id := paste0(nid2, " (", location_ascii_name2, ": ", sex2, " ", age_start2, "-", age_end2, ") - ", nid, " (", location_ascii_name, ": ", sex, " ", age_start, "-", age_end, ")")]
  dt <- dt[, c("id", "log_ratio", "log_rse", "def", "def2", vars), with = F]
  return(list(dt, vars))
}

find_sex_match <- function(dt){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence", "incidence")]
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end",
                  names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
  sex_dt[, match_n := .N, by = match_vars]
  sex_dt <- sex_dt[match_n == 2]
  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt[, id := .GRP, by = match_vars]
  sex_dt <- dplyr::select(sex_dt, keep_vars)
  sex_dt <- dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"))
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0]
  sex_dt[, id := .GRP, by = c("nid", "location_id")]
  sex_dt[, midage := (age_end + age_start)/2] ##
  return(sex_dt)
}

calc_sex_ratios <- function(dt){
  ratio_dt <- copy(dt)
  ratio_dt[, `:=` (ratio = mean_Female/mean_Male,
                   ratio_se = sqrt((mean_Female^2 / mean_Male^2) * (standard_error_Female^2/mean_Female^2 + standard_error_Male^2/mean_Male^2)))]
  ratio_dt[, log_ratio := log(ratio)]
  ratio_dt$log_se <- sapply(1:nrow(ratio_dt), function(i) {
    mean_i <- ratio_dt[i, "ratio"]
    se_i <- ratio_dt[i, "ratio_se"]
    deltamethod(~log(x1), mean_i, se_i^2) ##
  })
  return(ratio_dt)
}



# RUN SEX SPLIT -----------------------------------------------------------

decomp2_dir <- paste0(j_root, "FILEPATH")
dt_marketscan<-read.csv(file=paste0("\"FILEPATH", "FILEPATH"), sep=",", header=TRUE)
dt<-dt_marketscan
dt<-data.table(dt)
dem_sex_dt<-dt
dem_sex_dt <- get_cases_sample_size(dem_sex_dt)
dem_sex_dt <- get_se(dem_sex_dt)
dem_sex_matches <- find_sex_match(dem_sex_dt)
mrbrt_sex_dt <- calc_sex_ratios(dem_sex_matches)

mrbrt_sex_dt<-na.omit(mrbrt_sex_dt)

typeof(mrbrt_sex_dt$log_ratio)
typeof(mrbrt_sex_dt$log_se)
mrbrt_sex_dt$log_se<-unlist(mrbrt_sex_dt$log_se)
typeof(mrbrt_sex_dt$log_se)


model_name <- paste0("Epilepsy_sex_split", date)

sex_model_IE <- run_mr_brt(
  output_dir = mrbrt_dir,
  covs = list(cov_info("midage", "X", degree = 3, 
                       i_knots = paste(mrbrt_sex_dt[, quantile(midage, prob = seq(0, 1, by = 0.2))][2:5], collapse = ", "),
                       r_linear = T, l_linear = T)),
  model_label = model_name,
  data = mrbrt_sex_dt,
  mean_var = "log_ratio",
  se_var = "log_se",
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.10,      
  overwrite_previous=TRUE
)

# issue with running
sex_model_IE[[4]]$beta_var<-sex_model[[4]]$beta_var
sex_model_IE[[5]]<-sex_model[[5]]

sex_model_IE

plot_mr_brt(sex_model)
sex_predictions <- predict_mr_brt(sex_model, newdata = data.table(midage = seq(42.5, 97.5, by = 5)), write_draws = T)
plot_mr_brt(sex_model_IE)
sex_predictions <- predict_mr_brt(sex_model_IE, newdata = data.table(midage = seq(42.5, 97.5, by = 5)), write_draws = T)

sex_predictions <- predict_mr_brt(sex_model, newdata = data.table(midage = seq(0, 99, by = 5)), write_draws = T)

get_row <- function(n, dt){
  row <- copy(dt)[n]
  pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                          age_group_years_start >= row[, age_start] & age_group_years_end <= row[, age_end]])
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex")]
  row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
              both_N = agg[sex == "Both", pop_sum])]
  return(row)
}

# Note that to run this script, run the following:
dt<-dem_sex_dt
model<-sex_model

dt<-as.data.frame(dt) 
dt$midage <- (dt$age_start + dt$age_end)/2 
dt$midage <- round(dt$midage/5)*5
tosplit_dt <- copy(dt)
tosplit_dt <- as.data.table(copy(dt)) 
nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female")] 
tosplit_dt <- tosplit_dt[sex == "Both"] 
tosplit_dt[, midyear := floor((year_start + year_end)/2)] 

preds <- predict_mr_brt(sex_model, newdata = data.table(midage = seq(0, 99, by = 5), X_intercept = 1, Z_intercept = 1), write_draws = T)
pred_draws <- as.data.table(preds$model_draws)
pred_draws <- as.data.table(read.csv("FILEPATH", "model_draws.csv"))
pred_draws[, c("X_intercept", "Z_intercept") := NULL]
pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                         year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)])
pops[age_group_years_end == 125, age_group_years_end := 99]
tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt), mc.cores = 9))
tosplit_dt <- tosplit_dt[!is.na(both_N)] ## GET RID OF DATA THAT COULDN'T FIND POPS -
tosplit_dt[, merge := 1] 
pred_draws[, merge := 1]
split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
male_dt <- copy(split_dt)
male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = NA, lower = NA, 
                cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                      ratio_se, ")"))]
male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
male_dt <- dplyr::select(male_dt, names(dt))
female_dt <- copy(split_dt)
female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA, 
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                        ratio_se, ")"))]
female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
female_dt <- dplyr::select(female_dt, names(dt))
total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))
#return(list(final = total_dt, graph = split_dt))
#}
predict_sex <- list(final = total_dt, graph = split_dt)
#predict_sex <- split_data(dem_sex_dt, sex_model)

dim(total)[1]==dim(dt)[1]-395+dim(female_dt)[1]+dim(male_dt)[1]   


dim(dt)
table(dt$sex)
length(unique(dt$nid))
dim(predict_sex$final)
table(predict_sex$final$sex) 
length(unique(predict_sex$final$nid)) 


##########################
#  produces missing cases when sex-splitting even when full cases are in the parent row
predict_sex_final_cases_fromse<-calculate_cases_fromse(predict_sex$final)
sum(is.na(predict_sex$final$cases))
sum(is.na(predict_sex_final_cases_fromse$cases)) 

graph_predictions <- function(dt){
  ## dt<-predict_sex$graph
  graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
  graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
  graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- cbind(graph_dt_means, "error"=graph_dt_error$error) 
  graph_dt[, N := (mean*(1-mean)/error^2)]
  wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  graph_dt[, midage := (age_end + age_start)/2]
  ages <- c(60, 70, 80, 90)
  graph_dt[, age_group := cut2(midage, ages)]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) + 
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper)) +
    #facet_wrap(~age_group) +
    labs(x = "Both Sex Mean", y = " Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex Split Means Compared to Both Sex Mean by Age") +
    scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
    theme_classic()
  return(gg_sex)
}

pdf(paste0(dem_dir, "crosswalks_sex_split_onegraph.pdf"))
graph_predictions(predict_sex$graph)
dev.off()


pdf(paste0(mrbrt_dir, model_name, "/sex_split_onegraph.pdf"), width=12)
graph_predictions(predict_sex$graph)
dev.off()


# RUN DATA SETUP ----------------------------------------------------------
# Recall lifetime

dt_marketscan<-read.csv(file=paste0("FILEPATH", "cvadded_market_nationalUSA2019_06_17.csv"), sep=",", header=TRUE)
table(dt_marketscan$measure)
dt<-subset(dt_marketscan, dt_marketscan$measure=="prevalence" | dt_marketscan$measure=="incidence")
table(dt$measure)
dim(dt)

dt<-dem_dt
dim(dt) 
dt<-as.data.table(dt)
is.data.table(dt)
cv_drop <- c(names(dt)[grep("cv", names(dt))])[c(1:4, 6:15)] # keep only cv_recall_lifetime (so excluded from this list)



##### FIX FROM HERE #####
dem_dt<-dt
## dem_dt <- aggregate_marketscan(dem_dt) ## skip this, already done in aggregate_marketscan_IE
table(dem_dt$cv_marketscan)
dem_dt <- get_cases_sample_size(dem_dt)
dem_dt <- get_se(dem_dt) 
defs <- get_definitions(dem_dt)
dem_dt <- defs[[1]]
cvs <- defs[[2]]
cvs # cv_recall_lifetime
dem_dt <- subnat_to_nat(dem_dt) # need this for the loc_ids to run matches
dem_dt <- calc_year(dem_dt)
age_dts <- get_age_combos(dem_dt)
dem_dt <- age_dts[[1]]
age_match_dt <- age_dts[[2]] ## DON'T ACTUALLY NEED THIS BUT FOUND IT HELPFUL FOR VETTING
pairs <- combn(dem_dt[, unique(definition)], 2)
matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches_recall_lifetime(n = x, pair_dt = dem_dt)))
matches <- create_ratios(matches)
matches
matches<-na.omit(matches) # 
mrbrt_setup <- create_mrbrtdt(matches)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

# 26th April 2019 identified that an error was returning this: unimplemented type 'list' in 'EncodeElement'
# So used https://markhneedham.com/blog/2015/06/30/r-write-csv-unimplemented-type-list-in-encodeelement/ to identify that
# typeof(mrbrt_dt$log_ratio) = 'double' typeof(mrtbr_dt$log_rse) = list so needed to unlist mrbrt_dt$log_rse then it automatically reset to 'double'
mrbrt_dt$log_rse<-unlist(mrbrt_dt$log_rse)


# RUN MR-BRT MODEL --------------------------------------------------------

model_name <- "idiopathic_epilepsy_recall_lifetime"

cov_list <- lapply(mrbrt_vars, function(x) cov_info(x, "X"))

dem_model_recall_lifetime <- run_mr_brt(
  output_dir = mrbrt_dir,
  model_label = model_name,
  remove_x_intercept = T,
  data = mrbrt_dt,
  mean_var = "log_ratio",
  se_var = "log_rse",
  covs = cov_list,
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.10, overwrite_previous=TRUE # trim_pct = 0.1 originally but 
)

get_preds <- function(model, covs){
  preds <- unique(mrbrt_dt[, c(covs), with = F])
  predicts <- predict_mr_brt(model, newdata = preds, write_draws = T)
  return(predicts)
}

dem_predicts <- get_preds(dem_model_recall_lifetime, mrbrt_vars)


plot_mr_brt(dem_model_recall_lifetime) ## 


summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

graph_combos <- function(model, predictions){
  data <- as.data.table(model$train_data)
  data[, ratio_name := paste0(def2, " / ", def)]
  if (length(names(data)[grep("^X", names(data))]) > 0){
    name_change <- names(data)[grepl("^X", names(data))]
    setnames(data, name_change, gsub("^X", "", name_change))
  }
  data[, `:=` (log_ratio_l = log_ratio - 1.96*log_rse, log_ratio_u = log_ratio + 1.96*log_rse)]
  preds <- as.data.table(predictions$model_draws)
  preds <- summaries(preds, draws)
  xcov_names <- names(preds)[grepl("^X", names(preds))]
  setnames(preds, xcov_names, gsub("^X_", "", xcov_names))
  preds <- merge(preds, unique(data[, c("ratio_name", mrbrt_vars), with = F]), by = mrbrt_vars)
  fit_graph <- function(n){
    ratio <- preds[, ratio_name][n]
    ratio_mean <- preds[ratio_name == ratio, mean]
    ratio_lower <- preds[ratio_name == ratio, lower]
    ratio_upper <- preds[ratio_name == ratio, upper]
    graph_dt <- copy(data[ratio_name == ratio])
    graph_dt[, idnum := 1:.N]
    gg <- ggplot() +
      geom_rect(data = graph_dt[1,], xmin = ratio_lower, xmax = ratio_upper, ymin = -Inf, ymax = Inf, alpha = 0.2, fill = "darkorchid") +
      geom_point(data = graph_dt, aes(x = log_ratio, y = as.factor(idnum), color = as.factor(w))) +
      geom_errorbarh(data = graph_dt, aes(y = as.factor(idnum), x = log_ratio, xmin = log_ratio_l, xmax = log_ratio_u, color = as.factor(w))) +
      geom_vline(xintercept = ratio_mean, linetype = "dashed", color = "darkorchid") +
      geom_vline(xintercept = 0) +
      labs(x = "Log Effect Size", y = "") +
      xlim(-3, 5.5) +
      scale_color_manual(name = "", values = c("1" = "midnightblue", "0" = "red"), labels = c("0" = "Trimmed", "1" = "Included")) +
      scale_y_discrete(labels = graph_dt[, id]) +
      ggtitle(paste0("Model fit for ratio ", ratio)) +
      theme_classic() +
      theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(),
            axis.text.y = element_text(size = 5))
    return(gg)
  }
  fit_graphs <- lapply(1:length(preds[, unique(ratio_name)]), fit_graph)
  return(fit_graphs)
}

graphs_modelfit <- graph_combos(model = dem_model_recall_lifetime, predictions = dem_predicts)

pdf(paste0(mrbrt_dir, model_name, "FILEPATH"), width = 12)
graphs_modelfit
dev.off()


# RUN DATA SETUP ----------------------------------------------------------
# Marketscan year 2000

dt_marketscan<-read.csv(file=paste0("FILEPATH", "FILEPATH"), sep=",", header=TRUE)
table(dt_marketscan$measure)
dt<-subset(dt_marketscan, dt_marketscan$measure=="prevalence" | dt_marketscan$measure=="incidence")
table(dt$measure)
dt<-dem_dt
dim(dt)
dt<-as.data.table(dt)
is.data.table(dt)
cv_drop <- c(names(dt)[grep("cv", names(dt))])[c(1:7, 9:15)] # keep only cv_marketscan_all_2000 (so excluded from this list)

dem_dt<-dt
## dem_dt <- aggregate_marketscan(dem_dt) ## skip this, already done in aggregate_marketscan_IE
table(dem_dt$cv_marketscan)
dem_dt <- get_cases_sample_size(dem_dt)
print(dem_dt)
dem_dt <- get_se(dem_dt)
defs <- get_definitions(dem_dt)
dem_dt <- defs[[1]]
cvs <- defs[[2]]
cvs # cv_marketscan_all_2000
dem_dt <- subnat_to_nat(dem_dt) # need this for the loc_ids to run matches
# run manually lines 173-193, returns dt
dem_dt <- calc_year(dem_dt)
age_dts <- get_age_combos(dem_dt)
dem_dt <- age_dts[[1]]
age_match_dt <- age_dts[[2]] ## DON'T ACTUALLY NEED THIS BUT FOUND IT HELPFUL FOR VETTING
pairs <- combn(dem_dt[, unique(definition)], 2)
matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches_marketscan_2000(n = x, pair_dt = dem_dt))) 
matches <- create_ratios(matches)
matches
matches<-na.omit(matches)
mrbrt_setup <- create_mrbrtdt(matches)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

# 26th April 2019 identified that an error was returning this: unimplemented type 'list' in 'EncodeElement'
# So used https://markhneedham.com/blog/2015/06/30/r-write-csv-unimplemented-type-list-in-encodeelement/ to identify that
# typeof(mrbrt_dt$log_ratio) = 'double' typeof(mrtbr_dt$log_rse) = list so needed to unlist mrbrt_dt$log_rse then it automatically reset to 'double'
mrbrt_dt$log_rse<-unlist(mrbrt_dt$log_rse)


# RUN MR-BRT MODEL --------------------------------------------------------

model_name <- "idiopathic_epilepsy_marketscan_2000"

cov_list <- lapply(mrbrt_vars, function(x) cov_info(x, "X"))

dem_model_marketscan_2000 <- run_mr_brt(
  output_dir = mrbrt_dir,
  model_label = model_name,
  remove_x_intercept = T,
  data = mrbrt_dt,
  mean_var = "log_ratio",
  se_var = "log_rse",
  covs = cov_list,
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.10, overwrite_previous=TRUE
)

get_preds <- function(model, covs){
  preds <- unique(mrbrt_dt[, c(covs), with = F])
  predicts <- predict_mr_brt(model, newdata = preds, write_draws = T)
  return(predicts)
}

dem_predicts <- get_preds(dem_model_marketscan_2000, mrbrt_vars)


plot_mr_brt(dem_model_marketscan_2000)


summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

graph_combos <- function(model, predictions){
  data <- as.data.table(model$train_data)
  data[, ratio_name := paste0(def2, " / ", def)]
  if (length(names(data)[grep("^X", names(data))]) > 0){
    name_change <- names(data)[grepl("^X", names(data))]
    setnames(data, name_change, gsub("^X", "", name_change))
  }
  data[, `:=` (log_ratio_l = log_ratio - 1.96*log_rse, log_ratio_u = log_ratio + 1.96*log_rse)]
  preds <- as.data.table(predictions$model_draws)
  preds <- summaries(preds, draws)
  xcov_names <- names(preds)[grepl("^X", names(preds))]
  setnames(preds, xcov_names, gsub("^X_", "", xcov_names))
  preds <- merge(preds, unique(data[, c("ratio_name", mrbrt_vars), with = F]), by = mrbrt_vars)
  fit_graph <- function(n){
    ratio <- preds[, ratio_name][n]
    ratio_mean <- preds[ratio_name == ratio, mean]
    ratio_lower <- preds[ratio_name == ratio, lower]
    ratio_upper <- preds[ratio_name == ratio, upper]
    graph_dt <- copy(data[ratio_name == ratio])
    graph_dt[, idnum := 1:.N]
    gg <- ggplot() +
      geom_rect(data = graph_dt[1,], xmin = ratio_lower, xmax = ratio_upper, ymin = -Inf, ymax = Inf, alpha = 0.2, fill = "darkorchid") +
      geom_point(data = graph_dt, aes(x = log_ratio, y = as.factor(idnum), color = as.factor(w))) +
      geom_errorbarh(data = graph_dt, aes(y = as.factor(idnum), x = log_ratio, xmin = log_ratio_l, xmax = log_ratio_u, color = as.factor(w))) +
      geom_vline(xintercept = ratio_mean, linetype = "dashed", color = "darkorchid") +
      geom_vline(xintercept = 0) +
      labs(x = "Log Effect Size", y = "") +
      xlim(-3, 5.5) +
      scale_color_manual(name = "", values = c("1" = "midnightblue", "0" = "red"), labels = c("0" = "Trimmed", "1" = "Included")) +
      scale_y_discrete(labels = graph_dt[, id]) +
      ggtitle(paste0("Model fit for ratio ", ratio)) +
      theme_classic() +
      theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(),
            axis.text.y = element_text(size = 5))
    return(gg)
  }
  fit_graphs <- lapply(1:length(preds[, unique(ratio_name)]), fit_graph)
  return(fit_graphs)
}

graphs_modelfit <- graph_combos(model = dem_model_marketscan_2000, predictions = dem_predicts)

pdf(paste0(mrbrt_dir, model_name, "/years_20_age_5_fit_graphs.pdf"), width = 12)
graphs_modelfit
dev.off()

# NIHS data was checked for marketscan crosswalk (manually extracted and added in separately - i.e. not part of clinical bundle pulled from EpiUploader)
# this was assessed by SUPERVISOR NAME but did not produce a more feasible output so reverted back to using the clinical marketscan data for crosswalking  

# PREDICTION --------------------------------------------------------------

get_preds_adjustment_sex <- function(raw_data, model){
  dt <- copy(raw_data)
  cv_cols <- names(dt)[grepl("^cv", names(dt))]
  cv_cols <- c(cv_cols, names(dt)[grepl("midage", names(dt))]) #
  cv_cols <- cv_cols[!cv_cols %in% cv_drop]
  dt <- unique(dplyr::select(dt, cv_cols))
  setnames(dt, names(dt), gsub("^cv_", "", names(dt)))
  dt[, sum := rowSums(.SD), .SDcols = names(dt)]
  dt <- dt[!sum == 0]
  dt[, sum := NULL]
  preds <- predict_mr_brt(model, newdata = dt, write_draws = T)
  pred_dt <- as.data.table(preds$model_draws)
  pred_dt[, logadj := rowMeans(.SD), .SDcols = draws]
  pred_dt[, logadj_se := apply(.SD, 1, sd), .SDcols = draws]
  pred_dt[, c(draws, "Z_intercept") := NULL]
  setnames(pred_dt, names(pred_dt), gsub("^X_", "", names(pred_dt))) 
  return(pred_dt)
}

get_preds_adjustment <- function(raw_data, model){
  # raw_data<-adjust_dt #
  # model<-sex_model # 
  dt <- copy(raw_data)
  cv_cols <- names(dt)[grepl("^cv", names(dt))]
  cv_cols <- cv_cols[!cv_cols %in% cv_drop]
  dt <- unique(dplyr::select(dt, cv_cols))
  setnames(dt, names(dt), gsub("^cv_", "", names(dt)))
  dt[, sum := rowSums(.SD), .SDcols = names(dt)]
  dt <- dt[!sum == 0]
  dt[, sum := NULL]
  preds <- predict_mr_brt(model, newdata = dt, write_draws = T)
  pred_dt <- as.data.table(preds$model_draws)
  pred_dt[, logadj := rowMeans(.SD), .SDcols = draws]
  pred_dt[, logadj_se := apply(.SD, 1, sd), .SDcols = draws]
  pred_dt[, c(draws, "Z_intercept") := NULL]
  setnames(pred_dt, names(pred_dt), gsub("^X", "cv", names(pred_dt)))
  return(pred_dt)
}
make_adjustment_sex <- function(data_dt, ratio_dt){
  data_dt <- adjust_dt 
  ratio_dt <- adjust_preds 
  cvs <- names(ratio_dt)[grepl("^cv", names(ratio_dt))] 
  dt <- merge(data_dt, ratio_dt, by = "midage", all.x = T, allow.cartesian=TRUE) 
  adjust_dt <- copy(dt[!is.na(logadj) & !mean == 0])
  noadjust_dt <- copy(dt[is.na(logadj) | mean == 0])
  noadjust_dt[, c("logadj", "logadj_se") := NULL]
  
  ## ADJUST MEANS
  adjust_dt[, logmean := log(mean)]
  adjust_dt$log_se <- sapply(1:nrow(adjust_dt), function(i){
    mean_i <- adjust_dt[i, "mean"]
    se_i <- adjust_dt[i, "standard_error"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  adjust_dt[, logmean_adj := logmean - logadj]
  adjust_dt[, logmean_adj_se := sqrt(logadj_se^2 + log_se^2)]
  adjust_dt[, mean_adj := exp(logmean_adj)]
  adjust_dt$standard_error_adj <- sapply(1:nrow(adjust_dt), function(i){
    mean_i <- adjust_dt[i, "logmean_adj"]
    se_i <- adjust_dt[i, "logmean_adj_se"]
    deltamethod(~exp(x1), mean_i, se_i^2)
  })
  
  full_dt <- copy(adjust_dt)
  full_dt$note_modeler<-c(rep(NA, length(full_dt[,1])))
  full_dt[, `:=` (mean = mean_adj, standard_error = standard_error_adj, upper = "", lower = "", 
                  cases = "", sample_size = "", uncertainty_type_value = "", 
                  note_modeler = paste0(note_modeler, " | crosswalked with log(ratio): ", round(logadj, 2), " (", 
                                        round(logadj_se), ")"))]
  extra_cols <- setdiff(names(full_dt), names(noadjust_dt))
  full_dt[, c(extra_cols) := NULL]
  final_dt <- rbind(noadjust_dt, full_dt)
  return(list(epidb = final_dt, vetting_dt = adjust_dt))
}


make_adjustment <- function(data_dt, ratio_dt){
  data_dt <- adjust_dt
  ratio_dt <- adjust_preds 
  cvs <- names(ratio_dt)[grepl("^cv", names(ratio_dt))] 
  dt <- merge(data_dt, ratio_dt, by = cvs, all.x = T) 
  adjust_dt <- copy(dt[!is.na(logadj) & !mean == 0])
  noadjust_dt <- copy(dt[is.na(logadj) | mean == 0])
  noadjust_dt[, c("logadj", "logadj_se") := NULL]
  
  ## ADJUST MEANS
  adjust_dt[, logmean := log(mean)]
  adjust_dt$log_se <- sapply(1:nrow(adjust_dt), function(i){
    mean_i <- adjust_dt[i, "mean"]
    se_i <- adjust_dt[i, "standard_error"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  adjust_dt[, logmean_adj := logmean - logadj]
  adjust_dt[, logmean_adj_se := sqrt(logadj_se^2 + log_se^2)]
  adjust_dt[, mean_adj := exp(logmean_adj)]
  adjust_dt$standard_error_adj <- sapply(1:nrow(adjust_dt), function(i){
    mean_i <- adjust_dt[i, "logmean_adj"]
    se_i <- adjust_dt[i, "logmean_adj_se"]
    deltamethod(~exp(x1), mean_i, se_i^2)
  })
  
  full_dt <- copy(adjust_dt)
  full_dt$note_modeler<-c(rep(NA, length(full_dt[,1])))
  full_dt[, `:=` (mean = mean_adj, standard_error = standard_error_adj, upper = "", lower = "", 
                  cases = "", sample_size = "", uncertainty_type_value = "", 
                  note_modeler = paste0(note_modeler, " | crosswalked with log(ratio): ", round(logadj, 2), " (", 
                                        round(logadj_se), ")"))]
  extra_cols <- setdiff(names(full_dt), names(noadjust_dt))
  full_dt[, c(extra_cols) := NULL]
  final_dt <- rbind(noadjust_dt, full_dt)
  return(list(epidb = final_dt, vetting_dt = adjust_dt))
}


# 19th July 2019
adjust_dt<-predict_sex$final
dim(adjust_dt) 
adjust_dt<-data.table(adjust_dt) 

# Add the recall lifetime adjustment
########################
cv_drop <- c(names(dt)[grep("cv", names(dt))])[c(1:4, 6:15)] # keep only cv_recall_lifetime (so excluded from this list)
adjust_preds <- get_preds_adjustment(raw_data = adjust_dt, model = dem_model_recall_lifetime)
mean(adjust_preds$logadj)
mean(adjust_preds$logadj_se)
adjust_dt <- get_cases_sample_size(adjust_dt)
adjust_dt <- get_se(adjust_dt)
adjusted <- make_adjustment(data_dt = adjust_dt, ratio_dt = adjust_preds)
# check
dim(adjusted$epidb) # the final dataset, adjusted and unadjusted binded together 
dim(adjusted$vetting_dt) # the adjusted data 
########################
# check this one isn't being crosswalked (Cossu study)
cossu<-subset(dt, dt$nid==124495)
cossu$mean

cossu_toadj<-subset(adjust_dt, adjust_dt$nid==124495)
cossu_toadj$mean

cossu_toadj<-subset(adjusted$epidb, adjusted$epidb$nid==124495)
cossu_toadj$mean

########################

# Add the cv_marketscan_all_2000 adjustment 
adjust_dt<-adjusted$epidb
cv_drop <- c(names(dt)[grep("cv", names(dt))])[c(1:7, 9:15)] # keep only cv_marketscan_all_2000 (so excluded from this list)
adjust_preds <- get_preds_adjustment(raw_data = adjust_dt, model = dem_model_marketscan_2000)
mean(adjust_preds$logadj)
mean(adjust_preds$logadj_se)
adjust_dt <- get_cases_sample_size(adjust_dt)
adjust_dt <- get_se(adjust_dt)
adjusted <- make_adjustment(data_dt = adjust_dt, ratio_dt = adjust_preds)
# check
dim(adjusted$epidb) # the final dataset, adjusted and unadjusted binded together
dim(adjusted$vetting_dt) # the adjusted data
########################
# check this one isn't being crosswalked (Cossu study)
cossu<-subset(dt, dt$nid==124495)
cossu$mean

cossu_toadj<-subset(adjust_dt, adjust_dt$nid==124495)
cossu_toadj$mean

cossu_toadj<-subset(adjusted$epidb, adjusted$epidb$nid==124495)
cossu_toadj$mean

########################

# Add the cv_marketscan aggregate adjustment
adjust_dt<-adjusted$epidb
cv_drop <- c(names(dt)[grep("cv", names(dt))])[c(1:14)] # keep only cv_marketscan (so excluded from this list)
adjust_preds <- get_preds_adjustment(raw_data = adjust_dt, model = dem_model_marketscan)
mean(adjust_preds$logadj)
mean(adjust_preds$logadj_se)
adjust_dt <- get_cases_sample_size(adjust_dt)
adjust_dt <- get_se(adjust_dt)
adjusted <- make_adjustment(data_dt = adjust_dt, ratio_dt = adjust_preds)
# check
dim(adjusted$epidb) # the final dataset, adjusted and unadjusted binded together
dim(adjusted$vetting_dt) # the adjusted data
########################
# check this one isn't being crosswalked (Cossu study)
cossu<-subset(dt, dt$nid==124495)
cossu$mean

cossu_toadj<-subset(adjust_dt, adjust_dt$nid==124495)
cossu_toadj$mean

cossu_toadj<-subset(adjusted$epidb, adjusted$epidb$nid==124495)
cossu_toadj$mean

########################


prediction_plot <- function(vetting_data){
  dt <- copy(vetting_data)
  dt <- merge(dt, loc_dt[, .(location_id, super_region_name)], by = "location_id")
  dt[, `:=` (N_adj = (mean_adj*(1-mean_adj)/standard_error_adj^2),
             N = (mean*(1-mean)/standard_error^2))]
  wilson_norm <- as.data.table(binconf(dt$mean*dt$N, dt$N, method = "wilson"))
  wilson_adj <- as.data.table(binconf(dt$mean_adj*dt$N_adj, dt$N_adj, method = "wilson"))
  dt[, `:=` (lower = wilson_norm$Lower, upper = wilson_norm$Upper,
             lower_adj = wilson_adj$Lower, upper_adj = wilson_adj$Upper)]
  dt[, midage := (age_end + age_start)/2]
  ages <- c(60, 70, 80, 90)
  dt[, age_group := cut2(midage, ages)]
  gg <- ggplot(dt, aes(x = mean, y = mean_adj, color = as.factor(super_region_name))) +
    geom_point() +
    #facet_wrap(~measure+age_group, ncol = 5) +
    geom_errorbar(aes(ymin = lower_adj, ymax = upper_adj)) +
    geom_errorbarh(aes(xmin = lower, xmax = upper)) 
  scale_color_brewer(palette = "Spectral", name = "Super Region") + 
    labs(x = "Unadjusted means", y = "Adjusted means") +
    theme_classic()
  return(gg)
}

pdf(paste0(dem_dir, "crosswalks_predsnomkscn_onepanelsubset_", date, ".pdf"), width = 12)
prediction_plot(adjusted$vetting_dt[!cv_marketscan == 1])
dev.off()

pdf(paste0(dem_dir, "crosswalks_predsmkscn_", date, ".pdf"), width = 12)
prediction_plot(adjusted$vetting_dt[cv_marketscan == 1])
dev.off()


pdf(file=paste0("FILEPATH", "mean vs adjusted mean post crosswalk.pdf"))
plot(adjusted$vetting_dt$mean, adjusted$vetting_dt$mean_adj)
dev.off()

pdf(file=paste0("FILEPATH", "mean vs adjusted mean post crosswalk small values.pdf"))
adjusted_mean_small<-subset(adjusted$vetting_dt,adjusted$vetting_dt$mean<0.01)
plot(adjusted_mean_small$mean, adjusted_mean_small$mean_adj)
dev.off()


# Output 
write.csv(adjusted$epidb, paste0(file="FILEPATH", "FILEPATH", date, ".csv"))
write.csv(adjusted$epidb, paste0(file="FILEPATH", "FILEPATH", date, ".csv"))
# Save as xlsx file and rename worksheet as extraction
# take note of the change in sample size
dim(adjusted$epidb) # original dataset
dim(adjusted$vetting_dt) # adjusted dataset
