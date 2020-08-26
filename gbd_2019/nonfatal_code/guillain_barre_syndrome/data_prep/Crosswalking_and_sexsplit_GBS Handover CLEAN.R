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


clinical_dataset_gbs<-read.csv(file=paste0("FILEPATH", "2019_09_25", ".csv"), sep=",", header=TRUE)
dt<-as.data.table(clinical_dataset_gbs)

table(dt$clinical_data_type)
table(dt$group_review)
dt$group_review<-c(rep(1, length(dt$group_review)))
table(dt$group_review)


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

## GET DEFINITIONS (ALL BASED ON CV'S - WHERE ALL 0'S IS REFERENCE)
get_definitions <- function(ref_dt){
  dt <- copy(ref_dt)
  cvs <- names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop]
  cvs <- cvs[!cvs %in% cv_drop] ## DROPPED CV'S AS DIDN'T WANT TO USE IN THE CV_DROP OBJECT
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
  start_matches <- pull_dt[age_start_match == T, age_start] ## GET ALL START MATCHES (NOTE: NOT SURE WHAT WILL HAPPEN IF YOU HAVE TWO START MATCHES ON THE SAME AGE)
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
get_matches <- function(n, pair_dt, year_span = 10, age_span = 5){ # original
#get_matches <- function(n, pair_dt, year_span = 10, age_span = 5){
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
                       between(year_match, year_match2 - year_span/2, year_match2 + year_span/2)] ## FILTER OUT SEX, MEASURE, YEAR
  if (pair[1] == "_cv_marketscan" | pair[2] == "_cv_marketscan"){ ## EXACT LOCS FOR MARKETSCAN BUT CURRENTLY ALL ARE EXACT LOCS
    matched <- matched[location_id == location_id2]
  }
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

dt<-as.data.table(dt)
dim(dt) 

dt$group_review<-c(rep(1, length(dt$group_review))) 
dem_sex_dt<-dt 
dem_sex_dt <- get_cases_sample_size(dem_sex_dt)
dem_sex_dt <- get_se(dem_sex_dt)
dem_sex_matches <- find_sex_match(dem_sex_dt)

dem_sex_matches <- subset(dem_sex_matches, standard_error_Male > 0)
mrbrt_sex_dt <- calc_sex_ratios(dem_sex_matches)

typeof(mrbrt_sex_dt$log_ratio)
typeof(mrbrt_sex_dt$log_se)
mrbrt_sex_dt$log_se<-unlist(mrbrt_sex_dt$log_se)
typeof(mrbrt_sex_dt$log_se)

# 26th September 2019 - launch_rstudio_fair 5 50G
model_name <- paste0("gbs_sex_split", date)


sex_model <- run_mr_brt(
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


plot_mr_brt(sex_model)
sex_predictions <- predict_mr_brt(sex_model, newdata = data.table(midage = seq(0, 99, by = 5)), write_draws = T) # max age=99 # summary(dt$age_end)

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
    pred_draws <- as.data.table(read.csv("FILEPATH/model_draws.csv"))
    pred_draws[, c("X_intercept", "Z_intercept") := NULL]
    pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
    ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
    ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
    pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                             year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)])
    pops[age_group_years_end == 125, age_group_years_end := 99]
    tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt), mc.cores = 9))
    tosplit_dt <- tosplit_dt[!is.na(both_N)] ## GET RID OF DATA THAT COULDN'T FIND POPS - RIGHT NOW THIS IS HAPPENING FOR 100+ DATA POINTS
    tosplit_dt[, merge := 1] # 395 rows
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
    predict_sex <- list(final = total_dt, graph = split_dt)
##### predict_sex$final<-total_dt # if needed ##### 

# 12/09/2019
# Show that there are no missing NIDs from sex-split
# Extracted data (before sex-split)
dim(dt) 
table(dt$sex) 
length(unique(dt$nid)) 
# Extracted data (after sex-split)
dim(predict_sex$final) 
table(predict_sex$final$sex)
length(unique(predict_sex$final$nid)) 


##########################
# 19/09/2019 produces missing cases when sex-splitting even when full cases are in the parent row
# use one of the functions above to calculate the missing case values for the sex-split rows
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


# PREDICTION --------------------------------------------------------------
# No crosswalking adjustment for GBS
# Output just the sex-split dataset
adjusted<-predict_sex_final_cases_fromse # after re-calculating the case sizes (which were set to missing after sex-split)

write.csv(adjusted, paste0(file=mrbrt_dir, "step4_clinical_informatics_added_", "sex-split", date, ".csv")) 


# final check
# Show that there are no missing NIDs from sex-split
# Extracted data (after sex-split)
dim(adjusted) 
table(adjusted$sex) 
length(unique(adjusted$nid))


dt_sex_split<-as.data.table(adjusted)
df<-update_seq(dt_sex_split)
is.data.table(df) 
dim(df) 

# SAVE AS CROSSWALKED VERSION -------------------------------------------------------------- 


write.csv(crosswalked_bundle_gbs, paste0(file="FILEPATH", "step4_clinical_informatics_added_", "sex-split", "age-split", "crosswalked_", date, ".csv")) 

write.xlsx(crosswalked_bundle_gbs, paste0(file="FILEPATH", "step4_clinical_informatics_added_", "sex-split", "age-split", "crosswalked_", date, ".xlsx"), 
           sheetName="extraction")
