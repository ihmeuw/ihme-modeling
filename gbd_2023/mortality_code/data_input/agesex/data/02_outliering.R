# Title: Mark outliers/exclusions

# Description:
# (1) marks outliers - systematic and manual (including some data drops of sources we don't trust
# and locations we do not create estimates for)
# (2) formats and outputs final age-sex data file for modeling
# (3) mark best if T


# set-up ---------------------------------------------------------------------------------------------

rm(list=ls())

library(data.table)
library(readstata13)
library(foreign)
library(assertable)
library(argparse)
library(assertable)
library(plyr)
library(mortdb, lib.loc = "FILEPATH")

# For interactive testing
if(interactive()){

  version_id <-
  version_5q0_est_id <-
  version_5q0_data_id <-
  version_ddm_id <-
  gbd_year <-
  mark_best <-

}else{

  parser <- argparse::ArgumentParser()
  parser$add_argument("--version_id", type = "integer", required = TRUE,
                      help = "age sex version id")
  parser$add_argument("--ddm_version_id", type = "integer", required = TRUE,
                      help = "ddm version id")
  parser$add_argument("--model_5q0_version_id", type = "integer", required = TRUE,
                      help = "5q0 estimate version id")
  parser$add_argument("--data_5q0_version_id", type = "integer", required = TRUE,
                      help = "5q0 data version id")
  parser$add_argument("--gbd_year", type = "integer", required = TRUE,
                      help = "GBD year")
  parser$add_argument("--mark_best", type = "character", required = TRUE,
                      help = "Whether to upload with best status")

  args <- parser$parse_args()
  list2env(args, .GlobalEnv)

  version_ddm_id <- ddm_version_id
  version_5q0_est_id <- model_5q0_version_id
  version_5q0_data_id <- data_5q0_version_id
}

# directories
input_dir   = paste0("FILEPATH")
output_dir  = paste0("FILEPATH")
ddm_dir			= paste0("FILEPATH")

# 5q0 files
raw5q0_file 		 = paste0("FILEPATH")
estimate5q0_file = paste0("FILEPATH")

# 4 and 5 star VR locations
four_five_star <- fread(paste0("FILEPATH"))
four_five_star <- four_five_star[time_window == "full_time_series" & stars >= 4]
four_five_star <- four_five_star[,.(ihme_loc_id, stars)]

# age map
age_map <- c("enn" = 2,
             "lnn" = 3,
             "pnn" = 4,
             "pna" = 388,
             "pnb" = 389,
             "nn" = 42,
             "inf" = 28,
             "ch" = 5,
             "cha" = 238,
             # "cha" = 49,
             "chb" = 34,
             "u5" = 1)
age_map <- as.data.table(age_map, keep.rownames=T)
setnames(age_map, c("rn", "age_map"), c("age", "age_group_id"))

# location map
loc_map <- mortdb::get_locations(gbd_year = gbd_year, level = "all", gbd_type = "ap_old")[, c("ihme_loc_id", "location_id",
                                                                                              "region_name")]
region_map <- loc_map[, .(ihme_loc_id, region_name)]
loc_map[, region_name := NULL]
loc_map <- loc_map[ihme_loc_id != "CHN"]

# compiled data
dt <- fread(paste0("FILEPATH"))

# Subseting  -------------------------------------------------------------------------------------------

# subset to estimated locations
dt <- merge(dt, loc_map[, .(ihme_loc_id)], by = "ihme_loc_id")

# subset to obs with q_u5
dt <- dt[!is.na(q_u5)]

# remove unused or impossible years
dt <- dt[year >= 1950 & year <= gbd_year]

# Outliers ---------------------------------------------------------------------------------------------

# create exclusion variable
dt[, exclude := 0]

# fill source_y to merge
dt[is.na(source_y), source_y := source]

## exclude when we do not have male, female, and both-sex
dt[, count := .N, by = c("ihme_loc_id", "year", "source_y")]
assertable::assert_values(dt, "count", test = "lte", test_val = 3, warn_only = T)
dt[, allsex := ifelse(count==3,1,0)]
dt[, exclude_sex_mod := ifelse(allsex==0,1,0)]

## exclude CBH estimates more than 15 years before the survey ------------------------------------------

dt[(year < (survey_year - 15)) & !is.na(survey_year), exclude := 11]

## exclude anything that is outliered or scrubbed from 5q0 GPR -----------------------------------------

raw5q0 <- fread(raw5q0_file)
setnames(raw5q0, "source", "source_y")

# format raw 5q0 to match dt formating for vr
raw_vr <- raw5q0
raw_vr[grepl("DSP", source_y), source_type := "DSP"]
raw_vr[grepl("VR", source_y), source_type := "VR"]
raw_vr[grepl("SRS", source_y), source_type := "SRS"]

# subset to required sources (VR, SRS, DSP)
raw_vr <- raw_vr[!is.na(source_type)]

# remove duplicates for merging
raw_vr[, dup := .N, by = c("ihme_loc_id", "year", "source_type")]
raw_vr[, select_dup := max(ptid), by = c("ihme_loc_id", "year", "source_type")]
raw_vr <- raw_vr[!(dup>1 & (select_dup != ptid))]
raw_vr[, c("dup", "select_dup") := NULL]
raw_vr[, year := floor(year)]
raw_vr[, year:= as.double(year)]

# subset to required columns for each vr source type
raw_vr <- raw_vr[, .(ihme_loc_id, year, source_type, outlier, shock)]
raw_vr <- unique(raw_vr)

# mark raw 5q0 exclusions for vr
dt <- merge(dt, raw_vr, by = c('ihme_loc_id', 'year', 'source_type'), all.x=T)
dt[data_subset == "vr/dsp/srs" & (outlier == 1 | shock == 1 | is.na(shock)), exclude := 2]
dt[, c("outlier", "shock") := NULL]

# format and mark raw 5q0 exclusions for CBH - shocks
raw_cbh <- raw5q0[in.direct == "direct"]
raw_cbh <- raw_cbh[, .(ihme_loc_id, year, source_y, shock)]
raw_cbh <- unique(raw_cbh)
dt <- merge(dt, raw_cbh, by = c('ihme_loc_id', 'year', 'source_y'), all.x=T)
dt[data_subset == "cbh" & shock == 1, exclude := 2]
dt[, c("shock") := NULL]

# format and mark raw 5q0 exclusions for CBH - outliers
raw_cbh <- raw5q0[in.direct == "direct" & outlier == 1]
raw_cbh <- raw_cbh[, .(ihme_loc_id, source_y, outlier)]
raw_cbh <- unique(raw_cbh)
dt <- merge(dt, raw_cbh, by = c('ihme_loc_id', 'source_y'), all.x=T)
dt[data_subset == "cbh" & outlier == 1, exclude := 2]
dt[, c("outlier") := NULL]

# format and mark raw 5q0 exclusions for CBH - unmerged
raw_cbh <- raw5q0[in.direct == "direct" & outlier != 1 & shock != 1]
raw_cbh <- raw_cbh[, .(ihme_loc_id, source_y, outlier)]
raw_cbh <- unique(raw_cbh)
dt <- merge(dt, raw_cbh, by = c('ihme_loc_id', 'source_y'), all.x=T)
dt[data_subset == "cbh" & (is.na(outlier)), exclude := 2]
dt[, c("outlier") := NULL]

## exclude any VR/SRS that is incomplete -------

# subset dt to compare with 5q0 estimates
incomplete <- dt[data_subset == "vr/dsp/srs" & sex == "both"]
incomplete <- incomplete[ , .(ihme_loc_id, year, source, source_type, q_u5)]

# load 5q0 estimates and format
est5q0 <- fread(estimate5q0_file)
est5q0 <- est5q0[estimate_stage_id == 3]
est5q0 <- merge(est5q0, loc_map, by = "location_id")
setnames(est5q0, c("mean", "viz_year"), c("q5med", "year"))
est5q0[, year := floor(year)]
est5q0[, year:= as.double(year)]
est5q0 <- est5q0[, .(ihme_loc_id, year, q5med)]

# merge with incomplete
incomplete <- merge(incomplete, est5q0, by = c("ihme_loc_id", "year"), all.x = T)

# calculate average completeness over 9-year average of data
year_min <- min(incomplete$year)
year_max <- max(incomplete$year)

for(yr in year_min:year_max){
  lower <- yr - 4
  upper <- yr + 4
  if(lower < year_min) lower <- year_min
  if(upper > year_max) upper <- year_max

  incomplete[(year >= lower) & (year <= upper),
             tempmean1 := mean(q5med), by = c("ihme_loc_id", "source")]
  incomplete[(year >= lower) & (year <= upper),
             tempmean2 := mean(q_u5), c("ihme_loc_id", "source")]

  incomplete[year == yr, avg_complete := tempmean2/tempmean1, c("ihme_loc_id", "source")]
}

incomplete[, c("tempmean1", "tempmean2") := NULL]

# calculate completeness for an individual year
incomplete[, complete := q_u5/q5med]

## mark incomplete observations
incomplete[, keep := 0]

incomplete[complete < 0.85, keep := 1]

incomplete[avg_complete > 0.85, keep := 0]

incomplete[complete > 1.5 | complete < 0.5, keep := 1]

# subset incomplete to incomplete observations as defined above
incomplete <- incomplete[keep == 1]
incomplete <- incomplete[, .(ihme_loc_id, year, source, keep)]
incomplete <- unique(incomplete)

# merge with dt and update exclusions
dt <- merge(dt, incomplete, by = c("ihme_loc_id", "year", "source"), all.x = T)
dt[data_subset == "vr/dsp/srs" & keep == 1, exclude := 3]
dt[, keep := NULL]

# Other systematic exclusions ---------------------------------------------------------------------------

dt[withinsex == 1, exclude := 10]

## manual exclusions ------------------------------------------------------------------------------------

# create age-specific exclude
for(age in age_map$age){
  ex_age_name <- paste0("exclude_", age)
  dt[, (ex_age_name) := exclude]
  dt[exclude == 0 & get(paste0("q_", age)) < 0.0001, (ex_age_name) := 12]
}

dt[ihme_loc_id == "IND_43920" & year >= 1990 & year <= 1991, exclude := 8]
dt[grepl("IND_human_dev", source), exclude := 8]
dt[ihme_loc_id == "GAB" & year ==1988.5 & (sex=="male" | sex=="both"), exclude_lnn := 8]
dt[ihme_loc_id == "KAZ" & year ==1993 & (sex=="male" | sex=="both"), exclude_lnn := 8]
dt[exclude_enn==0 & ihme_loc_id=="BWA", exclude_enn := 8]
dt[exclude_enn==0 & ihme_loc_id=="ECU" & year<=1990, exclude_enn := 8]
dt[exclude_enn==0 & ihme_loc_id=="IND_43885" & year<=1990 & sex=="female", exclude_enn := 8]
dt[sex=="female" & ihme_loc_id =="IND_43900" & exclude_lnn==0 & source== "IND DHS urban rural", exclude_lnn := 8]
dt[sex=="female" & ihme_loc_id =="IND_43900" & exclude_ch==0 & source=="VR" & year<1999, exclude_ch := 8]
dt[ihme_loc_id =="MEX_4669" & year<1990 & year>1978 & exclude_enn ==0, exclude_enn := 8]
dt[ihme_loc_id == "PAK" & year>=1985 & year<=2000 & exclude_enn==0 &
     (source== "PAK_IHS_1998-1999" | source=="PAK IHS 2001-2002"), exclude_enn := 8]
dt[, exclude_sex_mod := 0]

# merge stars data
dt <- merge(dt, four_five_star, by = "ihme_loc_id", all.x = T)

# mark exclusion
dt[!is.na(stars) & source_type != "VR" & source_type!="DSP", exclude_sex_mod := 1]
dt[, stars := NULL]

to_interp <- unique(dt[sex == "both", .(ihme_loc_id, year)])
# merge all 5q0 obs for interp
to_interp <- merge(to_interp, est5q0, by = c("ihme_loc_id", "year"), all.x = T , all.y = T)
mean_interp <- to_interp

for(loc in unique(to_interp$ihme_loc_id)){
  # interpolate to fill missing 5q0 for non-integer years
  temp <- approx(x=to_interp[ihme_loc_id == loc]$year,
                 y=to_interp[ihme_loc_id == loc]$q5med,
                 xout= to_interp[ihme_loc_id == loc & is.na(q5med)]$year,
                 method = "linear", rule = 2)
  temp <- data.table("year" = temp$x, "interp" = temp$y)
  temp[, ihme_loc_id := loc]

  # fill missing values with interpolated values
  mean_interp = merge(mean_interp, temp, by = c("ihme_loc_id", "year"), all.x = T, all.y = T)
  mean_interp[is.na(q5med) & ihme_loc_id == loc, q5med := interp]
  mean_interp[, interp := NULL]
}

# Copy and append all sex groups
mean_interp[, sex := "both"]
for(s in c("female", "male")){
  temp <- mean_interp[sex == "both"]
  temp[, sex := s]
  mean_interp <- rbind(mean_interp, temp)
}

# merge with dt
dt <- merge(dt, mean_interp, by = c("ihme_loc_id", "year", "sex"), all.x = T)

# ensure interpolation filled all missing values
assertable::assert_values(dt, "q5med", test="not_na")

# calculate completeness
dt[, s_comp := q_u5/q5med]

# check there is no missing completeness
assert_values(dt, "s_comp", test = "not_na")

ddm45q15 <- as.data.table(
  haven::read_dta(
    paste0("FILEPATH"))
)[source_type == "VR" & !is.na(comp), .(ihme_loc_id, year, sex, source_type, comp)]

# check input
if(nrow(ddm45q15) != nrow(unique(ddm45q15[, .(ihme_loc_id, year, sex, comp)]))) stop("ddm not unique")

## calculate mean completeness (over sex) and keep > 0.95
ddm45q15[, comp := mean(comp), by = c("ihme_loc_id", "year")]
ddm45q15 <- ddm45q15[ comp >= 0.95]
ddm45q15 <- unique(ddm45q15[, .(ihme_loc_id, year, comp)])
ddm45q15[, year := as.double(year)]

# merge with dt and update exclusions
dt <- merge(dt, ddm45q15, by = c("ihme_loc_id", "year"), all.x = T)
dt[(exclude == 1 | exclude == 7) & grepl("VR", source) & !is.na(comp), exclude := 0]
dt[, comp := NULL]

dt[, drop := 0]
dt <- dt[ihme_loc_id =="MNG" & (source=="ICD9_BTL" | source=="Mongolia_2004_2008" | source=="Other_Maternal")
         & (year==1994 | year==2006 | year==2007), drop := 1]
dt <- dt[drop == 0]
dt[ihme_loc_id == "ALB", exclude_enn := 8]
dt[ihme_loc_id == "BHS", exclude_lnn := 8]
for(var in grep("exclude", names(dt), value = T)){
  dt[!is.na(s_comp) & s_comp >= 0.75 & ihme_loc_id == "BLR", (var) := 0]
  dt[source_type == "VR" & ihme_loc_id == "BLZ", (var) := 0]
  dt[ihme_loc_id == "DMA", (var) := 8]
}
dt[ihme_loc_id == "ECU" & source == "ECU_ENSANUT_2012" & year >= 2010 & year <= 2011, drop := 1]
dt <- dt[drop == 0]
dt[ihme_loc_id == "UKR", exclude_lnn := 8]
for (var in c("exclude_enn", "exclude_lnn", "exclude_pnn", "exclude_pna", "exclude_pnb")){
  dt[ihme_loc_id == "FJI", (var) := 8]
  dt[ihme_loc_id == "GBR" & year >= 1980 & year < 1986, (var) := 8]
}
dt[ihme_loc_id == "PSE" & grepl("DHS", source) & exclude == 0, exclude_enn := 8]
dt[ihme_loc_id == "UKR" & grepl("CDC", source) & exclude == 0 & year >= 1980 & year <= 2000, exclude_enn := 8]
dt[ihme_loc_id == "BRA_4751" & grepl("DHS", source) & exclude == 0 & year >= 1987 & year <= 1990, exclude_lnn := 8]
dt[ihme_loc_id == "BRA_4752" & grepl("DHS", source) & exclude == 0 & year >= 1990 & year <= 2000, exclude_enn := 8]
dt[ihme_loc_id == "BRA_4759" & exclude == 0 & year >= 1980 & year <= 1986, exclude_enn := 8]
dt[ihme_loc_id == "BRA_4762" & exclude == 0 & year >= 1986 & year <= 1990, exclude_enn := 8]
dt[ihme_loc_id == "BRA_4769" & exclude == 0 & year >= 1987 & year <= 1990, exclude_enn := 8]
dt[ihme_loc_id == "IND_43874" & exclude == 0 & year >= 1980 & year <= 1982, exclude_enn := 8]
dt[ihme_loc_id == "IND_43901" & exclude == 0 & year >= 2000 & year <= 2005, exclude_enn := 8]
dt[ihme_loc_id == "IND_43906" & exclude == 0 & year >= 1998 & year <= 2004, exclude_lnn := 8]
dt[grepl("MEX_", ihme_loc_id) & exclude == 0 & grepl("ENADID", source), exclude_enn := 8]
dt[ihme_loc_id == "NGA" & exclude == 0 & q_enn < 0.02, exclude_enn := 8]

for(var in grep("exclude", names(dt), value = T)){
  dt[source_type == "SRS" & ihme_loc_id == "IND" & year == 2017, (var) := 0]
}

dt[ihme_loc_id =="NIC" & year > 2000 & source=="NIC_DHS_ENDESA_2011-2012", exclude_enn := 8]
for(var in grep("exclude", names(dt), value = T)){
  dt[ihme_loc_id =="BOL" & year >2000 & source == "BOL_EDSA_2016_2016", (var) := 8]
}

dt[ ihme_loc_id == "ZAF" & year==2004.25 & source=="ZAF_DHS_2016_2016", exclude_lnn := 8]

dt[grepl("MCCD", source) & grepl("IND_", ihme_loc_id) & data_subset == "vr/dsp/srs", drop := 1]
dt <- dt[drop == 0]

for(var in grep("exclude", names(dt), value = T)){
  dt <- dt[grepl("PHL_", ihme_loc_id) & grepl("DHS", source), (var) := 8]
  dt <- dt[ihme_loc_id == "PHL_53582" & source_type == "VR", (var) := 8]
}

to_exclude <- c("exclude_enn", "exclude_lnn", "exclude_pnn")
for(var in to_exclude){
  dt <- dt[grepl("CHN_", ihme_loc_id) & source_type == "DSP" & get(var) == 0, (var) := 8]
}

dt[ihme_loc_id == "KEN_35626" & q_enn > 0.05, exclude_enn := 8]
dt[ihme_loc_id == "KEN_35626" & q_lnn > 0.01, exclude_lnn := 8]
dt[ihme_loc_id == "KEN_35626" & q_pnn > 0.05, exclude_pnn := 8]
dt[ihme_loc_id == "KEN_35626" & q_inf > 0.09, exclude_inf := 8]

dt[ihme_loc_id == "NGA_25321" & (year <= 1991 & year >= 1990), exclude_enn := 8]
dt[ihme_loc_id == "NGA_25321" & (year <= 1991 & year >= 1990), exclude_pnb := 8]
dt[ihme_loc_id == "NGA_25322" & sex == "female" & q_enn < 0.02, exclude_enn := 8]
dt[ihme_loc_id == "NGA_25339" & q_enn < 0.02, exclude_enn := 8]

for(var in grep("exclude", names(dt), value = T)){
  dt[ihme_loc_id == "PAK_53616" & get(var) == 0, (var) := 8]
}

for(var in grep("exclude", names(dt), value = T)){
  dt[ihme_loc_id == "GBR" & year > 2015, (var) := 0]
}

for(var in c("ch", "inf", "u5")){
  temp_name <- paste0("q_", var)
  dt[
    grepl("IND", ihme_loc_id) & grepl("SRS", source, ignore.case = TRUE) & get(temp_name) == 0,
    paste0("exclude_", var) := 12
  ]
}

for(var in c("ch", "cha", "chb", "lnn")){
  dt[
    ihme_loc_id == "BRA" & sex == "female" & year > 1993 & year < 1994 & source == "DHS",
    paste0("exclude_", var) := 9
  ]
}

## final formatting -------------------------------------------------------------------------------------

## create broadsource for modeling code
dt[, broadsource := source]
dt[grepl("DSP", broadsource), broadsource := "DSP"]
dt[grepl("VR", broadsource), broadsource := "VR"]
dt[grepl("SRS", broadsource), broadsource := "SRS"]

# categorize remaining cases VR/SRS/DSP in broadsource
dt[data_subset=="vr/dsp/srs", broadsource := source_type]

dt[data_subset != "cbh", source_y := "not CBH"]
dt[, source := paste0(source, "___", source_y)]

# reformat pop
dt[, pop_5 := pop_inf + pop_ch]

# reformat year for cod/noncod
dt[, to_add := 0.000]
dt[data_subset == "vr/dsp/srs", to_add := 0.500]
dt[, year := as.double(year, 4) + as.double(to_add, 4)]

# merge region name
dt <- merge(dt, region_map, by = "ihme_loc_id", all.x = T)
assert_values(dt, "region_name", test = "not_na")

# subset columns
necessary_cols <- c("ihme_loc_id", "region_name", "sex", "year", "age_type", "births", "withinsex", "exclude_sex_mod",
                    "source", "NID", "broadsource", "s_comp",
                    "q_enn", "q_lnn", "q_nn", "q_pnn", "q_pna", "q_pnb", "q_inf", "q_ch", "q_cha", "q_chb", "q_u5",
                    "prob_enn", "prob_lnn", "prob_pna", "prob_pnb", "prob_pnn", "prob_inf", "prob_ch",
                    "prob_cha", "prob_chb",
                    "pop_inf", "pop_ch", "pop_cha", "pop_chb", "pop_5",
                    "deaths_enn", "deaths_lnn", "deaths_nn", "deaths_pnn", "deaths_pna", "deaths_pnb", "deaths_inf", "deaths_ch",
                    "deaths_cha", "deaths_chb", "deaths_u5",
                    "exclude", "exclude_enn", "exclude_lnn", "exclude_nn", "exclude_pnn", "exclude_pna", "exclude_pnb",
                    "exclude_inf", "exclude_ch", "exclude_cha", "exclude_chb", "exclude_u5")

dt <- dt[, ..necessary_cols]

# drop data if it has no usable columns
dt <- dt[!(is.na(q_enn) & is.na(q_lnn) & is.na(q_nn) & is.na(q_pnn) & is.na(q_pna) & is.na(q_pnb) &
             is.na(q_ch) & is.na(q_cha) & is.na(q_chb) & is.na(q_u5))]

# assert no China national
assertable::assert_values(dt, "ihme_loc_id", test = "not_equal", test_val = "CHN")

# make sure there are no duplicates
dt[, dup := .N, by = c("ihme_loc_id", "region_name", "year", "source", "sex")]
assert_values(dt, "dup", test = "lte", test_val = 1)
dt[, dup := NULL]

# outputs file in stata format
readr::write_csv(dt, paste0("FILEPATH"))

# mark best if T
if(mark_best=="True"){
  update_status("age sex", "data", run_id = new_run_id, new_status = "best")
}
