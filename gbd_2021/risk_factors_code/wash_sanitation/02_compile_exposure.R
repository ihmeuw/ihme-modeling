### Author: [AUTHOR]
### Date: [DATE]
### Purpose: add newly extracted, prepped, and collapsed WaSH exposure data
###########################################################################

## libraries
library(data.table)
library(magrittr)
library(openxlsx)
library(binom)

## functions
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_crosswalk_version.R")
"%unlike%" <- Negate("%like%")
"%ni%" <- Negate("%in%")

## settings
gbd_cycle <- "GBD2020"
gbd_round_id <- 7
decomp_step <- "iterative"
reports_file <- "wash_reports_crosswalked_082420.csv" # output of 02_hh_crosswalk.R; update this file path as needed

## location info
source("FILEPATH/get_location_metadata.R")
locs <- get_location_metadata(location_set_id = 22, gbd_round_id = gbd_round_id)

## microdata ######################################################################
in.dir <- file.path("FILEPATH")
to_add <- list.files(in.dir, pattern = "collapse_wash", full.names = TRUE)
reprepped_collapse <- c("collapse_wash_2020-08-20_1.csv","collapse_wash_2020-08-20_census_1.csv",        # see 8/20/20 comment below; these are all the sources that were re-collapsed
                        "collapse_wash_2020-08-20_census_2.csv","collapse_wash_2020-08-20_census_3.csv", # i am subsetting them out so that `to_add` is all of the original collapses
                        "collapse_wash_2020-08-21.csv","collapse_wash_2020-08-24.csv",                   # will be deleting and replacing with the re-collapsed stuff below
                        "collapse_wash_2020-08-25.csv")
to_add <- to_add[to_add %unlike% paste(reprepped_collapse, collapse = "|")]

# combine all data
wash_data <- rbindlist(lapply(to_add, fread), fill = TRUE)
# [8/20/20] some sources in those collapses were re-prepped and re-collapsed. need to delete the old versions
source_log <- fread("FILEPATH/b_prepped_log.csv")
reprepped <- source_log[prep_date %in% c("6/18/2020","6/19/2020","8/6/2020","8/10/2020") & reprep == 1, file_path]
wash_data <- wash_data[file_path %ni% reprepped]
# and now add the new versions
new_data <- rbindlist(lapply(file.path(in.dir, reprepped_collapse), fread), fill = TRUE)
wash_data <- rbind(wash_data, new_data, fill = TRUE)

# clean up
wash_data <- wash_data[cv_HH == 0] # keep only the individual-level data
setnames(wash_data, "mean", "val")
wash_data[, cv_microdata := 1] # mark these data as microdata

# fix some issues
wash_data <- wash_data[!(nid == 227983 & survey_module == "HH")] # this NID is a HHM module, not sure why the collapse created HH as well
wash_data <- wash_data[!(nid == 25358 & survey_module == "HH")] # there are two sources for this NID - one HH, one HHM. keeping the HHM since we model at the individual level (not household)
wash_data <- unique(wash_data) # NIDs 56420 & 349843 have duplicate rows, don't know why

# manually calculate standard error instead of using ubcov's
# for data points that are NOT 0 or 1, use Wilson score interval
wash_data[, standard_error := (1/(1+(qnorm(0.975)^2/sample_size)))*sqrt(((val*(1-val))/sample_size)+((qnorm(0.975)^2)/(4*sample_size^2)))]
# for data points of 0 or 1, use (upper-mean)/1.96 & (mean-lower)/1.96, respectively
wash_data[, x := as.integer(round(val*sample_size))]
wash_data <- cbind(wash_data, data.table(binom.confint(wash_data$x, wash_data$sample_size, methods = "wilson"))[, .(mean, lower, upper)])
wash_data[abs(1-upper) <= .Machine$double.eps & lower > 0, standard_error := ((mean-lower)/qnorm(0.975))] # data points of 1
wash_data[abs(lower) <= .Machine$double.eps & upper < 1, standard_error := ((upper-mean)/qnorm(0.975))] # data points of 0
# calculate variance
wash_data[, variance := (standard_error*sqrt(sample_size))^2] # SE = SD/sqrt(n), variance = SD^2
# clean up
wash_data[, c("x","mean","lower","upper") := NULL]

# add location info
wash_data <- merge(locs[, .(ihme_loc_id, location_id, location_name, region_name)], wash_data, by = "ihme_loc_id")

# outlier data points where missingness was greater than 15%
wash_data[, id := .GRP, by = c("nid","ihme_loc_id","year_start","survey_name")]
acs_id <- wash_data[survey_name == "USA_ACS", unique(id)] # these were not collapsed with the missing_* vars (too large to collapse - was taking >10 hours & >200G)

for (n in unique(wash_data$id)[unique(wash_data$id) %ni% acs_id]) {
  print(n)
  # water
  if ("missing_w_source_drink" %in% wash_data[id == n, var]) {
    if (wash_data[id == n & var == "missing_w_source_drink", val > 0.15] & wash_data[id == n & var == "missing_w_source_drink", val != 1]) {
      wash_data[id == n & var %in% c("wash_water_piped","wash_water_imp_prop"), is_outlier := 1]
    } 
  }
  # sanitation
  if ("missing_t_type" %in% wash_data[id == n, var]) {
    if (wash_data[id == n & var == "missing_t_type", val > 0.15] & wash_data[id == n & var == "missing_t_type", val != 1]) {
      wash_data[id == n & var %in% c("wash_sanitation_piped","wash_sanitation_imp_prop"), is_outlier := 1]
    }
  }
  # hygiene
  if ("missing_hw_station" %in% wash_data[id == n, var]) {
    if (wash_data[id == n & var == "missing_hw_station", val > 0.15] & wash_data[id == n & var == "missing_hw_station", val != 1]) {
      wash_data[id == n & var == "wash_hwws", is_outlier := 1]
    } 
  }
  # water treatment
  if ("missing_w_treat" %in% wash_data[id == n, var]) {
    if (wash_data[id == n & var == "missing_w_treat", val > 0.15] & wash_data[id == n & var == "missing_w_treat", val != 1]) {
      wash_data[id == n & var %in% c("wash_no_treat","wash_filter_treat_prop"), is_outlier := 1]
    }
  }
}

wash_data[is.na(is_outlier), is_outlier := 0]
wash_data[is_outlier == 1, outlier_reason := "missingness >= 15%"]
wash_data[, id := NULL]

# other outliers
wash_data[survey_name %like% "HITT", `:=` (is_outlier = 1,
                                           outlier_reason = "the only questions relating to water/sanitation that this survey asks are 'do you have hot tap water/cold tap water' and 'do you have a flush toilet'")]
wash_data[survey_name %like% "IPUMS" & var == "wash_sanitation_imp_prop", `:=` (is_outlier = 1, # the only strings that they have for sanitation are either sewer/septic, unimproved, or flush_cw/latrine_cw --> there aren't any strings that would be directly tagged to wash_sanitation_imp_prop
                                                                                outlier_reason = "no strings that would be directly tagged to wash_sanitation_imp_prop")]
wash_data[survey_name %like% "IPUMS" & var == "wash_water_imp_prop", `:=` (is_outlier = 1, # <40% of the IPUMS sources have strings tagged to imp water (most only ask "piped" vs "not piped")
                                                                           outlier_reason = "most IPUMS only ask 'piped' vs 'not piped'")]

# add report data
wash_reports <- fread(file.path("FILEPATH", reports_file))
# some NIDs have both individual-level and household-level data; we want to keep only the individual
# first identify which NIDs
multi_hh <- c()
for (x in unique(wash_reports$nid)) {
  if (length(wash_reports[nid == x, unique(cv_HH)]) > 1) {
    multi_hh <- append(multi_hh, x)
  }
}
# then mark them for deletion & delete
wash_reports[nid %in% multi_hh & cv_HH == 1, keep := 0]
wash_reports[is.na(keep), keep := 1]
wash_reports <- wash_reports[keep == 1][, keep := NULL]
# calculate SE for data points of 0 or 1
wash_reports[, x := as.integer(round(val*sample_size))]
wash_reports <- cbind(wash_reports, data.table(binom.confint(wash_reports$x, wash_reports$sample_size, methods = "wilson"))[, .(mean, lower, upper)])
wash_reports[abs(1-upper) <= .Machine$double.eps & lower > 0, standard_error := ((mean-lower)/qnorm(0.975))] # data points of 1
wash_reports[abs(lower) <= .Machine$double.eps & upper < 1, standard_error := ((upper-mean)/qnorm(0.975))] # data points of 0
# calculate variance
wash_reports[, variance := (standard_error*sqrt(sample_size))^2]
# clean up
wash_reports[, c("x","mean","lower","upper") := NULL]
wash_reports[nid == 99519, nid := 95519] # this NID had a typo
# combine with microdata
wash_data <- rbind(wash_data, wash_reports, fill = TRUE)

# save all data
write.csv(wash_data, "FILEPATH/all_data.csv", row.names = F) # does not include model-specific outliers (done below)

################################################

# bundle stuff
wash_versioning <- data.table(read.xlsx("FILEPATH/versioning.xlsx", sheet = "GBD19"))[me_name %like% "wash"]

# add on all data that was not re-extracted or newly extracted this round
for (me in wash_versioning$me_name) {
  print(me)
  
  assign(me, wash_data[var == me])
  get(me)[, `:=` (sex_id = 3, sex = "Both", year_id = year_start, age_start = 0, age_end = 125, age_group_id = 22, 
                  measure = "proportion")]

  gbd19bv <- paste0(me, "_19")
  best_bv <- wash_versioning[me_name == me, best_bundle_version]
  
  print(gbd19bv)
  print(best_bv)
  
  # get GBD 2019 best bundle version (to add all the data that was not re-extracted or newly extracted this round)
  assign(gbd19bv, get_bundle_version(bundle_version_id = best_bv, fetch = "all"))
  get(gbd19bv)[, c("ihme_loc_id","field_citation_value") := NULL]
  assign(gbd19bv, merge(locs[, .(ihme_loc_id, location_id, location_name, region_name)], get(gbd19bv), by = c("location_name","location_id")))
  # for data points with sample size, manually calculate SE and variance
  get(gbd19bv)[!is.na(sample_size), standard_error := (1/(1+(qnorm(0.975)^2/sample_size)))*sqrt(((val*(1-val))/sample_size)+((qnorm(0.975)^2)/(4*sample_size^2)))]
  get(gbd19bv)[!is.na(sample_size), variance := (standard_error*sqrt(sample_size))^2]
  
  # replace re-extracted data & add new data
  overlap <- intersect(get(gbd19bv)$nid, get(me)$nid) # these are the NIDs that were re-extracted
  assign(gbd19bv, get(gbd19bv)[nid %ni% overlap]) # subset to only NIDs that were NOT re-extracted
  assign(me, rbind(get(gbd19bv), get(me), fill = T)) # add all of those NIDs to the dataset
  if ("Unnamed: 0" %in% names(get(me))) get(me)[, "Unnamed: 0" := NULL]
  if ("fake_data" %in% names(get(me))) get(me)[, fake_data := NULL]
  get(me)[, `:=` (var = me, seq = NA, sex_id = 3, measure_id = 18, unit_value_as_published = as.numeric(1),
                  year_start_orig = year_start, year_end_orig = year_end)][, `:=` (year_start = year_id, year_end = year_id)]
  get(me)[, unit_value_as_published := as.numeric(unit_value_as_published)]
  setcolorder(get(me), c("underlying_nid","nid","file_path","survey_name","survey_module","location_id","ihme_loc_id","location_name","region_name",
                         "year_id","year_start","year_end","sex_id","sex","age_group_id","val","variance"))
  assign(me, get(me)[order(location_id, year_id)])
  
  # offset the data slightly (to avoid values of exactly 0 or 1, which have been causing the model problems)
  get(me)[, val_orig := val]
  get(me)[, val := val + ((0.5-val)*0.01)]

}

## model-specific outliers
wash_sanitation_imp_prop[ihme_loc_id %like% "MEX" & val == 0, is_outlier := 1] # need to look at these more closely... LOTS of 0s
wash_sanitation_imp_prop[nid %in% c(44126,44138), is_outlier := 1] # ALB; these don't have answer options that can be mapped to improved sanitation
wash_sanitation_imp_prop[nid == 18843, is_outlier := 1] # ARM; this seems unreasonably low, especially given that this survey has piped sani at ~92%
wash_sanitation_imp_prop[nid == 627, is_outlier := 1] # DZA; doesn't jibe with other data in this location
wash_sanitation_imp_prop[nid == 12261, is_outlier := 1] # SUR; 0% seems wrong, given other imp sani and piped sani data
wash_sanitation_imp_prop[nid == 229389, is_outlier := 1] # KGZ; this is an HITT survey, which were all outliered this round (see above in "other outliers" section)
wash_sanitation_imp_prop[nid == 12489, is_outlier := 1] # TJK; 0% seems wrong & doesn't jibe with other data
# note about all the IPUMS being outliered below - I only checked the proportions of answers to "toilet" and not "sewage". so, the numbers below ("should be ***") are probably not exact
wash_sanitation_imp_prop[nid %in% c(56532,56538,56577), is_outlier := 1] # URY; these seem to be extraction errors - should be in the 0.8-0.9 range (re-extract next round) [IPUMS]
wash_sanitation_imp_prop[nid %in% c(39376,39380), is_outlier := 1] # IRL; extraction errors - should be ~0.25 (re-extract) [IPUMS]
wash_sanitation_imp_prop[nid == 43738, is_outlier := 1] # DEU; extraction error - should be ~0.55 (re-extract) [IPUMS]
wash_sanitation_imp_prop[nid == 39416, is_outlier := 1] # ISR; only answer options are "have toilet, type not specified" and "no toilet" [IPUMS]
wash_sanitation_imp_prop[nid %in% c(41861,41866), is_outlier := 1] # PRT; extraction errors - should be ~0.5 (re-extract) [IPUMS]
wash_sanitation_imp_prop[nid %in% c(41456,41460), is_outlier := 1] # PRI; extraction errors - should be 0.94 and 0.16 respectively [IPUMS]
wash_sanitation_imp_prop[nid == 282883, is_outlier := 1] # CRI; 0% seems unlikely, and source had no answer options that could be tagged to improved sani
wash_sanitation_imp_prop[nid == 25293, is_outlier := 1] # MEX; no strings that would be directly tagged to improved sani
wash_sanitation_imp_prop[nid == 19035, is_outlier := 1] # BRA; no strings that would be directly tagged to improved sani
wash_sanitation_imp_prop[nid == 150456, is_outlier := 1] # EGY; 0% seems unlikely [limited use, couldn't open file, should take a closer look]
wash_sanitation_imp_prop[nid == 20060, is_outlier := 1] # JOR; extraction error - should be ~0.9 (re-extract) [DHS]
wash_sanitation_imp_prop[nid == 20083, is_outlier := 1] # JOR; need to fix prep script so that wash_sanitation_imp_prop is set to NA for data points tagged to flush_cw
                                                        # but need to adjust so that number of NAs is equal to flush_imp_ratio*nrow(flush_cw)
                                                        # for example, flush_imp_ratio = 0.8 --> want 80% of flush_cw rows to be NA for wash_sanitation_imp_prop (since denominator for wash_sanitation_imp_prop is non-sewer/septic)
wash_sanitation_imp_prop[nid == 416273, is_outlier := 1] # JOR; 0% seems unlikely [limited use, couldn't open file, should take a closer look]
wash_sanitation_imp_prop[nid == 416272, is_outlier := 1] # TUN; 0% seems unlikely [limited use, couldn't open file, should take a closer look]
wash_sanitation_imp_prop[nid == 126396, is_outlier := 1] # PHL (and some of its subnats); no strings that would be directly tagged to improved sani (leads to 0%)
wash_sanitation_imp_prop[nid == 218555, is_outlier := 1] # AGO; no strings that would be directly tagged to improved sani (leads to 0%)
wash_sanitation_imp_prop[nid == 7440 & ihme_loc_id == "KEN_35623", is_outlier := 1] # 100% seems unlikely; also, this NID has a TON of missingness... should consider outliering the whole source (need to re-extract next round to be sure)
wash_sanitation_imp_prop[nid == 21559, is_outlier := 1] # CZE; this is causing trouble elsewhere in the region (leading to peaks in the fit in HUN, POL, SVN)
wash_sanitation_imp_prop <- wash_sanitation_imp_prop[!is.na(variance)] # not an outlier, but there's 1 data point (IND_43924, NID 409021) that I think was mistakenly included in GBD 2019 - the sample size is 0. getting rid of it here

wash_sanitation_piped[nid == 19001, is_outlier := 1] # BOL; no answer option for sewer/septic & this seems unreasonably low
wash_sanitation_piped[nid == 627, is_outlier := 1] # DZA; doesn't jibe with other data in this location
wash_sanitation_piped[nid == 20638, is_outlier := 1] # PER; 0% seems wrong
wash_sanitation_piped[nid == 27599, is_outlier := 1] # SLV; ~0% seems too low
wash_sanitation_piped[nid == 294258, is_outlier := 1] # EGY; only answer options in this survey were "have toilet, type not specified" and "no toilet"
wash_sanitation_piped[nid == 35572, is_outlier := 1] # EGY; only answer options in this survey were "have toilet, type not specified" and "no toilet"
wash_sanitation_piped[nid == 106684, is_outlier := 1] # ZAF subnats; only answer options are "flush toilet" and an empty string... looks like the empty string responses were getting dropped --> ~100% piped sani
wash_sanitation_piped[nid == 95520, is_outlier := 1] # BGD; this is a report that has 27.1% "sanitary" - don't think that really qualifies for our definition of piped sanitation
wash_sanitation_piped <- wash_sanitation_piped[!is.na(variance)] # not an outlier, but there's 1 data point (IND_43924, NID 409021) that I think was mistakenly included in GBD 2019 - the sample size is 0. getting rid of it here

wash_water_imp_prop[ihme_loc_id %like% "BRA" & val < 0.01, is_outlier := 1] # these were giving the model problems
wash_water_imp_prop[nid == 95510, is_outlier := 1] # KGZ; only answer options were 'piped' and 'not piped', resulting in 0% improved sani
wash_water_imp_prop[nid == 20145 & ihme_loc_id == "KEN_35637", is_outlier := 1] # doesn't agree with other data & causing kink in model fit
wash_water_imp_prop[nid == 7440 & ihme_loc_id == "KEN_35625", is_outlier := 1] # doesn't agree with other data & causing kink in model fit
wash_water_imp_prop[nid %in% c(56538,56577), is_outlier := 1] # URY; these are IPUMS - only answer options are "piped inside dwelling", "piped outside dwelling", and "not piped". "not piped" could include several improved sources, so these almost certainly undercount proportion using improved
wash_water_imp_prop[nid %in% c(294502,21872), `:=` (year_id = 2007, year_start = 2007, year_end = 2007)] # not an outlier - just a hacky fix for the strange fits in W Europe 
                                                                                                         # (all locs w/o data are driving upwards to hit the Spain data point and then sharply downwards to hit Greece - per JS's suggestion, changing year_id to the midpoint of those two data points to get rid of the kinks)
wash_water_imp_prop[nid == 298372 & ihme_loc_id == "CHN", is_outlier := 1] # this is a study done only in Yunnan province (shouldn't be tagged to CHN national)

wash_water_piped[nid == 264590, is_outlier := 1] # MEX; doesn't jibe with other data in this location
wash_water_piped[nid == 27770, is_outlier := 1] # SAU; 0% seems wrong
wash_water_piped[nid == 8819, is_outlier := 1] # MNE; 0% seems wrong (survey didn't have an answer option for piped into dwelling, so everything got categorized as imp)
wash_water_piped[nid == 11551, is_outlier := 1] # SRB; 0% seems wrong (survey didn't have an answer option for piped into dwelling, so everything got categorized as imp)
wash_water_piped[nid %in% c(19728,95440), is_outlier := 1] # HND; doesn't agree with other data & JMP has HND at 90.2% piped in 2017
wash_water_piped[nid == 161587, is_outlier := 1] # PAN; unreasonably low
wash_water_piped[nid %in% c(40897,40902,10277), is_outlier := 0] # PAN; don't know why these were outliered before
wash_water_piped[nid %in% c(19359,3100), is_outlier := 1] # COL; unreasonably low
wash_water_piped[nid == 416273, is_outlier := 1] # JOR; 0% seems wrong
wash_water_piped[nid == 7761, is_outlier := 0] # LBY; not sure why this was outliered
wash_water_piped[nid == 46480, is_outlier := 1] # NPL; 0% seems unlikely
wash_water_piped[nid == 46837, is_outlier := 1] # ECU; doesn't agree with other data and causing a small kink in the fit
wash_water_piped[nid == 21421 & ihme_loc_id == "PHL_53586", is_outlier := 1] # 100% seems unlikely given this is a small fishing island
wash_water_piped[nid == 39481, is_outlier := 1] # KEN; small variance is causing the model to shift upwards towards this point, which has downstream effects on subnational fits as well - need to re-extract next cycle
wash_water_piped[nid == 20145 & ihme_loc_id == "KEN_35625", is_outlier := 1] # 0% seems unlikely and is also causing the model to behave weirdly to hit this point
wash_water_piped[nid == 7440 & ihme_loc_id == "KEN_35623", is_outlier := 1] # seems too high
wash_water_piped[nid == 39466, is_outlier := 1] # KGZ; causing a weird dip in the fit & also looks like an extraction error (should be 0.30... which also seems too low tbh)
wash_water_piped[nid == 44861, is_outlier := 1] # LBN; causing a weird dip & also JMP has them at ~84% piped in 2005
wash_water_piped[nid == 7387 & ihme_loc_id == "KEN_44797", is_outlier := 1] # causing the trend to shoot sharply upwards towards this point and then back down... affecting some other subnats as well
wash_water_piped[nid %in% c(43526,43552,30235), is_outlier := 1] # IDN; extraction errors - should be ~0.15 (need to re-extract)

wash_no_treat[nid == 56153, is_outlier := 1] # SRB; extraction error - should be ~0.9

# upload data, save BV, and save CWV
clear_bundle <- F # if you want to reset whatever is in the bundle and upload new data
bv_table <- data.table(request_id = NA, bundle_version_id = NA, previous_step_bundle_version_id = NA, request_status = NA, me_name = NA)[-1]
cwv_table <- data.table(request_id = NA, crosswalk_version_id = NA, previous_step_crosswalk_version_id = NA, request_status = NA, me_name = NA)[-1]
cwv_description <- "offset all values slightly to avoid values of exactly 0 or 1 (which were causing issues in the model)"

for (me in wash_versioning$me_name[3]) {
  print(me)
  
  # save
  filepath <- paste0("FILEPATH/", me, "_iterative.xlsx")
  write.xlsx(get(me), filepath, sheetName = "extraction")
  
  # clear existing bundle
  if (clear_bundle == TRUE) {
    print("getting bundle data...")
    bundle <- get_bundle_data(bundle_id = wash_versioning[me_name == me, bundle_id], decomp_step = "iterative", gbd_round_id = 7)
    clear <- data.table(seq = bundle$seq, nid = NA, underlying_nid = NA, location_id = NA, sex = NA, measure = NA, year_id = NA, year_start = NA, year_end = NA, 
                               age_group_id = NA, age_start = NA, age_end = NA, is_outlier = NA, val = NA, sample_size = NA, variance = NA)
    write.xlsx(clear, "FILEPATH/clear_bundle.xlsx", sheetName = "extraction")
    print("clearing bundle data...")
    upload_bundle_data(bundle_id = wash_versioning[me_name == me, bundle_id], decomp_step = "iterative", gbd_round_id = 7,
                       filepath = "FILEPATH/clear_bundle.xlsx")
  }
  
  # upload
  print("uploading...")
  upload_bundle_data(bundle_id = wash_versioning[me_name == me, bundle_id], decomp_step = "iterative", gbd_round_id = 7, 
                     filepath = filepath)
  
  # save bv
  print("saving bv...")
  bv_info <- save_bundle_version(bundle_id = wash_versioning[me_name == me, bundle_id], decomp_step = "iterative", gbd_round_id = 7, include_clinical = NULL)
  bv_info[, me_name := me]
  bv_table <- rbind(bv_table, bv_info)
  bv_id <- bv_info$bundle_version_id
  
  # save cwv
  print("getting bv...")
  bv <- get_bundle_version(bundle_version_id = bv_id, fetch = "all")
  bv[, `:=` (unit_value_as_published = 1, crosswalk_parent_seq = NA)]
  filepath2 <- paste0("FILEPATH/", me, "_bv_", bv_id, ".xlsx")
  write.xlsx(bv, filepath2, sheetName = "extraction")
  print("saving cwv...")
  cwv_info <- save_crosswalk_version(bundle_version_id = bv_id, data_filepath = filepath2, description = cwv_description)
  cwv_info[, me_name := me]
  cwv_table <- rbind(cwv_table, cwv_info)
  
  print(paste(me, "done!"))
}

# fecal_prop (no new data in GBD 2020)
fecal_prop_19 <- get_bundle_version(bundle_version_id = 4451, fetch = "all")
fecal_prop_19 <- merge(locs[, .(ihme_loc_id, location_id, location_name, region_name)], fecal_prop_19, by = c("location_name","location_id"))
fecal_prop_19[, seq := NA]

write.xlsx(fecal_prop_19, "FILEPATH/fecal_prop_iterative.xlsx", sheetName = "extraction")

upload_bundle_data(bundle_id = 6050, decomp_step = "iterative", gbd_round_id = 7, filepath = "FILEPATH/fecal_prop_iterative.xlsx")

save_bundle_version(bundle_id = 6050, decomp_step = "iterative", gbd_round_id = 7, include_clinical = NULL)

bv <- get_bundle_version(bundle_version_id = 26114, fetch = "all")
bv[, unit_value_as_published := as.numeric(unit_value_as_published)]
bv[, `:=` (unit_value_as_published = as.numeric(1), crosswalk_parent_seq = NA)]
write.xlsx(bv, "FILEPATH/fecal_prop_bv_26114.xlsx", sheetName = "extraction")

save_crosswalk_version(bundle_version_id = 26114, data_filepath = "FILEPATH/fecal_prop_bv_26114.xlsx",
                       description = "GBD 2020 first upload; added new data & re-extracted lots of old data")

# cv_piped (do this after running wash_water_piped ST-GPR model)
source("FILEPATH/utility.r")
cv_piped <- model_load(162674, "raked") # update with best wash_water_piped run_id
cv_piped[, c("gpr_lower","gpr_upper") := NULL]
setnames(cv_piped, "gpr_mean", "cv_piped")

cv_piped_dir <- "FILEPATH"
write.csv(cv_piped, file.path(cv_piped_dir,"cv_piped.csv"), row.names = F)
