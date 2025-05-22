
### Purpose: add newly extracted, prepped, and collapsed WaSH exposure data
###########################################################################

# CONFIG #####################################################################
rm(list=ls())

## libraries
library(data.table)
library(magrittr)
library(openxlsx)
library(binom)
library(writexl)
library(xlsx)
library(readr)


## functions
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/get_location_metadata.R")
"%unlike%" <- Negate("%like%")
"%ni%" <- Negate("%in%")

## settings
gbd_cycle <- "GBD2022" #used for filepaths
loc_set<-22
release<-16 #release id, GBD2023
year<-2024

sewer_compile<-F #are we compiling only sewer data? This is for safely managed sanitation
imp_compile<-F #are we compiling on improved sanitation data? This is for safely managed sanitation
all_compile<-T #are we compiling all of wash microdatata (not subsetted). This is for the normal wash 


## location info
locs <- get_location_metadata(location_set_id = loc_set, release_id = release)

## microdata ######################################################################
# Determine input directory
## sewer and imp
in.dir<-paste0("FILEPATH",gbd_cycle,"FILEPATH")


# read in the files
## sewer
if(sewer_compile==T){to_add<-list.files(in.dir,pattern = "sewer", full.names = TRUE)}

## imp
if(imp_compile==T){to_add<-list.files(in.dir,pattern = "imp", full.names = TRUE)}

## all
if(all_compile==T){to_add<-list.files(in.dir,pattern = "collapse_wash", full.names = TRUE)
  to_add<-to_add[!grepl("imp|sewer",to_add)]
}

to_add<-c("FILEPATH/input_collapse_wash_2022-06-09.csv",
          list.files("FILEPATH",pattern=".csv",full.names = T),
          list.files("FILEPATH",pattern=".csv",full.names = T))
to_add<-to_add[!grepl("imp|sewer|05-03",to_add)]

to_add<-"FILEPATH/collapse_wash_2024-07-24_lu.csv"

# combine all data #############################################################################
wash_data <- rbindlist(lapply(to_add, fread), fill = TRUE)

#remove these nids as they were re-extracted in 05-03, then read in 05-03
nid_rm<-c(527435,529982,539851,527453,528571,466384,541281,538795,539788,466891,437993,527726,457894,546694,
          453328, 453334, 413741, 413663, 459477, 459500, 462027, 461987)

wash_data<-wash_data[!(nid %in% nid_rm),]

# clean up
wash_data <- wash_data[cv_HH == 0] # keep only the individual-level data
setnames(wash_data, "mean", "val")
wash_data[, cv_microdata := 1] # mark these data as microdata

#some of the ihme_loc_ids have a \r so we need to remove these
wash_data[, ihme_loc_id := sub("\r$", "", ihme_loc_id)]

# fix some issues
wash_data <- wash_data[!(nid == 227983 & survey_module == "HH")] # this NID is a HHM module
wash_data <- wash_data[!(nid == 25358 & survey_module == "HH")] # there are two sources for this NID - one HH, one HHM. keeping the HHM since we model at the individual level (not household)
wash_data <- unique(wash_data) # NIDs 56420 & 349843 have duplicate rows

# Standard Error/ Variance ##################################################################################
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
#no longer need upper and lower as those are only used to calculate the standard error
wash_data[, c("x","mean","lower","upper") := NULL]

# add location info
wash_data <- merge(locs[, .(ihme_loc_id, location_id, location_name, region_name)], wash_data, by = "ihme_loc_id")

# Outliers ############################################################################
# outlier data points where missingness was greater than 15%
wash_data[, id := .GRP, by = c("nid","ihme_loc_id","year_start","survey_name")]
acs_id <- wash_data[survey_name == "USA_ACS", unique(id)] # these were not collapsed with the missing_* vars (too large to collapse - was taking >10 hours & >200G)

#sewer and imp, since we are only looking at sanitation, we do not care about the other indicators
# for the normal wash model we outlier sewer and imp sanitation if the missing_t_type is >0.15.
# But due to the lack of data, we will be changing it to exclude surveys that have missing_t_type is >0.25.
if(sewer_compile==T | imp_compile==T) {
  for (n in unique(wash_data$id)[unique(wash_data$id) %ni% acs_id]) {
    print(n)

    # sanitation
    if ("missing_t_type" %in% wash_data[id == n, var]) {
      if (wash_data[id == n & var == "missing_t_type", val > 0.25] & wash_data[id == n & var == "missing_t_type", val != 1]) {
        wash_data[id == n & var %in% c("safely_managed"), is_outlier := 1]
        wash_data[id == n & var %in% c("safely_managed"), outlier_reason := "missing_t_type > 0.25"]
      }
    }
  }
}

# for the normal wash compile
if(all_compile==T){
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
}

# Clean up
wash_data[is.na(is_outlier), is_outlier := 0]
if(sewer_compile==T | imp_compile==T) {wash_data[is_outlier == 1, outlier_reason := "missingness >= 25%"]}
if(all_compile==T){wash_data[is_outlier == 1, outlier_reason := "missingness >= 15%"]}
wash_data[, id := NULL]

# other outliers
wash_data[survey_name %like% "HITT", `:=` (is_outlier = 1,
                                           outlier_reason = "the only questions relating to water/sanitation that this survey asks are 'do you have hot tap water/cold tap water' and 'do you have a flush toilet'")]
wash_data[survey_name %like% "IPUMS" & var == "wash_sanitation_imp_prop", `:=` (is_outlier = 1, # the only strings that they have for sanitation are either sewer/septic, unimproved, or flush_cw/latrine_cw --> there aren't any strings that would be directly tagged to wash_sanitation_imp_prop
                                                                                outlier_reason = "no strings that would be directly tagged to wash_sanitation_imp_prop")]
wash_data[survey_name %like% "IPUMS" & var == "wash_water_imp_prop", `:=` (is_outlier = 1, # <40% of the IPUMS sources have strings tagged to imp water (most only ask "piped" vs "not piped")
                                                                           outlier_reason = "most IPUMS only ask 'piped' vs 'not piped'")]


# add report data #################################################################################
if(all_compile==T){


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
}

# Outlier duplicate extractions ######################################
#there are some surveys that were done in waves and an NID was assigned to each wave. We only want to keep the latest wave, and need to remove the other
#waves as it'll be duplicate people
dup_nids<-c(531642, 531644, 535930, 536347, 482750, 530981, 531679)

wash_data[nid %in% dup_nids,':='(is_outlier=1,outlier_reason="duplicate data")]
wash_data[nid==531679,':='(is_outlier=1,outlier_reason="survey keeps erroring out in prep code, need to take closer look")]
wash_data[nid==530981,':='(is_outlier=1,outlier_reason="should have an encoding error to prevent processing, but got through anyways")]

# Remove unnecessary columns ###################
wash_data<-wash_data[,-c("mean","source_type")]

# Save CSV #########################

if(sewer_compile==T){
  write.csv(wash_data[var=="safely_managed"],paste0("FILEPATH",year,"/data/sewer/survey_data/sewer_survey_compiled_new.csv"), row.names = F)
}

if(imp_compile==T){
  write.csv(wash_data[var=="safely_managed"],paste0("FILEPATH",year,"/data/imp/survey_data/microdata_compiled_imp_new.csv"), row.names = F)
}
  
################################################
# Prep for bundle #############################
# bundle stuff
wash_versioning <- data.table(openxlsx::read.xlsx("/mnt/share/homes/sandrasp/tracking/versioning.xlsx", sheet = "GBD21"))[me_name %like% "wash"]

if(all_compile==T){
  wash_versioning<-wash_versioning[!(me_name %like% "sm")]
}

# add on all data that was not re-extracted or newly extracted this round
for (me in wash_versioning$me_name) {
  print(me)
  assign(me, wash_data[var == me])
  
  if(me=="wash_sm_imp" | me=="wash_sm_sewer"){
    # me<-"wash_sm_sewer"
    assign(me,wash_data[var=="safely_managed"])
  }

  get(me)[, `:=` (sex_id = 3, sex = "Both", year_id = year_end, age_start = 0, age_end = 125, age_group_id = 22,
                  measure = "proportion")]

  gbd20bv <- paste0(me, "_20")
  
  #we only upload safely managed improved this way. We do not upload sewer this way. See the safely managed sewer compile code
  if(me=="wash_sm_imp"){
    best_bv <- wash_versioning[me_name == me & Notes==2023, best_bundle_version]
  }
  
  if(me!="wash_sm_imp"){
    best_bv <- wash_versioning[me_name == me, best_bundle_version]
  }
  

  print(gbd20bv)
  print(best_bv)

  # get GBD 2019 best bundle version (to add all the data that was not re-extracted or newly extracted this round)
  assign(gbd20bv, get_bundle_version(bundle_version_id = best_bv, fetch = "all"))
  get(gbd20bv)[, c("ihme_loc_id","field_citation_value") := NULL]
  
  #This line of code is dropping rows, because not all of the rows have region names
  #assign(gbd20bv, merge(locs[, .(ihme_loc_id, location_id, location_name, region_name)], get(gbd20bv), by = c("location_name","location_id","region_name")))
  
  #To fix this I am going to only merge on location_id instead
  assign(gbd20bv, merge(locs[, .(ihme_loc_id, location_id, location_name, region_name)], get(gbd20bv), by = c("location_id"),all.y=T))
  
  #This will lead to .x and .y columns for location_name and region_name. .x is the GBD locations, .y are the original old bundle
  #We only want to keep the .x columns
  #Delete the .y columns
  get(gbd20bv)[,c("location_name.y","region_name.y"):=NULL]
  
  #Remove the .x part of the column
  setnames(get(gbd20bv),"location_name.x","location_name")
  setnames(get(gbd20bv),"region_name.x","region_name")
  
  # for data points with sample size, manually calculate SE and variance
  get(gbd20bv)[!is.na(sample_size), standard_error := (1/(1+(qnorm(0.975)^2/sample_size)))*sqrt(((val*(1-val))/sample_size)+((qnorm(0.975)^2)/(4*sample_size^2)))]
  get(gbd20bv)[!is.na(sample_size), variance := (standard_error*sqrt(sample_size))^2]

  # replace re-extracted data & add new data
  overlap <- intersect(get(gbd20bv)$nid, get(me)$nid) # these are the NIDs that were re-extracted
  #we may need to change this to include year and/or location
  
  assign(gbd20bv, get(gbd20bv)[nid %ni% overlap]) # subset to only NIDs that were NOT re-extracted
  
  #rbind the new and old since we are going to clear the bundle each time
  # if(length(overlap)!=0){
  assign(me, rbind(get(gbd20bv), get(me), fill = T)) # add all of those NIDs to the dataset
  # }
  
  #remove some columns
  if ("Unnamed: 0" %in% names(get(me))) get(me)[, "Unnamed: 0" := NULL]
  if ("fake_data" %in% names(get(me))) get(me)[, fake_data := NULL]
  
  #Add new necessary columns
  get(me)[, `:=` (var = me, seq = NA, sex_id = 3, measure_id = 18, unit_value_as_published = as.numeric(1),
                  year_start_orig = year_start, year_end_orig = year_end)][, `:=` (year_start = year_id, year_end = year_id)]
  get(me)[, unit_value_as_published := as.numeric(unit_value_as_published)]
  get(me)[!"underlying_nid" %in% names(get(me)),underlying_nid:=NA]
  
  #I don't think this is necessary
  #Order the columns
  # setcolorder(get(me), c("underlying_nid","nid","file_path","survey_name","survey_module","location_id","ihme_loc_id","location_name","region_name",
  #                        "year_id","year_start","year_end","sex_id","sex","age_group_id","val","variance"))
  # assign(me, get(me)[order(location_id, year_id)])

  # offset the data slightly (to avoid values of exactly 0 or 1, which have been causing the model problems)- this is only needed for non-safely managed 
  #sanitation risk factors
  if(all_compile==T){
  get(me)[, val_orig := val]
  get(me)[val_orig==1,val==0.9999]
  get(me)[val_orig==0, val := val + ((0.5-val)*0.01)]
  }
  
  #write the safely managed sewer and improved microdata to file so we can add the non-survey data to it and then to the bundle
  if(me=="wash_sm_imp"){
    write_excel_csv(get(me),paste0("FILEPATH",year,"FILEPATH/microdata_compiled_imp.csv"))
  }
  
  if(me=="wash_sm_sewer"){
    write_excel_csv(get(me),paste0("FILEPATH",year,"FILEPATH/microdata_compiled_sewer.csv"))
  }

}

# Outliers ######################################
## sanitaiton_imp_prop ################
## model-specific outliers
wash_sanitation_imp_prop[ihme_loc_id %like% "MEX" & val == 0, is_outlier := 1] # need to look at these more closely... LOTS of 0s
wash_sanitation_imp_prop[nid %in% c(44126,44138), is_outlier := 1] # ALB; these don't have answer options that can be mapped to improved sanitation
wash_sanitation_imp_prop[nid == 18843, is_outlier := 1] 
wash_sanitation_imp_prop[nid == 627, is_outlier := 1] 
wash_sanitation_imp_prop[nid == 12261, is_outlier := 1] 
wash_sanitation_imp_prop[nid == 229389, is_outlier := 1] # KGZ; this is an HITT survey, which were all outliered this round (see above in "other outliers" section)
wash_sanitation_imp_prop[nid == 12489, is_outlier := 1] 

# note about all the IPUMS being outliered below - I only checked the proportions of answers to "toilet" and not "sewage". so, the numbers below ("should be ***") are probably not exact
wash_sanitation_imp_prop[nid %in% c(56532,56538,56577), is_outlier := 1] # URY; these seem to be extraction errors - should be in the 0.8-0.9 range (re-extract next round) [IPUMS]
wash_sanitation_imp_prop[nid %in% c(39376,39380), is_outlier := 1] # IRL; extraction errors - should be ~0.25 (re-extract) [IPUMS]
wash_sanitation_imp_prop[nid == 43738, is_outlier := 1] # DEU; extraction error - should be ~0.55 (re-extract) [IPUMS]
wash_sanitation_imp_prop[nid == 39416, is_outlier := 1] # ISR; only answer options are "have toilet, type not specified" and "no toilet" [IPUMS]
wash_sanitation_imp_prop[nid %in% c(41861,41866), is_outlier := 1] # PRT; extraction errors - should be ~0.5 (re-extract) [IPUMS]
wash_sanitation_imp_prop[nid %in% c(41456,41460), is_outlier := 1] # PRI; extraction errors - should be 0.94 and 0.16 respectively [IPUMS]
wash_sanitation_imp_prop[nid == 282883, is_outlier := 1] 
wash_sanitation_imp_prop[nid == 25293, is_outlier := 1] # MEX; no strings that would be directly tagged to improved sani
wash_sanitation_imp_prop[nid == 19035, is_outlier := 1] # BRA; no strings that would be directly tagged to improved sani
wash_sanitation_imp_prop[nid == 150456, is_outlier := 1] # EGY; 0% seems unlikely [limited use, couldn't open file, should take a closer look]
wash_sanitation_imp_prop[nid == 20060, is_outlier := 1] # JOR; extraction error - should be ~0.9 (re-extract) [DHS]
wash_sanitation_imp_prop[nid == 20083, is_outlier := 1] # JOR; need to fix prep script so that wash_sanitation_imp_prop is set to NA for data points tagged to flush_cw
                                                        # but need to adjust so that number of NAs is equal to flush_imp_ratio*nrow(flush_cw)
                                                        # for example, flush_imp_ratio = 0.8 --> want 80% of flush_cw rows to be NA for wash_sanitation_imp_prop (since denominator for wash_sanitation_imp_prop is non-sewer/septic)
wash_sanitation_imp_prop[nid == 416273, is_outlier := 1] 
wash_sanitation_imp_prop[nid == 416272, is_outlier := 1] 
wash_sanitation_imp_prop[nid == 126396, is_outlier := 1] # PHL (and some of its subnats); no strings that would be directly tagged to improved sani (leads to 0%)
wash_sanitation_imp_prop[nid == 218555, is_outlier := 1] # AGO; no strings that would be directly tagged to improved sani (leads to 0%)
wash_sanitation_imp_prop[nid == 7440 & ihme_loc_id == "KEN_35623", is_outlier := 1] 
wash_sanitation_imp_prop[nid == 21559, is_outlier := 1] 
wash_sanitation_imp_prop <- wash_sanitation_imp_prop[!is.na(variance)]

## sanitation_piped #################
wash_sanitation_piped[nid == 19001, is_outlier := 1]
wash_sanitation_piped[nid == 627, is_outlier := 1]
wash_sanitation_piped[nid == 20638, is_outlier := 1]
wash_sanitation_piped[nid == 27599, is_outlier := 1] 
wash_sanitation_piped[nid == 294258, is_outlier := 1] # EGY; only answer options in this survey were "have toilet, type not specified" and "no toilet"
wash_sanitation_piped[nid == 35572, is_outlier := 1] # EGY; only answer options in this survey were "have toilet, type not specified" and "no toilet"
wash_sanitation_piped[nid == 106684, is_outlier := 1] # ZAF subnats; only answer options are "flush toilet" and an empty string
wash_sanitation_piped[nid == 95520, is_outlier := 1] 
wash_sanitation_piped <- wash_sanitation_piped[!is.na(variance)] 

## water_imp #####################
wash_water_imp_prop[ihme_loc_id %like% "BRA" & val < 0.01, is_outlier := 1] 
wash_water_imp_prop[nid == 95510, is_outlier := 1] # KGZ; only answer options were 'piped' and 'not piped', resulting in 0% improved sani
wash_water_imp_prop[nid == 20145 & ihme_loc_id == "KEN_35637", is_outlier := 1]
wash_water_imp_prop[nid == 7440 & ihme_loc_id == "KEN_35625", is_outlier := 1] 
wash_water_imp_prop[nid %in% c(56538,56577), is_outlier := 1] # URY; these are IPUMS - only answer options are "piped inside dwelling", "piped outside dwelling", and "not piped". "not piped" could include several improved sources, so these almost certainly undercount proportion using improved
wash_water_imp_prop[nid %in% c(294502,21872), `:=` (year_id = 2007, year_start = 2007, year_end = 2007)] 
wash_water_imp_prop[nid == 298372 & ihme_loc_id == "CHN", is_outlier := 1] # this is a study done only in Yunnan province (shouldn't be tagged to CHN national)

## water_piped ####################
wash_water_piped[nid == 264590, is_outlier := 1] 
wash_water_piped[nid == 27770, is_outlier := 1] 
wash_water_piped[nid == 8819, is_outlier := 1] 
wash_water_piped[nid == 11551, is_outlier := 1]
wash_water_piped[nid %in% c(19728,95440), is_outlier := 1] 
wash_water_piped[nid == 161587, is_outlier := 1] 
wash_water_piped[nid %in% c(40897,40902,10277), is_outlier := 0] 
wash_water_piped[nid %in% c(19359,3100), is_outlier := 1] 
wash_water_piped[nid == 416273, is_outlier := 1] 
wash_water_piped[nid == 7761, is_outlier := 0] 
wash_water_piped[nid == 46480, is_outlier := 1] 
wash_water_piped[nid == 46837, is_outlier := 1] 
wash_water_piped[nid == 21421 & ihme_loc_id == "PHL_53586", is_outlier := 1]
wash_water_piped[nid == 39481, is_outlier := 1] 
wash_water_piped[nid == 20145 & ihme_loc_id == "KEN_35625", is_outlier := 1] 
wash_water_piped[nid == 7440 & ihme_loc_id == "KEN_35623", is_outlier := 1]
wash_water_piped[nid == 39466, is_outlier := 1] 
wash_water_piped[nid == 44861, is_outlier := 1]
wash_water_piped[nid == 7387 & ihme_loc_id == "KEN_44797", is_outlier := 1] 
wash_water_piped[nid %in% c(43526,43552,30235), is_outlier := 1] # IDN; extraction errors - should be ~0.15 (need to re-extract)

## no_treat ###############
wash_no_treat[nid == 56153, is_outlier := 1] # SRB; extraction error - should be ~0.9

# upload data, save BV, and save CWV ######################################################################

#or use the epi uploader
#save each file
write_xlsx(list(extraction=wash_water_piped),path=paste0("FILEPATH",gbd_cycle,"FILEPATH/wash_water_piped.xlsx"))
write_xlsx(list(extraction=wash_water_imp_prop),path=paste0("FILEPATH",gbd_cycle,"FILEPATH/wash_water_imp_prop.xlsx"))
write_xlsx(list(extraction=wash_no_treat),path=paste0("FILEPATH",gbd_cycle,"FILEPATH/wash_no_treat.xlsx"))
write_xlsx(list(extraction=wash_filter_treat_prop),path=paste0("FILEPATH",gbd_cycle,"FILEPATH/wash_filter_treat_prop.xlsx"))
write_xlsx(list(extraction=wash_sanitation_piped),path=paste0("FILEPATH",gbd_cycle,"FILEPATH/wash_sanitation_piped.xlsx"))
write_xlsx(list(extraction=wash_sanitation_imp_prop),path=paste0("FILEPATH",gbd_cycle,"FILEPATH/wash_sanitation_imp_prop.xlsx"))
write_xlsx(list(extraction=wash_hwws),path=paste0("FILEPATH",gbd_cycle,"FILEPATH/wash_hwws_PMA.xlsx"))

clear_bundle <- T # if you want to reset whatever is in the bundle and upload new data
bv_table <- data.table(request_id = NA, bundle_version_id = NA, request_status = NA, me_name = NA)[-1]
cwv_table <- data.table(request_id = NA, crosswalk_version_id = NA, request_status = NA, me_name = NA)[-1]
cwv_description <- "Uploading newest data for GBD2023"

for (me in wash_versioning$me_name) {
  print(me)

  # save
  filepath<-paste0("FILEPATH",gbd_cycle,"FILEPATH/",me,".xlsx")
  table<-get(me)
  write_xlsx(list(extraction=table),path=filepath) #extraction is the sheet name, table is the datatable name in R
  

  # clear existing bundle
  if (clear_bundle == TRUE) {
    print("getting bundle data...")
    bundle <- get_bundle_data(bundle_id = unique(wash_versioning[me_name == me, bundle_id]))
    clear <- data.table(seq = bundle$seq, nid = NA, underlying_nid = NA, location_id = NA, sex = NA, measure = NA, year_id = NA, year_start = NA, year_end = NA,
                               age_group_id = NA, age_start = NA, age_end = NA, is_outlier = NA, val = NA, sample_size = NA, variance = NA)
    write.xlsx(clear, "FILEPATH/clear_bundle.xlsx", sheetName = "extraction")
    print("clearing bundle data...")
    upload_bundle_data(bundle_id = unique(wash_versioning[me_name == me, bundle_id]),
                       filepath = "FILEPATH/clear_bundle.xlsx")
  }

  # upload
  print("uploading...")
  upload_bundle_data(bundle_id = unique(wash_versioning[me_name == me, bundle_id]), filepath = filepath)

  # save bv
  print("saving bv...")
  bv_info <- save_bundle_version(bundle_id = unique(wash_versioning[me_name == me, bundle_id]), include_clinical = NULL)
  bv_info[, me_name := me]
  bv_table <- rbind(bv_table, bv_info)
  bv_id <- bv_info$bundle_version_id

  # save cwv
  print("getting bv...")
  bv <- get_bundle_version(bundle_version_id = bv_id, fetch = "all")
  bv[, `:=` (unit_value_as_published = 1, crosswalk_parent_seq = NA)]
  filepath2 <- paste0("FILEPATH/",gbd_cycle,"/FILEPATH/", me, "_bv_", bv_id, ".xlsx")
  write_xlsx(list(extraction=bv),path=filepath2)
  

  # Since safely managed imp is a custom bundle, it can't have a crosswalk version. So it will error out here, which is to be expected
  print("saving cwv...")
  cwv_info <- save_crosswalk_version(bundle_version_id = bv_id, data_filepath = filepath2, description = cwv_description)
  cwv_info[, me_name := me]
  cwv_table <- rbind(cwv_table, cwv_info)

  print(paste(me, "done!"))
}




# Extras ##########################################
# fecal_prop (no new data in GBD 2020)
fecal_prop_19 <- get_bundle_version(bundle_version_id = 4451, fetch = "all")
fecal_prop_19 <- merge(locs[, .(ihme_loc_id, location_id, location_name, region_name)], fecal_prop_19, by = c("location_name","location_id"))
fecal_prop_19[, seq := NA]

write.xlsx(fecal_prop_19, paste0("/mnt/share/scratch/users/sandrasp/wash/",gbd_cycle,"/bundles/fecal_prop_iterative.xlsx"), sheetName = "extraction")

upload_bundle_data(bundle_id = 6050, decomp_step = "iterative", gbd_round_id = 7, filepath = paste0("/mnt/share/scratch/users/sandrasp/wash/",gbd_cycle,"/bundles/fecal_prop_iterative.xlsx"))

save_bundle_version(bundle_id = 6050, decomp_step = "iterative", gbd_round_id = 7, include_clinical = NULL)

bv <- get_bundle_version(bundle_version_id = 26114, fetch = "all")
bv[, unit_value_as_published := as.numeric(unit_value_as_published)]
bv[, `:=` (unit_value_as_published = as.numeric(1), crosswalk_parent_seq = NA)]
write.xlsx(bv, paste0("/mnt/share/scratch/users/sandrasp/wash/",gbd_cycle,"/bundles/fecal_prop_bv_26114.xlsx"), sheetName = "extraction")

save_crosswalk_version(bundle_version_id = 26114, data_filepath = paste0("/mnt/share/scratch/users/sandrasp/wash/",gbd_cycle,"/bundles/fecal_prop_bv_26114.xlsx"),
                       description = "GBD 2020 first upload; added new data & re-extracted lots of old data")

# cv_piped (do this after running wash_water_piped ST-GPR model) #################################################################
source("/ihme/code/st_gpr/central/stgpr/r_functions/utilities/utility.r")
cv_piped <- model_load(162674, "raked") # update with best wash_water_piped run_id
cv_piped[, c("gpr_lower","gpr_upper") := NULL]
setnames(cv_piped, "gpr_mean", "cv_piped")

#cv_piped_dir <- "/ihme/scratch/users/jz1/wash/GBD20"
cv_piped_dir <- paste0("/mnt/share/scratch/users/sandrasp/wash/",gbd_cycle)
write.csv(cv_piped, file.path(cv_piped_dir,"cv_piped.csv"), row.names = F)
