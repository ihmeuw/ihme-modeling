################################################################################
## DESCRIPTION: Apply self-report adjustment to individual-level BMI data
## INPUTS: Cleaned individual-level data
## OUTPUTS: Adjusted individual-level data
## AUTHOR: 
## DATE:
################################################################################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir,"FILEPATH"))
source("FILEPATH/stgpr/api/public.R")
library(bit64)
library(tidyverse)
library(zoo)
library(plyr)
library(tidyr)

## Load configuration for meta data variables
load_config(paste0(code_dir, 'FILEPATH'), c('gbd', 'qsub', 'bmi'))

# Set arguments
args <- commandArgs(trailingOnly = TRUE)
data_dir <- args[1]
new_xwalk <- args[2] ## If there is a new crosswalk, then all cleaned data needs to be recalculated. If not, then only the new paths need to be loaded

## Should USA data sources be excluded to save time
exclude_usa <- TRUE

## Filepaths to save data
nlu_save_path <- "FILEPATH"
lu_save_path <- "FILEPATH"

## Helper datasets
ages <- get_age_metadata(age_group_set_id=age_group_set_id, release_id=release_id)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
ages$age_start <- round(ages$age_start)
ages$age_end <- round(ages$age_end)
locs <- get_location_metadata(location_set_id, release_id=release_id)
locs[, country_id := as.integer(as.factor(substr(ihme_loc_id, 1, 3)))]
locs[, country_name := substr(ihme_loc_id, 1, 3)]
usa_locs <- locs[location_id %in% c(102,385, 422, 298, 351, 376) | parent_id==102] 

# Read in child cutoffs (months only)
cutoffs <- fread("FILEPATH")

# Read in data tracker sheet
data_tracker_mean_bmi <- fread("FILEPATH/mean_bmi_data_tracker.csv")
data_tracker <- fread("FILEPATH/data_tracker.csv")

## Read in hard coded crosswalk year ranges
xwalk_years <- fread("FILEPATH/mean_bmi_crosswalk_years.csv")

##update for BMI categories
tracker <- subset(data_tracker_mean_bmi, !measure %in% c("mean_bmi_selfreported" ,"mean_bmi_measured"))
tracker <- tail(tracker, 30) ## select last 30 (newest) entries

tracker[, name := "stgpr_meas"][grepl("selfreported", measure), name := "stgpr_sr"]
tracker[, meas := gsub(".*_","",measure)]
tracker[, level := "country"][grepl("region", note), level := "region"][grepl("super", note), level := "super-region"]
tracker <- tracker[, c("bundle_id","bundle_version", "crosswalk_version_id", "stgpr_run_id","name","meas","level")]

message(print("Loading ratio information"))

cw_levels <- list.files("FILEPATH", pattern="specific_list", full.names = T)
levels <- data.table(name = c("severeunderweight","underweight","normalweight","overweight","obese"),
                     short = c("suw", "uw", "nm", "ow","ob"),
                     long = c("Severely underweight","Underweight","Normal","Overweight","Obese"))


for(r in c("severeunderweight","underweight","normalweight","overweight","obese")){
  track <- tracker[meas==r]
  run_ids <- track[, c("stgpr_run_id","meas","level","name", "crosswalk_version_id")]

  level <- levels[name==r, short]

  ##read in best bundle version used for crosswalking to get NIDs
  bv_meas <- get_crosswalk_version(run_ids$crosswalk_version_id[run_ids$level=="country" & run_ids$name=="stgpr_meas"])
  bv_sr <- get_crosswalk_version(run_ids$crosswalk_version_id[run_ids$level=="country" & run_ids$name=="stgpr_sr"])
  bv_meas <- merge(bv_meas, locs[,c("location_id","location_name","ihme_loc_id","country_name","region_name","super_region_name")], by=c("location_id","location_name"), all.x=T)
  bv_sr <- merge(bv_sr, locs[,c("location_id","location_name","ihme_loc_id","country_name","region_name","super_region_name")], by=c("location_id","location_name"), all.x=T)
  bmi_bv <- rbind(bv_meas, bv_sr)

  ## determine what level each country should be crosswalk at
  bmi_cts0 <- bmi_bv[is_outlier==0]
  bmi_cts0 <- bmi_cts0[, .N, by=c("nid","country_name","region_name","super_region_name","diagnostic","sex")]
  bmi_cts0 <- pivot_wider(bmi_cts0,id_cols=c("nid","sex","country_name","region_name","super_region_name"), values_from="N", names_from="diagnostic") %>% as.data.table()
  nids_drop_sr <-  unique(bmi_cts0$nid[!is.na(bmi_cts0$`self-report`) & !is.na(bmi_cts0$measured)]) ##both sr and measured captured

  bmi_cts <- bmi_bv[is_outlier==0]
  bmi_cts <- bmi_cts[!(nid %in% nids_drop_sr & diagnostic=="self-report"), .N, by=c("nid","country_name","region_name","super_region_name","diagnostic","sex", "age_group_id")] #remove cases where both measured adn sr captured
  bmi_cts <- pivot_wider(bmi_cts,id_cols=c("nid","sex", "age_group_id","country_name","region_name","super_region_name"), values_from="N", names_from="diagnostic") %>% as.data.table()
  bmi_ctry <- bmi_cts[, .(self_report = length(which(!is.na(`self-report`))), measured = length(which(!is.na(measured)))),
                      by=c("sex", "age_group_id","country_name","region_name","super_region_name")]
  bmi_ctry[, use_ctry := 0][self_report>=5 & measured>=5, use_ctry := 1]
  bmi_reg <- bmi_cts[, .(self_report = length(which(!is.na(`self-report`))), measured = length(which(!is.na(measured)))),
                     by=c("sex", "age_group_id","region_name","super_region_name")]
  bmi_reg[, use_reg := 0][self_report>=5 & measured>=5, use_reg := 1]
  cw_use <- unique(bmi_ctry[,-c("self_report","measured")])
  cw_use <- merge(cw_use, bmi_reg[,c("sex", "age_group_id","region_name","super_region_name","use_reg")], by=c("sex", "age_group_id","region_name","super_region_name"), all.x=T)
  cw_use[, use_sr := 0][use_ctry==0 & use_reg==0, use_sr := 1]
  cw_use[use_ctry==1, level := "country"][use_ctry==0 & use_reg==1, level :="region"][use_ctry==0 & use_reg==0 & use_sr==1, level :="super region"]

  ## make all Peru super region -- not much data avilability and overlap for this region
  cw_use[country_name=="PER" & level=="region",  `:=` (use_reg=0, use_sr=1, level="super region")]
  ## make IND location at country into region -- not much additional data availability and overlap for this region
  cw_use[country_name=="IND" & level=="country",  `:=` (use_ctry=0, use_reg=1, level="region")]
  ## Make USA all country level
  cw_use[country_name=="USA" & level!="country",  `:=` (use_ctry=1, use_reg=0, use_sr=0, level="country")]

  ######### Determine overlap between SR and Meas. Only use between 5 year overlap ########
  # Match dataset: country level
  matched_data <- merge(bv_meas[,c('location_id',"location_name","ihme_loc_id","country_name","region_name","super_region_name", 'age_group_id', 'sex','year_id')],
                        bv_sr[,c('location_id',"location_name","ihme_loc_id","country_name","region_name","super_region_name", 'age_group_id', 'sex','year_id')],
                        by = c('location_id',"location_name","ihme_loc_id","country_name","region_name","super_region_name", 'age_group_id', 'sex'), allow.cartesian = T, all=T)
  matched_data[, win5 := 0][abs(year_id.x - year_id.y) <= 5, win5 := 1]
  matched_data_ctry <- matched_data[abs(year_id.x - year_id.y) <= 5,.(sr_min=min(year_id.y),sr_max=max(year_id.y),meas_min=min(year_id.x),meas_max=max(year_id.x)),
               by=c("country_name","region_name","super_region_name","sex", "age_group_id")]
  matched_data_ctry[, min_year := ifelse(sr_min>=meas_min, meas_min, sr_min)][, max_year := ifelse(sr_max<=meas_max, meas_max, sr_max)]
  # Match dataset: region level
  matched_data <- merge(bv_meas[,c("region_name","super_region_name", 'age_group_id', 'sex','year_id')],
                        bv_sr[,c("region_name","super_region_name", 'age_group_id', 'sex','year_id')],
                        by = c("region_name","super_region_name", 'age_group_id', 'sex'), allow.cartesian = T, all=T)
  matched_data[, win5 := 0][abs(year_id.x - year_id.y) <= 5, win5 := 1]
  matched_data_reg <- matched_data[abs(year_id.x - year_id.y) <= 5,.(sr_min=min(year_id.y),sr_max=max(year_id.y),meas_min=min(year_id.x),meas_max=max(year_id.x)),
                                    by=c("region_name","super_region_name","sex", "age_group_id")]
  matched_data_reg[, min_year := ifelse(sr_min>=meas_min, meas_min, sr_min)][, max_year := ifelse(sr_max<=meas_max, meas_max, sr_max)]
  # Match dataset: super-region level
  matched_data <- merge(bv_meas[,c("super_region_name", 'age_group_id', 'sex','year_id')],
                        bv_sr[,c("super_region_name", 'age_group_id', 'sex','year_id')],
                        by = c("super_region_name", 'age_group_id', 'sex'), allow.cartesian = T, all=T)
  matched_data[, win5 := 0][abs(year_id.x - year_id.y) <= 5, win5 := 1]
  matched_data_sreg <- matched_data[abs(year_id.x - year_id.y) <= 5,.(sr_min=min(year_id.y),sr_max=max(year_id.y),meas_min=min(year_id.x),meas_max=max(year_id.x)),
                                    by=c("super_region_name","sex", "age_group_id")]
  matched_data_sreg[, min_year := ifelse(sr_min>=meas_min, meas_min, sr_min)][, max_year := ifelse(sr_max<=meas_max, meas_max, sr_max)]

  ## Determine overlap by crosswalk country, region, or super-region
    cw_use2 <- data.table()
    for(l in c("country","region","super region")){
      loc <- cw_use[level==l]
      if(l=="country") loc <- merge(loc, matched_data_ctry[,c("country_name","region_name","super_region_name","sex", "age_group_id","min_year","max_year")],
                                    by=c("country_name","region_name","super_region_name","sex", "age_group_id"), all.x=T)
      if(l=="region") loc <- merge(loc, matched_data_reg[,c("region_name","super_region_name","sex", "age_group_id","min_year","max_year")],
                                    by=c("region_name","super_region_name","sex", "age_group_id"), all.x=T)
      if(l=="super region") loc <- merge(loc, matched_data_sreg[,c("super_region_name","sex", "age_group_id","min_year","max_year")],
                                    by=c("super_region_name","sex", "age_group_id"), all.x=T)
      cw_use2 <- rbind(cw_use2,loc, fill=T)
    }
    cw_use2 <- merge(cw_use2, locs[level==3,c("location_id","country_name","region_name","super_region_name")], by=c("country_name","region_name","super_region_name"), allow.cartesian = T,all.x=T)
    cw_use2[, sex_id:=2][sex=="Male", sex_id:=1]

    bmi_cts2 <- bmi_cts[,.(measured = sum(measured,na.rm=T), self_report = sum(`self-report`,na.rm=T)), by=c("sex","age_group_id","country_name","region_name","super_region_name")]
    cw_use2 <- merge(cw_use2, bmi_cts2, by=c( "sex","age_group_id","country_name","region_name","super_region_name"))

    #### Check if there is min and max years for all cases. If not, and there is self-report data, downgrade
    downgrade <- cw_use2[is.na(cw_use2$min_year)]
    if(nrow(downgrade[self_report!=0])>0) {

      ## use super-region results if region selected
      dg_reg <- downgrade[use_reg==1]
      dg_reg[use_reg==1, `:=` (use_reg=0, use_sr=1, level="super region")]
      dg_reg <- merge(dg_reg[,-c("min_year","max_year")], matched_data_sreg[,c("super_region_name","sex", "age_group_id","min_year","max_year")],
                   by=c("super_region_name","sex", "age_group_id"), all.x=T)

      ## use region results if country selected
      dg_ctry <- downgrade[use_ctry==1]
      dg_ctry[use_ctry==1, `:=` (use_ctry=0, use_reg=1, level="region")]
      dg_ctry <- merge(dg_ctry[,-c("min_year","max_year")], matched_data_reg[,c("region_name","super_region_name","sex", "age_group_id","min_year","max_year")],
                      by=c("region_name","super_region_name","sex", "age_group_id"), all.x=T)

      ## use neighboring age group if super region selected
      dg_sr <- downgrade[use_sr==1]
      dg_sr[, age_group_id_orig := age_group_id]
      dg_sr[!(age_group_id_orig %in% c(6,30,34,235)), age_group_id := age_group_id_orig-1] ## select earlier age group for most
      dg_sr[age_group_id_orig==235, age_group_id:=32][age_group_id_orig==30, age_group_id:=20][age_group_id_orig==34, age_group_id:=6][age_group_id_orig==6, age_group_id:=7] ## for non-sequential, select earlier (or later if 2-4 years)
      dg_sr <- merge(dg_sr[,-c("min_year","max_year")], matched_data_sreg[,c("super_region_name","sex", "age_group_id","min_year","max_year")],
                      by=c("super_region_name","sex", "age_group_id"), all.x=T)
      dg_sr[age_group_id_orig==34 & age_group_id==6 & is.na(min_year), age_group_id := 7] ## if still na, grab
      dg_sr <- merge(dg_sr[,-c("min_year","max_year")], matched_data_sreg[,c("super_region_name","sex", "age_group_id","min_year","max_year")],
                                                                                                        by=c("super_region_name","sex", "age_group_id"), all.x=T)
      dg_sr$min_year <- na.locf(dg_sr$min_year)
      dg_sr$max_year <- na.locf(dg_sr$max_year)
      dg_sr[!is.na(age_group_id_orig), age_group_id_used := age_group_id] ##track which age group data came from
      dg_sr[!is.na(age_group_id_orig), age_group_id := age_group_id_orig] ##replace used age group so there is no overlap in next steps

    }
    cw_use3 <- do.call("rbind.fill", list(cw_use2[!is.na(min_year)], dg_reg, dg_ctry, dg_sr))
    cw_use3 <- as.data.table(cw_use3)

    ## Apply hard coded limits
    cw_use_ctry_ad <- merge(cw_use3[level=="country" & age_group_id %in% c(8:20,30:32,235)], xwalk_years[level=="country" & age_group=="adult",-c("level")],
                            by=c("sex","location_id","country_name","region_name","super_region_name"))
    cw_use_ctry_ch <- merge(cw_use3[level=="country" & age_group_id %in% c(34,6:7)], xwalk_years[level=="country" & age_group=="child",-c("level")],
                            by=c("sex","location_id","country_name","region_name","super_region_name"))
    cw_use_reg_ad <- merge(cw_use3[level=="region" & age_group_id %in% c(8:20,30:32,235)], xwalk_years[level=="region" & age_group=="adult",-c("location_id","country_name","level")],
                           by=c("sex","region_name","super_region_name"))
    cw_use_reg_ch <- merge(cw_use3[level=="region" & age_group_id %in% c(34,6:7)], xwalk_years[level=="region" & age_group=="child",-c("location_id","country_name","level")],
                           by=c("sex","region_name","super_region_name"))
    cw_use_sreg_ad <- merge(cw_use3[level=="super region" & age_group_id %in% c(8:20,30:32,235)], xwalk_years[level=="super region" & age_group=="adult",-c("location_id","country_name","region_name","level")],
                            by=c("sex","super_region_name"))
    cw_use_sreg_ch <- merge(cw_use3[level=="super region" & age_group_id %in% c(34,6:7)], xwalk_years[level=="super region" & age_group=="child",-c("location_id","country_name","region_name","level")],
                            by=c("sex","super_region_name"))
    cw_use3 <- do.call("rbind.fill", list(cw_use_ctry_ad, cw_use_ctry_ch, cw_use_reg_ad, cw_use_reg_ch, cw_use_sreg_ad, cw_use_sreg_ch))
    cw_use3 <- as.data.table(cw_use3)
    cw_use3[year_start != "x", min_year := as.integer(year_start)]
    cw_use3[year_end != "x", max_year := as.integer(year_end)]
    cw_use3 <- cw_use3[, -c("year_start","year_end","age_group")]

    assign(paste0("cw_use_", r), cw_use3) %>% as.data.table()
    fwrite(cw_use3, paste0("FILEPATH/crosswalk_level_",r,".csv"))

    ## a) Pull in STGPR draws

    ctry_model <- data.table()
    reg_model <- data.table()
    sreg_model <- data.table()
    for(id in run_ids$stgpr_run_id){
      row <- run_ids[stgpr_run_id==id]
      model <- get_estimates(id, entity="final")

      if(row$name=="stgpr_meas") model[, measure := "meas"]
      if(row$name=="stgpr_sr") model[, measure := "sr"]

      if(row$level=="country") ctry_model <- rbind(ctry_model, model, fill=T)
      if(row$level=="region") reg_model <- rbind(reg_model, model, fill=T)
      if(grepl("super",row$level)) sreg_model <- rbind(sreg_model, model, fill=T)
    }

    ## Merge with crosswalk tracking. Only keep groups to crosswalk in that level and get pruning information
    ctry_model <- merge(ctry_model, cw_use3[level=="country"], by=c("location_id","age_group_id","sex_id"), all.x=T)
    reg_model <- merge(reg_model, cw_use3[level=="region"], by=c("location_id","age_group_id","sex_id"), all.x=T)
    sreg_model <- merge(sreg_model, cw_use3[level=="super region"], by=c("location_id","age_group_id","sex_id"), all.x=T)

    ## match measured and self-report model results. Calculate ratio
    for(l in c("ctry","reg","sreg")){
      model <- copy(get(paste0(l, "_model")))

      ## prune to years with <=5 year overlap between measured and self-report
      model[, prune := 0][!(year_id %between% .(min_year, max_year)), prune := 1] ## flag years that need pruning
      setkey(model, age_group_id, sex_id, measure, location_id) ##set key for join

      # join unpruned data to dataset to get replacement values
      model[model[prune==0 & year_id==min_year], `:=` (val_lower=i.val, lower_lower=i.lower,upper_lower=i.upper), roll = "nearest"] ## earlier years
      model[model[prune==0 & year_id==max_year], `:=` (val_upper=i.val, lower_upper=i.lower,upper_upper=i.upper), roll = "nearest"] ## later years

      model[prune==1 & year_id<min_year, `:=` (val=val_lower, lower=lower_lower,upper=upper_lower)] ## replace vals for earlier years
      model[prune==1 & year_id>max_year, `:=` (val=val_upper, lower=lower_upper,upper=upper_upper)] ## replace vals for later years
      model <- model[order(measure,location_id, age_group_id, sex_id, year_id), -c("val_lower","val_upper","lower_lower","lower_upper","upper_lower","upper_upper")] ##remove extra variables

      model <- pivot_wider(model, id_cols=c("location_id","year_id","age_group_id","sex_id"),
                                values_from=c("val","lower","upper"), names_from="measure") %>% as.data.table()
      model[, `:=` (ratio= val_meas/val_sr, ratio_lower=lower_meas/lower_sr, ratio_upper=upper_meas/upper_sr)]
      assign(paste0(l, "_model_", r), model)
    }

    ## save ratio data
    all_ratio <- do.call("rbind", list(get(paste0("ctry_model_",r))[,`:=` (level="country", category=r)],
                                       get(paste0("reg_model_",r))[,`:=` (level="region", category=r)],
                                       get(paste0("sreg_model_",r))[,`:=` (level="super region", category=r)]))
    fwrite(all_ratio, paste0("FILEPATH/xwalk_", r,"_mean_bmi_ratios.csv"))

    ## close loop for BMI level
}


###################################################
### 3. Gather cleaned individual level data
###################################################
message(print("Reading data"))

## pull in cleaned individual-level data
clean_paths <- c("FILEPATH","FILEPATH")

loop_num <- 0
df <- data.table() ## table to gather all individual data
nids <- data.table() ## table to track NIDs and which path they are called from 
for(p in clean_paths){
  loop_num <- loop_num+1
  if(new_xwalk==T) l <- list.files(p, full.names = T, pattern = ".csv", recursive=T) ##get cleaned and new folder
  if(new_xwalk==F) l <- list.files(paste0(p,"new"), full.names = T, pattern = ".csv", recursive=F) ## only get new folder
  l <- do.call(rbind.fill, lapply(l, fread)) %>% as.data.table()
  l[, v:=loop_num]
  l <- l %>%
    mutate_if(is.integer64, as.integer)
  df<- rbind(df, l, fill=T)
}

## Clean dataset
if(!"pregnant" %in% names(df)) df[, pregnant := NA_integer_]
clean <- subset(df, sex_id==1 | (sex_id==2 & age_year<=10) | (sex_id==2 & age_year>10 & (pregnant==0 | is.na(pregnant) ))) ## remove pregnant women
clean <- subset(clean, age_year>=2) ## only keep ages >=2
clean <- merge(clean, locs[,c("ihme_loc_id","location_id","country_name")], by=c("ihme_loc_id")) ## add on location_id variable

clean[age_year>0 & age_month<13, age_month := age_year*12+age_month] ## age separated in years and months, combine to get total age in months 

clean[age_year >5, age_start := 5*floor(age_year/5)]
clean[age_start > 95, age_start := 95] 
clean[is.na(age_start) & age_year >=2, age_start := 2] 

# add GBD age groups:
clean <- merge(clean, ages[age_start!=0,c("age_group_id","age_start","age_end")], by="age_start",all.x=T)

clean[, year_id := ceiling((year_start+year_end)/2)]

## Separate into BMI categories
## This is needed because the crosswalk level and adjustment is determined by BMI category
if(!"bmi" %in% names(clean)) clean[, bmi := NA_integer_]
clean[!is.na(bmi) & is.na(bmi_rep), diagnostic := "measured"][!is.na(bmi) & is.na(bmi_rep), bmi_all := bmi] ## only measured data
clean[!is.na(bmi) & !is.na(bmi_rep), diagnostic := "measured"][!is.na(bmi) & !is.na(bmi_rep), bmi_all := bmi] ## measured and sr data, use measured
clean[!is.na(bmi_rep) & is.na(bmi), diagnostic := "self-report"][!is.na(bmi_rep) & is.na(bmi), bmi_all := bmi_rep] ## only sr data
clean[bmi_all <17, BMI_category := "Severely underweight"] ## super low BMI
clean[bmi_all>=17 & bmi_all<18.5 , BMI_category := "Underweight"] ## low BMI
clean[bmi_all>=18.5 & bmi_all<25 , BMI_category := "Normal"] ## normal BMI
clean[bmi_all>=25 & bmi_all<30, BMI_category := "Overweight"] ## overweight BMI
clean[bmi_all>=30 , BMI_category := "Obese"] ## obese BMI

##BMI quintiles
# Calculate quintiles
data2 <- clean[!is.na(bmi_all), .(quintile1=quantile(bmi_all, probs = seq(0, 1, .2))[1],
                                  quintile2=quantile(bmi_all, probs = seq(0, 1, .2))[2],
                                  quintile3=quantile(bmi_all, probs = seq(0, 1, .2))[3],
                                  quintile4=quantile(bmi_all, probs = seq(0, 1, .2))[4],
                                  quintile5=quantile(bmi_all, probs = seq(0, 1, .2))[5],
                                  quintile6=quantile(bmi_all, probs = seq(0, 1, .2))[6]), by=c("sex_id","nid", "age_year")]
clean <- merge(clean, data2, all=T, by=c("sex_id","nid", "age_year"))
clean[, quintile := 1][bmi_all>quintile2, quintile := 2][bmi_all>quintile3, quintile := 3][bmi_all>quintile4, quintile := 4][bmi_all>quintile5, quintile := 5]
## remove extra variables
clean <- clean[, -c("quintile1","quintile2","quintile3","quintile4","quintile5","quintile6")]

## Apply IOTF cuoffs for adjusted self-report children values
clean <- merge(clean, cutoffs[,-c("age_year")], by=c("age_month", "sex_id"), all.x=T)
message(paste0("There are ", nrow(clean[age_month>=min(cutoffs$age_month)&age_month<=max(cutoffs$age_month)]), " rows of children and adolescent data"))
clean[!is.na(bmi_all) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month),BMI_category:=""] # Setting child and adolescent rows to 0
clean[!is.na(bmi_all) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month) & bmi_all<uw_months_iotf, BMI_category := "Severely underweight"]
clean[!is.na(bmi_all) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month) & bmi_all>=uw_months_iotf, BMI_category := "Underweight"]
clean[!is.na(bmi_all) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month) & bmi_all>=normal_months_iotf, BMI_category := "Normal"]
clean[!is.na(bmi_all) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month) & bmi_all>=ow_months_iotf, BMI_category := "Overweight"]
clean[!is.na(bmi_all) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month) & bmi_all>=ob_months_iotf, BMI_category := "Obese"]
clean[,c( "sevuw_months_iotf","uw_months_iotf","normal_months_iotf","ow_months_iotf", "ob_months_iotf"):=NULL]

clean[diagnostic=="measured", overweight := 0][BMI_category=="Overweight" & diagnostic=="measured", overweight := 1]
clean[diagnostic=="self-report", overweight_rep := 0][BMI_category=="Overweight" & diagnostic=="self-report", overweight_rep := 1]
clean[diagnostic=="measured", obese := 0][BMI_category=="Obese" & diagnostic=="measured", `:=` (obese = 1, overweight=1)]
clean[diagnostic=="self-report", obese_rep := 0][BMI_category=="Obese" & diagnostic=="self-report", `:=` (obese_rep = 1, overweight_rep=1)]
clean[diagnostic=="measured", underweight := 0][BMI_category=="Underweight" & diagnostic=="measured", underweight := 1]
clean[diagnostic=="self-report", underweight_rep := 0][BMI_category=="Underweight" & diagnostic=="self-report", underweight_rep := 1]
clean[diagnostic=="measured", severe_underweight := 0][BMI_category=="Severely underweight" & diagnostic=="measured", `:=` (severe_underweight = 1, underweight=1)]
clean[diagnostic=="self-report", severe_underweight_rep := 0][BMI_category=="Severely underweight" & diagnostic=="self-report",  `:=` (severe_underweight_rep = 1, underweight_rep=1)]

## clean up cases where measured flags marked for self report and vice versa
clean[diagnostic=="self-report", `:=` (overweight=NA,obese=NA,underweight=NA,severe_underweight=NA)]
clean[diagnostic=="measured", `:=` (overweight_rep=NA,obese_rep=NA,underweight_rep=NA,severe_underweight_rep=NA)]

if (exclude_usa==TRUE) clean <- clean[!location_id %in% usa_locs$location_id]

##########################################################
### 3. Apply crosswalk adjustment factors on BMI by quintile
##########################################################
message(print("Applying crosswalk"))

df_comb <- data.table()
count <- 1
for(r in c("severeunderweight","underweight","normalweight","overweight","obese")){
  loc_list <- get(paste0("cw_use_", r))
  
  ##quintile
  clean_meas <-  subset(clean, quintile==count & diagnostic=="measured")
  clean_sub <- subset(clean, quintile==count & diagnostic=="self-report")

  ## Combine stgpr ratio and individual level data -- use appropriate crosswalk model based on number of sources in dataset
  cw_ctry <- merge(clean_sub, loc_list[loc_list$level=="country",c("country_name", "age_group_id", "sex","sex_id","level")] , by=c("country_name", "age_group_id",  "sex_id"))
  cw_ctry <- merge(cw_ctry, get(paste0("ctry_model_", r))[,c("location_id", "year_id", "age_group_id", "sex_id", "ratio")], by=c("location_id","year_id","age_group_id","sex_id"), all.x=T)
  
  cw_reg <- merge(clean_sub, loc_list[loc_list$level=="region",c("country_name", "age_group_id", "sex","sex_id","level")] , by=c("country_name", "age_group_id", "sex_id"))
  cw_reg <- merge(cw_reg, get(paste0("reg_model_", r))[,c("location_id", "year_id", "age_group_id", "sex_id", "ratio")], by=c("location_id","year_id","age_group_id","sex_id"), all.x=T)
  
  cw_sreg <- merge(clean_sub, loc_list[loc_list$level=="super region",c("country_name", "age_group_id", "sex","sex_id","level")] , by=c("country_name", "age_group_id", "sex_id"))
  cw_sreg <- merge(cw_sreg, get(paste0("sreg_model_", r))[,c("location_id", "year_id", "age_group_id", "sex_id", "ratio")], by=c("location_id","year_id","age_group_id","sex_id"), all.x=T)
  
  df_comb <- do.call("rbind.fill", list(df_comb,clean_meas,cw_ctry,cw_reg,cw_sreg))
  count <- count+1
}

summary(df_comb$ratio[!is.na(df_comb$ratio)])
df_comb <- as.data.table(df_comb)

##Find cases where self-report data in clean isn't in df_comb
check <- merge(clean,
               unique(df_comb[,c("country_name", "age_group_id", "location_id", "sex_id","diagnostic","BMI_category")])[,comb:=1], 
               by=c("country_name", "age_group_id", "location_id", "sex_id","diagnostic","BMI_category"), all=T)
check <- check[is.na(comb)]

check2 <- data.table()

## crosswalk at super region level
count <- 1
for(r in c("severeunderweight","underweight","normalweight","overweight","obese")){
  loc_list <- get(paste0("cw_use_", r))
  
  ##quintile
  check_sub <- subset(check, quintile==count & diagnostic=="self-report")
  
  check_sub <- merge(check_sub, locs[, c("location_id","region_name","super_region_name")], by="location_id",all.x=)
  
  ## adjust loc_list so unique location info for region and super region presented
  loc_list_reg <- unique(loc_list[use_reg==1 & use_ctry==0, -c("country_name","location_id","measured","self_report","age_group_id_orig","age_group_id_used")])
  loc_list_sreg <- unique(loc_list[use_sr==1 & use_ctry==0, -c("country_name","region_name","location_id","measured","self_report","age_group_id_orig","age_group_id_used")])
  
  ##Use region when feasible
  cw_reg <- merge(check_sub, loc_list_reg[,c("region_name", "age_group_id", "sex","sex_id","level")] , by=c("region_name", "age_group_id", "sex_id"), all.x=T)
  #Use super region in all other cases
  cw_sreg <- merge(cw_reg[is.na(level)][,-c("level","sex")], loc_list_sreg[,c("super_region_name", "age_group_id", "sex","sex_id","level")] , by=c("super_region_name", "age_group_id", "sex_id"), all.x=T)
  cw_reg <- cw_reg[level=="region"] ## only keep region matched cases for region merge
  cw_sreg[, level := "super region"]
  # merge to ratio data for region and super region levels
  cw_reg <- merge(cw_reg, get(paste0("reg_model_", r))[,c("location_id", "year_id", "age_group_id", "sex_id", "ratio")], by=c("location_id","year_id","age_group_id","sex_id"), all.x=T)
  cw_sreg <- merge(cw_sreg, get(paste0("sreg_model_", r))[,c("location_id", "year_id", "age_group_id", "sex_id", "ratio")], by=c("location_id","year_id","age_group_id","sex_id"), all.x=T)
  
  check2 <- do.call("rbind.fill", list(check2, cw_reg, cw_sreg))
  count <- count+1
}


##combine with df_comb to get all data
check2 <- as.data.table(check2)[, -c("comb")]
df_comb2 <- rbind(df_comb, check2,fill=T)

## Apply separate crosswalk values for USA results
if(exclude_usa!=TRUE){
  source("FILEPATH/crosswalk_function_code.R")
  ushd_xwalk <- readRDS("FILEPATH/correction_ratio.rds")
  setnames(ushd_xwalk, c("age_start", "age_end"), c("age_start_age20", "age_end_age20"))
  setnames(ushd_xwalk, "age", "age_start")
  ushd_85 <- ushd_xwalk[age_start==85]
  ushd_xwalk <- rbind(ushd_xwalk, ushd_85[, age_start:=90])
  ushd_xwalk <- rbind(ushd_xwalk, ushd_85[, age_start:=95])
  ushd_20 <- ushd_xwalk[age_start==20]
  ushd_xwalk <- rbind(ushd_xwalk, ushd_20[, age_start:=15])
  
  ##Add in territories as USA xwalk
  ctry_lvl <- ushd_xwalk[location_id==102]
  for(l in c(385, 422, 298, 351, 376)) ushd_xwalk <- rbind(ushd_xwalk, ctry_lvl[,`:=` (location_id = l, level="terr")])
  ##Add in additional years
  ctry_lvl <- ushd_xwalk[year_id==2000]
  for(y in c(1980:1999)) ushd_xwalk <- rbind(ushd_xwalk, ctry_lvl[,`:=` (year_id = y)])
  ctry_lvl <- ushd_xwalk[year_id==2020]
  for(y in c(2021:2022)) ushd_xwalk <- rbind(ushd_xwalk, ctry_lvl[,`:=` (year_id = y)])
  
  df_usa <- df_comb2[location_id %in% usa_locs$location_id]
  if(!"admin_1" %in% names(df_usa)) df_usa[, admin_1 := NA]
  
  ## USA locations unmapped, listed using FIPS code
  us_fips <- read.xlsx("FILEPATH/FIPS_codes.xlsx") %>% as.data.table()
  micro_temp <- df_usa[ihme_loc_id=="USA" & admin_1 %in% 1:80]
  micro_temp[, admin_1 := as.numeric(admin_1)]
  micro_temp <- merge(micro_temp, us_fips[level !="natl"], by.x="admin_1", by.y="FIPS", all.x=T)
  micro_temp[,  `:=` (admin_1 = location_name_short, admin_1_mapped=location_name_short)]
  micro_temp <- merge(micro_temp, locs[country_name=="USA",c("location_name_short", "ihme_loc_id")], by="location_name_short", all.x=T)
  setnames(micro_temp, c("ihme_loc_id.x"), c("ihme_loc_id"))
  micro_temp[, admin_1_id := ihme_loc_id.y]
  micro_temp[, c("ihme_loc_id.y", "location_name_short","level.y") := NULL]
  setnames(micro_temp,"level.x", "level")
  df_usa <- rbind(micro_temp, df_usa[!(ihme_loc_id=="USA" & admin_1 %in% 1:80)], fill=T)
  df_usa[ihme_loc_id=="USA" & tolower(admin_1)=="guam", admin_1_id := "GUM"]
  df_usa[ihme_loc_id=="USA" & tolower(admin_1)=="puerto rico", admin_1_id := "PRI"]
  df_usa[ihme_loc_id=="USA" & grepl("Islands",admin_1), admin_1_id := "VIR"]
  df_usa[ihme_loc_id=="USA" & grepl("virgin islands",admin_1), `:=` (admin_1_id="VIR", admin_1_mapped="United States Virgin Islands", admin_1="virgin islands")]
  df_usa[, admin_1 := tolower(admin_1)]
  df_usa[nid==492397 & admin_1=="xx", admin_1 := NA]
  df_usa <- df_usa[!(survey_name=="USA/PANEL_STUDY_OF_INCOME_DYNAMICS" & admin_1 %in% c(0,99))] ## drop 0 and 99 values 
  df_usa[!is.na(admin_1), location_id := as.numeric(substr(admin_1_id,5,7))]
  df_usa[admin_1_id == "GUM", location_id:=351][admin_1_id == "PRI", location_id:=385][admin_1_id == "VIR", location_id:=422][admin_1_id == "ASM", location_id:=298][admin_1_id == "MNP", location_id:=376]
  df_usa[nid==403299, location_id:=102][nid==142803, location_id:=102][nid==478397, location_id:=102]
  df_usa[ihme_loc_id=="USA" & tolower(admin_1)=="washignton", `:=` (admin_1_id="USA_570", admin_1_mapped="Washington", admin_1="washington",location_id=570)]
  
  ## Assign USHD quintile categories to USA data
  df_usa <- ushd_quantile(df_usa)
  if(nrow(df_usa)>0){
    df_usa[, survey:="none"][survey_name=="USA/BRFSS" | survey_name=="BRFSS", survey:="brfss"][survey_name=="US_GALLUP_DAILY", survey:="gallup"]
    
    df_usa <- merge(df_usa, ushd_xwalk, by=c("location_id","year_id","sex_id","bmi_report_percentile_bin","survey","age_start"), all.x=T) #"age20",
    setnames(df_usa, c("ratio.x", "ratio.y"), c("ratio_orig", "ratio"))
    
    ##Use our results, not USHD for territories
    df_usa[location_id %in% c(385, 422, 298, 351, 376), ratio := ratio_orig] ## our results so we can include regional components
    df_usa[age_start<15, ratio := ratio_orig] ##children SR will be dropped later, use original crosswalk values moving forward
    
    df_usa <- df_usa[, -c("age10","age_start_age20","age_end_age20","level.x","level.y","bmi_report_percentile","age20.x","age20.y")]
    setnames(df_usa,c("bmi_report_percentile_bin"), c("quintile_usa"))
  }
  df_comb_usa <- rbind(df_comb2[location_id %in% usa_locs$location_id & diagnostic=="measured"], df_usa, fill=T) ## results only has SR, add in measured
}

## Create adjusted BMI for reported values
df_comb2[!is.na(bmi_rep), bmi_rep_adj := bmi_rep * ratio] ## apply adjustment factor to reported BMI variables
df_comb2[!is.na(overweight_rep), overweight_rep_adj := 0][bmi_rep_adj >= 25, overweight_rep_adj := 1]
df_comb2[!is.na(obese_rep), obese_rep_adj := 0][bmi_rep_adj >= 30, obese_rep_adj := 1]
df_comb2[!is.na(underweight_rep), underweight_rep_adj := 0][bmi_rep_adj <18.5, underweight_rep_adj := 1]
df_comb2[!is.na(severe_underweight_rep), severe_underweight_rep_adj := 0][bmi_rep_adj <17, severe_underweight_rep_adj := 1]

setnames(df_comb2, c("bmi_rep","overweight_rep","obese_rep","underweight_rep","severe_underweight_rep"), 
         c("bmi_rep_original","overweight_rep_original","obese_rep_original","underweight_rep_original","severe_underweight_rep_original"))
setnames(df_comb2, c("bmi_rep_adj","overweight_rep_adj","obese_rep_adj","underweight_rep_adj","severe_underweight_rep_adj"), 
         c("bmi_rep","overweight_rep","obese_rep","underweight_rep","severe_underweight_rep"))


## Apply IOTF cuoffs for adjusted self-report children values
df_comb2 <- merge(df_comb2, cutoffs[,-c("age_year")], by=c("age_month","sex_id"), all.x=T)
message(paste0("There are ", nrow(df_comb2[age_month>=min(cutoffs$age_month)&age_month<=max(cutoffs$age_month)]), " rows of children and adolescent data"))
df_comb2[!is.na(overweight_rep) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month),overweight_rep:=0] # Setting child and adolescent rows to 0
df_comb2[!is.na(obese_rep) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month),obese_rep:=0] # Setting child and adolescent rows to 0
df_comb2[!is.na(overweight_rep) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month),underweight_rep:=0] # Setting child and adolescent rows to 0
df_comb2[!is.na(obese_rep) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month),severe_underweight_rep:=0] # Setting child and adolescent rows to 0
df_comb2[!is.na(overweight_rep) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month) & bmi_rep>=ow_months_iotf, overweight_rep := 1]
df_comb2[!is.na(obese_rep) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month) & bmi_rep>=ob_months_iotf, obese_rep := 1]
df_comb2[!is.na(overweight_rep) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month) & bmi_rep<normal_months_iotf, underweight_rep := 1]
df_comb2[!is.na(obese_rep) & age_month>=min(cutoffs$age_month) & age_month<=max(cutoffs$age_month) & bmi_rep<uw_months_iotf, severe_underweight_rep := 1]
df_comb2[,c("ow_months_iotf", "ob_months_iotf", "sevuw_months_iotf","uw_months_iotf","normal_months_iotf"):=NULL]

## Fix cases where child cutoffs used due to age month issues
df_comb2[age_year>18 & age_month<228 & !is.na(overweight_rep), `:=`(overweight_rep= 0,overweight_rep_original= 0)]
df_comb2[age_year>18 & age_month<228 & !is.na(obese_rep), `:=`(obese_rep= 0,obese_rep_original= 0)]
df_comb2[age_year>18 & age_month<228 & !is.na(underweight_rep), `:=`(underweight_rep= 0,underweight_rep_original= 0)]
df_comb2[age_year>18 & age_month<228 & !is.na(severe_underweight_rep), `:=`(severe_underweight_rep= 0,severe_underweight_rep_original= 0)]
df_comb2[age_year>18 & age_month<228 & !is.na(bmi_rep_original) & bmi_rep_original >= 25, overweight_rep_original := 1][age_year>18 & age_month<228 & !is.na(bmi_rep) & bmi_rep >= 25, overweight_rep := 1]
df_comb2[age_year>18 & age_month<228 & !is.na(bmi_rep_original) & bmi_rep_original >= 30, obese_rep_original := 1][age_year>18 & age_month<228 & !is.na(bmi_rep) & bmi_rep >= 30, obese_rep := 1]
df_comb2[age_year>18 & age_month<228 & !is.na(bmi_rep_original) & bmi_rep_original <18.5, underweight_rep_original := 1][age_year>18 & age_month<228 & !is.na(bmi_rep) & bmi_rep <18.5, underweight_rep := 1]
df_comb2[age_year>18 & age_month<228 & !is.na(bmi_rep_original) & bmi_rep_original <17, severe_underweight_rep_original := 1][age_year>18 & age_month<228 & !is.na(bmi_rep) & bmi_rep <17, severe_underweight_rep := 1]
df_comb2[age_year>18 & age_month<228 & !is.na(overweight), `:=`(overweight= 0)]
df_comb2[age_year>18 & age_month<228 & !is.na(obese), `:=`(obese= 0)]
df_comb2[age_year>18 & age_month<228 & !is.na(underweight), `:=`(underweight= 0)]
df_comb2[age_year>18 & age_month<228 & !is.na(severe_underweight), `:=`(severe_underweight= 0)]
df_comb2[age_year>18 & age_month<228 & !is.na(bmi) & bmi >= 25, overweight := 1]
df_comb2[age_year>18 & age_month<228 & !is.na(bmi) & bmi >= 30, obese := 1]
df_comb2[age_year>18 & age_month<228 & !is.na(bmi) & bmi <18.5, underweight := 1]
df_comb2[age_year>18 & age_month<228 & !is.na(bmi) & bmi <17, severe_underweight := 1]
df_comb2[age_year>18 & age_month<228, age_month := (age_year*12)+6]

df_comb2[severe_underweight==1 | severe_underweight_rep==1, BMI_category := "Severely underweight"] ## super low BMI
df_comb2[underweight==1 | underweight_rep==1 , BMI_category := "Underweight"] ## low BMI
df_comb2[overweight==1 | overweight_rep==1, BMI_category := "Overweight"] ## overweight BMI
df_comb2[obese==1 | obese_rep==1 , BMI_category := "Obese"] ## obese BMI
df_comb2[BMI_category != "Normal" & bmi_rep>=18.5 & bmi_rep<25, BMI_category := "Normal"] ## normal BMI

message(print("Saving"))

## d) Save each NID individually to all_rounds data folder
for(n in unique(df_comb2$nid)){
  dt <- df_comb2[nid==n, -c("v","ratio","level")]

  ## Fix issues that come up in collapse
  if(unique(dt$nid) %in% c(142787,142803)) dt[,admin_1_id := NULL] ## admin 1 id doesn't match ihme_loc_id causing errors
  if("admin_1_id" %in% names(dt)){
    if(length(unique(dt$ihme_loc_id))==length(unique(dt$admin_1_id))){
      dt[admin_1_id==ihme_loc_id,admin_1_id := NA] ## having admin_1_id when not need causing duplicates
    }
  }
  if("admin_2_id" %in% names(dt) & "admin_1_id" %in% names(dt)) dt[admin_1_id==admin_2_id,admin_2_id := NA] ## having admin_1_id when not need causing duplicates
  if("admin_2_id" %in% names(dt) & !("admin_1_id" %in% names(dt))) dt[,admin_2_id := NA] ## having admin_1_id when not need causing duplicates

  ## remove columns with all NA
  dt <- dt[,which(unlist(lapply(dt, function(x)!all(is.na(x))))),with=F]

  if(nrow(dt)>0){
    ## Add back in necessary variables that may have been dropped
    for (c in c("admin_1", "strata", "psu", "geospatial_id", "pweight", "psu_id", "strata_id", "hh_id", "line_id")){
      if (!(c %in% names(dt)) ) dt[, eval(c) := NA]
    }

    ## Fix year and survey_module info
    if(length(unique(dt$year_start))>1) dt[, year_start := min(unique(dt$year_start))]
    if(length(unique(dt$year_end))>1) dt[, year_end := max(unique(dt$year_end))]
    if("year_id" %in% names(dt)) if(length(unique(dt$year_id))>1) dt[, year_id := floor((year_end+year_start)/2)]
    if(length(unique(dt$survey_module))==2) dt[, survey_module := paste(unique(dt$survey_module)[1],unique(dt$survey_module)[2], sep="_")]
    if(length(unique(dt$survey_module))==3) dt[, survey_module := paste(unique(dt$survey_module)[1],unique(dt$survey_module)[2],unique(dt$survey_module)[3], sep="_")]

    ## save as csv
    dt[, save_name := paste(gsub("/","_", survey_name), year_id, nid, sep="_")]

    for(sn in unique(dt$save_name)) {

        ## save file to temp folder
        if (!unique(grepl("LIMITED_USE|limited_use", dt$file_path))) fwrite(dt[save_name==sn], paste0(nlu_save_path,sn,".csv"))
        if (unique(grepl("LIMITED_USE|limited_use", dt$file_path))) fwrite(dt[save_name==sn], paste0(lu_save_path,sn,".csv"))

    }
  } else {
    message(print("No data for NID:",n))
  }
}
