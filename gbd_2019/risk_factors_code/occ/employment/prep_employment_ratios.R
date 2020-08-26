############################################################################################################
## Purpose: Prep employment ratios
###########################################################################################################

## clear memory
rm(list=ls())

pacman::p_load(data.table,magrittr,ggplot2,parallel)

## settings
gbd_round_id <- 6
microdata_version <- 1
year_start <- 1980 ## oldest year estimated in st-gpr
do_split <- T  ## F if only using already age-specific data, T if splitting aggregate ages (requires a preliminary model to inform age splits)
split_run_id <- 95708 ## ID for model being used to inform age splits
decomp_step <- "step4" ## for specifying covariate/population versions to read in
generate_covs <- F ## T if generating square covariates for modeling purposes

## in/out
dir <- "FILEPATH"

## read in microdata
microdata_reg <- fread("FILEPATH")
microdata_cens <- fread("FILEPATH")
microdata <- rbind(microdata_reg,microdata_cens,fill=T)
microdata[,c("year_id","age_group_id","data","variance") := .(floor((year_start + year_end)/2),(age_start/5)+5,mean,standard_error**2)]

## load location hierarchy
source("FUNCTION")
source("FUNCTION")
locations <- get_location_metadata(gbd_round_id = gbd_round_id, location_set_id = 22)
locs <- copy(locations[, c("location_id","ihme_loc_id"), with=F])
pops <- get_population(location_id = "-1",age_group_id = microdata[,unique(age_group_id)],location_set_id = 22,year_id = seq(1960,2019),sex_id = c(1,2),gbd_round_id = 6,decomp_step = decomp_step)
pops[,run_id := NULL]


# CLEAN MICRODATA ---------------------------------------------------------
if ("missing_absent" %in% names(microdata)) microdata[,missing_absent := ifelse(is.na(missing_absent) | missing_absent == 0,0,1)]
if ("missing_military" %in% names(microdata)) microdata[,missing_military := ifelse(is.na(missing_military) | missing_military == 0,0,1)]

## crosswalking microdata
microdata <- microdata[var == "employed"]

## some variance is still NA. Impute from standard deviation and design effect
microdata[,variance_deff := (standard_deviation**2)*design_effect/sample_size]
microdata[,variance_nodeff := (standard_deviation**2)/sample_size]
microdata[is.na(variance) & !is.na(variance_deff),variance := variance_deff]
microdata[is.na(variance) & !is.na(variance_nodeff),variance := variance_nodeff]

## some variance is way too small. If recalculated variance from above helps, use that. Otherwise recalculate
## from sample size (and minimum/maximum non-zero data point if data is 0 or 1)
min_data <- microdata[data != 0,min(data)]
max_data <- microdata[data != 1,max(data)]
microdata[,imputed_variance := data*(1-data)/sample_size]
microdata[data == 0,imputed_variance := min_data*(1-min_data)/sample_size]
microdata[data == 1,imputed_variance := max_data*(1-max_data)/sample_size]
microdata[variance < imputed_variance & variance_deff > imputed_variance,variance := variance_deff]
microdata[variance < imputed_variance & variance_nodeff > imputed_variance,variance := variance_nodeff]
microdata[variance < imputed_variance,variance := imputed_variance]
microdata[,c("variance_deff","variance_nodeff","imputed_variance") := NULL]
microdata <- merge(microdata,locs,by="ihme_loc_id")

## use microdata to impute age and sex disaggregated sample size for tabulations,
## using 5th percentile of non-subnational data
## (excluding ISSP since there are a lot of them and they are unusually tiny)
impute_ss <- microdata[var == "employed" & !grepl("_",ihme_loc_id) & !grepl("ISSP",survey_name),.(sample_size = quantile(sample_size,.05)),by=age_group_id]


# NEW ILO TABULATIONS -----------------------------------------------------
new <- fread("FILEPATH")[!is.na(obs_value),list(ihme_loc_id = ref_area,country = ref_area.label,year_id = time,survey_id = paste0(source.label," - ",time),sex_id = ifelse(grepl("M",sex),1,ifelse(grepl("F",sex),2,3)),age = classif1.label,data = obs_value/100,note1 = note_classif.label,note2 = note_source.label)]

## clean locations
new[ihme_loc_id == "HKG",ihme_loc_id := "CHN_354"]
new[ihme_loc_id == "MAC",ihme_loc_id := "CHN_361"]
new[!ihme_loc_id %in% locations$ihme_loc_id,.(ihme_loc_id,country)] %>% unique
new <- merge(locs,new,by="ihme_loc_id")

## clean ages
new <- new[!grepl("<15",age)]
new[,age := gsub("5-year age bands: ","",age)]
new[,age := gsub("10-year age bands: ","",age)]
new[,age := gsub("Aggregate age bands: ","",age)]
new[,age := gsub("Age: ","",age)]
new[age == "Total",c("age_start","age_end") := .(15,69)]
new[substr(age,3,3) == "+",age_end := 69]
new[is.na(age_start),age_start := as.numeric(substr(age,1,2))]
new[is.na(age_end),age_end := as.numeric(substr(age,4,5))]
new[age_end - age_start == 4,age_group_id := (age_start/5) + 5]
new <- unique(new,by=c("survey_id","age_start","age_end","sex_id"))
new <- new[age_end >= 19 & age_start <= 65]
new_og <- copy(new)

## identify gold_standard survey_ids where data exists in every 5-year age group between 15 and 69 for both sexes separately
gold_ids <- new[sex_id != 3,all(seq(8,18) %in% unique(age_group_id)),by=.(survey_id,sex_id)][V1 == T,all(c(1,2) %in% unique(sex_id)),by=survey_id][V1 == T,survey_id]
new_gold <- new[survey_id %in% gold_ids & sex_id %in% c(1,2) & age_group_id %in% seq(8,18)]
new <- new[!survey_id %in% gold_ids]

## drop all sex_id 3 data where comparative-age data for both sexes separately already exists
both_sex_ages <- merge(new[sex_id == 1,.(survey_id,age)],new[sex_id == 2,.(survey_id,age)],by=c("survey_id","age"))
both_sex_ages[,both_sex := 1]
new <- merge(new,both_sex_ages,by=c("survey_id","age"),all.x=T)
new[,both_sex := ifelse(is.na(both_sex),0,both_sex)]
new <- new[sex_id != 3 | both_sex != 1]
new[,both_sex := NULL]
new[sex_id == 3]

## identify surveys where sex splitting needs to occur
new <- merge(new,new[sex_id == 3,list(survey_id,age,sexsplitting = 1)],by=c("survey_id","age"),all.x=T)

## in those surveys with both-sex data and one of the sexes, splitting can be done immediately
new <- merge(new,new[sexsplitting == 1 & sex_id != 3,list(survey_id,age,sexsplit_now = 1)],by=c("survey_id","age"),all.x=T)
missing_sex <- merge(new[sexsplit_now == 1 & sex_id == 3,],new[sexsplit_now == 1,3-min(sex_id),by=.(survey_id,age)],by=c("survey_id","age"))
missing_sex[,sex_id := V1]
missing_sex[,c("V1","data") := .(NULL,NA)]
new <- rbind(new,missing_sex)
for (sid in new[sexsplit_now == 1,unique(survey_id)]) {
  for (agegroup in new[sexsplit_now == 1 & survey_id == sid,unique(age)]) {
    new[sexsplit_now == 1 & survey_id == sid & age == agegroup,split_this := 1]
    locid <- new[split_this == 1,unique(location_id)]
    yearid <- new[split_this == 1,unique(year_id)]
    ageids <- seq(new[split_this == 1,(unique(age_start)/5) + 5],new[split_this == 1,((unique(age_end)-4)/5) + 5])

    split_pops <- pops[location_id == locid & year_id == yearid & age_group_id %in% ageids,sum(population),by=sex_id]
    split_pops[,split_this := 1]
    new <- merge(new,split_pops,by=c("sex_id","split_this"),all.x=T)
    new[split_this == 1,split_pops := V1]
    new[split_this == 1,split_pops := split_pops/sum(split_pops,na.rm=T)]

    sex_to_split <- new[split_this == 1 & is.na(data),sex_id]
    new[split_this == 1 & is.na(data),data := (new[split_this == 1 & sex_id == 3,data] - new[split_this == 1 & sex_id == (3-sex_to_split),data*split_pops])/split_pops]
    new[,c("split_this","V1") := NULL]
  }
}
new[data > 1, data := max_data]
new[data < 0, data := min_data]
new <- new[is.na(sexsplit_now) | sex_id %in% c(1,2)]
new[sexsplit_now == 1,sexsplitting := NA]
new[, split_pops := NULL]

## identify most-detailed age data for each sex
## we care more about sex pattern than age pattern, so priorize by-sex data even if it means keeping a larger age aggregation
new[,age_width := age_end - age_start]
for (this.age in seq(15,65,5)) {
  new[this.age < age_end & this.age >= age_start,covered := 1]
  merge_min <- new[covered == 1,min(age_width),by=.(survey_id,sex_id)]
  merge_min[,V2 := sum(sex_id),by=survey_id]
  merge_min <- merge_min[V2 != 6 | sex_id %in% c(1,2)]
  new <- merge(new,merge_min,by=c("survey_id","sex_id"),all.x=T)
  new[covered == 1 & age_width == V1,most_detailed := 1]
  new[,c("covered","V1","V2") := NULL]
}
new <- new[most_detailed == 1]
new[,most_detailed := NULL]

## separate out gold-standard (directly modelable) data from data that needs to be split
new_gold <- rbind(new_gold,new[!is.na(age_group_id) & sex_id %in% c(1,2)],fill=T)
new_split <- new[is.na(age_group_id) | sex_id == 3]

## impute sample size and variance from microdata sample sizes
new_gold <- merge(new_gold,impute_ss,by="age_group_id")
new_split[,sample_size := impute_ss[,mean(sample_size)]*((age_end - age_start + 1)/5)*ifelse(sex_id == 3,2,1)]
new_gold[,variance := data*(1-data)/sample_size]
new_gold[data == 0,variance := min_data*(1-min_data)/sample_size]
new_gold[data == 1,variance := max_data*(1-max_data)/sample_size]


# OLD ILO TABULATIONS -----------------------------------------------------
old <- fread(file.path(dir,"ilo/emp_ilo_byagesex_old.csv"))[!is.na(obs_value),list(ihme_loc_id = ref_area,country = ref_area.label,year_id = time,survey_id = paste0(ref_area," - ",source.label," - ",time),sex_id = ifelse(grepl("M",sex),1,ifelse(grepl("F",sex),2,3)),age = classif1.label,data = obs_value/100,note1 = note_classif.label,note2 = notes_source.label)]

## clean locations
old[ihme_loc_id == "HKG",ihme_loc_id := "CHN_354"]
old[ihme_loc_id == "MAC",ihme_loc_id := "CHN_361"]
old[ihme_loc_id == "TZ1",ihme_loc_id := "TZA"]
old[ihme_loc_id == "MY1",ihme_loc_id := "MYS"]
old[!ihme_loc_id %in% locations$ihme_loc_id,.(ihme_loc_id,country)] %>% unique
old <- merge(locs,old,by="ihme_loc_id")

## clean ages
old <- old[!grepl("<15",age)]
old[,age := gsub("5-year age bands: ","",age)]
old[,age := gsub("10-year age bands: ","",age)]
old[,age := gsub("Aggregate age bands: ","",age)]
old[,age := gsub("Age: ","",age)]
old[age == "Total",c("age_start","age_end") := .(15,69)]
old[substr(age,3,3) == "+",age_end := 69]
old[is.na(age_start),age_start := as.numeric(substr(age,1,2))]
old[is.na(age_end),age_end := as.numeric(substr(age,4,5))]
old[age_end - age_start == 4,age_group_id := (age_start/5) + 5]
old <- unique(old,by=c("survey_id","age_start","age_end","sex_id"))
old <- old[age_end >= 19 & age_start <= 65]

## check for duplicates with new data and drop where present
old <- merge(old,new_og[,list(ihme_loc_id,year_id,sex_id,age_start,age_end,new_source = survey_id)],by=c("ihme_loc_id","year_id","sex_id","age_start","age_end"),all.x=T)
old <- old[is.na(new_source)]
old[,new_source := NULL]
old <- unique(old)
old_og <- copy(old)

## identify gold_standard survey_ids where data exists in every 5-year age group between 15 and 69 for both sexes separately
gold_ids <- old[sex_id != 3,all(seq(8,18) %in% unique(age_group_id)),by=.(survey_id,sex_id)][V1 == T,all(c(1,2) %in% unique(sex_id)),by=survey_id][V1 == T,survey_id]
old_gold <- old[survey_id %in% gold_ids & sex_id %in% c(1,2) & age_group_id %in% seq(8,18)]
old <- old[!survey_id %in% gold_ids]

## drop all sex_id 3 data where comparative-age data for both sexes separately already exists (turns out to be all of sex_id 3, so get to skip some stuff)
both_sex_ages <- merge(old[sex_id == 1,.(survey_id,age)],old[sex_id == 2,.(survey_id,age)],by=c("survey_id","age"))
both_sex_ages[,both_sex := 1]
old <- merge(old,both_sex_ages,by=c("survey_id","age"),all.x=T)
old[,both_sex := ifelse(is.na(both_sex),0,both_sex)]
old <- old[sex_id != 3 | both_sex != 1]
old[,both_sex := NULL]
old[sex_id == 3]

## identify surveys where sex splitting needs to occur
old <- merge(old,old[sex_id == 3,list(survey_id,age,sexsplitting = 1)],by=c("survey_id","age"),all.x=T)

## in those surveys with both-sex data and one of the sexes, splitting can be done immediately
old <- merge(old,old[sexsplitting == 1 & sex_id != 3,list(survey_id,age,sexsplit_now = 1)],by=c("survey_id","age"),all.x=T)
missing_sex <- merge(old[sexsplit_now == 1 & sex_id == 3,],old[sexsplit_now == 1,3-min(sex_id),by=.(survey_id,age)],by=c("survey_id","age"))
missing_sex[,sex_id := V1]
missing_sex[,c("V1","data") := .(NULL,NA)]
old <- rbind(old,missing_sex)
for (sid in old[sexsplit_now == 1,unique(survey_id)]) {
  for (agegroup in old[sexsplit_now == 1 & survey_id == sid,unique(age)]) {
    old[sexsplit_now == 1 & survey_id == sid & age == agegroup,split_this := 1]
    locid <- old[split_this == 1,unique(location_id)]
    yearid <- old[split_this == 1,unique(year_id)]
    ageids <- seq(old[split_this == 1,(unique(age_start)/5) + 5],old[split_this == 1,((unique(age_end)-4)/5) + 5])

    split_pops <- pops[location_id == locid & year_id == yearid & age_group_id %in% ageids,sum(population),by=sex_id]
    split_pops[,split_this := 1]
    old <- merge(old,split_pops,by=c("sex_id","split_this"),all.x=T)
    old[split_this == 1,split_pops := V1]
    old[split_this == 1,split_pops := split_pops/sum(split_pops,na.rm=T)]

    sex_to_split <- old[split_this == 1 & is.na(data),sex_id]
    old[split_this == 1 & is.na(data),data := (old[split_this == 1 & sex_id == 3,data] - old[split_this == 1 & sex_id == (3-sex_to_split),data*split_pops])/split_pops]
    old[,c("split_this","V1") := NULL]
  }
}
old[data > 1, data := max_data]
old[data < 0, data := min_data]
old <- old[is.na(sexsplit_now) | sex_id %in% c(1,2)]
old[sexsplit_now == 1,sexsplitting := NA]
old[, split_pops := NULL]

## identify most-detailed age data for each sex
## we care more about sex pattern than age pattern, so priorize by-sex data even if it means keeping a larger age aggregation
old[,age_width := age_end - age_start]
for (this.age in seq(15,65,5)) {
  old[this.age < age_end & this.age >= age_start,covered := 1]
  merge_min <- old[covered == 1,min(age_width),by=.(survey_id,sex_id)]
  merge_min[,V2 := sum(sex_id),by=survey_id]
  merge_min <- merge_min[V2 != 6 | sex_id %in% c(1,2)]
  old <- merge(old,merge_min,by=c("survey_id","sex_id"),all.x=T)
  old[covered == 1 & age_width == V1,most_detailed := 1]
  old[,c("covered","V1","V2") := NULL]
}
old <- old[most_detailed == 1]
old[,most_detailed := NULL]

## separate out gold-standard (directly modelable) data from data that needs to be split
old_gold <- rbind(old_gold,old[!is.na(age_group_id) & sex_id %in% c(1,2)],fill=T)
old_split <- old[is.na(age_group_id) | sex_id == 3]

## impute sample size and variance from microdata sample sizes
old_gold <- merge(old_gold,impute_ss,by="age_group_id")
old_split[,sample_size := impute_ss[,mean(sample_size)]*((age_end - age_start + 1)/5)*ifelse(sex_id == 3,2,1)]
old_gold[,variance := data*(1-data)/sample_size]
old_gold[data == 0,variance := min_data*(1-min_data)/sample_size]
old_gold[data == 1,variance := max_data*(1-max_data)/sample_size]


# CREATE COMBINED ILO DATA ------------------------------------------------
both_gold <- rbind(old_gold,new_gold,fill=T)
both_split <- rbind(old_split,new_split,fill=T)


# SPLIT AGGREGATE AGES ----------------------------------------------------
if (do_split) {
  ## read in ST-GPR results for the split
  central_root <- "FILEPATH"
  setwd(central_root)
  source("FUNCTION")
  source("FUNCTION")
  gpr <- model_load(split_run_id, "raked")

  ## perform all splitting together
  new_split[,new_split := 1]
  old_split[,old_split := 1]
  split <- rbind(new_split,old_split,fill=T)

  ## first split by sex
  sexsplit <- split[sex_id == 3 & !is.na(age_group_id)]
  bothsplit <- split[sex_id == 3 & is.na(age_group_id)]
  split <- split[sex_id != 3 & is.na(age_group_id)] ## age split

  ## data that are already age-specific
  sexsplit <- merge(sexsplit[,-c("sex_id"),with=F],gpr,by=c("location_id","year_id","age_group_id"))
  sexsplit <- merge(sexsplit,pops,by=c("location_id","year_id","age_group_id","sex_id"))
  sexsplit[,gpr_est := weighted.mean(gpr_mean,population),by=.(survey_id,age_group_id)]
  sexsplit[,split_data := gpr_mean*data/gpr_est]
  sexsplit[,data_og := data]
  sexsplit[split_data > 1]

  ## in cases where split data surpasses 1, relax the sex-pattern as much as needed to prevent this from occuring
  sexsplit[split_data > max_data,maximum := max(split_data),by=.(survey_id,age_group_id)]
  sexsplit[,maximum := mean(maximum,na.rm=T),by=.(survey_id,age_group_id)]  ## mark surveys where splitting results in data above 1
  sexsplit[!is.na(maximum),deviation := split_data - data] ## within each group, calculate actual deviations from mean
  sexsplit[!is.na(maximum),max_deviation := max_data - data] ## calculate maximum possible deviation from mean without surpassing 1
  sexsplit[!is.na(maximum),dev_rescale := max_deviation/max(deviation),by=.(survey_id,age_group_id)] ## determine factor by which to scale deviations such that maximum point will not surpass 1
  sexsplit[!is.na(dev_rescale),rescaled_split := data + (dev_rescale * deviation)]
  sexsplit[!is.na(dev_rescale),split_data := rescaled_split]
  sexsplit[,data := split_data]
  sexsplit[,sample_size := sample_size*.5] ## no appreciable difference in sample sizes in surveys by sex

  ## data that needs age and sex splitting
  bothsplit <- merge(bothsplit[,-c("age_group_id","sex_id"),with=F],gpr,by=c("location_id","year_id"))
  bothsplit <- bothsplit[age_group_id >= ((age_start/5)+5) & age_group_id < (((age_end + 1)/5)+5)] ## subset to only relevant st-gpr results
  bothsplit <- merge(bothsplit,pops,by=c("location_id","year_id","age_group_id","sex_id"))
  bothsplit[,gpr_est := weighted.mean(gpr_mean,population),by=.(survey_id,age)]
  bothsplit[,split_data := gpr_mean*data/gpr_est]
  bothsplit[,data_og := data]
  bothsplit[split_data > 1]

  ## in cases where split data surpasses 1, prioritize sex pattern (adjust age pattern to make it work)
  bothsplit[split_data > max_data,maximum := max(split_data),by=.(survey_id,sex_id,age)]
  bothsplit[,maximum := mean(maximum,na.rm=T),by=.(survey_id,sex_id,age)]  ## mark surveys where splitting results in data above 1
  bothsplit[!is.na(maximum),deviation := split_data - data] ## within each group, calculate actual deviations from mean
  bothsplit[!is.na(maximum),max_deviation := max_data - data] ## calculate maximum possible deviation from mean without surpassing 1
  bothsplit[!is.na(maximum),dev_rescale := max_deviation/max(deviation),by=.(survey_id,sex_id,age)] ## determine factor by which to scale deviations such that maximum point will not surpass 1
  bothsplit[!is.na(dev_rescale),rescaled_split := data + (dev_rescale * deviation)]
  bothsplit[!is.na(dev_rescale),split_data := rescaled_split]
  bothsplit[,data := split_data]
  bothsplit[,sample_size := sample_size*.5]

  ## just age-splitting
  split <- merge(split[,-c("age_group_id"),with=F],gpr,by=c("location_id","year_id","sex_id"))
  split <- split[age_group_id >= ((age_start/5)+5) & age_group_id < (((age_end + 1)/5)+5)] ## subset to only relevant st-gpr results
  split <- merge(split,pops,by=c("location_id","year_id","age_group_id","sex_id"))
  split[,gpr_est := weighted.mean(gpr_mean,population),by=.(survey_id,sex_id,age)]
  split[,split_data := gpr_mean*data/gpr_est]
  split[,data_og := data]
  split[split_data > 1]

  ## in cases where split data surpasses 1 (or highest observed data), reset highest data point from that source to highest otherwise
  ## observed data, and rescale by-age deviations from the mean accordingly to keep weighted mean consistent with the data
  split[split_data > max_data,maximum := max(split_data),by=.(survey_id,sex_id,age)]
  split[,maximum := mean(maximum,na.rm=T),by=.(survey_id,sex_id,age)]  ## mark surveys where age splitting results in data above 1
  split[!is.na(maximum),deviation := split_data - data] ## within each group, calculate actual deviations from mean
  split[!is.na(maximum),max_deviation := max_data - data] ## calculate maximum possible deviation from mean without surpassing 1
  split[!is.na(maximum),dev_rescale := max_deviation/max(deviation),by=.(survey_id,sex_id,age)] ## determine factor by which to scale deviations such that maximum point will not surpass 1
  split[!is.na(dev_rescale),rescaled_split := data + (dev_rescale * deviation)] ## apply rescaled deviations to the mean
  split[!is.na(dev_rescale),split_data := rescaled_split]
  split[,data := split_data]

  ## bind split datasets back together
  split <- rbindlist(list(split,sexsplit,bothsplit))

  ## calculate proportional sample size splits based on observations in microdata (by age, by sex they are approximately the same)
  split <- merge(split,impute_ss[,.(age_group_id,imputed_ss = sample_size)],by="age_group_id")
  split[,ss_prop := imputed_ss/sum(imputed_ss),by=.(survey_id,sex_id,age)]
  split[,sample_size := ss_prop * sample_size]
  split[,variance := data*(1-data)/sample_size]
  split[,split := 1]

  ## clean up splitting vars
  split[,c("imputed_ss","gpr_mean","gpr_lower","gpr_upper","population","gpr_est","ss_prop","split_data","maximum","deviation","max_deviation","dev_rescale","rescaled_split") := NULL]

  ## append split data to already-split data
  new_gold <- rbind(new_gold,split[new_split == 1,-c("new_split","old_split"),with=F],fill=T)
  old_gold <- rbind(old_gold,split[old_split == 1,-c("new_split","old_split"),with=F],fill=T)
  both_gold <- rbind(both_gold,split[,-c("new_split","old_split"),with=F],fill=T)

  ## in some cases had to split aggregate-age data to obtain estimates for part of the age span, even though other
  ## rows provided data on some sections of the age span with more specificity. After splitting this results in
  ## overlapping data from the same survey. In any overlap only keep the point that came from the most detailed original data
  both_gold[,most_detailed := min(age_width),by=.(survey_id,sex_id,age_group_id)]
  both_gold <- both_gold[is.na(most_detailed) | age_width == most_detailed]
  both_gold[,most_detailed := NULL]
}


# CREATE FINAL DATASETS ---------------------------------------------------
## bind together different datasets and format for modeling
new_gold[,nid := 309568]
old_gold[,nid := 309568]
both_gold[,nid := 309568]

df_new <- rbind(new_gold,microdata,fill=T)
df_old <- rbind(old_gold,microdata,fill=T)
df_both <- rbind(both_gold,microdata,fill=T)

df_new[,c("is_outlier","measure") := .(0,"proportion")]
df_old[,c("is_outlier","measure") := .(0,"proportion")]
df_both[,c("is_outlier","measure") := .(0,"proportion")]

# OUTLIERING --------------------------------------------------------------

df_both[ihme_loc_id == "CHN",c("ihme_loc_id","location_id") := .("CHN_44533",44533)] ## none of the China data actually includes Hong Kong and Macau
df_both[grepl("issp",tolower(survey_name)) & grepl("_|IND",ihme_loc_id),is_outlier := 1] ## ISSP subnats, IND nat

## Female outliers
df_both[ihme_loc_id == "OMN" & year_id == 2016 & sex_id == 2 & data > .55,is_outlier := 1]
df_both[ihme_loc_id == "KWT" & year_id > 2010 & age_group_id %in% c(9) & sex_id == 2 & data < .3,is_outlier := 1]
df_both[ihme_loc_id == "MKD" & year_id == 2012 & age_group_id %in% c(8,10) & sex_id == 2 & data > .25,is_outlier := 1]
df_both[ihme_loc_id == "JAM" & year_id == 2012 & age_group_id %in% c(18) & sex_id == 2 & data > .3,is_outlier := 1]
df_both[ihme_loc_id == "QAT" & year_id == 2012 & age_group_id %in% c(8,18) & sex_id == 2 & data > .2,is_outlier := 1]
df_both[ihme_loc_id == "AFG" & year_id == 2008 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "YEM" & year_id == 1999 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "BGD" & year_id %in% seq(1989,2000) & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[grepl("CHN",ihme_loc_id) & year_id == 1990 & sex_id == 2,is_outlier := 1]
df_both[ihme_loc_id == "CHN_354" & year_id %in% seq(2012,2015) & age_group_id %in% c(8,9,16,17) & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "TWN" & age_group_id %in% c(8,18) & sex_id == 2 & nid != 309568,is_outlier := 1]
df_both[ihme_loc_id == "FJI" & year_id == 2014 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "KIR" & year_id == 2010 & age_group_id < 14 & sex_id == 2,is_outlier := 1]
df_both[ihme_loc_id == "LAO" & year_id == 2017 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "DJI" & year_id == 1996 & age_group_id %in% c(18) & sex_id == 2 & data > .4,is_outlier := 1]
df_both[ihme_loc_id == "MWI" & year_id == 2017 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "ZMB" & year_id %in% c(1990,2017) & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "RWA" & year_id == 2017 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "RWA" & year_id == 1996 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "BLR" & year_id %in% seq(2010,2015) & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "UGA" & year_id %in% c(2002,2003,2017) & age_group_id > 8 & sex_id == 2 & data < 0.55,is_outlier := 1]
df_both[ihme_loc_id == "UGA" & year_id %in% c(2002,2003,2017) & age_group_id == 8 & sex_id == 2 & data < 0.3,is_outlier := 1]
df_both[ihme_loc_id == "UGA" & year_id %in% c(2005,2013) & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "BWA" & year_id == 1985 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "LSO" & year_id == 2008 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "MLI" & year_id == 2009 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "BFA" & year_id == 2006 & age_group_id == 8 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "BHS" & year_id == 2009 & age_group_id == 18 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "ZWE" & year_id == 1992 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "GUM" & year_id == 2000 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "GUM" & year_id == 2006 & age_group_id == 9 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "IND" & year_id == 2005 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "KEN" & year_id == 1999 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "LUX" & year_id == 1980 & sex_id == 2 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "THA" & year_id == 1989 & sex_id == 2 & nid == 309568,is_outlier := 1]
if ("split" %in% names(df_both)) df_both[ihme_loc_id == "NGA" & year_id %in% c(2015,2016) & split == 1 & sex_id == 2 & nid == 309568,is_outlier := 1]


## Male outliers
df_both[ihme_loc_id == "AZE" & year_id %in% seq(2002,2005) & age_group_id %in% seq(9,16) & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "BEL" & year_id %in% seq(1987,1992) & age_group_id %in% seq(10,15) & sex_id == 1 & data < 0.7,is_outlier := 1]
df_both[ihme_loc_id == "BEL" & year_id %in% seq(1987,1992) & age_group_id %in% c(9,16) & sex_id == 1 & data < 0.45,is_outlier := 1]
df_both[ihme_loc_id == "SMR" & year_id == 2015 & age_group_id %in% seq(17,18) & sex_id == 1,is_outlier := 1]
df_both[ihme_loc_id == "SMR" & year_id == 2016 & age_group_id == 16 & sex_id == 1,is_outlier := 1]
if ("split" %in% names(df_both)) df_both[ihme_loc_id == "BOL" & year_id %in% c(1980,1996,1997,2007,2010) & sex_id == 1 & split == 1,is_outlier := 1]
df_both[ihme_loc_id == "CUB" & year_id %in% c(2012,2013) & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "HND" & year_id %in% c(1991,1992,1995,1998,2001) & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "PAN" & year_id %in% c(2009) & age_group_id %in% seq(10,17) & sex_id == 1 & data < .6,is_outlier := 1]
df_both[ihme_loc_id == "BHR" & year_id %in% c(2001) & age_group_id %in% c(15) & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "EGY" & year_id %in% seq(2000,2004) & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "KWT" & year_id %in% seq(2015,2016) & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "MAR" & year_id %in% c(1999,2006,2009,2013) & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "OMN" & year_id == 2016 & age_group_id %in% c(8,9,17,18) & sex_id == 1 & data > 0.5,is_outlier := 1]
df_both[ihme_loc_id == "BGD" & year_id %in% seq(1984,1991) & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[grepl("CHN",ihme_loc_id) & year_id == 1990 & sex_id == 1,is_outlier := 1]
df_both[ihme_loc_id == "CHN_354" & year_id %in% seq(2012,2015) & age_group_id %in% c(8,9,16,17,18) & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "CHN_354" & year_id == 1980 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "TWN" & age_group_id %in% c(8,18) & sex_id == 1 & nid != 309568,is_outlier := 1]
df_both[ihme_loc_id == "FJI" & year_id %in% c(1996,2014) & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "GUM" & year_id == 2000 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "KIR" & year_id == 2010 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "KHM" & year_id == 2004 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "LAO" & year_id == 2017 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "DJI" & year_id == 1996 & age_group_id %in% c(18) & sex_id == 1 & data > .5,is_outlier := 1]
df_both[ihme_loc_id == "RWA" & year_id == 1996 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "RWA" & year_id == 2017 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "MWI" & year_id == 2017 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "UGA" & year_id %in% c(2002,2003,2017) & age_group_id > 9 & sex_id == 1 & data < 0.73,is_outlier := 1]
df_both[ihme_loc_id == "UGA" & year_id %in% c(2002,2003,2017) & age_group_id == 9 & sex_id == 1 & data < 0.55,is_outlier := 1]
df_both[ihme_loc_id == "UGA" & year_id %in% c(2002,2003,2017) & age_group_id == 8 & sex_id == 1 & data < 0.375,is_outlier := 1]
df_both[ihme_loc_id == "ZMB" & year_id %in% c(1990,2017) & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "BWA" & year_id == 1985 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "LSO" & year_id == 2008 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "TGO" & year_id == 2015 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "MLI" & year_id == 2009 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "BHS" & year_id == 2009 & age_group_id == 18 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "ZWE" & year_id == 1992 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "KIR" & year_id == 2010 & age_group_id < 14 & sex_id == 1,is_outlier := 1]
df_both[ihme_loc_id == "MHL" & year_id == 1980 & sex_id == 1,is_outlier := 1]
df_both[ihme_loc_id == "BHR" & year_id == 2006 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "UZB" & year_id < 2008 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "TJK" & year_id == 2004 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "GRD" & year_id > 2012 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "TUV" & year_id == 2004 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "KOR" & year_id %in% c(1994,1995,1996) & age_group_id == 17 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "BRN" & year_id == 1981 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "SGP" & year_id < 1990 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "CYP" & year_id %in% c(1985,1987) & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "LUX" & year_id == 1980 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "CHL" & year_id %in% c(1984,1985,1986) & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "GTM" & year_id == 2006 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "ARE" & year_id == 1980 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "KEN" & year_id == 1999 & sex_id == 1 & nid == 309568,is_outlier := 1]
if ("split" %in% names(df_both)) df_both[ihme_loc_id == "NGA" & year_id %in% c(2015,2016) & split == 1 & sex_id == 1 & nid == 309568,is_outlier := 1]
df_both[ihme_loc_id == "SEN" & year_id == 2002 & sex_id == 1 & nid == 309568,is_outlier := 1]


# WRITE OUTPUTS -----------------------------------------------------------
if (do_split) {
  write.csv(df_both, "FILEPATH")
} else {
  write.csv(df_both, "FILEPATH")
  write.csv(new_split, "FILEPATH")
  write.csv(old_split, "FILEPATH")
}


# PREP CUSTOM COVARIATES --------------------------------------------------
if (generate_covs) {
  locs <- locations[is_estimate == 1,list(level,ihme_loc_id,location_id,region_name,super_region_name)]

  ## proportion of total population
  pops <- get_population(location_id = "-1",age_group_id = c(22,microdata[,unique(age_group_id)]),location_set_id = 22,year_id = seq(1970,2019),sex_id = c(1,2),gbd_round_id = 6,decomp_step = decomp_step)
  tot_pop <- pops[age_group_id == 22,sum(population),by=.(location_id,year_id)]
  setnames(tot_pop,"V1","totpop")
  pop_perc <- merge(pops[age_group_id != 22],tot_pop,by=c("location_id","year_id"))
  pop_perc[,pop_perc := population/totpop]
  pop_perc[,c("run_id","population","totpop") := NULL]
  custom_covs <- pop_perc[location_id %in% locs$location_id]

  ## government expenditure
  gov_exp <- fread("FILEPATH")[scenario == 0 & year < 2020,list(ihme_loc_id = iso3,year_id = year,gov_exp = mean)]
  gov_exp <- merge(gov_exp,locs,by="ihme_loc_id")
  gov_exp <- merge(gov_exp,tot_pop,by=c("location_id","year_id"))
  missing_locs <- locs[!location_id %in% gov_exp$location_id]
  gov_exp <- rbind(gov_exp,merge(CJ(location_id = missing_locs[,location_id],year_id = seq(1980,2019)),locs,by="location_id"),fill=T)
  gov_exp[,nat_id := substr(ihme_loc_id,1,3)]
  gov_exp[,nat_mean := weighted.mean(gov_exp,totpop,na.rm = T),by=.(nat_id,year_id)]
  gov_exp[,reg_mean := weighted.mean(gov_exp,totpop,na.rm = T),by=.(region_name,year_id)]
  gov_exp[is.na(gov_exp),gov_exp := nat_mean]
  gov_exp[is.na(gov_exp),gov_exp := reg_mean]
  custom_covs <- merge(custom_covs,gov_exp[,list(location_id,year_id,gov_exp)],by=c("location_id","year_id"))
  setnames(custom_covs,c("pop_perc","gov_exp"),c("cv_pop_perc","cv_gov_exp"))


  custom_covs[,cv_pop_perc := round(cv_pop_perc,3)]
  custom_covs[,cv_gov_exp := round(cv_gov_exp,3)]
  write.csv(custom_covs[,-c("cv_pop_perc"),with=F],file.path("FILEPATH"))
  write.csv(custom_covs,file.path("FILEPATH"))
}

