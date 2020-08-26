#purpose is to impute the supply side data based on the ratios between the various series

# load libraries
library(dplyr)
library(zoo)

# get location information using a central function
locs<-get_location_metadata(location_set_id = 22, gbd_round_id = 7)

# read in data that was cleaned from previous step
df_test <- fread("FILE PATH")

# get rid of the places where ihme_loc_id == NA; these are just years that didn't have any supply side data, and thus the ihme_loc_id was not merged on
df_test <- df_test[!is.na(ihme_loc_id)]


##### a little more outliering ########
# outlier a few more points before we begin imputation:
# Bermuda
df_test[location_id == 305 & year_id > 2007, fao_outlier_sum := fao_outlier_sum + 13]
# Belarus
df_test[location_id == 57 & year_id > 1986 & year_id < 2005, fao_outlier_sum := fao_outlier_sum + 13]
df_test[location_id == 57 & year_id > 1986 & year_id < 2005, usda_outlier_sum := usda_outlier_sum + 13]
# Saint Vincent and Grenadines
df_test[location_id == 117 & year_id > 1990, fao_outlier_sum := fao_outlier_sum + 13]
# Saint Lucia
df_test[location_id == 116 & year_id > 1982 & year_id < 1991, fao_outlier_sum := fao_outlier_sum + 13]
# Bahrain - outlier all data
df_test[location_id == 140, usda_outlier_sum := usda_outlier_sum + 13]
# Libya
df_test[location_id == 147 & year_id > 1969 & year_id < 1981, usda_outlier_sum := usda_outlier_sum + 13]
# Iraq
df_test[location_id == 143 & year_id > 1989 & year_id < 2004, fao_outlier_sum := fao_outlier_sum + 13]
# Mozambique
df_test[location_id == 184 & year_id > 1998, fao_outlier_sum := fao_outlier_sum + 13]
df_test[location_id == 184 & year_id > 1998, usda_outlier_sum := usda_outlier_sum + 13]
# Guinea-Bissau - high points that are driving a decreasing trend and are not super plausible
df_test[location_id == 209 & year_id < 1975, fao_outlier_sum := fao_outlier_sum + 13]
# UAE
df_test[location_id == 156 & year_id > 1999, usda_outlier_sum := usda_outlier_sum + 13]
df_test[location_id == 156 & year_id > 1999, em_outlier_sum := em_outlier_sum + 13]
# Oman
#df_test[location_id == 150 & year_id > 1992, usda_outlier_sum := usda_outlier_sum + 13]
df_test[location_id == 150 & year_id < 1993, fao_outlier_sum := fao_outlier_sum + 13]
df_test[location_id == 150 & year_id < 1995, usda_outlier_sum := usda_outlier_sum + 13]
# Samoa
df_test[location_id == 27 & year_id == 2006, fao_outlier_sum := fao_outlier_sum + 13]
# Congo
df_test[location_id == 170 & year_id < 1982, fao_outlier_sum := fao_outlier_sum + 13]
# Brazil
df_test[location_id == 135 & year_id > 1998, fao_outlier_sum := fao_outlier_sum + 13]
# Angola
df_test[location_id == 168 & fao_pc > 700, fao_outlier_sum := fao_outlier_sum + 13]
# Sudan
df_test[location_id == 522, fao_outlier_sum := fao_outlier_sum + 13]
# Niger
df_test[location_id == 213 & year_id >= 2010, fao_outlier_sum := fao_outlier_sum + 13]
# Zimbabwe
df_test[location_id == 198 & year_id == 2013, fao_outlier_sum := fao_outlier_sum + 13]
# Tanzania
df_test[location_id == 189 & year_id %in% c(2012,2013), fao_outlier_sum := fao_outlier_sum + 13]
# Djibouti
df_test[location_id == 177 & year_id == 2012, fao_outlier_sum := fao_outlier_sum + 13]
# Dominica
df_test[location_id == 110 & year_id %in% c(2010,2011), fao_outlier_sum := fao_outlier_sum + 13]
# Belize
df_test[location_id == 108 & year_id %in% c(2005,2006,2007), fao_outlier_sum := fao_outlier_sum + 13]
# Antigua and Barbuda
df_test[location_id == 105 & year_id == 2011, fao_outlier_sum := fao_outlier_sum + 13]
# Fiji
df_test[location_id == 22 & year_id %in% c(2012,2013), fao_outlier_sum := fao_outlier_sum + 13]

# assigns NA to rows where points were outliered
df_test[fao_outlier_sum > 12 , fao_pc := NA]
df_test[em_outlier_sum > 12, em_pc := NA]
df_test[usda_outlier_sum > 12 , usda_pc := NA]

# get rid of some duplicates
df_test <- df_test[!duplicated(df_test[,c("year_id","ihme_loc_id")]),c("year_id","ihme_loc_id","fao_pc","usda_pc","em_pc")]
df_test <- merge(df_test,locs[,.(ihme_loc_id,location_id,region_id,super_region_id,super_region_name,region_name)],by="ihme_loc_id")

# create a larger dataset with all of the years/locations we want to predict for
# earliest year for which we have data id 1960; latest year is 2017
all_years <- as.data.table(expand.grid(year_id=1960:2017,location_id = unique(df_test$location_id)))
all_years <- merge(all_years,locs[,.(ihme_loc_id,location_id,region_id,super_region_id,super_region_name,region_name)],by="location_id")
# merge onto the data
df_test <- merge(df_test,all_years,all=T,by=c("year_id","ihme_loc_id","region_id","super_region_id","super_region_name","region_name","location_id"))
# clean up the number of columns
df_test <- df_test[,.(location_id,region_id,super_region_id,year_id,fao_pc,usda_pc,em_pc,super_region_name,region_name,ihme_loc_id)]

# data cleaning
df_test$location_id <- as.character(df_test$location_id)
df_test$region_id <- as.character(df_test$region_id)
df_test$super_region_id <- as.character(df_test$super_region_id)

######################## use a simple linear model to predict the relationship between these series #################
# set FAO to always be the reference in order to create the combos of series; this decision is not important
ref <-  "fao_pc"
alts <- c("usda_pc","em_pc")

# establish all of the pairings that you want to model
all_combos <- as.data.table(expand.grid(alternates = alts,reference = c(ref,alts)))
all_combos[,reference := as.character(reference)]
all_combos[,alternates := as.character(alternates)]
all_combos <- all_combos[reference != alternates]
# and get rid of one redundancy:
all_combos <- all_combos[!(alternates == "usda_pc" & reference == "em_pc")]


# for each of the pairings between the series..
for (i in 1:nrow(all_combos)){
  line <- all_combos[i]
  message(paste0(line$alternates," and ", line$reference))
  df <- df_test[!is.na(get(line$alternates)) & !is.na(get(line$reference))]
  
  # calculate the log ratio
  df[,ratio := get(line$alternates)/get(line$reference)]
  df[,log_ratio := log(ratio)]
  
  # model the
  model_ratio <- lme4::lmer(log_ratio~1 + (1|super_region_name/region_name/ihme_loc_id), data = df)
  
  df$predicted_log_ratio <- predict(model_ratio,df)
  df[,predicted_ratio := exp(predicted_log_ratio)]
  
  df_test_full <- copy(df_test)
  df_test_full[,ratio := get(line$alternates)/get(line$reference)]
  df_test_full[,log_ratio := log(ratio)]
  df_test_full$predicted_log_ratio <- predict(model_ratio,df_test_full,allow.new.levels=T)
  df_test_full[,predicted_ratio := exp(predicted_log_ratio)]
  
  #ggplot() + geom_point(data=df[region_id == 100],aes(predicted_ratio,ratio,color=ihme_loc_id)) + geom_abline(intercept=0,slope=1) + ggtitle(paste0(line$alternates,"_versus_",line$reference))
  #ggplot() + geom_point(data=df[location_id==102],aes(predicted_ratio,ratio,color=year_id)) + geom_abline(intercept=0,slope=1) + ggtitle(paste0(line$alternates,"_versus_",line$reference))
  
  
  setnames(df_test_full,old=c("predicted_log_ratio","predicted_ratio"),new=c(paste0("predicted_log_ratio_",line$alternates,"_versus_",line$reference),paste0("predicted_ratio_",line$alternates,"_versus_",line$reference)))
  label <- paste0(line$alternates,"_versus_",line$reference)
  
  write.csv(df_test_full,'FILE PATH')
}

# read in the data from above model
filenames <- list.files('FILE PATH')

# Load data sets
df_compiled <- data.table()
j <- 0
for(k in filenames){
  df_temp <- fread(k)
  df_temp$V1 <- NULL
  df_temp$ratio <- NULL
  df_temp$log_ratio <- NULL
  if(j == 0){
    j <- 1
    df_compiled <- df_temp
  } else{
    df_compiled <- merge(df_compiled,df_temp,by=c("year_id","ihme_loc_id","region_id","super_region_id","super_region_name","region_name","location_id","fao_pc","usda_pc","em_pc"),all=T)
  }
}

# now we need to backcast and forecast
# for each series...
# if a value is NA, then create multiple imputations for that source by multiplying each of the predicted ratios by the reference
# e.g. predicted_ratio_usda_pc_versus_fao_pc * fao_pc if usda is empty but fao_pc is non-NA

# replace some of the location-specific imputations with region-level imputations because the location-specific imputations looked unreasonable due to low crossover between series
region_level <- as.data.table(read.csv('FILE PATH')) # list of locations for which to use regional imputations
region_level$series <- gsub("$","_reg",region_level$series)
region_level <- dcast(location_name~series,data=region_level, fun.aggregate = length)
region_level <- merge(region_level,locs[,.(location_name,location_id)],by="location_name")

df_compiled <- merge(df_compiled,region_level,by="location_id",all=T)
df_compiled$location_name <- NULL
# make region averages:
for (nm in names(df_compiled)[names(df_compiled) %like% "_versus_"]){
  message(nm)
  df_compiled[,paste0(nm,"_region_level") := mean(get(nm),na.rm=T),by=c("region_name","year_id")]
}

# fill in missing columns
if (!("em_reg" %in% names(df_compiled))){
  df_compiled$em_reg <- NA
}
if (!("usda_reg" %in% names(df_compiled))){
  df_compiled$usda_reg <- NA
}
if (!("fao_reg" %in% names(df_compiled))){
  df_compiled$fao_reg <- NA
}

# first deal with the entries that we want to be location-specific
df_compiled[usda_reg != 1 | is.na(usda_reg),usda_from_fao := predicted_ratio_usda_pc_versus_fao_pc * fao_pc]
df_compiled[usda_reg != 1 | is.na(usda_reg),usda_from_em := em_pc/predicted_ratio_em_pc_versus_usda_pc]
df_compiled[em_reg != 1 | is.na(em_reg),em_from_fao := predicted_ratio_em_pc_versus_fao_pc * fao_pc]
df_compiled[em_reg != 1 | is.na(em_reg),em_from_usda := predicted_ratio_em_pc_versus_usda_pc * usda_pc]
df_compiled[fao_reg != 1 | is.na(fao_reg),fao_from_usda := usda_pc/predicted_ratio_usda_pc_versus_fao_pc]
df_compiled[fao_reg != 1 | is.na(fao_reg),fao_from_em := em_pc/predicted_ratio_em_pc_versus_fao_pc]

# and then those that should use the regional average:
df_compiled[usda_reg == 1,usda_from_fao := predicted_ratio_usda_pc_versus_fao_pc_region_level * fao_pc]
df_compiled[usda_reg == 1,usda_from_em := em_pc/predicted_ratio_em_pc_versus_usda_pc_region_level]
df_compiled[em_reg == 1,em_from_fao := predicted_ratio_em_pc_versus_fao_pc_region_level * fao_pc]
df_compiled[em_reg == 1,em_from_usda := predicted_ratio_em_pc_versus_usda_pc_region_level * usda_pc]
df_compiled[fao_reg == 1 ,fao_from_usda := usda_pc/predicted_ratio_usda_pc_versus_fao_pc_region_level]
df_compiled[fao_reg == 1 ,fao_from_em := em_pc/predicted_ratio_em_pc_versus_fao_pc_region_level]

# and then those that should use the regional average
df_compiled[,avg_fao_pred := mean(c(fao_from_usda,fao_from_em),na.rm=T),by=c("location_id","year_id")]
df_compiled[,avg_em_pred := mean(c(em_from_fao,em_from_usda),na.rm=T),by=c("location_id","year_id")]
df_compiled[,avg_usda_pred := mean(c(usda_from_fao,usda_from_em),na.rm=T),by=c("location_id","year_id")]

# make an indicator so we know which points were imputed
df_compiled[is.na(fao_pc) & !is.na(avg_fao_pred),fao_imputed := 1]
df_compiled[is.na(usda_pc) & !is.na(avg_usda_pred),usda_imputed := 1]
df_compiled[is.na(em_pc) & !is.na(avg_em_pred),em_imputed := 1]

df_compiled[,fao_pc := ifelse(is.na(fao_pc),avg_fao_pred,fao_pc)]
df_compiled[,em_pc := ifelse(is.na(em_pc),avg_em_pred,em_pc)]
df_compiled[,usda_pc := ifelse(is.na(usda_pc),avg_usda_pred,usda_pc)]

#### just checking a few locations
## save the final dataset
write.csv(df_compiled,'FILE PATH')

#### calculate variance for ST-GPR
n_sources <- 3
iterator <- n_sources - 1

all_lox <- unique(df_compiled[, .(ihme_loc_id)])

df_compiled <- df_compiled[,c("year_id","ihme_loc_id","location_id","fao_pc","usda_pc","em_pc","fao_imputed","usda_imputed","em_imputed")]

## Set all NAs to 0 (denotes that the point was not imputed)
df_compiled[is.na(df_compiled)] <- 0

# calculate variance across series
df_compiled[, all_variance := matrixStats::rowVars(as.matrix(.SD)), .SDcols = c("fao_pc", "usda_pc", "em_pc")]

## Create a column showing how many of the country-year datapoints were modeled (m)
df_compiled[, all_modeled_number := rowSums(.SD), .SDcols = c("fao_pc", "usda_pc", "em_pc")]
df_compiled[, all_total_series := n_sources]

# calculate the mean tobacco pc value across all the series
df_compiled[, all_mean := rowMeans(.SD), .SDcols = c("fao_pc", "usda_pc", "em_pc")]

df_compiled_melt_variance <- melt(df_compiled[,.(year_id,ihme_loc_id,fao_imputed,em_imputed,usda_imputed)], c("year_id","ihme_loc_id"), variable.name = "series", value.name = "yvar")
# calculate the number of series that were predicted, versus being raw values
df_compiled_melt_variance[,predicted := sum(yvar),by=c("ihme_loc_id","year_id")]

### melt and make more transformations
df_compiled_melt_combined <- merge(df_compiled, unique(df_compiled_melt_variance[,.(year_id,ihme_loc_id,predicted)]), c("year_id", "ihme_loc_id"))
df_compiled_melt_combined <- df_compiled_melt_combined[all_variance != 0]

# and now, calculate the variance across time within each of the series
df_compiled_melt_combined <- df_compiled_melt_combined[, time_variance := rollapply(.SD$all_mean, 10, sd, na.rm=T, fill = 0), by="location_id"]
df_compiled_melt_combined <- df_compiled_melt_combined[, time_variance_left := rollapply(.SD$all_mean, 10, sd, na.rm=T, fill = 0,align="left"), by="location_id"]
df_compiled_melt_combined <- df_compiled_melt_combined[, time_variance_right := rollapply(.SD$all_mean, 10, sd, na.rm=T, fill = 0,align="right"), by="location_id"]
# fix the endpoints
df_compiled_melt_combined[,time_variance := ifelse(time_variance == 0, time_variance_left,time_variance)]
df_compiled_melt_combined[,time_variance := ifelse(time_variance == 0, time_variance_right,time_variance)]
df_compiled_melt_combined[,time_variance := time_variance^2]

# sum the variance across series (inflated by number of imputations) and variance across time
df_compiled_melt_combined[, all_variance := all_variance + time_variance]

# data now ready for ST-GPR
write.csv(df_compiled_melt_combined,'FILE PATH')
