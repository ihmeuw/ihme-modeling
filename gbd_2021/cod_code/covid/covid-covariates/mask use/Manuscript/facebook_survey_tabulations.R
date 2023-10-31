## Intention here is to pull raw Facebook survey data, tabulate in different ways ##
# Facebook symptom survey - global
# source("/ihme/code/covid-19/user/ctroeger/covid-beta-inputs/src/covid_beta_inputs/mask_use/Manuscript/facebook_survey_tabulations.R")

rm(list=ls())

library(data.table)
# library(spatstat)
source("FILEPATH/get_location_metadata.R")

# get locations

# read in locs - using a 'merge_name' field, which is still the ISO code for countries, but the country_admin1 for admin1s
gbd_locs <- get_location_metadata(location_set_id = 111, location_set_version_id = 680)
gbd_locs <- gbd_locs[, .(location_id, location_name, ihme_loc_id, parent_id, level)]
parent_locs <- gbd_locs[,c('location_name', 'location_id')]
setnames(parent_locs, old = c('location_name'), new = c('parent_name'))
gbd_locs <- merge(gbd_locs, parent_locs, by.x = 'parent_id', by.y = 'location_id', all.x=T, all.y=F)
#gbd_locs[, gbd := 1]
#gbd_locs <- unique(gbd_locs)
gbd_locs[level==0,merge_test := ihme_loc_id]
gbd_locs[level==1,merge_test := paste0(parent_name, '_', location_name)]

# read in a file that has a bunch of manual matches when the names didn't match
x <- fread('FILEPATH/merge_names.csv')
x <- x[,c('merge_name', 'location_id')]
y <- gbd_locs[,c('location_id', 'location_name', 'ihme_loc_id', 'merge_test')]
x <- merge(x, y, by = 'location_id', all.x=T, all.y=F)
locations <- copy(x)
locations[nchar(merge_test) != 3, merge_test := merge_name]
locations[,merge_name := NULL]
setnames(locations, old = 'merge_test', new = 'merge_name')

# Bind all files together
daily_files <- list.files(
  path = 'FILEPATH/cleanfiles_v1.6', 
  full.names = T)
daily_files <- daily_files[!daily_files %like% 'CODEBOOK|iso_country_region_response_map|parta']
raw_data_list <- lapply(daily_files, fread)
raw_data <- rbindlist(raw_data_list, fill = T)

rm(raw_data_list)

# Quick checks
dt <- copy(raw_data)
rm(raw_data)

# New values
dtn <- subset(dt, is.na(RecordedDate))
dtn$RecordedDate <- dtn$StartDate
dto <- subset(dt, !is.na(RecordedDate))

dt <- rbind(dto, dtn, fill = T)
rm(dtn)
rm(dto)

# Date
dt[, date := as.Date(substr(dt$RecordedDate, 1, 10))]

# ------------------------------------------------------------------------------
# Quick checks
names(dt)[!names(dt) %in% names(fread(daily_files[[1]]))]
names(dt)

# Look for incomplete surveys
names(dt)
unique(dt$Finished)
sum(dt$Finished) / nrow(dt) 
nrow(dt[Finished == 0]) / nrow(dt)

# Look for missing countries
nrow(dt[ISO_3 == "" | is.na(ISO_3) | ISO_3 == "NA"])
nrow(dt[country_agg == "" | is.na(country_agg) | country_agg == "NA"])
nrow(dt[region_agg == "" | is.na(region_agg) | region_agg == "NA"])

# ------------------------------------------------------------------------------
# Helpers
# Re-order
names_desc <- c('ISO_3', 'country_agg', 'region_agg', 'date', 'weight', 'survey_region', 'survey_version', 
                'Finished', 'RecordedDate',
                'GID_0', 'GID_1', 'NAME_0', 'NAME_1', 
                'country_region_numeric', 'urbanicity', 'gender', 'age_group', 'contacts_24h', 'income')
                #'work', 'public_event', 'anxious', 'nervous', 'worried'


# Make more informative factors for tabulation
dt[, urbanicity := ifelse(E2 == 1, "city", ifelse(E2 == 2, "town", "village/rural"))]
dt[, gender := ifelse(E3 == 1, "male", ifelse(E3 == 2, "female", ifelse(E3 == 3, "self-describe", "no answer")))]
dt[, age_group := ifelse(E4 == 1, "18-24", ifelse(E4 == 2, "25-34",
                        ifelse(E4 == 3, "35-44", ifelse(E4 == 4, "45-54",
                               ifelse(E4 == 5, "55-64", ifelse(E4 == 6, "65-74","> 75"))))))]
dt[, contacts_24h := ifelse(C2 == 1, "1-4", ifelse(C2 == 2, "5-9", ifelse(C2 == 3, "10-19",">20")))]
dt[, income := ifelse(D5 == 1, "very worried", ifelse(D5 == 2, "worried", "not worried"))]

# Aggregation variables
byvars <- c('ISO_3', 'country_agg','region_agg','date', 'merge_name', 'urbanicity', 'gender', 'age_group','contacts_24h', 'income')
parent_vars <- c('ISO_3', 'country_agg','date', 'urbanicity', 'gender', 'age_group', 'contacts_24h', 'income')

# ------------------------------------------------------------------------------
# COUNTY SUMMARY
# Unique countries
countries <- unique(dt[, .(ISO_3, country_agg, region_agg)])
countries <- countries[order(ISO_3)]

# Counts
countries <- dt[, .(count = .N), by = c('ISO_3', 'country_agg', 'region_agg')]
countries <- countries[order(ISO_3)]

countries[region_agg == "", merge_name := ISO_3]
countries[is.na(region_agg), merge_name := ISO_3]
countries[region_agg != "", merge_name := paste0(country_agg, '_', region_agg)]

countries <- merge(x = countries, y = locations, by.x = 'merge_name', by.y = 'merge_name', all.x = T, all.y = F)

countries <- countries[order(ISO_3)]
countries <- countries[!is.na(location_id),]

dt[is.na(region_agg), merge_name := ISO_3]
dt[region_agg == "",merge_name := ISO_3]
dt[region_agg != "", merge_name := paste0(country_agg, '_', region_agg)]

# ==============================================================================
# MASKS
# C5 In the last 7 days, how often did you wear a mask when in public?	
#  1=All of the time 2=Most of the time 3=About half of the time 4=Some of the time 
#  5=None of the time 6=I have not been in public during the last 7 days"
# ==============================================================================

# For renaming
old_names <- c('C5_1', 'C5_2', 'C5_3', 'C5_4', 'C5_5', 'C5_6')
new_names <- c('mask_7days_all', 'mask_7days_most', 'mask_7days_half', 'mask_7days_some', 'mask_7days_none', 'mask_7days_no_public')

# Subset
dt_sub <- dt[!is.na(C5), c(names_desc, 'C5', 'merge_name'), with = F]
dt_sub[, `:=`(
  C5_1 = ifelse(C5 == 1, 1, 0), C5_2 = ifelse(C5 == 2, 1, 0), C5_3 = ifelse(C5 == 3, 1, 0),
  C5_4 = ifelse(C5 == 4, 1, 0), C5_5 = ifelse(C5 == 5, 1, 0), C5_6 = ifelse(C5 == 6, 1, 0))]

dt_sub_collapsed <- copy(dt_sub)

# Prepare two aggregations

dt_sub_subnat <- dt_sub_collapsed[region_agg != "",]
dt_sub_subnat <- dt_sub_subnat[!is.na(region_agg),]

# Aggregate
dt_sub_collapsed <- dt_sub_collapsed[, .(
  C5_1 = sum(C5_1), C5_2 = sum(C5_2), C5_3 = sum(C5_3),
  C5_4 = sum(C5_4), C5_5 = sum(C5_5), C5_6 = sum(C5_6)),
  by = parent_vars]

dt_sub_subnat <- dt_sub_subnat[, .(
  C5_1 = sum(C5_1), C5_2 = sum(C5_2), C5_3 = sum(C5_3),
  C5_4 = sum(C5_4), C5_5 = sum(C5_5), C5_6 = sum(C5_6)),
  by = byvars]

dt_sub_collapsed[,merge_name := ISO_3][,region_agg := ""]
dt_sub_collapsed <- rbind(dt_sub_collapsed, dt_sub_subnat)


# Convert to correct date format
dt_sub_collapsed[, date := format(dt_sub_collapsed$date, "%d.%m.%Y")]

# Merge on location IDs
dt_sub_collapsed <- merge(dt_sub_collapsed, locations, by.x = 'merge_name', by.y = 'merge_name', all.x=T, all.y=F)
dt_sub_collapsed[,merge_name := NULL]

# Separate missing locations
dt_sub_collapsed_missing <- dt_sub_collapsed[is.na(location_id)]

# Clean up
dt_sub_collapsed <- dt_sub_collapsed[, `:=`(ISO_3 = NULL, country_agg = NULL)]
setcolorder(dt_sub_collapsed, c('location_id', 'location_name', 'date'))
dt_sub_collapsed <- dt_sub_collapsed[!is.na(location_id)]


# Rename survey question columns
setnames(dt_sub_collapsed, old = old_names, new = new_names)

# Create copy for no_public excluded
dt_sub_collapsed_exclude <- copy(dt_sub_collapsed)

# Calculate ratios
dt_sub_collapsed[, N := mask_7days_all + mask_7days_most + mask_7days_half + 
                   mask_7days_some + mask_7days_none + mask_7days_no_public]
dt_sub_collapsed[, `:=`(
  prop_mask_7days_all = mask_7days_all / N, prop_mask_7days_most = mask_7days_most / N, 
  prop_mask_7days_half = mask_7days_half / N, prop_mask_7days_some = mask_7days_some / N, 
  prop_mask_7days_none = mask_7days_none / N, prop_mask_7days_no_public = mask_7days_no_public / N)]

dt_sub_collapsed_exclude[, N := mask_7days_all + mask_7days_most + mask_7days_half + 
                           mask_7days_some + mask_7days_none]
dt_sub_collapsed_exclude[, `:=`(
  prop_mask_7days_all = mask_7days_all / N, prop_mask_7days_most = mask_7days_most / N, 
  prop_mask_7days_half = mask_7days_half / N, prop_mask_7days_some = mask_7days_some / N, 
  prop_mask_7days_none = mask_7days_none / N)]

fwrite(dt_sub_collapsed, 
       'FILEPATH/mask_ts_tabulations.csv')

## Find tetrachoric correlation between mask use and >20 contacts ##
# Request from Emm in the paper.
# rtet = cos (180/(1 + √(BC/AD)) (https://www.statisticshowto.com/tetrachoric-correlation/#:~:text=The%20tetrachoric%20correlation%20coefficient%20r,1%E2%80%9D%20represents%20a%20perfect%20agreement.)
dt_sub$contacts20 <- ifelse(dt_sub$contacts_24h == ">20", "g20","l20")
dt_sub$mask <- ifelse(dt_sub$C5 == 1, "mask","no mask")

t <- table(dt_sub$contacts20, dt_sub$mask)
a <- t[1,1] / 10000
b <- t[1,2] / 10000
c <- t[2,1] / 10000
d <- t[2,2] / 10000

rtet <- cos(180 / (1 + sqrt(b*c/a/d)))
rtet
