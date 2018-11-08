###############################################
## Format_for_upload
## Purpose: Transform GBD 2017 5q0 data for upload to database
################################################

## configure R
library(data.table)
library(haven)
library(RMySQL)
library(reshape2)
library(foreign)
library(ini)
library(readr)
library(mortdb)

master_folder <-"FILEPATH"
input_folder <- paste0(master_folder, "/inputs")
output_folder <- paste0(master_folder, "/outputs")

## set up locations
locations <- fread(paste0(input_folder, "/loc_map.csv"))
locs2017 <- locations[, c("ihme_loc_id", "location_id", "location_name")]
locs2016 <- fread(paste0(input_folder, "/loc_map_2016.csv"))
locs2015 <- fread(paste0(input_folder, "/loc_map_2015.csv"))

# get mort_ids
mortids <- fread(paste0(input_folder, "/source_type_ids.csv"))
methods <- fread(paste0(input_folder, "/method_ids.csv"))
methods$method_short[1] <- "Direct"

merged <- fread(paste0(output_folder, "/cac_output.csv"))
setnames(merged, "q5", "raw")
merged <- merge(merged, locs2016, by = c("ihme_loc_id"), all.x = TRUE)

## get source_type_id
merged[, type_short := tolower(source)]
merged <- merge(mortids, merged, by="type_short", all.y = TRUE)
merged[, type_short := NULL]
merged[is.na(source_type_id), source_type_id := 16]


## get method_id
merged[in.direct %like% "indirect", method_short := "SBH"]
merged[in.direct %like% "indirect, MAC only", method_short := "SBH-MAC"]
merged[in.direct == "direct" & microdata == 0, method_short := "Direct"]
merged[in.direct == "direct" & is.na(method_short), method_short := "CBH"]
merged[is.na(method_short), method_short := "Direct"]
merged <- merge(methods, merged, by ="method_short", all.y = TRUE)
merged[, c("method_short", "location_id") := NULL]

merged <- merge(merged, locations[, c('ihme_loc_id', 'location_id')], by = "ihme_loc_id", all.x = TRUE)

long <- melt(merged, measure.vars= c("raw"), variable.name = "estimate_type", value.name="mean")

long[, sex_id := 3]
long[, age_group_id := 1]
long[, year_id := floor(year)]
setnames(long, "year", "viz_year")

dup_vars <- c("year_id", "location_id", "sex_id", "age_group_id", "method_id", "source_type_id", "nid", "underlying_nid", "viz_year", "shock", "outlier")

dupes1 <- long[, dupvar := 1L * (.N > 1L), by= c(dup_vars, "source")]
dupes2 <- long[, dupvar := 1L * (.N > 1L), by= dup_vars]

long <- long[, .SD, .SDcols = c(dup_vars, "mean", "log10.sd.q5", "source", "source.date", "microdata")]
setcolorder(long, c(dup_vars, "mean", "log10.sd.q5", "source", "source.date", "microdata"))
setorder(long, location_id, year_id, sex_id, age_group_id)
setnames(long, c("log10.sd.q5", "source.date"), c("sd", "source_year"))

#find duplicates and save
dups = duplicated(long, by = key(long));
long[, fD := dups | c(tail(dups, -1), FALSE)]
dup <- long[fD == TRUE]
write_csv(dup, paste0(output_folder, "/5q0_duplicates.csv"))

long[, fD := NULL]

dup_vars_no_outlier <- dup_vars[!dup_vars %in% c("shock", "outlier")]
long <- long[order(-outlier, microdata, decreasing=FALSE),]
long <- long[!duplicated(long, by = dup_vars_no_outlier, fromLast=T),]

dedup_cbh <- function(df){
  under_five = df[!(method_id %in% c(1,3) & source_type_id %in% c(6,7,11,14,15))]
  cbh = df[method_id %in% c(1,3) & source_type_id %in% c(6,7,11,14,15)] # if source_type is that of CBH
  cbh_subset = unique(cbh[, c("nid", "underlying_nid", "source_type_id", "method_id")])
  dup_nids = unique(cbh_subset[duplicated(cbh_subset, by = c("nid", "underlying_nid", "source_type_id"))]$nid)
  cbh[, drop:= ifelse(nid %in% dup_nids & microdata == 0, 1, 0)] # if duplicated and report data, then drop
  cbh <- cbh[drop == 0]
  cbh[, drop := NULL]
  df = rbind(under_five, cbh)
  cat(paste0(dup_nids, "(nid) report data dropped \n"))
  return(df)
}

long <- dedup_cbh(long)

write_csv(long, paste0(output_folder,"/5q0_data.csv"))

cbh <- long[microdata == 1 & method_id %in% c(3)]
agg <- long[microdata == 0]
sbh <- long[microdata == 1 & method_id %in% c(4,10)]

intercept_agg <- merge(sbh, agg, by = c("year_id", "location_id", "source_year"))
intercept_not_cbh <- sbh[tolower(source) == "dhs" & !(nid %in% unique(cbh$nid))]
intercept_not_cbh <- intercept_agg[tolower(source.x) == "dhs" & grepl('dhs', tolower(source.y)) & !(nid.x %in% unique(cbh$nid))]

intercept_agg_2 <- merge(cbh, agg, by = c("year_id", "location_id", "source_year"))
intercept_not_sbh <- intercept_agg_2[tolower(source.x) == "dhs" & grepl('dhs', tolower(source.y)) & !(nid.x %in% unique(sbh$nid))]

write_csv(intercept_not_cbh, paste0(output_folder, "/microdata_for_sbh_dhs_only.csv"))