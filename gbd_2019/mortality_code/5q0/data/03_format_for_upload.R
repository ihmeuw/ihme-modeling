###############################################
## Purpose: Transform 5q0 data for upload to database
################################################

## configure R
library(data.table)
library(haven)
library(RMySQL)
library(reshape2)
library(foreign)
library(ini)

## set directories
## h: mort-data root dir
if (Sys.info()[1] == 'Windows') {
  username <- Sys.getenv("USERNAME")
  library(readr)
  root <- "FILEPATH"
  h <<- "FILEPATH"
} else {
  username <- Sys.getenv("USER")
  library(readr)
  root <- "FILEPATH"
  h <<- Sys.getenv("HOME")
}

args <- commandArgs(trailingOnly = T)
new_run_id <- as.numeric(args[1])
master_folder <- "FILEPATH"
input_folder <- "FILEPATH"
output_folder <- "FILEPATH"

library(mortdb, lib = paste0(root,"FILEPATH"))
# load get paths
source(paste0(h, "FILEPATH/get_paths.R"))

## set up locations
locations <- fread(paste0(input_folder, "/loc_map.csv"))
locs2017 <- locations[, c("ihme_loc_id", "location_id", "location_name")]
locs2016 <- fread(paste0(input_folder, "/loc_map_2016.csv"))
locs2015 <- fread(paste0(input_folder, "/loc_map_2015.csv"))

# get mort_ids
mortids <- fread(paste0(input_folder, "/source_type_ids.csv"))
methods <- fread(paste0(input_folder, "/method_ids.csv"))
methods$method_short[1] <- "Direct"

merged <- fread(paste0(output_folder, "/cac_output_w_nids.csv"))

merged[filename == "d00_compiled_deaths.dta", nid := nid_from_c_all_child]
merged[filename == "d00_compiled_deaths.dta", underlying_nid := underlying_NID]

merged[source %like% "srs" & !(source %like% "dss"), source := "srs"]
merged[source %like% "vr", source := "vr"]

setnames(merged, "q5", "raw")

merged <- merge(merged, locs2016, by = c("ihme_loc_id"), all.x = TRUE)

#Manage type_short to merge on type_id
merged[, type_short := tolower(source)]

#srs
merged[source %like% "srs" , type_short := "srs" ]
merged[source %like% "pak_demographic_survey" , type_short := "srs" ]
#dss
merged[source %like% "dss" , type_short := "dss" ]
#census
for (cen in c("census", "ipums")) {
  merged[source %like% cen , type_short := "census" ]
}
#dhs
for (sdhs in c("ndhs", "dhs sp", "dhs_sp", "dhs itr", "126952#nic_dhs_2011_2012", "135803#phl_dhs_2011", "palestine bureau of statistics dhs", "164898#zaf_dhs_1987", "20596#pse_dhs_2004", "20597#png_dhs_1991" , "20599", "223669#tur 2013-2014 dhs report", "tur_dhs", "90705")) {
  merged[source %like% sdhs , type_short := "other dhs" ]
}
merged[source %like% "dhs" & type_short != "other dhs" , type_short := "standard dhs" ]
#rhs
merged[source %like% "rhs"  , type_short := "rhs" ]
merged[source %like% "reproductive health survey"  , type_short := "rhs" ]
#papfam
for (fam in c("papfam", "pan arab project for family", "gulf family", "oman family")) {
  merged[source %like% fam , type_short := "papfam" ]
}
#papchild
for (child in c("papchild", "pan arab project for child", "gulf child", "oman child", "_chs_", "7503#kwt_1987_child_health_survey", "905#bhr 1989 child health survey report")) {
  merged[source %like% child , type_short := "papchild" ]
}
#mics
merged[source %like% "mics"  , type_short := "mics" ]
merged[source %like% "10001#pse_hs_2000"  , type_short := "mics" ]
#wfs
merged[source %like% "wfs"  , type_short := "wfs" ]
#lsms
merged[source %like% "lsms"  , type_short := "lsms" ]
merged[source %like% "living standards measurement"  , type_short := "lsms" ]
#mis
merged[source %like% "mis"  , type_short := "mis" ]
#ais
merged[source %like% "ais"  , type_short := "ais" ]


## merge on get_mort_ids to get source_type_id
merged <- merge(mortids, merged, by="type_short", all.y = TRUE)
merged[, type_short := NULL]
merged[is.na(source_type_id), source_type_id := 16]


## need to merge on get_mort_ids to get method_id
## Note that direct in method_id refers to the fact that it is VR SRS
## This use of direct is different from the "direct/indirect" categorization in the indirect variable refers to the processing methods within the birth history method.
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

long[location_id == 44892 & year_id ==2001, outlier := 1]
long[location_id ==154 & year_id %in% c(2009,2013), outlier := 1] #TUN
long[location_id ==136 & year_id == 192, outlier := 1] #PRY

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

long = long[!((nid == 218590)&(source=="mis"))]

#duplicates check
if(nrow(long[duplicated(long[, .(nid, underlying_nid,location_id, year_id, method_id, viz_year)])]) > 0){
  stop("You have duplicates across unique identifiers")
}

write_csv(long, paste0(output_folder,"/5q0_data.csv"))

#find and save the list of sources where we have microdata for SBH DHS but not for CBH
cbh <- long[microdata == 1 & method_id %in% c(3)]
agg <- long[microdata == 0]
sbh <- long[microdata == 1 & method_id %in% c(4,10)]

intercept_agg <- merge(sbh, agg, by = c("year_id", "location_id", "source_year"))
intercept_not_cbh <- sbh[tolower(source) == "dhs" & !(nid %in% unique(cbh$nid))]
intercept_not_cbh <- intercept_agg[tolower(source.x) == "dhs" & grepl('dhs', tolower(source.y)) & !(nid.x %in% unique(cbh$nid))]

intercept_agg_2 <- merge(cbh, agg, by = c("year_id", "location_id", "source_year"))
intercept_not_sbh <- intercept_agg_2[tolower(source.x) == "dhs" & grepl('dhs', tolower(source.y)) & !(nid.x %in% unique(sbh$nid))]

write_csv(intercept_not_cbh, paste0(output_folder, "/microdata_for_sbh_dhs_only.csv"))