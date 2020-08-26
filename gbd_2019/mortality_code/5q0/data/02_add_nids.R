###############################################
## Purpose: Add NIDs
################################################

## configure R
rm(list=ls())
library(data.table)
library(haven)
library(RMySQL)
library(reshape2)
library(foreign)
library(readr)

## set directories
## h: mort-data root dir
if (Sys.info()[1] == 'Windows') {
  username <- Sys.getenv("USERNAME")
  root <- "FILEPATH"
  h <<- "FILEPATH"
} else {
  username <- Sys.getenv("USER")
  root <- "FILEPATH"
  h <<- Sys.getenv("HOME")
}

args <- commandArgs(trailingOnly = T)
new_run_id <- as.numeric(args[1])
master_folder <- "FILEPATH"
input_folder <- "FILEPATH"
output_folder <- "FILEPATH"

# load get paths
source(paste0(h, "FILEPATH/get_paths.R"))

## set up locations
locations <- fread(paste0(input_folder, "/loc_map.csv"))
locs2017 <- locations[, .(ihme_loc_id, location_id, location_name)]
locs2016 <- fread(paste0(input_folder, "/loc_map_2016.csv"))
locs2015 <- fread(paste0(input_folder, "/loc_map_2015.csv"))

# get mort_ids
mortids <- fread(paste0(input_folder, "/source_type_ids.csv"))
methods <- fread(paste0(input_folder, "/method_ids.csv"))

data <- fread(paste0(output_folder, "/cac_output.csv"), colClasses = list("character" = "source.date"))
data[, dupvar := 0]

cols_to_keep <- c("ihme_loc_id","year","log10.sd.q5", "q5","source","in.direct","outlier","shock","ptid","source.date", "microdata")
dup_by_vars1 <- cols_to_keep[!cols_to_keep %in% c("log10.sd.q5", "q5", "source", "ptid", "dupvar")]
dup_by_vars2 <- cols_to_keep[!cols_to_keep %in% c("log10.sd.q5", "source", "ptid", "dupvar")]
cols_to_keep_with_nid <- c(cols_to_keep, c("NID", "underlying_NID", "filename"))

raw_nonids <- data[, cols_to_keep, with=FALSE]

dupes <- raw_nonids[, dupvar := as.integer(1L * (.N > 1L), by = dup_by_vars1)]
dupes <- dupes[dupvar == 1,]
dupes0 <- raw_nonids[, dupvar := as.integer(1L * (.N > 1L), by = dup_by_vars2)]
dupes0 <- dupes0[dupvar == 1,]

raw_withNIDs <- data[, cols_to_keep_with_nid, with=FALSE]

raw <- merge(raw_nonids, raw_withNIDs, by = cols_to_keep, all.x = TRUE)
raw <- raw[!(floor(year) > 2017),]
raw[, year2 := floor(year) + 0.5]
setnames(raw, "NID", "nid_from_c_all_child")
raw[, nid_from_c_all_child := as.integer(nid_from_c_all_child)]

raw[nid_from_c_all_child == 18812, nid_from_c_all_child := 18815]
raw[nid_from_c_all_child == 237445, nid_from_c_all_child := 237414]
raw[nid_from_c_all_child == 237681, nid_from_c_all_child := 237659]
raw[nid_from_c_all_child == 237478, nid_from_c_all_child := 121922]
raw[nid_from_c_all_child == 0, nid_from_c_all_child := NA]

raw[, source.date2 := as.numeric(source.date)]
raw[, source.date3 := floor(source.date2)]
raw[source.date3 != source.date2, source.date := NA]
raw[, c("source.date2", "source.date3") := NULL]

split <- setDT(colsplit(raw[is.na(nid_from_c_all_child) & is.na(source.date)]$source, 
                        " " , 
                        names = c("source1", "source2", "source3", "source4", "source5")))
split[, source.date := ""]
split[, yearrange := ""]
split[source2 %like% "-",  yearrange := source2]
split[yearrange == "CDC-RHS", yearrange := ""]
split[!is.na(as.integer(source2)), source.date := source2]
split[source.date == "", source.date := yearrange]
split[, yearrange := ""]
split[source3 %like% "-" , yearrange := source3]
split[!is.na(as.integer(source3)), source.date := source3]
split[source.date == "", source.date := yearrange]
split[source4=="5017", source4 := ""]
split[!is.na(as.integer(source4)), source.date := source4]
split[!is.na(as.integer(source5)) & split$source.date == "", source.date := source5]
raw[is.na(nid_from_c_all_child) & is.na(source.date), source.date := split$source.date]

enddate <- setDT(colsplit(raw[is.na(nid_from_c_all_child) & source.date %like% "-"]$source.date,  "-", names = c("start", "end")))
raw[is.na(nid_from_c_all_child) & source.date %like% "-", source.date := enddate$end]

#Bring in NIDS for raw sources
nids1 <- fread(paste0(root, "FILEPATH"))
nids1[, nid := as.integer(nid)]
nids1[, ParentNID := as.integer(ParentNID)]
setnames(nids1, c("year", "indirect"), c("source.date", "in.direct"))

nids2 <- fread(paste0(root, "FILEPATH"))
nids2 <- unique(nids2[,c("ihme_loc_id", "year", "source_type", "deaths_source" , "ParentNID", "nid")])
setnames(nids2, c("year", "source_type"), c("source.date", "source"))
nids2[, nid := as.integer(nid)]
nids2[, ParentNID := as.integer(ParentNID)]
nids1 <- rbind(nids1, nids2, fill =TRUE)
nids1[, source := tolower(source)]

for (survey in c("ais", "cdc rhs", "dhs", "dhs sp", "ifhs", "imira", "lsms", "mics" , "mis", "papchild", "papfam")) {
  nids1[source %like% survey, source := paste0(survey, " " , source.date)]
}

for (survey in c("ais", "dhs", "dhs sp", "ifhs", "imira", "lsms", "mis", "papchild", "papfam")) {
  nids1[source %like% survey & in.direct == "indirect", source := survey]
}

nids1 <- unique(nids1[, c("ihme_loc_id", "source.date", "source", "deaths_source", "in.direct", "ParentNID", "nid")])
nids1[, dupvar := 1L * (.N > 1L), by = c("ihme_loc_id","source", "source.date", "in.direct")]

for (id in c(106686,12105,43152, 43158, 25100, 133731,12146)) {
  nids1 <- nids1[!(nids1$nid == id & nids1$dupvar == 1),]
}

nids1 <- nids1[!(nids1$dupvar == 1 & is.na(deaths_source))]
nids1[, c("dupvar") := NULL]

# Merge NIDS onto raw sources
raw[, source := tolower(source)]
merged <- merge(raw, nids1, by= c("ihme_loc_id" ,"source", "source.date"), all.x = TRUE)
merged[,in.direct.y := NULL]
names(merged)[names(merged) == 'in.direct.x'] <- 'in.direct'

merged[, source.date := as.integer(source.date)]
merged[, nid_from_c_all_child := as.integer(nid_from_c_all_child)]

merged[source == "vr - transmonee" & source.date == 1999 & ihme_loc_id == "TKM" & is.na(merged$nid),  nid := 139170 ]
merged[source == "vr - transmonee" & source.date == 1999 & ihme_loc_id == "TKM" & is.na(merged$ParentNID),  ParentNID := nid_from_c_all_child ]
merged[source == "vr - transmonee" & source.date == 1993 & ihme_loc_id == "GEO" & is.na(merged$nid),  nid := 93506 ]
merged[source == "vr - transmonee" & source.date == 1993 & ihme_loc_id == "GEO" & is.na(merged$ParentNID),  ParentNID := nid_from_c_all_child ]

############   #UN DYB           ##############################################################################
merged[nid_from_c_all_child == 931 & nid ==913,   nid_from_c_all_child:= 913]
merged[nid_from_c_all_child == 140201 & nid_from_c_all_child != 0 & !is.na(nid_from_c_all_child),   ParentNID := nid_from_c_all_child]
merged[nid == 11168 & nid_from_c_all_child != 0 & !is.na(nid_from_c_all_child),   nid := nid_from_c_all_child]
merged[nid_from_c_all_child == 3356 & is.na(nid),   nid := nid_from_c_all_child]
merged[nid_from_c_all_child == 3356 & is.na(ParentNID),   ParentNID := 140201]
merged[nid_from_c_all_child == 11123 & is.na(ParentNID),   ParentNID := 140201]
merged[nid_from_c_all_child == 121922 & ParentNID == 140966, ParentNID := nid_from_c_all_child]
merged[nid_from_c_all_child == 237659, ParentNID := 237659]

#########################################################           other      VR             ######################
for (cid in c(242281, 242292, 242293, 242294, 242301, 242303, 242315, 242317, 242319, 242320 )) {
  merged[nid_from_c_all_child == cid,   nid := cid]
  merged[nid_from_c_all_child == cid,   ParentNID := NA]
}
for (vid in c(279537, 222681, 222682, 222683, 222684, 284015, 284016, 284017)) {
  merged[nid_from_c_all_child == vid & is.na(nid),   nid := nid_from_c_all_child]
}
#########################################################################################
########################################################################################
## replace NID with prepped data nid for sources that Do no require ParentNID
merged[nid_from_c_all_child != 0 & !is.na(nid_from_c_all_child) & !source %in% c("vr", "vr - transmone", "undyb census"),   nid := nid_from_c_all_child]
####################################################################################################################
#####################################################################################################################

#N. Ireland
merged[ihme_loc_id == "GBR_433" & source.date < 2013 & is.na(nid) , nid:= 121426 ]
merged[ihme_loc_id == "GBR_433" & source.date == 2013 & is.na(nid), nid:= 205458 ]
merged[ihme_loc_id == "GBR_433" & source.date == 2013 & is.na(nid), ParentNID:= 287600 ]
# Scotland
merged[ihme_loc_id == "GBR_434" & source.date < 2013 & is.na(nid), nid:= 121426 ]
merged[ihme_loc_id == "GBR_434" & source.date == 2013 & is.na(nid), nid:= 205459 ]
merged[ihme_loc_id == "GBR_434" & source.date == 2013 & is.na(nid), ParentNID:= 287600 ]
merged[ihme_loc_id == "GBR_434" & source.date == 2013 & is.na(nid), nid:= 268422 ]
merged[ihme_loc_id == "GBR_434" & source.date == 2013 & is.na(nid), ParentNID:= 287600 ]

add.nids <-fread(paste0(root,"FILEPATH"))
add.nids <- add.nids[, .(Nid, Year)]
setnames(add.nids, "Year", "source.date")
merged_subset <- merged[ihme_loc_id %like% "GBR_" & source == "vr" & is.na(nid),]
merged_sub <- merge(merged_subset, add.nids, by= "source.date", all.x = TRUE)
merged_sub[, nid := Nid]
merged_sub[, Nid := NULL]

merged <- merged[!(ihme_loc_id %like% "GBR_" & source == "vr" & is.na(nid)),]
merged <- rbind(merged, merged_sub)

merged[ !(is.na(ParentNID)), NID := ParentNID]
merged[is.na(NID) & !(is.na(nid)), NID:= nid]
merged[!(is.na(nid)) & !(is.na(ParentNID)), underlying_nid := nid]
merged[, c("ParentNID", "nid") := NULL]
setnames(merged, "NID", "nid")

add.nids <- fread(paste0(root,"FILEPATH"))
add.nids <- add.nids[, .(year, nid)]
names(add.nids)[names(add.nids) == 'year'] <- 'source.date'
names(add.nids)[names(add.nids) == 'nid'] <- 'Nid'
merged_subset <- merged[ihme_loc_id %like% "IND_" & source == "vr" & is.na(nid),]
merged_sub <- merge(merged_subset, add.nids, by= "source.date", all.x = TRUE)
merged_sub[, nid := Nid]
merged_sub[, Nid := NULL]
merged <- merged[!(ihme_loc_id %like% "IND_" & source == "vr" & is.na(nid)),]
merged <- rbind(merged, merged_sub)

add.nids <- fread(paste0(root, "FILEPATH"))
add.nids <- add.nids[, .(year, nid)]
names(add.nids)[names(add.nids) == 'year'] <- 'source.date'
names(add.nids)[names(add.nids) == 'nid'] <- 'Nid'
merged_subset <- merged[ihme_loc_id %like% "USA_" & source == "vr" & is.na(nid),]
merged_sub <- merge(merged_subset, add.nids, by= "source.date", all.x = TRUE)
merged_sub[, nid := Nid]
merged_sub[, Nid := NULL]
merged <- merged[!(ihme_loc_id %like% "USA_" & source == "vr" & is.na(nid)),]
merged <- rbind(merged, merged_sub)

add.nids <- fread(paste0(root, "FILEPATH"))
merged_subset <- merged[ihme_loc_id %like% "BRA" & source == "vr" & is.na(nid) ,]
merged_sub <-  merge(merged_subset, add.nids, by= "source.date", all.x = TRUE)
merged_sub[, nid := Nid]
merged_sub[, Nid := NULL]
merged <- merged[!(ihme_loc_id %like% "BRA" & source == "vr" & is.na(nid)) ,]
merged <- rbind(merged, merged_sub)

merged[ihme_loc_id %like% "ZAF_" & source == "vr" & source.date == 2014 & is.na(nid) , nid := 267740 ]

add.nids <- fread(paste0(root, "FILEPATH"))
merged_subset <- merged[source=="vr" & is.na(nid) & is.na(underlying_nid) & !(ihme_loc_id %like% "_"),]
merged_subset[, source.date := as.integer(source.date)]
merged_sub <- merge(merged_subset, add.nids, by = c("ihme_loc_id", "source.date"), all.x = TRUE)
merged_sub[, nid := Nid]
merged_sub[, underlying_nid := Unid]
merged_sub[, c("Nid", "Unid") := NULL]
merged <- merged[!(source=="vr" & is.na(nid) & is.na(underlying_nid) & !(ihme_loc_id %like% "_")),]
merged <- rbind(merged, merged_sub)

add.nids <- setDT(read.dta(paste0(root, "FILEPATH")))
add.nids <- add.nids[!deaths_nid == "", .(ihme_loc_id, year, deaths_source, source_type, deaths_nid)]
add.nids[, deaths_nid := as.integer(deaths_nid)]
add.nids[, source_type := tolower(source_type)]
add.nids <- unique(add.nids)
setnames(add.nids, c("year", "source_type"), c("source.date", "source"))

merged_subset <- merged[is.na(nid),]
merged_subset[ihme_loc_id %like% "MEX" & source == "vr", source:= "mex_vr_post2011"]
merged_sub <- merge(merged_subset, add.nids, by = c("ihme_loc_id", "source", "source.date","deaths_source"), all.x = TRUE)
merged_sub[, nid := deaths_nid]
merged_sub[, deaths_nid := NULL]

merged <- merged[!is.na(nid),]
merged <- rbind(merged, merged_sub)

merged[ihme_loc_id == "KOR" & source == "vr" & source.date ==1966 & is.na(nid), nid:= 140574]
merged[ihme_loc_id == "SRB" & source == "vr" & source.date ==2014 & is.na(nid), underlying_nid:= 268414]
merged[ihme_loc_id == "SRB" & source == "vr" & source.date ==2014 & is.na(nid), nid:= 265138 ]
merged[ihme_loc_id == "MEX" & source %like% "vr" & source.date == 2015 & is.na(nid), nid:=281783]

add.nids <- fread(paste0(root, "FILEPATH"))
add.nids <- add.nids[!(is.na(underlying_nid)), .(ihme, source, source.date, nid, underlying_nid)]
names(add.nids)[names(add.nids) == 'ihme'] <- 'ihme_loc_id'
merged_subset <- merged[nid %in% c(237414,237659,287600), ]
merged_subset[nid==underlying_nid, underlying_nid :=NA]
merged_sub <- merge(merged_subset, add.nids, by= c("ihme_loc_id","source","source.date","nid"), all.x = TRUE )
merged_sub[is.na(underlying_nid.x), underlying_nid.x := underlying_nid.y]
names(merged_sub)[names(merged_sub) == 'underlying_nid.x'] <- 'underlying_nid'
merged_sub[, underlying_nid.y := NULL]
merged <- merged[!(nid %in% c(237414,237659,287600)), ]
merged <- rbind(merged, merged_sub)

merged[nid_from_c_all_child == 237659 & ihme_loc_id == "ISR" & source.date =="2014", nid := 284337]
merged[nid_from_c_all_child == 237659 & ihme_loc_id == "MAR" & source.date =="2007", nid := 138017]


########################################################## done with VR ################################################################
merged[ihme_loc_id == "BOL" & source == "census" & source.date == 2012 & nid == 237659, nid:= 152786]
merged[ihme_loc_id == "BTN" & source == "census" & source.date == 2005 & nid == 237659, nid:= 1175]
merged[ihme_loc_id == "NAM" & source == "census" & source.date == 2011 & nid == 237659, nid:= 134132]

#IDN Census
merged[ihme_loc_id %like% "IDN_" & source == "census" & source.date == 1980 & is.na(nid), nid:= 22645]
merged[ihme_loc_id %like% "IDN_" & source == "census" & source.date == 1990 & is.na(nid), nid:= 22670]
merged[ihme_loc_id %like% "IDN_" & source == "census" & source.date == 2000 & is.na(nid), nid:= 22674]
merged[ihme_loc_id %like% "IDN_" & source == "census" & source.date == 2010 & is.na(nid), nid:= 91740]
merged[ihme_loc_id %like% "IDN_" & source %like% "idn census" & is.na(nid), nid:= 22674]

#ZAF Census
merged[ihme_loc_id %like% "ZAF" & source == "census" & source.date == 2011 & is.na(nid), nid:= 12146]
merged[ihme_loc_id %like% "ZAF" & source == "census" & source.date == 2006 & is.na(nid), nid:= 43158]
merged[ihme_loc_id %like% "ZAF" & source == "census" & source.date == 2000 & is.na(nid), nid:= 43152]
merged[ihme_loc_id %like% "ZAF" & source == "census" & source.date == 2001 & is.na(nid), nid:= 43152]

#IND census
merged[ihme_loc_id %like% "IND" & source == "5291#ind census 1981" & is.na(nid), nid:= 5291]
merged[ihme_loc_id %like% "IND" & source == "60372#ind_census_2011" & is.na(nid), nid:= 60372]
merged[ihme_loc_id %like% "IND" & source == "ind_census_2001_state_5314" & is.na(nid), nid:= 5314]

#Remaining Census
merged[ihme_loc_id == "BWA" & source == "census" & source.date == 2001 & is.na(nid), nid:= 1430]
merged[ihme_loc_id == "PRK" & source == "census" & source.date == 1993 & is.na(nid), nid:= 58750]
merged[ihme_loc_id == "GHA" & source == "census" & source.date == 2010 & nid == 237659, underlying_nid := 218222 ]
merged[ihme_loc_id == "JAM" & source == "census" & source.date == 2012 & nid == 237659, underlying_nid := 153004 ]
merged[ihme_loc_id == "MLI" & source == "census" & source.date == 2009 & nid == 237659, underlying_nid := 34589 ]
merged[ihme_loc_id == "PAK" & source == "census" & source.date == 2007 & nid == 237659, underlying_nid := 293181 ]
merged[ihme_loc_id == "SSD" & source == "census" & source.date == 2008 & nid == 237659, underlying_nid := 24134 ]
merged[ihme_loc_id == "STP" & source == "census" & source.date == 2012 & nid == 237659, underlying_nid := 218790 ]
merged[ihme_loc_id == "WSM" & source == "census" & source.date == 2011 & nid == 237659, underlying_nid := 139682 ]
merged[ihme_loc_id == "ZMB" & source == "census" & source.date == 2010 & nid == 237659, underlying_nid := 62703 ]
################################################################# done with census##########################################################

#DHS
#Peru Continuous DHS
merged[ihme_loc_id == "PER" & source == "dhs 2003-2008", nid:= 275090]
merged[ihme_loc_id == "PER" & source == "dhs 2004-2008", nid:= 275090]
merged[ihme_loc_id == "PER" & source %like% "dhs" & source.date == "2006", nid:= 275090]
merged[ihme_loc_id == "PER" & source %like% "dhs" & source.date == "2009", nid:= 270404]
merged[ihme_loc_id == "PER" & source %like% "dhs" & source.date == "2010", nid:= 270469]
merged[ihme_loc_id == "PER" & source %like% "dhs" & source.date == "2011", nid:= 270470]
merged[ihme_loc_id == "PER" & source %like% "dhs" & source.date == "2012", nid:= 270471]
merged[ihme_loc_id == "PER" & source %like% "dhs" & source.date == "2014", nid:= 210182]

#ind dhs 2015
merged[ source %like% "ind dhs 2015" & source.date == 2015 & is.na(nid) , nid := 157050]

#IDN direct
# As per conversation with Amber setting it to the latest IDN DHS nid
merged[ source == "idn dhs" & is.na(nid), nid := 76705 ]

##################################################### done with DHS ##########################################

merged[ source == 'idn fls' & is.na(nid), nid := 264956 ]

merged[ihme_loc_id == "IDN" & source == "survey" & source.date == "1964" & is.na(nid), nid := 6550]
merged[ihme_loc_id %like% "IDN_" & source %like% "census" & source.date == "2004" & is.na(nid), nid := 6904]

#EFLS
merged[ source %like% "efls" & is.na(nid), nid:= 219201]
#MCHS
merged[source == "china provincial mchs" & source.date == "2000" & is.na(nid), nid:= 138539]
#SUPAS
merged[ source %like% "supas" & source.date == "1985" & is.na(nid) , nid := 6505 ]
merged[ source %like% "supas" & source.date == "1995" & is.na(nid) , nid := 6535 ]
merged[ source %like% "supas" & source.date == "2005" & is.na(nid) , nid := 6547 ]
#ZAF IPUMS
merged[ihme_loc_id %like% "ZAF" & source %like% "ipums" & source.date == "2007" & is.na(nid), nid:= 43158]
#PMA
merged[ source == "pma_gha_2014_2015" & is.na(nid), nid := 256244 ]
merged[ source == "ken_pma_2015" & is.na(nid), nid := 256365 ]
merged[ source == "uga_pma_2015" & is.na(nid), nid := 256201 ]
#DSP
merged[ ihme_loc_id %like% "CHN" & source == "dsp" & source.date == "2015" & is.na(nid), nid := 270013 ]
#PHS
merged[ source == "249999_eri_phs_2010" & is.na(nid), nid := 249999]
#HDS
merged[ ihme_loc_id %like% "IND" & source %like% "hds" & source.date == "2005" & is.na(nid), nid := 26919 ]
#DLHS
merged[ ihme_loc_id %like% "IND" & source %like% "dlhs" & source.date == "2008" & is.na(nid), nid := 23258 ]
merged[ ihme_loc_id %like% "IND" & source %like% "dlhs" & source.date == "2003" & is.na(nid), nid := 23219 ]
merged[ ihme_loc_id %like% "IND" & source %like% "dlhs" & source.date == "1999" & is.na(nid), nid :=23183 ]

#BGD SRS
merged[ ihme_loc_id == "BGD" & source == "srs" & source.date == "2001" & nid == 57646, nid := 57666 ]
merged[ ihme_loc_id == "BGD" & source == "srs" & source.date == "2002" & nid == 57646, nid := 57666 ]


#ARE MOH survey 1998
merged[ ihme_loc_id %like% "ARE" & source == "moh survey" & source.date == "1998" & is.na(nid), nid := 93395]
#PNG
merged[ ihme_loc_id %like% "PNG" & source == "dhs preliminary report, via alan" & source.date == "2006" & is.na(nid), nid := 105069]
#HTI 1972
merged[ ihme_loc_id == "HTI" & source == "survey" & source.date == "1972" & is.na(nid), nid :=93404]
#KHM 1959
merged[ ihme_loc_id == "KHM" & source == "survey" & source.date == "1959" & is.na(nid), nid :=148325]
#TGO 1961
merged[ ihme_loc_id == "TGO" & source == "survey" & source.date == "1961"& is.na(nid), nid :=12873]
#TUR 1967
merged[ ihme_loc_id == "TUR" & source == "survey" & source.date == "1967" & is.na(nid), nid :=44859]
#TZA 1973
merged[ ihme_loc_id == "TZA" & source == "survey" & source.date == "1973" & is.na(nid), nid :=12674]
#ZAF 2006.69
merged[ ihme_loc_id == "ZAF" & source == "survey" & year == 2006.69 & is.na(nid), nid:=150015]


#NRU 2002
merged[ihme_loc_id == "NRU" & source == "vital registration, www.spc.int", nid:=8993]

#Tokelau report 1980
merged[ihme_loc_id == "TKL" & source == "Tokelau_Annual_Report_on_Health_Services_1982", underlying_nid:=375489]


#GBD 2017 subnat VR
add.nids <- fread(paste0(root, "FILEPATH"))
names(add.nids)[names(add.nids) == "year_id"] = "year"
merged_subset <- merged[ihme_loc_id %in% unique(add.nids$ihme_loc_id),]
merged_subset[, nid := NULL]
merged_sub <- merge(merged_subset, add.nids, by= c("ihme_loc_id", "year") , all.x = TRUE)
merged <- merged[!(ihme_loc_id %in% unique(add.nids$ihme_loc_id)),]
merged <- rbind(merged, merged_sub)

merged[filename == "FILEPATH" & ihme_loc_id == "MLI", nid := 218587]
merged[nid == 30389, nid := 30394]
merged[nid == 43106, nid := 125230]
merged[nid == 58190, nid := 58191]
merged[nid == 150015, nid := 25100]

# Standardize "NA" rows
merged[underlying_NID == ".", underlying_NID := NA]
merged[underlying_NID == "", underlying_NID := NA]

merged[is.na(nid) & !is.na(nid_from_c_all_child), nid := nid_from_c_all_child]
merged[is.na(underlying_nid) & !is.na(underlying_NID), underlying_nid := underlying_NID]

# Null checks for NID 
if(nrow(merged[is.na(nid)])>0) stop("You have sources that don't have NID.")

write_csv(merged, paste0(output_folder,"/cac_output_w_nids.csv"))

merged$flag <- 0
merged$flag[merged$underlying_NID != merged$underlying_nid] <- 1
merged$flag[merged$nid != merged$nid_from_c_all_child] <- 1
nids_to_check <- merged[merged$flag == 1]
nids_to_check <- nids_to_check[, c("ihme_loc_id", "source", "source.date", "year", "in.direct", "outlier", "nid_from_c_all_child", "underlying_NID", "deaths_source", "nid", "underlying_nid", "filename")]
names(nids_to_check)[names(nids_to_check) == "underlying_NID"] <- "cac_underlying_nid"
names(nids_to_check)[names(nids_to_check) == "nid_from_c_all_child"] <- "cac_nid"