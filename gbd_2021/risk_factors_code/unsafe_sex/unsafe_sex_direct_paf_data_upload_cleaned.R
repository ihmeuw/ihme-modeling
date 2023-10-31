####################################################
## Author: NAME
## Date: DATE
## Description: UNSAFE SEX DIRECT PAF DATA UPLOAD
## Note: once data are uploaded, any save_bulk_outliering
##        is being done in
##        "FILEPATH"
####################################################

# SET-UP
rm(list=ls())
  
if (Sys.info()[1] == "Linux"){
  X <- "FILEPATH"
  X <- "FILEPATH"
} else if (Sys.info()[1] == "Windows"){
  X <- "FILEPATH"
  X <- "FILEPATH"
}

source("FILEPATH")

# SOURCE FUNCTIONS
library(ggplot2)
library(data.table)
library(openxlsx)
library(plyr)
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

date <- gsub("-", "_", Sys.Date())

## HIV RISK FACTOR DIRECT PAF DATA UPLOAD #####
paf_map <- data.table(rei_id = c(140, 170, 170, 170, 170),
                      bundle_id = c(385, 386, 386, 386, 387),
                      paf_name = c("hiv_idu", "hiv_sex_base", "hiv_sex_1_min_p", "hiv_backtransform", "hiv_other"),
                      has_bundle = c(1, 1, 1, 0, 1),
                      step4_2019_xwalk = c(13013, 7073, 13019, NaN, 6173),
                      iterative_bvid = c(25613, 25616, 25616, NaN, 25619),
                      iterative_xwid = c(19910, 19712, 19715, NaN, 19718),
                      acause = c("unsafe_sex"))

paf_map <- paf_map[has_bundle == 1]

## GET GBD 2019 BUNDLE DATA (DID NOT SYNC PRE SWITCH TO ITERATIVE) ####
for (i in 1:nrow(paf_map)) {
  dt2 <- get_bundle_data(paf_map[i, bundle_id], decomp_step = "step4", gbd_round_id = 6, sync = F)
  assign(paste0(paf_map[i, paf_name], "_bundle_2019"), dt2)
}
  # OTHER HAD NO CHANGES IN STEP 4, PULL EARLIER BUNDLE
dt2 <- get_bundle_data(387, decomp_step = "step2", gbd_round_id = 6, sync = F)
assign(paste0(paf_map[bundle_id == 387, paf_name], "_bundle_2019"), dt2)

## UPLOAD BUNDLE DATA TO GBD 2020 ITERATIVE ####
for (i in unique(paf_map$bundle_id)) {
  name <- paf_map[bundle_id == i, paf_name][1]
  print(name)
  bundle_id <- i
  acause <- unique(paf_map$acause)
  upload_data <- get(paste0(name, "_bundle_2019"))
  upload_path <- paste0(X, "FILEPATH")
  write.xlsx(upload_data, upload_path, sheetName = "extraction")
  result <- upload_bundle_data(i, decomp_step = "iterative", gbd_round_id = 7, filepath = upload_path)
  print(result)
}

# CHECK THAT SEQS WERE MAINTAINED
test <- get_bundle_data(387, decomp_step = "iterative", gbd_round_id = 7)
setdiff(test$seq, hiv_other_bundle_2019$seq) 

## SAVE BUNDLE VERSIONS ####
for (i in unique(paf_map$bundle_id)) {
  name <- paf_map[bundle_id == i, paf_name][1]
  print(name)
  bundle_id <- i
  acause <- unique(paf_map$acause)
  result <- save_bundle_version(bundle_id, gbd_round_id = 7, decomp_step = "iterative", include_clinical = "None")
  print(paste0("Iterative bundle data for ", name, " saved to bundle_version_id ", result$bundle_version_id))
}
## OUTPUT ####
# [1] "hiv_idu"
# [1] "Iterative bundle data for hiv_idu saved to bundle_version_id 25613"
# [1] "hiv_sex_base"
# [1] "Iterative bundle data for hiv_sex_base saved to bundle_version_id 25616"
# [1] "hiv_other"
# [1] "Iterative bundle data for hiv_other saved to bundle_version_id 25619"
#####

## PULL GBD 2019 BEST CROSSWALK VERSIONS ####
for (i in 1:nrow(paf_map)) {
  dt2 <- get_crosswalk_version(paf_map[i, step4_2019_xwalk])
  assign(paste0(paf_map[i, paf_name], "_xwalk_2019"), dt2)
}

# TEST SEQ COMPARISON
bv_seq <- hiv_idu_bundle_2019$seq
wxv_seq <- hiv_idu_xwalk_2019$seq
wxv_parent <- hiv_idu_xwalk_2019$crosswalk_parent_seq

F %in% (wxv_parent %in% bv_seq) # T
which(!(wxv_parent %in% bv_seq))
F %in% is.na(wxv_parent[which(!(wxv_parent %in% bv_seq))]) # ALL NAs - EXPECTED

## SAVE CROSSWALK VERSIONS ####
for (i in 1) { # 1:nrow(paf_map)
  name <- paf_map[i, paf_name]
  print(name)
  bundle_id <- paf_map[i, bundle_id]
  gbd2019_xwalk <- paf_map[i, step4_2019_xwalk]
  bv_id <- paf_map[i, iterative_bvid]
  acause <- unique(paf_map$acause)

  upload_data <- get(paste0(name, "_xwalk_2019"))
  upload_data <- as.data.table(upload_data)
  upload_data <- upload_data[!is.na(crosswalk_parent_seq), seq := NA]
  upload_data[is.na(upper), `:=` (lower = NaN, uncertainty_type_value = NA)]
  upload_data[!is.na(upper), uncertainty_type_value := 95]
  upload_data[measure == "proportion" & !is.na(standard_error) & upper > 1, `:=` (upper = NaN, lower = NaN, uncertainty_type_value = NA)]
  upload_data[measure == "proportion" & standard_error > 1 & !is.na(cases) & !is.na(sample_size), standard_error := NaN]

  upload_path <- paste0(X, "FIEPATH")
  write.xlsx(upload_data, upload_path, sheetName = "extraction")

  result <- save_crosswalk_version(bv_id, data_filepath = upload_path,
                                   description = paste0("copy of ", name, " - version_id ", gbd2019_xwalk))
  print(paste0("Iterative crosswalk version for ", name, " saved to crosswalk_version_id ", result$crosswalk_version_id))
}

df <- copy(hiv_idu_bundle_2019)
df <- as.data.table(df)
drops<-copy(df)

## DROP ID STATEMENTS REPLICATED FROM 385_prep.R
## ONLY WANT TO MAKE A SEQ MAP WITH DATA THAT WERE USED ####
  # Only keep data that were used in the past
df<-df[is_outlier==1, drop:=1]
df<-df[note_modeler%like%"using the super region" & group_review==1, drop:=1]
df<-df[group_review==0 & !(note_modeler%like%"using the super region"), drop:=1]

  # There are duplicate data, identify and drop these from the bundle
df<-unique(df, by = c("location_id", "year_start", "year_end", "nid", "underlying_nid", "mean", "lower", "upper", "cases", "sample_size"))

drops<-unique(drops[,.(nid, location_id, year_start, year_end, group_review)])
drops<-drops[is.na(group_review), group_review:=1]
drops<-drops[, sum_gr:=sum(group_review), by = c("location_id", "nid", "year_start", "year_end")]
drops<-drops[, drop:=ifelse(sum_gr==0, 1, 0)]
drops<-unique(drops[drop==1, .(location_id, nid, year_start, year_end, drop)]) # 0
# df<-merge(df, drops, by = c("location_id", "nid", "year_start", "year_end"), all.x=T)

to_drop <- df[drop == 1,]
not_drop <- df[is.na(drop)]

## CREATE SEQ MAP TO ASSIGN CROSSWALK PARENT SEQS ####
        ## (POSSIBLE BECAUSE ALL RAW DATA ARE 0-99 AND BOTH SEX; FEW EXCEPTIONS WITH M/F
        ## BUT THEY WEREN'T TAGGED INCORRECTLY; ONLY UNIQUE CASE WAS DUPLICATIVE B/M/F FOR MARSHALL ISLANDS
        ## AND CROSSWALKED DATA SHOWS THE BOTH SEX POINT WAS SEX SPLIT SO DROPPED SEX SPECIFIC -
        ## ALSO NOT AN ISSUE BECAUSE B/M/F ALL MEAN == 0) ####
seq_map <- not_drop[, c("nid", "location_id", "location_name", "sex", "age_start", "age_end", "year_start", "year_end", "mean", "standard_error", "seq")]
seq_map[, many_sexes := .N, by = c("nid", "location_id", "year_start")]
seq_map <- seq_map[order(location_id, year_start)]
View(seq_map[many_sexes > 2])

not_drop[location_id == 24 & nid == 136718 & sex != "Both", drop := 1]
drop2 <- not_drop[drop == 1]

to_drop <- rbind.fill(to_drop, drop2)
not_drop <- not_drop[is.na(drop)]
seq_map <- not_drop[, c("nid", "location_id", "location_name", "sex", "year_start", "year_end", "seq")]
for (i in 1:nrow(seq_map)) {
  upload_data[is.na(crosswalk_parent_seq) & nid == seq_map[i, nid] & location_id == seq_map[i, location_id], crosswalk_parent_seq := ifelse(year_start == seq_map[i, year_start],
                                                                                                              seq_map[i, seq],
                                                                                                              crosswalk_parent_seq)]
}
upload_data <- upload_data[!is.na(crosswalk_parent_seq), seq := NA]

upload_path <- paste0(X, "FILEPATH")
write.xlsx(upload_data, upload_path, sheetName = "extraction")

result <- save_crosswalk_version(bv_id, data_filepath = upload_path,
                                 description = paste0("copy of ", name, " - version_id ", gbd2019_xwalk))
print(paste0("Iterative crosswalk version for ", name, " saved to crosswalk_version_id ", result$crosswalk_version_id))
# [1] "Iterative crosswalk version for hiv_idu saved to crosswalk_version_id 19910"

## DROP DATA TO DROP FROM GBD 2020 BUNDLE ####
write.xlsx(to_drop, paste0(X, "FILEPATH"))
drop_seq <- data.table(seq = to_drop$seq)
T %in% (c(upload_data$seq, upload_data$crosswalk_parent_seq) %in% drop_seq$seq) 

write.xlsx(drop_seq, paste0(X, "FILEPATH"),
           sheetName = "extraction")
result <- upload_bundle_data(385, decomp_step = "iterative", gbd_round_id = 7, filepath = paste0("FILEPATH"))
