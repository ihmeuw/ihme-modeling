##########################################################################
### Project: GBD Nonfatal Estimation
### Purpose: Format Linkage Data
##########################################################################

# SET UP OBJECTS ----------------------------------------------------------

parent_data_dir <- paste0("FILEPATH")
death_folder <- paste0("FILEPATH")
hosp_folder <- paste0("FILEPATH")
clinical_folder <- paste0("FILEPATH")
perscrip_folder <- paste0("FILEPATH")
italy_folder <- paste0("FILEPATH")

# READ DATA ---------------------------------------------------------------

icd9 <- as.data.table(feather::read_feather(paste0("FILEPATH")))
icd9[, value := gsub("\\.", "", value)]
cause_names <- fread("FILEPATH")
icd9 <- merge(icd9, cause_names[, .(cause_id, cause_name)], by = "cause_id")

print("Reading Hospital Data")
## HOSPITAL
hosp_files <- list.files(hosp_folder)
hosp_files <- hosp_files[grepl("SDO_HOSPITALIZATION", hosp_files)]
hosp_files <- hosp_files[!grepl("^~", hosp_files)]
hosp_files <- hosp_files[grepl(".XLSX$", hosp_files)]
hosp_names <- c("sid", "hid", "start_date", "end_date", paste0("diag", 1:6))

read_hosp_files <- function(num){
  print(num)
  dt <- as.data.table(readxl::read_excel(paste0(hosp_folder, hosp_files[num])))
  setnames(dt, names(dt), hosp_names)
  return(dt)
}
hosp <- rbindlist(lapply(1:length(hosp_files), read_hosp_files))
hosp <- unique(hosp)
hosp <- hosp[!is.na(hid)]

print("Reading Mortality Data")
## MORTALITY
mort_cols <- as.data.table(read.xlsx(paste0(italy_folder, "mortality_cols.xlsx")))
mort_cols <- mort_cols[!is.na(next_name)]
mort_files <- list.files(death_folder)
mort_files <- mort_files[grepl("COD", mort_files)]
mort_files <- mort_files[!grepl("^~", mort_files)]
mortcol_types <- c(rep("text", 16), "date", rep("text", 51), "date", rep("text", 2))

read_mort_files <- function(num){
  print(num)
  dt <- as.data.table(readxl::read_excel(paste0(death_folder, mort_files[num]), col_types = mortcol_types))
  dt <- dplyr::select(dt, mort_cols[, mort_cols])
  setnames(dt, mort_cols[, mort_cols], mort_cols[, next_name])
  return(dt)
}
mort <- rbindlist(lapply(1:length(mort_files), read_mort_files))

## CHECK SUBJECTS
msid <- mort[, unique(sid)]
hsid <- hosp[, unique(sid)]

# FORMAT AND RECODE -------------------------------------------------------

print("Recoding Data")
## MORTALITY
mort[, `:=` (year_death = as.numeric(year_death), sid = as.numeric(sid))]
mort[!is.na(morbid3_post) & !is.na(morbid3), morbid3 := morbid3_post] ## COMBINE PRE AND POST 2011 MORBID 3 COLS
mort[, morbid3_post := NULL]
convert_cols <- names(mort)[grepl("morbid|cod", names(mort))]
mort_exceptions <- "M935"
for (col in convert_cols){
  print(col)
  merge_map <- copy(icd9[, .(value, cause_id, cause_name)])
  setnames(merge_map, names(merge_map), c("value", paste0(col, "_id"), paste0(col, "_name")))
  mort <- merge(mort, merge_map, by.x = col, by.y = "value", all.x = T, sort = F)
  if (nrow(mort[!get(col) %in% mort_exceptions & !is.na(get(col)) & is.na(get(paste0(col, "_id")))]) > 0) stop("Not everything merged, go back and check")
}
mort[, age_calc := as.numeric(difftime(date_death, date_birth, units = "days")/365.24)]
mort[, age := as.numeric(age)]
mort <- mort[age_calc >= 40]
sid40 <- mort[, unique(sid)]

## ONLY KEEP LAST DEATH FOR THOSE THAT DIED TWICE
mort[, N := .N, by = "sid"]
mort[, max_date := max(date_death), by = "sid"]
mort <- mort[!(N == 2 & !date_death == max_date)]
mort[, c("N", "max_date") := NULL]

## HOSPITAL
convert_cols <- names(hosp)[grepl("diag", names(hosp))]
for (col in convert_cols){
  print(col)
  merge_map <- copy(icd9[, .(value, cause_id, cause_name)])
  setnames(merge_map, names(merge_map), c("value", paste0(col, "_id"), paste0(col, "_name")))
  hosp <- merge(hosp, merge_map, by.x = col, by.y = "value", all.x = T, sort = F)
  hosp[!is.na(get(col)) & is.na(get(paste0(col, "_id"))), c(col) := gsub(".$", "", get(col))]
  setnames(merge_map, names(merge_map), c("value", paste0(col, "_id2"), paste0(col, "_name2")))
  hosp <- merge(hosp, merge_map, by.x = col, by.y = "value", all.x = T, sort = F)
  hosp[is.na(get(paste0(col, "_name"))) & !is.na(get(col)), paste0(col, "_id") := get(paste0(col, "_id2"))]
  hosp[is.na(get(paste0(col, "_name"))) & !is.na(get(col)), paste0(col, "_name") := get(paste0(col, "_name2"))]
  hosp[!is.na(get(col)) & is.na(get(paste0(col, "_id"))), c(col) := gsub(".$", "", get(col))]
  setnames(merge_map, names(merge_map), c("value", paste0(col, "_id3"), paste0(col, "_name3")))
  hosp <- merge(hosp, merge_map, by.x = col, by.y = "value", all.x = T, sort = F)
  hosp[is.na(get(paste0(col, "_name"))) & !is.na(get(col)), paste0(col, "_id") := get(paste0(col, "_id3"))]
  hosp[is.na(get(paste0(col, "_name"))) & !is.na(get(col)), paste0(col, "_name") := get(paste0(col, "_name3"))]
  hosp[, c(paste0(col, c("_id2", "_name2", "_id3", "_name3"))) := NULL]
  if (nrow(hosp[!is.na(get(col)) & is.na(get(paste0(col, "_id")))]) > 0) stop("Not everything merged, go back and check")
}
hosp <- hosp[sid %in% sid40]

# FIND DEMENTIA CASES -----------------------------------------------------

print("Finding Dementia Cases")
get_cond_hosp <- function(hosp_dt, id, name){
  dt <- copy(hosp_dt)
  dt[, cond := (diag1_id %in% id | diag2_id %in% id | diag3_id %in% id | diag4 %in% id | diag5_id %in% id | diag6_id %in% id)]
  dt[, mentions := sum(cond), by = "sid"]
  dt <- unique(dt, by = "sid")
  dt <- dt[, .(sid, mentions)]
  setnames(dt, "mentions", name)
  return(dt)
}

get_cond_mort <- function(mort_dt, id, name){
  dt <- copy(mort_dt)
  dt[, cond := as.numeric(ucod_id %in% id | cod1_id %in% id | cod2_id %in% id | cod3_id %in% id | cod4_id %in% id |
                            morbid1_id %in% id | morbid2_id %in% id | morbid3_id %in% id)]
  dt <- dt[, .(sid, cond)]
  setnames(dt, "cond", name)
  return(dt)
}

hosp_dem <- get_cond_hosp(hosp, 543, "dem_hosp")
mort_dem <- get_cond_mort(mort, 543, "dem_mort")

dem_status <- merge(hosp_dem, mort_dem, by = "sid", all = T)
dem_status[, dem := as.numeric(dem_hosp > 0 | dem_mort > 0)]
dem_status[is.na(dem), dem := 0]
