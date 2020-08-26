##########################################################################
### Project: GBD Nonfatal Estimation
### Purpose: Instructions for identifying End-stage ICD codes for dementia
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
}


pacman::p_load(data.table, openxlsx, ggplot2)
date <- gsub("-", "_", Sys.Date())

# GET ARGUMENTS -----------------------------------------------------------

## Here you need to enter the icd code using - this needs to be run for each
## code. At IHME we did this in parallel on the computing cluster, but if that
## is not possible it would need to be run serially.

code <- ID

# SET UP OBJECTS ----------------------------------------------------------

## change to where you have your folder set up and where you want to save all files
folder_dir <- paste0("FILEPATH")
save_dir <- paste0()

code_cols <- paste0("diag", 1:6) ## THE COLUMNS FOR ICD CODES IN THE FILE HAS ICD CODES IN COLUMNS LABELED diag_1, diag_2 etc.

# SOURCE FUNCTIONS --------------------------------------------------------

## FUNCTION TO TURN A VARIABLE WITH CONTINUOUS AGE INTO AGE GROUPS
get_age_groups <- function(change_dt){
  dt <- copy(change_dt)
  start_years <- age_dt[, age_group_years_start]
  dt[, age_group_years_start := start_years[findInterval(age, vec = start_years)]]
  dt <- merge(dt, age_dt[, .(age_group_years_start, age_group_name)], by = "age_group_years_start", sort = F)
  dt[, age_group_years_start := NULL]
  return(dt)
}

## FUNCTION TO CALCULATE NUMBER OF TARGET ICD CODES IN YEAR BEFORE DEATH
calc_comp <- function(cond_dt, comp_col, num_months){
  num_days <- 30.44 * num_months
  dt <- copy(cond_dt)
  dt <- merge(dt, mort_dates, by = "sid", sort = F)
  dt[, date_diff := difftime(date_death, end_date, units = "days")]
  dt[, in_range := (date_diff <= num_days)]
  dt[in_range == T, paste0(comp_col, "_range") := get(comp_col)][in_range == F, paste0(comp_col, "_range") := 0]
  dt[, paste0(comp_col, "_range") := sum(get(paste0(comp_col, "_range")), na.rm = T), by = "sid"]
  dt <- unique(dt, by = "sid")
  dt <- dt[, c("sid", paste0(comp_col, "_range")), with = F]
  return(dt)
}

## FUNCTION TO CALCULATE PROPORTIONS BY DEMENTIA STATUS
prop_yes_noage <- function(range_dt, dem_dt, comp_col, num_months){
  dt <- copy(range_dt)
  dt <- merge(dt, dem_dt, by = "sid")
  dt <- merge(dt, mort_dates, by = "sid")
  dt[, mean := mean(get(paste0(comp_col, "_range")) > 0), by = c("dem")]
  dt <- unique(dt, by = c("dem"))
  dt <- dt[, .(dem, mean, cond = comp_col, months = num_months)]
  return(dt)
}

# GET AGE DATA ------------------------------------------------------------

age_dt <- fread(paste0(folder_dir, "age_dt.csv"))

# DATA PREP CODE ----------------------------------------------------------

## THREE FILES ARE NEEDED

## 1 - ADMISSION FILE
## The file with all of the admissions needs to be read in here and called "hosp"
## It should have the following columns:
## diag_1, diag_2, diag_3...with the ICD codes
## sid: the subject id (unique identifier)
## end date: the end date of the hospital admission

## 2 - DEMENTIA FILE (called "dem_dt")
## The file should have two columns:
## sid: the subject id (unique identifier)
## dem: yes/no does that individual have dementia (based on formula in instructions)

## 3 - MORTALITY FILE (called "mort_dates")
## The file should have three columns
## sid: the subject id (unique identifier)
## date_death: date of death
## age: age at death

# MAP ICD9 CODES ----------------------------------------------------------

## Note: if you have more than icd9 codes, we can provide these maps as well
icd9 <- fread(paste0("FILEPATH"))


for (col in code_cols){
  print(col)
  merge_map <- copy(icd9[, .(value, code_id, code_name)])
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

# CALCULATE CODE FREQUENCY ------------------------------------------------

code_dt <- copy(hosp)

## SUM THE NUMBER OF TIMES THAT THE CODE APPEARS PER OBSERVATION (HOSPITALIZATION) ACROSS THE DIAGNOSTIC COLUMNS
code_dt[, code_freq := apply(.SD, 1, function(x) sum(grepl(paste0("^", as.character(code), "$"), x))), .SDcols = code_cols]

## FOR EACH INDIVIDUAL DID THEY HAVE THE CODE IN THE YEAR BEFORE DEATH
code_list <- calc_comp(cond_dt = code_dt, comp_col = "code_freq", num_months = 12)

## CALCULTE THE PROPORTION THAT HAD THE CODE IN THE YEAR BEFORE DEATH FOR DEMENTIA/NO DEMENTIA
means_dt <- prop_yes_noage(range_dt = code_list[[x]], dem_dt = dem_status, comp_col = "code_freq", num_months = 12)

## INCLUDE CODE AND SAVE
means_dt[, icdcode := code]
readr::write_rds(means_dt, paste0(save_dir, code, ".rds"))

# SUMMARIZE ACROSS ALL CODES ----------------------------------------------

## After the above has been run for all ICD codes, compile and summarize
## using the code below

# COMPILE -----------------------------------------------------------------

files <- list.files(save_dir)

compile_files <- function(num){
  dt <- readr::read_rds('FILEPATH')
  return(dt)
}

results <- rbindlist(parallel::mclapply(1:length(files), compile_files, mc.cores= 9))

# CALCULATE DIFFERENCE ----------------------------------------------------

dif_dt <- dcast(results, icdcode + code_name ~ dem, value.var = "mean")
setnames(dif_dt, c("0", "1"), c("no_dem", "dem"))
dif_dt[, dif := dem - no_dem]
dif_dt <- dif_dt[order(-dif)]
write.csv(dif_dt, paste0("FILEPATH"), row.names = F)
