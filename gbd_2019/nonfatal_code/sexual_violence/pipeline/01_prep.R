## SETUP -----------------------------------------------------------------

# clear workspace environment

rm(list = ls())

if(Sys.info()[1] == 'Windows') {
  username <- "USERNAME"
  root <- "FILEPATH"
  code_dir <- "FILEPATH"
} else {
  username <- "USERNAME"
  root <- "FILEPATH"
  code_dir <- "FILEPATH"
}

pacman::p_load(magrittr, data.table, readstata13, lubridate)

# settings for this run
local = F

if(!local) {
  # source job array function
  source("FILEPATH/job_array.R")
  
  grab <- job.array.child()
  
  params <- grab[[1]]
  args <- grab[[2]]
  
  file <- params[1]
  out_dir <- args[1]
  data_dir <- args[2]
} else {
  
  file <- "FILEPATH"
  data_dir <- "FILEPATH"
  out_dir <- "FILEPATH"
}

print(grab)
filename <- gsub(".dta", ".csv", file)

## READ IN THE MAPS AND FORMAT FOR ANALYSIS -------------------------------

# set ICD9 codes used for sexual violence
sv_icd <- c("99553", "99583", "E9601")
sv_map <- data.table(icd = sv_icd, name = "SV")

# get n-code ICD9 map
ncode_map <- paste0(code_dir, "FILEPATH") %>% fread
setnames(ncode_map, "gbd", "n_code")

# garbage codes
ncode_map <- ncode_map[!grepl("GS", n_code)]

# format the ICD codes
ncode_map[, icd := gsub("[.]", "", as.character(icd))]
setnames(ncode_map, "n_code", "name")

# get icd-codes map to gbd categories of other results of sexual violence
seq_map <- paste0(code_dir, "FILEPATH") %>% fread

# only keep those to model
seq_map <- seq_map[tab_category %in% c("DEP", "ANX", "ACT")]

# format the ICD codes
seq_map[, icd := as.character(ICD9)]
seq_map[, icd := gsub("[.]", "", icd)]

seq_map <- seq_map[, list(tab_category, icd)]
setnames(seq_map, "tab_category", "name")

map <- rbindlist(list(sv_map, ncode_map, seq_map), use.names = TRUE)
setnames(map, "name", "condition")

## READ IN THE FILE TO ANALYZE --------------------------------------------

# read in data
data <- paste0(data_dir, "/", file) %>% read.dta13 %>% data.table

# get diagnosis fields
dxcols <- names(data)[grepl("DX", names(data))]

# convert diagnosis codes to factors
data[, (dxcols) := lapply(.SD, as.character), .SDcols = dxcols]

dx.loop <- function(x, icds) {
  index <- ifelse(x %in% icds, 1, 0)
  return(index)
}

col.apply <- function(name, data){
  cat(name, "\n")
  icds <- map[condition == name, icd] %>% as.character
  
  newdata <- copy(data)
  
  cols <- names(newdata)[grepl("DX", names(newdata))]
  newdata[, (cols) := lapply(.SD, dx.loop, icds), .SDcols = cols]
  
  newdata[, col := apply(.SD, 1, sum), .SDcols = cols]
  setnames(newdata, "col", name)
  
  return(newdata[, name, with = F])
}

# get the subsets of names for each analysis: injuries or mental
inj_names <- map[grepl("^N", condition) | condition == "SV", condition] %>% unique
mental_names <- map[!condition %in% inj_names | condition == "SV", condition] %>% unique

# loop through all of the diagnosis codes and sum at the end
inj_data <- do.call(cbind, lapply(inj_names, col.apply, data))
mental_data <- do.call(cbind, lapply(mental_names, col.apply, data))

# subset of data that will be needed for both analyses
admin <- data[, list(date, age, sex, enrolid, sex, claim_num)]

# get inj and mental master data sets to be saved and passed on to another script
inj <- cbind(admin, inj_data)
mental <- cbind(admin, mental_data)

## CONDENSE FILES OVER EACH DAY ------------------------------------

inj <- inj[, lapply(.SD, sum), .SDcols = inj_names, by = c("enrolid", "date", "sex", "age")]
mental <- mental[, lapply(.SD, sum), .SDcols = mental_names, by = c("enrolid", "date", "sex", "age")]

replace.1s <- function(x) {
  x[x > 1] <- 1
  return(x)
}

inj[, (inj_names) := lapply(.SD, replace.1s), .SDcols = inj_names]
mental[, (mental_names) := lapply(.SD, replace.1s), .SDcols = mental_names]

# pull the enrolment id's of those that had at least one sexual violence incident
sv_enrolid <- inj[SV == 1, enrolid] %>% unique

inj <- inj[enrolid %in% sv_enrolid]

## SPECIAL PREP FOR MENTAL ANALYSIS ---------------------------------------

num <- nrow(mental[SV == 1])

# delete observations before the first sexual violence event if there are sexual violence observations
# in this dataset

if(num > 0){
  mental[, date := date(date)]
  mental[, date_1mo := date + 30]
  mental[, date_neg1mo := date - 30]
  
  mental[SV == 1, sv_date := date]
  
  # get the minimum date of sexual violence incident
  mental[SV == 1, sv_min := min(sv_date, na.rm = T), by = "enrolid"]
  
  # subset to only those observations that are at or after sv incident
  mental <- mental[(date >= sv_min & SV == 1) | SV == 0]
}

mental <- mental[, lapply(.SD, sum), .SDcols = c("SV", "DEP", "ANX", "ACT"), by = c("enrolid", "sex", "age")]

replace.1s <- function(x) {
  x[x > 1] <- 1
  return(x)
}

dxcols <- c("SV", "DEP", "ANX", "ACT")

mental[, (dxcols) := lapply(.SD, replace.1s), .SDcols = dxcols]

## SAVE RESULTS ------------------------------------------------------------

# set directories and create folders for these results
inj_out_dir <- paste0(out_dir, "FILEPATH")
mental_out_dir <- paste0(out_dir, "FILEPATH")

lapply(list(inj_out_dir, mental_out_dir), dir.create, recursive = TRUE)

# write the files
write.csv(inj, paste0(inj_out_dir, "/", filename), row.names = F)
write.csv(mental, paste0(mental_out_dir, "/", filename), row.names = F)
