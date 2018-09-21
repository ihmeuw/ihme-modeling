##########################################################################
## Author: USERNAME (USERNAME)
## Date: March 15, 2017
## Purpose: APPLY THE MATRICES TO SHORT-TERM PREVALENCE FOR SEXUAL VIOLENCE
##          BOTH INJURIES AND MENTAL
## Output: Prevalence of each of the sexual violence sequelae
##########################################################################

## SETUP -----------------------------------------------------------------

# clear workspace environment

rm(list = ls())

if(Sys.info()[1] == 'Windows') {
  username <- "USERNAME"
  root <- "FILEPATH"
  code_dir <- paste0("H:/inj")
} else {
  username <- "USERNAME"
  root <- "FILEPATH"
  code_dir <- paste0("FILEPATH")
  code_dir <- "FILEPATH"
}

pacman::p_load(magrittr, data.table, lubridate)

# source job array function & shared functions
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")


# settings for this run

local = F

# if it's not local, get the parameters and arguments from the job.array master script
# if it's local, just use toy ones

if(!local) {
  grab <- job.array.child()
  
  params <- grab[[1]] %>% as.numeric
  args <- grab[[2]]
  
  print(params)
  
  print(args)
  
  sex_id <- params[1]
  location_id <- params[2]
  year_id <- params[3]
  
  in_dir <- args[1]
  out_dir <- args[2]
  como_dir <- args[3]
  date <- args[4]
} else {
  
  date <- "2017_06_21"
  
  location_id <- 101
  sex_id <- 2
  year_id <- 2010
  
  in_dir <- "FILEPATH"
  out_dir <- paste0("FILEPATH")
  como_dir <- "FILEPATH"
}

## ------------- READ IN THE MATRICES ----------------------------------- ##

inj_matx <- paste0(in_dir, "FILEPATH.csv") %>% fread
inj_matx[, V1 := NULL]

mental_matx <- paste0(in_dir, "FILEPATH.csv") %>% fread
mental_matx[, V1 := NULL]
mental_matx[, Estimate := NULL]
mental_matx[, SE := NULL]
mental_matx[, id := NULL]

## ----------- GET PREVALENCE DATA FROM DISMOD -------------------------- ##

ages <- get_demographics(gbd_team = "epi")$age_group_ids

orig <- get_draws(gbd_id_field = "modelable_entity_id", gbd_id = 10470, source = "epi",
                    measure_ids = 5, location_ids = location_id, year_ids = year_id, sex_ids = sex_id,
                    age_group_ids = ages, status = "best", gbd_round_id = 4)

draws <- paste0("draw_", 0:999)
orig <- orig[, c("age_group_id", "location_id", "sex_id", "year_id", draws), with= F]

## ------------- GET AGE INFORMATION ------------------------------------ ##

ageconvert <- paste0(code_dir, "FILEPATH.csv") %>% fread
ageconvert15 <- paste0(code_dir, "FILEPATH.csv") %>% fread
ageconvert1 <- paste0(code_dir, "FILEPATH.csv") %>% fread
setnames(ageconvert1, "age_start_15", "age_start_1")

master <- merge(orig, ageconvert15, by = c("age_group_id"))
master <- merge(master, ageconvert1, by = c("age_group_id"))

master <- melt(master, measure.vars = patterns("draw_"), variable.name = "draw", value.name = "prevalence")
master[, draw := gsub("draw_", "", draw)]
setnames(master, "prevalence1", "prevalence")

## ------------- MERGE ON MATRICES ---------------------------------------- ##

# merge on the injuries matrix
setnames(inj_matx, paste0("draw_", 0:999), paste0("prop_draw_", 0:999))

inj_matx <- inj_matx[sex == sex_id]
inj_matx[, c("sex", "mean", "events", "id", "count", "sd") := NULL, with = F]

# reshape matrix
inj_matx <- melt(inj_matx, measure.vars = patterns("prop_draw_"), variable.name = "draw", value.name = "prop")
inj_matx[, draw := gsub("prop_draw_", "", draw)]
setnames(inj_matx, "prop1", "prop")

inj_matx <- dcast(inj_matx, draw ~ ncode, value.var = "prop")

master <- merge(inj_matx, master, by = c("draw"))

# reshape mental matrix and merge onto injuries + master
mental_matx <- melt(mental_matx, measure.vars = patterns("draw_"), variable.name = "draw", value.name = "N50")
mental_matx[, draw := gsub("draw_", "", draw)]
setnames(mental_matx, "N501", "N50")
setnames(mental_matx, "age", "age_start_1")

mental_matx[, sex := sex + 1]

mental_matx <- mental_matx[sex == sex_id]
mental_matx[, sex := NULL]

master <- merge(master, mental_matx, by = c("age_start_1", "draw"))

## --------------- MULTIPLY AT THE DRAW LEVEL ---------------------------- ##

cols <- c(names(master)[grepl("^N", names(master))])
# double-check if the column names are all 3 letters long

# create a column for the "no result" N-code
master[, N51 := apply(.SD, 1, function(x) 1 - sum(x)), .SDcols = cols]

cols <- c(cols, "N51")

master[, (cols) := lapply(.SD, function(x) x * master$prevalence), .SDcols = cols]

## --------------- FORMAT ------------------------------------------------ ##

master[, prevalence := NULL]
master <- melt(master, measure.vars = patterns("^N"), variable.name = "ncode", value.name = "prevalence")
setnames(master, "prevalence1", "prevalence")

master <- master[, list(age_group_id, location_id, year_id, sex_id, ncode, draw, prevalence)]
master[, draw := paste0("draw_", draw)]

## ---------------- MULTIPLY BY DURATIONS TO GET PREVALENCE -------------- ##

durations_dir <- paste0("FILEPATH")
dur_data <- paste0(durations_dir, "durations_", location_id, "_", year_id, ".csv") %>% fread
 
# average over inpatient + outpatient
setnames(dur_data, names(dur_data)[grepl("draw", names(dur_data))], paste0("draw_", 0:999))
dur_data <- melt(dur_data, measure.vars = patterns("draw_"), variable.name = "draw", value.name = "duration")
setnames(dur_data, "duration1", "duration")
dur_data <- dur_data[, lapply(.SD, mean), .SDcols = "duration", by = c("ncode", "draw")]
 
n50dur <- data.table(ncode = c("N50", "N51"))
n50dur[, (draws) := 1]
n50dur <- melt(n50dur, measure.vars = patterns("draw_"), variable.name = "draw", value.name = "duration")
setnames(n50dur, "duration1", "duration")
 
dur_data <- rbind(dur_data, n50dur, use.names = TRUE)

master <- merge(master, dur_data, by = c("ncode", "draw"))
master[, prevalence := (prevalence * duration)/(1 + (prevalence * duration))]

## --------------- MULTIPLY BY DISABILITY WEIGHTS TO GET YLDS ------------ ##

anx_dws <- fread("FILEPATH")
anx_dws <- anx_dws[healthstate == "anxiety_mild" | healthstate == "mdd_mild"]
anx_dws <- melt(anx_dws, measure.vars = patterns("draw"))
anx_dws <- anx_dws[, lapply(.SD, mean), .SDcols = "value1", by = "variable"]
anx_dws <- dcast(anx_dws, . ~ variable)
anx_dws[, n_code := "N50"]
anx_dws[, . := NULL]

inj_dws <- fread("FILEPATH.csv")

oth_dws <- data.table(n_code = "N51")
oth_dws[, (paste0("draw", 0:999)) := 0]

dws <- rbindlist(list(anx_dws, inj_dws, oth_dws), use.names = TRUE)
setnames(dws, "n_code", "ncode")

dws <- melt(dws, measure.vars = patterns("draw"))
setnames(dws, c("variable", "value1"), c("draw", "dw"))
dws[, draw := gsub("draw", "draw_", draw)]

# need to pull population

pop <- get_population(age_group_id = ages, location_id = location_id, sex_id = sex_id, year_id = year_id, gbd_round_id = 4, status = "best", location_set_id = 35)
pop[, process_version_map_id := NULL]

master <- merge(master, dws, by = c("ncode", "draw"))
master <- merge(master, pop, by = c("location_id", "sex_id", "year_id", "age_group_id"))

# calculate YLDs

master[, ylds := dw * prevalence * population]

## --------------------- SAVE -------------------------------------------- ##

master[prevalence < 0, prevalence := 0]
master[ylds < 0, ylds := 0]

# SAVE RESULTS FOR COMO -------------------------------------------------- ##

# E-code YLDs ----------------------------- 

ylds_dir <- paste0(como_dir, "/FILEPATH/", c("E", "N"))
ylds_dir_E <- paste0(ylds_dir[1], "/FILEPATH")
prev_dir_E <- paste0(como_dir, "/FILEPATH")

ylds_E <- master[, lapply(.SD, sum), .SDcols = "ylds", by = c("age_group_id", "draw")]
ylds_E <- dcast(ylds_E, age_group_id ~ draw, value.var = "ylds")

write.csv(ylds_E, paste0(ylds_dir_E, "/3_", location_id, "_", year_id, "_", sex_id, ".csv"))
write.csv(ylds_E, paste0(prev_dir_E, "/3_", location_id, "_", year_id, "_", sex_id, ".csv"))

# E-code prevalence -----------------------

#prev_E <- orig[, c("age_group_id", draws), with = F]

prev_E <- master[, lapply(.SD, sum), .SDcols = "prevalence", by = c("age_group_id", "draw")]
prev_E <- dcast(prev_E, age_group_id ~ draw, value.var = "prevalence")
write.csv(prev_E, paste0(ylds_dir_E, "/5_", location_id, "_", year_id, "_", sex_id, ".csv"))

# N-code YLDs

ylds_N <- master[, list(age_group_id, draw, ylds, ncode)]

# function that writes data for only one n-code
write.NCODE <- function(N){
  cat("Writing ", N, "\n")
  data <- ylds_N[ncode == N, list(age_group_id, draw, ylds)]
  data <- dcast(data, age_group_id ~ draw, value.var = "ylds")
  
  dir <- paste0(ylds_dir[2], "/", N)
  dir.create(dir, recursive = T)
  write.csv(data, paste0(dir, "/3_", location_id, "_", year_id, "_", sex_id, ".csv"))
}

ncodes <- ylds_N[, ncode] %>% unique

# loop over n-codes
lapply(ncodes, write.NCODE)

# N-code prevalence

prev_N <- master[, list(age_group_id, draw, prevalence, ncode)]

# function that writes data for only one n-code
write.NCODE.prev <- function(N){
  cat("Writing ", N, "\n")
  data <- prev_N[ncode == N, list(age_group_id, draw, prevalence)]
  data <- dcast(data, age_group_id ~ draw, value.var = "prevalence")
  
  dir <- paste0(ylds_dir[2], "/", N)
  dir.create(dir, recursive = T)
  write.csv(data, paste0(dir, "/5_", location_id, "_", year_id, "_", sex_id, ".csv"))
}

ncodes <- prev_N[, ncode] %>% unique

# loop over n-codes
lapply(ncodes, write.NCODE.prev)

checkdir <- paste0("FILEPATH")
dir.create(checkdir, recursive = T)
fileConn <- file(paste0(checkdir, "/finished_", location_id, "_", year_id, "_", sex_id, ".txt"))
writeLines(c("finished!"), fileConn)
close(fileConn)