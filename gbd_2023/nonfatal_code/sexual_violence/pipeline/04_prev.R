

rm(list = ls())

# Libraries and Shared Functions ----------------------------------------------
library(data.table)
library(magrittr)
library(lubridate)

central.comp.dir = FILEPATH
shared.functions <- list(
FILEPATHS
)
lapply(paste0(central.comp.dir, shared.functions), source)

username <- Sys.info()[["user"]]
source(paste0(FILEPATH, username, FILEPATH))


## Load Parameters and Arguments -----------------------------------------------
grab <- job.array.child()
print(grab)

params <- grab[[1]] %>% as.numeric
args <- grab[[2]]


sex_id <- params[1]
location_id <- params[2]
year_id <- params[3]

in_dir <- args[1]
out_dir <- args[2]
como_dir <- args[3]
version <- args[4]
decomp <- args[5]
name <- args[6]


## ------------- READ IN THE MATRICES ----------------------------------- ##
path <- paste0(in_dir, FILEPATH)
cat(paste0("Loading injury matrix: ", path))
inj_matx <- path %>% fread
inj_matx[, V1 := NULL]

path <- paste0(in_dir, FILEPATH)
cat(paste0("Loading mental matrix: ", path))
mental_matx <- path %>% fread
mental_matx[, V1 := NULL]
mental_matx[, Estimate := NULL]
mental_matx[, SE := NULL]
mental_matx[, id := NULL]

## ----------- GET PREVALENCE DATA FROM DISMOD -------------------------- ##
orig <- get_draws('modelable_entity_id', 
                  10470, 
                  source='epi', 
                  measure_id=5, 
                  location_id=location_id, 
                  year_id=year_id, 
                  sex_id=sex_id, 
                  version=version,
                  decomp_step=decomp)

# remove birth age group
orig <- orig[age_group_id != 164]

draws <- paste0("draw_", 0:999)
orig <- orig[, c("age_group_id", "location_id", "sex_id", "year_id", draws), with= F]

## ------------- GET AGE INFORMATION ------------------------------------ ##

ageconvert <- paste0(FILEPATH) %>% fread
ageconvert15 <- paste0(FILEPATH) %>% fread
ageconvert1 <- paste0(FILEPATH) %>% fread
setnames(ageconvert1, "age_start_15", "age_start_1")

master <- merge(orig, ageconvert15, by = c("age_group_id"))
master <- merge(master, ageconvert1, by = c("age_group_id"))

master <- melt(master, measure.vars = patterns("draw_"), variable.name = "draw", value.name = "prevalence")
master[, draw := gsub("draw_", "", draw)]

## ------------- MERGE ON MATRICES ---------------------------------------- ##

# merge on the injuries matrix
setnames(inj_matx, paste0("draw_", 0:999), paste0("prop_draw_", 0:999))

inj_matx <- inj_matx[sex == sex_id]
inj_matx[, c("sex", "mean", "events", "id", "count", "sd") := NULL, with = F]

# reshape matrix
inj_matx <- melt(inj_matx, measure.vars = patterns("prop_draw_"), variable.name = "draw", value.name = "prop")
inj_matx[, draw := gsub("prop_draw_", "", draw)]
# setnames(inj_matx, "prop", "prop")

inj_matx <- dcast(inj_matx, draw ~ ncode, value.var = "prop")

master <- merge(inj_matx, master, by = c("draw"))

# reshape mental matrix and merge onto injuries + master
mental_matx <- melt(mental_matx, measure.vars = patterns("draw_"), variable.name = "draw", value.name = "N50")
mental_matx[, draw := gsub("draw_", "", draw)]
# setnames(mental_matx, "N501", "N50")
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
# setnames(master, "prevalence1", "prevalence")

master <- master[, list(age_group_id, location_id, year_id, sex_id, ncode, draw, prevalence)]
master[, draw := paste0("draw_", draw)]

## ---------------- MULTIPLY BY DURATIONS TO GET PREVALENCE -------------- ##

durations_dir <- paste0(FILEPATH)
dur_data <- paste0(durations_dir, FILEPATH) %>% fread

# average over inpatient + outpatient
setnames(dur_data, names(dur_data)[grepl("draw", names(dur_data))], paste0("draw_", 0:999))
dur_data <- melt(dur_data, measure.vars = patterns("draw_"), variable.name = "draw", value.name = "duration")
# setnames(dur_data, "duration1", "duration")
dur_data <- dur_data[, lapply(.SD, mean), .SDcols = "duration", by = c("ncode", "draw")]

n50dur <- data.table(ncode = c("N50", "N51"))
n50dur[, (draws) := 1]
n50dur <- melt(n50dur, measure.vars = patterns("draw_"), variable.name = "draw", value.name = "duration")
# setnames(n50dur, "duration1", "duration")

dur_data <- rbind(dur_data, n50dur, use.names = TRUE)

master <- merge(master, dur_data, by = c("ncode", "draw"))
master[, prevalence := (prevalence * duration)/(1 + (prevalence * duration))]


## --------------- MULTIPLY BY DISABILITY WEIGHTS TO GET YLDS ------------ ##

anx_dws <- fread(FILEPATH)
anx_dws <- anx_dws[healthstate == "anxiety_mild" | healthstate == "mdd_mild"]
anx_dws <- melt(anx_dws, measure.vars = patterns("draw"))
anx_dws <- anx_dws[, lapply(.SD, mean), .SDcols = "value", by = "variable"]
anx_dws <- dcast(anx_dws, . ~ variable)
anx_dws[, . := NULL]
anx_dws <- cbind(n_code = 'N50', anx_dws)

inj_dws <- fread(FILEPATH)

oth_dws <- data.table(n_code = "N51")
oth_dws[, (paste0("draw", 0:999)) := 0]

dws <- rbindlist(list(anx_dws, inj_dws, oth_dws), use.names = FALSE)

dws <- melt(dws, measure.vars = patterns("draw"))
setnames(dws, c("variable", "value"), c("draw", "dw"))
dws[, draw := gsub("draw", "draw", draw)]

colnames(dws)[colnames(dws)=="n_code"] <- "ncode"
master <- merge(master, dws, by = c("ncode", "draw"))

# calculate YLDs
master[, ylds := dw * prevalence]

## --------------------- SAVE -------------------------------------------- ##

# get rid of negative draws that cause save results to break!
master[prevalence < 0, prevalence := 0]
master[ylds < 0, ylds := 0]

# SAVE RESULTS FOR COMO -------------------------------------------------- ##
# Folders
ylds_dir <- paste0(como_dir, FILEPATH)
ylds_dir_E <- paste0(ylds_dir[1], FILEPATH)
prev_dir_E <- paste0(como_dir, FILEPATH)

# E-code YLDs ----------------------------- 
ylds_E <- master[, lapply(.SD, sum), .SDcols = "ylds", by = c("age_group_id", "draw")]
ylds_E <- dcast(ylds_E, age_group_id ~ draw, value.var = "ylds")
draw_names <- paste0("draw_", 0:999)
cols <- c("age_group_id", draw_names)
colnames(ylds_E) <- cols

write.csv(ylds_E,FILEPATH)


# E-code prevalence -----------------------
prev_E <- master[, lapply(.SD, sum), .SDcols = "prevalence", by = c("age_group_id", "draw")]
prev_E <- dcast(prev_E, age_group_id ~ draw, value.var = "prevalence")
draw_names <- paste0("draw_", 0:999)
cols <- c("age_group_id", draw_names)
colnames(prev_E) <- cols

write.csv(prev_E, paste0(ylds_dir_E, FILEPATH)

# N-code YLDs

ylds_N <- master[, list(age_group_id, draw, ylds, ncode)]

# function that writes data for only one n-code
write.NCODE <- function(N){
  cat("Writing ", N, "\n")
  data <- ylds_N[ncode == N, list(age_group_id, draw, ylds)]
  data <- dcast(data, age_group_id ~ draw, value.var = "ylds")
  draw_names <- paste0("draw_", 0:999)
  cols <- c("age_group_id", draw_names)
  colnames(data) <- cols
  
  dir <- paste0(ylds_dir[2], "/", N)
  dir.create(dir, recursive = T)
  write.csv(data, paste0(dir, FILEPATH)
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
  draw_names <- paste0("draw_", 0:999)
  cols <- c("age_group_id", draw_names)
  colnames(data) <- cols
  
  dir <- paste0(ylds_dir[2], FILEPATH)
  dir.create(dir, recursive = T)
  write.csv(data, paste0(dir, FILEPATH)
}

ncodes <- prev_N[, ncode] %>% unique

# loop over n-codes
lapply(ncodes, write.NCODE.prev)

checkdir <- paste0(FILEPATH)
dir.create(checkdir, recursive = T)
fileConn <- file(paste0(FILEPATH)
writeLines(c("finished!"), fileConn)
close(fileConn)

print("DONE")
