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

pacman::p_load(magrittr, data.table, readstata13, lubridate, bit64)

# source the central functions
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_ids.R")

# define directories to use

data_dir <- "FILEPATH"
out_dir <- "FILEPATH"
dir.create(out_dir, recursive = T)

## ------------ DATA PREP ----------------------------- ##

# read in the data
data <- lapply(paste0(data_dir, "/", list.files(data_dir)), fread, verbose = TRUE)

# get the indices that have rows
indices <- lapply(data, nrow) %>% unlist
indices <- which(indices != 0)

master <- data[indices]

# define function to convert enrolid to character
switch <- function(data){
  result <- data[, enrolid := as.character(enrolid)]
  result <- result[, V1 := NULL]
  return(result)
}

master <- lapply(master, switch) %>% rbindlist(use.names = T)
master[, age := NULL]

## --------- FORMAT THE DATA SUCH THAT WE ONLY HAVE THE SEXUAL VIOLENCE INCIDENTS --------------------- ##

# convert date to a date variable
master[, date := date(date)]
master[, date_1mo := date + 30]
master[, date_neg1mo := date - 30]
master[SV == 1, sv_date := date]

# get the minimum date of sexual violence incident
master[, sv_min := min(sv_date, na.rm = T), by = "enrolid"]

# subset to only those observations that are at or after sv incident
master <- master[date >= sv_min]

# create observation number for an enrolid
master[, n := sum(.N), by = enrolid]

# create a sexual violence observation number for an enrolid
master[SV == 1, svid := seq_len(.N), by = enrolid]

# look back at all of the preceding values and replace SV = 0 if there was an SV = 1 within the last month
enrolid <- master[, enrolid] %>% unique

index <- 1

for(id in enrolid){
  cat("Enrolid: ", id, " which is the ", index, " enrolid out of ", length(enrolid), "!", "\n", "\n")
  max <- max(master[enrolid == id, n])

  svidmax <- max(master[enrolid == id, svid], na.rm = T)
  master <- master[order(enrolid, date, svid)]

  if(svidmax > 1){
    for(sv in 2:svidmax){
      for(lookback in 1:max){
        cat("SV ID: ", sv, " out of ", svidmax, "\n")
        cat("Lookback: ", lookback, " out of ", max, "\n")
        master[enrolid == id & shift(enrolid, lookback) == id & shift(SV, lookback) == 1 & date_neg1mo <= shift(date, lookback) & svid == sv, SV := 0]
      }
    }
  }
  index <- index + 1
}

ncodes <- names(master)[grep("^N", names(master))]
ncodes <- ncodes[ncodes != "N46"]

sv <- master[SV == 1, c("enrolid", "date", "sex", ncodes), with = F]

## ------------ CALCULATE PROPORTIONS RESULTING IN INJURIES -------------------------------------------- ##

# get the total number of sv events for each sex and age group
sv[, events := sum(.N), by = "sex"]

# get the number of injuries by each sex and age group and calculate the proportion
# and the mean and standard deviation
sv <- sv[, lapply(.SD, sum), .SDcols = ncodes, by = c("sex", "events")]

# drop N-codes that never appear
counter = 0
ncodes_drop = vector(length = length(ncodes))

# need to drop the ncodes that
for(NCODES in 1:length(ncodes)){
  if(sum(sv[,ncodes[NCODES],with = F]) == 0){ncodes_drop[NCODES] = T; counter = counter + 1}
}

sv = sv[, c("sex", "events", ncodes[!ncodes_drop]), with = F]

ncodes <- names(sv)[grep("^N", names(sv))]

# reshape sv to be able to generate draws
sv <- melt(sv, measure.vars = patterns("^N"), variable.name = "ncode", value.name = "count")
sv[, mean := count/events]
sv[, sd := sqrt(mean)]

# generate draws based on the probability and the number of trials
# from a binomial distribution
draws <- paste0("draw_", 0:999)

sv[, id := seq_len(.N)]

sv[, (draws) := as.list(rbinom(n = 1000, size = events, prob = mean)), by = id]

# calculate the probability of resulting in the events
sv[, (draws) := lapply(.SD, function(x) x / events), .SDcols = draws]

## -------------- SAVE THE MATRIX ---------------------------------------------------------------------- ##

# format the matrix for saving
sv <- sv[, c("sex", "ncode", draws), with = F]

filename <- paste0(out_dir, "FILEPATH")
write.csv(sv, filename)
