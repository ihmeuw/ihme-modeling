# create cfr model inputs
# in gbd 2019, 2017 cfrs used (new subnationals pull from parent)
# custom subnationals south sudan 1990-1994, africa, europe
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7], "/")
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) 
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPAT")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
  location_id <- 214
}

# source relevant libraries
library(data.table)
library(stringr)
library(ggplot2)
library(plotly)
library(lme4)
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

my_shell   <- paste0(code_root, "FILEPATH")

logit <- function (x){
  if (any(omit <- (is.na(x) | x <= 0 | x >= 1))) {
    lv <- x
    lv[omit] <- NA
    if (any(!omit)) 
      lv[!omit] <- Recall(x[!omit])
    return(lv)
  }
  log(x/(1 - x))
}

ilogit <- function(x)1/(1+exp(-x))

gbd_round_id <- 7
decomp_step <- 'step2'

run_file <- fread(paste0("FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
draws_dir    <- paste0(run_dir, "FILEPATH")
interms_dir    <- paste0(run_dir, "FILEPATH")
params_dir  <- paste0(data_root, "FILEPATH")

### ======================= Main Execution ======================= ###

study_dems <- readRDS(paste0(data_root, 'FILEPATH', gbd_round_id, '.rds'))
year_ids <- study_dems$year_id


#load in vl death data
vl_deaths <- get_cod_data(cause_id=348, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
vl_deaths <- vl_deaths[deaths > 0 & age_group_id != 22]

leish_gr <- fread(paste0(params_dir, "FILEPATH"))
endemic_countries <- unique(leish_gr[value_endemicity == 1, location_id])

vl_deaths <- vl_deaths[location_id %in% endemic_countries]
vl_deaths <- vl_deaths[year %in% c(year_ids)]

# Drop Locs: Brazil, Ethiopia, India, China, Mexico, Ukraine, Kenya - gbd 2017
drop_locs <- c(135, 179, 163, 6, 130, 63, 180, 4843, 4844, 4851, 4853, 4854:4860, 4865, 4868:4970, 4873, 4874, 4875)
vl_deaths <- vl_deaths[!(location_id %in% drop_locs)]

loc_ids <- intersect(unique(vl_deaths[,location_id]), endemic_countries)


no_incidence_file <- c()
yes_incidence_file  <- c()

for (loc in loc_ids){
  if (paste0(loc, ".csv") %in% list.files(paste0(draws_dir, "ADDRESS"))){
    yes_incidence_file <- c(loc, yes_incidence_file)
  } else {
    no_incidence_file <- c(loc, no_incidence_file)
  }}

for (loc_id in yes_incidence_file){
  incidence_draws <- fread(paste0(draws_dir, "FILEPATH", loc_id, ".csv"))
  incidence_draws <- incidence_draws[measure_id == 6]
  if (loc_id == yes_incidence_file[1]){ vl_incidence <- x }  # init table
  else {vl_incidence <- rbind(vl_incidence, x)}
  cat(paste0("\nFinished ", i, " of ", length(yes_incidence_file)))
}

# merge values for calculation
cat("\nGetting mean incidence\n")
vl_incidence <- melt(vl_incidence, measure.vars = paste0("draw_", 0:999), variable.name = "draw", value.name = "value")
vl_incidence <- vl_incidence[, .("mean" = mean(value)), by = c("location_id", "year_id", "age_group_id", "sex_id")]
vl_incidence <- vl_incidence[, .(location_id, year_id, age_group_id, sex_id, mean)]

cat("\nGetting Population\n")
population   <- get_population(location_id = loc_ids, age_group_id = "all", year_id = year_ids, sex_id = c(1,2), gbd_round_id = gbd_round_id, decomp_step = decomp_step)
population[, run_id := NULL]

setnames(vl_deaths, "year", "year_id")
setnames(vl_deaths, "sex", "sex_id")

# merge w/ vl deaths + merge w/ population
cat("\nMerging\n")
vl_deaths <- merge(vl_deaths, vl_incidence, by = c("age_group_id", "location_id", "sex_id", "year_id"))
vl_deaths <- merge(vl_deaths, population, by = c("age_group_id", "location_id", "sex_id", "year_id"))
vl_deaths[, cases := mean * population]
vl_deaths[, cfr_estimate := deaths / cases]

# drop where cfr estimate > 1
vl_deaths <- vl_deaths[cfr_estimate <= 1]

cat("\nWriting\n")
fwrite(vl_deaths, paste0(interms_dir, "FILEPATH"))

cat("\nDone\n")