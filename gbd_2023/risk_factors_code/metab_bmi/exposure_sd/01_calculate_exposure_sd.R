### Calculate exposure standard deviation draws

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

invisible(sapply(list.files("FILEPATH", full.names = T), source))
# source the adapted exp_sd calculator
source("FILEPATH/exp_sd_functions.R")
library(parallel)
library(dplyr)

# get locations
locs <- get_location_metadata(location_set_id=35, release_id=16)

# get the job arg
args <- commandArgs(trailingOnly = TRUE)
task_map_filepath <- args[1]
global <- args[2] %>% as.logical
draw_num <- args[3] %>% as.integer
out_dir <- args[4]

task_id <- ifelse(Sys.getenv('SLURM_ARRAY_TASK_ID') != '', as.integer(Sys.getenv('SLURM_ARRAY_TASK_ID')), NA)
task_map <- fread(task_map_filepath)

lid <- task_map[task_id, location_id] # location_id
years <- task_map[task_id, years]  # year_id
yid <- as.numeric(unlist(strsplit(years, ",")))
sid <- task_map[task_id, sex_id] # sex_id

print(paste0(lid, " ", locs[location_id==lid, location_name],", sex_id = ", sid))

# round ID info
release_id <- 16

# risk MEID, adult 
rei_id <- 370

# updated weights directory
if(global){
  weights_dir <- "FILEPATH/global_weights.csv"
} else {
  weights_dir <- "FILEPATH/weights.csv"
}

# Pull exposure draws
exp_dt <- get_draws(gbd_id_type = "rei_id", gbd_id = rei_id,
                    location_id = lid, year_id = yid,
                    sex_id = sid, release_id = release_id,
                    n_draws = draw_num, downsample = TRUE,
                    source = "exposure")
if (!"parameter" %in% names(exp)) exp_dt[, parameter := "continuous"]
exp_dt <- melt(exp_dt, id.vars = c("age_group_id", "sex_id", "location_id", "year_id","parameter"),
               measure.vars = paste0("draw_", 0:(draw_num - 1)),
               variable.name = "draw", value.name = "exp_mean")
exp_dt[, draw := as.numeric(gsub("draw_", "", draw))]

# calculate the exp_sd
sd_dt <- calc_metab_bmi_adult_exp_sd(
  exp_dt=exp_dt, location_id=lid, year_id=yid, sex_id=sid,
  n_draws=draw_num, release_id=release_id, weight_dir=weights_dir, global=global)

sd_dt[,exp_sd] %>% summary %>% print

# save the SD results
save_exp_sd(sd_dt, location_id = lid, sex_id = sid, draw_num, out_dir)
