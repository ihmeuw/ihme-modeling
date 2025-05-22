# NTDS: Onchocerciasis
# Purpose: Apply ratio adjustments
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <- "FILEPATH"
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common /ihme/ IO Paths
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
  draws_dir <- paste0(data_root, "FILEPATH")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
}

# Source relevant libraries
library(data.table)
library(stringr)
library(dplyr)
source("/FILEPATH/get_draws.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_population.R")
library(argparse)
params_dir <- paste0(data_root, "/FILEPATH/params")
run_file <- fread(paste0(params_dir, '/run_file.csv'))
run_folder_path <- run_file[nrow(run_file), run_folder_path]
draws_dir <- paste0(run_folder_path, "/draws")
interms_dir <- paste0(run_folder_path, "/interms")

# ## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--param_path", help = "param path", default = "168", type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

task_id <- as.integer(Sys.getenv("ADDRESS"))
params  <- fread(param_path)

my_loc  <- params[task_id, location_ids]
outcome <- params[task_id, outcome]
meid <- params[task_id, meid]

#############################################################################################
###                                     Get data                                          ###
#############################################################################################

ratio_df <- fread(paste0(params_dir, '/FILEPATH.csv')) %>%  select(location_id,year_id,ratio)

df <- fread(paste0(interms_dir, '/', meid, '/', my_loc, ".csv"))

df1 <- subset(df, year_id <= 2013)
df2 <- subset(df, year_id > 2013)

df2 <- left_join(df2, ratio_df, by =c('location_id','year_id'))

df2[, paste0("draw_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * ratio)]
df2[, c('ratio') := NULL]

df_new <- rbind(df1,df2)
df_new <- subset(df_new, year_id != 2013)

file_path <- paste0(draws_dir, '/', meid, "/", my_loc, ".csv")
write.csv(df_new, file = file_path, row.names = F)
