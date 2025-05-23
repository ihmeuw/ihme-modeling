# NTDS: Onchocerciasis
# Purpose: child script for extrapolation
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <- "FILEPATH"
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common   IO Paths
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

#'[ new run dir]
source(paste0(code_root, "FILEPATH"))

params_dir <- paste0(data_root, "FILEPATH")
run_file <- fread(paste0(params_dir, 'FILEPATH'))
run_folder_path <- run_file[nrow(run_file), run_folder_path]
draws_dir <- paste0(run_folder_path, "FILEPATH")
interms_dir <- paste0(run_folder_path, "FILEPATH")

# Source relevant libraries
library(data.table)
library(stringr)
source("/FILEPATH/get_draws.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_population.R")
library(argparse)

## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--param_path", help = "param path", default = "168", type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

task_id <- as.integer(Sys.getenv("ADDRESS"))
params  <- fread(param_path)

onc_sim_directory_prepped  <- params[task_id, onc_sim_directory_prepped]
my_loc  <- params[task_id, location_ids]
outcome <- params[task_id, outcome]
meid <- params[task_id, meid]

#############################################################################################
###                                     Get data                                          ###
#############################################################################################

## Pull in data
df <- fread(paste0(onc_sim_directory_prepped, 'FILEPATH'))

# change name to cases
for (prev in grep("prev", names(df), value = T)){
  setnames(df, old = prev, new = paste0("cases_", strsplit(prev, "lence")[[1]][2]))}

draws <- grep("case", names(df), value = T)
meids <- unique(df$outvar)
ages  <- unique(df$age_group_id)
sexes <- unique(df$sex_id)

for (draw in draws){
  # log every 200th draw to ensure progress
  if (as.numeric(strsplit(draw, "_")[[1]][2]) %% 200 == 0) { cat(paste0("We are at draw: ", strsplit(draw, "_")[[1]][2]), "\n") }
  new_col <- paste0("draw_", strsplit(draw, "_")[[1]][2])
  
  data <- data.table(age_group_id = as.integer(), location_id = as.integer(),
                     year_id = as.integer(), sex_id = as.integer())
  
  data <- data[,  new_col := as.numeric()]
  
  for(age in ages){
    for (sex in sexes){
      
      temp <- df[, c("age_group_id", "location_id", "year_id", "sex_id", draw), with = F]
      temp <- temp[age_group_id == age & sex_id == sex]
      
        if (sum(temp[year_id %in% c(1990,1995,2000,2005,2010,2013), draw, with = F]) == 0) {

        temp[, new_col := 0, with = F]
        temp[, draw := NULL, with = F]
        
      } else {
        temp[get(draw) == 0, (draw) := 0.00001] ## Offset
        temp[get(draw) < 0.00001, (draw) := get(draw) + 0.00001] 

        log_lm <- lm(formula = as.formula(paste0("log(", draw, ") ~ year_id")), data = temp[year_id %in% c(1990,1995,2000,2005,2010,2013)])
    
          temp <- temp[, 'log_diff_2013' := log(get(draw)[year_id == 2013]) - log(get(new_col)[year_id == 2013])]
          temp <- temp[, 'log_mean_adj' := log(get(new_col)) + log_diff_2013]
          temp <- temp[, 'mean_adj' := exp(log_mean_adj)]

          temp[year_id > 2013, (new_col) := mean_adj]
          
          temp[year_id <= 2013, new_col := get(draw), with = F]
          temp[year_id <= 2013 & get(draw) < 0.00001, (draw) := get(draw) + 0.00001] 
        
      }
      data <- rbind(data, temp)
    }
  }
  if(draw == draws[1]) dt <- data
  if(draw != draws[1]) dt <- merge(dt, data, by = c("age_group_id", "location_id", "year_id", "sex_id"))
}

dt <- dt[order(sex_id, age_group_id)]
dt[, outvar := outcome]
dt[, metric_id := 3L]
dt[, measure_id := 5L]
dt[, model_id:= meid]

dt[age_group_id %in% c(age_ids), paste0('draw_', 0:999) := 0]

file_path <- paste0(draws_dir, 'FILEPATH')
write.csv(dt, file = file_path, row.names = F)