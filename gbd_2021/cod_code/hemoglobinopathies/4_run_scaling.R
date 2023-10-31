rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}


library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')



source("FILEPATH")


## Getting QSub arguments
args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- args[1]
## Retrieving array task_id
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
param_map <- fread(param_map_filepath)
### getting the variables from the param map
loc_id <- param_map[task_id, location_id]
gbd_round <- param_map[task_id, gbd_round]
decomp_step <- param_map[task_id, decomp_step] 
repo_dir <- param_map[task_id, repo_dir] 
out_dir <- param_map[task_id, out_dir] 
year_list <- c(1980:2022)

source("FILEPATH")

input_map <- read.csv(paste0(repo_dir, "input_map.csv"))

causes <- c(614, 615, 616, 618)
sex_ids <- c(1, 2)


print("Starting scaling")

scaled_df <- scale_cod_data(gbd_round_id = gbd_round, decomp_step = decomp_step, loc = loc_id,
                            out_dir = out_dir, causes = causes)

for (cause in causes){
  for (sex in sex_ids){
    light_df <- scaled_df[scaled_df$cause_id == cause & scaled_df$sex_id == sex, ]
    
    write.csv(light_df, 
              file = paste0(out_dir, "scaled_files/", cause, "/", sex, "/final_hemog_", loc_id, ".csv"),
              row.names = FALSE)
    
  }
}





