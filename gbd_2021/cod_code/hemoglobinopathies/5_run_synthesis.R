
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library("stringr")
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')
library("haven")
library("openxlsx")
#################

invisible(lapply(list.files("FILEPATH", full.names = T), function(x){  source(x)  }))

## Getting QSub arguments
args <- commandArgs(trailingOnly = TRUE)
param_map_filepath_2 <- args[1]
## Retrieving array task_id
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
param_map <- fread(param_map_filepath_2)
### getting the variables from the param map
loc <- param_map[task_id, location_id]
gbd_round <- param_map[task_id, gbd_round]
decomp_step <- param_map[task_id, decomp_step] 
repo_dir <- param_map[task_id, repo_dir] 
out_dir <- param_map[task_id, out_dir] 


source(paste0(repo_dir, 'cod_functions.R'))

cause_ids <- c(614, 615, 616, 618)
years <- c(1980:2022)

sex_ids <- c(1, 2)

version_map <- read.xlsx(paste0(out_dir, '/data_rich_version_map.xlsx'))



for (cause in cause_ids){
  for (sex in sex_ids){
    version_id <- version_map$dr_version_id[version_map$sex_id == sex & version_map$cause_id == cause]
    
    synth_df_rate <- get_synth_draws(gbd_round_id = gbd_round, decomp_step = decomp_step,
                                     all_years = years, loc_id = loc, sex_id = sex, 
                                     cause = cause, version_id = version_id)
    
    print("Finished synth")
    
    write.csv(synth_df_rate, file = paste0(out_dir, 'scaled_files/', cause, '/', sex, '/final_hemog_', loc, '.csv'),
              row.names = FALSE)
    
  }
}







