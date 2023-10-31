##' The interpolate function is used to interpolate draws that only exist for estimation years, in order to get a full time series of draws. 
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


source(paste0(repo_dir, 'cod_functions.R'))



input_map <- read.csv(paste0(repo_dir, "input_map.csv"))

year_list <- c(1980:2022)


print("Starting interpolation")

print(year_list)

# Interpolate for causes 614, 615 and 616 (MEs 2085, 2087, 2089, 2097, 2100, 2103, and 2112)
interp_df <- dismod_interpolate(gbd_round_id = gbd_round, decomp_step = 'iterative', loc_id = loc_id,
                                year_start = year_list[1], year_end = tail(year_list, n = 1))





#Pulling in other hemog, adding to file


other_hemog <- read.xlsx(paste0(out_dir, "new_618_draws.xlsx"))

other_hemog <- other_hemog %>%
  filter(!age_group_id %in% c(164, 27, 22)) # remove birth, age-standardized and all ages from the global deaths of other hemog

#Setting all negative draws to zero for other hemoglobinopathies

other_hemog[other_hemog < 0] <- 0

other_hemog$mean_1 <- NULL
other_hemog$se_1 <- NULL
colnames(other_hemog)[which(names(other_hemog) == "sex")] <- "sex_id"
colnames(other_hemog)[which(names(other_hemog) == "year")] <- "year_id"
other_hemog$cause_id <- 618
other_hemog$location_id <- loc_id

other_hemog <- data.table(other_hemog)


print("Passed")

#Write out full file -- if its 618 just write out the pooled other hemog draws, 1 per location (currently its the same for each location-- is this right?)

for (cause in c(614, 615, 616, 618)){
  if (cause == 618){
    print("Writing")
    
    write.csv(other_hemog, 
              file = paste0(out_dir, "interp_files/", cause, "/interp_hemog_", loc_id, ".csv"),
              row.names = FALSE)
  } else{
    print("Writing")
    
    cause_df <- interp_df[interp_df$cause_id == cause, ] # Subset the data frame of all interpolated MEs to the cause specific results
    
    write.csv(cause_df, 
              file = paste0(out_dir, "interp_files/", cause, "/interp_hemog_", loc_id, ".csv"),
              row.names = FALSE)
    
  }
}


## Get model draws of parent hemog cod model

model_df <- get_model_draws(gbd_round_id = gbd_round, decomp_step = decomp_step, loc_id = loc_id,
                            all_years = year_list)


write.csv(model_df, 
          file = paste0(out_dir, "model_draws/model_hemog_", loc_id, ".csv"),
          row.names = FALSE)


