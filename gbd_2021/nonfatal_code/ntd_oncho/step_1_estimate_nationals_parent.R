# NTDS: Onchocerciasis
# Purpose: Extrapolate national prevalence from Oncho Sim Draws

### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
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
  location_id <- 214
}


library(data.table)

params_dir <- paste0(data_root, "FILEPATH")
run_file <- fread(paste0(params_dir, 'FILEPATH'))
run_folder_path <- run_file[nrow(run_file), run_folder_path]
draws_dir <- paste0(run_folder_path, "FILEPATH")
interms_dir <- paste0(run_folder_path, "FILEPATH")

# Source relevant libraries
code_dir   <- paste0(code_root, "FILEPATH")
my_shell <- paste0(code_root, "FILEPATH")

library(readstata13)
library(stringr)
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_demographics.R")
source(paste0(code_root, "FILEPATH/submit_function.R"))

gbd_round_id <- ADDRESS
decomp_step <- ADDRESS

#############################################################################################
###                              submit extrapolate                                       ###
#############################################################################################

if (!(exists('study_dems'))) {
  study_dems <- get_demographics("ADDRESS", gbd_round_id = gbd_round_id)
} 

# gets overwritten by extrapolation regression - just need for structure to produce results
oncho_sim_directory <- paste0(draws_dir, "FILEPATH")
my_locs <- gsub('.csv', '', list.files(oncho_sim_directory))
if (length(unique(my_locs)) != 26) { stop("there are 26 APOC and OCP countries, these have not been written out in the oncho_sim_directory")}

# new_draws_directory is oncho_sim_directory overwritten by extrapolation regression - need for structure to produce results
onc_sim_directory_prepped <- paste0(draws_dir, "FILEPATH")
ifelse(!dir.exists(onc_sim_directory_prepped), dir.create(onc_sim_directory_prepped), FALSE)

my_years <- study_dems$year_id
new_years <- my_years[my_years > 2013]
meids <- c(ADDRESSES)
outcomes = c("mfcases", "osdcases1acute", "osdcases1chron", "osdcases3acute", "osdcases3chron", 
            "osdcases2acute",  "vi_mod_cases", "vi_sev_cases", "blindcases")

for (loc in my_locs){
   
   draws_path <- paste0(oncho_sim_directory, 'FILEPATH')
   
   df <- fread(draws_path)
   
   df_years <- unique(df$year_id)
   years_to_add <- new_years[!(new_years %in% df_years)]
   
   new_df <- copy(df)
   
   if (length(years_to_add) > 0) {
     
     for (year_to_add in years_to_add){
       # Copying shape and filling with zeros, 
       # values get updated in child script
       new_rows <- copy(df[year_id == 2013])
       new_rows[, year_id:=year_to_add]
       new_rows[, OKyear:=0]
       new_df <- rbind(new_df, new_rows)
     }     
   }
   
   for (outcome in outcomes){
     df <- new_df[outvar == outcome]
     fwrite(df, paste0(onc_sim_directory_prepped, 'FILEPATH'))
   }
}
 
# create meids and their directories
for (meid in meids){
  ifelse(!dir.exists(paste0(draws_dir, '/', meid)), dir.create(paste0(draws_dir, '/', meid)), FALSE)
}
 
# Get locations and worm types
outcome_meids <- data.table(meid = c(ADDRESS),
                            outcome = c("mfcases", "osdcases1acute", "osdcases1chron", "osdcases3acute", "osdcases3chron", 
                                         "osdcases2acute",  "vi_mod_cases", "vi_sev_cases", "blindcases"))

param_map <- as.data.table(expand.grid(location_ids = my_locs,
                         onc_sim_directory_prepped = onc_sim_directory_prepped,
                         meid = outcome_meids$meid))
param_map <- merge(param_map, outcome_meids, by = "meid", all.x = TRUE)
num_jobs  <- nrow(param_map)
fwrite(param_map, paste0(interms_dir, "FILEPATH"), row.names = F)


