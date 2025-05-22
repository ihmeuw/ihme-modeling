#------------------------------------------------
# save the pafs for some selected years
# Date: 04/07/2021
#-----------------------------------------------

rm(list=ls())
invisible(sapply(list.files(FILEPATH, full.names = T), source))
args <- commandArgs(trailingOnly = TRUE)
phase <- ifelse(!is.na(args[1]),args[1], "one")

#------------------------------------------------------------------------------------------------------------------------------------------------------------------
if(phase=="one"){
  message("saving pafs for selected years...")
  main_dir <- paste0(FILEPATH, "pafs_ss_annual")
  
  years <- c(1990,1995,2000,2005,2010,2015,2020,2022,2023,2024)
  # save smoking pafs. 
  result <- save_results_risk(input_dir = main_dir, 
                              input_file_pattern = '{location_id}_{sex_id}.csv',
                              risk_type = 'paf', 
                              description = 'GBD2023 final PAFs of direct smoking for selected years', 
                              modelable_entity_id = 8759,
                              measure_id=c(3, 4),
                              sex_id = c(1,2),
                              year_id = years,
                              release_id = 16,
                              mark_best = TRUE)
  print(result)
  print("save pafs done!")
  message("save pafs done!")  
} 

if(phase=="two"){
  message("saving annual pafs from 1990 to 2024...")
  main_dir <- paste0(FILEPATH, "/paf_interpolated")
  
  years <- 1990:2024
  # save smoking pafs. 
  result <- save_results_risk(input_dir = main_dir, 
                              input_file_pattern = '{location_id}.csv',
                              risk_type = 'paf', 
                              description = 'Annual PAFs of direct smoking from 1990 to 2024', 
                              modelable_entity_id = 8759,
                              measure_id=c(3, 4),
                              sex_id = c(1,2),
                              year_id = years,
                              release_id = 16,
                              mark_best = TRUE)
  print(result)
  print("save pafs done!")
  message("save pafs done!")  
  
}

