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
  message("saving unmediated pafs for selected years...")
  main_dir <- paste0(FILEPATH, "pafs_ss_annual_um")
  
  
  years <- c(1990,1995,2000,2005,2010,2015,2020,2022,2023,2024)
  
  # save smoking pafs. 
  result <- save_results_risk(input_dir = main_dir, 
                              input_file_pattern = '{location_id}_{sex_id}.csv',
                              risk_type = 'paf', 
                              description = 'Unmediated PAFs of direct smoking for selected years', 
                              modelable_entity_id = 26975,
                              measure_id=c(3, 4),
                              sex_id = c(1,2),
                              year_id = years,
                              release_id = 16,
                              mark_best = TRUE)
  
  print(result)
  print("save unmediated pafs done!")
  message("save unmediated pafs done!")  
} 

if(phase=="two"){
  message("saving annual unmediated pafs from 1990 to 2024...")
  main_dir <- paste0(FILEPATH, "/paf_interpolated_um")
  
  years <- 1990:2024
  # save smoking pafs. 
  result <- save_results_risk(input_dir = main_dir, 
                              input_file_pattern = '{location_id}.csv',
                              risk_type = 'paf', 
                              description = 'Annual unmediated PAFs of direct smoking from 1990 to 2024', 
                              modelable_entity_id = 26975,
                              measure_id=c(3, 4),
                              sex_id = c(1,2),
                              year_id = years,
                              release_id = 16,
                              mark_best = TRUE)
  print(result)
  print("save unmediated pafs done!")
  message("save unmediated pafs done!")  
  
}

