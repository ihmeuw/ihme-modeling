## HEADER #################################################################
# Date: 6/28/2021, updated 02/20/2024
# Purpose: Saving RR draws to central comp
#          

## SET-UP #################################################################
source(FILEPATH)
library(dplyr)
print(commandArgs(trailingOnly = T))
## SCRIPT ##################################################################  
file_path <- commandArgs(trailingOnly = T)[1]
file_version <- commandArgs(trailingOnly = T)[2]
me_id <- commandArgs(trailingOnly = T)[3] %>% as.numeric
description <- commandArgs(trailingOnly = T)[4]
best <- commandArgs(trailingOnly = T)[5] %>% as.logical
year_id <- commandArgs(trailingOnly = T)[6] %>% as.numeric
release <- commandArgs(trailingOnly = T)[7] %>% as.numeric

print(file_path)
print(file_version)
print(me_id)
print(best)
print(year_id)
print(release)

# save all rr draws
save_results_risk(input_dir = file_path,
                  input_file_pattern = file_version,
                  modelable_entity_id = me_id,
                  year_id = year_id,
                  description = paste("GBD Release ID", release, "for", me_id, "-", description),
                  risk_type = "rr",
                  release_id = release,
                  mark_best = best)


