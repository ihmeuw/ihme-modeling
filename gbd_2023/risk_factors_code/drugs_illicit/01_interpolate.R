
### Interpolation -------------------------------------------------------------

rm(list = ls())
message("***** START INTERPOLATION *****")
source("FILEPATH")

# Read the location as an argument
args <- commandArgs(trailingOnly = TRUE)
l <- ifelse(!is.na(args[1]), args[1], 101) # take location_id
ages <- c(8:20, 30:32, 235, 2, 3, 388, 389, 6, 7, 34, 238)
sexes <- c(1, 2)

message("**** Running interpolations ****")
df <- interpolate(gbd_id_type = "modelable_entity_id",
                  gbd_id = 24731,
                  source = "epi",
                  measure_id = 5,
                  location_id = l,
                  reporting_year_start = 1990,
                  reporting_year_end = 2022,
                  gbd_round_id = 7,
                  decomp_step = "iterative",
                  age_group_id = ages,
                  sex_id = sexes)

df <- df[, modelable_entity_id:=16436]

write.csv(df, paste0("FILEPATH", l, ".csv"), na = "", row.names = F)

message("***** FINISHED INTERPOLATION *****")

### Save: Run this in R on Cluster with a qlogin ------------------------------

# # Run this after 03_calc_paf. Qlogin needs 150g and 80threads. Takes like 5 hours
# source("FILEPATH")
# save_results_risk(
#     input_dir = "FILEPATH",
#     input_file_pattern = "paf_{measure}_{location_id}_{year_id}_{sex_id}.csv",
#     modelable_entity_id = 8798,
#     risk_type = "paf",
#     description = "updated year beta, revert back to AR",
#     year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019, 2020),
#     mark_best = T,
#     decomp_step = "iterative"
# )
