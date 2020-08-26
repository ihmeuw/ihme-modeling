# Pull new qx results using function from mortality team 
# --------------

print(paste("Working dir:", getwd()))
j <- "FILEPATH"
my_libs = "FILEPATH"

library(data.table)
library(magrittr)
library(ggplot2)
library(readstata13)

source("FILEPATH/get_location_metadata.R"  )
source("FILEPATH/get_life_table_with_shock.R"  )

#pull one version with standard location set PLUS China, India, Brazil, U.S.
locs <- get_location_metadata(location_set_id = 9, gbd_round_id = 6)

results <- get_life_table_with_shock(gbd_round_id =6, age_group_id = 2:3, decomp_step = "step1")
results <- results[life_table_parameter_id == 3]

results <- dcast(results, location_id + sex_id + year_id ~ paste0("age_group_id_", age_group_id), value.var = "mean")

results[, q_nn_med := 1 - (1 - age_group_id_2) * (1 - age_group_id_3)]

results <- merge(results, locs[, list(location_id, ihme_loc_id, location_name, region_name)], all.y = T, by = "location_id")

setnames(results, c("sex_id", "year_id"), c("sex", "year"))

results <- results[, list(location_id, sex, q_nn_med, year, location_name, ihme_loc_id, region_name)]

write.csv(results, "FILEPATH/qx_results_v2019_loc_set_9.csv", row.names = F, na = "")
save.dta13(results, "FILEPATH/qx_results_v2019_loc_set_9.dta")

compare <- fread("FILEPATH/qx_results_v2019_all_locs.csv")
