################################
# Pull draws from Dismod       #
################################



#############################################
# Source packages and initial user settings #
#############################################

library(data.table)
source("FILEPATH")
source("FILEPATH")

# #####################
# # Read in arguments #
# #####################

message("Started!")

# Read the location as an argument
args <- commandArgs(trailingOnly = TRUE)
location <- ifelse(!is.na(args[1]), args[1], 101) # take location_id

temp_folder <- "FILEPATH"

IDU_exposure <- 16436
hep_incidence <- c(1653, 1654, 2944, 1657, 1658, 2945)

ages <- c(8:20, 30:32, 235)
sexes <- c(1, 2)
round <- 7
step <- "iterative"
year_start <- 1990
year_end <- 2022 # GBD 2021 should go to 2023 year end.

#Pull IDU exposure, then hep exposures
df <- get_draws(gbd_id_type = "modelable_entity_id",
                gbd_id = IDU_exposure,
                source = "epi",
                location_id = location,
                age_group_id = ages,
                year_id = year_start:year_end,
                sex_id = sexes,
                measure_id = 5,
                gbd_round_id = round,
                decomp_step = step)

write.csv(df, paste0(temp_folder, "/", location, "_", IDU_exposure, ".csv"),
          row.names = FALSE)

# print(IDU_exposure)

for (id in hep_incidence) {
  df <- interpolate(gbd_id_type = "modelable_entity_id",
                    gbd_id = id,
                    source = "epi",
                    reporting_year_start = year_start,
                    reporting_year_end = year_end,
                    location_id = location,
                    age_group_id = ages,
                    sex_id = sexes,
                    measure_id = 6,
                    status = "best",
                    gbd_round_id = round,
                    decomp_step = step)
  write.csv(df, paste0(temp_folder, "/", location, "_", id, ".csv"),
            row.names = FALSE)
  print(id)
}

message("Finished")