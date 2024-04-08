
# Meta --------------------------------------------------------------------

# Prepare Excess calculation method sheet


# Load packages -----------------------------------------------------------

library(data.table)


# Set parameters ----------------------------------------------------------

current_date <- Sys.Date()

dir_covid_plots <- "FILEPATH"

default_observed <- "GBD"
default_expected <- "AVG"


# Load data ---------------------------------------------------------------

dt_choices <- googlesheets4::read_sheet("URL")
setDT(dt_choices)


# load maps ---------------------------------------------------------------

map_locs <- demInternal::get_locations(gbd_year = 2021)

map_loc_ita_99999 <- data.table(
  location_id = 99999,
  ihme_loc_id = "ITA_99999",
  location_name = "Provincia autonoma di Bolzano + Provincia autonoma di Trento",
  region_name = "Western Europe",
  super_region_name = "High-income",
  level = 4,
  parent_id = 86,
  is_estimate = 1,
  path_to_top_parent = "1,64,73,86,99999"
)

map_locs <- rbind(map_locs, map_loc_ita_99999, fill = TRUE)


# Prep data ---------------------------------------------------------------

dt_choices_prep <- copy(dt_choices)
setnames(dt_choices_prep, stringr::word(colnames(dt_choices_prep)))

dt_choices_prep <- dt_choices_prep[
  j = .(ihme_loc_id = location, expected, observed = data)
]

dt_choices_prep_sub <- dt_choices_prep[
  ihme_loc_id %like% "subnational",
  .(parent_ihme_loc_id = toupper(substr(ihme_loc_id, 1, 3)), expected, observed)
]

dt_choices_prep_sub[
  map_locs,
  parent_id := i.location_id,
  on = .(parent_ihme_loc_id = ihme_loc_id)
]

dt_choices_prep_sub <- rbind(
  dt_choices_prep_sub[, .(ihme_loc_id = parent_ihme_loc_id, expected, observed)],
  map_locs[dt_choices_prep_sub, .(ihme_loc_id, expected, observed), on = "parent_id"]
)

dt_choices_prep <- rbind(
  dt_choices_prep[!ihme_loc_id %like% "subnational", .(ihme_loc_id = toupper(ihme_loc_id), expected, observed)],
  dt_choices_prep_sub
)

dt_choices_prep <- dt_choices_prep[, lapply(.SD, toupper)]


# Special cases -----------------------------------------------------------

dt_choices_prep[
  ihme_loc_id == "ITA_99999",
  c("expected", "observed") := "EM"
]


# Fill method choices -----------------------------------------------------

dt_choices_all <- dt_choices_prep[
  map_locs[is_estimate == 1],
  .(location_id, ihme_loc_id, expected, observed),
  on = "ihme_loc_id"
]

dt_choices_all[is.na(expected), expected := default_expected]
dt_choices_all[is.na(observed), observed := default_observed]


# Special cases post-filling ----------------------------------------------

dt_choices_all[
  ihme_loc_id %like% "NOR_",
  c("expected", "observed") := "GBD"
]


# Save --------------------------------------------------------------------

file_name_out <- paste0("em_method_choices-", current_date, ".csv")
readr::write_csv(
  dt_choices_all,
  fs::path(dir_covid_plots, current_date, file_name_out)
)
