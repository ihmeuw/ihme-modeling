# Replace Adult DDM completeness estimates with a version of DDM that used
# with shock VRP

# Drop adult estimates (except IND SRS b/c we want those updated est)
ind_locs <- codes[grepl("IND", ihme_loc_id), ihme_loc_id]
all_final_comp[!(ihme_loc_id %in% ind_locs & source == "SRS"), final_comp := NA_real_]
all_final_comp[!(ihme_loc_id %in% ind_locs & source == "SRS"), sd := NA_real_]
all_final_comp[!(ihme_loc_id %in% ind_locs & source == "SRS"), adjust := NA_real_]

# Read in DDM estimates that used w/ shock VRP. Substitute version depending on with or without 2020+ data
wshock_dir <- gsub(version_id, "", main_dir)

wshock_ddm <- haven::read_dta(paste0("FILEPATH"))
wshock_ddm <- as.data.table(wshock_ddm)

wshock_ddm <- wshock_ddm[
  ,
  .(source, ihme_loc_id, year, sex, ws_final_comp = final_comp, ws_sd = sd, ws_adjust = adjust)
]


# replace adult completeness
all_final_comp <- merge(
  all_final_comp,
  wshock_ddm,
  by = c("source", "ihme_loc_id", "year", "sex")
)

all_final_comp[is.na(final_comp), final_comp := ws_final_comp]
all_final_comp[is.na(sd), sd := ws_sd]
all_final_comp[is.na(adjust), adjust := ws_adjust]

all_final_comp[, c("ws_final_comp", "ws_sd", "ws_adjust") := NULL]
