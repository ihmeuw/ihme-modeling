# Replace Adult DDM completeness estimates with a version of DDM that used
# with shock VRP

# Drop adult estimates (except IND SRS b/c we want those updated est)
ind_locs <- locations[grepl("IND", ihme_loc_id), location_id]
est <- est[age_group_id != 199 | (location_id %in% ind_locs & source == "SRS")]

# Read in DDM estimates that used w/ shock VRP. Substitute version depending on with or without 2020+ data
wshock_dir <- gsub(version_id, "", main_dir)

wshock_ddm <- fread(paste0("FILEPATH"))

# Subset to adult completeness
wshock_ddm <- wshock_ddm[age_group_id == 199 & !(location_id %in% ind_locs & source == "SRS")]

# replace adult completeness
est <- rbind(est, wshock_ddm)
