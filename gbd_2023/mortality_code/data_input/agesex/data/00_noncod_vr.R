# Title: Prep VR data for age-sex

## Description: Combine claude (cod) and non-cod VR into one file, use outputs from empirical deaths

rm(list=ls())

library(data.table)
library(argparse)
library(assertable)
library(reshape2)
library(haven)
library(mortdb, lib.loc = "FILEPATH")
library(mortcore, lib.loc = "FILEPATH")

# For interactive testing
if(interactive()){

  version_id <-
  ddm_version <-
  gbd_year <-
  empir_deaths_version <-
  empir_deaths_dir <- paste0("FILEPATH")
}else{

  parser <- argparse::ArgumentParser()
  parser$add_argument("--version_id", type = "integer", required = TRUE,
                      help = "age sex version id")
  parser$add_argument("--ddm_version_id", type = "integer", required = TRUE,
                      help = "ddm version id")
  parser$add_argument("--gbd_year", type = "integer", required = TRUE,
                      help = "GBD year")

  args <- parser$parse_args()
  list2env(args, .GlobalEnv)

  ddm_version <- ddm_version_id

  # get empirical deaths parent version
  parents <- get_proc_lineage(model_name = "age sex",
                              model_type = "data",
                              run_id = version_id,
                              lineage_type = "parent",
                              gbd_year = gbd_year)
  empir_deaths_version <- parents[parent_process_name == "death number empirical data", parent_run_id]

  empir_deaths_dir <- paste0("FILEPATH")
}

# directories
input_dir   = paste0("FILEPATH")
output_dir  = paste0("FILEPATH")

# age map
age_map <- c("enn" = 2,
             "lnn" = 3,
             "pnn" = 4,
             "pna" = 388,
             "pnb" = 389,
             "nn" = 42,
             "inf" = 28,
             "ch" = 5,
             "cha" = 238,
             # "cha" = 49,
             "chb" = 34,
             "u5" = 1)
age_map <- as.data.table(age_map, keep.rownames=T)
setnames(age_map, c("rn", "age_map"), c("age", "age_group_id"))

# location map
loc_map <- mortdb::get_locations(gbd_year = gbd_year, level = "all", gbd_type = "ap_old")[, c("ihme_loc_id", "location_id")]
loc_map <- loc_map[ihme_loc_id != "CHN"]
subnat_map <- fread(paste0("FILEPATH"))
subnat_map <- merge(loc_map, subnat_map, by = "ihme_loc_id")

# create table of all location/year/age/sex combination of the subnationals
location_id <- subnat_map$location_id
age_group_id <- age_map$age_group_id
sex_id <- c(1, 2, 3)
year_id <- seq(1950, gbd_year, by=1)
combinations <- tidyr::crossing(location_id, year_id, age_group_id, sex_id)

combinations <- merge(combinations, subnat_map, by = "location_id")

# source_type_id map
id_map <- get_mort_ids("source_type")[, .(source_type_id, type_short)]
vr_id_map <- id_map[grepl("VR", type_short)][, .(source_type_id)]
nonvr_id_map <- id_map[grepl("SRS", type_short) | grepl("DSP", type_short) | grepl("MCCD", type_short)
                       | grepl("CRS", type_short)][, .(source_type_id)]


# Special exceptions prep --------------------------------------------------------------------------

## Get scaled IND SRS data
ddm <- haven::read_dta(paste0("FILEPATH"))
ddm <- as.data.table(ddm)

# Subset to IND SRS data
ddm <- ddm[ihme_loc_id == "IND" & source_type == "SRS" & sex != "both"]

# create u5
ddm[, DATUM0to4 := DATUM0to0 + DATUM1to4]

# format IND SRS datas
ddm <- ddm[, .(ihme_loc_id, year, sex, source_type, deaths_source, deaths_nid, deaths_underlying_nid,
               DATUM0to0, DATUM1to4, DATUM0to4)]
ddm <- melt(ddm, id.vars = c("ihme_loc_id", "year", "sex", "source_type", "deaths_source",
                             "deaths_nid", "deaths_underlying_nid"),
            variable.name = "datum",
            value.name = "deaths")
setDT(ddm)

ddm[datum=="DATUM0to0", age_group_id := 28]
ddm[datum=="DATUM1to4", age_group_id := 5]
ddm[datum=="DATUM0to4", age_group_id := 1]
ddm[sex == "female", sex_id := 2]
ddm[sex == "male", sex_id := 1]
ddm[, source_type_id := 2] # set to SRS data
ddm[, deaths_nid := as.numeric(deaths_nid)]
ddm[, deaths_underlying_nid := as.numeric(deaths_underlying_nid)]
setnames(ddm, c("year", "deaths_nid", "deaths_underlying_nid","deaths_source"),
         c("year_id", "nid", "underlying_nid", "source"))

ddm <- merge(ddm, age_map, by = "age_group_id", all.x = T)
ddm <- merge(ddm, loc_map, by = "ihme_loc_id", all.x = T)

# subset columns to match cod
ddm <- ddm[, c("age_group_id", "location_id", "year_id", "sex_id",
               "nid", "deaths", "source", "underlying_nid", "age", "source_type_id")]

# non-cod VR ---------------------------------------------------------------------------------------
noncod <- fread(paste0("FILEPATH"))

# subset to no shock data
noncod <- noncod[estimate_stage_id == 21]
noncod[, estimate_stage_id := NULL]

# replace inconsistent age_group_id
noncod[age_group_id == 49, age_group_id := 238]

# remove >5 age groups
noncod <- merge(noncod, age_map, by = "age_group_id")

## remove non-VR
noncod_nonvr <- merge(noncod, nonvr_id_map, by = "source_type_id")
noncod <- merge(noncod, vr_id_map, by = "source_type_id")

# Special exception: replace IND SRS with IND SRS
noncod_nonvr <- noncod_nonvr[!(location_id == 163 & grepl("SRS", source))]
noncod_nonvr <- rbind(noncod_nonvr, ddm)

# create aggregate age group death counts for VR
noncod[, age := paste0("deaths_", age)]
noncod <- dcast(noncod, location_id + nid + sex_id + year_id + source + underlying_nid + source_type_id ~ age,
                value.var = "deaths")
noncod <- as.data.table(noncod)

## replace aggregate age groups with sum of granular age group
noncod[!is.na(deaths_enn) & !is.na(deaths_lnn) & is.na(deaths_nn), deaths_nn := deaths_enn + deaths_lnn]
noncod[!is.na(deaths_pna) & !is.na(deaths_pnb) & is.na(deaths_pnn), deaths_pnn := deaths_pna + deaths_pnb]
noncod[!is.na(deaths_nn) & !is.na(deaths_pnn) & is.na(deaths_inf), deaths_inf := deaths_nn + deaths_pnn]
noncod[!is.na(deaths_cha) & !is.na(deaths_chb) & is.na(deaths_ch), deaths_ch := deaths_cha + deaths_chb]
noncod[!is.na(deaths_inf) & !is.na(deaths_ch) & is.na(deaths_u5), deaths_u5 := deaths_inf + deaths_ch]

# reshape long and format
noncod <- melt(noncod, id.vars = c("location_id", "year_id", "sex_id",
                                   "source", "nid", "underlying_nid", "source_type_id"),
               variable.name = 'age', value.name = 'deaths')
setDT(noncod)
noncod[, age := gsub('deaths_', '', age)]
noncod <- merge(noncod, age_map, by = 'age')

noncod <- noncod[!is.na(deaths)]

# check for duplicates in VR
noncod[, dup := .N, by = c("location_id", "year_id", "sex_id", "age_group_id")]

# remove dups
noncod <- noncod[!(dup == 2 & source == "DYB")]
noncod[, dup := .N, by = c("location_id", "year_id", "sex_id", "age_group_id")]
noncod <- noncod[!(dup == 2 & source != "WHO")]
noncod[, dup := .N, by = c("location_id", "year_id", "sex_id", "age_group_id")]

assert_values(noncod, "dup", test="equal", test_val = 1)

## aggregate subnationals to national and append missing national level to noncod object
noncod_subnat <- merge(noncod, combinations, by = c("location_id", "year_id", "age_group_id", "sex_id"), all.y=T)

# remove any parent/aggregates that are not complete for a location/year/age/sex
noncod_subnat[, missing := 0]
noncod_subnat[is.na(deaths), missing := 1]
noncod_subnat[, missing := max(missing), by = c("parent_id", "year_id", "age_group_id", "age", "sex_id",
                                             "source_type_id")]
noncod_subnat <- noncod_subnat[missing == 0]

# aggregate complete subnationals
noncod_subnat[, aggs := "original data"]
for(lev in c(6, 5, 4)){
  # aggregate subnationals up one level
  temp <- as.data.table(aggregate(deaths ~ parent_id + year_id + sex_id + age_group_id + age +
                                    source_type_id, data=noncod_subnat[level == lev], FUN=sum, na.rm=T))
  temp[, aggs := "collapse subnat"]
  temp[, level := lev - 1]
  setnames(temp, "parent_id", "location_id")
  if(lev != 4) temp <- merge(temp, subnat_map, by = c("location_id", "level"))

  # keep original subnational if aggregate is a duplicate
  if(lev == 4){
    temp <- temp[location_id != 6]
    noncod_subnat <- noncod_subnat[level != lev | (grepl("CHN_", ihme_loc_id) & level == lev)]
  }else{
    noncod_subnat <- noncod_subnat[level != lev]
  }
  noncod_subnat <- rbind(noncod_subnat, temp, fill = T)
  noncod_subnat[, dup := .N, by = c("location_id", "year_id", "age_group_id", "age", "sex_id",
                                 "source_type_id")]
  noncod_subnat <- noncod_subnat[dup == 1 | (dup > 1 & aggs == "original data")]
}

# append aggregates to noncod
noncod[, aggs := "original data"]
noncod <- rbind(noncod_subnat, noncod, fill = T)

# remove noncod national duplicates, keep pre-existing aggregates
noncod[, dup := .N, by = c("location_id", "year_id", "age_group_id", "age", "sex_id",
                        "source_type_id")]
noncod <- noncod[dup == 1 | (dup > 1 & aggs == "original data")]

noncod <- noncod[!((location_id == 354 | location_id == 361) & is.na(ihme_loc_id))]

# check for duplicates in VR
noncod[, dup := .N, by = c("location_id", "year_id", "sex_id", "age_group_id")]
assert_values(noncod, "dup", test="equal", test_val = 1)

# subset columns to match cod
noncod <- noncod[, c("age_group_id", "location_id", "year_id", "sex_id",
                     "nid", "deaths", "source", "underlying_nid", "age", "source_type_id")]
noncod_nonvr <- noncod_nonvr[, c("age_group_id", "location_id", "year_id", "sex_id",
                                 "nid", "deaths", "source", "underlying_nid", "age", "source_type_id")]

# cod VR -------------------------------------------------------------------------------------------
cod <- fread(paste0("FILEPATH"))

# subset to no shock data
cod <- cod[estimate_stage_id == 21]
cod[, estimate_stage_id := NULL]

# remove >5 age groups
cod <- merge(cod, age_map, by = "age_group_id")

# reshape wide
cod[, age := paste0("deaths_", age)]
cod <- dcast(cod, location_id + nid + sex_id + year_id + source + underlying_nid + source_type_id ~ age,
             value.var = "deaths")
cod <- as.data.table(cod)

## replace aggregate age groups with sum of granular age group
cod[!is.na(deaths_enn) & !is.na(deaths_lnn) & !is.na(deaths_nn), deaths_nn := deaths_enn + deaths_lnn + deaths_nn]
cod[!is.na(deaths_enn) & !is.na(deaths_lnn) & is.na(deaths_nn), deaths_nn := deaths_enn + deaths_lnn]

cod[!is.na(deaths_pna) & !is.na(deaths_pnb) & !is.na(deaths_pnn), deaths_pnn := deaths_pna + deaths_pnb + deaths_pnn]
cod[!is.na(deaths_pna) & !is.na(deaths_pnb) & is.na(deaths_pnn), deaths_pnn := deaths_pna + deaths_pnb]

cod[!is.na(deaths_nn) & !is.na(deaths_pnn) & !is.na(deaths_inf), deaths_inf := deaths_nn + deaths_pnn + deaths_inf]
cod[!is.na(deaths_nn) & !is.na(deaths_pnn) & is.na(deaths_inf), deaths_inf := deaths_nn + deaths_pnn]

cod[!is.na(deaths_cha) & !is.na(deaths_chb) & !is.na(deaths_ch), deaths_ch := deaths_cha + deaths_chb + deaths_ch]
cod[!is.na(deaths_cha) & !is.na(deaths_chb) & is.na(deaths_ch), deaths_ch := deaths_cha + deaths_chb]

cod[!is.na(deaths_inf) & !is.na(deaths_ch) & !is.na(deaths_u5), deaths_u5 := deaths_inf + deaths_ch + deaths_u5]
cod[!is.na(deaths_inf) & !is.na(deaths_ch) & is.na(deaths_u5), deaths_u5 := deaths_inf + deaths_ch]

# reshape long and format
cod <- melt(cod, id.vars = c("location_id", "year_id", "sex_id",
                             "source", "nid", "underlying_nid", "source_type_id"),
            variable.name = 'age', value.name = 'deaths')
setDT(cod)
cod[, age := gsub('deaths_', '', age)]
cod <- merge(cod, age_map, by = 'age')
cod <- cod[!is.na(deaths)]

# check for duplicates in VR
cod[, dup := .N, by = c("location_id", "year_id", "sex_id", "age_group_id")]
assert_values(cod, "dup", test="equal", test_val = 1)

## aggregate subnationals to national and append missing national level to cod object
cod_subnat <- merge(cod, combinations, by = c("location_id", "year_id", "age_group_id", "sex_id"), all.y=T)

# remove any parent/aggregates that are not complete for a location/year/age/sex
cod_subnat[, missing := 0]
cod_subnat[is.na(deaths), missing := 1]
cod_subnat[, missing := max(missing), by = c("parent_id", "year_id", "age_group_id", "age", "sex_id",
                                             "source_type_id")]
cod_subnat <- cod_subnat[missing == 0]

# aggregate complete subnationals
cod_subnat[, aggs := "original data"]
for(lev in c(6, 5, 4)){
  # aggregate subnationals up one level
  temp <- as.data.table(aggregate(deaths ~ parent_id + year_id + sex_id + age_group_id + age +
                                    source_type_id, data=cod_subnat[level == lev], FUN=sum, na.rm=T))
  temp[, aggs := "collapse subnat"]
  temp[, level := lev - 1]
  setnames(temp, "parent_id", "location_id")
  if(lev != 4) temp <- merge(temp, subnat_map, by = c("location_id", "level"))

  # keep original subnational if aggregate is a duplicate
  if(lev == 4){
    temp <- temp[location_id != 6]
    cod_subnat <- cod_subnat[level != lev | (grepl("CHN_", ihme_loc_id) & level == lev)]
  }else{
    cod_subnat <- cod_subnat[level != lev]
  }
  cod_subnat <- rbind(cod_subnat, temp, fill = T)
  cod_subnat[, dup := .N, by = c("location_id", "year_id", "age_group_id", "age", "sex_id",
                                 "source_type_id")]
  cod_subnat <- cod_subnat[dup == 1 | (dup > 1 & aggs == "original data")]
}

# append aggregates to cod
cod[, aggs := "original data"]
cod <- rbind(cod_subnat, cod, fill = T)

# remove cod national duplicates, keep pre-existing aggregates
cod[, dup := .N, by = c("location_id", "year_id", "age_group_id", "age", "sex_id",
                        "source_type_id")]
cod <- cod[dup == 1 | (dup > 1 & aggs == "original data")]

cod <- cod[!((location_id == 354 | location_id == 361) & is.na(ihme_loc_id))]

# check for duplicates in VR
cod[, dup := .N, by = c("location_id", "year_id", "sex_id", "age_group_id")]
assert_values(cod, "dup", test="equal", test_val = 1)

# subset columns
cod <- cod[, c("location_id", "year_id", "age_group_id", "age", "sex_id", "deaths",
               "source", "nid", "underlying_nid", "source_type_id")]

# Combine non-cod and cod --------------------------------------------------------------------------

# merge non-cod to cod
setnames(cod, c("deaths", "nid", "source", "underlying_nid", "source_type_id"),
         c("deaths_cod", "nid_cod", "source_cod", "underlying_nid_cod", "source_type_id_cod"))
vr <- merge(cod, noncod, by = c("location_id", "year_id", "age_group_id", "age", "sex_id"),
            all.x = T, all.y = T)

# check for duplicates
vr[, dup := .N, by = c('location_id', 'year_id', 'age_group_id', 'sex_id')]
assert_values(vr, "dup", test="equal", test_val = 1)

## Special exceptions for non-COD replacement
vr <- vr[location_id == 81 & year_id >= 1980 & year_id <= 1990, deaths_cod := NA_real_]
vr <- vr[location_id == 92 & year_id <= 1975, deaths_cod := NA_real_]
vr <- vr[location_id == 85 & year_id <= 1970, deaths_cod := NA_real_]
vr <- vr[location_id == 50 & year_id == 2019, deaths_cod := NA_real_]

# replace noncod with cod if present
vr[!is.na(deaths_cod), nid := nid_cod]
vr[!is.na(deaths_cod), underlying_nid := underlying_nid_cod]
vr[!is.na(deaths_cod), source := source_cod]
vr[!is.na(deaths_cod), deaths := deaths_cod]
vr[!is.na(deaths_cod), source_type_id := source_type_id_cod]

# subset columns
vr <- vr[, c("age_group_id", "age", "location_id", "year_id", "sex_id",
             "nid", "deaths", "source", "underlying_nid", "source_type_id")]

# Combine vr and nonvr --------------------------------------------------------------------------

# append on noncod nonvr
vr <- rbind(vr, noncod_nonvr)

# add ihme_loc_id
vr <- merge(vr, loc_map, by = "location_id", all.x = T)
assertable::assert_values(vr, "ihme_loc_id", test="not_na", warn_only = T)
starting_locs <- unique(vr$location_id)
vr <- vr[!is.na(ihme_loc_id)]
drops <- setdiff(starting_locs, unique(vr$location_id))
if(length(drops) > 0) warning("Dropped location ids ",
                              paste0(drops, sep = " "),
                              " for mis-merge location_id to ihme_loc_id")

# Add source_type and adjust
vr[, source_type := "VR"]
vr[grepl("SRS", source), source_type := "SRS"]
vr[grepl("DSP", source), source_type := "DSP"]
vr[grepl("CHN", ihme_loc_id) & (source == "China_2004_2012" | source == "China_1991_2002"), source_type := "DSP3"]
vr[source_type == "VR" & ihme_loc_id == "TUR" & year_id < 2009, source_type := "VR1"]
vr[source_type == "VR" & ihme_loc_id == "TUR" & year_id >= 2009, source_type := "VR2"]

# remove unknown sex and both sexes
vr <- vr[sex_id != 9 & sex_id != 3]

# reshape wide
vr[, age := paste0("deaths_", age)]
vr <- dcast(vr, location_id + ihme_loc_id + nid + sex_id + year_id +
              source + underlying_nid + source_type_id + source_type ~ age, value.var = "deaths")
vr <- as.data.table(vr)

# replace aggregates with sum smaller age groups
vr[!is.na(deaths_enn) & !is.na(deaths_lnn) & !is.na(deaths_nn) & grepl("VR", source_type),
   deaths_nn := deaths_enn + deaths_lnn]
vr[!is.na(deaths_nn) & !is.na(deaths_pnn) & !is.na(deaths_inf) & grepl("VR", source_type),
   deaths_inf := deaths_nn + deaths_pnn]

# generate both-sex
deaths_cols <- names(vr)[names(vr) %like% "deaths_"]
byvars <- c("ihme_loc_id", "nid", "year_id")
vr_both <- copy(vr)
vr_both[, n_sexes := .N, by = byvars]
vr_both <- vr_both[n_sexes == 2]
vr_both[, n_sexes := NULL]
vr_both <- vr_both[, (deaths_cols) := lapply(.SD, sum), .SDcols = deaths_cols, by = byvars]
vr_both[, sex_id := 3]
vr_both <- unique(vr_both)
vr <- rbind(vr, vr_both)
setorderv(vr, c(byvars, "sex_id"))

# ids to names
vr[sex_id == 1, sex := "male"]
vr[sex_id == 2, sex := "female"]
vr[sex_id == 3, sex := "both"]
setnames(vr, c('nid', 'year_id'), c('NID', 'year'))

# fill u5 deaths for all sources
vr[, deaths_u5 := deaths_inf + deaths_ch]

# check that there are no duplicates in deaths_u5 for VR
vr[!is.na(deaths_u5) & grepl("VR", source_type), dup := .N, by=c("location_id", "year", "sex_id")]
vr[is.na(dup), dup := 0]
assert_values(vr, "dup", test="lte", test_val=1)

# save for input to next script
write.csv(vr,
          paste0("FILEPATH"),
          row.names = F)
