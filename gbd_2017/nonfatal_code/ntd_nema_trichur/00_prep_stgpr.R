## Empty the environment
rm(list = ls())

## Set up focal drives
os <- .Platform$OS.type
if (os=="windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

## Load functions and packages
gbd_functions <- "FILEPATH"
source(paste0(gbd_functions, "get_location_metadata.R"))
source(paste0(gbd_functions, "get_epi_data.R"))

#############################################################################################
###                                     Get data                                          ###
#############################################################################################

## Helper vectors
out_dir <- "FILEPATH"
worm    <- "ascariasis"
worm    <- "hookworm"
worm    <- "trichuriasis"

## Initialize objects to get correct values for each worm
if (worm == "ascariasis"){
  my_bundle <- 3464
  my_start  <- 0
  my_end    <- 16
} else if (worm == "hookworm"){
  my_bundle <- 3470
  my_start  <- 5
  my_end    <- 20
} else if (worm == "trichuriasis"){
  my_bundle <- 3467
  my_start  <- 5
  my_end    <- 20
}

## Get data from epi database
df <- get_epi_data(bundle_id = my_bundle)
df <- df[, ages := paste0(age_start, "_", age_end)]
df <- df[is_outlier == 0]
df <- df[, c("ihme_loc_id", "location_id", "sex", "age_start", "age_end", "ages", "year_start", "nid", "mean", "cases", "sample_size")]
df <- df[age_start >= my_start & age_end <= my_end]

## Prep for ST-GPR
df[, age_group_id := 22]
df[, year_id := year_start]
df[, sex_id  := 3]

df[, data := mean]
df[, variance := (data * (1 - data) / sample_size)]
df[, me_name := (worm)]

df <- df[data != 0]
df <- df[, c("ihme_loc_id", "location_id", "nid", "year_id", "sex_id", "age_group_id", "me_name", "data", "variance", "sample_size")]
if (worm == "ascariasis") df <- df[location_id != 97]

## Compute site years; Number of rows for site year data frame is global site years
site_years <- unique(df[, .(location_id, year_id)])
site_years <- site_years[, count := .N, by = "location_id"]
site_years <- site_years[order(count)]

## Save data
write.csv(df, file = paste0(j, out_dir, worm, "_gpr_input.csv"), row.names = F)



