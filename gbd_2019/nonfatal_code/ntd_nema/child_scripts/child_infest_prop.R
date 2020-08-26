## Empty the environment
rm(list = ls())

## Set up focal drives
os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <- FILEPATH
  ADDRESS <- FILEPATH
} else {
  ADDRESS <- FILEPATH
  ADDRESS <- paste0(FILEPATH, Sys.info()[7], "/")
}

## Load functions and packages
source(FILEPATH)
source(FILEPATH)
library(argparse, lib.loc= FILEPATH)
library(assertable)
library(parallel)
library(data.table)
library(readstata13)

## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--my_worm", help = "worm that we want to compute proportions for", default = "ascariasis", type = "character")

parser$add_argument("--interms_directory", help = "Site where to output proportion interm file",
                    default = "", type = "character")
parser$add_argument("--params_directory", help = "Site where to input expert draws",
                    default = "", type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

#############################################################################################
###                                     Get data                                          ###
#############################################################################################

## Establish correct worm to compute proportions for
if (tolower(my_worm) == "ascariasis")   helminth <- "asc"
if (tolower(my_worm) == "trichuriasis") helminth <- "tt"
if (tolower(my_worm) == "hookworm")     helminth <- "hk"

## Get infestation data frame
cat("Loading and preparing proportion data frame\n")
df <- as.data.table(read.dta13(paste0(params_directory, FILEPATH)))
df <- df[helminth_type == helminth & ihme_country == 1]
df <- df[, c("lower", "upper", "gbd_region", "gbd_super_region", "countryname", "ihme_country") := NULL]
df <- reshape(df, idvar = c("iso3", "year", "age", "helminth_type"), timevar = "intensity", direction = "wide")

## Get location metadata
setnames(df, old = "iso3", new = "ihme_loc_id")
setnames(df, old = "year", new = "year_id")

locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 6)
locs <- locs[ihme_loc_id %in% unique(df$ihme_loc_id)]
locs <- locs[, c("location_id", "ihme_loc_id", "location_name", "most_detailed", "region_name", "super_region_name")]

df <- merge(df, locs, by = "ihme_loc_id")
df <- df[, c(1, 9:13, 2:8)]

#############################################################################################
###                                 Ascariasis Fix                                        ###
#############################################################################################

## Fix issue with 1990 10to14 data. We replace the row with 1990 5to9 data
if (unique(df$helminth_type) == "asc"){
  cat("Fixing implausible Ascariasis data points for 1990\n")
  ## Fix the large issue with 1990
  replacement <- df[year_id == 1990 & age == "5to9"]
  replacement <- replacement[, age := "10to14"]

  df <- df[!(year_id == 1990 & age == "10to14")]
  df <- rbind(df, replacement)

  ## Fix 4 problem rows in 2005 that had the same issues
  replacement <- df[(location_id %in% c(22, 30, 115, 144)) & (year_id == 2005) & (age == "5to9")]
  replacement <- replacement[, age := "10to14"]

  df <- df[!((location_id %in% c(22, 30, 115, 144)) & (year_id == 2005) & (age == "10to14"))]
  df <- rbind(df, replacement)
  df <- df[order(ihme_loc_id, year_id, age)]

}

#############################################################################################
###                     Compute proportion and get remaining years                        ###
#############################################################################################

## Compute proportion of heavy infestation
cat("Computing proportions and imputing missing years\n")
df[, prop_heavy := mapvar.heavy / mapvar.prev]
df[, region_heavy_ave := mean(prop_heavy), by = c("region_name", "age", "year_id")]

## Compute proportion of medium infestation
df[, prop_med := mapvar.med / mapvar.prev]
df[, region_med_ave := mean(prop_med), by = c("region_name", "age", "year_id")]

## Get remaining years
df_1995 <- copy(df)
df_1995 <- df_1995[year_id == 1990]
df_1995 <- df_1995[, year_id := 1995]

df_2000 <- copy(df)
df_2000 <- df_2000[year_id == 1990]
df_2000 <- df_2000[, year_id := 2000]

df_2015 <- copy(df)
df_2015 <- df_2015[year_id == 2010]
df_2015 <- df_2015[, year_id := 2015]

df_2017 <- copy(df)
df_2017 <- df_2017[year_id == 2010]
df_2017 <- df_2017[, year_id := 2017]

df_2019 <- copy(df)
df_2019 <- df_2019[year_id == 2010]
df_2019 <- df_2019[, year_id := 2019]

## Append everything together
df <- do.call("rbind", list(df, df_1995, df_2000, df_2015, df_2017, df_2019))
df <- df[order(location_id, year_id)]
df <- df[, c("helminth_type", "mapvar.heavy", "mapvar.light", "mapvar.med", "mapvar.prev") := NULL]

#############################################################################################
###                           Deal with missing subnationals                              ###
#############################################################################################

## Get a data frame with all subnational units
cat("Extending national values to subnationals\n")
locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 6)
locs <- locs[parent_id %in% unique(df[most_detailed == 0]$location_id)]
locs <- locs[, c("location_id", "ihme_loc_id")]

rural <- get_location_metadata(location_set_id = 35, gbd_round_id = 6)
rural <- rural[grep("Rural|Urban", location_name)]
rural <- rural[, c("location_id", "ihme_loc_id")]
locs  <- rbind(locs, rural)
locs  <- locs[, parent_loc := tstrsplit(ihme_loc_id, "_")[[1]]]

## Get all years
for (year in unique(df$year_id)){

  temp <- copy(locs)
  temp[, year_id := year]

  if (year == 1990) data <- copy(temp)
  if (year != 1990) data <- rbind(data, temp)
}

## Get all ages
for (my_age in unique(df$age)){

  temp <- copy(data)
  temp[, age := my_age]

  if (my_age == "0to4") data <- copy(temp)
  if (my_age != "0to4") data <- rbind(data, temp)
}

## Now merge on national values
subnats <- df[most_detailed == 0]
setnames(subnats, old = "ihme_loc_id", new = "parent_loc")

data <- merge(data, subnats, by = c("parent_loc", "year_id", "age"))
data <- data[, location_id.y := NULL]
data <- data[, parent_loc := NULL]
setnames(data, old = "location_id.x", new = "location_id")

## Now append subnational values
df <- rbind(df, data)
df <- df[, most_detailed := NULL]
df <- df[, cause := my_worm]
df <- unique(df)
df[, location_name := NULL]
locs <- get_location_metadata(gbd_round_id = 6, location_set_id = 35)
locs <- locs[, c("location_name", "location_id")]
df <- merge(df, locs, by = "location_id")

write.csv(df, paste0(interms_directory, my_worm, "/", my_worm, FILEPATH), row.names = F)
