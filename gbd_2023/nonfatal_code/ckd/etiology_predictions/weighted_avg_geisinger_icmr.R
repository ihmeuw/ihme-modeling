################################################################################
# Description: Generate weighted average of Geisinger/ICMR etiology proportions
################################################################################

rm(list = ls())

# Load libraries ----------------------------------------------------------

library(data.table)
library(tidyr)
library(dplyr)

# Read in args ------------------------------------------------------------

message("Read in input arguments")
args               <- commandArgs(trailingOnly = TRUE)
loc_id             <- as.numeric(args[1])
age_group_under_15 <- args[2]
inputdir           <- args[3]
outdir             <- args[4]
weights_path       <- args[5]
icmr_loc_id        <- as.numeric(args[6])

age_group_under_15 <- strsplit(age_group_under_15, ",")
age_group_under_15 <- as.numeric(unlist(age_group_under_15))

# Get folders -------------------------------------------------------------

if (!dir.exists(outdir)) {
  dir.create(outdir, recursive = TRUE)
  message(paste0("Created dir", outdir))
} 

stage_folders <- list.dirs(inputdir, full.names = F, recursive = F)
etios <- list.dirs(paste0(inputdir, stage_folders[1]), full.names = F, recursive = F)

#  Reading in Geisinger csvs, results by stage and etiology ------------------------

geisinger_csvs <- data.table()
for (s in stage_folders) {
  message(paste("Reading in stage", s))
  for (e in etios) {
    temp <- fread(paste0(inputdir, s, "/", e, "/", loc_id, ".csv"))
    temp[, etio := e]
    temp[, stage := s]
    geisinger_csvs <- rbind(geisinger_csvs, temp, fill = TRUE)
  }
}
rm(temp)

# Adjust for dm under age 15 ----------------------------------------------

message("Fixing dm")
# find columns that starts with "draw_"
draws_columns <- grep("^draw_", names(geisinger_csvs), value = TRUE)

# fixing geisinger proportions for dm2 > 0 for <15 years
# Filter for rows under 15, dm1 and dm2.
geisinger_csvs_under15 <- copy(geisinger_csvs)
geisinger_csvs_under15 <- geisinger_csvs_under15[age_group_id %in% age_group_under_15 & etio %in% c("dm1", "dm2"), ]
geisinger_csvs_under15 <- tidyr::pivot_longer(
  geisinger_csvs_under15,
  cols = draws_columns, names_to = "draw", values_to = "val")

# Filter to dm type 1
geisinger_csvs_under15_type1 <- setDT(copy(geisinger_csvs_under15))
# Aggregate dm1 and dm2 results by yasl 
geisinger_csvs_under15_type1 <- geisinger_csvs_under15_type1[, .(val_new = sum(val)),
  by = .(draw, year_id, age_group_id, sex_id, measure_id, location_id, stage)
]
# Aggregate results will be for type 1
geisinger_csvs_under15_type1[, etio := "dm1"]

# Get results for type 2 dm by assigning a value of 0 (zero)
# Obtain shape of data
geisinger_csvs_under15_type2 <- subset(
  geisinger_csvs_under15_type1, 
  select = c("draw", "year_id", "age_group_id", "sex_id", "measure_id", "location_id", "stage"))
geisinger_csvs_under15_type2[, val_new := 0]
geisinger_csvs_under15_type2[, etio := "dm2"]

geisinger_csvs_under15_type1 <- tidyr::pivot_wider(
  geisinger_csvs_under15_type1, names_from = "draw", values_from = "val_new")
geisinger_csvs_under15_type2 <- tidyr::pivot_wider(
  geisinger_csvs_under15_type2, names_from = "draw", values_from = "val_new")

# Filter out age under 15 from original dataframe and then row bind with dm 1 and dm 2 results
# obtain number of rows before transformation
rows_before <- nrow(geisinger_csvs)
message(paste(rows_before, "rows before transformation"))

geisinger_csvs <- geisinger_csvs[!(age_group_id %in% age_group_under_15 & etio %in% c("dm1", "dm2")), ]
geisinger_csvs <- rbind(geisinger_csvs, geisinger_csvs_under15_type1)
geisinger_csvs <- rbind(geisinger_csvs, geisinger_csvs_under15_type2)

# obtain rows after transformation
rows_after <- nrow(geisinger_csvs)
message(paste(rows_after, "rows after transformation"))

if (rows_before != rows_after) {
  stop("Number of rows before and after transformation do not match")
}

rm(geisinger_csvs_under15, geisinger_csvs_under15_type1, geisinger_csvs_under15_type2)

# Reading in ICMR csvs -------------------------------------------------------

message("Reading in ICMR")
icmr_csvs <- data.table()
for (s in stage_folders) {
  message(paste("Reading in", s))
  for (e in etios) {
    temp_path <- paste0(inputdir, "FILEPATH.csv")
    message(paste0("Reading in", temp_path))
    temp <- fread(temp_path)
    temp[, etio := e]
    temp[, stage := s]
    icmr_csvs <- rbind(icmr_csvs, temp, fill = TRUE)
  }
}
rm(temp)

icmr_csvs <- icmr_csvs[, location_id := NULL]
draws_columns <- grep("^draw_", names(icmr_csvs), value = TRUE)

# fixing icmr proportions for dm2 > 0 for <15 years
icmr_csvs_under15 <- copy(icmr_csvs)
icmr_csvs_under15 <- icmr_csvs_under15[age_group_id %in% c(age_group_under_15) & etio %in% c("dm1", "dm2"), ]
icmr_csvs_under15 <- tidyr::pivot_longer(icmr_csvs_under15, 
                                         cols = draws_columns, 
                                         names_to = "draw", 
                                         values_to = "val")

icmr_csvs_under15_type1 <- setDT(copy(icmr_csvs_under15))
icmr_csvs_under15_type1 <- icmr_csvs_under15_type1[, .(val_new = sum(val)),
    by = .(draw, year_id, age_group_id, sex_id, measure_id, stage)]
icmr_csvs_under15_type1[, etio := "dm1"]

icmr_csvs_under15_type2 <- subset(icmr_csvs_under15_type1, select = c("draw", "year_id", "age_group_id", "sex_id", "measure_id", "stage"))
icmr_csvs_under15_type2[, val_new := 0]
icmr_csvs_under15_type2[, etio := "dm2"]

icmr_csvs_under15_type1 <- tidyr::pivot_wider(icmr_csvs_under15_type1, names_from = "draw", values_from = "val_new")
icmr_csvs_under15_type2 <- tidyr::pivot_wider(icmr_csvs_under15_type2, names_from = "draw", values_from = "val_new")

# get rows before
n_rows_before <- nrow(icmr_csvs)
icmr_csvs <- icmr_csvs[!(age_group_id %in% age_group_under_15 & etio %in% c("dm1", "dm2")), ]
icmr_csvs <- rbind(icmr_csvs, icmr_csvs_under15_type1)
icmr_csvs <- rbind(icmr_csvs, icmr_csvs_under15_type2)

# get rows after
n_rows_after <- nrow(icmr_csvs)
if (n_rows_before != n_rows_after) {
  stop("Number of rows before and after transformation do not match")
}

rm(icmr_csvs_under15, icmr_csvs_under15_type1, icmr_csvs_under15_type2)

# Apply weights ------------------------------------------------------

message("Applying weights")
weights <- fread(weights_path)
weights[, avg_weight_icmr := (age_weight_icmr+stage_weight_icmr)/2]
weights[, avg_weight_geisinger := 1-avg_weight_icmr]
# filter weights df
weights <- weights[, .(stage, age_group_id, avg_weight_icmr, avg_weight_geisinger)]

# reshape data
geisinger_csvs_long <- data.table::melt(data = geisinger_csvs, 
  id.vars = setdiff(names(geisinger_csvs), draws_columns), 
  measure.vars = draws_columns, variable.name = "draw", value.name = "val_geisinger")

icmr_csvs_long <- data.table::melt(data = icmr_csvs, 
  id.vars = setdiff(names(icmr_csvs), draws_columns), 
  measure.vars = draws_columns, variable.name = "draw", value.name = "val_icmr")

# Applying weights and averaging by location/year/age/sex
dt <- merge(geisinger_csvs_long, icmr_csvs_long,
            by = c("age_group_id", "year_id", "sex_id", "measure_id", "draw", "etio", "stage"), all.x=TRUE)
dt <- merge(dt, weights, by = c("age_group_id", "stage"), all.x=TRUE)

# Get weighted average
dt[, val_geisinger_weighted := val_geisinger * avg_weight_geisinger]
dt[, val_icmr_weighted := val_icmr * avg_weight_icmr]
dt[, val_weighted_combined := val_geisinger_weighted + val_icmr_weighted]

rm(geisinger_csvs_long, icmr_csvs_long)

# Save results ------------------------------------------------------------

dt_wide <- dt[, .(year_id, age_group_id, sex_id, measure_id, location_id, stage, etio, draw, val_weighted_combined)]

# Reshape data to wide
dt_wide <- dcast(dt_wide,
                 year_id + age_group_id + sex_id + measure_id + location_id + stage + etio ~ draw, 
                 value.var = "val_weighted_combined")

# Writing csvs by stage and etiology folder
message("Writing csvs by stage and etiology folder")
for (s in stage_folders) {
  message(paste("Writing stage", s))
  for (e in etios) {
    outdir_stage_etio <- paste0(outdir, s, "/", e)
    
    if (!dir.exists(outdir_stage_etio)) {
      message(paste("Created dir", outdir_stage_etio))
      dir.create(outdir_stage_etio, recursive = TRUE)
    }
    
    # Filter results by stage and etio
    temp <- copy(dt_wide)
    temp <- temp[stage == s & etio == e, ]
    temp <- temp[, c("stage", "etio") := NULL]
    write.csv(temp, paste0(outdir_stage_etio, "/", loc_id, ".csv"), row.names = FALSE)
  }
}

message(paste("Output path", outdir))
message("Done")
