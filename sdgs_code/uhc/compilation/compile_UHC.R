##########################################################
## DEFINE ROOT AND LIBRARIES
rm(list=ls())
library(argparse)
library(data.table)
library(plyr)
library(parallel)
library(rPython)

##########################################################
## PARSE ARGUMENTS
parser <- ArgumentParser()
parser$add_argument("--prog_dir", help="Program directory",
                    dUSERt=paste0(h, "/risk_stand_amenable_mort/"), type="character")
parser$add_argument("--data_dir", help="Space where results are to be saved",
                    dUSERt="/ihme/scratch/projects/sdg/", type="character")
parser$add_argument("--lsid", help="Location set",
                    dUSERt=35, type="integer")
parser$add_argument("--yids", help="Years to run",
                    dUSERt=1990:2016, nargs="+", type="integer")
args <- parser$parse_args()
list2env(args, .GlobalEnv)
rm(args)

##########################################################
## DEFINE FUNCTIONS
source(paste0(prog_dir, "/utilities.R"))


scaleIndicator <- function(df, rangelocs) {
  # Reshape long for remainder of process
  df <- melt(df,
             id.vars = c("location_id", "year_id", "age_group_id", "sex_id", "indicator"),
             measure.vars = paste0("draw_", 0:999),
             variable.name = "draw",
             value.name = "value")

  # Calculate indicator-specific index values
  print(paste0("Scaling rate -- ", Sys.time()))
  df <- df[indicator %like% "rsm_", value := log(value)]
  minmaxdf <- df[location_id %in% rangelocs,] # Only use locations with populations > 1 million for setting minimum and maximum
  minmaxdf <- minmaxdf[, .(min_value = min(value), max_value = max(value)), by = .(indicator, draw)]
  df <- merge(df,
              minmaxdf,
              by = c("indicator", "draw"))
  df <- df[value < min_value, value := min_value][value > max_value, value := max_value]
  df <- df[, scaled_value := (value - min_value) / (max_value - min_value)]
  df <- df[indicator %like% "rsm_", scaled_value := 1 - scaled_value]

  df$age_group_id <- 22
  df$sex_id <- 3
  df <- dcast(df,
              location_id + year_id + age_group_id + sex_id + indicator ~ draw,
              value.var = "scaled_value")

  return(df)
}


getIndex <- function(data_dir, prog_dir, yids, lsid) {
  # Get data frames
  print(paste0("Reading and formatting data -- ", Sys.time()))
  locsdf <- getLocations(lsid=1)
  locsdf <- locsdf[location_type == "admin0"]
  popsdf <- getPops(lsid, yids = 2016, agids = 22, sids = 3)
  popsdf <- popsdf[location_id %in% locsdf$location_id]
  python.exec('import sys')
  python.exec('sys.path.append("FILEPATH")')
  python.exec('import sdg_utils.draw_files as dw')
  uhc_version <- python.get('dw.UHC_VERS')
  uhcdf <- fread("FILEPATH")

  # Scale indicators
  uhcdfs <- split(uhcdf, uhcdf$indicator)
  cl <- makeCluster(15)
  invisible(clusterEvalQ(cl, library(data.table)))
  invisible(clusterEvalQ(cl, library(plyr)))
  rangelocs <- unique(popsdf$location_id[popsdf$population >= 1e6])
  uhcdfs <- parLapply(cl, uhcdfs, scaleIndicator, rangelocs)
  stopCluster(cl)
  uhcdf <- rbindlist(uhcdfs)

  # Make sure we have all 41 indicators (32 amenable mort + 3 vaccines + 4 maternal + met need + ART)
  if (length(unique(uhcdf$indicator)) != 41) {
    print(unique(uhcdf$indicator))
    stop("Missing indicator(s), should be 32 amenable mort + 3 vaccines + 4 maternal + met need + ART")
  }

  # Save scaled
  print(paste0("Saving scaled indicators -- ", Sys.time()))
  write.csv(uhcdf,
            "FILEPATH",
            row.names = FALSE)

  # Save the geometric mean of the 9 coverage indicators
  print(paste0("Saving coverage mean -- ", Sys.time()))
  covdf <- uhcdf[!indicator %like% "rsm_",]
  for (draw in paste0("draw_", 0:999)) {
    covdf <- covdf[get(draw) < 0.1, eval(draw) := 0.1]
  }
  covdf <- covdf[, lapply(.SD, function(x) exp(mean(log(x)))), by = .(location_id, year_id),
                 .SDcols = paste0("draw_", 0:999)]
  covdf <- covdf[, c("value_geom_mean", "value_geom_mean_l", "value_geom_mean_u") :=
                 .(apply(.SD, 1, mean), apply(.SD, 1, quantile, 0.025), apply(.SD, 1, quantile, 0.975)),
                 .SDcols = paste0("draw_", 0:999)]
  write.csv(covdf[, c("location_id", "year_id", "value_geom_mean", "value_geom_mean_l", "value_geom_mean_u"), with = FALSE],
            file = "FILEPATH",
            row.names = FALSE)

  # Make composite indicator (now just mean of individual indicators)
  print(paste0("Creating composite -- ", Sys.time()))
  uhcdf <- uhcdf[, lapply(.SD, mean), by = .(location_id, year_id, age_group_id, sex_id), .SDcols = paste0("draw_", 0:999)]

  # Save
  print(paste0("Saving composite -- ", Sys.time()))
  write.csv(uhcdf,
            file = "FILEPATH")
}

##########################################################
## RUN PROGRAM
getIndex(data_dir, prog_dir, yids, lsid)

##########################################################
## END SESSION
quit("no")

##########################################################
