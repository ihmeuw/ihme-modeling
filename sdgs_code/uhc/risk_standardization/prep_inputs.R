##########################################################
# Author: Ryan Barber
# Date: 03 August 2016
# Description: Prepare PAF inputs
##########################################################
## PARSE ARGUMENTS
parser <- ArgumentParser()
parser$add_argument("--prog_dir", help="Program directory",
                    type="character")
parser$add_argument("--data_dir", help="Space where results are to be saved",
                    type="character")
parser$add_argument("--cc_vers", help="CoDCorrect version number",
                    dUSERt=66, type="integer")
parser$add_argument("--paf_vers", help="PAF version (needs to be character)",
                    dUSERt="196_amenable", type="character")
parser$add_argument("--lsid", help="Location set",
                    dUSERt=35, type="integer")
parser$add_argument("--lid", help="Location",
                    dUSERt=102, type="integer")
parser$add_argument("--yid", help="Year for current job",
                    dUSERt=1995, type="integer")
parser$add_argument("--mid", help="Measure",
                    dUSERt=1, type="integer")
parser$add_argument("--drawnum", help="Number of draws",
                    dUSERt=1000, type="integer")
args <- parser$parse_args()
list2env(args, .GlobalEnv)
rm(args)

##########################################################
## DEFINE FUNCTIONS
source(paste0(prog_dir, "/utilities.R"))

dataGrabPAF <- function(lid, yid, mid, paf_vers, cc_vers, cids, drawnum) {
  # Collect PAF and death data for given location-year, then combine and return
  pafdf <- loadPAF(lid, yid, mid, paf_vers, cids, drawnum)
  deathsdf <- loadDeaths(lid, yid, mid, cc_vers, cids, drawnum)
  df <- merge(pafdf,
              deathsdf,
              by = c("location_id", "year_id", "age_group_id", "sex_id", "measure_id", "cause_id", "draw"),
              all = TRUE)
  df <- df[is.na(val), val := 0][is.na(paf), paf := 0]
  return(df)
}

saveData <- function(inputdf, data_dir, lid, yid, mid, prefix) {
  # Save
  save(inputdf,
       file = paste0(data_dir, "/draws/inputs/", yid, "/", prefix, "_", mid, "_", lid, ".RData"))
}

popScalar <- function(yids, lid, locsdf, popsdf) {
  childpopsdf <- popsdf[location_id %in% locsdf$location_id[locsdf$parent_id == lid & locsdf$location_id != lid] & year_id %in% yids,]
  childpopsdf <- childpopsdf[, .(child_pop_agg = sum(population)), by = .(year_id, age_group_id, sex_id)]
  aggpopsdf <- popsdf[location_id == lid & year_id %in% yids,]
  scaledf <- merge(aggpopsdf,
                   childpopsdf,
                   by = c("year_id", "age_group_id", "sex_id"))
  scaledf <- scaledf[, pop_scalar := population / child_pop_agg][pop_scalar < 1, pop_scalar := 1]
  return(scaledf[, c("location_id", "year_id", "age_group_id", "sex_id", "pop_scalar"), with = FALSE])
}

## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
mostDetailedCollector <- function(lid, yid, mid, paf_vers, cc_vers, data_dir) {
  # Load cause data (only keep amenable causes or their children)
  load(paste0(data_dir, "/causesdf.RData"))
  cause_list <- fread(paste0(prog_dir, "/amenable_cause_list_GBD.csv"))
  causesdf <- causesdf[most_detailed == 1 | cause_id == 297]
  causesdf <- causesdf[!path_to_top_parent %like% ",297,"]
  causesdf <- causesdf[, amenable := FALSE]
  for (cid in cause_list$cause_id) {
    causesdf <- causesdf[path_to_top_parent %like% cid, amenable := TRUE]
  }
  causesdf <- causesdf[amenable == TRUE]
  
  # PAF
  inputdfPAF <- dataGrabPAF(lid, yid, mid, paf_vers, cc_vers, cids = causesdf$cause_id, drawnum)
  
  # Save PAFs
  saveData(inputdfPAF[, c("location_id", "year_id", "age_group_id", "sex_id", "measure_id", "cause_id", "draw", "paf", "val"), with = FALSE], 
           data_dir, lid, yid, mid, "PAF")
  
  # Summarize PAFs for the purpose
  inputdfPAF <- inputdfPAF[, .(paf = mean(paf)), by = .(location_id, year_id, age_group_id, sex_id, measure_id, cause_id)]
  saveData(inputdfPAF, data_dir, lid, yid, mid, "md_sums/PAF")
  
}

aggsCollector <- function(lid, yid, mid, data_dir, locsdf, popsdf) {
  scaledf <- popScalar(yid, lid, locsdf, popsdf)
  ## ## ## ## ## ##
  # PAF
  inputdfPAF <- rbindlist(lapply(as.list(locsdf$location_id[locsdf$parent_id == lid & locsdf$location_id != lid]), function(lid, data_dir, yid, mid) {
                                                                                                                                                  load(paste0(data_dir, "/draws/inputs/", yid, "/PAF_", mid, "_", lid, ".RData"))
                                                                                                                                                  return(inputdf)
                                                                                                                                                }, 
                                                                                                                                                data_dir, yid, mid))
  inputdfPAF <- inputdfPAF[, location_id := lid][, .(paf = sum(paf * val) / sum(val), val = sum(val)), by = .(location_id, year_id, age_group_id, sex_id, measure_id, cause_id, draw)]
  inputdfPAF <- inputdfPAF[val == 0, paf := 0]
  if (lsid == 35) {
    inputdfPAF <- merge(inputdfPAF,
                        scaledf,
                        by = c("location_id", "year_id", "age_group_id", "sex_id"))
    inputdfPAF <- inputdfPAF[, val := val * pop_scalar]
  }
  
  # Save
  saveData(inputdfPAF[, c("location_id", "year_id", "age_group_id", "sex_id", "measure_id", "cause_id", "draw", "paf", "val"), with = FALSE], 
           data_dir, lid, yid, mid, "PAF")
  
}

##########################################################
## RUN PROGRAM
load(paste0(data_dir, "/locsdf_", lsid, ".RData"))
load(paste0(data_dir, "/popsdf_", lsid, ".RData"))

## If most detailed location, get from source
if (locsdf$most_detailed[locsdf$location_id == lid] == 1) { mostDetailedCollector(lid, yid, mid, paf_vers, cc_vers, data_dir)
} else {
  ## If aggregate, find most-detailed locations that have already been saved and aggregate them (attach pop for scaling)
  aggsCollector(lid, yid, mid, data_dir, locsdf, popsdf)
}

##########################################################
## END SESSION
quit("no")

##########################################################