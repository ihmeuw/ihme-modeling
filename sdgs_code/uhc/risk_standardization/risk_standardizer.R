##########################################################
# Author: Ryan Barber
# Date: 03 August 2016
# Description: Risk standardization
##########################################################
## PARSE ARGUMENTS
parser <- ArgumentParser()
parser$add_argument("--prog_dir", help="Program directory",
                    type="character")
parser$add_argument("--data_dir", help="Space where results are to be saved",
                     type="character")
parser$add_argument("--lsid", help="Location set",
                    dUSERt=35, type="integer")
parser$add_argument("--lid", help="Location",
                    dUSERt=13, type="integer")
parser$add_argument("--yid", help="Year for current job",
                    dUSERt=2005, type="integer")
parser$add_argument("--mid", help="Measure",
                    dUSERt=1, type="integer")
parser$add_argument("--scale_ceiling", help="Upper limit of PAFs (out of 100)",
                    dUSERt=90, type="integer")
args <- parser$parse_args()
list2env(args, .GlobalEnv)
rm(args)

##########################################################
## DEFINE FUNCTIONS
source(paste0(prog_dir, "/utilities.R"))

joinGlobal <- function(df, data_dir, lsid, mid, prefix) {
  # Load all years, and use average
  globdf <- rbindlist(lapply(as.list(c(seq(1990, 2010, 5), 2016)), function(yid, data_dir, prefix) {
                                                                                                      load(paste0(data_dir, "/draws/inputs/", yid, "/", prefix, "_", mid, "_1.RData"))
                                                                                                      return(inputdf)
                                                                                                    },
                             data_dir, prefix))
  if (prefix == "PAF") {
    # Aggregate attributable burden over all years and join
    globdf <- globdf[, .(glob_paf = sum(paf * val) / sum(val), val = sum(val)), by = .(age_group_id, sex_id, measure_id, cause_id, draw)]
    globdf <- globdf[val == 0, glob_paf := 0]
    globdf$val <- NULL
    df <- merge(df,
                globdf,
                by = c("age_group_id", "sex_id", "measure_id", "cause_id", "draw"))
  }
  return(df)
}

makeAgeWeights <- function(cause_list){
  ageweightdf <- getAgeGroups()
  cs_ageweightdf <- data.table()
  for (cid in cause_list$cause_id) {
    start <- cause_list[cause_id == cid]$age_group_years_start
    end <- cause_list[cause_id == cid]$age_group_years_end
    cause_weights <- data.table(ageweightdf)
    cause_weights$cause_id <- cid
    cause_weights <- cause_weights[age_group_years_start >= start & age_group_years_start < end]
    cause_weights <- cause_weights[, age_group_weight_value := age_group_weight_value / sum(age_group_weight_value)]
    cs_ageweightdf <- rbind(cs_ageweightdf,
                            cause_weights[, c("cause_id", "age_group_id", "age_group_weight_value"), with = FALSE])
  }
  return(cs_ageweightdf)
}


riskStandardize <- function(prefix, data_dir, yid, lid, mid, lsid, scale_ceiling) {
  # Read in PAFs and CoDCorrect for each most-detailed location
  load(paste0(data_dir, "/draws/inputs/", yid, "/", prefix, "_", mid, "_", lid, ".RData"))
  
  # Load location hierarchy data frame
  locsdf <- prepLocHierarchy(data_dir, lsid)
  
  # Use deaths to aggregate PAF up to global
  inputdf <- joinGlobal(inputdf, data_dir, lsid, mid, prefix)
  
  if (prefix == "PAF") {
    # # Drop HIV
    # inputdf <- inputdf[cause_id != 298, ]
    # 
    # Apply scalars
    load(paste0(data_dir, "/draws/inputs/PAF_", mid, "_scalar_", scale_ceiling, ".RData"))
    inputdf <- merge(inputdf,
                     scalardf,
                     by = c("age_group_id", "sex_id", "measure_id", "cause_id"))
    inputdf <- inputdf[, c("paf", "glob_paf") := .(paf * pafscalar, glob_paf * pafscalar)][, pafscalar := NULL]
    
    # Produce risk-standardized deaths
    inputdf <- inputdf[, rsval := val * (1 - paf) * (1 / (1 - glob_paf))]
    inputdf <- inputdf[is.na(rsval), rsval := 0]
    
    # For PAFs of one and for diarrhea, use observed
    inputdf <- inputdf[paf == 1 | cause_id == 302, rsval := val]
  }
  
  # Only keep Nolte/McKee causes and ages
  cause_list <- fread(paste0(prog_dir, "/amenable_cause_list_GBD.csv"))
  causesdf <- prepCauseHierarchy(data_dir)
  
  # Aggregate GBD causes and append amenable
  inputdf <- merge(inputdf,
                   causesdf,
                   by = "cause_id")
  inputdf <- rbindlist(lapply(as.list(c("root", paste0("L", seq(1, max(causesdf$level))))), hierarchyAgg, inputdf, "cause_id", c("rsval")))
  
  # Aggregate to Nolte/McKee list, attach to main data frame
  inputdf <- merge(inputdf,
                   cause_list[, c("cause_id", "age_group_id_start", "age_group_id_end"), with = FALSE],
                   by = "cause_id")
  inputdf <- inputdf[age_group_id >= age_group_id_start & age_group_id <= age_group_id_end,]
  
  # Aggregate sexes
  inputdf <- inputdf[, bothsex := 3]
  inputdf <- hierarchyAgg("bothsex", inputdf, "sex_id", c("rsval"))
  
  # Age-standardize
  ageweightdf <- makeAgeWeights(cause_list)
  load(paste0(data_dir, "/popsdf_", lsid, ".RData"))
  inputdf <- merge(inputdf,
                   popsdf,
                   by = c("location_id", "year_id", "age_group_id", "sex_id"))
  inputdf <- merge(inputdf,
                   ageweightdf,
                   by = c("cause_id", "age_group_id"),
                   all.x = TRUE)
  if (nrow(inputdf[is.na(age_group_weight_value)]) > 0) stop("Problem with age weight merge")
  inputdf <- inputdf[, age_group_id := 27]
  inputdf <- inputdf[, .(rsval = sum((rsval / population) * age_group_weight_value)), by = .(location_id, year_id, age_group_id, sex_id, cause_id, draw)]
  
  # Return data frame
  write.csv(inputdf[, c("location_id", "year_id", "age_group_id", "sex_id", "cause_id", "draw", "rsval"), with = FALSE],
            file = paste0(data_dir, "/draws/standardized/", yid, "/", lid, ".csv"),
            row.names = FALSE)
}

##########################################################
## RUN PROGRAM
# Risk standardize (should be changed from rsdeathsdf to rsdf, missed that in the update)
riskStandardize("PAF", data_dir, yid, lid, mid, lsid, scale_ceiling)

##########################################################
## END SESSION
quit("no")

##########################################################