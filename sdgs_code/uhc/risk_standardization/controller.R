##########################################################
# Author: Ryan Barber
# Date: 03 August 2016
# Description: Controls mortality amenable to healthcare and avertable burden analyses
##########################################################
## ACTION TOGGLES
sweep.inputs <- FALSE
sweep.standardized <- FALSE

##########################################################
## PARAMETERIZATION
prog_dir <- "FILEPATH"
scale_ceiling <- 90
lsid <- 35
csid <- 3
cc_vers <- 66
paf_vers <- "199_amenable"
yids <- c(seq(1990, 2010, 5), 2016)
drawnum <- 1000

##########################################################
## BUILD FILE SYSTEM
data_dir <- "FILEPATH"
dir.create(data_dir)
dir.create(paste0(data_dir, "/draws"))
# Inputs
if (sweep.inputs) {
  print("About to delete input directory...")
  Sys.sleep(10)
  print("... too late! It's happening!")
  unlink(paste0(data_dir, "/draws/inputs"), recursive=TRUE)
  print("Finished.")
}
dir.create(paste0(data_dir, "/draws/inputs"))
for (yid in yids) {
  dir.create(paste0(data_dir, "/draws/inputs/", yid))
  dir.create(paste0(data_dir, "/draws/inputs/", yid, "/md_sums"))
}

# Risk-standardized
if (sweep.standardized) {
  print("About to delete risk-standardized directory...")
  Sys.sleep(10)
  print("... too late! It's happening!")
  unlink(paste0(data_dir, "/draws/standardized"), recursive=TRUE)
  print("Finished.")
}
dir.create(paste0(data_dir, "/draws/standardized"))
for (yid in yids) {
  dir.create(paste0(data_dir, "/draws/standardized/", yid))
}

##########################################################
## DEFINE FUNCTIONS
source(paste0(prog_dir, "/utilities.R"))

##########################################################
## SAVE COMMON DATA BASE FETCHES
if (!file.exists(paste0(data_dir, "/locsdf_", lsid, ".RData"))) {
  locsdf <- getLocations(lsid)
  save(locsdf,
       file = paste0(data_dir, "/locsdf_", lsid, ".RData"))
} else load(paste0(data_dir, "/locsdf_", lsid, ".RData"))

if (!file.exists(paste0(data_dir, "/popsdf_", lsid, ".RData"))) {
  popsdf <- getPops(lsid, sids = c(1, 2, 3))
  save(popsdf,
       file = paste0(data_dir, "/popsdf_", lsid, ".RData"))
} else load(paste0(data_dir, "/popsdf_", lsid, ".RData"))

if (!file.exists(paste0(data_dir, "/causesdf.RData"))) {
  causesdf <- getCauses(csid)
  save(causesdf,
       file = paste0(data_dir, "/causesdf.RData"))
} else load(paste0(data_dir, "/causesdf.RData"))

##########################################################
## SUBMISSION BY LOCATION SET
# Risk standardize by measure (only deaths)
mid <- 1

# Save combined inputs
for (loclvl in sort(unique(locsdf$level), decreasing = TRUE)) {
  for (lid in locsdf$location_id[locsdf$level == loclvl]) {
    # Determine slots to use based on children #
    childnum <- nrow(locsdf[parent_id == lid,])
    slotnum <- childnum
    if (slotnum < 5) slotnum <- 5
    if (lsid != 35) slotnum <- 70
    # # # # # # # # # # # # # # # # # # # # # #
    for (yid in yids) {
      if (!file.exists(paste0(data_dir, "/draws/inputs/", yid, "/PAF_", mid, "_", lid, ".RData"))) { 
        qsub(jobname = paste0("input_draws_location_", lid, "_year_", yid, "_measure_", mid), 
             shell = paste0(prog_dir, "/r_shell.sh"), 
             code = paste0(prog_dir, "/prep_inputs.R"), 
             project = "sdg", 
             args = list("--prog_dir", prog_dir, "--data_dir", data_dir, "--cc_vers", cc_vers, "--paf_vers", paf_vers, "--lsid", lsid, "--lid", lid, "--yid", yid, "--mid", mid, "--drawnum", drawnum), 
             slots = slotnum)
      }
    }
  }
  # Wait for saved files from this location level before submitting next level
  for (lid in locsdf$location_id[locsdf$level == loclvl]) {
    for (yid in yids) {
      while (!file.exists(paste0(data_dir, "/draws/inputs/", yid, "/PAF_", mid, "_", lid, ".RData"))) {
        print(paste0("Waiting for PAF input draws to be stored (location_id: ", lid, "; year_id: ", yid, "; measure_id: ", mid, ") -- ", Sys.time()))
        Sys.sleep(30)
      }
    }
  }
}

# Read in PAF summaries, save file that contains all requisite scalars
if (!file.exists(paste0(data_dir, "/draws/inputs/PAF_", mid, "_scalar_", scale_ceiling, ".RData"))) {
  qsub(jobname = paste0("paf_scalar_", mid), 
       shell = paste0(prog_dir, "/r_shell.sh"), 
       code = paste0(prog_dir, "/set_scalars.R"), 
       project = "sdg", 
       args = list("--prog_dir", prog_dir, "--data_dir", data_dir, "--lsid", lsid, "--yids", paste0(yids, collapse = " "), "--mid", mid, "--scale_ceiling", scale_ceiling), 
       slots = 5)
}
while (!file.exists(paste0(data_dir, "/draws/inputs/PAF_", mid, "_scalar_", scale_ceiling, ".RData"))) {
  print(paste0("Waiting for PAF scalar to be stored -- ", Sys.time()))
  Sys.sleep(30)
}

# Risk-standardized mortality calculator
for (yid in yids) {
  for (lid in locsdf$location_id) {
    if (!file.exists(paste0(data_dir, "/draws/standardized/", yid, "/", lid, ".csv"))) {
      qsub(jobname = paste0("risk_stand_", lid, "_", yid, "_", mid), 
           shell = paste0(prog_dir, "/r_shell.sh"), 
           code = paste0(prog_dir, "/risk_standardizer.R"), 
           project = "sdg", 
           args = list("--prog_dir", prog_dir, "--data_dir", data_dir, "--lsid", lsid, "--lid", lid, "--yid", yid, "--mid", mid, "--scale_ceiling", scale_ceiling), 
           slots = 3)
    }
  }
  Sys.sleep(30) # Sleep 30 seconds between year submissions
}
# Check files
for (yid in yids) {
  for (lid in locsdf$location_id) {
    while (!file.exists(paste0(data_dir, "/draws/standardized/", yid, "/", lid, ".csv"))) {
      print(paste0("Waiting for risk-standardized deaths to be stored (location_id: ", lid, "; year_id: ", yid, "; measure_id: ", mid, ") -- ", Sys.time()))
      Sys.sleep(30)
    }
  }
}

##########################################################