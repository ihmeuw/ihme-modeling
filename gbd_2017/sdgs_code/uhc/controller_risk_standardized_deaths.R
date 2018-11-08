##########################################################
# Description: Controls mortality amenable to healthcare and avertable burden analyses
##########################################################
## DEFINE ROOT AND LIBRARIES
rm(list=ls())
if (Sys.info()[1] == "Linux"){
  j <- "/home/j"
  h <- paste0("/homes/",Sys.info()[7])
}else if (Sys.info()[1] == "Windows"){
  j <- "J:"
  h <- "H:"
}else if (Sys.info()[1] == "Darwin"){
  j <- "/Volumes/snfs/"
  h <- paste0("/Volumes/", Sys.info()[6], "/")
}

##########################################################
## ACTION TOGGLES
sweep.inputs <- FALSE
sweep.standardized <- FALSE

##########################################################
## PARAMETERIZATION
prog_dir <- paste0("FILEPATH")
scale_ceiling <- 90
lsid <- 35
csid <- 3
cc_vers <- 89
paf_vers <- "228_amenable"
yids <- c(1990:2017)
drawnum <- 1000
mid <- 1
rndid <- 5 
##########################################################
## BUILD FILE SYSTEM
data_dir <- paste0("FILEPATH")
dir.create(data_dir)
dir.create(paste0(data_dir, "FILEPATH"))
# Inputs
if (sweep.inputs) {
 print("About to delete input directory...")
 Sys.sleep(10)
 print("... Deleting directory")
 unlink(paste0(data_dir, "FILEPATH"), recursive=TRUE)
 print("Finished.")
}
dir.create(paste0(data_dir, "FILEPATH"))
for (yid in yids) {
 dir.create(paste0(data_dir, "FILEPATH", yid))
 dir.create(paste0(data_dir, "FILEPATH"))
}

# Risk-standardized
if (sweep.standardized) {
 print("About to delete risk-standardized directory...")
 Sys.sleep(10)
 print("... Deleting directory")
 unlink(paste0(data_dir, "FILEPATH"), recursive=TRUE)
 print("Finished.")
}
dir.create(paste0(data_dir, "FILEPATH"))
for (yid in yids) {
 dir.create(paste0(data_dir, "FILEPATH", yid))
}

##########################################################
## DEFINE FUNCTIONS
source(paste0(prog_dir, "FILEPATH"))

##########################################################
## SAVE COMMON DATA BASE FETCHES
if (!file.exists(paste0(data_dir, "FILEPATH"))) { #location file
  locsdf <- get_location_metadata(location_set_id = lsid, gbd_round_id = rndid)
  save(locsdf,
       file = paste0(data_dir, "FILEPATH"))
} else load(paste0(data_dir, "FILEPATH"))

if (!file.exists(paste0(data_dir, "FILEPATH"))) { #population file
  popsdf <-  get_population(location_set_id = lsid, age_group_id = 'all', location_id = 'all',
                            sex_id = c(1,2,3), year_id = yids, gbd_round_id = rndid)
  save(popsdf,
       file = paste0(data_dir, "FILEPATH"))
} else load(paste0(data_dir, "FILEPATH"))

if (!file.exists(paste0(data_dir, "FILEPATH"))) { # causes file
  causesdf <- get_cause_metadata(cause_set_id =  csid, gbd_round_id = rndid)
  save(causesdf,
       file = paste0(data_dir, "FILEPATH"))
} else load(paste0(data_dir, "FILEPATH"))

##########################################################
# JOB SUBMISSION


# Risk-standardized mortality calculator
## SUBMISSION BY LOCATION SET
# Risk standardize by measure (only deaths)

#Save combined inputs
for (loclvl in sort(unique(locsdf$level), decreasing = TRUE)) {
  for (lid in locsdf$location_id[locsdf$level == loclvl]) {
    # Determine slots to use based on children #
    childnum <- nrow(locsdf[parent_id == lid,])
    slotnum <- childnum
    if (slotnum < 10) slotnum <- 8
    if (lsid != 35) slotnum <- 70
    # # # # # # # # # # # # # # # # # # # # # #
    for (yid in yids) {
      if (!file.exists(paste0(data_dir, "FILEPATH"))) {
        qsub(jobname = paste0("input_draws_location_", lid, "_year_", yid, "_measure_", mid),
             shell = paste0(prog_dir, "/r_shell.sh"),
             code = paste0(prog_dir, "prep_noncancer_inputs.R"),
             logloc = 'FILEPATH/USER',
             project = "sdg",
             args = list("--prog_dir", prog_dir, "--data_dir", data_dir, "--cc_vers", cc_vers, "--paf_vers", paf_vers, "--lsid", lsid, "--lid", lid, "--yid", yid, "--mid", mid, "--drawnum", drawnum),
             slots = slotnum)
      }
    }
  }
  # Wait for saved files from this location level before submitting next level
  for (lid in locsdf$location_id[locsdf$level == loclvl]) {
    for (yid in yids) {
      while (!file.exists(paste0(data_dir, "FILEPATH"))) {
        print(paste0("Waiting for PAF input draws to be stored (location_id: ", lid, "; year_id: ", yid, "; measure_id: ", mid, ") -- ", Sys.time()))
        Sys.sleep(30)
      }
    }
  }
}

# Read in PAF summaries, save file that contains all requisite scalars
if (!file.exists(paste0(data_dir, "FILEPATH"))) {
 qsub(jobname = paste0("paf_scalar_", mid),
      shell = paste0(prog_dir, "/r_shell.sh"),
      code = paste0(prog_dir, "/set_scalars.R"),
      logloc = 'FILEPATH/USER',
      project = "sdg",
      args = list("--prog_dir", prog_dir, "--data_dir", data_dir, "--lsid", lsid, "--yids", paste0(yids, collapse = " "), "--mid", mid, "--scale_ceiling", scale_ceiling),
      slots = 5)
}
while (!file.exists(paste0(data_dir, "FILEPATH"))) {
 print(paste0("Waiting for PAF scalar to be stored -- ", Sys.time()))
 Sys.sleep(30)
}


# Risk-standardized mortality calculator
for (yid in yids) {
 for (lid in locsdf$location_id) {
   if (!file.exists(paste0(data_dir, "FILEPATH"))) {
    qsub(jobname = paste0("risk_stand_", lid, "_", yid, "_", mid),
       shell = paste0(prog_dir, "/r_shell.sh"),
       code = paste0(prog_dir, "/risk_standardizer.R"),
       project = "sdg",
       logloc = "FILEPATH/USER",
       args = list("--prog_dir", prog_dir, "--data_dir", data_dir, "--lsid", lsid, "--lid", lid, "--yid", yid, "--mid", mid, "--scale_ceiling", scale_ceiling),
       slots = 3)
   }
 }
Sys.sleep(30) # Sleep 30 seconds between year submissions
}
# Check files
for (yid in yids) {
for (lid in locsdf$location_id) {
  while (!file.exists(paste0(data_dir, "FILEPATH"))) {
    print(paste0("Waiting for risk-standardized deaths to be stored (location_id: ", lid, "; year_id: ", yid, "; measure_id: ", mid, ") -- ", Sys.time()))
    Sys.sleep(30)
  }
}
}

# ##########################################################