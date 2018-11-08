##########################################################
# Description: Qsubs age-standardized MI ratios 
##########################################################
## DEFINE ROOT AND LIBRARIES
rm(list=ls())
if (Sys.info()[1] == "Linux"){
  j <- "/home/j"
  h <- paste0("/homes/",Sys.info()[8])
}else if (Sys.info()[1] == "Windows"){
  j <- "J:"
  h <- "H:"
}else if (Sys.info()[1] == "Darwin"){
  j <- "/Volumes/snfs/"
  h <- paste0("/Volumes/", Sys.info()[6], "/")
}


##########################################################
## PARAMETERIZATION
prog_dir <- ("FILEPATH")
data_dir <- ("FILEPATH")
lsid <- 35
gbdrid <-5
csid <- 3
cc_vers <- 89
paf_vers <- "228_amenable"
yids <- c(1990:2017)
drawnum <- 1000
mid <- 1

##########################################################
## BUILD FILE SYSTEM
dir.create(data_dir)
dir.create(paste0(data_dir, "FILEPATH"))

# Inputs
dir.create(paste0(data_dir, "FILEPATH"))
for (yid in yids) {
  dir.create(paste0(data_dir, "FILEPATH"))

}
##########################################################
## DEFINE FUNCTIONS
source(paste0(data_dir, "FILEPATH"))

##########################################################
## SAVE COMMON DATA BASE FETCHES
if (!file.exists(paste0(data_dir, "FILEPATH"))) { #location file
  locsdf <- get_location_metadata(location_set_id = lsid, gbd_round_id = gbdrid)
  save(locsdf,
       file = paste0(data_dir, "FILEPATH"))
} else load(paste0(data_dir, "FILEPATH"))

if (!file.exists(paste0(data_dir, "FILEPATH"))) { #population file
  popsdf <- get_population(location_set_id = lsid, gbd_round_id = gbd_round_id)
  save(popsdf,
       file = paste0(data_dir, "FILEPATH"))
} else load(paste0(data_dir, "FILEPATH"))

if (!file.exists(paste0(data_dir, "FILEPATH"))) { #causes file
  causesdftest <- get_cause_metadata(cause_set_id = csid, gbd_round_id = gbdrid)
  save(causesdf,
       file = paste0(data_dir, "FILEPATH"))
} else load(paste0(data_dir, "FILEPATH"))

##########################################################

locs <- unique(locsdf$location_id)

# Risk-standardized mortality calculator
for (yid in yids) {
  for (lid in locs) {
    if (!file.exists(paste0(data_dir, "FILEPATH")))
      qsub(jobname = paste0("mi_", lid , "_", yid ), 
           logloc = 'FILEPATH',
           shell = paste0(prog_dir, "/r_shell.sh"), 
           code = paste0(prog_dir, "/cancer_mi_ratios.R"), 
           project = "sdg", 
           args = list("--prog_dir", prog_dir, "--data_dir", data_dir, "--lsid", lsid, "--lid", lid, "--yid", yid, "--mid", mid), 
           slots = 3)
    }
  }
  Sys.sleep(30) # Sleep 30 seconds between year submissions
#}
# Check files
for (yid in yids) {
  for (lid in locsdf$location_id) {
    while (!file.exists(paste0(data_dir, "FILEPATH"))) {
      print(paste0("Waiting for MIRs to be stored (location_id: ", lid, "; year_id: ", yid, "; measure_id: ", mid, ") -- ", Sys.time()))
      Sys.sleep(30)
    }
  }
}

###########################################################

  

  