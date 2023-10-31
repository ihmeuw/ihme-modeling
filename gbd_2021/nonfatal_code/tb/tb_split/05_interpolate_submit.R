## EMPTY THE ENVIRONMENT
rm(list = ls())

## ESTABLISH FOCAL DRIVES
os <- .Platform$OS.type
if (os=="windows") {
  j <-"J:/"
  h <-"H:/"
  k <-"K:/"
} else {
  j <-"/home/j/"
  h <-paste0("homes/", Sys.info()[7], "/")
  k <-"/ihme/cc_resources/"
}

## LOAD FUNCTIONS AND PACKAGES
source(paste0(k, "libraries/current/r/get_demographics.R"))
source(paste0(k, "libraries/current/r/save_results_epi.R"))
source(paste0("/ihme/epi/tb/code/qsub.R"))

#############################################################################################
###                             INITIALIZE OBJECTS AND TOGGLES                            ###
#############################################################################################

## HELPER VECTORS
param_dir <- "/ihme/epi/tb/code/"
code_dir  <- "/ihme/epi/tb/code/"
shell_dir <- "/ihme/singularity-images/rstudio/shells/execRscript.sh"
out_dir   <- "/ihme/epi/tb/hiv_tb/temp/interpolation/draws/"

## MODEL VERSION
meids       <- 10832:10834
mvid        <- "dismod_504482"
decomp_step <- "iterative"
xwalk_id    <- 21923
out_dir     <- paste0(out_dir, mvid, "/")

## CREATE DIRECTORS
dir.create(path = paste0(out_dir))
dir.create(path = paste0(out_dir, "ds/"))
dir.create(path = paste0(out_dir, "mdr/"))
dir.create(path = paste0(out_dir, "xdr/"))
dir.create(path = paste0(out_dir, "hivtb/"))

## FUNCTION FOR GETTING DIRECTORY
get_dir <- function(meid, directory){
  if (meid == 10832) draw_directory <- paste0(directory, "ds/")
  if (meid == 10833) draw_directory <- paste0(directory, "mdr/")
  if (meid == 10834) draw_directory <- paste0(directory, "xdr/")
  return(draw_directory)
}

#############################################################################################
###                                    START SCRIPT                                       ###
#############################################################################################

## GET LOCATIONS TO PARALLIZE BY
dems  <- get_demographics(gbd_team = "epi")
locs  <- dems$location_id
years <- dems$year_id

## SET UP PARAMETER MAP
param_map <- as.data.table(expand.grid(location_id = locs))
num_jobs  <- nrow(param_map)

## CREATE EXTRA COLUMNS AND SAVE
param_map[, `:=` (year_start = min(years), year_end = max(years))]
fwrite(param_map, paste0(param_dir, "parameters.csv"), row.names = F)

## SUBMIT ARRAY JOBS
for (meid in meids) {
  
  draw_dir <- get_dir(meid, out_dir)

  qsub(job_name = paste0("interpolation_for_", meid),
       shell    = shell_dir,
       code     = paste0(code_dir, "interpolate_child.R"),
       args     = list("--param_path", paste0(param_dir, "parameters.csv"),
                       "--decomp_step", decomp_step, 
                       "--meid", meid,
                       "--out_dir", draw_dir),
       project  = "proj_tb",
       num_jobs = num_jobs,
       threads  = 1,
       memory   = 4,
       queue    = "all",
       output   = "/ihme/temp/sgeoutput/joyma/output",
       error    = "/ihme/temp/sgeoutput/joyma/error")
}

#############################################################################################
###                                   WAIT FOR JOBS TO FINISH                             ###
#############################################################################################
  
## WAIT FOR JOBS TO COMPLETE
for (meid in meids) {
  for (loc in unique(param_map$location_id)) {
    for (measure in 5:6){
      draw_directory <- get_dir(meid, out_dir)
      while (!file.exists(paste0(draw_directory, measure, "_", loc, ".csv"))) {
        cat(paste0("Waiting for file: ", draw_directory, measure, "_", loc, ".csv -- ", Sys.time(), "\n"))
        Sys.sleep(60)
      }
    }
  }
}

#############################################################################################
###                                    SAVE RESULTS                                       ###
#############################################################################################

## SUBMIT ARRAY JOBS
for (meid in meids) {
  
  draw_dir <- get_dir(meid, out_dir)

  qsub(job_name = paste0("upload_save_results_", meid),
       shell    = shell_dir,
       code     = paste0(code_dir, "save_results_child.R"),
       args     = list("--mvid", mvid,
                       "--decomp_step", decomp_step, 
                       "--meid", meid,
                       "--draw_dir", draw_dir,
                       "--year_start", min(years),
                       "--year_end", max(years),
                       "--xwalk_id", xwalk_id),
       project  = "proj_tb",
       num_jobs = 1,
       threads  = 40,
       memory   = 300,
       queue    = "long", 
       time     = "240:00:00",
       output   = "/ihme/temp/sgeoutput/joyma/output",
       error    = "/ihme/temp/sgeoutput/joyma/error")
       # output   = "/home/j/temp/TB/sgeoutput/joyma/output",
       # error    = "/home/j/temp/TB/sgeoutput/joyma/error")
}

#############################################################################################
###                                   RUN AGGREGATOR                                      ###
#############################################################################################

## SUBMIT AGGREGATOR
qsub(job_name = paste0("hivtb_aggregator"),
     shell    = shell_dir,
     code     = paste0(code_dir, "agg_hivtb_child.R"),
     args     = list("--param_path", paste0(param_dir, "parameters.csv"),
                     "--draw_dir", out_dir),
     project  = "proj_tb",
     num_jobs = num_jobs,
     threads  = 1,
     memory   = 3,
     queue    = "all",
     output   = "/ihme/temp/sgeoutput/joyma/output",
     error    = "/ihme/temp/sgeoutput/joyma/error")

## WAIT FOR JOBS TO COMPLETE
for (loc in unique(param_map$location_id)) {
  for (measure in 5:6){
    while (!file.exists(paste0(out_dir, "hivtb/", measure, "_", loc, ".csv"))) {
      cat(paste0("Waiting for file: ", out_dir, "hivtb/", measure, "_", loc, ".csv -- ", Sys.time(), "\n"))
      Sys.sleep(60)
    }
  }
}

#############################################################################################
###                                    SAVE RESULTS                                       ###
#############################################################################################

qsub(job_name = paste0("upload_save_results_", 1176),
     shell    = shell_dir,
     code     = paste0(code_dir, "save_results_child.R"),
     args     = list("--mvid", mvid,
                     "--decomp_step", decomp_step, 
                     "--meid", 1176,
                     "--draw_dir", paste0(out_dir, "hivtb/"),
                     "--year_start", min(years),
                     "--year_end", max(years),
                     "--xwalk_id", xwalk_id),
     project  = "proj_tb",
     num_jobs = 1,
     threads  = 15,
     memory   = 140,
     queue    = "long", 
     time     = "14:30:00",
     output   = "/ihme/temp/sgeoutput/joyma/output",
     error    = "/ihme/temp/sgeoutput/joyma/error")

#############################################################################################
###                                         DONE                                          ###
#############################################################################################

