#--------------------------------------------------------------
# Project: CKD data processing pipeline
# Purpose: Process and upload crosswalk versions for CKD bundles
#--------------------------------------------------------------


# setup -------------------------------------------------------------------

user <- Sys.info()["user"]

source(paste0("FILEPATH", "/function_lib.R"))
source(paste0("FILEPATH","data_processing_functions.R"))
source(paste0("FILEPATH","get_bundle_version_ids.R"))

# Set decomp step
ds<-"step2"

# Set crosswalk version description
description<-"first_attempt_at_running_full_pipeline_for_sex_split_and_crosswalk"

# Set bundles to run for
ckd_bids<-c(917)

# Pull in most recent bundle version ids to prep for sex splitting
bvids<-get_bundle_version_ids(ckd_bids)
bvids[,last_updated:=as.POSIXct(last_updated,format='%Y-%m-%d %H:%M:%S')]
bvids<-bvids[decomp_step_id==as.numeric(gsub("\\D","",ds)),
             .SD[which.max(last_updated)],keyby=bundle_id]
if (nrow(bvids)==0) stop("No bundle version ids for this decomp step")

# Set gbd round
gbd_rnd<-unique(bvids[,gbd_round_id])

# Supply path to .csv mapping bundle ids to crosswalks 
cv_ref_map_path<-paste0("FILEPATH") # must be a .csv

# Set path to script to launch
process_bundles<-paste0("FILEPATH")

# Set path to shell script
shell <- 'FILEPATH'

# Set memory, threads, and runtime
mem <- 50
thrds <- 1
runt <- "00:30:00"

# launch pipline ----------------------------------------------------------

for (bid in ckd_bids){
  error_path <- "FILEPATH"
  if (!file.exists(error_path)){dir.create(error_path,recursive=T)}
  bvid<-bvids[bundle_id==bid, bundle_version_id]
  acause<-bvids[bundle_id==bid, acause]
  output_file_name<-paste0("FILEPATH",bvid,".xlsx")
  pass_args<-list(bid, bvid, acause, ds, gbd_rnd, output_file_name, description, cv_ref_map_path,
                  thrds)
  job_n <- paste0("process_bundle_data_bid_",bid)
  construct_qsub(mem_free = mem, threads = thrds, runtime = runt, script = process_bundles, 
                 pass = pass_args, jname = job_n, shell = shell, submit = T, errors = error_path)
}

