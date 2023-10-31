# launch save_pafs_cdiff

# Validation: make sure all locations are present in yll and yld pafs before running save pafs.
source("/PATH/pafs_check_unfinished_locs.R")

# Check missingness in estimation year pafs.
yll <- check_unfinished_locs(location_set_id=35, gbd_round_id=7, decomp_step="iterative",
                             paf_dir="/PATH",
                             measure_name="yll")
yld <- check_unfinished_locs(location_set_id=35, gbd_round_id=7, decomp_step="iterative",
                             paf_dir="/PATH",
                             measure_name="yld")


# If no locations are missing, run save pafs.
if(length(yll) == 0 & length(yld) == 0) {
  qsub = paste0(JOB)
  
  # submit job
  system(qsub)
}
