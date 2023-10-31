# launch save_pafs_cdiff
# use qhosts in putty to check which queue has the most memory and threads available and edit accordingly.

# Validation: make sure all locations are present in yll and yld pafs before running save pafs.
source("/FILEPATH/pafs_check_unfinished_locs.R")

# Check missingness in estimation year pafs.
yll <- check_unfinished_locs(location_set_id=35, gbd_round_id=7, decomp_step="iterative",
                             paf_dir="/FILEPATH",
                             measure_name="yll")
yld <- check_unfinished_locs(location_set_id=35, gbd_round_id=7, decomp_step="iterative",
                             paf_dir="/FILEPATH",
                             measure_name="yld")


# If no locations are missing, run save pafs.
# For annual pafs: 24 threads, 100G memory.
# For estimation year pafs: 2 threads, 40G memory - runs and finishes overnight. 
if(length(yll) == 0 & length(yld) == 0) {
  qsub = paste0('qsub -e /FILEPATH/errors/ -o /FILEPATH/output/ -cwd -N save_cdiff_pafs -P ihme_general ',
                '-l fthread=24 -l m_mem_free=100G -q all.q -l archive=TRUE ',
                '/FILEPATH/execRscript.sh -s /FILEPATH/03_save_pafs_cdiff.R')
  
  # submit job
  system(qsub)
}
