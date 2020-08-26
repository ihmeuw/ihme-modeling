### Notes: Used to calculate the PAF for CKD
### USERNAME


# Last Used:
# OCTOBER 2019, Step 4
# 
#

require(ini)

source("FILEPATH/launch_paf.R")
launch_paf(341, decomp_step="step4", cluster_proj = 'proj_yld', m_mem_free="8G", resume=FALSE)


### Check outputs 
source("FILEPATH/paf_scatter.R")
paf_scatter(rei_id = 341, year_id = 1995, loc_level = 4, version_id = 478685, gbd_round_id = 6, measure_id = 3,
            file_path = "FILEPATH/ikf_scatter_1995.pdf")
