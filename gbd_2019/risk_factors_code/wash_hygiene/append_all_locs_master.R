# Author: NAME
# Date: 8/29/19
# Purpose: Master script for append_all_locs

# config
library(data.table)
library(openxlsx)
library(magrittr)

decomp_step <- "iterative" # change as needed
run <- "run1" # first run

# read in best stgpr run_ids & add parent me_name
stgpr_runs <- read.xlsx("FILEPATH") %>% data.table
stgpr_runs <- stgpr_runs[(me_name %like% "wash" | me_name %like% "fecal") & iterative_best == 1, .(me_name, run_id)] # change [step]_best to whatever is current
stgpr_runs[me_name %like% "sanitation", me_parent := "wash_sanitation"]
stgpr_runs[me_name %like% "water" | me_name %like% "fecal" | me_name %like% "treat", me_parent := "wash_water"]
stgpr_runs[me_name %like% "hwws", me_parent := "wash_hwws"]

# launch
for (me in stgpr_runs$me_name) {
  run_id <- stgpr_runs[me_name == me, run_id]
  me_parent <- stgpr_runs[me_name == me, me_parent]
  
  job <- paste0("qsub -N append_all_locs_", me, "_", run_id,
                " -l m_mem_free=10G -l fthread=10 -q all.q -l h_rt=1:00:00 ",
                "-P proj_custom_models -o FILEPATH -e FILEPATH ",
                "FILEPATH ",
                "FILEPATH ", me, " ", run_id, " ", me_parent, " ", run, " ", decomp_step)
  
  system(job); print(job)
}
