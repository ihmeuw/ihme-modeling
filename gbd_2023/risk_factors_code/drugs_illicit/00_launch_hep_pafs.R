### Master launching script for hep pafs
# Three separate script launches and save the results
# Make sure to go into each individual to modify dates and directories if needed

# Pre work:
# 1) Need a correctly run IDU model
# 2) Need the year beta from the dismod IDU model (view effects)
# 3) Run mrbrt on hep risk data and get the associated risk and standard error

# Process
# 1) Launch script 1
# 2) Save results of interpolation
# 3) Run script 2


source("FILEPATH")

locs <- get_location_metadata(location_set_id = 22, gbd_round_id = 7)
loc_list <- unique(locs$location_id[locs$level >= 3])
loc_list <- c(loc_list, 44952, 44649, 4661, 35509)
project <- "proj_yld"

loc_list <- 102  # run with one location to test first. Comment out if it works properly


### Script 1: Interpolate IDU model -------------------------------------------

for(loc in loc_list) {
  idx <- which(loc_list == loc)
  if (idx %% 100 == 0) {
    message(idx)
  }

  command <- paste0("qsub -l m_mem_free=20G -l fthread=4 -q all.q -P ",
                    project,
                    " -e FILEPATH -N loc_", loc,
                    " FILEPATH ", loc)
  system(command)
}

## Save: Run this in R on Cluster with a qlogin ------------------------------
source("FILEPATH")
save_results_epi(
  input_dir = "FILEPATH",
  input_file_pattern = "{location_id}.csv",
  modelable_entity_id = 16436,
  description = "Iterative Interpolated Annual Results, Final with more ages",
  year_id = 1990:2022,
  sex_id = 1:2,
  measure_id = 5,
  gbd_round_id = 7,
  decomp_step = "iterative", mark_best = T,
  bundle_id = 7142, crosswalk_version_id = 24557
)


## Script 2: Interpolate hepatitis models ------------------------------------

for(loc in  loc_list) {
    idx <- which(loc_list == loc)
    if (idx %% 100 == 0) {
      message(idx)
    }

    command <- paste0("qsub -l m_mem_free=16G -l fthread=1 -q all.q -P ",
                      project,
                      " -e FILEPATH", loc,
                      " FILEPATH ", loc)
    system(command)
}

### Script 3: Calculate cumulative pafs ---------------------------------------

for(loc in  loc_list) {
  idx <- which(loc_list == loc)
  if (idx %% 100 == 0) {
    message(idx)
    Sys.sleep(3)
  }

    command <- paste0("qsub -l m_mem_free=20G -l fthread=4 -q all.q -P ",
                      project, " -e FILEPATH", loc,
                      "FILEPATH ", loc)
    system(command)
}


source("FILEPATH")

threads <- 4
memory <- 20
runtime <- "5:00:00"
q <- "all.q"

# locs <- loc_list[1:10]
job.array.master(tester = F,
                 paramlist = loc_list,
                 username = "USERNAME",
                 project = "proj_yld",
                 threads = threads,
                 mem_free = memory,
                 runtime = runtime,
                 errors = "FILEPATH",
                 q = q,
                 jobname = "calc_paf_03",
                 childscript = "FILEPATH",
                 shell = "FILEPATH")


### Save: Run this in R on Cluster with a qlogin ------------------------------

# Run this after 03_calc_paf. Qlogin needs 150g and 80threads.
# Takes like 5 hours
source("FILEPATH")
save_results_risk(
  input_dir = "FILEPATH",
  input_file_pattern = "paf_{measure}_{location_id}_{year_id}_{sex_id}.csv",
  modelable_entity_id = 8798,
  risk_type = "paf",
  description = "AR, update IDU, updated mrbrt",
  year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019, 2020),
  mark_best = T,
  decomp_step = "iterative"
)
