#-------------------------------------------------------------------------------------------------
# DESCRIPTION ##  Gets draws (via interpolate) from GBD 2020 pafs, saves, and begins upload
# INPUTS ##   Called via qsub from launch_mean_sd.R. Requires arguments for the location_id, exp_sd_id, and me_name
# OUTPUTS ##  
# DATE ##  March 1, 2019
#-------------------------------------------------------------------------------------------------

# Drives 

## LOAD DEPENDENCIES
source(FILEPATH)

########################################################
parameters <- fread(paste0(FILEPATH, "smoking_histories/array_paf_interpolate.csv"))
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
location <- parameters[task_id, location_id]
me_name <- parameters[task_id, me_name]
id <- parameters[task_id, id]

print(me_name)
print(id)
print(location)

dir <- paste0(FILEPATH, "outputs/paf_no_gamma/")

outdir <- paste0(FILEPATH, "paf_interpolated_um")

  print("Interpolating paf results")
  dir.create(outdir)
  
  
  data3 <- interpolate(gbd_id_type = "rei_id", gbd_id = id, source="paf_unmediated", release_id = 16, location_id = location,reporting_year_start = 1990, reporting_year_end = 2024, measure_id = 3)
  data4 <- interpolate(gbd_id_type = "rei_id", gbd_id = id, source="paf_unmediated", release_id = 16, location_id = location,reporting_year_start = 1990, reporting_year_end = 2024, measure_id = 4)
  
  all_data <- rbindlist(list(data3, data4), use.names = T)  #bind YLLs and YLDs together
  
  write.csv(all_data, paste0(outdir, "/", location, ".csv"), row.names = FALSE)
  
  print("Interpolate has finished")
