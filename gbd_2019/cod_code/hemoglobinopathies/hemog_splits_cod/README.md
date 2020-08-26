# CoD hemoglobinopathy Splits

## Likely updates for GBD2019

- need to change all shared function calls (gbd_round_id 5 -> 6, likely argument changes as those have changed between rounds)
- will need to look into whether we are keeping the same strategy for the CoD workaround that Katie implemented last year
- qsub calls will likely need to be changed for usage with the new cluster (-l mem_free argument)

## Run Steps (as of 1/23/19, for Jordan hand-off)
1. qlogin to the cluster and open a stata session (_stata-mp_)

2. Submit jobs by sourcing the cod_split_hemog.ado file (_run 'location of script'_)

3. This will submit jobs that run split_loc_year.do

4. Check output, may need to troubleshoot output to make sure all locations ran properly (see _troubleshoot_ folder)

5. Open an R session on the cluster (_R_), and source(00_launch_savejobs_parallel.R), which will submit jobs to save each bundle of data

## Notes from GBD 2017 and prior
**Updated for [GBD2017] by Chad Ikeda**
Have split out the running and saving steps (would cause hang-ups, as there are manual troubleshoots that have been needed this round prior to upload; SEE TROUBLESHOOT FOLDER README)

Have updated with shared functions for this round. Jobs can sometimes fail, this requires a check of missing csv's and resubmitting those jobs (worse if cluster is slammed)

Wrote the upload script as a parallelized R save_results_cod call. As of 3/22/17, this uploads smoothly

###############################################
Notes from Brian Whetter (??) [GBD2016]

This code splits hemoglobinopathies based off of the parent model. To launch run the cod_split_hemog.ado which calls save.do and split_loc_year.do.

The code grabs vital registry data which is used to find CSMR and perform custom splits. Unfortanetely these splits can't be done using the shared function. 

Debugging Notes: 
I had trouble uploading all sexes at once so I split the calculations into male and female. This sometimes caused a database lock so I upload males first. This could have been a sloppy way of doing things. 

Another error that occured was having insufficient vital registration data to perform splits. When ran in GBD 2016 the only VR data we had was for Iceland. If your uploads are failing from missing data check to make sure the 618_csmr.dta file in the tmp folder only has missing data for age_group 27. For this reason, we copied over 2015 for the GBD 2016 round.

The last type of common error is not grabbing data when using get_draws in the split_loc_year.do. If you aren't grabbing 46 rows (age_groups*sexes) for each year-locations.






