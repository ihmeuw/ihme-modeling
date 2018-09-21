## Notes on code files for U5M Africa mapping:

* This is a code snapshot from published version of maps in _The Lancet_, output data available at http://ghdx.healthdata.org/
* Custom functions reside in ./utils/all_functions.R
* ./model/1_run_mbg_died.R is the main master script that analysis get run off
* ./model/mbg_bybin.R is the main modelling script that gets qsubbed from run_mbg_died and run in parallel
* Internal IHME filepaths, usernames, and passwords have been removed.
* We are working on a clean package for IHME-MBG, please be patient
* Contact royburst@uw.edu with questions.
