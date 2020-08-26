# Live Birth Adjustment Code

The purpose of this code is to calculate live-birth adjustments for maternal 
causes using age-specific fertility ratio (ASFR). More detailed information can 
be read in the detailed_modeling.docx on the OneDrive.

### Run Steps:
	1) Login to fair cluster (putty)
		a. Host: FILEPATH
	2) Submit a qlogin for fair cluster archive node
		a. 'qlogin -l archive -l fthread=1 -l m_mem_free=1G -P ihme_general -now no -q all.q'
	3) Activate  the current gbd environment
		a. 'source FILEPATH'
	4) cd into your directory  holding the /live-birth-adjustments code folder
		a. cd ~/repos/live-birth-adjustments
	5) Execute python submit_jobs.py to launch jobs
	6) qstat and check errors/outputs
		a. watch -n 30 qstat
		b. use bitvise to visually explore the files in your scratch folders, open with text editor
			i. errors are usually the more informative ones


#### submit_jobs.py

This part of the code sets up directories to store draws retrieved from 
the epi database and executes the maternal_core.py, save.py, and 
upload_maternal_epi_bundles.py scripts.

Users will need to change the gbd_round_id as appropriate.

To execute the submit_jobs.py script, login to the new cluster, activate the 
gbd 2017_archive environment, navigate to the folder where the code is stored, 
and type 'python submit_jobs.py'

Jobs are submitted in parallel (by year). You can change the me_ids that are 
adjusted in the following ways:
* Modify the conditional statements in the submit_jobs.py script to look for 
specific me_ids and/or bundle_ids. The me_ids are listed in the dependency map 
csv that is read in at the beginning of the submit_jobs.py script. 
* Delete rows in the dependency map until only the rows you want adjusted 
remain. Remember to make a backup copy of the map when doing this so you
can revert to the backup when the code needs to be run again. 
Do not change the row(s) in the dependency map in any other way unless you 
are given specific instructions to do so. 
* Comment out blocks of code as needed.

The qsub calls are formatted for use with new cluster nodes only. All qsub
calls request 'archive' nodes that have access to the J drive. If jobs are 
running too slowly or stay in 'qw' status for unsual lengths of time, try
refactoring the code so that -l archive=TRUE is no longer neeeded, or 
switch to qsub calls that work with old cluster nodes.

#### maternal_core.py

Each main me_id group is organized into a class (such as Eptopic, Hemorrhage, 
etc.) which inherits methods from the "base" class. The base class defines 
methods that grab locations, ASFR, draws from the causes, etc. Each class then 
has the same structure of: 
1. Grabbing draws
2. Grabbing ASFR
3. Multiplying draws with ASFR
4. Converting incidence to prevalence by multiplying by a duration

Some me_ids (such as abortion) are saved to csv and uploaded using a 
centrally maintained shared function while others (such as eclampsia) output a 
sheet for the epi-uploader to run another DisMod model.

Users will need to change the gbd_round_id as appropriate.

The following methods in the maternal_core.py script require additional 
attention:

##### zero_locs(self, df)

The current modeling strategy for Fistula uses DisMod data for a subset of the 
GBD locations and sets the rest of the locations to zero. The areas for which 
DisMod data are used are most detailed locations in Sub-Saharan Africa (SSA); 
most detailed locations in South Asia (SA); and Afghanistan, Yemen, and 
Sudan. The zero_locs method filters Dismod data to the appropriate locations 
using a pre-prepared Stata file. The Stata file must be updated annually to 
accommodate any changes to the location hierarchy. Currently, it is the 
responsibility of the Fistula researcher to update the Stata file but the code
can be refactored to create the same reference file in submit_jobs.py.

##### replace\_with\_quantiles(self, numbers, lower_quantile, upper_quantile)

Severe pre-eclampsia is subtracted from hypertension and sometimes results in 
negative values for "other hypertenstion". Begining GBD 2016 and going 
forward, the solution for this problem was to set a floor for the lowest 
possible value at a certain percentile. The method "replace_wth_quantiles" 
is used to accomplish this goal.

The method uses a while loop to increment lower and upper quantiles by .05 
until negatives are completely removed. Final lower and upper
quantile values are exported to a csv with year_id in the file name. The 
csv is saved in the same time-stamped output folder as the rest of the data 
for the current run.

In order to retain as much information as possible, users will need to run the
code twice for hypertension. Once using an initial lower quantile value of 0.00 
and inital upper quantile value of 1.00 to obtain a quantile report for each 
year, and then again using the highest, lower quantile listed in the report 
files (and corresponding upper quantile) to ensure that the same floor is used 
accross all years. 

The quantile values are hard coded in the script since the amount we want to 
cap depends on the model version. If all years report the same quantile during 
the initial run, there is no need to execute the code again.

##### export_negatives(self, df)

This method is called from within replace\_with\_quantiles(). If negative
values exist in the hypertension data, the values are exported to an excel file 
for examination. The excel file is saved in the same time-stamped output folder 
as the rest of the data for the current run.

##### data_rich_data_poor(self, df)

The Eclampsia class creates input data for ME 2627 
"Long term sequelae of eclampsia" using the "Data dense and sparse" 
location hierarchy (location_set_id 43). If the location hierarchy hasn't 
been updated for the round yet, you might need to request a flat file from the 
COD team instead. The flat file will likely include country-level data only, 
and you will need to copy the data down to most detailed location levels 
or risk losing important information during merging.

#### save.py

Uploads transformed data to the epi database via centrally maintained shared 
function. Users will need to change the gbd_round_id as appropriate.

#### upload_maternal_epi_bundles.py

This script deletes old data from the given bundle_id and uploads new data to 
the same bundle_id. Files output from maternal_core.py are aggregated into a 
single dataframe prior to upload. The relevent researcher should be notified 
once the bundle uploads are complete. The research should then launch the 
corresponding Dismod models using the new, uploaded data. 

#### Notes:
For GBD 2017 we decided to change the age restriction on the fistula model 
from 10-54 to 10-95+. There was also some discussion regarding the zero'ing 
out of specific locations in the 1553 and 1554 me_ids. 

To create me_ids 1553 and 1554 without zero'd out locations, change the 
dependency map so that 1552 is the input_me for output_mes 1553;1554.

To create me_ids 1553 and 1554 with zero'd out locations, change the dependency 
map so that 16535 is the input_me for output_mes 1553;1554. This is the 
configuation currently in use. Note that 16535 is populated with data 
regardless of whether or not it used further in the process.

Items in this repository were copied from [Brian's repo](https://stash.ihme.washington.edu/projects/MNCH/repos/brian/browse/nonfatal/live_birth_adjustments). 
Revision histories for the two repositories are unfortunately not connected.