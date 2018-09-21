/* **************************************************************************
NEONATAL REGRESSIONS: META-ANALYSIS
Amelia Bertozzi-Villa
8.10.14
Adapted for GBD 2015 by Stephanie
Last Edited: (15 June 2016) Updated documentation. */

clear all
set more off
set maxvar 32000
ssc install estout, replace 
ssc install metan, replace




/*  //////////////////////////////////////////////
		WORKING DIRECTORY
////////////////////////////////////////////// */

		//root dir
	if c(os) == "Windows" {
		local j /*FILEPATH*/
		// Load the PDF appending application
		quietly do /*FILEPATH*/
	}
	if c(os) == "Unix" {
		local j /*FILEPATH*/
		
	} 
	

local acause "`1'"
local grouping "`2'"
local parent_dir "`3'"
local in_dir "`4'"
local timestamp "`5'" 


di in red "acause is `acause'"
di in red "grouping is `grouping'"
di in red "parent_dir is `parent_dir'"
di in red "in_dir is `in_dir'"
di in red "timestamp is `timestamp'"

local out_dir /*FILEPATH*/
capture mkdir /*FILEPATH*/

/* /////////////////////////
/// Import data prepared in 
/// step 01_dataprep
///////////////////////// */

di in red "importing data"
use "`in_dir'", clear

rename mean data_val

//make sure every study has a mean, lower, and upper in the proper domain [0,1]
keep location_id year location_name super_region_id data_val cases sample_size
gen data_val_se = (data_val * (1-data_val)/sample_size)^0.5
gen data_val_lower = data_val - 1.96*data_val_se
gen data_val_upper = data_val + 1.96*data_val_se
replace data_val_lower=0 if data_val_lower<0
replace data_val_upper=1 if (data_val_upper>1 & data_val_upper!=.)

/* /////////////////////////////////////////////////////////////////////
/// Run meta-analysis,transform outputs from locals to actual variables 
/// in the dataset
///////////////////////////////////////////////////////////////////// */

di in red "performing meta-analysis"
metan data_val data_val_lower data_val_upper, random

gen mean = r(ES)
gen lower = r(ci_low)
gen upper = r(ci_upp)
gen se = (mean - lower)/1.96

//this is all done with sex==3.  Expand to both sexes.
expand 2, gen(iscopy)
gen sex=2
replace sex=1 if iscopy==1 

/* /////////////////////////
///Save these summary stats.
/// Also take a thousand draws
/// and save those as well.
///////////////////////// */


//summary stats
preserve
keep location_id year sex location_name super_region_id mean lower upper data_val 
di in red "saving summary stats!"
local summ_out_dir /*FILEPATH*/
local summ_archive_dir /*FILEPATH*/
capture mkdir /*FILEPATH*/
capture mkdir /*FILEPATH*/
local summ_fname /*FILEPATH*/

save /*FILEPATH*/, replace
export delimited using /*FILEPATH*/, replace
export delimited using /*FILEPATH*/, replace
restore

keep location_id year sex mean se 

//draws
di in red "generating draws"
forvalues i=1/1000{
	gen draw_`i' = rnormal(mean, se)
}


di in red "saving all draws"
drop mean se
local draw_out_dir = /*FILEPATH*/
local archive_dir = /*FILEPATH*/
capture mkdir /*FILEPATH*/
capture mkdir /*FILEPATH*/
local fname /*FILEPATH*/

save /*FILEPATH*/, replace
export delimited using /*FILEPATH*/, replace
export delimited using /*FILEPATH*/, replace
	

	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************


	// write check file to indicate step has finished
		file open finished using "`out_dir'/finished.txt", replace write
		file close finished
		
	// if step is last step, write finished.txt file
		local i_last_step 0
		foreach i of local last_steps {
			if "`i'" == "`this_step'" local i_last_step 1
		}
		
		// only write this file if this is one of the last steps
		if `i_last_step' {
		
			// account for the fact that last steps may be parallel and don't want to write file before all steps are done
			local num_last_steps = wordcount("`last_steps'")
			
			// if only one last step
			local write_file 1
			
			// if parallel last steps
			if `num_last_steps' > 1 {
				foreach i of local last_steps {
					local dir: dir /*FILEPATH*/ dirs "`i'_*", respectcase
					local dir = subinstr(`"`dir'"',`"""',"",.)
					cap confirm file /*FILEPATH*/
					if _rc local write_file 0
				}
			}
			
			// write file if all steps finished
			if `write_file' {
				file open all_finished using /*FILEPATH*/, replace write
				file close all_finished
			}
		}
	
	
	
	
	
	
	
	
	
	
	
	

