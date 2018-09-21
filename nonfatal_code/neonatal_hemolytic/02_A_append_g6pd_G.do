******************************************************************************************************
** NEONATAL HEMOLYTIC MODELING
** PART 2: G6PD
** Part A: Prevalence of G6PD
** 6.9.14

** We get G6PD prevalence from the GBD Dismod models of congenital G6PD (modeled in DisMod)  
 
*****************************************************************************************************


clear all
set more off
set graphics off
set maxvar 32000


/*  //////////////////////////////////////////////
		WORKING DIRECTORY
////////////////////////////////////////////// */ 

// discover root
	if c(os) == "Windows" {
		local j /*FILEPATH*/
		// Load the PDF appending application
		quietly do /*FILEPATH*/
	}
	if c(os) == "Unix" {
		local j /*FILEPATH*/
		ssc install estout, replace 
		ssc install metan, replace
	} 
	

// locals
local me_id 2112 // "G6PD deficiency"

// functions
adopath + /*FILEPATH*/
quietly do /*FILEPATH*/

// directories 	
	local working_dir = /*FILEPATH*/

	local log_dir = /*FILEPATH*/
	local out_dir "`working_dir'"
	capture mkdir "`out_dir'"	
	local plot_dir "`out_dir'/time_series"
	capture mkdir "`plot_dir'"
	
// Create timestamp for logs
    local c_date = c(current_date)
    local c_time = c(current_time)
    local c_time_date = "`c_date'"+"_" +"`c_time'"
    display "`c_time_date'"
    local time_string = subinstr("`c_time_date'", ":", "_", .)
    local timestamp = subinstr("`time_string'", " ", "_", .)
    display "`timestamp'"



************************************************************************************

// get draws from best congenital G6PD model 
get_draws, gbd_id_field(modelable_entity_id) gbd_id(`me_id') source(epi) measure_ids(5) location_ids() year_ids() age_group_ids(2) sex_ids() status(best) clear
tempfile g6pd_data
save `g6pd_data'

// store best model version 
local best_model_version = model_version_id[1]

// format
keep location_id sex_id year_id draw*
rename sex_id sex 
rename year_id year

// export 
export delimited /*FILEPATH*/, replace
