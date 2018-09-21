
/* **************************************************************************
NEONATAL REGRESSIONS

******************************************************************************/

/* ***************************************************************************
PART I: Data prep

Here, for each of the five causes of interest (encephalopathy, sepsis, and the three 
preterms) we import the data, do some cleaning/checks, import covariates,
and otherwise get the dataset ready for analysis.
*****************************************************************************/
clear all
set more off
set maxvar 32000
version 13

/*  //////////////////////////////////////////////
		WORKING DIRECTORY
////////////////////////////////////////////// */
	

//root dir
if c(os) == "Unix" {
	local j // FILEPATH
	local working_dir = // FILEPATH
} 
else if c(os) == "Windows" {
	local j // FILEPATH
	local working_dir = // FILEPATH
}

// Create timestamp for logs
local c_date = c(current_date)
local c_time = c(current_time)
local c_time_date = "`c_date'"+"_" +"`c_time'"
display "`c_time_date'"
local time_string = subinstr("`c_time_date'", ":", "_", .)
local timestamp = subinstr("`time_string'", " ", "_", .)
display "`timestamp'"

// set directories
	
local data_dir = // FILEPATH 
local parent_source_dir = // FILEPATH 
local cov_dir = // FILEPATH 

adopath + // FILEPATH 

run // FILEPATH 
run // FILEPATH 

/*  //////////////////////////////////////////////

		COVARIATES
		The covariates used are the following: 
		--ln_NMR: each country-year's NMR, in log space
		
		RANDOM EFFECTS 
		Regressions have superregion random effects, except
		long_mild_ga2, which has only global. 

////////////////////////////////////////////// */

// NMR

use /* FILEPATH */, clear 
keep region_name location_name ihme_loc_id sex year q_nn_med

// the value we want is q_nn_med, but it is in a per-thousand format right now;
// we multiply to get it to scale

gen NMR = q_nn_med * 1000
gen ln_NMR = log(NMR)

rename sex gender
gen sex = 1 if gender == "male"
replace sex = 2 if gender == "female" 
replace sex = 3 if gender == "both"

//The mortality data explicitly estimates year at midyear; we rename this to 
// simply 'year', keeping in mind that the value is a mid-year estimate

replace year = year - 0.5
drop gender q_nn_med
tempfile neo
save `neo', replace

// convert ihme_loc_ids to location_ids

get_location_metadata, location_set_id(9) gbd_round_id(4) clear //draw location metadata for new year
tostring location_id, replace
replace ihme_loc_id = "GBR_" + location_id if location_type == "health_districts" | local_id == "WLS" | local_id == "NIR" | local_id == "SCT"
destring location_id, replace

destring developed, replace
replace developed = 1 if parent_id == 4749 //England subnationals
replace developed = 0 if developed == . 
tempfile country_code_data
save `country_code_data', replace

// create a dataset that has developed/developing country information
preserve
keep location_id developed
duplicates drop 
tempfile developing 
save `developing', replace
restore 

//create an empty dataset that we will fill with our predictions
keep location_id location_name location_type super_region_id super_region_name region_id region_name ihme_loc_id
expand 67 // we need each current record to be expanded to 67 records, one for every year 1950-2015
bysort location_id: gen year = 1949 + _n 
tempfile template
save `template', replace


merge 1:m ihme_loc_id year using `neo' // `neo' is split by sex: males, females and both
drop if _merge == 2 //_m==2 denotes locations modeled for mortality that we don't model for epi - national-level for countries where we have subnats (e.g., Japan, Saudi, US, UK), India state-level (w/o urban/rural)
drop _merge

merge m:1 location_id using `developing'
drop _merge

gen year_dev_int = year*developed

tempfile covariate_template
save `covariate_template', replace
	
/*  //////////////////////////////////////////////
		DATA PREP
////////////////////////////////////////////// */	

//To start: we have created our own rendition of the 'dimensions' spreadsheet, 
// that has information on the names of each sequela, what modeling method to use 
// with it, and any relevant covariates/random effects.  We bring this in, and 
// loop through it. 

import delimited /* FILEPATH */, clear 

//the file contains information on other neonatal conditions that we 
// don't use these methods on retinopathy or kernicterus
drop if grouping == "retino" | grouping == "kernicterus"
tempfile small_dimensions
save `small_dimensions', replace

// Keep enceph, preterm and sepsis

local acause_list " "neonatal_enceph" "neonatal_sepsis" "neonatal_preterm" "

//here begins the acause looping
foreach acause of local acause_list { 

	// first, delete pre-existing files 
	// so we will be able to check at the
	// end that this step finished
	di "Removing old files"
	cd /* FILEPATH */
	local files: dir . files "`acause'*"
	foreach file of local files {
		erase `file'
	}

	di in red "`acause'"
	
	local out_dir /* FILEPATH */
	local archive_dir /* FILEPATH */
	
	capture mkdir /* FILEPATH */
	capture mkdir /* FILEPATH */
	
	use `small_dimensions', clear
	
	keep if acause=="`acause'"
	levelsof(gest_age), local(gest_age_list)
	
	tempfile local_dimensions
	save `local_dimensions', replace
	
	// enceph and sepsis don't have gestational age splits, but we want to loop 
	// through every gestational age (for the sake of preterm), so we temporarily generate 
	// a gestational age 
	if "`acause'"=="neonatal_enceph" | "`acause'" == "neonatal_sepsis" {
		local gest_age_list "none"
	}

	
	levelsof modelable_entity_id, local(modelable_entity_list)  
	levelsof bundle_id, local(bundle_id_list)

	local source_dir /* FILEPATH */
	
	//here begins the gestational age loop
	foreach gest_age of local gest_age_list{
	
		use `local_dimensions', clear
		di in red "gest age is `gest_age' for acause `acause'"
		
		//reverting gest_age back to nothing for encephalopathy
		if "`gest_age'"=="none"{
			local gest_age ""
		}
		
		tostring gest_age, replace
		replace gest_age="" if gest_age=="."
		
		//bring in data points we will use in our regression.

		di in red "importing data"

		local x = 0

		foreach bundle_id of local bundle_id_list{
			
			di in red "bundle_id is `bundle_id'"
			get_epi_data, bundle_id(`bundle_id') clear

			capture confirm variable modelable_entity_id
			if !_rc {
                       di in red "modelable_entity_id already exists as column in dataset. We drop this for now."
                       drop modelable_entity_id

               }
               else {
                       di in red "modelable_entity does not exist as column in dataset, create the column"
                       
               }

            gen modelable_entity_id = 0

            //  This code here creates a new column for bundle_id
            preserve
            use `local_dimensions', clear
            keep if bundle_id == `bundle_id'
            local current_me = modelable_entity[1]
            restore

            replace modelable_entity_id = `current_me'

			di "Counting observations"
			count
			if `r(N)' == 0 {
				di "Obs = 0. No data for me_id `bundle_id'"
			}
			else if `x' == 0 {
				di "saving original file"
				tempfile `acause'_`gest_age'_data
				save ``acause'_`gest_age'_data', replace
			}
			else if `x' == 1 {
				di "appending subsequent files"
				append using ``acause'_`gest_age'_data', force 
				di "saving subsequent files"
				save ``acause'_`gest_age'_data', replace
			}
		local x = 1
		}

			// create 'grouping' variable 
			gen grouping = ""

			// neonatal_preterm
			replace grouping = "ga1" if modelable_entity_id == 1557
			replace grouping = "ga2" if modelable_entity_id == 1558
			replace grouping = "ga3" if modelable_entity_id == 1559
			drop if grouping == "ga1" | grouping == "ga2" | grouping == "ga3" & measure == "mtexcess"
			replace grouping = "long_mild_ga1" if modelable_entity_id == 1560
			replace grouping = "long_mild_ga2" if modelable_entity_id == 1561
			replace grouping = "long_mild_ga3" if modelable_entity_id == 1562
			replace grouping = "long_modsev_ga1" if modelable_entity_id == 1565
			replace grouping = "long_modsev_ga2" if modelable_entity_id == 1566
			replace grouping = "long_modsev_ga3" if modelable_entity_id == 1567
			replace grouping = "cfr1" if modelable_entity_id == 2571
			replace grouping = "cfr2" if modelable_entity_id == 2572
			replace grouping = "cfr3" if modelable_entity_id == 2573

			// neonatal_enceph
			replace grouping = "cases" if modelable_entity_id == 2525
			drop if grouping == "cases" & measure == "mtexcess" // this is the EMR data that must be included for Dismod Step 1
			replace grouping = "cfr" if modelable_entity_id == 2524
			replace grouping = "long_mild" if modelable_entity_id == 1581
			replace grouping = "long_modsev" if modelable_entity_id == 1584

			// neonatal_sepsis
			replace grouping = "cases" if modelable_entity_id == 1594
			drop if grouping == "cases" & measure == "mtexcess" // this is the EMR data that must be included for Dismod Step 1
			replace grouping = "cfr" if modelable_entity_id == 3964
			replace grouping = "long_mild" if modelable_entity_id == 3965
			replace grouping = "long_modsev" if modelable_entity_id == 3966

			// change sex var to numeric
			replace sex = "1" if sex == "Male"
			replace sex = "2" if sex == "Female"
			replace sex = "3" if sex == "Both"
			destring sex, replace

		// naming conventions have changed, fix grouping names
		replace grouping="cases" if grouping == "bprev"
		keep if regexm(grouping, "`gest_age'") == 1
		
		// make sure there are no doubled files
		drop seq
		drop if is_outlier == 1
		duplicates drop
		
		drop if cases > sample_size

		// make sure you have some kind of value for numerator and denominator
		
		replace sample_size = (mean*(1-mean))/(standard_error)^2 if sample_size == .
		replace cases = mean * sample_size if cases == .

		count if mean==.
		di in red "acause `acause' at gestational age `gest_age' has `r(N)' missing mean values!"
		drop if mean== .
		
		// get proper iso3 names
		merge m:1 location_id using `developing', keep(1 3) nogen
		drop if location_id == .
		
		//merge the dimensions sheet back on so we have the name/covariate information again
		di in red "merging dimensions onto data"
		merge m:1 grouping using `local_dimensions', keep(3) nogen
		levelsof standard_grouping, local(standard_grouping_list)
		
		keep acause *grouping location_* sex year* mean sample_size cases covariates random_effect_levels

		count if mean > 1
		if `r(N)' > 0{
			di in red "too-large parameter estimate!"  
			BREAK
		}

		// Place year at midyear
		gen year = floor((year_start+year_end)/2)
		drop year_*
		
		replace sex = 3 if standard_grouping != "birth_prev"
		collapse(sum) cases sample_size, by(acause *grouping location_* sex year covariates random_effect_levels)
		gen mean = cases/sample_size
		
		tempfile dataset
		save `dataset'
		
		/*  //////////////////////////////////////////////
		Parameter-specific analysis
		Now we have a prepped dataset.  All that
		remains is to merge this dataset with a template
		(such that we can make predictions even for years 
		where we have missing data), merge on covariates,
		and call the code for the next step.
		////////////////////////////////////////////// */
		
		// here begins the parameter-specific loop

		 foreach standard_grouping of local standard_grouping_list {
			di in red "saving template for `standard_grouping' of `acause'"
			use `dataset', clear
			
			keep if standard_grouping == "`standard_grouping'" 
			
			local grouping = grouping
			local covariates = covariates
			local random_effects = random_effect_levels
			
			// Pass the regression script a local that 
			// contains the names of our covariates and random effects.

			local cov_count: word count `covariates'
			local re_count: word count `random_effects'
			
			if `cov_count' > 1{
				local covs_to_pass: word 1 of `covariates'
				forvalues i = 2 / `cov_count'{
					local to_append: word `i' of `covariates'
					local covs_to_pass "`covs_to_pass'__`to_append'"
				}
				di in red "`covs_to_pass'"
				local covariates = "`covs_to_pass'"
			}
			
			if `re_count'>1{
				local random_effects_to_pass: word 1 of `random_effects'
				forvalues i = 2 / `re_count'{
					local to_append: word `i' of `random_effects'
					local random_effects_to_pass "`random_effects_to_pass'__`to_append'"
				}
				di in red "`random_effects_to_pass'"
				local random_effects = "`random_effects_to_pass'"
			}

			if "`random_effects'" == "global_level"{
				gen global_level = 1
			}
			drop covariates random_effect_levels *grouping acause 
			
			di in red "official grouping name is `grouping'"
			merge 1:1 location_id year sex using `covariate_template'
			
			if "`standard_grouping'" == "birth_prev"{
				drop if sex == 3
			}
			else{
				drop if sex != 3
			}
			
			// save so the next script can find it
			local fname /* FILEPATH */
			local prepped_dta_dir /* FILEPATH */
			save /* FILEPATH */, replace
			export delimited using /* FILEPATH */, replace
			
			// archive
			save /* FILEPATH */, replace
			export delimited using /* FILEPATH */,  replace
			
			// if the covariates list is empty, that means we should run a meta-analysis. 
			// if the covariates list is nonempty, that means we should run a hierarchichal
			// mixed-effects model.

			if "`covariates'" == "meta"{
				
				/* QSUB for next script */

			}
			
			else if "`covariates'" == "ln_NMR"{
				
				/* QSUB for next script */
			}

			else if "`covariates'" == "severity_regression" {
				
				/* QSUB for next script */  
			}
			
			di in red "analysis submitted for `grouping' of `acause'!"
		
		}
		
	
	}
	

}

// wait until meta-analysis and regression files are finished 
foreach acause of local acause_list {

	use `small_dimensions', clear
	keep if acause == "`acause'" 
	drop if regex(grouping, "^ga")
	drop if covariates == ""
	levelsof grouping, local(grouping_list)

	foreach grouping of local grouping_list {
		capture noisily confirm file "`data_dir'/02_analysis/`acause'/draws/`acause'_`grouping'_draws.csv"
		while _rc!=0 {
			di "File `acause'_`grouping'_draws.csv not found :["
			sleep 60000
			capture noisily confirm file "`data_dir'/02_analysis/`acause'/draws/`acause'_`grouping'_draws.csv"
		}
		if _rc == 0 {
			di "File `acause'_`grouping'_draws.csv found!"
		}
		
	}

}



// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

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
					local dir: dir "`root_j_dir'/03_steps/`date'" dirs "`i'_*", respectcase
					local dir = subinstr(`"`dir'"',`"""',"",.)
					cap confirm file "`root_j_dir'/03_steps/`date'/`dir'/finished.txt"
					if _rc local write_file 0
				}
			}
			
			// write file if all steps finished
			if `write_file' {
				file open all_finished using "`root_j_dir'/03_steps/`date'/finished.txt", replace write
				file close all_finished
			}
		}
		
	// close log if open
		//if `close_log' log close
	
