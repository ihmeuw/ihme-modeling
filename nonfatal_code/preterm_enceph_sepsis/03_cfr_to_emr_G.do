
// priming the working environment
clear all
set more off
set maxvar 30000
version 13.0


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
	di in red "J drive is `j'"

// functions

run /*FILEPATH*/

** directories
local data_dir /*FILEPATH*/


** locals
local acause_list " "neonatal_preterm" "neonatal_enceph" "neonatal_sepsis" "


************************************************************************

foreach acause of local acause_list {

	// set gestational ages. Assign null value for neonatal_enceph because there are none. 
	di "CFR to EMR transformation for `acause'"

	if "`acause'" == "neonatal_preterm" {
		local me_ids 8676 2572 2571 2573 
	}
	if "`acause'" == "neonatal_enceph" {
		local me_ids 2524
	}
	if "`acause'" == "neonatal_sepsis" {
		local me_ids 3964
	}

	// begin gestational age loop
	di "begin me loop"
	foreach me_id of local me_ids {

		di "Me_id is `me_id'"

		// create local that will hold target me_id (ie, corresponding birth prevalence)
		if `me_id' == 2571 {
			local new_me_id = 1557
			local new_bundle_id = 80
			local bundle_id = 351
		}
		if `me_id' == 2572 {
			local new_me_id = 1558
			local new_bundle_id = 81			
			local bundle_id = 352
		}
		if `me_id' == 2573 {
			local new_me_id = 1559
			local new_bundle_id = 82
			local bundle_id = 353
		}
		if `me_id' == 8676 {			
			local new_me_id = 8675
			local new_bundle_id = 500
			local bundle_id = 501
		}
		if `me_id' == 2524 {
			local new_me_id = 2525
			local new_bundle_id = 338
			local bundle_id = 337

		}
		if `me_id' == 3964 {
			local new_me_id = 1594
			local new_bundle_id = 92
			local bundle_id = 460
		}

		di in red "`me_id'"
		di in red "`new_me_id'"
		di in red "`new_bundle_id'"
		di in red "`bundle_id'"

		// import data 
		di "retrieving most recent data"
  		get_epi_data, bundle_id(`bundle_id') clear

		di "arithmetic"
		destring mean, replace
		replace mean = -ln(1-mean)/(28/365.25)
		
		// can't calculate an EMR from a cfr of 1 (would be infinite) so drop these rows
		di "dropping cfr=1/infinite EMRs"
		drop if mean == . 

		// format a bit
		replace measure = "mtexcess" 
		replace age_start = 0
		replace age_end = 28/365 // because our definition of cfr is death w/i the first 28 days of life 
		replace lower = .
		replace upper = .
		replace uncertainty_type_value = .
		replace standard_error = . 
		capture confirm variable note_modeler
		if !_rc {
                       di in red "note_modeler already exists"
               }
               else {
                       di in red "note_modeler does not exist, create the column"
                       gen note_modeler = .
               }
		tostring note_modeler, replace 
		replace note_modeler = "Transformed from raw cfr data by -ln(1-mean)/(28/365.25)"
		replace bundle_id = `new_bundle_id'
		replace seq = .
		replace underlying_nid = .
		replace sampling_type = .
		replace recall_type_value = .
		replace design_effect = .
		gen response_rate = .
		capture confirm variable modelable_entity_id
		if !_rc {
                       drop modelable_entity_id
               }
               else {
                       di in red "no modelable_entity_id in dataset"
                       
               }
		capture confirm variable modelable_entity_name
		if !_rc {
                       drop modelable_entity_name
               }
               else {
                       di in red "no modelable_entity_name in dataset"
                       
               }



		// format modelable_entity info
		//drop modelable_entity_name 
		//replace modelable_entity_id = `new_me_id'
		//merge m:1 modelable_entity_id using `me_metadata', nogen keep(3)
		//drop *date* // drops all the problematic additional vars
		
		// save
		di "saving"
		export excel /*FILEPATH*/, firstrow(variables) sheet("extraction") replace 
	}

}
