// AUTHOR
// Code does two things:
// 1) Splits parent according to the five proportion models
// 2) Runs the "save_results" shared function for the five proportion models
// The description should be updated for each run

// Enter the following in the cluster to submit this

// Set locals
	clear all
	local v_parent = 999
	local v_decomp = 999
	local v_prop_hepb = 999 // final for 2016 == 999
	local v_prop_hepc = 999 // final for 2016 == 999
	local v_prop_etoh = 999 // final for 2016 == 999
	local v_prop_other = 999 // final for 2016 == 999
	local v_prop_nash = 999

//	create date - must be on same date you ran parent decompensated R file 
	local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
	local date = subinstr(`"`date'"'," ","_",.)
	local date "2018_07_13"

//////////////////////////////////////////////////////////////////////
// Step 1: Split Parent (ME 1919) according to the proportion models
//////////////////////////////////////////////////////////////////////
// Order for both proportion models and attribution models is: hep b, hep c, alcohol, other, NASH
	//clear all

	run "FILEPATH"

	split_epi_model, source_meid(999) target_meids(999) prop_meids(999) output_dir("FILEPATH") split_meas_ids(5 6) clear
	

/////////////////////////////////////////////////////////
// Step 3: Save Attributed Compensated Cirrhosis Results 
/////////////////////////////////////////////////////////

// Cirrhosis due to Hep B
	clear all

	run "FILEPATH"
	save_results_epi, modelable_entity_id("999") mark_best(False) input_file_pattern("{location_id}.h5") description("Cirrhosis due to Hep B - Parent version `v_decomp' using proportion version `v_prop_hepb' GBD2017 FINAL") input_dir("FILEPATH")

// Cirrhosis due to Hep C
	clear all
	
	run "FILEPATH"
	save_results_epi, modelable_entity_id("999") mark_best(False) input_file_pattern("{location_id}.h5") description("Cirrhosis due to Hep C - Parent version `v_decomp' using proportion version `v_prop_etoh' GBD2017 FINAL") input_dir("FILEPATH")

// Cirrhosis due to Alcohol
	clear all

	run "FILEPATH"
	save_results_epi, modelable_entity_id("999") mark_best(False) input_file_pattern("{location_id}.h5") description("Cirrhosis due to Alcohol - Parent version `v_decomp' using proportion version `v_prop_hepb' GBD2017 FINAL") input_dir("FILEPATH")

// Cirrhosi due to Other
	clear all

	run "FILEPATH"
	save_results_epi, modelable_entity_id("999") mark_best(False) input_file_pattern("{location_id}.h5") description("Cirrhosis due to Other - Parent version `v_decomp' using proportion version `v_prop_other' GBD2017 FINAL") input_dir("FILEPATH")

// Cirrhosi due to NASH
	clear all

	run "FILEPATH"
	save_results_epi, modelable_entity_id("999") mark_best(False) input_file_pattern("{location_id}.h5") description("Cirrhosis due to NASH - Parent version `v_deomp' using proportion version `v_prop_nash' GBD2017 FINAL") input_dir("FILEPATH")	

* the end
