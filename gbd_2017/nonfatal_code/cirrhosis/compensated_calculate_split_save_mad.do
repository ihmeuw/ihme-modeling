// AUTHOR
// Code does three things:
// 1) Runs the "save_results" shared function for the compensated cirrhosis (ME=11654) model
// 2) Splits parent according to the five proportion models
// 3) Runs the "save_results" shared function for the five proportion models

// The description should be updated for each run

// Enter the following in the cluster to submit this


// Set locals
	clear all
	local v_total =  999
	local v_decomp = 999
	local v_prop_hepb = 999 
	local v_prop_hepc = 999 
	local v_prop_etoh = 999 
	local v_prop_other = 999
	local v_prop_nash = 999
	
	local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
	local date = subinstr(`"`date'"'," ","_",.)
	local date "2018_07_13"
	
	local outdir "FILEPATH"
	cap mkdir `outdir'


/////////////////////////////////////////////////////
// Step 1: Save Compensated Cirrhosis Parent Results (ME=11654)
/////////////////////////////////////////////////////
	clear all

	run "FILEPATH"

	save_results_epi, modelable_entity_id("999") mark_best(True) input_file_pattern("{location_id}.csv") measure_id("5 6") ///
	description("Compensated cirrhosis: total cirrhosis (ME=11653, v=`v_total') - decompensated cirrhosis (ME=1919, v=`v_decomp')") ///
	input_dir("FILEPATH")

	//save_results_epi, modelable_entity_id("11654") mark_best(True) input_file_pattern("{measure_id}_{location_id}.csv") measure_id("5") description("Compensated cirrhosis: total cirrhosis (ME=11653, v=`v_total') - decompensated cirrhosis (ME=1919, v=`v_decomp')") input_dir("`outdir'/01_draws/asymptomatic")

//////////////////////////////////////////////////////////////////////
// Step 2: Split Parent (ME 11654) according to the proportion models
//////////////////////////////////////////////////////////////////////
// Order for both proportion models and attribution models is: hep b, hep c, alcohol, other
	clear all

	run "FILEPATH"

	split_epi_model, source_meid(999) target_meids(999) prop_meids(999) output_dir(`outdir') split_meas_ids(5 6)  clear

/////////////////////////////////////////////////////////
// Step 3: Save Attributed Compensated Cirrhosis Results 
/////////////////////////////////////////////////////////

// Cirrhosis due to Hep B
	clear all
	
	run "FILEPATH"

	save_results_epi, modelable_entity_id("999") mark_best(False) input_file_pattern("{location_id}.h5") ///
	description("Compensated cirrhosis due to Hep B: Total version `v_total', decompensated version `v_decomp', proportion model #`v_prop_hepb' GBD2017 FINAL") ///
	input_dir("FILEPATH") measure_id("5 6")

// Cirrhosis due to Hep C
	clear all

	run "FILEPATH"

	save_results_epi, modelable_entity_id("999") mark_best(False) input_file_pattern("{location_id}.h5") ///
	description("Compensated cirrhosis due to Hep C: Total version `v_total', decompensated version `v_decomp', proportion model #`v_prop_hepc' GBD2017 FINAL") ///
	input_dir("FILEPATH") measure_id("5 6")

// Cirrhosis due to Alcohol
	clear all

	run "FILEPATH"

	save_results_epi, modelable_entity_id("999") mark_best(False) input_file_pattern("{location_id}.h5") ///  
	description("Compensated cirrhosis due to EtOH: Total version `v_total', decompensated version `v_decomp', proportion model #`v_prop_etoh' GBD2017 FINAL") ///
	input_dir("FILEPATH") measure_id("5 6")

// Cirrhosis due to Other
	clear all

	run "FILEPATH"

	save_results_epi, modelable_entity_id("999") mark_best(False) input_file_pattern("{location_id}.h5") ///
	description("Compensated cirrhosis due to Other: Total version `v_total', decompensated version `v_decomp', proportion model #`v_prop_other' GBD2017 FINAL") ///
	input_dir("FILEPATH") measure_id("5 6")

//  Cirrhosis due to cirrhosis_nash	
	clear all

	run "FILEPATH"

	save_results_epi, modelable_entity_id("999") mark_best(False) input_file_pattern("{location_id}.h5") ///
	description("Compensated cirrhosis due to NASH: Total version `v_total', decompensated version `v_decomp', proportion model #`v_prop_nash' GBD2017 FINAL") ///
	input_dir("FILEPATH") measure_id("5 6")

* the end
