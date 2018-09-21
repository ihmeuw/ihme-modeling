// USERNAME
// Code does three things:
// 1) Runs the "save_results" shared function for the compensated cirrhosis (ME=11654) model
// 2) Splits parent according to the four proportion models
// 3) Runs the "save_results" shared function for the four proportion models

// The description should be updated for each run

// Enter the following in the cluster to submit this
/*
qsub -N "compensated" -pe multi_slot 15 -P "proj_custom_models" -e "FILEPATH" -o "FILEPATH" "FILEPATH/stata_shell.sh" "FILEPATH/cirrhosis_etiology_splits_02_compensated_save_parent_&_split_&_save_children.do" 
*/

// Set locals
	clear all
	local folder_date "YYYY-MM-DD"  // where asymptomatic draws are saved, e.g., "2017-05-31"
	local v_total =  142007
	local v_decomp = 155060
	local v_prop_hepb = 141662 // final for 2016 == 141662
	local v_prop_hepc = 141683 // final for 2016 == 141683
	local v_prop_etoh = 141641 // final for 2016 == 141641
	local v_prop_other = 141686 // final for 2016 == 141686

/////////////////////////////////////////////////////
// Step 1: Save Compensated Cirrhosis Parent Results (ME=11654)
/////////////////////////////////////////////////////
	clear all

	run "FILEPATH/save_results.do"

	save_results, modelable_entity_id("11654") gbd_round(2016) mark_best("yes") env("prod") file_pattern("{measure_id}_{location_id}.csv") description("Compensated cirrhosis: total cirrhosis (ME=11653, v=`v_total') - decompensated cirrhosis (ME=1919, v=`v_decomp')") in_dir("FILEPATH")

//////////////////////////////////////////////////////////////////////
// Step 2: Split Parent (ME 11654) according to the proportion models
//////////////////////////////////////////////////////////////////////
// Order for both proportion models and attribution models is: hep b, hep c, alcohol, other
	clear all

	run "FILEPATH/split_epi_model.ado"

	split_epi_model, source_meid(11654) target_meids(11655 11656 11657 11658) prop_meids(1920 1921 1922 1923) output_dir("FILEPATH") gbd_round_id(4) clear


/////////////////////////////////////////////////////////
// Step 3: Save Attributed Compensated Cirrhosis Results 
/////////////////////////////////////////////////////////

// Cirrhosis due to Hep B
	clear all

	run "FILEPATH/save_results.do"

	save_results, modelable_entity_id("11655") gbd_round(2016) mark_best("yes") env("prod") file_pattern("{location_id}.h5") h5_tablename("draws") description("Compensated cirrhosis due to Hep B: Total version `v_total', decompensated version `v_decomp', proportion model #`v_prop_hepb'") in_dir("FILEPATH")

// Cirrhosis due to Hep C
	clear all

	run "FILEPATH/save_results.do"

	save_results, modelable_entity_id("11656") gbd_round(2016) mark_best("yes") env("prod") file_pattern("{location_id}.h5") h5_tablename("draws") description("Compensated cirrhosis due to Hep C: Total version `v_total', decompensated version `v_decomp', proportion model #`v_prop_hepc'") in_dir("FILEPATH")

// Cirrhosis due to Alcohol
	clear all

	run "FILEPATH/save_results.do"

	save_results, modelable_entity_id("11657") gbd_round(2016) mark_best("yes") env("prod") file_pattern("{location_id}.h5") h5_tablename("draws") description("Compensated cirrhosis due to EtOH: Total version `v_total', decompensated version `v_decomp', proportion model #`v_prop_etoh'") in_dir("FILEPATH")

// Cirrhosis due to Other
	clear all

	run "FILEPATH/save_results.do"

	save_results, modelable_entity_id("11658") gbd_round(2016) mark_best("yes") env("prod") file_pattern("{location_id}.h5") h5_tablename("draws") description("Compensated cirrhosis due to Other: Total version `v_total', decompensated version `v_decomp', proportion model #`v_prop_other'") in_dir("/share/scratch/users/USER/cirrhosis/11658")
	

* the end
