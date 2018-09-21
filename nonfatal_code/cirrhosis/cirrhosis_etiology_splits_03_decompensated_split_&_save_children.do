// USERNAME
// Code does two things:
// 1) Splits parent according to the four proportion models
// 2) Runs the "save_results" shared function for the four proportion models

// split_cod_model.ado & save_results.do are part of the shared_code

// The description should be updated for each run

// Enter the following in the cluster to submit this
/*
qsub -N "decompensated" -pe multi_slot 16 -P "proj_custom_models" -e "FILEPATH" -o "FILEPATH" "FILEPATH/stata_shell.sh" "FILEPATH/cirrhosis_etiology_splits_03_decompensated_split_&_save_children.do" 
*/


// Set locals
	clear all
	local v_parent = 155018
	local v_prop_hepb = 141662 // final for 2016 == 141662
	local v_prop_hepc = 141683 // final for 2016 == 141683
	local v_prop_etoh = 141641 // final for 2016 == 141641
	local v_prop_other = 141686 // final for 2016 == 141686

//////////////////////////////////////////////////////////////////////
// Step 1: Split Parent (ME 1919) according to the proportion models
//////////////////////////////////////////////////////////////////////
// Order for both proportion models and attribution models is: hep b, hep c, alcohol, other
	clear all

	run "FILEPATH/split_epi_model.ado"

	split_epi_model, source_meid(1919) target_meids(2892 2893 2891 2894) prop_meids(1920 1921 1922 1923) output_dir("FILEPATH/cirrhosis") gbd_round_id(4) clear


/////////////////////////////////////////////////////////
// Step 3: Save Attributed Compensated Cirrhosis Results 
/////////////////////////////////////////////////////////

// Cirrhosis due to Hep B
	clear all

	run "FILEPATH/save_results.do"

	save_results, modelable_entity_id("2892") gbd_round(2016) mark_best("yes") env("prod") file_pattern("{location_id}.h5") h5_tablename("draws") description("Cirrhosis due to Hep B - Parent version `v_parent' using proportion version `v_prop_hepb'") in_dir("FILEPATH")

// Cirrhosis due to Hep C
	clear all

	run "FILEPATH/save_results.do"

	save_results, modelable_entity_id("2893") gbd_round(2016) mark_best("yes") env("prod") file_pattern("{location_id}.h5") h5_tablename("draws") description("Cirrhosis due to Hep C - Parent version `v_parent' using proportion version `v_prop_hepc'") in_dir("FILEPATH")

// Cirrhosis due to Alcohol
	clear all

	run "FILEPATH/save_results.do"

	save_results, modelable_entity_id("2891") gbd_round(2016) mark_best("yes") env("prod") file_pattern("{location_id}.h5") h5_tablename("draws") description("Cirrhosis due to EtOH - Parent version `v_parent' using proportion version `v_prop_etoh'") in_dir("FILEPATH")

// Cirrhosi due to Other
	clear all

	run "FILEPATH/save_results.do"

	save_results, modelable_entity_id("2894") gbd_round(2016) mark_best("yes") env("prod") file_pattern("{location_id}.h5") h5_tablename("draws") description("Cirrhosis due to Other - Parent version `v_parent' using proportion version `v_prop_other'") in_dir("FILEPATH")
	

* the end
