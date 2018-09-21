// USERNAME
// Code does two things:
// 1) Splits parent cirrhosis CoD model according to the four proportion models
// 2) Runs the "save_results" shared function for the four proportion models

// split_cod_model.ado & save_results.do are part of the shared_code

// The description should be updated for each run

// Enter the following in the cluster to submit this
/*
qsub -N "cirrhosis_cod_split" -pe multi_slot 20 -P "proj_custom_models" -e "FILEPATH" -o "FILEPATH" "FILEPATH/stata_shell.sh" "FILEPATH/cirrhosis_cod_split_models_&_save_results.do" 
*/

	clear all

// Set locals for verion number of best proportion models (as of 4/25/2017)
	local v_prop_hepb = 141662
	local v_prop_hepc = 141683
	local v_prop_etoh = 141641
	local v_prop_other = 141686

//////////////////////////////////////////////////////////////////////
// Step 1: Split Cirrhosis Parent (cause 521) according to the proportion models
//////////////////////////////////////////////////////////////////////
// Order for both proportion models and attribution models is: hep b, hep c, alcohol, other
	run "FILEPATH/split_cod_model.ado"
	split_cod_model, source_cause_id(521) target_cause_ids(522 523 524 525) target_meids(1920 1921 1922 1923) output_dir("FILEPATH") clear


/////////////////////////////////////////////////////////
// Step 2: Save Attributed Cirrhosis CoD Results 
/////////////////////////////////////////////////////////

// Cirrhosis due to Hep B
	clear 

	run "FILEPATH/save_results.do"
	
	save_results, cause_id(522) gbd_round(2016) in_rate("no") mark_best("yes") model_version_type_id(4) description("Cirrhosis due to Hep B using proportion model version `v_prop_hepb'") in_dir("FILEPATH/522")
	
// Cirrhosis due to Hep C
	clear 

	run "FILEPATH/save_results.do"

	save_results, cause_id(523) gbd_round(2016) in_rate("no") mark_best("yes") model_version_type_id(4) description("Cirrhosis due to Hep C using proportion model version `v_prop_hepc'") in_dir("FILEPATH/523")

// Cirrhosis due to Alcohol
	clear 

	run "FILEPATH/save_results.do"

	save_results, cause_id(524) gbd_round(2016) in_rate("no") mark_best("yes") model_version_type_id(4) description("Cirrhosis due to alcohol using proportion model version `v_prop_etoh'") in_dir("FILEPATH/524")

// Cirrhosi due to Other
	clear 

	run "FILEPATH/save_results.do"

	save_results, cause_id(525) gbd_round(2016) in_rate("no") mark_best("yes") model_version_type_id(4) description("Cirrhosis due to other using proportion model version `v_prop_other'") in_dir("FILEPATH/525")
	

* the end
