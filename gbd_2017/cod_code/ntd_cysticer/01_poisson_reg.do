/*====================================================================
project:       GBD2017
Dependencies:  IHME
----------------------------------------------------------------------
Creation Date:    10 Aug 2017 - 16:40:38
Do-file version:  GBD2017
Output:           Submit script to model mortality due to Cysticercosis and process predicted estimates
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	clear mata
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "FILEPATH"
	}


* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2017"
	*model step
	local step COD
	*local cause
	local cause ntd_cysticer
	*local bundle "/#/"
	local bundle /59/
	*local root
	local localRoot "FILEPATH"
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*local temporary directory for other things:
	local local_tmp_dir "FILEPATH"
	*directory for output of draws > on ihme/scratch
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

* Make and Clear Directories
	*set up base directories on shared
	capture shell mkdir "FILEPATH"
	capture shell mkdir "FILEPATH"/`gbd'
	capture shell mkdir "FILEPATH"/`cause'/`gbd'/tmp
	capture shell mkdir "FILEPATH"/`cause'/`gbd'/logs
	capture shell mkdir "FILEPATH"/`cause'/`gbd'/progress

	*make all directories
	local make_dirs code in tmp local_tmp out log progress
	foreach dir in `make_dirs' {
		capture shell mkdir ``dir'_dir'
	}
/*	*clear tempfiles, logs and progress files
	foreach dir in log progress tmp {
		capture cd ``dir'_dir'
		capture shell rm *
	}
*/
*Directory for standard code files
	adopath + "FILEPATH"

	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "FILEPATH", replace
	***********************

	*print macros in log
	macro dir

*Code Testing
	local rebuild 1
	*set equal to 0 if dataset should not be rebuilt



/*====================================================================
                        1: Setup Custom Model
====================================================================*/


*--------------------1.1: Load Custom CoD Functions

	run "FILEPATH".ado
	run "FILEPATH".ado
	run "FILEPATH".ado
	run "FILEPATH".ado



/*====================================================================
                        2: Create the Dataset
====================================================================*/

if `rebuild' == 1 {

*--------------------2.1: Set Dataset Settings

	local cause_id 352
	local covariate_ids 100 1099 208 57 99 142 119 1087 1218
	local get_model_results gbd_team(ADDRESS) model_version_id(294383) measure_id(5)
	local saveto "FILEPATH".dta
	local minAge 5

*--------------------2.2: Build CoD Dataset

	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') get_model_results(`get_model_results') saveto(`saveto', replace) clear
	save `saveto', replace

*--------------------2.3: Add Custom Covariate

	*Get GBD Info
		*Location Metadata
			get_location_metadata,location_set_id(35) clear
			keep location_id path_to_top_parent level most_detailed location_name region_id
			tempfile metadata
			save `metadata', replace
			levelsof location_id,local(alllocs) clean

		*Demographics
			get_demographics,gbd_team(ADDRESS) clear
			local codyears `r(year_id)'
			local codages `r(age_group_id)'
			local codsexes `r(sex_id)'

		*Population
			get_population, location_id(-1) sex_id("`codsexes'") age_group_id("`codages'") year_id("`codyears'") clear
			tempfile skeleton
			save `skeleton', replace

			keep location_id
			duplicates drop
			tempfile locs
			save `locs', replace

	*Get Covariate Info
		*Pull in Covariate Information File

			*Location Metadata
			get_location_metadata,location_set_id(35) clear
			tempfile metadata1
			save `metadata1', replace

			*Get metadata
			use `saveto', clear
			keep location_id region_id path_to_top_parent level
			duplicates drop
			tempfile metadata2
			save `metadata2', replace

			*Append metadatas
			append using `metadata1'
			keep location_id region_id path_to_top_parent level
			duplicates drop
			tempfile metadata_all
			save `metadata_all', replace

			*Pull in covariate
			insheet using "FILEPATH", clear double
			tempfile muslim_raw
			save `muslim_raw', replace

			*Merge with metadata
			merge 1:1 location_id using `metadata_all', nogen

			*replace missing subnationals with national value
			split path_to_top_parent,p(",")
			gen parent_prop_muslim_unfilled = prop_muslim if level==3

			*bysort path_to_top_parent4 year_id age_group_id sex_id data_type nid: egen parent_prop_muslim = total(parent_prop_muslim_unfilled)
			*replace prop_muslim=parent_prop_muslim if prop_muslim==. & level>3

			bysort path_to_top_parent4: egen parent_prop_muslim = total(parent_prop_muslim_unfilled)
			replace prop_muslim=parent_prop_muslim if prop_muslim==. & level>3

			*replace other missing with regional mean
			bysort path_to_top_parent3 : egen region_mean=mean(prop_muslim)
			replace prop_muslim=region_mean if prop_muslim==.

		*Adjust Missing Locations
			*Missing locations: American Samoa (298), Greenland (349), Guam(351), Northern Mariana Islands (376), Us Virgin Islands (422)
			replace prop_muslim=0 if inlist(location_id,298,349,351,376,422)

	*Add to Dataset
		*Format for merge with cod database
			keep location_id prop_muslim

		*Merge and Save
			tempfile muslim_prepped
			save `muslim_prepped', replace

			merge 1:m location_id using `saveto', keep (matched using) nogen
			save `saveto', replace



*--------------------2.4: Custom Variable Transformation

	*Sanitation
		gen logit_sanitation=logit(sanitation_prop)

	*Age splines
		mkspline ageS=age_group_id, cubic displayknots

		save `saveto', replace


}

/*====================================================================
                        3: Run Model
====================================================================*/

	use `saveto', clear

*--------------------3.1: se Custom CoD Function to Submit Model

/*GBD2017 Re-run*/
run_best_model mepoisson study_deaths i.age_group_id i.super_region_id c.health_system_access_capped i.sex_id  logit_sanitation religion_muslim_prop pop_dens_under_150_psqkm_pct, exposure(sample_size) difficult vce(robust) reffects(|| location_id: R.age_group_id)

*--------------------3.2: Preliminary Model Testing

	//Test dispersion parameter in negative binomial to see if it is significant, if not use poisson
		*test [lndelta]_cons = 1\
	//Test fit of model
		*estat ic


/*====================================================================
                        4: Process Predictions
====================================================================*/


*--------------------4.1: Use Custom CoD Function to Push Estimates to CodViz

	local description "GBD2017 Methods:mepoisson CF;+i.SA_inc**studydeaths,i.SR,HSAC,i.sex,logit_sanit,prop_muslim,popdens<150;RE-loc||R.newagecat"
	process_predictions `cause_id', link(ln) random(yes) min_age(`minAge') multiplier(envelope) description("`description'")





log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.
2.
3.


Version Control:
