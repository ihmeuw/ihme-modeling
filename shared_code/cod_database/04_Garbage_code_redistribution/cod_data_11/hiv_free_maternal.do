** Purpose: Adjust the sample size for maternal sources that do not use the envelop in data prep
** ********************************************************************************
// Set up driver for connection string
forvalues dv=4(.1)6 {
	local dvfmtd = trim("`: di %9.1f `dv''")
	cap odbc query, conn("DRIVER={strConnection};SERVER=strServer;UID=strUsername;PWD=strPassword")
	if _rc==0 {
		local driver "MySQL ODBC `dvfmtd' Unicode Driver"
		local db_conn_str "DRIVER={strConnection}; UID=strUsername; PWD=strPassword"
		continue, break
	}
}

 ** global j "J:"

** ********************************************************************************
// Save aggregated data
	tempfile agg_data
	save `agg_data', replace

// Identify the maternal sources that do not use the envelope in the prep and adjust the sample size
	clear
	gen foo = . 
	tempfile master
	save `master', replace

local mat_env_sources : dir "$j/WORK/03_cod/01_database/02_programs/compile/data/maternal_env_sources" files "mat_env_*"
	foreach file of local mat_env_sources {
		import excel using "$j/WORK/03_cod/01_database/02_programs/compile/data/maternal_env_sources/`file'", clear firstrow

		keep NID
		duplicates drop
		append using `master'
		tempfile master
		save `master', replace
	}
	drop foo
	local maxtries 10
	local tries 0
	capture save "$j/WORK/03_cod/01_database/02_programs/compile/data/maternal_env_sources/NIDS_mat_env_sources.dta", replace
	while _rc & `tries'<`maxtries' {
		capture save "$j/WORK/03_cod/01_database/02_programs/compile/data/maternal_env_sources/NIDS_mat_env_sources.dta", replace
		local tries = `tries' +1		
	}
	if _rc {
		** try again so that we can see the error being thrown (or succeed)
		save "$j/WORK/03_cod/01_database/02_programs/compile/data/maternal_env_sources/NIDS_mat_env_sources.dta", replace
	}
	merge 1:m NID using `agg_data', keep (2 3) 
	
	
	// HIV-free envelope adjustments for non-VR, VA, cancer maternal sources that do not use the envelope to calculate cc_code, except CHina DSP
		egen NID_id = group(NID)
		gen has_mat = 1 if regexm(acause, "maternal") & _m == 2 & sex == 2
		replace has_mat =. if source == "China_1991_2002"
		bysort NID_id sex: egen mat_tag = total(has_mat)
		** bysort NID_id: egen other_tag = total(has_other) 
		drop _m
		save `agg_data', replace

		// Work with this data	
		** gen adj_mat_source = 1 if mat_tag >0 & other_tag == 0
		gen adj_mat_source = 1 if mat_tag >0 & (age >=10 & age <=50)
		tempfile adj_data
		save `adj_data', replace
		save "$j/WORK/03_cod/01_database/03_datasets/$source/explore/adj_data.dta", replace

		count if adj_mat == 1
		display in red "There are `r(N)' observations to be adjusted"
		
	if `r(N)' > 0 {	
		// Count the deaths before
		gen deaths_check  = cf_final* sample_size
		summ deaths_check
		local deaths_before = `r(sum)'
		drop deaths_check
		gen version = "before"
		tempfile b
		save `b', replace
		save "$j/WORK/03_cod/01_database/03_datasets/$source/explore/test_before_hiv_free.dta", replace
		
		// Load env with HIV included 
		local lsvid 38
		#delimit ;
			odbc load, exec("
				SELECT 
					o.year_id as year, o.location_id, o.sex_id as sex, o.age_group_id, o.mean_pop as pop, o.mean_env_whiv as env, loc.path_to_top_parent, o.output_version_id, loc.ihme_loc_id
				FROM 
					mortality.output o 
				INNER JOIN 
					mortality.output_version ov ON o.output_version_id = ov.output_version_id and ov.is_best = 1 
				INNER JOIN 
					shared.age_group ag ON o.age_group_id = ag.age_group_id 
				INNER JOIN 
					shared.location_hierarchy_history loc ON o.location_id=loc.location_id 
														  AND loc.location_set_version_id = `lsvid'
														  AND loc.level >= 3
				WHERE 
					o.age_group_id BETWEEN 2 and 21 and 
					o.sex_id IN(1,2) and 
					o.year_id BETWEEN 1970 and 2015
			") conn("`db_conn_str'") clear ;
			#delimit cr
	
			** Clean
			keep sex year age_group_id location_id ihme_loc_id env year
			gen iso3 = substr(ihme_loc_id,1,3)
			keep if sex == 2
			drop if age_group_id > 21
			** Convert age_groupids to gbd ages
			gen double age = (age_group_id-5)*5
			replace age = 0 if age_group_id==2
			replace age = .01 if age_group_id==3
			replace age = .1 if age_group_id==4
			replace age = 1 if age_group_id==5
			drop age_group_id
			** Save
			rename env env_hiv
			tempfile hiv_inc
			save `hiv_inc', replace
		
		// Load normal env with HIV
		** Need location ids
			odbc load, exec("select location_id, map_id as iso3 from shared.location") conn("`db_conn_str'") clear
			replace iso3 = "TWN" if location_id == 8
			replace iso3 = "ASM" if location_id == 298
			replace iso3 = "BMU" if location_id == 305
			replace iso3 = "GRL" if location_id == 349
			replace iso3 = "GUM" if location_id == 351
			replace iso3 = "MNP" if location_id == 376
			replace iso3 = "VIR" if location_id == 422
			replace iso3 = "SSD" if location_id == 435
			replace iso3 = "SDN" if location_id == 522
			replace iso3 = "PRI" if location_id == 385
			drop if location_id == 188
			gen length = length(iso3)
			drop if length != 3
			drop length
			tempfile locs
			save `locs', replace
		
		do "$j/WORK/03_cod/01_database/02_programs/prep/code/env_long.do"
		drop pop
		rename env env_no_hiv
		rename location_id sub_locs
		merge m:1 iso3 using `locs', assert(2 3) keep(3) nogen
		replace location_id = sub_locs if sub_locs != .
		drop sub_locs
		
		// Find the ratio 
		merge 1:1 iso3 location_id year sex age using `hiv_inc', assert(1 3) keep(3) nogen
		gen double ratio = env_no_hiv/env_hiv
		** Fix the location_ids again to match the data
		split ihme_loc_id, p("_")
		destring ihme_loc_id2, replace
		replace location_id = ihme_loc_id2
		drop ihme_loc_id*
		tempfile ratio
		save `ratio', replace

		// Adjust the sample size to exclude HIV
		merge 1:m iso3 location_id sex age year using `adj_data', assert(1 3) keep(3) nogen
		gen double sample_size_adj = sample_size * ratio if adj_mat_source == 1 
		replace sample_size_adj = sample_size if adj_mat_source!=1
		// replace cause fractions with hiv cause fractions
		keep acause age NID iso3 list location_id national region sex source source_label source_type subdiv year sample_size sample_size_adj cf_raw cf_corr cf_rd cf_final adj_mat_source
		foreach var of varlist cf* {
			replace `var' = (`var'*sample_size)/sample_size_adj 
		}
		// Compare with deaths before
		replace sample_size = sample_size_adj 
		
		gen deaths_check = cf_final* sample_size
		summ deaths_check
		local deaths_after = `r(sum)'
		di "`deaths_before'"
		di "`deaths_after'"
		save "$j/WORK/03_cod/01_database/03_datasets/$source/explore/test_after_hiv_free.dta", replace

		assert abs(`deaths_before' - `deaths_after') < .001

	}
		
	keep acause age NID iso3 list location_id national region sex source source_label source_type subdiv year sample_size cf_raw cf_corr cf_rd cf_final
	// DONE!
		
		
