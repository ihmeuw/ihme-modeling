
** Purpose: correct Turner and Klinefelter input data to the correct denominators (# live births, sex-specific rather than both sexes)
** {AUTHOR NAME}

**--------------------------------
** setup
	clear all 
	set more off
	set maxvar 30000
	cap restore, not
	cap log close 

	** set prefix for the OS 
	if c(os) == "Windows" {
		global dl "{FILEPATH}"
	}
	if c(os) == "Unix" {
		global dl "{FILEPATH}"
		set odbcmgr unixodbc
	}

run "$dl/{FILEPATH}/get_ids.ado"
run "$dl/{FILEPATH}/upload_epi_data.ado"
run "$dl/{FILEPATH}/get_covariate_estimates.ado"

local dir "{FILEPATH}"
local birth_file "{FILEPATH}"
local date 4_11

** set toggle for whether to delete data from database first:
	local delete_data 0

**------------------------------------
 local bundle_list {BUNDLE ID LIST}


foreach b in `bundle_list' {

	** Turner -- FEMALE only 
		if (`b'==228 | `b'==437) {
		local c cong_turner
		}

	** klinefelter -- MALE only
	if (`b'==229 | `b'==438) {
		local c cong_klinefelter
		}

**----------------------------------------------------------------------------------------
** If necessary to clear out database first: 
	if `delete_data' == 1 {

	** updated 3/6, using get_epi_data: 	
		get_epi_data, bundle_id("`b'") clear 
		keep seq bundle_id

		local destination_file  "{FILEPATH}/deleting_all_`b'_data.xlsx"
			export excel using "`destination_file'", firstrow(variables) sheet("extraction") replace
			upload_epi_data, bundle_id("`b'") filepath("`destination_file'") clear 
	} 

**--------------------------------------------
** 1. Append together all prepped data files (lit and non-lit)
di in red "Appending all `b' data"

	** upload all files from bundle folders, to then delete these data 
		local f "`dir'/{FILEPATH}"
		local files : dir "`f'" files *
			local i 1
			foreach ff in `files' {
				local name = substr("`ff'", 1, 5)
				import excel using "`f'/`ff'", firstrow clear
					foreach var in table_num uncertainty_type specificity input_type page_num case_diagnostics underlying_field_citation file_path sampling_type note_modeler {
						cap tostring `var', replace
						}
					cap replace input_type= "extracted" if input_type==.


			cap replace uncertainty_type="" if uncertainty_type==".", replace 

					gen source = "`name'"
					tempfile `name'
					save ``name'', replace 
				di "`name' imported"

					if `i'==1 {
					local `b'_file_names `name'
					}
				if `i' >1 {
					local `b'_file_names = "``b'_file_names'" + " " + "`name'"
					}

				local i = `i'+1
				} 

		local f2 "`dir'/{FILEPATH}"
		local files : dir "`f2'" files *
			foreach ff in `files' {
				local name = substr("`ff'", 1, 18)
				import excel using "`f2'/`ff'", firstrow clear
					foreach var in table_num case_definition case_name note_SR uncertainty_type representative_name unit_type specificity input_type page_num case_diagnostics underlying_field_citation file_path sampling_type note_modeler prenatal_fd_def extractor {
						cap tostring `var', replace
						}
					foreach var in location_id smaller_site_unit sex_issue year_start year_end year_issue age_start age_end age_issue age_demo unit_value_as measure_issue response uncertainty_type_value cv_* {
						cap destring `var', replace			
							}
					cap replace uncertainty_type="" if uncertainty_type==".", replace 
					cap replace input_type= "extracted" if input_type=="."
					replace extractor="{USERNAME}" if extractor==""

					gen source = "`name'"
					tempfile `name'
					save ``name'', replace 
				di "`name' imported"

				local `b'_file_names = "``b'_file_names'" + " " + "`name'"
				}

	di "``b'_file_names'"

	**------------------------------
	** append all files together 
		local i 0
			foreach f of local `b'_file_names {
		 		di "`f'"
		 		local f = "``f''" 
	
		 		if `i'==0 {
					use "`f'", clear
					} 
				if `i' > 0 {
	 				append using "`f'"
	 				} 
			
				local i= `i'+1 
				} 

		tempfile all_input_data_`b'
		save `all_input_data_`b'', replace 

		** clean up tempfiles 
		foreach f_name in ``b'_file_names' {
			erase ``f_name''
			}

**----------------------------------------------------
		** 1.2 change formatting as necessary for uploader
				cap gen underlying_field_citation=""
				foreach var in sampling_type uncertainty_type spec input_type underlying_field_citation {
					replace `var' ="" if `var'=="."
					}

				foreach var in count count2 ihme_loc_id cv_inpatient parent_id cv_no_still_births data_sheet_filepath is_outlier outlier_type_id cv_autopsy cv_bias cv_diag_postnatal cv_echo {
					cap drop `var'
					}

			** 1.3 drop covariate columns (cv_*) that don't apply 
				cap drop cv_includes_chromos cv_excludes_chromos cv_aftersurgery
					drop cv_under_report

		** preserve the non-prevalence data 
			preserve
				keep if measure !="prevalence"
				tempfile mtwith
				save `mtwith', replace
			restore

**----------------------------------------------------------------------
** 2. Identify data that is sex-specific and does not need correction.
**		Drop data from the wrong sex

	if "`c'"=="cong_turner" {
		drop if sex=="Male"
		}

	if "`c'"=="cong_klinefelter" {
		drop if sex=="Female"
		}

	preserve
		keep if sex != "Both"
		tempfile sex_specific
		save `sex_specific', replace
	restore

	drop if sex !="Both"

	if "`c'"=="cong_turner" {
		replace sex="Female"
		}

	if "`c'"=="cong_klinefelter" {
		replace sex="Male"
		}

**---------------------------
** 3. Pull in the # of births & calc the male:female ratios for each location 
	preserve
		import delimited using "`birth_file'", clear 	
		drop covariate_id covariate_name_short lower_value upper_value
		
		rename year_id year_end  
		gen sex = "Both" if sex_id==3
			replace sex = "Male" if sex_id==1
			replace sex = "Female" if sex_id==2 
			drop sex_id
		rename mean_value births
		
			reshape wide births, i(location_id year_end) j(sex) string 
				gen prop_Male = birthsMale/birthsBoth
				gen prop_Female = birthsFemale/birthsBoth
					drop births*
			reshape long prop_, i(location_id year_end) j(sex) string 
				rename prop_ birth_prop

		tempfile births
		save `births', replace
	restore

	merge m:1 location_id  year_end sex using `births', keepusing(birth_prop) keep(3) nogen 


**-----------------------------------------------------
** 4. Correct the denominators to be sex-specfiic, append the already sex-specific data	 
	replace sample_size = birth_prop *sample_size 
		append using `sex_specific'
	replace mean = cases/sample_size if cases!=. & sample_size !=.
	replace note_SR = note_SR + "discounted both-sex births denominator using sex-specific birth covariate estimates; "
	replace measure_adj =1

** 5. Apply correction factors using literature values for the proportion of cases that are diagnosed at birth and/or before one year of age 

	** Turner Syndrome: 
	 ** NID 283283 -- Belgium: in 1991 15% of cases were diagnosed before age 1; in 2003 30% were diagnosed.
	 		** use these proportions to convert registry birth prevalence data to later-in-life data 
			** interpolate linearly between 1991 and 2003; hold correction factor constant pre-1991 and post-2003. 
			
		
		if `b'==228 {
			** apply correction factor to prevalence values with age_end > 1

		egen year = rowmean(year_start year_end) 
		gen correction = .
			replace correction = .15 if year <=1991
			replace correction = .30 if year >=2003
				replace correction = 0.30 - (.0125*(2003-year)) if (year > 1991 & year < 2003)

				replace mean = mean * 1/correction if measure=="prevalence"

					drop correction year birth_prop
					replace note_SR = note_SR + "applied literature correction factor for under-reporting in registry data" if measure=="prevalence"
					replace measure_adj =1 if measure=="prevalence"
					}


		if `b'== 437 {
			** apply correction factor to prevalence values with age_end < 1

		egen year = rowmean(year_start year_end) 
		gen correction = .
		replace correction = .15 if year <=1991
		replace correction = .30 if year >=2003
			replace correction = 0.30 - (.0125*(2003-year)) if (year > 1991 & year < 2003)

			replace mean = mean * 1/correction if age_end <1

				drop correction year birth_prop
				replace note_SR = note_SR + "applied literature correction factor for under-reporting in registry data" if measure=="prevalence" & age_end <1
				replace measure_adj =1 if age_end <1
				}

	**-----------------------------------
	** Klinefelter Syndrome: 
 		if `b'==229 {
		** NID 310196: 20% of all Klinefelter cases diagnosed prenatally; another 26% diagnosed before age 11
		** if we assume that there is some diagnosis between birth and registry data collection (up to 1 year) 
		** and that the rate of diagnosis is constant over age, this results in 20% + 2.6% of cases diagnosed before age 1

		egen year = rowmean(year_start year_end) 
		gen correction = .226
			
			replace mean = mean * 1/correction if age_end <1

				drop correction year birth_prop
				replace note_SR = note_SR + "applied literature correction factor for under-reporting in registry data" if measure=="prevalence" & age_end <1
				replace measure_adj =1 if age_end <1
				}		 
		

**----------------------------
** mark zero prevalence values as outliers and export 
	append using `mtwith'

		gen outlier_type_id=0
			replace outlier_type_id=1 if mean==0 & measure=="prevalence"
		
	local destination_file "`dir'/{FILEPATH}/`b'_with_sex_adjustment_and_correction_factors.xlsx"
 		export excel using "`destination_file'", replace firstrow(variables) sheet("extraction")
		di in red "`c' `b' input data saved" 
 
 	** upload: 
 		upload_epi_data, bundle_id(`b') filepath("`destination_file'") clear 
