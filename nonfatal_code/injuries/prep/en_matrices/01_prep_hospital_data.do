// Author:	USERNAME
// Date:	DATE
// Puropse: Prep the hospital data for creation of EN-matrices;

	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		set odbcmgr unixodbc
	}

	local check=1
	if `check'==1 {
		capture log close
		local 1 "FILEPATH"
		local 2 "DATE"
		local 3 "03b"
		local 4 "EN_matrices"
		local 5 "FILEPATH"
		local 6 "FILEPATH"
		local 7 "FILEPATH"
		local 8 "FILEPATH"
		local 9 "FILEPATH"
		local 10 "ihme_data"
	}

	// Import macros
	// prefix
	global prefix `1'
	// date of run
	local date `2'
	// step number (03c)
	local step_num `3'
	// step name EN_matrices
	local step_name `4'
	// code repo
	local code_dir `5'
	local cleaned_dir `6'
	local prepped_dir `7'
	// directory for external inputs
	local in_dir `8'
	// directory for external inputs (to get from other steps)
	local root_j_dir `9'	
	// ihme_data chinese_niss
	local dataset `10'

	local functional _inj
	local ages 0 1 5 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95

	// directories for standard code files
	adopath + "`code_dir'/ado"
	adopath + "FILEPATH"
	
	** get the step number & filepath to the hierarchies
	get_step_num, name(hierarchies) stepfile("`code_dir'/FILEPATH.xlsx")
	local hierarchies_dir = "`root_j_dir'/FILEPATH"
	di "`hierarchies_dir'"

	** store the parent/child combos for summing over later
	import excel using "`in_dir'/FILEPATH.xlsx", firstrow clear
	levelsof acause, local(modeled_ecodes)
	rename acause child_model
	gen parent_model=subinstr(DisModmodels, "Child of ", "", .) if regexm(DisModmodels, "Child of")
	replace DisModmodels="Single model" if child_model=="inj_disaster" | child_model=="inj_war"
	replace parent_model=child_model if DisModmodels=="Single model"
	keep if parent_model!=""
	keep child_model parent_model DisModmodels
	** cycle through the parent e_codes, pulling the list of child codes for each particular parent
	** this list contains all of the e-codes WITH child models
	levelsof parent_model if regexm(DisModmodels, "Child of"), local(parents_with_children) clean
	** this list contains all of the e-codes that ARE child models
	levelsof child_model if regexm(DisModmodels, "Child of"), local(children_with_parents) clean
	** this list contains all of the e-codes that are single models
	levelsof child_model if regexm(DisModmodels, "Single model"), local(single_models) clean
	tempfile child_parent
	save `child_parent', replace
	
	import excel "`hierarchies_dir'/FILEPATH.xls", sheet("noninp") firstrow clear
	levelsof ncode if Comments=="Inpatient only", local(inp_only_cs) clean
	levelsof ncode if Comments!="Inpatient only", local(noninp_cs) clean
	
	
	** bring in the cleaned hospital data
	insheet using "`cleaned_dir'/FILEPATH`FILEPATH'.csv", comma names clear
	** check if any of the "outpatient" marked cases have inpatient-only n-codes
	gen marked_otp=0
	foreach nvar of varlist final_ncode_* {
		tostring `nvar', replace
		quietly replace `nvar'="" if `nvar'=="."
		quietly replace `nvar'=`nvar' if `nvar'!=""
		foreach code of local inp_only_cs {
			quietly replace marked_otp=1 if `nvar'=="`code'" & inpatient==0
		}
		** end inpatient-only ncode loop
	}
	** end marked_otp loop

	** now replace all non-inpatient-only n-codes with "" but only for those who had n-codes warrenting inpatient care but were marked as outpatient
	foreach nvar of varlist final_ncode_* {
		di "------------------------- `nvar' -------------------------"
		foreach code of local noninp_cs {
			di "`code'"
			quietly replace `nvar'="" if `nvar'=="`code'" & marked_otp==1
		}
	}
	
	replace inpatient = 1 if marked_otp==1
	drop marked_otp
	
	count if inpatient==1
	local inp_count=`r(N)'
	count if inpatient==0	
	local noninp_count=`r(N)'
	local platforms ""
	if `inp_count'>0 {
		local platforms "`platforms' inp"		
	}
	if `noninp_count'>0 {
		local platforms "`platforms' noninp"
	}
	display "`platforms'"
	
	
	tempfile cleaned
	save `cleaned', replace
	
	local plat_count=1
	
	foreach platform of local platforms {
		
		if ("`platform'"=="inp") local plat_num = 1 
		if ("`platform'"=="noninp") local plat_num = 0 
		
		** bring in hierarchy
		import excel "`hierarchies_dir'/FILEPATH.xls", sheet("`platform'") firstrow clear
		
		destring rank, replace
		
		if "`platform'"=="noninp" {
			drop if Comments == "Inpatient only"
		}
		
		** sort hierarchy from last to first ranking
		gsort - rank
		** make sure there are no spaces in the hierarcy for ncodes
		** because later we are going to use "[ncode] " to see if the hospital cases have each ncode
		replace ncode = subinstr(ncode, " ", "", .)
		replace ncode = ncode + " "
		clear mata
		putmata ncode
		count
		local total_ncodes = `r(N)'
		
		** bring in the hospital data, apply hierarchy to n-codes, replace age groups, collapse over e-codes, reshape
		use if inpatient==`plat_num' using `cleaned', clear
		** concatenate all n-code variables into one variable seperated by spaces
		gen all_ncodes=""
		foreach nvar of varlist final_ncode_* {
			tostring `nvar', replace
			replace `nvar'="" if `nvar'=="."
			replace all_ncodes = all_ncodes + `nvar' + " " if `nvar'!=""
			drop `nvar'
		}
		
		gen single_ncode = ""
		** starting from the lowest ranked n-code from the hierarchy (stored as index 1 in the matrix)
		** replace the single ncode variable with that n-code if the concatenated string of n-codes contains that ncode + " "
		forvalues i = 1/`total_ncodes' {
			mata: st_local("ncode", ncode[`i'])			
			replace single_ncode = subinstr("`ncode'"," " , "", .) if regexm(all_ncodes, "`ncode'")
		}
		drop all_ncodes
		drop if single_ncode == ""
		
		** replace age groups with the reduced group of ages
		local last_age = 0
		gen reduced_age = .
		foreach age of local ages {
			local this_age = `age'
			replace reduced_age = `last_age' if age<`this_age' & age>=`last_age'
			local last_age = `age'
		}
		local temp = subinstr("`ages'", " ", ",", .)
		
		replace reduced_age = max(`temp') if age >= max(`temp') & age != .
		
		** some final_ecode_ variables will be byte with completely empty values & we want to drop those before the reshape
		foreach evar of varlist final_ecode_* {
			tostring `evar', replace
			count if `evar'=="."
			if `r(N)' == _N {
				drop `evar'
			}
		}
		** reshape by ecode- so that each age-sex-iso3-year-ecode has own line
		gen unique = _n
		tempfile prereshaped
		save `prereshaped', replace
		
		reshape long final_ecode_, i(unique) j(ecode)
		
		drop if final_ecode_==""
		drop unique ecode
		rename final_ecode_ e_code
		rename single_ncode n_code
		
		** collapse counts of E/N combinations by age/sex/year/iso3
		contract e_code n_code reduced_age sex iso3 year inpatient
				
		** cycle through all of the parent causes, sum together all of their child causes and add back onto the parent
		** generate a new variable for the parent model if the e_code contains the parent name (i.e. inj_suicide_fire > inj_suicide)
		generate parent = ""
		local parent_models `parents_with_children' `single_models'
		foreach parent of local parent_models {	
			replace parent = "`parent'" if regexm(e_code,"`parent'")
		}
		levelsof e_code if parent=="", local(eS) clean
		display in red "These e_codes have no parents in `dataset': `eS'"
		drop if parent == ""
		** replace parent = e_code if parent == ""
		
		** we do want to sum over the parents of children that we model, but we also want to keep the disaggregated counts of those modeled child E-codes
		gen child=0
		foreach child of local children_with_parents {	
			replace child = 1 if e_code=="`child'"
		}			

		preserve
		keep if child == 1
		drop child
		tempfile child_counts
		save `child_counts', replace
		restore

		collapse (sum) _freq, by(n_code reduced_age sex iso3 year inpatient parent)
		
		rename parent e_code
		append using `child_counts'
		
		** sum over all of the e_codes to get the final counts with the child counts added to the parents
		
		collapse (sum) _freq, by(e_code n_code reduced_age sex iso3 year inpatient)
				
		if `plat_count' == 1 {
		
			tempfile final_counts
			save `final_counts'
			local ++plat_count
		}
		else {
			append using `final_counts'
			save `final_counts', replace
			local ++plat_count
		}
		
	}
	** end platform loop
	
	use `final_counts', clear
	
	rename _freq cases
	rename reduced_age age
	
	drop if n_code==""
	
	outsheet using "`prepped_dir'/FILEPATH.csv", comma replace
	
	
	