** Purpose: Make HIV-free, shock-free cause fractions. The envelope is constructed without hiv or shocks. Deaths and rates are biased downward

** SAVE DATA 
	tempfile data
	save `data', replace

** FIRST, ENSURE THAT SAMPLE SIZE ID VARIABLES ARE KNOWN AND THAT SAMPLE SIZE IS CONSISTENT WITHIN THESE VARIABLES
	** identify variables within which sample_size is calculated
	if "$source" != "Cancer_Registry" local id_vars = "iso3 location_id year sex source_label NID subdiv age"
	else local id_vars = "iso3 location_id year sex"

	
** FOR NON-MATERNAL SOURCES: GET CAUSES USED TO ADJUST SAMPLE SIZE, THEN GET ALL AFFECTED CAUSES THAT ARE NOT ADJUSTED
	** get parents by finding all the elements of the path_to_top_parents of sample_size_adjust causes that aren't already in the list
	** load a cause_id-acause-path_to_top_parent map
	odbc load, exec("SELECT cause_id, acause, path_to_top_parent FROM shared.cause_hierarchy_history WHERE cause_set_version_id=48") strConnection clear
	tempfile causes
	save `causes', replace

	** get map path_to_top_parent to the acauses we know to adjust for
	import excel using "$j/WORK/03_cod/01_database/02_programs/compile/data/sample_size_adjust_causes.xlsx", firstrow clear
	merge 1:m acause using `causes', nogen assert(2 3) keep(3)
	tempfile ss_adj_causes
	save `ss_adj_causes', replace
	** split the path_top_parent into cause ids and reshape long
	split path_to_top_parent, p(,)
	drop path_to_top_parent
	reshape long path_to_top_parent, i(acause) j(level)
	destring path_to_top_parent , replace
	** drop the last entry of path_to_top_parent that is the original cause_id that we already know about
	drop if cause_id==path_to_top_parent
	keep path_to_top_parent
	rename path_to_top_parent cause_id
	duplicates drop
	** get the acauses of the parent ids
	merge 1:m cause_id using `causes', assert(2 3) keep(3) nogen
	tempfile parents
	save `parents', replace

	** children are easier because we can query directly for them using the cause_id
	use `ss_adj_causes', clear
	levelsof cause_id, local(cause_ids)
	clear
	gen foo = .
	tempfile children
	save `children', replace
	foreach cause_id of local cause_ids {
		odbc load, exec("SELECT cause_id, acause, path_to_top_parent FROM shared.cause_hierarchy_history WHERE path_to_top_parent LIKE '%,`cause_id',%' AND cause_set_version_id=49") strConnection clear
		append using `children'
		save `children', replace
	}
	drop foo
	append using `parents'
	append using `ss_adj_causes'
	tempfile all_affected_causes
	save `all_affected_causes'

** ADJUST SAMPLE SIZE DOWN BY THE NUMBER OF DEATHS IN SAMPLE SIZE ADJUST CAUSES (ONLY IF IT IS NOT A CHILD/PARENT/INSTANCE OF SAME)
	use `ss_adj_causes', clear
	keep acause
	merge 1:m acause using `data', keep(2 3)
	gen is_adjustment_cause = 1 if _merge==3
	drop _merge
	merge m:1 acause using `all_affected_causes', keep(1 3) keepusing(acause)
	gen is_affected_cause = 1 if _merge==3
	drop _merge

	** remove sample size based on cause list
	** save the deaths to remove; these are the deaths from causes that are not in the envelope (hiv, inj_war, etc)
	gen deaths_remove = cf_final*sample_size if is_adjustment_cause==1
	replace deaths_remove = 0 if deaths_remove==.
	bysort `id_vars': egen sample_size_remove = sum(deaths_remove)
	** drop the sample size for all but the hiv/shock causes
	gen sample_size_adj = sample_size
	replace sample_size_adj = sample_size_adj-sample_size_remove if is_affected_cause != 1
	drop is_affected_cause is_adjustment_cause sample_size_remove deaths_remove
	tempfile has_new_ss
	save `has_new_ss', replace

** REMAKE CAUSE FRACTIONS BASED ON ADJUSTED SAMPLE SIZE
	foreach stage in raw corr rd final {
		rename cf_`stage' cf_`stage'_old
		gen cf_`stage' = (cf_`stage'_old*sample_size)/sample_size_adj
	}
	gen deaths_final = cf_final*sample_size_adj
	rename sample_size sample_size_with_hiv
	rename sample_size_adj sample_size
	drop *_old sample_size_with_hiv deaths_final
	
