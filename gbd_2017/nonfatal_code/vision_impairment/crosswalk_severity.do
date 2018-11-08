
clear all 
set more off 

	
use "FILEPATH", clear 
tempfile all_vision_data_long
save `all_vision_data_long'

//Create template for all crosswalks
duplicates drop cause diagcode, force 
keep cause diagcode 
order cause diagcode
sort cause diagcode
split diagcode, parse(+)
replace diagcode=subinstr(diagcode, "+", "_", .) //+ can't go in new variable names 
foreach sev in DMILD DMOD DSEV DVB {
	gen beta_`sev' = . 
	gen se_beta_`sev' = . 
	gen n_beta_`sev' = . 
	}
tempfile crosswalks
save `crosswalks' 


use `all_vision_data_long', clear 

** All uncertainty to SE	
	replace uncertainty_type = "Sample size" if sample_size != . & uncertainty_type == ""
	replace uncertainty_type = "Standard error" if standard_error != . & uncertainty_type == ""
	replace uncertainty_type = "Confidence interval" if lower != . & uncertainty_type == ""
	drop if uncertainty_type == "" 

		// proportion parameters (Wilson's score interval):
		replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if uncertainty_type =="Sample size"
		
		// non-ratio 
		replace standard_error = max(upper-mean, mean-lower) / invnormal(0.975) if uncertainty_type =="Confidence interval"
		
		replace lower=.
		replace upper=.
		replace sample_size=.
		replace uncertainty_type ="Standard error"
	
	tempfile clean
	save `clean', replace






//Get variables prepped for isid and reshaping 
foreach var in nid field_citation_value age_start age_end year_start year_end sex location_id cv_best_corrected site urbanicity_type diagcode {
	 cap count if `var' == ""
	 cap count if `var' == .
	di "# missing `var': `r(N)'"
	}
	replace nid = -9999 if nid == . 
	replace site = "." if site == ""

//Don't want crosswalked data 
drop if regexm(note_modeler, "Split from combined severities via crosswalk")
replace is_outlier = 0 if note_modeler == "Outliered due to inability to calculate prevalence for given severity"
replace note_modeler = "" if note_modeler == "Outliered due to inability to calculate prevalence for given severity"
drop if nid == 218731 //this surveillance source should not be used 

sort nid field_citation_value sex age_start cause diagcode 
order nid field_citation_value age_start age_end year_start year_end sex location_id cv_best_corrected site urbanicity_type cause diagcode

duplicates tag nid field_citation_value age_start age_end year_start year_end sex location_id cv_best_corrected site urbanicity_type cause diagcode, gen(dup)
levelsof field if dup > 0, sep(",") local(dups)
bysort field dup: tab cause if inlist(field, `dups')
tab description if dup > 0 
br if dup > 0 
drop dup 


isid nid field_citation_value age_start age_end year_start sex location_id cv_best_corrected site urbanicity_type cause diagcode

keep mean standard_error nid field_citation_value age_start age_end year_start sex location_id cv_best_corrected site urbanicity_type cause diagcode
rename mean mean_
rename standard_error se_
replace diagcode=subinstr(diagcode, "+", "_", .) //+ can't go in new variable names 
reshape wide mean_ se_, i(field_citation_value age_start age_end year_start sex location_id cv_best_corrected site urbanicity_type cause) j(diagcode) str 
tempfile data_wide 
save `data_wide'




//For all possible combined severities, add subsituent components to get aggregrate (for crosswalk)
use `crosswalks', clear 
levelsof diagcode if regexm(diagcode, "_"), local(dxs)
* local dxs "ALL"
foreach diagcode in `dxs' {
		local dx = "`diagcode'"
		local dx_space = subinstr("`diagcode'", "_", " ", .)
		use `data_wide', clear 
		tokenize "`dx_space'"
		local dx_num 2
			if "`3'" != "" local dx_num 3
			if "`4'" != "" local dx_num 4

		//Calculate aggregate categories 
		if `dx_num' == 2 replace mean_`dx' = mean_`1' + mean_`2' ///
							if mean_`1' != . & mean_`2' != . 
		if `dx_num' == 3 replace mean_`dx' = mean_`1' + mean_`2' + mean_`3' ///
							if mean_`1' != . & mean_`2' != . & mean_`3'!= . 
		if `dx_num' == 4 replace mean_`dx' = mean_`1' + mean_`2' + mean_`3' + mean_`4' ///
							if mean_`1' != . & mean_`2' != . & mean_`3'!= . & mean_`4' != . 
		
		tempfile data_wide_temp
		save `data_wide_temp'
		levelsof cause if mean_`dx' != ., local(causes)
		*local causes "ALL"
		foreach cause in `causes' {
			di in red "RUNNING CROSSWALK REGRESSIONS FOR `cause' ~~~ `diagcode'"
			use `data_wide_temp' if cause == "`cause'"
			sum mean_`dx' mean_`1' mean_`2'			
			sum mean_`dx' mean_`1' mean_`2' if !missing(mean_`1') & !missing(mean_`2')
			if `r(N)' > 5 { //If there are less than 5 observations, we should not crosswalk

					local i 1 
						while `i' <= `dx_num' {
							regress mean_`dx' mean_``i'', noconstant //we want multiplicative crosswalk 
							local beta_``i'' = _b[mean_``i'']
							local se_beta_``i'' = _se[mean_``i'']
							local n_beta_``i'' = e(N)
							local ++ i  
							}
					

					use `crosswalks', clear 
						local i 1 
						while `i' <= `dx_num' {
							replace beta_``i'' = `beta_``i''' if cause == "`cause'" & diagcode == "`diagcode'"
							replace se_beta_``i'' = `se_beta_``i''' if cause == "`cause'" & diagcode == "`diagcode'"
							replace n_beta_``i'' = `n_beta_``i''' if cause == "`cause'" & diagcode == "`diagcode'"
							local ++ i  
							}
					save `crosswalks', replace 

					} //End condition on r(N)

			} //next cause 

		} //next diagcode 

save "FILEPATH", replace 
export excel "FILEPATH", firstrow(var) replace  


