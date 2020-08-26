clear all


import excel using "FILEPATH", firstrow clear

	
	gen diag_class = 1
	tempfile collapsed
	
	save `collapsed', replace

	*****************
	** S japonicum **
	*****************
	preserve
	keep if case_name == "S japonicum"
	rename cases cases_j
	rename sample_size sample_size_j
	rename case_diagnostics case_diagnostics_j
	duplicates tag nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure, gen(dup)
	*tab case_diagnostics_j dup
	*kk1, kk2 dups
	replace diag_class = 2 if dup == 1 & (case_diagnostics_j == "kk2")
	
	drop dup
	tempfile j_cases
	save `j_cases', replace
	*****************
	*** S mansoni ***
	*****************
	restore, preserve
	keep if case_name == "S mansoni"  
	rename cases cases_m
	rename sample_size sample_size_m
	rename case_diagnostics case_diagnostics_m
	duplicates tag nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure, gen(dup)
	replace diag_class = 2 if dup == 1 & (case_diagnostics_m == "fec")
	replace diag_class = 3 if dup == 1 | dup ==2 | dup==3 | dup==4 | dup==5 | dup==6 | dup==7 | dup==8 | dup==9 | dup==10 | dup==11 | dup==13 | dup==14 & (case_diagnostics_m == "kk1")
	replace diag_class = 4 if dup == 1 | dup == 2 |dup == 3 & (case_diagnostics_m == "kk2")
	replace diag_class = 5 if dup == 1 | dup == 2 |dup == 3 & (case_diagnostics_m == "kk3")
	replace diag_class = 6 if dup == 1 & (case_diagnostics_m == "pcr")
	replace diag_class = 7 if dup == 1 & (case_diagnostics_m == "sed")

	
	drop dup
	tempfile m_cases
	save `m_cases', replace
	********************
	** S intercalatum **
	********************
	restore, preserve
	keep if case_name == "S intercalatum"
	rename cases cases_i
	rename sample_size sample_size_i
	rename case_diagnostics case_diagnostics_i

	tempfile i_cases
	save `i_cases', replace
	*******************
	** S haematobium **
	*******************
	restore, preserve
	keep if case_name == "S haematobium"
	rename cases cases_h
	rename sample_size sample_size_h
	rename case_diagnostics case_diagnostics_h
	duplicates tag nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure, gen(dup)
	replace diag_class = 2 if dup == 1  & (case_diagnostics_h == "dip")
	replace diag_class = 3 if dup == 1  & (case_diagnostics_h == "filt")
	replace diag_class = 4 if dup == 1  & (case_diagnostics_h == "sed")
	replace diag_class = 5 if dup == 1 & (case_diagnostics_h == "pcr")

	drop dup
	tempfile h_cases
	save `h_cases', replace
	
	*****************
	*** S mekongi ***
	*****************
	
	restore, preserve
	keep if case_name == "S mekongi"
	rename cases cases_k
	rename sample_size sample_size_k
	rename case_diagnostics case_diagnostics_k
	*drop dup
	tempfile k_cases
	save `k_cases', replace



	
	
	restore


	
	
	** merge all the species together
	** start with haematobium
	use `h_cases', clear
	** add mansoni
	merge m:m nid underlying_nid diag_class field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure using `m_cases', nogen
	** add japonicum
	merge m:m nid underlying_nid diag_class field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure using `j_cases', nogen
	** add intercalatum
	merge m:m nid underlying_nid diag_class field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure using `i_cases', nogen
	** add mekongi
	merge m:m nid underlying_nid diag_class field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure using `k_cases', nogen

	
	** save file
	tempfile merged_species
	save `merged_species', replace


	export delimited "FILEPATH", replace
	
	
	duplicates tag nid underlying_nid diag_class location_name ihme_loc_id location_id field_citation_value sex year_start year_end age_start age_end measure case_diagnostics*, gen (keep)
	keep if keep==0
	
	**************************************************************************************************************************
	******create draws where necessary, and output prevalence with upper and lower confidence intervals for co-infection******
	**************************************************************************************************************************
		
	** keep data where two species are measured (only occurs for haematobium and mansoni or intercalatum)
	preserve
	keep if cases_h != . & (cases_m != . | cases_i != .)

	foreach var in m i h {
		** create offset for 0s and 100s
		replace cases_`var' = cases_`var' + 1e-4 if cases_`var' == 0
		replace cases_`var' = cases_`var' - 1e-4 if cases_`var' == sample_size_`var'
		** get draws using gamma distribution
		forvalues i = 0/999 {
			qui gen gammaA = rgamma((cases_`var'), 1)
			qui gen gammaB = rgamma((sample_size_`var' - cases_`var'), 1)
			qui gen mean_`var'_`i' = (gammaA / (gammaA + gammaB))
			replace mean_`var'_`i' = 0 if mean_`var'_`i' == .
			drop gamma*
		}
	}

	** assume a normal binomial distribution to get coinfection
	forvalues i = 0/999 {
		gen mean_hm_`i' = mean_h_`i' + mean_m_`i' - (mean_h_`i')*(mean_m_`i')
		gen mean_hi_`i' = mean_h_`i' + mean_i_`i' - (mean_h_`i')*(mean_i_`i')
		drop mean_h_`i' mean_i_`i' mean_m_`i'
	}

	egen mean_hi = rowmean(mean_hi_*)
	egen lower_hi = rowpctile(mean_hi_*), p(2.5)
	egen upper_hi = rowpctile(mean_hi_*), p(97.5)
	egen meanco = rowmean(mean_hm_*)
	egen lowerco = rowpctile(mean_hm_*), p(2.5)
	egen upperco = rowpctile(mean_hm_*), p(97.5)
	replace meanco = mean_hi if mean_hi > mean
	replace lowerco = lower_hi if mean_hi > mean
	replace upperco = upper_hi if mean_hi > mean
	drop mean_* lower_* upper_*

	tempfile all_schisto
	save `all_schisto', replace
	restore

	merge 1:1 nid underlying_nid diag_class location_name ihme_loc_id location_id field_citation_value sex year_start year_end age_start age_end measure case_diagnostics* using `all_schisto', nogen keep(1 3)
	
	
	
	
	gen sample_size = .
	gen cases = .

	
	foreach type in h i j k m {
		replace sample_size = sample_size_`type' if sample_size_`type' != . & meanco == .
		replace cases = cases_`type' if cases_`type' != . & meanco == .	
	}

	sort location_name nid year_start year_end age_start

	egen case_diagnostics = concat(case_diagnostics_i case_diagnostics_h case_diagnostics_m case_diagnostics_j), punct(" ")
	replace case_diagnostics = subinstr(case_diagnostics, "  ", " ", .)
	drop case_diagnostics_* cases_* sample_size_*
	
	
	
	replace mean=meanco if meanco!=. 
	replace lower=lowerco if lowerco!=.
	replace upper=upperco if upperco!=.
	replace standard_error= (upper-lower)/3.92 if standard_error==. & lowerco!=. & upperco!=. & meanco!=.
		

drop if cases>sample_size 

	
replace upper = (mean + (1.96*standard_error)) if upper==.
replace lower = (mean - (1.96*standard_error)) if lower==.

replace sample_size = (mean*(1-mean)/standard_error^2) if cases==. & sample_size==. & measure=="prevalence" 
replace cases = mean * sample_size if cases==. 



drop if cases<1

drop if sample_size<1
*0 obs dropped


*case diagnostics missing for mekongi
replace case_diagnostics="kk1" if case_diagnostics=="" & case_name=="S mekongi"

*setting neg lower CI and >1 upper CI to 0 and 1
replace lower=0 if lower<0
replace upper=1 if upper>1


drop if group_review==0

*only keep data post 1980 
keep if year_start>=1980


replace unit_value_as_published = 1 if  unit_value_as_published==.
replace group = 900 if specificity== "; Sex split using ratio from MR-BRT" & group==.
replace group_review = 1 if specificity!="" & group!=.
replace uncertainty_type_value = 95 if mean != .
replace uncertainty_type = "Standard error" if mean !=. 
gen effective_sample_size = sample_size



drop if mean<0.10


drop if mean>0.75 & ihme_loc_id=="BRA_4760"
drop if mean>0.75 & ihme_loc_id=="BRA_4766"
drop if mean>0.75 & ihme_loc_id=="CHN_504"
drop if mean>0.75 & ihme_loc_id=="CHN_507"
drop if age_end<=20 & mean>0.3

export excel "FILEPATH", replace sheet("extraction") firstrow(var)

