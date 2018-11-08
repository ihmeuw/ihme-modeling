********************************************************
** Author: 
** Date created: August 10, 2009
** Description:
** Formats population and deaths data.
**
**
**
** NOTE: IHME OWNS THE COPYRIGHT
********************************************************

** **********************
** Set up Stata 
** **********************

	clear all
	capture cleartmp 
	set mem 500m
	set more off

** **********************
** Filepaths 
** **********************

	if (c(os)=="Unix") global root "/home/j"	
	if (c(os)=="Windows") global root "J:"
	local version_id `1'
	global main_dir "FILEPATH"
	global ddm_file "FILEPATH/d04_raw_ddm.dta"
	global save_file "FILEPATH/d05_formatted_ddm.dta"


** **********************
** Apply age trims dependent upon the age categories present
** **********************	

	use "$ddm_file", clear

** Find the open age interval 
	forvalues j = 0/100 {
		qui lookfor agegroup`j'
		qui return list
		if("`r(varlist)'" ~= "") {
			local maxnum = `j'
		}
	}
	local mplus = `maxnum'+1
	local maxnumminus = `maxnum'-1

	g openend = .
	forvalues j = 0/`maxnumminus' {
		local jplus = `j'+1
		replace openend = `j' if vr_`jplus' == . & openend == .
	}
	replace openend = `maxnum' if openend == .


	g atu = openend - 1 if openend <= 17
	g atl = atu - 5 if openend <= 17 & openend >= 7
	replace atl = 4 if openend == 6
	replace atl = 3 if openend == 5 
	replace atl = 2 if openend == 4
	replace atl = 1 if openend == 3 
	replace atu = . if openend <= 2
	replace atl = . if openend <= 2

	replace atu = 16 if openend > 17 
	replace atl = 11 if openend > 17


	g orthogregslope_ggb = .
	g seg_avgcompleteness_seg = .
	g ggbseg_avgcompleteness_ggbseg = . 
	g completenessc1toc2_adj = .

	forvalues w = 1/`maxnumminus' {
		local trim_upper = `maxnum'-`w'
		local upper = `trim_upper'-4
		forvalues trim_lower = 1/`upper' {
			replace orthogregslope_ggb = orthogregslope_`trim_lower'to`trim_upper' if (atl-3) == `trim_lower' & (atu-2) == `trim_upper'
			replace seg_avgcompleteness_seg = seg_avgcompleteness_`trim_lower'to`trim_upper' if atl == `trim_lower' & atu == `trim_upper'
			replace ggbseg_avgcompleteness_ggbseg = ggbseg_avgcompleteness_`trim_lower'to`trim_upper' if (atl-1) == `trim_lower' & (atu-2) == `trim_upper'
			replace completenessc1toc2_adj = completenessc1toc2_`trim_lower'to`trim_upper' if (atl+1) == `trim_lower' & atu == `trim_upper'
		}
	}

	g ggb = 1/orthogregslope_ggb
	g seg = seg_avgcompleteness_seg
	g ggbseg = (ggbseg_avgcompleteness_ggbseg*sqrt(1/completenessc1toc2_adj))
	
	tempfile ddm_data
	save `ddm_data', replace

	keep if sex == "both" & mi(ggb)
	keep ihme_loc_id source_type pop_years pop_source openend atl atu agegroup* orthogregslope_*
	export delim using "FILEPATH/missing_ggb.csv", replace

	use `ddm_data', clear
	keep if sex == "both" & mi(seg)
	keep ihme_loc_id source_type pop_years pop_source openend atl atu agegroup* seg_avgcompleteness_*
	export delim using "FILEPATH/missing_seg.csv", replace

	use `ddm_data', clear

	preserve
	gen keep_it = 0
	foreach var of varlist ggb seg ggbseg { 
		replace keep_it = 1 if `var' < 0 | `var' > 5
	}
	keep if keep_it == 1
	export delim using "FILEPATH/dropped_method_estimates.csv", replace
	restore

	foreach var of varlist ggb seg ggbseg { 
		replace `var' = . if `var' < 0 | `var' > 5
	}

	drop if ggb == . & seg == . & ggbseg == .
	keep ihme_loc_id country sex pop_years pop_source pop_footnote pop_nid underlying_pop_nid deaths_years deaths_source deaths_footnote deaths_nid deaths_underlying_nid source_type ggb seg ggbseg completenessc1toc2_adj 

	preserve
	save "FILEPATH/d05_with_census_completeness.dta", replace
	restore

	drop completenessc1toc2_adj

	save "$save_file", replace
