********************************************************
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

	if (c(os)=="Unix") global root "FILEPATH"	
	if (c(os)=="Windows") global root "FILEPATH"
	global ddm_file "FILEPATH/d04_raw_ddm.dta"
	global save_file "FILEPATH/d05_formatted_ddm.dta"


** **********************
** Apply age trims dependent upon the age categories present
** **********************	

	use "$ddm_file", clear

** Find the open age interval 
	forvalues j = 0/100 {
		qui looUSER agegroup`j'
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
	replace openend = `maxnum' if openend == . // Grabs the agegroup # that corresponds to the ____+ age group

** This code is getting the age trims to use, but it depends on the terminal age group and on the granularity of the age groups
** That means that it doesn't necessarily choose the optimal age trim as validated in the paper in PLOS, just sometimes. Other times, it chooses one close to optimal
	g atu = openend - 1 if openend <= 17 // Upper trim should be one less than the maximum value
	g atl = atu - 5 if openend <= 17 & openend >= 7 // Lower trim should be five age groups less than upper, unless one of the cases below
	replace atl = 4 if openend == 6 // This sets lower trim at one age group below the upper trim if the open-ended interval is too low
	replace atl = 3 if openend == 5 
	replace atl = 2 if openend == 4
	replace atl = 1 if openend == 3 
	replace atu = . if openend <= 2
	replace atl = . if openend <= 2

	replace atu = 16 if openend > 17 // Take age groups 11 as lower trim and 16 as upper trim, if more than 17 age groups
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
	
	foreach var of varlist ggb seg ggbseg { 
		replace `var' = . if `var' < 0 | `var' > 5
	}

	drop if ggb == . & seg == . & ggbseg == .
	keep ihme_loc_id country sex pop_years pop_source pop_footnote pop_nid deaths_years deaths_source deaths_footnote deaths_nid source_type ggb seg ggbseg  
	save "$save_file", replace
