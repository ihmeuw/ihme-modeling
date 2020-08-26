
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
	local version_id `1'
	global main_dir FILEPATH
	global ddm_file FILEPATH
	global save_file FILEPATH
	global methods_rank_file FILEPATH

** **********************
** Apply age trims dependent upon the age categories present
** **********************

** format ranking of age-trim-lower and age-trim-upper by method (ggb, seg, ggbseg) for merge
	import delimited "$methods_rank_file", clear
	keep method rank start_age_trim end_age_trim
	keep if inlist(method, "ggb", "seg", "ggbseg")
	rename start_age_trim atl
	rename end_age_trim atu
	bysort method (rank) : replace rank = _n

	** calculate the maximum rank value
	preserve
	summarize rank
	local maxrank = r(max)
	restore

	** reshape wide
	gen i = 1
	reshape wide atl atu, i(i method) j(rank)
	reshape wide atl* atu*, i(i) j(method) string
	tempfile rankings
	save `rankings', replace

** open ddm file and merge on rankings
	use "$ddm_file", clear
	gen i = 1
	merge m:1 i using `rankings', nogen
	drop i

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
		replace openend = agegroup`j' if vr_`jplus' == . & openend == .
	}
	replace openend = agegroup`maxnum' if openend == . // Grabs the agegroup # that corresponds to the ____+ age group

** loop over ranking and method, select best possible atl and atu
	foreach method in "ggb" "seg" "ggbseg" {

		gen atl_`method' = .
		gen atu_`method' = .
		gen selected = 0
		local rank = 1

		forvalues rank = 1/`maxrank' {
			** replace atl and atu with the atl and atu for this rank if possible
			if "`method'" == "ggbseg" {
				replace atl_`method' = atl`rank'`method' if selected == 0 & atu`rank'`method' < openend & atl`rank'`method' >= agegroup1
				replace atu_`method' = atu`rank'`method' if selected == 0 & atu`rank'`method' < openend & atl`rank'`method' >= agegroup1
			} 
			else {
				replace atl_`method' = atl`rank'`method' if selected == 0 & atu`rank'`method' <= openend & atl`rank'`method' >= agegroup1
				replace atu_`method' = atu`rank'`method' if selected == 0 & atu`rank'`method' <= openend & atl`rank'`method' >= agegroup1
			}
			** change selected to 1 if we picked this rank, so that we won't pick a greater rank later.
			replace selected = 1 if !missing(atl_`method') & !missing(atu_`method')
		}
		drop selected

		** replace age number with age group number
		gen atl_`method'_temp = .
		gen atu_`method'_temp = .
		forvalues a = 0/19 {
			replace atl_`method'_temp = `a' if atl_`method'>=agegroup`a'
			replace atu_`method'_temp = `a' if atu_`method'>=agegroup`a'
		}
		replace atl_`method' = atl_`method'_temp
		replace atu_`method' = atu_`method'_temp
		drop atl_`method'_temp atu_`method'_temp
	}

	g orthogregslope_ggb = .
	g seg_avgcompleteness_seg = .
	g ggbseg_avgcompleteness_ggbseg = . 
	g completenessc1toc2_adj = .

	forvalues trim_lower = 1/`maxnumminus' {
		local first_trim_upper = `trim_lower' + 1
		forvalues trim_upper = `first_trim_upper'/`maxnumminus' {
			if (`trim_upper' - `trim_lower' >= 1) {
				replace orthogregslope_ggb = orthogregslope_`trim_lower'to`trim_upper' if atl_ggb == `trim_lower' & atu_ggb == `trim_upper'
				replace seg_avgcompleteness_seg = seg_avgcompleteness_`trim_lower'to`trim_upper' if atl_seg == `trim_lower' & atu_seg == `trim_upper'
				cap replace ggbseg_avgcompleteness_ggbseg = ggbseg_avgcompleteness_`trim_lower'to`trim_upper' if atl_ggbseg == `trim_lower' & atu_ggbseg == `trim_upper'
				replace completenessc1toc2_adj = completenessc1toc2_`trim_lower'to`trim_upper' if atl_ggbseg == `trim_lower' & atu_ggbseg == `trim_upper'
				}
		}
	}

	g ggb = 1/orthogregslope_ggb
	g seg = seg_avgcompleteness_seg
	g ggbseg = ggbseg_avgcompleteness_ggbseg
	
	tempfile ddm_data
	save `ddm_data', replace

	keep ihme_loc_id source_type pop_years pop_source openend atl* atu* agegroup*
	save FILEPATH, replace

	use `ddm_data', clear
	keep if sex == "both" & mi(ggb)
	keep ihme_loc_id source_type pop_years pop_source openend atl* atu* agegroup* orthogregslope_*
	save FILEPATH, replace

	use `ddm_data', clear
	keep if sex == "both" & mi(seg)
	keep ihme_loc_id source_type pop_years pop_source openend atl* atu* agegroup* seg_avgcompleteness_*
	save FILEPATH, replace

	use `ddm_data', clear

	preserve
	gen keep_it = 0
	foreach var of varlist ggb seg ggbseg { 
		replace keep_it = 1 if `var' < 0 | `var' > 5
	}
	keep if keep_it == 1
	export delim using FILEPATH, replace
	restore

	foreach var of varlist ggb seg ggbseg { 
		replace `var' = . if `var' < 0 | `var' > 5
	}

	drop if ggb == . & seg == . & ggbseg == .
	keep ihme_loc_id country sex pop_years pop_source pop_footnote pop_nid underlying_pop_nid deaths_years deaths_source deaths_footnote deaths_nid deaths_underlying_nid source_type ggb seg ggbseg completenessc1toc2_adj 

	preserve
	save FILEPATH, replace
	restore

	drop completenessc1toc2_adj

	save FILEPATH, replace
