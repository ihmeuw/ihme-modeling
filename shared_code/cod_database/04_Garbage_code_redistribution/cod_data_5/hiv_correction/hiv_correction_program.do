// Purpose: Apply HIV corrections for VR data in select countries

** ******************
// Files and settings
** ******************
	// Prefix
	set more off
	if c(os) == "Unix" {
		global prefix "/home/j/"
		do "$prefix/Usable/Tools/ADO/pdfmaker.do"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
		do "$prefix/Usable/Tools/ADO/pdfmaker_Acrobat11.do"
	}
	
	if lower("`1'") == "test" {
		local step "`2'"
		local dataset "`3'"
		if "`step'" == "PRE" {
			use "/ihme/cod/prep/01_database/03_datasets/`dataset'/data/intermediate/03_corrected_restrictions.dta", clear
		}
		else if "`step'" == "POST" {
			use "/ihme/cod/prep/01_database/03_datasets/`dataset'/data/final/05b_for_compilation.dta", clear
		}
	}
	else {
		local step "`1'"
	}
	
	// Mata function to increase collapse speed
	quietly do "$prefix/WORK/10_gbd/00_library/functions/fastcollapse.ado"
	
	// Correction graphs
	global graph_dir "$prefix/WORK/03_cod/01_database/02_programs/hiv_correction/reallocation_program/outputs/`step'/$source"
	!rm -rf "$graph_dir"
	capture mkdir "$graph_dir"
	
	// Inputs
	global in_dir "/ihme/cod/prep/01_database/02_programs/hiv_correction/reallocation_program/inputs/"
	
	// Define cause group
	if "`step'" == "PRE" local cause_group "garbage"
	else if "`step'" == "POST" local cause_group "non_garbage"
	
	// Identify source
	levelsof source, local(src) c
		
	// Graphs on/off
	local graph_set 0
	if inlist("$source","Russia_FMD_1989_1998","Russia_FMD_1999_2011","Russia_ROSSTAT","MOZ_MoH_urban") local graph_set 1
	
** ******************
// Define the age globals
** ******************
		// Terminal age group
		global maxage = 80
		
		// Terminal age group TO CORRECT (past this age, we will just leave the cause composition of deaths as-is)
		global maxcorrect = 70
		
		// Reference age groups
		global all_ref 65 70 75
		global tb_ref 55 60 65
		global epilepsy_ref 60 65
		global perinatal_ref 0 1
		global maternal_ref 35 40 45
		
		// Set age restrictions for each country in which we move garbage
		** South Africa
		local ZAF_age_min = 15
		local ZAF_age_max = 55
		** Zimbabwe
		local ZWE_age_min = 10
		local ZWE_age_max = 45
		** Mozambique
		local MOZ_age_min = 15
		local MOZ_age_max = 45
	
** ******************
// Identify country-specific cause lists
** ******************
	// Identify non-injury garbage
	if "`cause_group'" == "garbage" {
		gen inj_gar = .
		** ICD8_detail **
		foreach t in E 8 9 {
			replace acause = "_inj_gc" if strmatch(cause,"`t'*") & list == "ICD8_detail"
		}
		** ICD8A **
		replace acause = "_inj_gc" if strmatch(cause,"A14*") & list == "ICD8A"
		** ICD9_detail
		foreach t in E 8 9 {
			replace acause = "_inj_gc" if strmatch(cause,"`t'*") & acause == "_gc" & list == "ICD9_detail"
		}
		** ICD9_BTL
		foreach t in B47 B48 B49 B50 B51 B52 B53 B54 B55 B56 {
			replace acause = "_inj_gc" if substr(cause,1,3) == "`t'"  & acause == "_gc" & list == "ICD9_BTL"
		}
		foreach t in E 8 9 {
			replace acause = "_inj_gc" if substr(cause,1,12) == "acause__gc_`t'"  & acause == "_gc" & list == "ICD9_BTL"
		}
		** ICD9_USSR_Tabulation
		foreach t in B47 B48 B49 B50 B51 B52 B53 B54 B55 B56 {
			replace acause = "_inj_gc" if substr(cause,1,3) == "`t'" & acause == "_gc" & list == "ICD9_USSR_Tabulation"
		}
		foreach t in E 8 9 {
			replace acause = "_inj_gc" if substr(cause,1,12) == "acause__gc_`t'" & acause == "_gc" & list == "ICD9_USSR_Tabulation"
		}
		** ICD10
		foreach t in V W X Y {
			replace acause = "_inj_gc" if substr(cause,1,1) == "`t'" & acause == "_gc" & list == "ICD10"
		}
		** ICD10_tabulated
		foreach t in V W X Y {
			replace acause = "_inj_gc" if substr(cause,1,12) == "acause__gc_`t'" & acause == "_gc" & list == "ICD10_tabulated"
		}
		replace acause = "_inj_gc" if strmatch(cause,"11*") & acause == "_gc" & list == "ICD10_tabulated"
	}
// SIDE JOBS...
	preserve
	** ******************
	// Identify start year for HIV correction
	** ******************
		use "${in_dir}/Master_Cause_Selections.dta", clear
		** Pre-redist
		if "`cause_group'" == "garbage" drop if cause != "_gc"
		** Post-redist
		if "`cause_group'" == "non_garbage" drop if cause == "_gc"
		** Produce cause lists
		replace iso3 = trim(iso3)
		levelsof iso3, local(cntry_list)
		foreach cntry of local cntry_list {
			levelsof cause if iso3 == "`cntry'", local(`cntry'_sources) c
		}
		duplicates drop
		** No longer taking garbage in ZAF & THA & GEO in BTL
		if "`cause_group'" == "garbage" {
			if "`src'" == "ICD9_BTL" drop if (iso3 == "ZAF" | iso3 == "THA" | iso3 == "GEO")
		}
		levelsof iso3, local(iso3s)
		
	** ******************
	// Identify start year for HIV correction
	** ******************
		** BELOW IS ORIGIN OF GIT-TRACKED FILE: UNAIDS estimates from 2013
		** Use epidemic start year found in UNAIDS 2013 models
		import delimited using "$h/cod-data/01_inputs/hiv_correction/reallocation_program/UNAIDS2013_HIV_mort_start_years.csv", clear
		levelsof iso3, local(HIV_isos)
		foreach HIV_iso of local HIV_isos {
			levelsof start_year if iso3 == "`HIV_iso'", local(`HIV_iso'_year)
		}
		** There are no 2013 estimates for Russia and Brazil, so will use UNAIDS 2012
		local BRA_year 1972
		local KAZ_year 1982
		** Use GBD2010 for remainder that have no UNAIDS estimates
		local CHN_year 1982
		local DZA_year 1986
		local JOR_year 1980
		local LCA_year 1980
		local MNE_year 1980
		local MNG_year 1980
		local OMN_year 1980
		local VCT_year 1980
		** Set Soviet Union countries all to 1985
		foreach former_soviet in ARM AZE BLR EST GEO KAZ KGZ LTU LVA MDA RUS TJK TKM UKR UZB {
			local `former_soviet'_year 1985
		}
		levelsof iso3, local(start_isos)
		foreach start_iso of local start_isos {
			levelsof start_year if iso3 == "`start_iso'", local(`start_iso'_year)
		}
		
	** ******************
	// Prep population inputs	
	** ******************
		do "$h/cod-data/02_programs/prep/code/env_wide.do"
		replace location_id = 0 if location_id == .
		tempfile pop
		save `pop', replace
	restore
	
** ******************
// Prep input data	
** ******************
	// Preserve unadjusted
		replace location_id = 0 if location_id == .
		tempfile before
		save `before', replace
		fastcollapse deaths*, by(iso3 location_id subdiv year sex source* NID national region dev_status list acause) type(sum)

	// Merge with pop
		sort iso3 year sex
		merge m:1 iso3 location_id year sex using `pop', keep(1 3) keepusing(pop*) nogen
		
	// Rename deaths variables to reflect age groupings; use the underscore so that the old and new names don't overlap
		foreach var in deaths1 deaths2 deaths4 deaths5 deaths6 deaths24 deaths25 deaths26 deaths91 deaths92 deaths93 pop_source pop91 pop93 {
			capture drop `var'
		}
		rename deaths94 deaths_0
		rename deaths3 deaths_1
		rename pop94 pop_0
		rename pop3 pop_1
		forvalues i= 7/22  {
			local j = (`i'-6) * 5
			rename deaths`i' deaths_`j'
			rename pop`i' pop_`j'
		}

	// Save for later
	tempfile master
	save `master', replace
	
** ******************
// Loop through countries and apply corrections
** ******************
	local used_isos = ""
	quietly {
	foreach iso3 of local iso3s {
		noisily di "***************"
		noisily di "CORRECTING `iso3'"
		noisily di "***************"
		// Start each country with with original data
		use `master', clear
	
		** ** ** ** ** ** ** ** ** ** ** ** ** **
		// Check for country-years, skip if not present
		count if iso3 == "`iso3'" & year >= ``iso3'_year' & strmatch(source_type,"VR*")
		if `r(N)' == 0 {
			noisily di "----> no usable `iso3' data in `src' (start year: ``iso3'_year') <----"
			continue
		}
		** ** ** ** ** ** ** ** ** ** ** ** ** **
		// Add to list of corrected countries
		local used_isos "`used_isos'`iso3' "
		
		// Make directory for graphs
		capture mkdir "$graph_dir/`iso3'"

		// Allow for subnational 
		levelsof location_id if iso3 == "`iso3'" & year >= ``iso3'_year', local(loc_ids)
		foreach loc_id of local loc_ids {
			noisily di in red "  `iso3' - `loc_id'"
			** ******************
			//  Compute cause-specific death rates relative to a reference for country of analysis and global population
			** ******************
				noisily di "	Computing reference death rates"
				// Start each location ID with original data
					use `master', clear
				// Country: Pool over years
					keep if iso3 == "`iso3'" & location_id == `loc_id' & year >= ``iso3'_year' & strmatch(source_type,"VR*")
					**
					if "`cause_group'" == "garbage" keep if acause == "_gc"
					**
					fastcollapse deaths_* pop_*, by(iso3 sex acause) type(sum)
					tempfile cntry
					save `cntry', replace
					
				// Compute reference death rates
					// General reference
						local j = 1
						foreach i of global all_ref {
							gen double rate`j' = deaths_`i'/pop_`i' if substr(acause,1,8) != "maternal" & substr(acause,1,8) != "neonatal"
							local ++j
						}
					// Tuberculosis
						local j = 1
						foreach i of global tb_ref {
							replace rate`j' = deaths_`i'/pop_`i' if acause == "tb"
							local ++j
						}
					// Epilepsy
						local j = 1
						foreach i of global epilepsy_ref {
							replace rate`j' = deaths_`i'/pop_`i' if acause == "neuro_epilepsy"
							local ++j
						}
					// Perinatal
						local j = 1
						foreach i of global perinatal_ref {
							capture gen rate`j' = deaths_`i'/pop_`i' if substr(acause,1,8) == "neonatal"
							if _rc replace rate`j' = deaths_`i'/pop_`i' if substr(acause,1,8) == "neonatal"
							local ++j
						}
					// Maternal
						local j = 1
						foreach i of global maternal_ref {
							capture gen rate`j' = deaths_`i'/pop_`i' if substr(acause,1,8) == "maternal"
							if _rc replace rate`j' = deaths_`i'/pop_`i' if substr(acause,1,8) == "maternal"
							local ++j
						}
					
				// Average to get the baseline reference and compute relative death rates
					egen baserate = rowmean(rate*)
					foreach i of numlist 0 1 5(5)$maxage {
						gen rrate`i' = (deaths_`i'/pop_`i') / baserate
					}
					keep iso3 sex acause rrate* baserate
			
			** ******************
			// Flag cause-sex-age groups for which we will remove deaths and assign to HIV
			** ******************	
				noisily di "	Flagging ages/sexes that exceed reference"
				// Reshape to have country-global wide	
					reshape long rrate, i(acause sex iso3 baserate) j(agecat)
					reshape wide rrate baserate, i(acause sex agecat) j(iso3) string
				
				// Merge on global relative rate
					merge 1:1 acause sex agecat using "${in_dir}/`step'_global_rates/active/rates_`iso3'.dta", keep(1 3) nogen
				
				// Some causes are not present in reference data (first confirm not corrected causes)
					foreach check_cause of local `iso3'_sources {
						count if acause == "`check_cause'" & rrateWLD == .
						if `r(N)' > 0 {
							di "`check_cause' set for correction in `iso3', but not found in source!!!"
						}
					}
					replace rrateWLD = rrate`iso3' if rrateWLD == .

				// Gen an indicator for those cause-age groups that we will take deaths from
					gen flag = 0
					foreach cod of local `iso3'_sources {
						replace flag = 1 if acause == "`cod'" & rrate`iso3' > rrateWLD
					}
					replace flag = 0 if "`iso3'" == "GTM" & acause == "endo_other" & agecat < 5
					if "`cause_group'" == "garbage" replace flag = 0 if agecat < ``iso3'_age_min' | agecat > ``iso3'_age_max'
				
				// Save for later
					sort acause sex agecat
					tempfile rates
					save `rates', replace
					
			** ******************
			// Prep the data for correction
			** ******************	
				noisily di "	Prepping the data for correction"
				use `master', clear
				keep if iso3 == "`iso3'" & location_id == `loc_id' & year >= ``iso3'_year' & strmatch(source_type,"VR*")
				**
				if "`cause_group'" == "garbage" keep if acause == "_gc"
				**
				fastcollapse deaths_*, by(pop_* iso3 location_id subdiv year sex acause source* NID list national region dev_status) type(sum)
				
				// Compute reference death rates
					// General reference
						local j = 1
						foreach i of global all_ref {
							gen double rate`j' = deaths_`i'/pop_`i' if substr(acause,1,8) != "maternal" & substr(acause,1,8) != "neonatal"
							local ++j
						}
					// Tuberculosis
						local j = 1
						foreach i of global tb_ref {
							replace rate`j' = deaths_`i'/pop_`i' if acause == "tb"
							local ++j
						}
					// Epilepsy
						local j = 1
						foreach i of global epilepsy_ref {
							replace rate`j' = deaths_`i'/pop_`i' if acause == "neuro_epilepsy"
							local ++j
						}
					// Perinatal
						local j = 1
						foreach i of global perinatal_ref {
							capture gen rate`j' = deaths_`i'/pop_`i' if substr(acause,1,8) == "neonatal"
							if _rc replace rate`j' = deaths_`i'/pop_`i' if substr(acause,1,8) == "neonatal"
							local ++j
						}
					// Maternal
						local j = 1
						foreach i of global maternal_ref {
							capture gen rate`j' = deaths_`i'/pop_`i' if substr(acause,1,8) == "maternal"
							if _rc replace rate`j' = deaths_`i'/pop_`i' if substr(acause,1,8) == "maternal"
							local ++j
						}
				
				// Average to get a baseline
					egen double baserate = rowmean(rate*)
				
				// Reshape long
					drop rate*
					reshape long deaths_ pop_, i(iso3 year sex acause source* NID) j(agecat)
					ren deaths_ deaths
					ren pop_ pop
				
				// Merge in World data
					sort acause sex agecat
					merge m:1 acause sex agecat using "`rates'", keep(1 3) nogen
			
			** ******************
			// Compute the excess deaths in source causes: "Positive Excess" method
			** ******************	
				noisily di "	Computing excess deaths"
				// Start with the original death count
				gen double deathsPE = deaths
						
				// Compute expected deaths if flag==1
				replace deathsPE = baserate*pop*rrateWLD if flag==1 & agecat < $maxcorrect & year >= ``iso3'_year'

				// Compute the excess
				gen double addtoHIV_PE = deaths - deathsPE if flag==1 & agecat < $maxcorrect & year >= ``iso3'_year'

				// If it's negative, return the death counts to what they were, and reset the excess to zero
				capture label drop _all
				replace deathsPE = deaths if addtoHIV_PE <0
				replace addtoHIV_PE = 0 if addtoHIV_PE <0 | addtoHIV_PE == .
								
				// Adjust former soviet countries to move a TB deaths according to a 5% proportion fit to 
				// the age-pattern of HIV in the raw data
				capture drop _merge
				merge m:1 iso3 acause agecat sex using "${in_dir}/USSR_TB_HIV_proportions.dta", keep(1 3)
				replace addtoHIV_PE = (deaths*tb_hiv_prop) if _merge == 3
				replace deathsPE = (deaths*(1-tb_hiv_prop)) if _merge == 3
				drop tb_hiv_prop _merge
				
				// Leave tuberculosis over age 60
				replace deathsPE = deaths if agecat > 60 & acause == "tb"
				replace addtoHIV_PE = 0 if agecat > 60 & acause == "tb"
				
				if `graph_set' == 1 {
				// Store list and make stacked bar graph
				summ addtoHIV_PE
				if `r(max)' > 0 {
					preserve
						keep if ((addtoHIV_PE > 0 & addtoHIV_PE != .) | index(acause,"hiv")) & agecat < $maxcorrect
						levelsof acause, local(acause_`iso3') c
						drop if index(acause,"hiv")
						keep addtoHIV_PE iso3 year agecat sex acause
						levelsof year if addtoHIV_PE > 0, local(yrs)
						levelsof sex if addtoHIV_PE > 0, local(sxs)
						tempfile graph_orig
						save `graph_orig', replace
						keep acause
						duplicates drop
						gen sort = _n
						tempfile cause_ids
						save `cause_ids'
						local orders ""
						local labels ""
						pdfstart using "$graph_dir/`iso3'/HIV_add_cause_composition_`iso3'_`loc_id'_`src'.pdf"
						foreach yr of local yrs {
							foreach sx of local sxs {
								use `graph_orig', clear
								count if year == `yr' & sex == `sx'
								if `r(N)' > 0 {
									keep if year == `yr' & sex == `sx'
									keep acause
									levelsof acause, local(grp_cs) c
									duplicates drop
									gen sort = _n
									tempfile cause_ids
									save `cause_ids'
									use `graph_orig', clear
									keep if year == `yr' & sex == `sx'
									merge m:1 acause using `cause_ids', keep(1 3) nogen
									local orders ""
									local labels ""
									levelsof sort, local(sorts)
									foreach sort of local sorts {
										levelsof acause if sort == `sort', local(acause_srt) c
										local orders "`sort' `orders'"
										local labels `"`labels' label(`sort' "`acause_srt'")"' //"
									}
									keep age sort addtoHIV_PE
									reshape wide addtoHIV_PE, i(age) j(sort)
									foreach var of varlist addtoHIV_PE* {
										replace `var' = 0 if `var' == .
									}
									local sex1 = "Males"
									local sex2 = "Females"
									capture graph bar addtoHIV_PE*, over(age, label(angle(45) labsize(vsmall)) gap(0)) stack plotregion(lwidth(none) margin(zero)) graphregion(margin(small)) ///
									bar(1, color(cranberry)) bar(2, color(red)) bar(3, color(orange_red)) bar(4, color(orange)) bar(5, color(yellow)) bar(6, color(lime)) bar(7, color(green)) bar(8, color(forest_green)) bar(9, color(emerald)) bar(10, color(cyan)) bar(11, color(midblue)) bar(12, color(blue)) bar(13, color(navy)) bar(14, color(magenta)) bar(15, color(purple)) bar(16, color(olive)) bar(17, color(sienna)) bar(18, color(cranberry)) bar(19, color(red)) bar(20, color(orange_red)) bar(21, color(orange)) bar(22, color(yellow)) bar(23, color(lime)) bar(24, color(green)) bar(25, color(forest_green)) bar(26, color(emerald)) bar(27, color(cyan)) ///
									title("Proportion of HIV Addition by Cause, `yr', `sex`sx''""Causes: `grp_cs'", size(small)) ytitle("Deaths", size(vsmall)) ylabel(,labsize(vsmall) angle(0) format(%15.0fc)) ///
									legend(col(1) position(3) region(lwidth(none)) size(vsmall) symxsize(*.5) symysize(*.5) ///
									order(`orders') `labels')
									capture pdfappend
								}
							}
						}
						pdffinish
						capture erase "$graph_dir/`iso3'/HIV_add_cause_composition_`iso3'_`loc_id'_`src'.log"
					restore
				}
			}
			
			** ******************
			// Redistribute the excess deaths to HIV 
			** ******************
				noisily di "	Redistributing excess deaths"
				if "`cause_group'" == "non_garbage" {
					// Isolate the deaths to add to HIV
					keep iso3 location_id subdiv year sex agecat acause source* NID list national region dev_status deathsPE addtoHIV_PE
					preserve
						** Cause-specific assign to detail
						gen hiv_target = "hiv_other"
						replace hiv_target = "hiv_tb" if acause == "tb"
						fastcollapse addtoHIV_PE, by(iso3 location_id subdiv hiv_target year sex agecat source* NID list national region dev_status) type(sum)
						rename hiv_target acause
						sort iso3 location_id subdiv year sex agecat acause source* NID list national region dev_status
						tempfile add
						save `add', replace
					restore
				}
				
				if "`cause_group'" == "garbage" {
					// Isolate the deaths to add to HIV
					keep iso3 location_id subdiv year sex agecat acause source* NID list national region dev_status deaths* addtoHIV_PE
					preserve
						fastcollapse addtoHIV_PE, by(iso3 location_id subdiv year sex agecat source* NID list national region dev_status) type(sum)
						gen acause = "hiv"
						sort iso3 location_id subdiv year sex agecat acause source* NID list national region dev_status
						tempfile add
						save `add', replace
					restore
				}
				
				// Merge these back onto the data
				drop addtoHIV_PE
				sort iso3 location_id subdiv year sex agecat acause source* NID list national region dev_status
				merge 1:1 iso3 location_id subdiv year sex agecat acause source* NID list national region dev_status using `add', nogen
				
				if "`cause_group'" == "non_garbage" {
					// This is at acause level, so no need to proportionally reallocate
					egen double new_HIV = rowtotal(deathsPE addtoHIV_PE)
					replace deathsPE = new_HIV if index(acause,"hiv")
					drop addtoHIV_PE new_HIV
					
					// make age wide
					reshape wide deathsPE, i(iso3 year sex acause source* NID) j(agecat)
					rename deathsPE0 deaths_new94
					rename deathsPE1 deaths_new3
					forvalues i = 5(5)$maxage {
						local j = (`i'/5) + 6
						rename deathsPE`i' deaths_new`j'
					}
					
					// store the proportions
					tempfile props
					save `props', replace
					
					// load the before correction data
					use if iso3=="`iso3'" & location_id == `loc_id' & year >= ``iso3'_year' & strmatch(source_type,"VR*") using `before', clear
					drop orig* beforeafter
				
					// merge on the proportions
					merge m:1 iso3 location_id subdiv year sex acause source* NID list national region dev_status using `props', nogen
				}
				
				if "`cause_group'" == "garbage" {
					// This step is before redistribution, so we need to compute proportions at the acause level, then go back to the ICD level to actually reallocate deaths
					// now that this is multiplicative and not additive we have to add an arbitrary small amount (same as above) to anything that is zero, then subtract it back
					gen flag = 1 if (deaths == 0 | deaths == .) & (addtoHIV_PE!=0 | deathsPE!=0)
					gen double allocation_proportion = deathsPE/deaths
					replace allocation_proportion = addtoHIV_PE if acause == "hiv"
					keep iso3 location_id subdiv year sex age acause source* NID list national region dev_status allocation_proportion
					
					// make age wide
					reshape wide allocation_proportion, i(iso3 location_id subdiv year sex acause source* NID) j(agecat)
					rename allocation_proportion0 allocation_proportion94
					rename allocation_proportion1 allocation_proportion3
					forvalues i = 5(5)$maxage {
						local j = (`i'/5) + 6
						rename allocation_proportion`i' allocation_proportion`j'
					}
					
					// store the proportions (as frmat 2)
					gen frmat = 2
					gen im_frmat = 2
					tempfile props
					save `props', replace
					
					// load the before correction data
					use if iso3=="`iso3'" & location_id == `loc_id' & year >= ``iso3'_year' & strmatch(source_type,"VR*") using `before', clear
					
					// merge on the proportions and make a cause for HIV if it is new
					merge m:1 iso3 location_id subdiv year sex acause source* *frmat NID list national region dev_status using `props', nogen
					replace cause = "acause_hiv" if acause == "hiv" & cause == ""
					foreach var of varlist allocation* {
						replace `var' = 1 if `var' == .
					}
					
					// allocate the excess. now that this is multiplicative and not additive we have to add an arbitrary small amount (same as above), then subtract it back
					foreach i of numlist 3 7/22 94 {
						gen flag = 1 if deaths`i' == 0 | deaths`i' == .
						replace deaths`i' = .0001 if flag == 1
						egen prop`i' = pc(deaths`i'), by(iso3 location_id subdiv year sex acause source* *frmat NID list national region dev_status) prop
						gen double deaths_new`i' = deaths`i' * allocation_proportion`i'
						replace deaths_new`i' = deaths`i' + (prop`i' * allocation_proportion`i') if acause == "hiv"
						replace deaths`i' = deaths`i' - .0001 if flag == 1
						replace deaths`i' = 0 if deaths`i' < 0
						replace deaths_new`i' = 0 if deaths_new`i' == .0001 & flag == 1
						drop flag
					}
					drop allocation_proportion*
				}
				


			** ******************		
			//  Save
			** ******************	
				noisily di "	Save product"
				// calculate deaths1/deaths2
				aorder
				egen double deaths_new1 = rowtotal(deaths_new3-deaths_new94 deaths23 deaths24 deaths25 deaths91 deaths93)
				egen double deaths_new2 = rowtotal(deaths_new94 deaths91 deaths93)
					
				if `graph_set' == 1 {
				// produce relative age pattern plots with post-corrections data
				count if acause == "hiv"
				if `r(N)' > 0 {
					preserve
						foreach val in 23 24 25 91 93 {
							gen deaths_new`val' = deaths`val'
						}
						replace acause = "hiv" if index(acause,"hiv")
						keep if acause == "hiv"
						fastcollapse deaths_new*, by(iso3 location_id sex year) type(sum)
						merge 1:1 iso3 location_id year sex using `pop', keep(3) keepusing(pop*) nogen
						drop if deaths_new1 == 0
						drop deaths_new1 deaths_new2
						foreach num of numlist 3 7/22 94 {
							gen r_rate`num' = (deaths_new`num'/pop`num')/(deaths_new14/pop14)
						}
						reshape long r_rate pop death_new, i(iso3 sex year) j(agecat)
						label drop _all
						label define sex_lbl 1 "Males" 2 "Females"
						label values sex sex_lbl
						gen age = (agecat-6)*5
						replace age = 0 if agecat == 91
						replace age = 0.01 if agecat == 93
						replace age = 0.1 if agecat == 94
						replace age = 1 if agecat == 3
						sort year age
						pdfstart using "$graph_dir/`iso3'/HIV_relative_age_pattern_`iso3'_`loc_id'_`src'.pdf"
						levelsof year, local(yrs)
						foreach yr of local yrs {
							scatter r_rate age if year == `yr', by(sex, title("Relative age pattern of HIV""`iso3' `loc_id', `src' [`yr']", size(med))) xtitle("Age") ytitle("Relative Rate") connect(l)
							pdfappend
						}
						pdffinish
						capture erase "$graph_dir/`iso3'/HIV_relative_age_pattern_`iso3'_`loc_id'_`src'.log"
					restore
				}
			}
					
				// save
				tempfile `iso3'_`loc_id'_corrected
				save ``iso3'_`loc_id'_corrected', replace
		}
		
		// Append location IDs to make one tempfile
		clear
		gen iso3 = ""
		foreach loc_id of local loc_ids {
			append using ``iso3'_`loc_id'_corrected'
		}
		tempfile `iso3'_corrected
		save ``iso3'_corrected', replace
		
	}
	}
** ****************************************************************************************************************
** ****************************************************************************************************************
	// load original data
	use `before', clear

local used_isos = trim("`used_isos'")
 if "`used_isos'" != "" {
	// merge back onto original dataset
	foreach iso3 of local used_isos {
		if "`cause_group'" == "non_garbage" merge m:1 iso3 location_id subdiv year sex acause source* NID list national region dev_status using ``iso3'_corrected', update nogen
		if "`cause_group'" == "garbage" merge m:1 iso3 location_id subdiv year sex cause cause_name acause source* *frmat NID list national region dev_status using ``iso3'_corrected', update nogen
	}
		
	if `graph_set' == 1 {
	// graph old vs new deaths [total by cause per year]
	preserve
		keep if (deaths_new1 > deaths1 + 1) | (deaths_new1 < deaths1 - 1)
		fastcollapse deaths*, by(iso3 location_id sex year acause) type(sum)
		foreach iso3 of local used_isos {
			levelsof location_id if iso3 == "`iso3'" & year >= ``iso3'_year', local(loc_ids)
			foreach loc_id of local loc_ids {
				pdfstart using "$graph_dir/`iso3'/HIV_corrections_deaths_`iso3'_`loc_id'_by_cause_`src'.pdf"
				foreach acause of local acause_`iso3' {
					scatter deaths1 year if acause == "`acause'" & iso3 == "`iso3'" & location_id == `loc_id', by(sex) || scatter deaths_new1 year if acause == "`acause'" & iso3 == "`iso3'" & location_id == `loc_id', by(sex) title("`acause'")
					pdfappend
				}
				pdffinish
				capture erase "$graph_dir/`iso3'/HIV_corrections_deaths_`iso3'_`loc_id'_by_cause_`src'.log"
			}
		}
	restore
	
	// graph old vs new deaths
	preserve
		keep if index(acause,"hiv")
		fastcollapse deaths*, by(iso3 location_id sex year acause) type(sum)
		reshape long deaths deaths_new, i(iso3 location_id sex year acause) j(agecat)
		gen age = (agecat-6)*5
		replace age = 0 if agecat == 91
		replace age = 0.01 if agecat == 93
		replace age = 0.1 if agecat == 94
		replace age = 1 if agecat == 3
		drop if inlist(agecat,1,2,4,5,6,92,24,25,26)
		tempfile orig
		save `orig', replace
		foreach hiv_type in hiv_other hiv_tb {
			foreach iso3 of local used_isos {
				use `orig', clear
				levelsof year if iso3 == "`iso3'" & year >= ``iso3'_year', local(years)
				levelsof location_id if iso3 == "`iso3'" & year >= ``iso3'_year', local(loc_ids)
				reshape wide deaths*, i(iso3 location_id sex age acause) j(year)
				foreach loc_id of local loc_ids {
					pdfstart using "$graph_dir/`iso3'/HIV_corrections_deaths_`iso3'_`loc_id'_`hiv_type'_by_year_over_age_`src'.pdf"
					foreach year of local years {
						scatter deaths`year' age if acause == "`hiv_type'" & iso3 == "`iso3'" & location_id == `loc_id', by(sex) || scatter deaths_new`year' age if acause == "`hiv_type'" & iso3 == "`iso3'" & location_id == `loc_id', by(sex) title("`year'")
						pdfappend
					}
					pdffinish
					capture erase "$graph_dir/`iso3'/HIV_corrections_deaths_`iso3'_`loc_id'_hiv_by_year_over_age_`src'.log"
				}
			}
		}
	restore
	}

	// We don't want to add zeroes
	drop if deaths_new1 == 0 & deaths1 == .
	
	// swap in new deaths 
	foreach i of numlist 1/26 91/94 {
		capture replace deaths`i' = deaths_new`i' if deaths_new`i' != .
	}
	drop deaths_new*
	
	// get rid of nulls
	foreach var of varlist *deaths* {
		capture replace `var' = 0 if `var' == .
	}

}

** ******************
// Fix modifications
** ******************
	// restore garbage coding
	replace acause = "_gc" if acause == "_inj_gc"
	
	// drop injury garbage identifier
	capture drop inj_gar
	
	// restore location_id
	replace location_id = . if location_id == 0
	
	// lose proportion variables
	capture drop prop*
	
** ******************
// Make sure we have not created negatives, and do not lose deaths
** ******************
	quietly  {
	local break = 0
	** Check for negatives
	foreach var of varlist deaths* {
		count if `var' < 0
		if `r(N)' > 0 {
			local break = 1
			keep if `var' < 0
			gen country_cause_`var' = acause + " in " + iso3
			levelsof country_cause_`var', local(ccs) c sep(", ")
			noisily di "Negative deaths found (`var') : `ccs'"
		}
	}
	if "`cause_group'" == "non_garbage" {
	** Check that we did not lose deaths
	preserve
		fastcollapse deaths1 orig_deaths1, by(iso3 year) type(sum)
		keep if abs(orig_deaths1-deaths1) > (orig_deaths1/1000)
		count
		if `r(N)' > 0 {
			local break = 1
			gen country_year = iso3 + " in " + string(year)
			levelsof country_year, local(cys) c sep(", ")
			noisily di ""
			noisily di "Difference in deaths greater than .1% : `cys'"
			noisily di ""
		}
	restore
	}
	if `break' == 1 BREAK
	}
	
** ****************************************************************************************************
