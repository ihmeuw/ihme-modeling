** *****************************************************************************													
** Purpose: generate ICD10, ICD9_detail, ICD9_BTL, ICD8A, ICD7A age weights
** *****************************************************************************	

if c(os) == "Windows" {
	global j "J:"
	global h "H:"
}
if c(os) == "Unix" {
	global j "/home/j" 
	set odbcmgr unixodbc
	global h "~"
}

do "$j/WORK/10_gbd/00_library/functions/fastcollapse.ado"

local in_dir "$j/WORK/03_cod/01_database/03_datasets/"
local data_dir	"$j/WORK/03_cod/01_database/02_programs/age_sex_splitting/data/input_for_weights/"
local out_dir	"$j/WORK/03_cod/01_database/02_programs/age_sex_splitting/data/weights/"

clear
set more off, perm
pause on
set mem 25g

// Prep the completeness weights
	use "$j/Project/Mortality/GBD Envelopes/00. Input data/00. Format all age data/d08_smoothed_completeness.dta", clear
		keep iso3 year iso3_sex_source u5_comp_pred trunc_pred
		** drop if completeness isn't for VR
		drop if substr(iso3_sex_source, -2, .)!="VR"
		** there are duplicates, create mean across them
		** and create both sex mean for saudi, which is by sex
		replace iso3_sex_source= subinstr(iso3_sex_source, "male", "both", .)
		replace iso3_sex_source= subinstr(iso3_sex_source, "feboth", "both", .)
		fastcollapse u5_comp_pred trunc_pred, by(iso3 year) type(mean)
		** generate a weight for 5-14
		egen double kid_comp = rowmean(u5 trunc)
		replace u5=1 if u5>1
		replace trunc=1 if trunc>1
		replace kid=1 if kid>1
		tempfil comp
		save `comp', replace
		
// Get population
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/env_wide.do"
	tempfile pop
	save `pop', replace
	
// Bring in the datasets
quietly {
	insheet using "$j/WORK/03_cod/01_database/02_programs/age_sex_splitting/code/age_sex_split_vr_sources.csv", comma names clear
	levelsof vr_sources, local(data_sources)
	noisily di "Reading data..."
	foreach data_source of local data_sources {
		noisily di "     ... `data_source'"
		use "`in_dir'/`data_source'/data/intermediate/01_mapped.dta", clear
		tempfile tmp_`data_source'
		save `tmp_`data_source'', replace
	}
	clear
	noisily di "Appending data..."
	foreach data_source of local data_sources {
		noisily di "     ... `data_source'"
		append using `tmp_`data_source''
	}
}
	
// Drop South Africa
	drop if iso3=="ZAF"
	
// Aggregate to national
	replace location_id = .
	fastcollapse deaths*, by(iso3 location_id subdiv year list NID acause sex *frmat source source_label national ///
		region cause*) type(sum)
	
// if sex == 3 (both) and sex == 1 and sex == 2 each have deaths, set deaths variables to 0
// WARNING: Don't get rid of deaths for sex == 3 until you have confirmed that deaths for males and females
	sort iso3 location_id subdiv cause year source NID national sex
	foreach var of varlist deaths* {
		replace `var' = 0 if sex == 3 & (sex[_n-1] == 1 | sex[_n-1] == 2) & (sex[_n-2] == 1 | sex[_n-2] == 2) ///
			& `var'[_n-1] !=0 & `var'[_n-2]!=0 
	}
	
// recode any remaining sex == 3 (both) observations as sex == 9 (unknown) for the age-splitting code to run
	replace sex = 9 if sex ==3

// now we have both sex == 3 and sex == 9 for some causes; collapse to combine
	fastcollapse deaths*, by(iso3 location_id year list acause sex *frmat source source_label national ///
		region cause*) type(sum)
		
// for observations with deaths in age unknown (deaths26) as well as in other age groups, split these two cases ///
// into two different observations
	** identify and duplicate these observations
	capture drop tmp
	egen tmp = rowtotal(deaths3-deaths25 deaths91-deaths94)
	gen tag = (tmp != 0 & deaths26 != 0)
	expand 2 if tag == 1, gen(new)

	** change values for the first of the observations to just hold non-deaths26 deaths
	replace deaths26 = 0 if tag == 1 & new == 0

	** change values for the second of the observations to just hold deaths26 deaths
		// put deaths* in the right order first
		order region iso3 source source_label national frmat im_frmat ///
		list sex year cause acause ///
		deaths2 deaths3 deaths4 deaths5 deaths6 deaths7 deaths8 deaths9 deaths10 deaths11 /// 
		deaths12 deaths13 deaths14 deaths15 deaths16 deaths17 deaths18 deaths19 deaths20 deaths21 ///
		deaths22 deaths23 deaths24 deaths25 deaths91 deaths92 deaths93 deaths94 deaths1

		foreach var of varlist deaths3-deaths25 deaths91-deaths1 {
		replace `var' = 0 if tag == 1 & new == 1
	}
	replace frmat = 9 if tag == 1 & new == 1
	
	** cleanup
	drop tmp tag new
	capture drop _merge

// Drop both sexes
	drop if sex==3 | sex==9
	
// Replace Hong Kong location identifier
	replace iso3 = "CHN" if iso3 == "HKG"
// Replace Macao  location identifier
	replace iso3 = "CHN" if iso3 == "MAC"
	
// set age frmat to be 9 if appropriate
	aorder
	move deaths26 deaths1
	egen deaths_known = rowtotal(deaths3-deaths94)
	replace frmat = 9 if deaths_known == 0 & deaths26 > 0 & deaths26 != .
	drop deaths_known
	
// Recalculate deaths1 and deaths2 here, to make sure that we have the right deaths for the age weights 
	drop deaths1
	aorder 
	egen deaths1 = rowtotal(deaths3-deaths94)
	drop if deaths1==0 | deaths1==.
	
	egen tot_im = rowtotal(deaths91-deaths94)
	replace deaths2 = tot_im 
	drop tot_im

// keep WHO standard age formats
	keep if (frmat == 1 | frmat == 2) |  (im_frmat == 1 | im_frmat == 2)
	drop if frmat==9
	
// collapse deaths91 and deaths92 where im_frmat == 1
	replace deaths91 = deaths91 + deaths92 if im_frmat == 1
	replace im_frmat = 2 if im_frmat == 1
	drop deaths92
	
// collapse deaths3-deaths6 where frmat = 1
	replace deaths3 = deaths3 + deaths4 + deaths5 + deaths6 if frmat == 1
	replace deaths4 = 0 if frmat == 1
	replace deaths5 = 0 if frmat == 1
	replace deaths6 = 0 if frmat == 1
	replace frmat = 2 if frmat == 1
	
// save
	compress
	save "`data_dir'/acause_weight_data.dta", replace
	tempfile alldat
	save `alldat', replace

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// MAKE ACAUSE WEIGHTS
	use "`data_dir'/acause_weight_data.dta", clear
	drop if inlist(iso3, "HKG", "MAC")
	drop if sex==3 | sex==9
	drop if frmat==9
	drop if deaths1==0 | deaths1==.
	tempfile alldat
	save `alldat', replace
	
	// make a "cc_code" weight, which will also be the all-cause weight
		use `alldat', clear
		replace acause = "cc_code"
		fastcollapse deaths*, by(iso3 location_id acause year sex frmat im_frmat) type(sum)
		tempfile fil2
		save `fil2', replace
			
	// rename the cc_code to ALL
		replace acause = "all"
		tempfile fil3
		save `fil3', replace
		
	// collapse to acause
		use `alldat', clear
		** typos in some acauses
		replace acause = lower(acause)
		fastcollapse deaths*, by(iso3 location_id acause year sex frmat im_frmat) type(sum)
		
	// Aggregate up the levels
		
		** prepare the cause list
			preserve
			use "$j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta", clear
			** keep if cause_version==2 
			keep cause_id path_to_top_parent level acause yld_only yll_age_start yll_age_end male female
			** drop the parent "all_cause"
			levelsof cause_id if level == 0, local(top_cause)
			drop if path_to_top_parent =="`top_cause'"
			replace path_to_top_parent = subinstr(path_to_top_parent,"`top_cause',", "", .)	
			** make cause parents for each level
			rename path_to_top_parent agg_
			split agg_, p(",")
			** take the cause itself out of the path to parent
			forvalues i = 1/5 {
				replace agg_`i' = "" if level == `i'
			}
			** nothing aggregates to level5
			drop agg_5
			tempfile cause
			save `cause', replace
			restore
			replace acause = lower(acause)
		
		** merge with the data, keep only aggregation-appropriate variables
			merge m:1 acause using `cause', keepusing(cause level agg* yld_only) keep(1 3)
			drop if acause=="_u"

		** aggregate without conditions because this is VR data
			fastcollapse deaths*, by(iso3 location_id cause_id acause level agg* year sex frmat im_frmat) type(sum)
			tempfile orig
			save `orig', replace
			foreach level in 5 4 3 2 {
				use `orig', clear
				local agg_level = `level'-1
				if `level'<=4 append using `level`level''
				if `level'<=4 merge m:1 cause_id using `cause', update keepusing(level agg*) keep(1 3 4 5)
				if `level'<=4 tab cause if _m==1
				keep if level == `level'
				fastcollapse deaths*, by(iso3 location_id agg_`agg_level' year sex frmat im_frmat) type(sum)
				rename agg_`agg_level' cause_id
				destring cause_id, replace
				gen level = `agg_level'
				tempfile level`agg_level'
				save `level`agg_level'', replace
			}
		** orig has level 5
		use `orig', clear
		gen orig=1
		append using `level4'
		append using `level3'
		append using `level2'
		append using `level1'
		replace orig=0 if orig==.
		** Keep only level 3 and above for making weights
		drop if level>3 & level!=.

**	+++++++++++++++++++++++++++++++++++++++++++++++
		fastcollapse deaths*, by(iso3 location_id cause_id year sex frmat im_frmat) type(sum)
**	+++++++++++++++++++++++++++++++++++++++++++++++

		** bring back other variables of interest
		merge m:1 cause_id using `cause', keepusing(acause male female yll_age_start yll_age_end) update keep(1 3 4 5) nogen
		drop cause_id
		** append these extra files
		append using `fil2'		
		append using `fil3'
		
	// combine deaths22-25
		aorder
		egen tmp = rowtotal(deaths22-deaths25)
		replace deaths22 = tmp
		drop tmp
		drop deaths23 deaths24 deaths25
		
	// merge in population so we can calculate rates
		** drop before 1970 for now
			drop if year<1970

		merge m:1 iso3 location_id year sex using "`pop'", assert (using matched) keep(3) keepusing(pop*) nogen
		
	// rename variables to actual ages
		foreach var in pop deaths {
				rename `var'91 `var'_91
				rename `var'93 `var'_93
				rename `var'94 `var'_94
				rename `var'3 `var'_1
			forvalues i = 7/22 {
				local j = (`i'-6)*5
				rename `var'`i' `var'_`j'
			}
		}
		capture drop deaths1 deaths2 deaths4 deaths5 deaths6 deaths26

	// ADJUST FOR COMPLETENESS
		
		merge m:1 iso3 year using `comp', keep(1 3)
		
		** We are missing Pakistan 1993-1994 & Turkey 1999-2012 (Also China, Korea, and US Virgin Islands)
		codebook iso3 if _m==1
		codebook year if _m==1
		** pause
		drop if _m==1
		drop _m
		
		** child completeness
		foreach i of numlist 91 93 94 1 {
			replace deaths_`i' = deaths_`i' / u5_comp
			}
		** 5-14 comp adjust
		foreach i of numlist 5 10 {
			replace deaths_`i' = deaths_`i' / kid_comp
			}
		** adult completeness
		foreach i of numlist 15(5)80 {
			replace deaths_`i' = deaths_`i' / trunc_pred
			}
		
	// for country-years where we have good im_frmats but a bad frmat (eg, im_frmat = 1, frmat = 3), or vice versa, set the deaths corresponding to the bad frmat to missing
		foreach i of numlist 1 5(5)80 {
			replace deaths_`i' = 0 if frmat != 2
			replace pop_`i' = 0 if frmat != 2
		}
		foreach i of numlist 91 93 94 {
			replace deaths_`i' = 0 if im_frmat != 2
			replace pop_`i' = 0 if im_frmat != 2
		}
		
**	+++++++++++++++++++++++++++++++++++++++++++++++
		// Save aggregated and completeness adusted file
		fastcollapse pop_* deaths*, by(iso3 location_id acause year sex yll_age_start yll_age_end male female) type(sum)
		save "`data_dir'/acause_weight_adjusted_data.dta", replace
**	+++++++++++++++++++++++++++++++++++++++++++++++

	// now collapse pop and deaths across all countries and years	
		fastcollapse pop_* deaths_*, by(sex acause yll_age_start yll_age_end male female) type(sum)
		
	// create rates
		foreach i of numlist 1 5(5)80 91 93 94 {
			generate rate`i' = deaths_`i'/pop_`i'
		}
		
	// make age long
		drop deaths_* pop_*
		reshape long rate, i(sex acause) j(age)
		rename rate weight
		replace weight = 0 if weight == . | weight<0
		sort sex age
	
	// Drop data that is unusable per restrictions
		** make the NULL restrictions usable
			replace yll_age_end = 80 if yll_age_end == .
			replace yll_age_start = 0 if yll_age_start == .
			replace male = 1 if male == .
			replace female = 1 if female == .
			destring yll_age_start yll_age_end male female, replace
		** drop restricted sexes
			replace weight=0 if sex==1 & male==0
			replace weight=0 if sex==2 & female==0
		** drop restricted ages. Have to encode as something slightly larger than the true code so that we dont' have problems with precision.
			recast double age
			replace age = 0 if age==91
			replace age = .01 if age==93
			replace age = .1 if age==94
			replace weight=0 if age<yll_age_start & yll_age_start!=.
			replace weight=0 if age>yll_age_end & yll_age_end!=.
			replace age = 91 if age==0
			replace age = 93 if age>0.009 & age<0.0111
			replace age = 94 if age>0.099 & age<0.111

** ******************************************
	// some have only male or female, make other weight
		reshape wide weight, i(acause age yll* male female) j(sex)
		replace weight1 = 0 if weight1 == .
		replace weight2 = 0 if weight2 == .
		reshape long weight, i(acause age yll* male female) j(sex)
** ******************************************
		
 	// save
		quietly levelsof acause, local(causes)
		foreach c of local causes {
			preserve
				drop yll* male female
				keep if acause == "`c'"
				sort sex age
				order acause sex age weight
				save "`out_dir'/acause_age_weight_`c'.dta", replace
				capture saveold "`out_dir'/acause_age_weight_`c'.dta", replace
				if "`c'" == "maternal" {
					save "`out_dir'/acause_age_weight_maternal_hiv.dta", replace
					capture saveold "`out_dir'/acause_age_weight_maternal_hiv.dta", replace
				}
			restore
		}

	// Make reference group weights
		preserve
			keep if male == 0
			keep if sex==2
			keep if age==40
			keep acause weight
			rename weight weight_f_40
			tempfil f40
			save `f40', replace
		restore, preserve
			keep if sex==1
			keep if age==40
			rename weight weight_m_40
			keep acause weight
			tempfil m40
			save `m40', replace
		restore, preserve
			keep if yll_age_end<=1
			keep if sex==1
			keep if age==91
			rename weight weight_m_inf
			keep acause weight
			tempfil m_infant
			save `m_infant', replace
		restore
			merge m:1 acause using `f40', assert(master matched) nogen
			merge m:1 acause using `m40', assert(master matched) nogen
			merge m:1 acause using `m_infant', assert(master matched) nogen
			replace weight = weight / weight_m_40 if male!=0 & yll_age_end>1
			replace weight = weight / weight_f_40 if male==0 & yll_age_end>1
			replace weight = weight / weight_m_inf if yll_age_end<=1
			drop weight_* yll*
		
	// plot the weights by age
		quietly do "$j/Usable/Tools/ADO/pdfmaker.do"
		pdfstart using "`out_dir'/acause_weights.pdf"
		foreach c of local causes {
			preserve
			keep if acause == "`c'"
			// finagle with ages so that we can get the labels right
				replace age = 4 if age == 1
				replace age = 1 if age == 91
				replace age = 2 if age == 93
				replace age = 3 if age == 94
			// label them so that the graph axis looks normal
				label define agelab 1 "enn" 2 "lnn" 3 "pnn" 4 "1" 5 "5" 10 "10" 15 "15" 20 "20" 25 "25" 30 "30" 35 "35" 40 "40" 45 "45" 50 "50" 55 "55" 60 "60" 65 "65" 70 "70" 75 "75" 80 "80+"
				label values age agelab
				sort sex age
			// label sex
				label define sexlab 1 "Male" 2 "Female"
				label values sex sexlab
			scatter weight age, by(sex, title("Age Split Weights, `c'")) xtitle("Age") ytitle("Weight") connect(l)
			pdfappend 
			restore
		}
		pdffinish
