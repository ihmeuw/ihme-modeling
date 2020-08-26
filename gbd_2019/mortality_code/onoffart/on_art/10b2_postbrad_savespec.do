// Author NAME
// Created: 1/25/14
// Prep HIV on ART adjusted death rates into Spectrum format of conditional PROBABILITIES

// Last Updated by: NAME
// Last Updated Date: Feb 25, 2014

// settings
	clear all
	set more off
	
	global user "`c(username)'"
	
	if (c(os)=="Unix") {
		global root "ADDRESS" 
		local code_dir = "FILEPATH"
	}

	if (c(os)=="Windows") {
		global root "ADDRESS" 
		local code_dir = "FILEPATH"
	}
	
	global archive = 1
	local save_files_for_spectrum=1
	local graph=1  
	local separate = 1
	
	*you can replace on art with off art if it is higher, liberal replace will use one higher cd4 category for 7-12 months and two higher for GT12Mo (idea being that on treatment your cd4 will increase)
	*calc off_art will create the off art mort used for comparison. Only has to be run once and takes a while
	local replaceon = 1
	local calc_offart = 1
	local liberal_replace = 0

/// locals
	cap restore, not
	adopath + "FILEPATH"
	
	
	local dismod_dir "FILEPATH"
	local bradmod_dir "FILEPATH"
	local data_dir "FILEPATH"
	local store_draws "FILEPATH"
	local stage_dir = "FILEPATH"
	local save_spectrum "FILEPATH"
	local spectrum_archive_dir "FILEPATH" 
	
	foreach type in DisMod DisMod_HI DisMod_BestSSA {
		cap mkdir "`save_spectrum'/`type'"
		if $archive == 1{
			cap mkdir "`spectrum_archive_dir'/`type'"
			cap mkdir "`spectrum_archive_dir'/`type'/$date"
		}
	} 
	
	local store_dir "`save_spectrum'/DisMod"
	local store_dir_HI "`save_spectrum'/DisMod_HI"
	local store_dir_BESTSSA "`save_spectrum'/DisMod_BestSSA"
	local store_graphs "FILEPATH"
	local spec_xbs "`save_spectrum'/UNAIDS_HIVmort_onART.csv"
	local HIV_free_mort "FILEPATH"

// Initialize pdfmaker
if 0==0 {
	if c(os) == "Windows" {
		global prefix "$root"
		do "FILEPATH"
	}
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		do "FILEPATH"
		set odbcmgr unixodbc
	}
}
 
 
********* COMPILE PROBABILITY RESULTS *****

 
	******** BRING IN EACH OF THE OUTPUTS
	// keep last 1000
	// Merge together results (on draw# and period (duration))
	

*create empty data set for appending 
set obs 1 
gen blank = . 
tempfile master 
save `master', replace	
	
	
	
if `separate'{ 
	foreach a in 15_25 25_35 35_45 45_55 55_100{
		foreach s in 1 2 {
			foreach per in 0_6 7_12 12_24 { 
				foreach sup in high other ssa{
					di "`a' `s' `per' `sup'"
					insheet using "`dismod_dir'/HIV_KM_`a'_`s'_`per'_`sup'/model_draw2.csv", comma names clear 
					
					keep `sup'*
					
					keep if _n>4000 
					
					gen draw=_n
					gen dur="`per'" 
					gen sex = `s' 
					gen age = "`a'"
					foreach var of varlist _all {
						rename `var' prob`var'
					}
					rename probdur dur
					rename probdraw draw 
					rename probsex sex 
					rename probage age

				
					reshape long prob, i(draw dur sex age) j(super_cd4) string
					reshape wide prob, i(super_cd4 dur sex age) j(draw) 
					tempfile `a'`s'`per'`sup' 
					save ``a'`s'`per'`sup'', replace
					
							
				}
				
			}
		}
	} 
		 
		 
		 use `master', clear

		 foreach a in 15_25 25_35 35_45 45_55 55_100{
			foreach s in 1 2 {
				foreach per in 0_6 7_12 12_24 { 
					foreach sup in high other ssa{
				
						append using ``a'`s'`per'`sup''
					
							

					}
				}
			}
		}  
}

else{	
	
	foreach a in 15_25 25_35 35_45 45_55 55_100{
		foreach s in 1 2 {
			foreach per in 0_6 7_12 12_24 {
				insheet using "`dismod_dir'/HIV_KM_`a'_`s'_`per'/model_draw2.csv", comma names clear 
				
				keep if _n>4000 
				
				gen draw=_n
				gen dur="`per'" 
				gen sex = `s' 
				gen age = "`a'"
				foreach var of varlist _all {
					rename `var' prob`var'
				}
				rename probdur dur
				rename probdraw draw 
				rename probsex sex 
				rename probage age

			
				reshape long prob, i(draw dur sex age) j(super_cd4) string
				reshape wide prob, i(super_cd4 dur sex age) j(draw) 
				tempfile `a'`s'`per' 
				save ``a'`s'`per'', replace
				
						

			
			}
		}
	} 
	 
	 
	 use `master', clear

	 foreach a in 15_25 25_35 35_45 45_55 55_100{
		foreach s in 1 2 {
			foreach per in 0_6 7_12 12_24 {
			
				append using ``a'`s'`per''
				
						

			
			}
		}
	}  
} 

 
drop blank 
drop if _n == 1 

gen durationart = "6to12Mo" 
replace durationart = "GT12Mo" if dur == "12_24" 
replace durationart = "LT6Mo" if dur == "0_6" 
drop dur 

replace age = subinstr(age, "_","-",1) 

split super_cd4, p("_") 
drop super_cd4

rename super_cd41 super 
rename super_cd42 cd4_lower 
destring cd4_lower, replace

generate cd4_upper = 50 
replace cd4_upper = 100 if cd4_lower == 50 
replace cd4_upper = 200 if cd4_lower == 100 
replace cd4_upper = 250 if cd4_lower == 200 
replace cd4_upper = 350 if cd4_lower == 250 
replace cd4_upper = 500 if cd4_lower == 350 
replace cd4_upper = 1000 if cd4_lower == 500 

gen cd4_category="ARTLT50CD4" if cd4_lower==0
replace cd4_category="ART50to99CD4" if cd4_lower==50
replace cd4_category="ART100to199CD4" if cd4_lower==100
replace cd4_category="ART200to249CD4" if cd4_lower==200
replace cd4_category="ART250to349CD4" if cd4_lower==250
replace cd4_category="ART350to500CD4" if cd4_lower==350
replace cd4_category="ARTGT500CD4" if cd4_lower==500

forvalues i=1/1000 {
				rename prob`i' mort`i'  
				replace mort`i' = 1-(1-mort`i')^2 if durationart == "LT6Mo" | durationart =="6to12Mo"
			} 

saveold "`data_dir'/mortality_probs_new_model", replace 

if `replaceon'{ 
	
	*this only has to be run once and takes quite a while
	if `calc_offart'{
		preserve
		do "`code_dir'/calc_p.do"
		restore
	}
	
	gen merge_inc = "hi" if super == "high" 
	replace merge_inc = "nohi" if super == "ssa" | super == "other"
	gen merge_age = age
	replace merge_age = "45-100" if merge_age == "45-55" | merge_age == "55-100"
	
	if `liberal_replace'{ 
		gen merge_cd4 = cd4_category 
		replace merge_cd4 = "ARTGT500CD4" if cd4_category == "ART350to500CD4" & durationart == "6to12Mo"
		replace merge_cd4 = "ART350to500CD4" if cd4_category == "ART250to349CD4" & durationart == "6to12Mo"
		replace merge_cd4 = "ART250to349CD4" if cd4_category == "ART200to249CD4" & durationart == "6to12Mo"
		replace merge_cd4 = "ART200to249CD4" if cd4_category == "ART100to199CD4" & durationart == "6to12Mo"
		replace merge_cd4 = "ART100to199CD4" if cd4_category == "ART50to99CD4" & durationart == "6to12Mo"
		replace merge_cd4 = "ART50to99CD4" if cd4_category == "ARTLT50CD4" & durationart == "6to12Mo"
		replace merge_cd4 = "ARTGT500CD4" if cd4_category == "ART350to500CD4" & durationart == "GT12Mo"
		replace merge_cd4 = "ARTGT500CD4" if cd4_category == "ART250to349CD4" & durationart == "GT12Mo"
		replace merge_cd4 = "ART350to500CD4" if cd4_category == "ART200to249CD4" & durationart == "GT12Mo"
		replace merge_cd4 = "ART250to349CD4" if cd4_category == "ART100to199CD4" & durationart == "GT12Mo"
		replace merge_cd4 = "ART200to249CD4" if cd4_category == "ART50to99CD4" & durationart == "GT12Mo"
		replace merge_cd4 = "ART100to199CD4" if cd4_category == "ARTLT50CD4" & durationart == "GT12Mo"
	}
	else{ 
		gen merge_cd4 = cd4_category
	}
	
	merge m:1 merge_age merge_inc merge_cd4 using "`data_dir'/off_art_mort.dta", nogen
	
	foreach d of numlist 1/1000{ 

		replace mort`d' = LT6Mo_offartmort`d' if mort`d' > LT6Mo_offartmort`d'

	}

	drop LT6Mo_offartmort* 
	drop merge_age merge_inc merge_cd4

	saveold "`data_dir'/mortality_probs_new_model_replace.dta", replace


}

tempfile tmp_prep 
save `tmp_prep', replace



foreach rrr in high ssa other  {
		use `tmp_prep' if super == "`rrr'", clear
		drop super
		outsheet using "`stage_dir'/`rrr'.csv", comma replace 
	}

	
if `save_files_for_spectrum' {	
	** create iso3 list to save... 
		get_locations
		keep ihme_loc_id super_region_name region_id
		qui levelsof ihme_loc_id, local(isos) c
		rename ihme_loc_id iso3
		replace super_region_name ="Sub-Saharan Africa"  if iso=="SSD"
		gen super="high" if regexm(super_region_name,"High")
		replace super="ssa" if super_region_name=="Sub-Saharan Africa"
		replace super="other" if regexm(super_region_name, "Latin America") | regexm(super_region_name,"East Asia") | regexm(super_region_name, "North Africa") | regexm(super_region_name,"South Asia") | regexm(super_region_name,"Central Europe")
		
		tempfile tmp_compile
		save `tmp_compile', replace 

	if (c(os)=="Unix") {
		
		// Remove the results from the previous run
		! perl -e 'unlink <`store_dir'/\*>' 
		! perl -e 'unlink <`store_dir_HI'/\*>' 
		
		
		foreach i of local isos {
			di "`i'"
			use `tmp_compile' if iso3 == "`i'", clear
			local super_file = super[1]
			! cp "`stage_dir'/`super_file'.csv" "`store_dir'/`i'_HIVonART.csv"
			! cp "`stage_dir'/high.csv" "`store_dir_HI'/`i'_HIVonART.csv"
		}
		
		local ccc = 0
		local counter = wordcount("`isos'") 
		local counter = `counter' * 2 // Get number of expected files
			
		while `ccc' == 0 {
		local filecount = 0
				
		foreach i of local isos {
			capture confirm file "`store_dir'/`i'_HIVonART.csv"
			if !_rc local ++filecount

				capture confirm file "`store_dir_HI'/`i'_HIVonART.csv"
				if !_rc local ++filecount

			}
				
		di "Checking `c(current_time)': `filecount' of `counter' Save GBD files found"
		if (`filecount' == `counter') local ccc = 1
				else sleep 10000
		}	
	}
	
	else {
		** loop through iso3s and save data... 
		use "`tmp_compile'", clear 
		levelsof iso3, local(isos)
		foreach i of local isos {
			preserve
				di in red "Prepping `i' Current Mortality"
				keep if iso=="`i'"
				merge 1:m super using "`tmp_prep'", keep(3) nogen
				drop iso super 
				outsheet using "`store_dir'/`i'_HIVonART.csv", delim(",") replace 
			restore
		} 
		
		** store HI counterfactual for all 
		use "`tmp_compile'", clear 
		replace super="high"
		levelsof iso3, local(isos)
		foreach i of local isos {
			preserve
				di in red "Prepping `i' High Counterfactual"
				keep if iso=="`i'"
				merge 1:m super using "`tmp_prep'", keep(3) nogen 
				drop iso super 
				outsheet using "`store_dir_HI'/`i'_HIVonART.csv", delim(",") replace 
			restore
		}
		
		** store BEST SSA counterfactual for all 
		use "`tmp_compile'", clear 
		replace super="ssabest"
		levelsof iso3, local(isos)
		foreach i of local isos {
			preserve
				di in red "Prepping `i' Best SSA Counterfactual"
				keep if iso=="`i'"
				merge 1:m super using "`tmp_prep'", keep(3) nogen 
				drop iso super 
				outsheet using "`store_dir_BESTSSA'/`i'_HIVonART.csv", delim(",") replace 
			restore
		}
	}	
}	
