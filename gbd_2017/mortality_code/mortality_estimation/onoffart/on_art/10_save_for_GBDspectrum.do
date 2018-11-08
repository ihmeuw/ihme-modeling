// Author: NAME
// Created: 1/25/14
// Prep HIV on ART adjusted death rates into Spectrum format of conditional PROBABILITIES

// Last Updated by: NAME
// Last Updated Date: Feb 25, 2014

// settings
	clear all
	set more off

	if (c(os)=="Unix") {
		global root "ADDRESS"
	}

	if (c(os)=="Windows") {
		global root "ADDRESS"
	}

	local save_files_for_spectrum=1
	local graph=1

/// locals
	cap restore, not
	adopath + "FILEPATH"
	
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


** ****************************
// Subtract background mortality from Spectrum rates and convert to probabilities
** ****************************

	** match mortality to spectrum
	insheet using "`HIV_free_mort'", clear 
		tostring sex, replace 
		replace sex="female" if sex=="2"
		replace sex="male" if sex=="1"
		replace age=subinstr(age,"-","_",.)

	tempfile tmp_mort
	save `tmp_mort', replace
	
	** pull in estimates and subtract background mort... 
	insheet using "`data_dir'/adj_death_rates.csv", clear 
		merge m:1 super age sex using "`tmp_mort'" 
		

		forvalues i=1/1000 {
			rename rate`i' draw`i'	
		}
	outsheet using "`store_draws'/HIVonART_dth_RATE_DisMod_WITHBACKGROUND_FORTABLE.csv", delim(",") replace 

	// SUBTRACT BACKGROUND MORTALITY AS A RATE
		forvalues i=1/1000 {
			replace draw`i'=draw`i'-hiv_free_dth_rt	
			replace draw`i'=0 if draw`i'<0
		}
	outsheet using "`store_draws'/HIVonART_dth_RATE_DisMod_FORTABLE.csv", delim(",") replace 
	
		forvalues i=1/1000 {
			// Convert rates into ANNUAL conditional probabilities
			replace draw`i'=1-(exp(-1*draw`i'*1)) if dur=="0_6"
			replace draw`i'=1-(exp(-1*draw`i'*1)) if dur=="7_12"
			replace draw`i'=1-(exp(-1*draw`i'*1)) if dur=="12_24"
		}
		
		drop hiv_free
		
	outsheet using "`store_draws'/HIVonART_dth_prob_DisMod.csv", delim(",") replace 

	
** ****************************
// Prep death probabilities and save in gbd format
** ****************************

	insheet using "`store_draws'/HIVonART_dth_prob_DisMod.csv", clear 
		replace dur="6to12Mo" if dur=="7_12"
		replace dur="LT6Mo" if dur=="0_6"
		replace dur="GT12Mo" if dur=="12_24"

		gen cd4_category="ARTLT50CD4" if cd4_lower==0
		replace cd4_category="ART50to99CD4" if cd4_lower==50
		replace cd4_category="ART100to199CD4" if cd4_lower==100
		replace cd4_category="ART200to249CD4" if cd4_lower==200
		replace cd4_category="ART250to349CD4" if cd4_lower==250
		replace cd4_category="ART350to500CD4" if cd4_lower==350
		replace cd4_category="ARTGT500CD4" if cd4_lower==500
		replace sex="1" if sex=="male"
		replace sex="2" if sex=="female"
		replace age=subinstr(age,"_","-",.) //"
		rename dur durationart
		
		order super dur cd4_category age sex draw*
		
		forvalues i=1/1000 {
			rename draw`i' mort`i'
		}
		
		sort super duration cd4_category age sex 
		
	tempfile tmp_prep
	save `tmp_prep', replace
	
	save "FILEPATH", replace
	
	foreach rrr in high ssa other best_ssa {
		use `tmp_prep' if super == "`rrr'", clear
		drop super
		outsheet using "`stage_dir'/`rrr'.csv", comma replace 
	}
	

if `save_files_for_spectrum' {	
	** create iso3 list to save... 
		insheet using "FILEPATH", clear
		keep ihme_loc_id super_region_name region_id
		qui levelsof ihme_loc_id, local(isos) c
		qui levelsof region_id, local(regions)
		rename ihme_loc_id iso3
		replace super_region_name ="Sub-Saharan Africa"  if iso=="SSD"
		gen super="high" if regexm(super_region_name,"High")
		replace super="ssa" if super_region_name=="Sub-Saharan Africa"
		replace super="other" if regexm(super_region_name, "Latin America") | regexm(super_region_name,"East Asia") | regexm(super_region_name, "North Africa") | regexm(super_region_name,"South Asia") | regexm(super_region_name,"Central Europe")
		
		tempfile tmp_compile
		save `tmp_compile', replace 

	if (c(os)=="Unix") {
		// Remove results from previous run
			foreach i of local isos {
				// Remove the results from the previous run
				cap rm "`store_dir'/`i'_HIVonART.csv"
				cap rm "`store_dir_HI'/`i'_HIVonART.csv"
				cap rm "`store_dir_BESTSSA'/`i'_HIVonART.csv"
			}
		
		foreach i of local isos {
			di "`i'"
			use `tmp_compile' if iso3 == "`i'", clear
			local super_file = super[1]
			! cp "`stage_dir'/`super_file'.csv" "`store_dir'/`i'_HIVonART.csv"
			! cp "`stage_dir'/high.csv" "`store_dir_HI'/`i'_HIVonART.csv"
			! cp "`stage_dir'/best_ssa.csv" "`store_dir_BESTSSA'/`i'_HIVonART.csv"
		}
		
		/*
		// Launch filesaving jobs
			cd "$code_dir"
			foreach region in `regions' {
				use `tmp_compile' if region_id == `region', clear
				local super_submit = super[1]
				di in red "Submitting Region `region' as super `super_submit'"
				! FILEPATH -e FILEPATH -o FILEPATH -P proj_hiv -pe multi_slot 2 -N save_`region' "run_all.sh" "$code_dir/10b_save_parallel.do" "`region' `super_submit' $archive $date"
			}
		*/
		// Check that all file-saving jobs have finished before proceeding
			local ccc = 0
			local counter = wordcount("`isos'") 
			local counter = `counter' * 3 // Get number of expected files
			
			while `ccc' == 0 {
				local filecount = 0
				
				foreach i of local isos {
					capture confirm file "`store_dir'/`i'_HIVonART.csv"
					if !_rc local ++filecount

					capture confirm file "`store_dir_HI'/`i'_HIVonART.csv"
					if !_rc local ++filecount

					capture confirm file "`store_dir_BESTSSA'/`i'_HIVonART.csv"
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
		
		
	/*
	// Outdated, use cluster only and save on clustertmp
	// Archive, if desired
		if $archive == 1 {
			foreach runtype in `runtypes' {
				cap mkdir "`store_dir'/archive_$date/`i'_HIVonART.csv"
				cap mkdir "`store_dir_HI'/archive_$date/`i'_HIVonART.csv"
				cap mkdir "`store_dir_BESTSSA'/archive_$date/`i'_HIVonART.csv"
					
				foreach i of local isos {
					! cp "`store_dir'/`i'_HIVonART.csv" "`store_dir'/archive_$date/`i'_HIVonART.csv"
					! cp "`store_dir_HI'/`i'_HIVonART.csv" "`store_dir_HI'/archive_$date/`i'_HIVonART.csv"
					! cp "`store_dir_BESTSSA'/`i'_HIVonART.csv" "`store_dir_BESTSSA'/archive_$date/`i'_HIVonART.csv"
				}
			}
		}
	*/
}	

	
if `graph' {


/// GRAPH TO CHECK SOUTH AFRICA VERSUS SEPCTRUM ASSUMPTIONS
	
	insheet using "`save_spectrum'/FILEPATH/ZAF_HIVonART.csv", clear comma names
	gen source="spectrum"
	tempfile spectrum
	save `spectrum', replace
	
	insheet using "`save_spectrum'/FILEPATH/ZAF_HIVonART.csv", clear comma names
	gen source="IHME"
	append using `spectrum'
	
	split age, parse(-)
	
	gen cd4_mid=150 if cd4_categ=="ART100to199CD4"
	replace cd4_mid=225 if cd4_categ=="ART200to249CD4"
	replace cd4_mid=300 if cd4_categ=="ART250to349CD4"
	replace cd4_mid=425 if cd4_categ=="ART350to500CD4"
	replace cd4_mid=75 if cd4_categ=="ART50to99CD4"
	replace cd4_mid=750 if cd4_categ=="ARTGT500CD4"
	replace cd4_mid=25 if cd4_categ=="ARTLT50CD4"

	egen avg=rowmean(mort*)
	
	sort duration cd4_mid
	
	foreach dur in 6to12Mo GT12Mo LT6Mo {
	
	pdfstart using "FILEPATH"
	
		foreach age in 15 25 35 44 55 {
			preserve
			keep if dur=="`dur'"
			keep if age1=="`age'"
			
			twoway line avg cd4_mid   if source=="spectrum" & sex==1, lpattern(dash) lcolor(red)  || ///
			 line avg cd4_mid  if source=="spectrum" & sex==2, lpattern(solid) lcolor(red)  || ///
			 line avg cd4_mid  if source=="IHME" & sex==1, lpattern(dash) lcolor(black)  || ///
			 line avg cd4_mid  if source=="IHME" & sex==2, lpattern(solid) lcolor(black)  ///
			title(" ZAF: `dur' `age' ") legend(lab(1 "Spectrum, males") lab(2 "Spectrum, females") lab(3 "IHME, males") lab(4 "IHME, females"))
			
			pdfappend
			
			restore
		}
	
	pdffinish
	
	}	
	

** ************************************
// Compare Dismod outputs with Spectrum estimates
** ************************************
	// Prep Spectrum inputs... 

		insheet using "`spec_xbs'", clear 
			drop if strpos(region, "ART")>0
		tempfile tmp_spec
		save `tmp_spec', replace

	
	// Prep IHME estiamtes

		use "`tmp_prep'", clear 
			** rename draws
			forvalues i=1/1000 {
				rename mort`i' draw`i'
			}
			** generate means and CIs
			egen mort=rowmean(draw*)
			egen mort_lo= rowpctile(draw*), p(2.5)
			egen mort_hi= rowpctile(draw*), p(97.5)
			drop draw* 
			** rename regions... 
			gen region="HI_IHME" if super=="high"
			replace region="LA_IHME" if super=="other"
			replace region="SSA_IHME" if super=="ssa"
			replace region="SSA_BEST_IHME" if super=="ssabest"
			destring sex, replace 
		tempfile tmp_ihme_regions
		save `tmp_ihme_regions', replace

	
	// Append together and graph... 
		use "`tmp_spec'", clear
		append using "`tmp_ihme_regions'"
		** Generate graphs... 
				gen cd4_order=1 if cd4_cat=="ARTGT500CD4"
				replace cd4_order=2 if cd4_cat=="ART350to500CD4"
				replace cd4_order=3 if cd4_cat=="ART250to349CD4"
				replace cd4_order=4 if cd4_cat=="ART200to249CD4"
				replace cd4_order=5 if cd4_cat=="ART100to199CD4"
				replace cd4_order=6 if cd4_cat=="ART50to99CD4"
				replace cd4_order=7 if cd4_cat=="ARTLT50CD4"
				order region dur cd4_order age sex
		** keep regions of interest... 
			keep if region=="Developed Countries" | region=="HI_IHME" | region=="Southern Africa" | region=="Latin America and Caribbean" ///
			| region=="East Africa" | region=="West Africa" | region=="LA_IHME" | region=="SSA_IHME" | region=="SSA_BEST_IHME" | region=="Asia" | region=="North Africa Middle East"
			tostring sex, replace
			replace sex="Male" if sex=="1"
			replace sex="Female" if sex=="2"	
			replace age =subinstr(age,"_","-",.) // "
			replace age="15-25" if age=="15-24"
			replace age="25-35" if age=="25-34"
			replace age="35-45" if age=="35-44"
			replace age="45+" if age=="45-54" | age=="45-99"
		tempfile tmp_data
		save `tmp_data', replace 
		
		
		// Graph SSA to respond to reviewers
			// create a separate graph for each age, sex, duration.
			// On each graph show the mean and CI estimate for our mortality by CD4 count
			pdfstart using "`store_graphs'/Death_rts_IHME_v_UNAIDS_SSA.pdf"
			
			use "`tmp_data'", clear
				keep if region=="Southern Africa" | region=="East Africa" | region=="West Africa" | region=="SSA_IHME"
				levelsof sex, local(sexes)
				levelsof age, local(ages)
				
				foreach d in LT6Mo 6to12Mo GT12Mo {
					foreach s of local sexes {
						foreach a of local ages {
							preserve
							keep if sex=="`s'" & age=="`a'" & dur=="`d'"
							sort cd4_o
								twoway rarea mort_lo mort_hi cd4_o if region=="SSA_IHME", color(blue*.3) || ///
								line mort cd4_o if region=="SSA_IHME",  lcolor(blue) ||  ///
								line mort cd4_o if region=="Southern Africa" ,  lcolor(red) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if region=="East Africa" ,  lcolor(red) lwidth(medthick) lpattern(dot) || ///
								line mort cd4_o if region=="West Africa" ,  lcolor(red) lwidth(medthick) lpattern(dash_dot) ///
								title("SSA: `s', `a', `d'") subtitle("HIV mortality on ART using ALL sites: DisMod output", size(medsmall)) ///
								ytitle("HIV mort conditional prob on ART", size(small)) ///
								leg( lab(1 "IHME - All SSA") lab(2 "IHME - All SSA") lab(3 "UNAIDS, S.Af") lab(4 "UNAIDS, E.Af") lab(5 "UNAIDS, W.Af") ///
								row(3) size(vsmall)) ///
								xlabel(1 "{bf:>500}" 2 "{bf:350-499}" 3 "{bf:250-349}" 4 "{bf:200-249}" 5 "{bf:100-199}" ///
								6 "{bf:50-99}" 7 "{bf:<50}", labsize(vsmall)) xtitle("CD4 count category", size(medsmall)) ///
								ylabel(, labsize(vsmall) grid format(%12.0gc))
							pdfappend
							restore
						}
					}
				}
		
			pdffinish
			
			pdfstart using "`store_graphs'/Death_rts_IHME_v_UNAIDS_SSABEST.pdf"
			use "`tmp_data'", clear
				keep if region=="Southern Africa" | region=="East Africa" | region=="West Africa" | region=="SSA_BEST_IHME"
				levelsof sex, local(sexes)
				levelsof age, local(ages)
				
				foreach d in LT6Mo 6to12Mo GT12Mo {
					foreach s of local sexes {
						foreach a of local ages {
							preserve
							keep if sex=="`s'" & age=="`a'" & dur=="`d'"
							sort cd4_o
								twoway rarea mort_lo mort_hi cd4_o if region=="SSA_BEST_IHME", color(blue*.3) || ///
								line mort cd4_o if region=="SSA_BEST_IHME",  lcolor(blue) ||  ///
								line mort cd4_o if region=="Southern Africa" ,  lcolor(red) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if region=="East Africa" ,  lcolor(red) lwidth(medthick) lpattern(dot) || ///
								line mort cd4_o if region=="West Africa" ,  lcolor(red) lwidth(medthick) lpattern(dash_dot) ///
								title("SSA using BEST sites: `s', `a', `d'") subtitle("HIV mortality on ART: DisMod output", size(medsmall)) ///
								ytitle("HIV mort conditional prob on ART", size(small)) ///
								leg( lab(1 "IHME - Best SSA") lab(2 "IHME - Best SSA") lab(3 "UNAIDS, S.Af") lab(4 "UNAIDS, E.Af") lab(5 "UNAIDS, W.Af") ///
								row(3) size(vsmall)) ///
								xlabel(1 "{bf:>500}" 2 "{bf:350-499}" 3 "{bf:250-349}" 4 "{bf:200-249}" 5 "{bf:100-199}" ///
								6 "{bf:50-99}" 7 "{bf:<50}", labsize(vsmall)) xtitle("CD4 count category", size(medsmall)) ///
								ylabel(, labsize(vsmall) grid format(%12.0gc))
							pdfappend
							restore
						}
					}
				}
		
			pdffinish
			

		// Graph Regions
			pdfstart using "`store_graphs'/Death_rts_IHME_v_UNAIDS_regions.pdf"
				** High Income
				use "`tmp_data'", clear
					keep if region=="Developed Countries" | region=="HI_IHME" 
					levelsof sex, local(sexes)
					levelsof age, local(ages)
					foreach s of local sexes {
						foreach a of local ages {
							preserve
							keep if sex=="`s'" & age=="`a'"
							sort cd4_o
							drop *lo *hi
								line mort cd4_o if dur=="LT6Mo" & region=="HI_IHME",  lcolor(blue) lwidth(thick) ||  ///
								line mort cd4_o if dur=="LT6Mo" & region=="Developed Countries" ,  lcolor(blue) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if dur=="6to12Mo" & region=="HI_IHME",  lcolor(lime) lwidth(thick) || ///
								line mort cd4_o if dur=="6to12Mo" & region=="Developed Countries" ,  lcolor(lime) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if dur=="GT12Mo" & region=="HI_IHME",  lcolor(red) lwidth(thick) || ///
								line mort cd4_o if dur=="GT12Mo" & region=="Developed Countries" ,  lcolor(red) lwidth(medthick) lpattern(longdash)  ///
								title("High Income: `s', `a'") subtitle("HIV mortality on ART: DisMod output", size(medsmall)) ///
								ytitle("HIV mort conditional prob on ART", size(small)) ///
								leg( lab(1 "IHME: 0-6") lab(2 "UNAIDS: 0-6") lab(3 "IHME: 7-12") lab(4 "UNAIDS: 7-12")  ///
								lab(5 "IHME: 12-24") lab(6 "UNAIDS: 12-24") row(2) size(vsmall)) ///
								xlabel(1 "{bf:>500}" 2 "{bf:350-499}" 3 "{bf:250-349}" 4 "{bf:200-249}" 5 "{bf:100-199}" ///
								6 "{bf:50-99}" 7 "{bf:<50}", labsize(vsmall)) xtitle("CD4 count category", size(medsmall)) ///
								ylabel(, labsize(vsmall) grid format(%12.0gc))
							pdfappend
							restore
						}
					}
				** SSA
				use "`tmp_data'", clear
					keep if region=="Southern Africa" | region=="East Africa" | region=="West Africa" | region=="SSA_IHME"
					levelsof sex, local(sexes)
					levelsof age, local(ages)
					foreach s of local sexes {
						foreach a of local ages {
							preserve
							keep if sex=="`s'" & age=="`a'"
							sort cd4_o
							drop *lo *hi
								line mort cd4_o if dur=="LT6Mo" & region=="SSA_IHME",  lcolor(blue) lwidth(thick) ||  ///
								line mort cd4_o if dur=="LT6Mo" & region=="Southern Africa" ,  lcolor(blue) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if dur=="LT6Mo" & region=="East Africa" ,  lcolor(blue) lwidth(medthick) lpattern(dot) || ///
								line mort cd4_o if dur=="LT6Mo" & region=="West Africa" ,  lcolor(blue) lwidth(medthick) lpattern(dash_dot) || ///
								line mort cd4_o if dur=="6to12Mo" & region=="SSA_IHME",  lcolor(lime) lwidth(thick) ||  ///
								line mort cd4_o if dur=="6to12Mo" & region=="Southern Africa" ,  lcolor(lime) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if dur=="6to12Mo" & region=="East Africa" ,  lcolor(lime) lwidth(medthick) lpattern(dot) || ///
								line mort cd4_o if dur=="6to12Mo" & region=="West Africa" ,  lcolor(lime) lwidth(medthick) lpattern(dash_dot) || ///
								line mort cd4_o if dur=="GT12Mo" & region=="SSA_IHME",  lcolor(red) lwidth(thick) ||  ///
								line mort cd4_o if dur=="GT12Mo" & region=="Southern Africa" ,  lcolor(red) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if dur=="GT12Mo" & region=="East Africa" ,  lcolor(red) lwidth(medthick) lpattern(dot) || ///
								line mort cd4_o if dur=="GT12Mo" & region=="West Africa" ,  lcolor(red) lwidth(medthick) lpattern(dash_dot)  ///
								title("SSA: `s', `a'") subtitle("HIV mortality on ART: DisMod output", size(medsmall)) ///
								ytitle("HIV mort conditional prob on ART", size(small)) ///
								leg( lab(1 "IHME, SSA: 0-6") lab(2 "UNAIDS, S.Af: 0-6") lab(3 "UNAIDS, E.Af: 0-6") lab(4 "UNAIDS, W.Af: 0-6") ///
								lab(5 "IHME, SSA: 7-12") lab(6 "UNAIDS, S.Af: 7-12") lab(7 "UNAIDS, E.Af: 7-12") lab(8 "UNAIDS, W.Af: 7-12") ///
								lab(9 "IHME, SSA: 12-24") lab(10 "UNAIDS, S.Af: 12-24") lab(11 "UNAIDS, E.Af: 12-24") lab(12 "UNAIDS, W.Af: 12-24") ///
								row(3) size(vsmall)) ///
								xlabel(1 "{bf:>500}" 2 "{bf:350-499}" 3 "{bf:250-349}" 4 "{bf:200-249}" 5 "{bf:100-199}" ///
								6 "{bf:50-99}" 7 "{bf:<50}", labsize(vsmall)) xtitle("CD4 count category", size(medsmall)) ///
								ylabel(, labsize(vsmall) grid format(%12.0gc))
							pdfappend
							restore
						}
					}
				** Latin America 
				use "`tmp_data'", clear
					keep if region=="Latin America and Caribbean" | region=="LA_IHME" 
					levelsof sex, local(sexes)
					levelsof age, local(ages)
					foreach s of local sexes {
						foreach a of local ages {
							preserve
							keep if sex=="`s'" & age=="`a'"
							sort cd4_o
							drop *lo *hi
								line mort cd4_o if dur=="LT6Mo" & region=="LA_IHME",  lcolor(blue) lwidth(thick) ||  ///
								line mort cd4_o if dur=="LT6Mo" & region=="Latin America and Caribbean" ,  lcolor(blue) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if dur=="6to12Mo" & region=="LA_IHME",  lcolor(lime) lwidth(thick) || ///
								line mort cd4_o if dur=="6to12Mo" & region=="Latin America and Caribbean" ,  lcolor(lime) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if dur=="GT12Mo" & region=="LA_IHME",  lcolor(red) lwidth(thick) || ///
								line mort cd4_o if dur=="GT12Mo" & region=="Latin America and Caribbean" ,  lcolor(red) lwidth(medthick) lpattern(longdash)  ///
								title("Latin America and Caribbean: `s', `a'") subtitle("HIV mortality on ART: DisMod output", size(medsmall)) ///
								ytitle("HIV conditional prob on ART", size(small)) ///
								leg( lab(1 "IHME: 0-6") lab(2 "UNAIDS: 0-6") lab(3 "IHME: 7-12") lab(4 "UNAIDS: 7-12")  ///
								lab(5 "IHME: 12-24") lab(6 "UNAIDS: 12-24") row(2) size(vsmall)) ///
								xlabel(1 "{bf:>500}" 2 "{bf:350-499}" 3 "{bf:250-349}" 4 "{bf:200-249}" 5 "{bf:100-199}" ///
								6 "{bf:50-99}" 7 "{bf:<50}", labsize(vsmall)) xtitle("CD4 count category", size(medsmall)) ///
								ylabel(, labsize(vsmall) grid format(%12.0gc))
							pdfappend
							restore
						}
					}
			pdffinish
	
		
		// Graph durations
			pdfstart using "FILEPATH"
				** 0-6
				use "`tmp_data'", clear
					keep if dur=="LT6Mo"
					levelsof sex, local(sexes)
					levelsof age, local(ages)
					foreach s of local sexes {
						foreach a of local ages {
							preserve
							keep if sex=="`s'" & age=="`a'"
							sort cd4_o
							drop *lo *hi
								line mort cd4_o if region=="HI_IHME",  lcolor(blue) lwidth(thick) ||  ///
								line mort cd4_o if region=="Developed Countries" ,  lcolor(blue) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if region=="LA_IHME",  lcolor(lime) lwidth(thick) || ///
								line mort cd4_o if region=="Latin America and Caribbean" ,  lcolor(lime) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if region=="SSA_IHME",  lcolor(red) lwidth(thick) || ///
								line mort cd4_o if region=="Southern Africa" ,  lcolor(red) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if  region=="East Africa" ,  lcolor(red) lwidth(medthick) lpattern(dot) || ///
								line mort cd4_o if  region=="West Africa" ,  lcolor(red) lwidth(medthick) lpattern(dash_dot)   ///
								title("0-6 Mo Treat: `s', `a'") subtitle("HIV mortality on ART by region: DisMod output", size(medsmall)) ///
								ytitle("HIV mort cond prob on ART", size(small)) ///
								leg( lab(1 "IHME: HI") lab(2 "UNAIDS: HI") lab(3 "IHME: LA") lab(4 "UNAIDS: LA")  ///
								lab(5 "IHME: SSA") lab(6 "UNAIDS: S.Af") lab(7 "UNAIDS: E.Af")  lab(8 "UNAIDS: W.Af") row(2) size(vsmall)) ///
								xlabel(1 "{bf:>500}" 2 "{bf:350-499}" 3 "{bf:250-349}" 4 "{bf:200-249}" 5 "{bf:100-199}" ///
								6 "{bf:50-99}" 7 "{bf:<50}", labsize(vsmall)) xtitle("CD4 count category", size(medsmall)) ///
								ylabel(, labsize(vsmall) grid format(%12.0gc))
							pdfappend
							restore
						}
					}
					
				** 7-12
				use "`tmp_data'", clear
					keep if dur=="6to12Mo"
					levelsof sex, local(sexes)
					levelsof age, local(ages)
					foreach s of local sexes {
						foreach a of local ages {
							preserve
							keep if sex=="`s'" & age=="`a'"
							sort cd4_o
							drop *lo *hi
								line mort cd4_o if region=="HI_IHME",  lcolor(blue) lwidth(thick) ||  ///
								line mort cd4_o if region=="Developed Countries" ,  lcolor(blue) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if region=="LA_IHME",  lcolor(lime) lwidth(thick) || ///
								line mort cd4_o if region=="Latin America and Caribbean" ,  lcolor(lime) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if region=="SSA_IHME",  lcolor(red) lwidth(thick) || ///
								line mort cd4_o if region=="Southern Africa" ,  lcolor(red) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if  region=="East Africa" ,  lcolor(red) lwidth(medthick) lpattern(dot) || ///
								line mort cd4_o if  region=="West Africa" ,  lcolor(red) lwidth(medthick) lpattern(dash_dot)   ///
								title("7-12 Mo Treat: `s', `a'") subtitle("HIV mortality on ART by region: DisMod output", size(medsmall)) ///
								ytitle("HIV mort conditional prob on ART", size(small)) ///
								leg( lab(1 "IHME: HI") lab(2 "UNAIDS: HI") lab(3 "IHME: LA") lab(4 "UNAIDS: LA")  ///
								lab(5 "IHME: SSA") lab(6 "UNAIDS: S.Af") lab(7 "UNAIDS: E.Af")  lab(8 "UNAIDS: W.Af") row(2) size(vsmall)) ///
								xlabel(1 "{bf:>500}" 2 "{bf:350-499}" 3 "{bf:250-349}" 4 "{bf:200-249}" 5 "{bf:100-199}" ///
								6 "{bf:50-99}" 7 "{bf:<50}", labsize(vsmall)) xtitle("CD4 count category", size(medsmall)) ///
								ylabel(, labsize(vsmall) grid format(%12.0gc))
							pdfappend
							restore
						}
					}
				** 12-24
				use "`tmp_data'", clear
					keep if dur=="GT12Mo"
					levelsof sex, local(sexes)
					levelsof age, local(ages)
					foreach s of local sexes {
						foreach a of local ages {
							preserve
							keep if sex=="`s'" & age=="`a'"
							sort cd4_o
							drop *lo *hi
								line mort cd4_o if region=="HI_IHME",  lcolor(blue) lwidth(thick) ||  ///
								line mort cd4_o if region=="Developed Countries" ,  lcolor(blue) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if region=="LA_IHME",  lcolor(lime) lwidth(thick) || ///
								line mort cd4_o if region=="Latin America and Caribbean" ,  lcolor(lime) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if region=="SSA_IHME",  lcolor(red) lwidth(thick) || ///
								line mort cd4_o if region=="Southern Africa" ,  lcolor(red) lwidth(medthick) lpattern(longdash) || ///
								line mort cd4_o if  region=="East Africa" ,  lcolor(red) lwidth(medthick) lpattern(dot) || ///
								line mort cd4_o if  region=="West Africa" ,  lcolor(red) lwidth(medthick) lpattern(dash_dot)   ///
								title("12-24 Mo Treat: `s', `a'") subtitle("HIV mortality on ART by region: DisMod output", size(medsmall)) ///
								ytitle("HIV mort conditional prob on ART", size(small)) ///
								leg( lab(1 "IHME: HI") lab(2 "UNAIDS: HI") lab(3 "IHME: LA") lab(4 "UNAIDS: LA")  ///
								lab(5 "IHME: SSA") lab(6 "UNAIDS: S.Af") lab(7 "UNAIDS: E.Af")  lab(8 "UNAIDS: W.Af") row(2) size(vsmall)) ///
								xlabel(1 "{bf:>500}" 2 "{bf:350-499}" 3 "{bf:250-349}" 4 "{bf:200-249}" 5 "{bf:100-199}" ///
								6 "{bf:50-99}" 7 "{bf:<50}", labsize(vsmall)) xtitle("CD4 count category", size(medsmall)) ///
								ylabel(, labsize(vsmall) grid format(%12.0gc))
							pdfappend
							restore
						}
					}
			pdffinish
		
		
	/// GRAPH IN CUMULATIVE SPACE
		
		
		use `tmp_data', clear
			order region
			gen timepoint=6 if durationart=="LT6Mo"
			replace timepoint=12 if durationart=="6to12Mo"
			replace timepoint=24 if durationart=="GT12Mo"

			// calculate 6 month cumulative probability
				// convert the annual probability back into a rate
				gen mort_rate=-ln(1-mort)
				gen conditional_prob_timeperiod=1-exp(-mort_rate*.5) if timepoint==6 | timepoint==12
				replace conditional_prob_timeperiod=mort if timepoint==24
				expand=2 if timepoint==24, gen(dup)
				replace timepoint=36 if dup==1
				expand=2 if timepoint==36, gen(dup2)
				replace timepoint=48 if dup2==1
				expand=2 if timepoint==48, gen(dup3)
				replace timepoint=60 if dup3==1
				gen cumul_prob=conditional_prob if timepoint==6 
			
			order region age sex cd4_order timepoint cumul_prob conditional_prob_timeperiod
			
			sort region age sex cd4_order timepoint
			bysort region age sex cd4_order: replace cumul_prob=cumul_prob[_n-1]+((1-cumul_prob[_n-1])*conditional_prob_timeperiod) if timepoint==12
			bysort region age sex cd4_order: replace cumul_prob=cumul_prob[_n-1]+((1-cumul_prob[_n-1])*conditional_prob_timeperiod) if timepoint==24
			bysort region age sex cd4_order: replace cumul_prob=cumul_prob[_n-1]+((1-cumul_prob[_n-1])*conditional_prob_timeperiod) if timepoint==36
			bysort region age sex cd4_order: replace cumul_prob=cumul_prob[_n-1]+((1-cumul_prob[_n-1])*conditional_prob_timeperiod) if timepoint==48
			bysort region age sex cd4_order: replace cumul_prob=cumul_prob[_n-1]+((1-cumul_prob[_n-1])*conditional_prob_timeperiod) if timepoint==60

			expand=2 if timepoint==12, gen(dup4)
			replace timepoint=0 if dup4==1
			replace cumul_prob=0 if timepoint==0
			
			sort region age sex cd4_order timepoint

			gen source="IHME" if region=="HI_IHME" | region=="LA_IHME" | region=="SSA_IHME" | region=="SSA_BEST_IHME"
			replace source="UNAIDS" if region=="Developed Countries" | region=="East Africa" | region=="Latin America and Caribbean" | region=="Southern Africa" | region=="West Africa"
			
			tempfile pre
			save `pre', replace


		// FOR MANUSCRIPT - cumulative probability of mortality for 25-35 year olds; by CD4 count; by sex

		global color1 "215 48 39"
		global color2 "244 109 67"
		global color3 "253 174 97"
		global color4 "254 224 144"
/* 		global color1 "255 255 191"
		global color1 "224 243 248" */
		global color5 "171 217 233"
		global color6 "116 173 209"
		global color7 "69 117 180"

			use `pre', clear
			keep if region=="SSA_IHME"
			keep if age=="25-35"
			
			sort region cd4_cat timepoint
			
			pdfstart using "FILEPATH"
			twoway 	line cumul_prob timepoint if cd4_cat=="ARTLT50CD4", lcolor("$color1") || ///
					line cumul_prob timepoint if cd4_cat=="ART50to99CD4", lcolor("$color2") || ///
					line cumul_prob timepoint if cd4_cat=="ART100to199CD4", lcolor("$color3") || ///
					line cumul_prob timepoint if cd4_cat=="ART200to249CD4", lcolor("$color4") || ///
					line cumul_prob timepoint if cd4_cat=="ART250to349CD4", lcolor("$color5") || ///
					line cumul_prob timepoint if cd4_cat=="ART350to500CD4", lcolor("$color6") || ///
					line cumul_prob timepoint if cd4_cat=="ARTGT500CD4", lcolor("$color7") ///
					legend(lab(1 "<50") lab(2 "50-99") lab(3 "100-199") lab(4 "200-249") lab(5 "250-349") lab(6 "350-500") lab(7 ">500") symxsize(2) r(1) size(small) region(lwidth(none))) ///
					xlab(0 "0" 12 "1" 24 "2" 36 "3" 48 "4" 60 "5", labsize(small)) xtitle("Years since ART initiation", size(small)) ///
					ylabel(, labsize(small) angle(0) grid format(%12.0gc)) ytitle("Cumulative mortality", size(small)) ///
					by(sex, note("") title("HIV-specific mortality in sub-Saharan Africa by initial CD4 count" "Age group: 25-34", size(medsmall))) subtitle( ,fcolor(white))
				pdfappend
				pdffinish
	

		// AFRICA
		pdfstart using "FILEPATH"

			foreach age in 15-25 25-35 35-45 {
				foreach sex in Male Female {
					foreach cd4_cat in ARTLT50CD4 ART50to99CD4 ART100to199CD4 ART200to249CD4 ART250to349CD4 ART350to500CD4 ARTGT500CD4 {
						use `pre', clear
						keep if region=="East Africa" | region=="SSA_IHME" | region=="Southern Africa" | region=="West Africa" | region=="SSA_BEST_IHME"
						
						keep if sex=="`sex'"
						keep if age=="`age'"
						keep if cd4_cat=="`cd4_cat'"
						
						sort region  timepoint
						
						twoway line cumul_prob timepoint if region=="SSA_IHME", lcolor(blue) || ///
						line cumul_prob timepoint if region=="SSA_BEST_IHME", lcolor(blue) lpattern(dash) || ///
						line cumul_prob timepoint if region=="East Africa", lcolor(red) lpattern(dash) || ///
						line cumul_prob timepoint if region=="Southern Africa", lcolor(red) lpattern(dot) || ///
						line cumul_prob timepoint if region=="West Africa", lcolor(red) lpattern(dash_dot) ///
						legend(lab(1 "IHME") lab(2 "IHME top-performers") lab(3 "UNAIDS-East Africa") lab(4 "UNAIDS-Southern Africa") lab(5 "UNAIDS-West Africa")) ///
						title("Cumulative probability of mortality in Sub-Saharan Africa") subtitle("`cd4_cat'" "`sex'" "`age'") ///
						ysc(r(0(.1).4)) ylab(0(.1).4)
						
						pdfappend
				}
			}
		}
		
		pdffinish
		
	
		// HIGH
		pdfstart using "FILEPATH"
		
			foreach age in 15-25 25-35 35-45 {
				foreach sex in Male Female {
					foreach cd4_cat in ARTLT50CD4 ART50to99CD4 ART100to199CD4 ART200to249CD4 ART250to349CD4 ART350to500CD4 ARTGT500CD4 {
						use `pre', clear
						keep if region=="HI_IHME" | region=="Developed Countries"
						
						keep if sex=="`sex'"
						keep if age=="`age'"
						keep if cd4_cat=="`cd4_cat'"
						
						sort region  timepoint
						
						twoway line cumul_prob timepoint if region=="HI_IHME", lcolor(blue) || ///
						line cumul_prob timepoint if region=="Developed Countries", lcolor(red) ///
						legend(lab(1 "IHME") lab(2 "UNAIDS")) ///
						title("Cumulative probability of mortality in High Income") subtitle("`cd4_cat'" "`sex'" "`age'") ///
						ysc(r(0(.1).4)) ylab(0(.1).4)
						
						pdfappend
						
				}
			}
		}
		pdffinish
		
		
		// OTHER
		pdfstart using "FILEPATH"
			foreach age in 15-25 25-35 35-45 {
				foreach sex in Male Female {
					foreach cd4_cat in ARTLT50CD4 ART50to99CD4 ART100to199CD4 ART200to249CD4 ART250to349CD4 ART350to500CD4 ARTGT500CD4 {
						use `pre', clear
						keep if region=="LA_IHME" | region=="Asia" | region=="North Africa Middle East" | region=="Latin America and Caribbean"
						
						keep if sex=="`sex'"
						keep if age=="`age'"
						keep if cd4_cat=="`cd4_cat'"
						
						sort region  timepoint
						
						twoway line cumul_prob timepoint if region=="LA_IHME", lcolor(blue) || ///
						line cumul_prob timepoint if region=="Asia", lcolor(red) lpattern(dash) || ///
						line cumul_prob timepoint if region=="North Africa Middle East", lcolor(red) lpattern(dot) || ///
						line cumul_prob timepoint if region=="Latin America and Caribbean", lcolor(red) lpattern(long_dash) ///
						legend(lab(1 "IHME-Other") lab(2 "UNAIDS-Asia") lab(3 "UNAIDS-North Africa Middle East") lab(4 "UNAIDS-Latin America Caribbean")) ///
						title("Cumulative probability of mortality in Other low/middle income") subtitle("`cd4_cat'" "`sex'" "`age'") ///
						ysc(r(0(.1).4)) ylab(0(.1).4)
						
						pdfappend
						
				}
			}
		}
		pdffinish
	

	}




