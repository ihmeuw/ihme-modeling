** ************************************************************************************************* **
** Purpose: Redistribute deaths from specific tabulated cause codes onto ICD detailed cause codes using ICD detailed VR proportions. Template uses ICD9 BTL as an example.		
** ************************************************************************************************* **
// Load data and super regions 
	use "$formatted_steps/detailed_recodes.dta", clear
	preserve
	do "$j/WORK/03_cod/01_database/03_datasets/ICD9_BTL/code/BTL_two_step_fraction.do"
	** get region ids
		use iso3 type indic_cod gbd_country_iso3 gbd_non_developing gbd_superregion_local if inlist(type, "admin0","admin1","nonsovereign","urbanicity") & indic_cod == 1 using "$j/DATA/IHME_COUNTRY_CODES/IHME_COUNTRY_CODES_Y2013M07D26.DTA", clear
		** make iso3s the same as they are in the database
		replace iso3 = gbd_country_iso3 if gbd_country_iso3 !=""
		** make region a numerical code
		replace gbd_superregion_local = subinstr(gbd_superregion_local, "S", "", .)
		destring gbd_superregion_local, replace
		** rename for clarity 
		rename gbd_superregion_local super_region
		drop gbd_country_iso3 type indic_cod
		duplicates drop
		tempfil geo
		save `geo', replace
	restore
			
	// Merge super regions and get fractions for each country
		merge m:1 iso3 using `geo', keepusing(super_region) keep(1 3) nogen
	** Use global proportions in locations where we don't have super region. 
		replace super_region = 0 if super_region == .
		** If we are missing proportions in a location, change to global
		replace super_region = 0 if super_region == 4 | super_region == 5	
	** Custom proportions
	** Use fractions from C Europe, E Europe, C Asia for ckd in South Korea
		replace super_region = 2 if iso3 == "KOR" & cause == "B269"
	
	** Apply the fractions
		joinby super_region cause sex frmat im_frmat using "${rdp_frac}", unmatched(master)
		bysort iso3 : egen check = max(_m)
		replace super_region = 0 if check == 1
		tab _m
		rename _m first_m
		drop num check
		
		** More Custom proportions
		** Swap out super regs for global proportions in Latin America
			foreach prop of varlist percent* {
				replace `prop' = . if cause == "B529"
			}
			replace super_region = 0 if cause == "B529"

	** Apply fractions where we are missing
		joinby super_region target cause sex frmat im_frmat using "${rdp_frac_global}", unmatched(master) update
		tab iso3 _m
		
	** More Custom proportions
	** Adjust proportions for ARG, CHL, and URY ischaemic stroke and hemorrhagic
		gen keep = 1 if inlist(iso3, "ARG", "CHL", "URY") & cause == "B299" 			
		sort sex iso3 location_id year frmat im_frmat cause target
		preserve
			keep if keep == 1
			foreach prop of varlist percent* {
				gen double move_30`prop' = `prop' * .30 if target == "cvd_stroke_isch" 
				replace move_30`prop' = move_30`prop'[_n+1] if target == "cvd_stroke_cerhem" 
				replace `prop' = `prop' - move_30`prop' if target == "cvd_stroke_isch" 
				replace `prop' = `prop' + move_30`prop' if target == "cvd_stroke_cerhem" 
				drop move_30*
			}
			tempfile stroke
			save `stroke', replace
		restore
		drop if keep == 1
		append using `stroke'
		drop keep
		sort sex iso3 location_id year frmat im_frmat cause target		
		
		** Adjust the deaths with fractions 
		forvalues i = 1/26 {
			replace deaths`i' = deaths`i' * percent`i' if _m==3 | first_m == 3
			}
		forvalues i = 91/94 {
			replace deaths`i' = deaths`i' * percent`i' if _m==3 | first_m == 3
			}
			
	** Clean up		
	replace cause = "acause_" + target if target!=""
	drop _merge target percent* num first_m

// Double check age formats and deaths
    // separate age-missing observation for age-sex splitting
		egen tmp = rowtotal(deaths3-deaths25 deaths91-deaths94)
		** tag deaths26 observations which are in line with deaths in other ages
		gen tag = 1 if tmp!=0 & deaths26>0 & deaths26!=.
		expand 2 if tag==1, gen(new)
		replace deaths26 = 0 if tag==1 & new==0
		replace frmat = 9 if tag == 1 & new == 1
		replace im_frmat = 8 if tag == 1 & new == 1
		** zero out deaths in other ages in the new deaths26 observation
		aorder
		foreach var of varlist deaths2-deaths25 deaths91-deaths94 {
			replace `var' = 0 if tag==1 & new==1
		}
		replace deaths1 = deaths26 if tag==1 & new==1
		drop tmp tag new
		** now check that rows that only have deaths26 are assigned to frmat9 for age-sex splitting
		egen tmp = rowtotal(deaths2-deaths25 deaths91-deaths94)
		replace frmat = 9 if tmp==0 & tmp!=. & deaths26!=0 & deaths26!=.
		replace im_frmat = 8 if tmp==0 & tmp!=. & deaths26!=0 & deaths26!=.
		drop tmp
	// recalculate deaths1
		 capture drop deaths1
		 foreach i of numlist 2/26 91/94 {
			capture gen deaths`i' = 0
		 }
		 aorder
		 egen deaths1 = rowtotal(deaths3-deaths94)