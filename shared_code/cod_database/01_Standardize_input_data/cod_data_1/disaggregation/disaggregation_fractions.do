** ************************************************************************************************* **
** Purpose: Generate proportions by location, cause, age, and sex based off fractions from ICD detail VR used to disaggregate tabulated cause lists. Use ICD9 BTL as an example
** ************************************************************************************************* **
// set the prefix for whichever os we're in
	if c(os) == "Windows" {
		global j "J:"
	}
	if c(os) == "Unix" {
		global j "/home/j"
	}
** set up fastcollapse
	do "$j/WORK/04_epi/01_database/01_code/04_models/prod/fastcollapse.ado"

** ************************************************************************************	
	
** define and clean super regions
	use iso3 type indic_cod gbd_country_iso3 gbd_non_developing gbd_superregion_local if inlist(type, "admin0","admin1","nonsovereign","urbanicity") & indic_cod == 1 using "$j/DATA/IHME_COUNTRY_CODES/IHME_COUNTRY_CODES_Y2013M07D26.DTA", clear
	** make iso3s the same as they are in the database
	replace iso3 = gbd_country_iso3 if gbd_country_iso3 !=""
	** make region a numerical code
	replace gbd_superregion_local = subinstr(gbd_superregion_local, "S", "", .)
	destring gbd_superregion_local, replace
	** clean 
	rename gbd_superregion_local super_region
	drop gbd_country_iso3 type indic_cod
	duplicates drop
	tempfil geo
	save `geo', replace

** bring in the stat-transfered and cleaned map of summary to target codes
	use "$j/WORK/03_cod/01_database/03_datasets/ICD9_BTL/maps/cause_list/Two_step_list_ICD9_detail.dta", clear
	** expand to create fractions by sex
	expand 3, gen(new)
	bysort cause_code target: gen n = _n
	rename n sex
	replace sex = 9 if sex == 3
	drop new
	** expand to create regions
	expand 8
	bysort cause_code target sex: gen super_region = _n
	replace super_region = super_region-1
	rename cause_code cause
	tempfile btl
	save `btl'

	
** use ICD9 detail data and ICD9 dependent VR sources at the total deaths per code level to make the fractions
	clear
	gen foo = .
	tempfile master
	save `master', replace
	local source_list ICD9_detail US_NCHS_counties_ICD9 Sweden_ICD9 Japan_by_prefecture_ICD9 Brazil_SIM_ICD9 UK_1981_2000 NZL_MOH_ICD9 Zimbabwe_95
	foreach file of local source_list {
		use "$j/WORK/03_cod/01_database/03_datasets/`file'/data/intermediate/01_mapped.dta", clear
		fastcollapse deaths*, by(cause acause iso3 frmat im_frmat sex) type(sum)
		append using `master'
		di in red "appended `file' source"
		save `master', replace
	}
	merge m:1 iso3 using `geo', assert(2 3) keep(3) nogen
	expand 2, gen(new)
	replace super_region = 0 if new == 1
	drop new
	keep if frmat == 2 & im_frmat == 2 & sex<3
	expand 2, gen(new)
	replace sex = 9 if new == 1
	fastcollapse deaths*, by(cause acause sex super_region) type(sum)
	renpfix deaths input
	gen target = substr(cause,1,3)
	replace target = substr(cause,1,4) if substr(cause,1,1)=="E"
	rename cause cause_code
	replace acause = "neo_liver" if acause == "neo_colorectal_cancer" & cause_code == "1559"
	tempfil detail
	save `detail', replace

** merge on the deaths and create fractions
	use `btl', clear
	joinby target sex super_region using `detail', unmatched(master)
	egen target_exists = max(_m), by(target)
	levelsof target if _m==1 & target_exists == 1, local(missing_targets)
	display in red "These causes are not in the data"
	keep if _m==3
	replace target = cause_code
	drop _m target_exists cause_code
	** For B099 only target 159.9, not 159 and other 4 digits beginning with 159
	display in red "Drop these from the target list"
	tab target if strmatch(target, "159*")
	drop if cause == "B099" & inlist(target, "159", "1590","1591","1596","1598")
	tempfile k_btl
	save `k_btl', replace 
	
** In data we have 9 different frmat im_frmat combinations, make the inputs for each of them
	expand 9, gen(new)
	bysort cause acause target sex super_region new: gen n = _n
	** before we start adding things up, make sure that any missing values are zero
		foreach i of numlist 1/26 91/94 {
			replace input`i' = 0 if input`i' == .
			}
	** first if frmat 2 im_frmat 2
		gen frmat = 2
		gen im_frmat = 2
	** second is frmat 2 im_frmat 8
		replace im_frmat = 8 if n == 1 & new == 1
		replace input91 = (input91 + input93 + input94) if n == 1 & new == 1
		replace input93 = 0 if n == 1 & new == 1
		replace input94 = 0 if n == 1 & new == 1
	** third is frmat 4 im_frmat 2
		replace frmat = 4 if n == 2 & new == 1
		replace input21 = (input21+input22+input23) if n == 2 & new == 1
		replace input22 = 0 if n == 2 & new == 1
		replace input23 = 0 if n == 2 & new == 1
	** fourth is frmat 4 im_frmat 8
		replace frmat = 4 if n == 3 & new == 1
		replace im_frmat = 8 if n == 3 & new == 1
		replace input21 = (input21+input22+input23) if n == 3 & new == 1
		replace input22 = 0 if n == 3 & new == 1
		replace input23 = 0 if n == 3 & new == 1
		replace input91 = (input91 + input93 + input94) if n == 3 & new == 1
		replace input93 = 0 if n == 3 & new == 1
		replace input94 = 0 if n == 3 & new == 1
	** fifth is frmat 5 im_frmat 8
		replace frmat = 5 if n == 4 & new == 1
		replace im_frmat = 8 if n == 4 & new == 1
		replace input20 = (input20+input21+input22+input23) if n == 4 & new == 1
		replace input21 = 0 if n == 4 & new == 1
		replace input22 = 0 if n == 4 & new == 1
		replace input23 = 0 if n == 4 & new == 1
		replace input91 = (input91 + input93 + input94) if n == 4 & new == 1
		replace input93 = 0 if n == 4 & new == 1
		replace input94 = 0 if n == 4 & new == 1
	** sixth is frmat 7 im_frmat 2
		replace frmat = 7 if n == 5 & new == 1
		foreach i of numlist 7 9 11 13 15 17 19 {
			local j = `i' + 1
			replace input`i' = input`i' + input`j' if n == 5 & new == 1
			replace input`j' = 0 if n == 5 & new == 1
		}
		replace input21 = (input21+input22+input23) if n == 5 & new == 1
		replace input22 = 0 if n == 5 & new == 1
		replace input23 = 0 if n == 5 & new == 1
	** seventh is frmat 7 im_frmat 8
		replace frmat = 7 if n == 6 & new == 1
		replace im_frmat = 8 if n == 6 & new == 1
		foreach i of numlist 7 9 11 13 15 17 19 {
			local j = `i' + 1
			replace input`i' = input`i' + input`j' if n == 6 & new == 1
			replace input`j' = 0 if n == 6 & new == 1
		}
		replace input21 = (input21+input22+input23) if n == 6 & new == 1
		replace input22 = 0 if n == 6 & new == 1
		replace input23 = 0 if n == 6 & new == 1
		replace input91 = (input91 + input93 + input94) if n == 6 & new == 1
		replace input93 = 0 if n == 6 & new == 1
		replace input94 = 0 if n == 6 & new == 1
	** eight is frmat 8 im_frmat 8s
		replace frmat = 8 if n == 7 & new == 1
		replace im_frmat = 8 if n == 7 & new == 1
		foreach i of numlist 7 9 11 13 15 17 {
			local j = `i' + 1
			replace input`i' = input`i' + input`j' if n == 7 & new == 1
			replace input`j' = 0 if n == 7 & new == 1
		}
		replace input19 = (input19+input20+input21+input22+input23) if n == 7 & new == 1
		replace input20 = 0 if n == 7 & new == 1
		replace input21 = 0 if n == 7 & new == 1
		replace input22 = 0 if n == 7 & new == 1
		replace input23 = 0 if n == 7 & new == 1
		replace input91 = (input91 + input93 + input94) if n == 7 & new == 1
		replace input93 = 0 if n == 7 & new == 1
		replace input94 = 0 if n == 7 & new == 1
	** ninth (and last) is frmat 9 im_frmat 8
		replace frmat = 9 if n == 8 & new == 1
		replace im_frmat = 8 if n == 8 & new == 1
		replace input26 = input1
		foreach i of numlist 2/25 91/94 {
			replace input`i' = 0 if n == 8 & new == 1
		}

** make percentages
	foreach i of numlist 1/26 91/94 {
		bysort cause sex super_region frmat im_frmat: egen double percent`i' = pc(input`i'), prop
	}
	drop input*
	
** Manual garbage recodes
	replace acause = "endo" if target == "244"
	replace acause = "nutrition" if target == "264"
	replace acause = "mental_drug" if target == "296"
	replace acause = "mental_drug" if target == "298"
	replace acause = "cvd_ihd" if target == "426"
	replace acause = "gyne" if target == "619"
	replace acause = "gyne" if target == "621"
	replace acause = "gyne" if target == "622"
	replace acause = "gyne" if target == "623"
	replace acause = "gyne" if target == "624"
	replace acause = "gyne" if target == "625"

** keep target cause in the _gc acauses
	replace acause = acause + "_" + target if acause=="_gc"
	collapse(sum) percent*, by(cause acause sex frmat im_frmat super_region) fast
	bysort cause sex frmat im_frmat super_region: gen num = _N
	rename acause target
	
** Save
	preserve
	drop if super_region == 0
	save "$j/WORK/03_cod/01_database/03_datasets/ICD9_BTL/maps/cause_list/Two_step_fractions_ICD9_super_region.dta", replace
	capture saveold "$j/WORK/03_cod/01_database/03_datasets/ICD9_BTL/maps/cause_list/Two_step_fractions_ICD9_super_region.dta", replace
	restore
	
	preserve
	keep if super_region == 0
	save "$j/WORK/03_cod/01_database/03_datasets/ICD9_BTL/maps/cause_list/Two_step_fractions_ICD9_global.dta", replace
	capture saveold "$j/WORK/03_cod/01_database/03_datasets/ICD9_BTL/maps/cause_list/Two_step_fractions_ICD9_global.dta", replace	
	restore
	
** Ensure proportions exist for every age group in every super region. Use global proportions if possible. If global proportions don't exist, fill in proportionally. 
	levelsof super_region if super_region != 0, local(regions)
	tostring super_region, replace
	replace super_region = "_" + super_region
	drop num
	reshape wide percent*, i(cause target sex frmat im_frmat) j(super_region) string

	gen x = 1
	foreach n of numlist 1/26 91/94 {
		egen tot_percent`n'_0 = total(percent`n'_0), by(cause sex frmat im_frmat)
		egen prop_percent`n' = pc(x), prop by(cause sex frmat im_frmat)
		replace percent`n'_0 = prop_percent`n' if round(tot_percent`n'_0,0.1) == 0.0
		foreach region of local regions {
			egen tot_percent`n'_`region' = total(percent`n'_`region'), by(cause sex frmat im_frmat)
			replace percent`n'_`region' = percent`n'_0 if round(tot_percent`n'_`region',0.1) == 0.0
		}
	}
	drop tot_percent* x prop_percent*
	reshape long percent1 percent2 percent3 percent4 percent5 percent6 percent7 percent8 percent9 percent10 percent11 percent12 percent13 percent14 percent15 percent16 percent17 percent18 percent19 percent20 percent21 percent22 percent23 percent24 percent25 percent26 percent91 percent92 percent93 percent94, i(cause target sex frmat im_frmat) j(super_region) string
	replace super_region = subinstr(super_region,"_","",.)
	destring super_region, replace	
	

** Save final file to be used
	collapse(sum) percent*, by(cause target sex frmat im_frmat super_region) fast
	bysort cause sex frmat im_frmat super_region: gen num = _N
	capture export excel using "$j/WORK/03_cod/01_database/03_datasets/ICD9_BTL/maps/cause_list/Two_step_fractions_ICD9.xlsx", firstrow(variables) replace
	saveold "$j/WORK/03_cod/01_database/03_datasets/ICD9_BTL/maps/cause_list/Two_step_fractions_ICD9.dta", replace
	
	capture export excel using "$j/WORK/03_cod/01_database/03_datasets/ICD9_BTL/maps/cause_list/archive/GBD2015/Two_step_fractions_ICD9_${date}.xlsx", firstrow(variables) replace
	saveold "$j/WORK/03_cod/01_database/03_datasets/ICD9_BTL/maps/cause_list/archive/GBD2015/Two_step_fractions_ICD9_${date}.dta", replace
