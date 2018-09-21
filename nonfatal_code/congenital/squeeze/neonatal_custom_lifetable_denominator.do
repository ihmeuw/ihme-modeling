
** "Life table" model for neonatal prevalence of anencephaly
** {AUTHOR NAME}
** May 2017
**------------------------------------------


** setup 
clear all
set more off
cap restore, not
set maxvar 30000

	/ set prefix for the OS 
	if c(os) == "Windows" {
		global dl "{FILEPATH}"
	}
	if c(os) == "Unix" {
		global dl "{FILEPATH}"
		set odbcmgr unixodbc
	}

	local function_dir "$dl/{FILEPATH}"
		run "`function_dir'/get_location_metadata.ado"

local dir "$dl/WORK/12_bundle"
local date 4_19_17


** draws of live births by sex are saved in individual files by location_id, and are long on draw:
	local draw_files: dir "{FILEPATH}" files *

		clear
		foreach ff in `draw_files' {
			di "`ff'"
			append using "{FILEPATH}/`ff'"
			}

			tempfile all_births
			save `all_births', replace 

	** example: use "{FILEPATH}", clear  
		keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016)
		drop ihme_loc_id sex
		drop if sex_id==3

		*want draws to be 0-999 rather than 1-1000
		replace sim = sim-1

		reshape wide births, i(location_id year_id sex_id) j(sim)
		rename births* births_*
		
		*export file to troubleshoot and save time on reruns
		*export delimited using "$dl/{FILEPATH}", replace
		tempfile births_prepped
		save `births_prepped', replace 
	
		*import delimited using "$dl/temp/neonatal_denom/births_prepped.csv", clear 
		*tempfile births_prepped
		*save `births_prepped', replace 

*----------------------------------------------
** pull mortality estimates:
	** draws are saved in individual files by location_id, and are long on draw
	
	** example: import delimited using "{FILEPATH}", clear 

	local draw_files: dir "{FILEPATH}" files *

		foreach ff in `draw_files' {
			di "`ff'"
		import delimited using "{FILEPATH}/`ff'", clear 
			local f = subinstr("`ff'", ".csv", "", .)
			local loc_id = subinstr("`f'", "qx_", "", .)
				gen location_id = "`loc_id'"

				keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016)
				keep if inlist(age_group_id, 2, 3)
				drop if sex_id==3

				tempfile `f'_file
				save ``f'_file', replace 
				}

		** ran to here, come back and append the files
		clear
		foreach ff in `draw_files' {
			local f = subinstr("`ff'", ".csv", "", .)
			append using ``f'_file'
			}
			*export file to troubleshoot and save time on reruns
			export delimited using "$dl/{FILEPATH}", replace
			tempfile all_qx
			save `all_qx', replace 

	*import delimited using "$dl/temp/neonatal_denom/all_qx.csv", clear
	*tempfile all_qx
	*save `all_qx', replace 
 	
	use `all_qx', clear 

		reshape wide qx, i(location_id year_id sex_id age_group_id) j(draw)
		rename qx* qx_*

		
		*export file to troubleshoot and save time on reruns
		export delimited using "$dl/{FILEPATH}", replace
		*import delimited using "$dl/{FILEPATH}", clear
		tempfile qx_prepped
		save `qx_prepped', replace


	*import delimited using "$dl/{FILEPATH}", clear 
	*	tempfile births_prepped
	*	save `births_prepped', replace
	*import delimited using "$dl/{FILEPATH}", clear
	*	tempfile qx_prepped
	*	save `qx_prepped', replace

**------------------------------------------
** Merge births to population
use `qx_prepped', clear 
	destring location_id, replace 	
	merge m:1 location_id year_id sex_id using `births_prepped'
		rename _m _merge1


**--------------------
** Merge births, population and mortality estimates; keep only locations that are most_detailed==1 and is_estimate==1

	preserve
	get_location_metadata, location_set_id(9) clear 
	keep if (most_detailed==1 & is_estimate==1)
		tempfile locs 
		save `locs', replace
	restore

	merge m:1 location_id using `locs', keepusing(location_name is_estimate most_detailed) keep(3)
	count if _merge1 !=3
	drop _merge* is_estimate most_detailed

	tempfile births_and_qx
	save `births_and_qx', replace

*---------------------------------------------------
** Calculate time lived by neonates in the population
	
	** ax = average time lived by those who die in the age 
		** NID 307273: "use a_enn = (0.6 + 0.4*3.5)/365  this assumes that 60% of early neonates who die do so in the first day of life and that the rest die, on average, in the middle of the period"
		** NID 307274: "use a_lnn = 7/365, assuming that on average late neonates die in the first 7 days of the age period."

	local a_enn = ((0.6 + 0.4*3.5) / 365)
	local a_lnn = 7/365

	local age_start_enn = 0
	local age_end_enn = 7/365

	local age_start_lnn = 7/365
	local age_end_lnn = 28/365
	
	** reshape to be wide on age_group, even though this is VERY wide 
		tostring age_group_id, replace
		replace age_group_id="enn" if age_group_id=="2"
		replace age_group_id="lnn" if age_group_id=="3"

	reshape wide births_* qx_*, i(location_id year_id sex_id) j(age_group_id) string 
		rename (*enn *lnn) (enn* lnn*)
		rename (lnnqx* ennqx*) (qx_lnn* qx_enn*)
		** ennbirths_* lnnbirths_* are the same
		** we only need one of them
		drop ennbirths_*
		rename lnnbirths_* births_*

		forvalues i = 0/999 {
			di "`i'"

	** calc number of infants until day 7 (this is the starting population for the LNN period)
		gen pop_surv_enn_`i' = births_`i' * (1-(qx_enn_`i'))
		
	** calc number of infants until day 28 (this is the starting population for the PNN period)
		gen pop_surv_lnn_`i' = pop_surv_enn_`i' * (1-(qx_lnn_`i'))
		
	** calc life-years in early neonatal pop
		gen ly_enn_`i' = births_`i' * (`a_enn' * qx_enn_`i') + births_`i' *((`age_end_enn' - `age_start_enn')*(1-qx_enn_`i'))

	** calc life-years in late neonatal pop
		gen ly_lnn_`i' = (pop_surv_enn_`i') * (`a_lnn' * qx_lnn_`i') + ((`age_end_lnn' - `age_start_lnn')*(1-qx_lnn_`i'))*(pop_surv_enn_`i')
		
	** calc all-cause mortality rate for early neonatal pop
		gen mort_rate_enn_`i' = (births_`i' - pop_surv_enn_`i') / ly_enn_`i'
		
	** calc all-cause mortality rate for late neonatal pop
		gen mort_rate_lnn_`i' = (births_`i' - pop_surv_lnn_`i') / ly_lnn_`i'
		}
	
	order location_id location_name year_id sex_id births_* pop_surv_enn* pop_surv_lnn* qx_enn* qx_lnn* ly_enn* ly_lnn* mort_rate_enn* mort_rate_lnn*

	tempfile denominators 
	save `denominators', replace
	
	export delimited using "$dl/{FILEPATH}", replace
** This gives is the denominators (life-years lived during ENN and LNN periods for each location/year/sex combination). Numerators are calculated separately depending on model needs**
			


	








