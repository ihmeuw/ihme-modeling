** ****************************************************
** Purpose: Move miscalculated HIV deaths, which are an artifact of redistribtion 
** ******************************************************************************************************************
 ** Prep Stata
	clear all
	set more off, perm
	if c(os) == "Unix" {
		set mem 10G
		set odbcmgr unixodbc
		global j "/home/j"
	}
	else if c(os) == "Windows" {
		set mem 800m
		global j "J:"
	}
	quietly do "$j/WORK/10_gbd/00_library/functions/fastcollapse.ado"
	
** Source
	global source "`1'"

** Date
	global timestamp "`2'"
	
**  Local for the directory where all the COD data are stored
	local in_dir "$j/WORK/03_cod/01_database/03_datasets/${source}"
	
** Log output
	capture log close _all
	log using "`in_dir'//logs/06_hiv_correction_${timestamp}", replace

** Identify source list
	insheet using "$j/WORK/03_cod/01_database/02_programs/hiv_correction/VR_list.csv", comma names clear
	count if vr_list == "$source"
	local VR_count = `r(N)'
	
** Get parent_ids for county aggregation
	odbc load, exec("SELECT location_id, location_parent_id AS parent_id FROM shared.location") strConnection clear
	tempfile parent_ids
	save `parent_ids', replace
	
** Read post-redistribution file
	use "`in_dir'//data/final/05b_for_compilation.dta", clear
	
** Aggregate counties
	if index("$source","US_NCHS_counties") {
		** Don't trust assert...
		merge m:1 location_id using `parent_ids', keep(1 3)
		count if _m == 1
		if `r(N)' > 0 {
			di "MISMATCHED COUNTIES PRESENT"
			BREAK
		}
		replace location_id = parent_id
		fastcollapse *deaths*, by(location_id dev_status region iso3 national subdiv source source_label source_type NID list year sex im_frmat frmat acause) type(sum)
		gen beforeafter = .
	}
	
** Apply correction (if there are data that need adjustment)
	if `VR_count' > 0 {
		** Run program
		noisily do  "$j/WORK/03_cod/01_database/02_programs/hiv_correction/reallocation_program/code/hiv_correction_program.do" "POST"
	}

** Add South Africa injury redistribution
	if "$source" == "South_Africa_by_province" {
		** Prepare proportions
		sum deaths1
		return list
		local dstart = `r(sum)'
		preserve
			** Save injury data
			keep if index(acause,"inj")
			tempfile orig
			save `orig', replace
			fastcollapse deaths1, by(iso3 year sex) type(sum)
			tempfile ZAF_inj
			save `ZAF_inj', replace

			** Save pseudo-RDP
			insheet using "$j/WORK/03_cod/01_database/03_datasets/South_Africa_by_province/data/injury_proportions.csv", comma names clear
			keep if most_detailed == 1
			keep acause rdp*
			reshape long rdp, i(acause) j(sex)
			egen double rdp_fix = pc(rdp), by(sex) prop
			drop rdp
			joinby sex using `ZAF_inj', unmatched(both)
			replace deaths1 = deaths1*rdp_fix
			drop rdp_fix _m
			rename deaths1 inj_env
			save `ZAF_inj', replace

			** Get age patterns
			use `orig', clear
			replace acause = "inj_poisoning" if index(acause,"inj_poisoning")
			replace acause = "inj_suicide" if index(acause,"inj_suicide")
			replace acause = "inj_war" if index(acause,"inj_war")
			** ** ** **
			** The homicide and road injury parents have a fraction of a death each, just drop them
			drop if inlist(acause,"inj_homicide","inj_trans_road")
			** ** ** **
			fastcollapse *deaths*, by(location_id dev_status region iso3 national subdiv source source_label source_type NID list year sex im_frmat frmat acause) type(sum)
			egen double province_split = pc(deaths1), prop by(sex year acause)
			foreach age_val of numlist 3 7/22 91 93 94 {
				gen double prop`age_val' = deaths`age_val'/deaths1
				replace prop`age_val' = prop`age_val'*province_split
			}
			drop deaths* province_split

			** Join on death totals and apply
			merge m:1 iso3 year sex acause using `ZAF_inj', assert(3) nogen
			foreach age_val of numlist 3 7/22 91 93 94 {
				gen double deaths`age_val' = prop`age_val'*inj_env
			}
			drop prop* inj_env
			aorder
			egen deaths1 = rowtotal(deaths*)
			egen deaths2 = rowtotal(deaths91 deaths93 deaths94)
			aorder
			order location_id dev_status region iso3 national subdiv source source_label source_type NID list year sex im_frmat frmat acause
			save `ZAF_inj', replace
		restore
		drop if index(acause,"inj")
		append using `ZAF_inj'
		summ deaths1
		return list
		assert abs(`r(sum)'-`dstart') < 1
	}
	
** Save product
	save "`in_dir'//data/final/06_hiv_corrected.dta", replace
	save "`in_dir'//data/final/_archive/06_hiv_corrected_${timestamp}.dta", replace
	sleep 100
	
	capture log close all