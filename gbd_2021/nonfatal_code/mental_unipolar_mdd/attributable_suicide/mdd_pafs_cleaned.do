
qui {
clear all
pause on
set maxvar 32767, perm
set more off, perm
	if c(os) == "Windows" {
		global j "J:"
		set mem 1g
	}
	if c(os) == "Unix" {
		global j "/home/j"
		set mem 2g
		set odbcmgr unixodbc
	}
	if c(os) == "MacOSX" {
		global j "/Volumes/snfs"
	}

set seed 745026

//if "`2'" == "" {

	local risk = "Major depressive disorder"
	local rei_id = 346											
	//local location_id = "`2'"
	//local year_id = 2015
	//local sex_id = 1

import delimited using "FILEPATH", varnames(1)
levelsof location_id, local(locations) clean
drop location_id
	
foreach location_id of local locations {
foreach sex_id in 1 2 {
foreach year_id in 1990 1995 2000 2005 2010 2015{

adopath + "FILEPATH"					
run $FILEPATH\get_draws.ado
run $FILEPATH\paf_calc_categ.do

cap mkdir /ihme/epi/risk/paf/`risk'_interm						

local n = 0
foreach me in 1981 {						
	** pull exposure
		get_draws, gbd_id_field(modelable_entity_id) gbd_id(`me') location_ids(`location_id') year_ids(`year_id') sex_ids(`sex_id') status(best) source(epi) gbd_round_id(3) clear 	
		renpfix draw_ exp_										
		** prevalence model is 5
		keep if measure_id==5

		gen parameter="cat1"
		expand 2, gen(dup)										
		forvalues i = 0/999 {									
			qui replace exp_`i' = 1-exp_`i' if dup==1			
		}
		replace parameter="cat2" if dup==1
		drop dup
		tempfile exp 											
		save `exp', replace										

	** prep RRs - 
		
	clear
	set obs 1
	gen cause_id = 719 in 1										
	if `me' == 1981 {	// Major Depression						
		gen modelable_entity_id = `me'
		gen sd = ((ln(41.71)) - (ln(9.47))) / (2*invnormal(.975)) 
		forvalues i = 0/999 {
			gen double rr_`i' = exp(rnormal(ln(19.88), sd))		
		}
	}

	if `me' == 1986 {	// Bipolar Disorder
		gen modelable_entity_id = `me'
		gen sd = ((ln(12.41)) - (ln(2.63))) / (2*invnormal(.975))
		forvalues i = 0/999 {
			gen double rr_`i' = exp(rnormal(ln(5.71), sd))
		}
	}

	if `me' == 1989 {	// Anxiety disorder
		gen modelable_entity_id = `me'
		gen sd = ((ln(4.32)) - (ln(1.66))) / (2*invnormal(.975))
		forvalues i = 0/999 {
			gen double rr_`i' = exp(rnormal(ln(2.67), sd))
		}
	}

	if `me' == 1993 {	// Anorexia Nervosa
		gen modelable_entity_id = `me'
		gen sd = ((ln(25.62)) - (ln(2.24))) / (2*invnormal(.975))
		forvalues i = 0/999 {
			gen double rr_`i' = exp(rnormal(ln(7.57), sd))
		}
	}
	
	if `me' == 1964 {	// Schizophrenia
		gen modelable_entity_id = `me'
		gen sd = ((ln(14.48)) - (ln(11.01))) / (2*invnormal(.975))
		forvalues i = 0/999 {
			gen double rr_`i' = exp(rnormal(ln(12.63), sd))
		}
	}

	replace cause_id = 718										
	
	gen parameter = "cat1"

	gen n = 1
	tempfile r
	save `r', replace											

	clear
	set obs 50													
	gen age_group_id = _n										
	keep if age_group_id<=21									
	drop if age_group_id==.										
	gen n = 1
	joinby n using `r'
	keep cause_id age_group_id modelable_entity_id parameter rr*

	expand 2, gen(dup)
	forvalues i = 0/999 {
		replace rr_`i' = 1 if dup==1
	}
	replace parameter="cat2" if dup==1 
	drop dup
	gen mortality=1
	gen morbidity=1

	tempfile rr
	save `rr', replace											

	merge m:1 age_group_id modelable_entity_id parameter using `exp', keep(3) nogen		

		** generate TMREL																
		bysort age_group_id year_id sex_id cause_id mortality morbidity: gen level = _N
		levelsof level, local(tmrel_param) c
		drop level

		forvalues i = 0/999 {
			qui gen tmrel_`i' = 0
			replace tmrel_`i' = 1 if parameter=="cat`tmrel_param'"
		}

		cap drop rei_id
		gen rei_id = `rei_id'
		calc_paf_categ exp_ rr_ tmrel_ paf_, by(age_group_id rei_id location_id sex_id year_id cause_id modelable_entity_id mortality morbidity)

		local n = `n' + 1
		tempfile `n'
		save ``n'', replace

} // end me loop												






** append PAFs and calculate joint for all drug use
		clear
		forvalues i = 1/`n' {
			append using ``i''									
		}

		** expand mortliaty and morbidity
		//gen mortality=1
		//gen morbidity=1

		expand 2 if mortality == 1 & morbidity == 1, gen(dup)
		replace morbidity = 0 if mortality == 1 & morbidity == 1 & dup == 0
		replace mortality = 0 if mortality == 1 & morbidity == 1 & dup == 1
		drop dup

		levelsof mortality, local(morts)

		cap gen modelable_entity_id=.

		noi di c(current_time) + ": saving PAFs"

			foreach mmm of local morts {
				if `mmm' == 1 local mmm_string = "yll"
				else local mmm_string = "yld"
				
				outsheet age_group_id rei_id location_id sex_id year_id cause_id modelable_entity_id paf* using "FILEPATH.csv" if year_id == `year_id' & sex_id == `sex_id' & mortality == `mmm', comma replace
				no di "saved: FILEPATH.csv"
			}
		}
	}
	}
} // end quiet loop




