// set up 
clear all
set more off 
set maxvar 20000

local ier_dir `1'
di in red "`ier_dir'"
local out_dir_draws `2'
di in red "`out_dir_draws'"
local loc `3'
di in red "`loc'"
local year `4'
di in red "`year'"
local loc_dta `5'
di in red "`loc_dta'"


use "`loc_dta'", clear 
keep if ihme_loc_id=="`loc'"
levelsof location_id, local(loc_id)
// ...................................................... YLL RRs (mortality)...................................................................
		//Prep IER output file - IHD, Stroke, and LRI; 
		insheet using "`ier_dir'/yll_`loc'_`year'.csv", clear
		rename cause acause
		// only keep diseases we analyze 
		*keep if acause=="lri" |acause=="cvd_stroke" |acause=="cvd_ihd" 
		drop v1 *mean *lower *upper
		order age sex acause draw*, first
		forvalues k=1/1000 {
				local l = `k' - 1 
				rename draw_yll_`k' rr_`l'
			}	

		// assign age_group_id to each age --could improve 
		gen age_group_id=.
		egen rank=rank(age) if acause=="lri", by(sex)
		replace age_group_id=rank+1 if acause=="lri"
		drop rank
		egen rank=rank(age) if acause !="lri",by(sex acause)
		replace age_group_id=rank+9 if acause !="lri"
		drop age rank
		**clean up LRI and age groups: for under five: both sex, for 5-80: split by sex
		drop if age_group_id<=5 & (sex==1|sex==2) & acause=="lri"
		drop if age_group_id>5 & sex==3 & acause=="lri"
		
		**expand stroke RRs and apply them to ischemic and hemorrhagic 
		expand 2 if acause=="cvd_stroke", gen(id)
		replace acause="cvd_stroke_cerhem" if acause=="cvd_stroke" & id==0
		replace acause="cvd_stroke_isch" if acause=="cvd_stroke" & id==1
		drop id
		
		drop if sex==3 & (acause=="neo_lung"|acause=="resp_copd")


		gen parameter = "cat1"
		gen morbidity = 0
		gen mortality = 1
		
		tempfile ier_yll_draws
		save `ier_yll_draws', replace

*******************************************************************************
	// prepare RRs etarcted from meta-analysis
*******************************************************************************	
		// Males
			clear
			set obs 2
			gen acause = "resp_copd" in 1
			replace acause = "neo_lung" in 2
			// Create RR draws using rr and standard deviation
			forvalues draw = 0/999 {
				** Create COPD RR draws first
				gen rr_`draw' = rnormal(`copd_m_mean_logrr', `copd_m_sd_logrr') in 1
				** Create LC RR draws second
				replace rr_`draw' = rnormal(`lc_m_mean_logrr', `lc_m_sd_logrr') in 2
				** transfer from log space to normal
				replace rr_`draw' = exp(rr_`draw')
			}			
		
			gen sex=1 
			// for age 25-80+, 12 age groups (age_group_id: 10-21)
			expand 12 if acause=="resp_copd" | acause=="neo_lung"
			bysort acause: gen id = _n
			gen age_group_id=id+9
			drop id
			
			gen parameter = "cat1" // exposed 
			
			gen mortality = 1 
			gen morbidity = 0 if acause=="resp_copd"
			replace morbidity = 1 if acause=="neo_lung"
	
			tempfile males
			save `males', replace
			
		// Females
			clear
			set obs 2
			gen acause = "resp_copd" in 1
			replace acause = "neo_lung" in 2
			// Create RR draws using rr and standard deviation
			forvalues draw = 0/999 {
				** Create COPD RR draws first
				gen rr_`draw' = rnormal(`copd_f_mean_logrr', `copd_f_sd_logrr') in 1
				** Create LC RR draws second
				replace rr_`draw' = rnormal(`lc_f_mean_logrr', `lc_f_sd_logrr') in 2
				** transfer from log space to normal
				replace rr_`draw' = exp(rr_`draw')
			}			
			
			gen sex=2
			expand 12 if acause=="resp_copd" | acause=="neo_lung"
			bysort acause: gen id = _n
			gen age_group_id=id+9
			drop id
			
			gen parameter = "cat1"
			
			gen mortality = 1 
			gen morbidity = 0 if acause=="resp_copd"
			replace morbidity = 1 if acause=="neo_lung"

			tempfile females
			save `females', replace

			****************
		
	// add RR for cataracts for women ONLY - morbidity ONLY 
		clear
		set obs 17 // 17 age groups (15-95+ years old)
gen acause = "sense_cataract"
gen id=_n
gen age_group_id=_n+7
drop id
gen sex=2
gen rr_mean = 2.47
gen rr_lower = 1.63
gen rr_upper = 3.73
	// generate 1,000 RRs, one observation at a time:

		// put mean, sd in log space:
gen rr_sd = ((ln(rr_upper)) - (ln(rr_lower))) / (2*1.96)
replace rr_mean = ln(rr_mean)

		// draw 1,000 RR values at the mean age
di in red "draw 1,000 RR values at the mean age"
quietly {
	forvalues k = 0/999 {	
		** gen draw from log space RR distribution
		gen rr_`k' = rnormal(rr_mean, rr_sd)
		
		** put draw in normal space
		replace rr_`k' = exp(rr_`k')
	}
}
drop rr_mean rr_lower rr_upper rr_sd

gen parameter = "cat1"
gen morbidity = 1 
gen mortality = 0

tempfile cataract
save `cataract', replace 

**********************

// Compile all RRs from C-R curves
append using `ier_yll_draws'
*append using `males'
*append using `females'

tempfile yll_rr
save `yll_rr', replace 

// ...................................................... YLD RRs (morbidity)...................................................................

	//Prep IER output file - IHD, Stroke, and LRI; 
insheet using "`ier_dir'/yld_`loc'_`year'.csv", clear
rename cause acause
// only keep diseases we analyze
drop if acause == "resp_copd"
*keep if acause=="lri" |acause=="cvd_stroke" |acause=="cvd_ihd" 
**clean up IER output file
drop v1 *mean *lower *upper
order age sex acause draw*, first
forvalues k=1/1000 {
	local l = `k' - 1 
	rename draw_yld_`k' rr_`l'
}
			
		**clean up LRI and age groups
		// assign age_group_id to each age 
gen age_group_id=.
egen rank=rank(age) if acause=="lri", by(sex)
replace age_group_id=rank+1 if acause=="lri"
drop rank
egen rank=rank(age) if acause !="lri",by(sex acause)
replace age_group_id=rank+9 if acause !="lri"
drop age rank
**clean up LRI and age groups: for under five: both sex, for 5-80: split by sex
drop if age_group_id<=5 & (sex==1|sex==2) & acause=="lri"
drop if age_group_id>5 & sex==3 & acause=="lri"
			
		**expand stroke RRs and apply them to ischemic and hemorrhagic 
expand 2 if acause=="cvd_stroke", gen(id)
replace acause="cvd_stroke_cerhem" if acause=="cvd_stroke" & id==0
replace acause="cvd_stroke_isch" if acause=="cvd_stroke" & id==1
drop id

drop if sex==3 & (acause=="neo_lung"|acause=="resp_copd")


gen parameter = "cat1"
gen morbidity = 1
gen mortality = 0

tempfile ier_yld_draws
save `ier_yld_draws', replace

	// Generate morbidity RRs for COPD
use `yll_rr', clear
keep if acause=="resp_copd"

//  generate 1000 ratios for each cause 
forvalues k=0/999 {
	quietly gen ratio`k'=.
}

// COPD ratio --> draw from uniform dist 0.25-0.75 (keeping the ratio the same across all ages in one draw)
forvalues k=0/999 {	
	quietly replace ratio`k' = 0.25 + (0.5 * runiform()) in 1
	quietly replace ratio`k' = ratio`k'[1] if ratio`k'==.
}	

// multiply RR draws by ratios (NOTE: ratio only applies to excess risk above 1)
forvalues k=0/999 {
quietly replace rr_`k' = ((rr_`k' - 1) * ratio`k') + 1
quietly replace rr_`k' = 1 if rr_`k'<1
quietly drop ratio`k'
}	

//Mark these as morbidity/YLD pafs
replace mortality = 0
replace morbidity = 1

// append on RRs for ALRI, lung cancer (which are the same as YLL RRs)
append using  `yll_rr'
append using `ier_yld_draws'

tempfile rr_draws
save `rr_draws', replace

**************************************************
// prepare counterfactual 
**************************************************
	***Generate observations indicating no exposure category
expand 2, gen(id)
replace parameter = "cat2" if id==1 
drop id
forvalues k = 0/999 {
	quietly replace rr_`k' = 1 if parameter=="cat2"
}

order acause age_group_id sex mortality morbidity parameter, first
sort acause sex age_group_id

	***************************
	** Save draws on share folder**
	***************************
	// Recode acause name to cause_id 
gen cause_id=.
replace cause_id=493 if acause=="cvd_ihd"
replace cause_id=496 if acause=="cvd_stroke_cerhem"
replace cause_id=495 if acause=="cvd_stroke_isch"
replace cause_id=322 if acause=="lri"
replace cause_id=426 if acause=="neo_lung"
replace cause_id=509 if acause=="resp_copd"
replace cause_id=671 if acause=="sense_cataract"
drop acause 

	// lri for both sex 
expand 2 if sex==3, gen(id)
replace sex=1 if id==0 & sex==3
replace sex=2 if id==1 & sex==3

//Janky fix to expand to older age groups for GBD 2016
replace age_group_id = 30 if age_group_id == 21
replace age_group_id = 31 if age_group_id == 22
replace age_group_id = 32 if age_group_id == 23
replace age_group_id = 235 if age_group_id == 24

	// save in appropriate format: by location_id, year, and sex
		foreach s in 1 2 {
			export delimited age_group_id cause_id mortality morbidity parameter rr* using "`out_dir_draws'/rr_`loc_id'_`year'_`s'.csv" if sex == `s', replace
		}
	
