set more off
	if c(os) == "Windows" {
		global j "J:"
		set mem 1g
	}
	if c(os) == "Unix" {
		global j "/home/j"
		set mem 2g
		set odbcmgr unixodbc
	}

local conn_string = `"ADDRESS + PASSWORD"'

odbc load, exec("SELECT rei_id, rei AS risk, rei_name as risk_name FROM shared.rei") `conn_string' clear
tempfile r
save `r', replace

odbc load, exec("SELECT cause_id, acause, cause_name FROM shared.cause") `conn_string' clear
tempfile c
save `c', replace

use "$FILEPATH/summary_G_2013", clear
keep if age==99 & sex==3 & risk !="" & year !=9999
rename risk med_
keep acause med_
tempfile riskcause
save `riskcause', replace
	
use $FILEPATH/all_mediations_adj_4-6_corrected.dta, clear
replace risk = "envir_lead_bone" if risk=="envir_lead"
replace risk = "metab_fpg" if risk=="metab_fpg_cont"
replace risk = "smoking_direct_prev" if risk=="smoking"

forvalues i = 0/999 {
qui replace draw_`i' = .99999999999999999 if draw_`i'==1
qui replace draw_`i' = .00000000000000001 if draw_`i'==0
}


replace acause="msk_pain_lowback" if acause=="msk_lowbackpain"
replace mediator="yes" if risk=="activity" & acause=="cvd_stroke_isch" & med_=="metab_fpg"
merge m:1 acause using `c', keep(3) nogen
merge m:1 risk using `r', keep (3) nogen
tempfile nsmoke
save `nsmoke', replace
clear
set obs 100
gen rei_id = 166
gen cause_id = .
	replace cause_id = 690 in 1
	replace cause_id = 691 in 2
	replace cause_id = 692 in 3
	replace cause_id = 693 in 3
	replace cause_id = 694 in 4
	replace cause_id = 695 in 5
	replace cause_id = 697 in 6
	replace cause_id = 707 in 7
	replace cause_id = 711 in 8
	replace cause_id = 727 in 9
	replace cause_id = 729 in 10
	drop if cause_id == .

merge 1:1 cause_id using `c', keep(3) nogen
merge m:1 rei_id using `r', keep (3) nogen
gen med_ = "metab_bmd"
forvalues i = 0/999 {
	gen double draw_`i' = .999
}
gen mediator="metab_bmd"
append using `nsmoke'

drop _merge
replace risk = "unsafe_sex" if risk=="sex"
rename cause_id ancestor_cause
joinby ancestor_cause using "FILEPATH/cause_expand.dta", unmatched(master)
replace ancestor_cause = descendant_cause if ancestor_cause!=descendant_cause & descendant_cause!=. // if we have a no match for a sequelae
drop descendant_cause _merge
rename ancestor_cause cause_id
drop acause risk
merge m:1 cause_id using `c', keep(3) nogen
merge m:1 rei_id using `r', keep (3) nogen
save $FILEPATH/mediation_matrix_corrected.dta, replace
tempfile corrected
save `corrected', replace
cap drop _merge
cap drop mediator
cap drop risk_old
rename risk risk_old
joinby rei_id cause_id using $FILEPATH/mediation_draws_final.dta, unmatched(both)
egen one = rowmean(draw_*)

forvalues i = 0/999 {
	qui replace draw_`i' = med_fac_`i' if med_fac_`i'!=. & one<.9
	qui replace draw_`i' = med_fac_`i' if _merge==2
}
replace med_ = mediator if med_=="" & mediator!=""
replace risk_old = risk if risk_old=="" & risk!=""
drop med_fac*
drop risk
rename risk_old risk
replace med_ = "metab_cholesterol" if med_=="metab_chol"
recast double draw*
forvalues i = 0/999 {
qui replace draw_`i' = .99999999999999999 if draw_`i'==1
qui replace draw_`i' = .00000000000000001 if draw_`i'==0
}
replace acause="cvd_ihd" if cause_id==493
replace acause="diabetes" if cause_id==587
replace acause="cvd_stroke_isch" if cause_id==495
replace acause="cvd_stroke_isch" if cause_id==495
replace acause="cvd_stroke_cerhem" if cause_id==496

save $FILEPATH/mediation_matrix_corrected.dta, replace

tempfile corrected
save `corrected', replace

** metab
keep if inlist(risk,"metab_fpg","metab_cholesterol","metab_bmi","metab_ikf","metab_bmd","metab_sbp")
keep if inlist(med_,"metab_fpg","metab_cholesterol","metab_bmi","metab_ikf","metab_bmd","metab_sbp")
keep risk acause cause_id rei_id med_ draw*

forvalues i = 0/999 {
qui replace draw_`i' = .99999999999999999 if draw_`i'==1
qui replace draw_`i' = .00000000000000001 if draw_`i'==0
}


foreach var of varlist draw* {
	qui replace `var' = log(1 - `var')
}

collapse (sum) draw*, by(acause risk rei_id cause_id) fast

foreach var of varlist draw* {
	qui replace `var' = exp(`var')
}

foreach var of varlist draw* {
	qui replace `var' = .00000000000000001 if `var'<=.01
}

renpfix draw_ mediate_
gen descriptor = "mediated"
save $FILEPATH/metab.dta, replace

** behav
use `corrected', clear
** we now drop air_pm and air_hap
drop if inlist(risk,"air_hap","air_pm")

keep if inlist(risk,"abuse_csa","abuse_ipv","activity","diet_calcium_low","diet_fiber","diet_fish","diet_fruit","diet_grains","diet_milk") | inlist(risk,"diet_procmeat","diet_pufa","diet_redmeat","diet_salt","diet_ssb","diet_transfat","diet_veg","drugs_alcohol","drugs_illicit") | inlist(risk,"nutrition_iron","nutrition_underweight","nutrition_vitamina","nutrition_zinc","smoking_direct_prev","unsafe_sex") | inlist(risk,"diet_nuts","nutrition_breast")

keep if inlist(med_,"abuse_csa","abuse_ipv","activity","diet_calcium_low","diet_fiber","diet_fish","diet_fruit","diet_grains","diet_milk") | inlist(med_,"diet_procmeat","diet_pufa","diet_redmeat","diet_salt","diet_ssb","diet_transfat","diet_veg","drugs_alcohol","drugs_illicit") | inlist(med_,"nutrition_iron","nutrition_underweight","nutrition_vitamina","nutrition_zinc","smoking_direct_prev","unsafe_sex") | inlist(med_,"diet_nuts","nutrition_breast")

** drop if mediator==""
keep risk acause rei_id cause_id med_ draw*
** merge m:1 acause med_ using `riskcause', keep(1 3) nogen

foreach var of varlist draw* {
	qui replace `var' = log(1 - `var')
}

collapse (sum) draw*, by(acause risk rei_id cause_id) fast

foreach var of varlist draw* {
	qui replace `var' = exp(`var')
}

foreach var of varlist draw* {
	qui replace `var' = .00000000000000001 if `var'<=.01
}

renpfix draw_ mediate_
gen descriptor = "mediated"
save $FILEPATH/_behav.dta, replace

** all
use `corrected', clear
** we now drop air_pm and air_hap
drop if inlist(risk,"air_hap","air_pm")
replace acause="msk_pain_lowback" if acause=="msk_lowbackpain"
replace mediator="yes" if risk=="activity" & acause=="cvd_stroke_isch" & med_=="metab_fpg"

** drop if mediator==""
keep risk acause rei_id cause_id med_ draw*

foreach var of varlist draw* {
	qui replace `var' = log(1 - `var')
}

collapse (sum) draw*, by(acause risk rei_id cause_id) fast

foreach var of varlist draw* {
	qui replace `var' = exp(`var')
}

foreach var of varlist draw* {
	qui replace `var' = .99999999999999999 if `var'==1
	qui replace `var' = .00000000000000001 if `var'<=.01
}

renpfix draw_ mediate_
gen descriptor = "mediated"

save $FILEPATH/_all.dta, replace

** overlaps metabolic_environmental - m_e
** behavioral_environmental - b_e
** metabolic_behavioral - m_b
** environmental_metabolic_behavioral - e_m_b

** m_e
use `corrected', clear
** we now drop air_pm and air_hap
drop if inlist(risk,"air_hap","air_pm")
replace acause="msk_pain_lowback" if acause=="msk_lowbackpain"
replace mediator="yes" if risk=="activity" & acause=="cvd_stroke_isch" & med_=="metab_fpg"

keep if (inlist(risk,"air_hap","air_ozone","air_pm","envir_lead_bone","envir_radon","occ_asthmagens","occ_backpain","occ_carcino","occ_carcino_acid") | inlist(risk,"occ_carcino_arsenic","occ_carcino_asbestos","occ_carcino_benzene","occ_carcino_beryllium","occ_carcino_cadmium","occ_carcino_chromium","occ_carcino_diesel","occ_carcino_formaldehyde","occ_carcino_nickel") | inlist(risk,"occ_carcino_pah","occ_carcino_silica","occ_carcino_smoke","occ_hearing","occ_injury","occ_particulates","wash_sanitation","wash_water") | inlist(risk,"metab_fpg","metab_cholesterol","metab_bmi","metab_ikf","metab_bmd","metab_sbp")) & (inlist(med_,"metab_fpg","metab_cholesterol","metab_bmi","metab_ikf","metab_bmd","metab_sbp") | inlist(med_,"metab_fpg","metab_cholesterol","metab_bmi","metab_ikf","metab_bmd","metab_sbp") | inlist(med_,"air_hap","air_ozone","air_pm","envir_lead_bone","envir_radon","occ_asthmagens","occ_backpain","occ_carcino","occ_carcino_acid") | inlist(med_,"occ_carcino_arsenic","occ_carcino_asbestos","occ_carcino_benzene","occ_carcino_beryllium","occ_carcino_cadmium","occ_carcino_chromium","occ_carcino_diesel","occ_carcino_formaldehyde","occ_carcino_nickel") | inlist(med_,"occ_carcino_pah","occ_carcino_silica","occ_carcino_smoke","occ_hearing","occ_injury","occ_particulates","wash_sanitation","wash_water"))

// drop if mediator==""
keep risk acause med_ cause_id rei_id draw*
** merge m:1 acause med_ using `riskcause', keep(1 3) nogen

foreach var of varlist draw* {
	qui replace `var' = log(1 - `var')
}

collapse (sum) draw*, by(acause risk rei_id cause_id) fast

foreach var of varlist draw* {
	qui replace `var' = exp(`var')
}

foreach var of varlist draw* {
	replace `var' = .00000000000000001 if `var'<=.01
}
renpfix draw_ mediate_
gen descriptor = "mediated"

save $FILEPATH/m_e.dta, replace

** b_e
use `corrected', clear
** we now drop air_pm and air_hap
drop if inlist(risk,"air_hap","air_pm")
replace acause="msk_pain_lowback" if acause=="msk_lowbackpain"
replace mediator="yes" if risk=="activity" & acause=="cvd_stroke_isch" & med_=="metab_fpg"

keep if (inlist(risk,"abuse_csa","abuse_ipv","activity","diet_calcium_low","diet_fiber","diet_fish","diet_fruit","diet_grains","diet_milk") | inlist(risk,"diet_procmeat","diet_pufa","diet_redmeat","diet_salt","diet_ssb","diet_transfat","diet_veg","drugs_alcohol","drugs_illicit") | inlist(risk,"nutrition_iron","nutrition_underweight","nutrition_vitamina","nutrition_zinc","smoking_direct_prev","unsafe_sex") | inlist(risk,"diet_nuts","nutrition_breast") | inlist(risk,"air_hap","air_ozone","air_pm","envir_lead_bone","envir_radon","occ_asthmagens","occ_backpain","occ_carcino","occ_carcino_acid") | inlist(risk,"occ_carcino_arsenic","occ_carcino_asbestos","occ_carcino_benzene","occ_carcino_beryllium","occ_carcino_cadmium","occ_carcino_chromium","occ_carcino_diesel","occ_carcino_formaldehyde","occ_carcino_nickel") | inlist(risk,"occ_carcino_pah","occ_carcino_silica","occ_carcino_smoke","occ_hearing","occ_injury","occ_particulates","wash_sanitation","wash_water")) & (inlist(med_,"abuse_csa","abuse_ipv","activity","diet_calcium_low","diet_fiber","diet_fish","diet_fruit","diet_grains","diet_milk") | inlist(med_,"diet_procmeat","diet_pufa","diet_redmeat","diet_salt","diet_ssb","diet_transfat","diet_veg","drugs_alcohol","drugs_illicit") | inlist(med_,"nutrition_iron","nutrition_underweight","nutrition_vitamina","nutrition_zinc","smoking_direct_prev","unsafe_sex") | inlist(med_,"diet_nuts","nutrition_breast") | inlist(med_,"air_hap","air_ozone","air_pm","envir_lead_bone","envir_radon","occ_asthmagens","occ_backpain","occ_carcino","occ_carcino_acid") | inlist(med_,"occ_carcino_arsenic","occ_carcino_asbestos","occ_carcino_benzene","occ_carcino_beryllium","occ_carcino_cadmium","occ_carcino_chromium","occ_carcino_diesel","occ_carcino_formaldehyde","occ_carcino_nickel") | inlist(med_,"occ_carcino_pah","occ_carcino_silica","occ_carcino_smoke","occ_hearing","occ_injury","occ_particulates","wash_sanitation","wash_water"))

// drop if mediator==""
keep risk acause med_ cause_id rei_id draw*

foreach var of varlist draw* {
	qui replace `var' = log(1 - `var')
}

collapse (sum) draw*, by(acause risk rei_id cause_id) fast

foreach var of varlist draw* {
	replace `var' = exp(`var')
}

foreach var of varlist draw* {
	replace `var' = .00000000000000001 if `var'<=.01
}

renpfix draw_ mediate_
gen descriptor = "mediated"

save $FILEPATH/b_e.dta, replace

** m_b
use `corrected', clear
** we now drop air_pm and air_hap
drop if inlist(risk,"air_hap","air_pm")
replace acause="msk_pain_lowback" if acause=="msk_lowbackpain"
replace mediator="yes" if risk=="activity" & acause=="cvd_stroke_isch" & med_=="metab_fpg"

keep if (inlist(risk,"abuse_csa","abuse_ipv","activity","diet_calcium_low","diet_fiber","diet_fish","diet_fruit","diet_grains","diet_milk") | inlist(risk,"diet_procmeat","diet_pufa","diet_redmeat","diet_salt","diet_ssb","diet_transfat","diet_veg","drugs_alcohol","drugs_illicit") | inlist(risk,"nutrition_iron","nutrition_underweight","nutrition_vitamina","nutrition_zinc","smoking_direct_prev","unsafe_sex") | inlist(risk,"diet_nuts","nutrition_breast") | inlist(risk,"metab_fpg","metab_cholesterol","metab_bmi","metab_ikf","metab_bmd","metab_sbp")) & (inlist(med_,"abuse_csa","abuse_ipv","activity","diet_calcium_low","diet_fiber","diet_fish","diet_fruit","diet_grains","diet_milk") | inlist(med_,"diet_procmeat","diet_pufa","diet_redmeat","diet_salt","diet_ssb","diet_transfat","diet_veg","drugs_alcohol","drugs_illicit") | inlist(med_,"nutrition_iron","nutrition_underweight","nutrition_vitamina","nutrition_zinc","smoking_direct_prev","unsafe_sex") | inlist(med_,"diet_nuts","nutrition_breast") | inlist(med_,"metab_fpg","metab_cholesterol","metab_bmi","metab_ikf","metab_bmd","metab_sbp"))

// drop if mediator==""
keep risk acause cause_id rei_id med_ draw*

foreach var of varlist draw* {
	qui replace `var' = log(1 - `var')
}

collapse (sum) draw*, by(acause risk rei_id cause_id) fast

foreach var of varlist draw* {
	qui replace `var' = exp(`var')
}

foreach var of varlist draw* {
	replace `var' = .00000000000000001 if `var'<=.01
}
renpfix draw_ mediate_
gen descriptor = "mediated"

save $FILEPATH\m_b.dta, replace

