// Created by: NAME
// Create on: Jan 24, 2014
// Purpose: Prepare age hazard ratio data to be input into 'Bradmod'

clear all
set more off
if (c(os)=="Unix") {
	global root "ADDRESS"
}

if (c(os)=="Windows") {
	global root "ADDRESS"
}


local dismod_templates "FILEPATH"
local HR_data "FILEPATH"
local dismod_dir "FILEPATH"
local pct_dir "FILEPATH" 

// Description of options
local all_together=0 // 
local separate = 1


************ PREP DATA **********
	import excel using "`HR_data'", firstrow clear
	rename *HR* *hr*

	keep include_age baseline nid year_start year_end sex gbd_region iso3 site cohort age_start age_end treat_mo_start treat_mo_end hr_ref hr_mort* std_hr_* hr_adj extractor notes

	** In excel, already standardized everything to be referenced to the lowest age category

	// Drop baseline info, lines not related to age, and studies that did not adjust their hazard ratios
	keep if include_age==1

	drop if std_hr_ref==""
	drop if std_hr_ref=="female" | std_hr_ref=="19-Nov" | std_hr_ref=="females"

	drop if std_hr_mort==.

	drop if hr_adj==0

	// Classify the regions/iso3 into 3 categories: high income, sub-saharan africa, other (if its mostly sub-saharan africa such as LINC, treat at sub-saharan africa)
	gen super="other" if inlist(iso3, "CHN", "ETH", "THA")
	replace super="other" if gbd_region=="Latin America/Carribean" | gbd_region=="East Asia" | gbd_region=="Central Asia" | gbd_region=="North Africa and Middle East" | gbd_region=="South Asia" | gbd_region=="Southeast Asia" | gbd_region == "Latin America and the Carribean"  
	replace super="high" if gbd_region=="High Income" | gbd_region=="High income" | gbd_region=="High-Income" | gbd_region=="Western Europe"
	replace super="ssa" if regexm(gbd_region, "Sahar")
	replace super="ssa" if gbd_region=="Sub-Saharan Africa"
	replace super="ssa" if gbd_region=="Sub-Saharan Africa"
	replace super="ssa" if inlist(iso3, "ZAF", "UGA", "ZMB", "SEN", "KEN", "TZA")
	replace super="ssa" if cohort=="ART-LINC"
		
	tempfile prep_hr
	save `prep_hr', replace

if `all_together' {		
********** ALL REGIONS TOGETHER *************
		
	use `prep_hr', clear
	
	// Set up data_in sheet
		// Want a random effect depending on the study, since they all have different reference categories (wont allow me to use the ref category as a 'region' because its not nested neatly within super region)
		egen unique_id=concat(nid iso3 treat_mo_start treat_mo_end)
		encode unique_id, gen(region)
		tostring region, replace force

		gen meas_value=std_hr_mort
		gen meas_stdev=(std_hr_hi-std_hr_mort)/1.96
		replace meas_stdev=meas_stdev*-1 if meas_stdev<0
		gen subreg="none" 
		replace sex=3 if sex==.
		gen x_sex=0 if sex==3 
		replace x_sex=-.5 if sex==1
		replace x_sex=.5 if sex==2
		gen age_lower=age_start
		replace age_lower=14 if age_lower<14
		gen age_upper=age_end
		gen time_lower=year_start
		gen time_upper=year_end
		replace time_lower="2000" if time_lower=="2000?"
		destring time_upper time_lower, replace
		replace time_lower=time_upper-5 if time_lower==.
		gen integrand="incidence"
		gen x_ones=1

		// set upper age as 99.9 if 100
		drop if age_upper>100

		// mean standard deviation cannot be 0
		replace meas_std=. if meas_std==0
		bysort nid: egen mean_meas_std=mean(meas_std)
		replace meas_std=mean_meas_std if meas_std==.
		drop if meas_std==.

		destring time_lower time_upper, replace force
		tostring meas_stdev, replace force

		// Append on the extra bottom lines that you need to always have for bradmod to run. the only values you would alter here are those in the 'age lower' and 'age upper' columns.
		preserve
		insheet using "`dismod_templates'//dismod_append.csv", comma names clear
		tempfile append
		save `append', replace
		restore

		append using `append'
		replace age_lower=14 if integrand=="mtall" & _n!=_N
		replace age_upper=15 if integrand=="mtall" & _n!=_N
		replace age_lower=15 if integrand=="mtall" & _n==_N
		replace age_upper=100 if integrand=="mtall" & _n==_N

		keep nid meas_value meas_stdev super region subreg x_sex sex age_lower age_upper time_lower time_upper integrand x_ones

		outsheet using "`dismod_dir'/HIV_HR_age/data_in.csv", comma names replace

		
	// Update the 'effect_in' file (from template)
		duplicates drop region, force
		drop if region=="none"
		keep region
		rename region name
		gen effect="region"
		gen integrand="incidence"
		gen lower=-2
		gen upper=2
		gen mean=0
		gen std="inf"
		tempfile effects
		save `effects', replace
		insheet using "`dismod_templates'//effect_in.csv", comma names clear
		append using `effects'
		outsheet using "`dismod_dir'/HIV_HR_age/effect_in.csv", comma names replace


	// Update plain in (from template)
		insheet using "`dismod_templates'//plain_in.csv", comma names clear
		outsheet using "`dismod_dir'/HIV_HR_age/plain_in.csv", comma names replace
		
	// Update value_in (from template)
		insheet using "`dismod_templates'//value_in.csv", comma names clear
		replace value=".01" if name=="eta_incidence"
		replace value = "1993" if name == "random_seed"
		outsheet using "`dismod_dir'/HIV_HR_age/value_in.csv", comma names replace
		
	// Update the 'draw_in' file (from hazard ratio dismod_ode file)
		insheet using "`pct_dir'/pct_male_med_age.csv", comma names clear

		foreach sup in high other ssa {
			sum age_med if super=="`sup'"
			local age_`sup'=`r(mean)'
		}

		insheet using "`dismod_dir'/HIV_HR_age/draw_in.csv", comma names clear

		replace age_lower=`age_high' if row_name=="high_med"
		replace age_upper=`age_high' if row_name=="high_med"
		replace age_lower=`age_other' if row_name=="other_med"
		replace age_upper=`age_other' if row_name=="other_med"
		replace age_lower=`age_ssa' if row_name=="ssa_med"
		replace age_upper=`age_ssa' if row_name=="ssa_med"

		outsheet using "`dismod_dir'/HIV_HR_age/draw_in.csv", comma names replace

}


if `separate' {
************** EACH REGION SEPERATELY FOR SSA AND HIGH ******************

foreach sup in ssa high {

	cap mkdir "`dismod_dir'/HIV_HR_`sup'"
	
	use `prep_hr', clear
	
	keep if super=="`sup'"
	replace super="none"

	// Set up data_in sheet
		// Want a random effect depending on the study, since they all have differnet reference categories (wont allow me to use the ref category as a 'region' because its not nested neatly within super region)
		egen unique_id=concat(nid iso3 treat_mo_start treat_mo_end)
		encode unique_id, gen(region)
		tostring region, replace force

		gen meas_value=std_hr_mort
		gen meas_stdev=(std_hr_hi-std_hr_mort)/1.96
		replace meas_stdev=meas_stdev*-1 if meas_stdev<0
		gen subreg="none" 
		replace sex=3 if sex==.
		gen x_sex=0 if sex==3 
		replace x_sex=-.5 if sex==1
		replace x_sex=.5 if sex==2
		gen age_lower=age_start
		replace age_lower=14 if age_lower<14
		gen age_upper=age_end
		gen time_lower=year_start
		gen time_upper=year_end
		replace time_lower="2000" if time_lower=="2000?"
		destring time_upper time_lower, replace
		replace time_lower=time_upper-5 if time_lower==.
		gen integrand="incidence"
		gen x_ones=1


		drop if age_upper>100

		// mean standard deviation cannot be 0
		replace meas_std=. if meas_std==0
		bysort nid: egen mean_meas_std=mean(meas_std)
		replace meas_std=mean_meas_std if meas_std==.
		drop if meas_std==.

		destring time_lower time_upper, replace force
		tostring meas_stdev, replace force

		// Append on the extra bottom lines that you need to always have for bradmod to run. the only values you would alter here are those in the 'age lower' and 'age upper' columns.
		preserve
		insheet using "`dismod_templates'//dismod_append.csv", comma names clear
		tempfile append
		save `append', replace
		restore

		sum age_lower
		local min_age=`r(min)'
		sum age_upper
		local max_age=`r(max)'
		
		append using `append'
		replace age_lower=`min_age' if integrand=="mtall" & _n!=_N
		replace age_upper=25 if integrand=="mtall" & _n!=_N
		replace age_lower=25 if integrand=="mtall" & _n==_N
		replace age_upper=`max_age' if integrand=="mtall" & _n==_N

		keep nid meas_value meas_stdev super region subreg x_sex sex age_lower age_upper time_lower time_upper integrand x_ones

		outsheet using "`dismod_dir'/HIV_HR_`sup'/data_in.csv", comma names replace

		
	// Update the 'effect_in' file (from template)
		duplicates drop region, force
		drop if region=="none"
		keep region
		rename region name
		gen effect="region"
		gen integrand="incidence"
		gen lower=-2
		gen upper=2
		gen mean=0
		gen std="inf"
		tempfile effects
		save `effects', replace
		insheet using "`dismod_templates'//effect_in.csv", comma names clear
		drop if name=="ssa" | name=="high" | name=="other"
		append using `effects'
		outsheet using "`dismod_dir'/HIV_HR_`sup'/effect_in.csv", comma names replace


	// Update plain in (from template)
		insheet using "`dismod_templates'//plain_in.csv", comma names clear
		outsheet using "`dismod_dir'/HIV_HR_`sup'/plain_in.csv", comma names replace
		
	// Update value_in (from template)
		insheet using "`dismod_templates'//value_in.csv", comma names clear
		replace value=".01" if name=="eta_incidence" 
		replace value = "1993" if name == "random_seed"
		outsheet using "`dismod_dir'/HIV_HR_`sup'/value_in.csv", comma names replace
		
	// Update the 'draw_in' file (from hazard ratio dismod_ode file)
		insheet using FILEPATH, comma names clear
		
		sum age_med if super=="`sup'"
		local median_age=`r(mean)'
		
		insheet using "`dismod_dir'/HIV_HR_`sup'/draw_in.csv", comma names clear
		drop if _n >= 7

		replace age_lower=`median_age' if row_name=="`sup'_med"
		replace age_upper=`median_age' if row_name=="`sup'_med" 
		append using "`dismod_templates'/extra_`sup'"

		outsheet using "`dismod_dir'/HIV_HR_`sup'/draw_in.csv", comma names replace
		
	// Update the 'rate_in' file
		insheet using "`dismod_dir'/HIV_HR_`sup'/rate_in.csv", comma names clear
		replace age=`min_age' if tempid=="low"
		replace age=`max_age' if tempid=="high"
		outsheet using "`dismod_dir'/HIV_HR_`sup'/rate_in.csv", comma names replace

}

}


************ FOR 'OTHER' RUN IT WITH SSA

	cap mkdir "`dismod_dir'/HIV_HR_other"
	
	use `prep_hr', clear
	
	keep if super=="other" | super=="ssa"

	// Set up data_in sheet
		// Want a random effect depending on the study, since they all have differnet reference categories (wont allow me to use the ref category as a 'region' because its not nested neatly within super region)
		egen unique_id=concat(nid iso3 treat_mo_start treat_mo_end)
		encode unique_id, gen(region)
		tostring region, replace force

		gen meas_value=std_hr_mort
		gen meas_stdev=(std_hr_hi-std_hr_mort)/1.96
		replace meas_stdev=meas_stdev*-1 if meas_stdev<0
		gen subreg="none" 
		replace sex=3 if sex==.
		gen x_sex=0 if sex==3 
		replace x_sex=-.5 if sex==1
		replace x_sex=.5 if sex==2
		gen age_lower=age_start
		replace age_lower=14 if age_lower<14
		gen age_upper=age_end
		gen time_lower=year_start
		gen time_upper=year_end
		replace time_lower="2000" if time_lower=="2000?"
		destring time_upper time_lower, replace
		replace time_lower=time_upper-5 if time_lower==.
		gen integrand="incidence"
		gen x_ones=1


		drop if age_upper>100

		// mean standard deviation cannot be 0
		replace meas_std=. if meas_std==0
		bysort nid: egen mean_meas_std=mean(meas_std)
		replace meas_std=mean_meas_std if meas_std==.
		drop if meas_std==.

		destring time_lower time_upper, replace force
		tostring meas_stdev, replace force

		// Append on the extra bottom lines that you need to always have for bradmod to run. the only values you would alter here are those in the 'age lower' and 'age upper' columns.
		preserve
		insheet using "`dismod_templates'//dismod_append.csv", comma names clear
		tempfile append
		save `append', replace
		restore

		sum age_lower
		local min_age=`r(min)'
		sum age_upper
		local max_age=`r(max)'
		
		append using `append'
		replace age_lower=`min_age' if integrand=="mtall" & _n!=_N
		replace age_upper=25 if integrand=="mtall" & _n!=_N
		replace age_lower=25 if integrand=="mtall" & _n==_N
		replace age_upper=`max_age' if integrand=="mtall" & _n==_N

		keep nid meas_value meas_stdev super region subreg x_sex sex age_lower age_upper time_lower time_upper integrand x_ones

		outsheet using "`dismod_dir'/HIV_HR_other/data_in.csv", comma names replace

		
	// Update the 'effect_in' file (from template)
		duplicates drop region, force
		drop if region=="none"
		keep region
		rename region name
		gen effect="region"
		gen integrand="incidence"
		gen lower=-2
		gen upper=2
		gen mean=0
		gen std="inf"
		tempfile effects
		save `effects', replace
		insheet using "`dismod_templates'//effect_in.csv", comma names clear
		drop if name=="high"
		append using `effects'
		outsheet using "`dismod_dir'/HIV_HR_other/effect_in.csv", comma names replace


	// Update plain in (from template)
		insheet using "`dismod_templates'//plain_in.csv", comma names clear
		outsheet using "`dismod_dir'/HIV_HR_other/plain_in.csv", comma names replace
		
	// Update value_in (from template)
		insheet using "`dismod_templates'//value_in.csv", comma names clear
		replace value=".01" if name=="eta_incidence"
		replace value = "1993" if name == "random_seed"
		outsheet using "`dismod_dir'/HIV_HR_other/value_in.csv", comma names replace
		
	// Update the 'draw_in' file (from hazard ratio dismod_ode file)
		insheet using FILEPATH, comma names clear
		
		sum age_med if super=="other"
		local median_age=`r(mean)'
		
		insheet using "`dismod_dir'/HIV_HR_other/draw_in.csv", comma names clear
		drop if _n >= 7

		replace age_lower=`median_age' if row_name=="other_med"
		replace age_upper=`median_age' if row_name=="other_med" 
		append using "`dismod_templates'/extra_other"

		outsheet using "`dismod_dir'/HIV_HR_other/draw_in.csv", comma names replace
		
	// Update the 'rate_in' file
		insheet using "`dismod_dir'/HIV_HR_other/rate_in.csv", comma names clear
		replace age=`min_age' if tempid=="low"
		replace age=`max_age' if tempid=="high"
		outsheet using "`dismod_dir'/HIV_HR_other/rate_in.csv", comma names replace

