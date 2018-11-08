 
clear all
set graphics off
set more off
set maxvar 32000

 
	if c(os) == "Windows" {
		local j "J:"
 
		quietly do FILEPATH
	}
	if c(os) == "Unix" {
		local j FILEPATH

		ssc install estout, replace 
		ssc install metan, replace
	} 
 
local me_id_list 1557 1558 1559
 
run FILEPATH
run FILEPATH
 
	local working_dir = FILEPATH
	local data_dir =FILEPATH
	 
    local c_date = c(current_date)
	local c_time = c(current_time)
	local c_time_date = "`c_date'"+"_" +"`c_time'"
	display "`c_time_date'"
    local time_string = subinstr("`c_time_date'", ":", "_", .)
    local timestamp = subinstr("`time_string'", " ", "_", .)
    display "`timestamp'"
	 
	capture log close
	log using "`log_dir'/retinopathy_`timestamp'.smcl", replace

	use FILEPATH, clear
	keep location_id region_name location_name ihme_loc_id sex year q_nn_med
 
	keep if year>=1980 
	rename q_nn_med nmr 
 
	replace nmr = nmr*1000
	replace sex = "1" if sex == "male"
	replace sex = "2" if sex == "female" 
	replace sex = "3" if sex == "both"
	destring sex, replace
	drop if sex==3
 
	replace year = year - 0.5
	tempfile nmr
	save `nmr'
 
	get_location_metadata, location_set_id(9) gbd_round_id(5) clear 
	tempfile locations
	save `locations'

	merge 1:m location_id using `nmr', keep(3) nogen 
 
	keep location_name location_id sex year nmr
	rename sex sex_id
	rename year year_id
	tempfile neo
	save `neo'
 
	local scalar_mean =0.65
	local upper = 0.83
	local lower = 0.44
	local scalar_se = (`upper' - `lower')/(2*1.96)
	local count_n = (`scalar_mean' * (1-`scalar_mean') ) / `scalar_se'^2 
	local iso_alpha = `count_n' * `scalar_mean'  
	local iso_beta = `count_n' * (1-`scalar_mean') 
 

  foreach me_id of local me_id_list {

	di in red "analyzing for me_id `me_id'"

	import delimited using "FILEPATH/rop_mult_vals_2017.csv", clear 
	keep if me_id == `me_id'
	keep nmr_level alpha beta  
	tempfile rop_scalars
	save `rop_scalars', replace

	local group = "group_`me_id'"
	 
	di in red "importing data"
 
	
	import delimited using "FILEPATH/all_draws.csv", varnames(1) clear
	drop if age_group_id == "age_group_id"
	destring age_group_id, replace
	keep if age_group_id == 4  
	replace age_group_id = 4

	destring location_id, replace
	destring year_id, replace
	destring sex_id, replace

	tempfile data
	save `data'
 
	merge m:1 location_id year_id sex_id using `neo', keep(3) nogen
	gen nmr_level = "mid"
	replace nmr_level = "high" if nmr > 15
	replace nmr_level = "low" if nmr < 5
	merge m:1 nmr_level using `rop_scalars', keep(3) nogen 
	
	di in red "looping"
	
	forvalues j=0/999{
		gen rop_draw = rbeta(alpha, beta)  
		gen iso_draw = rbeta(`iso_alpha', `iso_beta')
		destring draw_`j', replace
		gen group_`me_id'_draw_`j' = draw_`j' * rop_draw * iso_draw  
		drop draw_`j' rop_draw iso_draw  
		destring group_`me_id'_draw_`j', replace
		 
	}
 
	
	drop nmr alpha beta

	di in red "saving"
	
	save "FILEPATH/retino_`me_id'_alldraws.dta", replace
	export delimited using "FILEPATH/retino_`me_id'_alldraws.csv", replace 
 
	egen rop_prop = rowmean(group_`me_id'_draw_*)
	fastpctile group_`me_id'_draw_*, pct(2.5 97.5) names(rop_group_`me_id'_lower rop_group_`me_id'_upper)
	drop group_`me_id'_draw_*
	
	save "FILEPATH/retino_`me_id'_summary.dta", replace
	export delimited using "FILEPATH/retino_`me_id'_summary.csv", replace

} 
 
 
use "FILEPATH/retino_1557_alldraws.dta", clear
 
merge 1:1 location_id year sex using "FILEPATH/retino_1558_alldraws.dta", nogen
 
merge 1:1 location_id year sex using "FILEPATH/retino_1559_alldraws.dta", nogen

drop nmr_level location_name 
 
forvalues i=0/999{
	gen draw_`i' = group_1557_draw_`i' + group_1558_draw_`i' + group_1559_draw_`i'
	drop group_1557_draw_`i' group_1558_draw_`i' group_1559_draw_`i'

}

 
save "FILEPATH/neonatal_preterm_retino__parent_alldraws.dta", replace
export delimited using "FILEPATH/neonatal_preterm_retino__parent_alldraws.csv", replace 
 
egen mean = rowmean(draw_*)
egen std = rowsd(draw_*)
drop draw_* 
save "FILEPATH/neonatal_preterm_retino__parent_summary_stats.dta", replace
export delimited using "FILEPATH/neonatal_preterm_retino__parent_summary_stats.csv", replace 

 
local split_dir = "FILEPATH/rop_severity_splits.csv" // NOTE: tells the splits of ROP into different smaller sequelae. Healthstate here is the sequelae name, in fact. 
import delimited using "`split_dir'", clear 

 
	gen sim = 1
 
	levelsof healthstate, local(healthstate_list)
 
	foreach healthstate of local healthstate_list{
 
		preserve
 
			keep if healthstate=="`healthstate'"
 
			local scalar_mean = mean
			local upper = upper
			local lower = lower
			local scalar_se = (`upper' - `lower')/(2*1.96)
			local count_n = (`scalar_mean' * (1-`scalar_mean') ) / `scalar_se'^2 
			local alpha = `count_n' * `scalar_mean'
			local beta = `count_n' * (1-`scalar_mean')
 
			expand 1000
			replace sim=_n
			gen `healthstate'_draw = rbeta(`alpha', `beta')
 
			keep sim `healthstate'_draw
 
			tempfile `healthstate'_temp
			save ``healthstate'_temp', replace
		restore

	}

	drop mean lower upper
	tempfile acause_names
	save `acause_names', replace
	 
	local health_count: word count `healthstate_list'
 
	forvalues i=1/`health_count'{
	 
		local healthstate : word `i' of `healthstate_list'
		
		if `i'==1{
 
			use ``healthstate'_temp', clear
			gen sum = `healthstate'_draw
		}
		else{
 
			merge 1:1 sim using ``healthstate'_temp'
			drop _merge
			replace sum = sum + `healthstate'_draw
		}
	}
	
	gen scale_factor = 1/sum 
	gen new_sum = 0  

	foreach healthstate of local healthstate_list{
		gen `healthstate'_split_draw_ = `healthstate'_draw * scale_factor
		replace new_sum = new_sum + `healthstate'_split_draw_
		drop `healthstate'_draw
		
 
		preserve
			keep sim `healthstate'_split_draw_
			gen healthstate = "`healthstate'"
			reshape wide `healthstate'_split_draw_, i(healthstate) j(sim)
			
			merge m:1 healthstate using `acause_names', keep(3) nogen
			
			rename *draw_1000 *draw_0
			save "FILEPATH/`healthstate'_draws.dta", replace
			outsheet using "FILEPATH/`healthstate'_draws.csv", comma replace
		restore
	}   
 
use "FILEPATH/neonatal_preterm_retino__parent_alldraws.dta", clear

foreach healthstate of local healthstate_list{ 
	preserve 
		gen healthstate = "`healthstate'"
		merge m:1 healthstate using "FILEPATH/`healthstate'_draws.dta"
		drop _merge
		
		quietly{
			di in red "generating new draws"
			forvalues i=0/999{
				replace draw_`i' = draw_`i' * `healthstate'_split_draw_`i'
				drop `healthstate'_split_draw_`i'
			}
		}
		
		keep location_id year sex draw*
		
		local out_dir = "FILEPATH/`healthstate'"
		capture mkdir `out_dir'
		

		di in red "saving `healthstate'"
		save "`out_dir'/neonatal_preterm_retino_`healthstate'_alldraws.dta", replace
		export delimited using "`out_dir'/neonatal_preterm_retino_`healthstate'_alldraws.csv", replace
		
	restore

}
	 
foreach healthstate of local healthstate_list {

	** QSUB SCRIPT

}

