***********************************
** Retinopathy of prematurity 
***********************************

clear all
set graphics off
set more off
set maxvar 32000


/*  //////////////////////////////////////////////
		WORKING DIRECTORY
////////////////////////////////////////////// */ 


// discover root 
	if c(os) == "Windows" {
		local j /*FILEPATH*/
		// Load the PDF appending application
		quietly do /*FILEPATH*/
	}
	if c(os) == "Unix" {
		local j /*FILEPATH*/
		quietly do /*FILEPATH*/
		ssc install estout, replace 
		ssc install metan, replace
	} 
	
// locals
local me_id_list 1557 1558 1559

// functions
run /*FILEPATH*/
run /*FILEPATH*/
run /*FILEPATH*/



// directories 	
	local working_dir = /*FILEPATH*/
	local data_dir = /*FILEPATH*/
	local log_dir = /*FILEPATH*/
	
// Create timestamp for logs
    local c_date = c(current_date)
	local c_time = c(current_time)
	local c_time_date = "`c_date'"+"_" +"`c_time'"
	display "`c_time_date'"
    local time_string = subinstr("`c_time_date'", ":", "_", .)
    local timestamp = subinstr("`time_string'", " ", "_", .)
    display "`timestamp'"
	
	//log
	capture log close
	log using /*FILEPATH*/, replace


 ///////////////////////////////////////////////////////////
// Mild impairment props of Retinopathy of Prematurity,
// calculated from previous preterm modeling results 
///////////////////////////////////////////////////////////// 

	//bring in NMR data 

	use /*FILEPATH*/, clear 
	keep region_name location_name ihme_loc_id sex year q_nn_med 
	keep if year>=1980 
	rename q_nn_med nmr 

	// convert from probability to NMR
	replace nmr = nmr*1000
	replace sex = "1" if sex == "male"
	replace sex = "2" if sex == "female" 
	replace sex = "3" if sex == "both"
	destring sex, replace
	drop if sex==3

	//year is at midear in this file, switch it to beginning of the year
	replace year = year - 0.5
	tempfile nmr
	save `nmr'

	// merge on additional location data
	get_location_metadata, location_set_id(21) clear 
	tempfile locations
	save `locations'

	// _merge == 1 are global, super region and regions
	merge 1:m ihme_loc_id using `nmr', keep(3) nogen 

	// save prepped nmr template
	keep location_name location_id sex year nmr
	rename sex sex_id
	rename year year_id
	tempfile neo
	save `neo'

	// Multiply by isolation value -- Isolation value from meta-analysis (see retinopathy_metan.xlsx for analysis)

	local scalar_mean =0.65
	local upper = 0.83
	local lower = 0.44
	local scalar_se = (`upper' - `lower')/(2*1.96)
	local count_n = (`scalar_mean' * (1-`scalar_mean') ) / `scalar_se'^2 
	local iso_alpha = `count_n' * `scalar_mean' 
	local iso_beta = `count_n' * (1-`scalar_mean') 

	// Repeat calculations for each gestational age

  foreach me_id of local me_id_list {

	di in red "analyzing for me_id `me_id'"

	import delimited using /*FILEPATH*/, clear 
	keep if me_id == `me_id'
	keep nmr_level alpha beta  
	tempfile rop_scalars
	save `rop_scalars', replace

	local group = "group_`me_id'"
	
	// importing and formatting data 
	di in red "importing data"
	
	import delimited using /*FILEPATH*/, varnames(1) clear
	drop if age_group_id == "age_group_id"
	destring age_group_id, replace
	keep if age_group_id == 4 // draws values are all the same across age_groups
	replace age_group_id = 4

	destring location_id, replace
	destring year_id, replace
	destring sex_id, replace

	tempfile data
	save `data'

	//merge gets rid of : year<1980, sex=both

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
	
	save /*FILEPATH*/, replace
	export delimited using /*FILEPATH*/, replace 


	//summary stats
	egen rop_prop = rowmean(group_`me_id'_draw_*)
	fastpctile group_`me_id'_draw_*, pct(2.5 97.5) names(rop_group_`me_id'_lower rop_group_`me_id'_upper)
	drop group_`me_id'_draw_*
	
	save /*FILEPATH*/, replace
	export delimited using /*FILEPATH*/, replace

} 


//add the three together to get a single retinopathy prevalence value

di in red "loading 1557"
use "FILEPATH", clear
di in red "merging on 1558"
merge 1:1 location_id year sex using "FILEPATH", nogen
di in red "merging on 1559"
merge 1:1 location_id year sex using "FILEPATH", nogen

drop nmr_level location_name 

// Actually doing the adding. 
forvalues i=0/999{
	gen draw_`i' = group_1557_draw_`i' + group_1558_draw_`i' + group_1559_draw_`i'
	drop group_1557_draw_`i' group_1558_draw_`i' group_1559_draw_`i'

}


di in red "saving all draws"
save "FILEPATH", replace
export delimited using "FILEPATH", replace 

//summary stats
egen mean = rowmean(draw_*)
egen std = rowsd(draw_*)
drop draw_*
di in red "saving summary"
save "FILEPATH", replace
export delimited using "FILEPATH", replace 



// now run severity splits
// This creates a beta distribution from each of the means in the input rop_severity_splits.csv. It will then scale the draws from those so they sum to 1. 

local split_dir = "FILEPATH"
import delimited using "`split_dir'", clear 


// part I: we need to scale the splits (on the draw level) so they sum to one
	gen sim = 1

	//step 1: Get draws 
	levelsof healthstate, local(healthstate_list)

	// NOTE 1) Dropping everything except the row of interest 
	// 2) creating a beta distribution for that record
	// 3) 'expand' command just copies that one line (1,000 times)
	// 4) generate a draw from the alpha and beta values for each row 
	
	foreach healthstate of local healthstate_list{
		
		di in red "generating beta for `healthstate'"
		preserve
			di "keeping healthstate"
			keep if healthstate=="`healthstate'"
			
			di "generating beta distribution as before"
			local scalar_mean = mean
			local upper = upper
			local lower = lower
			local scalar_se = (`upper' - `lower')/(2*1.96)
			local count_n = (`scalar_mean' * (1-`scalar_mean') ) / `scalar_se'^2 
			local alpha = `count_n' * `scalar_mean'
			local beta = `count_n' * (1-`scalar_mean')
			
			di "expanding"
			expand 1000
			replace sim=_n
			gen `healthstate'_draw = rbeta(`alpha', `beta')
			
			di "keeping"
			keep sim `healthstate'_draw
			
			di "tempfiling"
			tempfile `healthstate'_temp
			save ``healthstate'_temp', replace
		restore

	}

	drop mean lower upper
	tempfile acause_names
	save `acause_names', replace
	
	//Step 2: scale to one 
	local health_count: word count `healthstate_list'


	forvalues i=1/`health_count'{
	
		di "creating local healthstate"
		local healthstate : word `i' of `healthstate_list'
		
		if `i'==1{
			di in red "using `healthstate'"
			use ``healthstate'_temp', clear
			gen sum = `healthstate'_draw
		}
		else{
			di in red "merging on `healthstate'"
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
			save "FILEPATH", replace
			outsheet using "FILEPATH", comma replace
		restore
	}   


// part II: Multiply our parent cause by each of the splits
// NOTE: End result will be 1,000 draws of the prevalence of each retinopathy mini-sequelae (vision_mild, vision_mod, etc.) 

use "FILEPATH", clear

foreach healthstate of local healthstate_list{
	di in red "generating splits for `healthstate'"
	preserve 
		gen healthstate = "`healthstate'"
		merge m:1 healthstate using "FILEPATH"
		drop _merge
		
		quietly{
			di in red "generating new draws"
			forvalues i=0/999{
				replace draw_`i' = draw_`i' * `healthstate'_split_draw_`i'
				drop `healthstate'_split_draw_`i'
			}
		}
		
		keep location_id year sex draw*
		
		local out_dir = FILEPATH
		capture mkdir `out_dir'
		

		di in red "saving `healthstate'"
		save FILEPATH, replace
		export delimited using FILEPATH, replace
		
	restore

}
	
// save results
foreach healthstate of local healthstate_list {

/* QSUB TO NEXT SCRIPT */

}

