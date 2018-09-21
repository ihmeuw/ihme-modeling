

clear all
set graphics off
set more off
set maxvar 32000


/*  //////////////////////////////////////////////
		WORKING DIRECTORY
////////////////////////////////////////////// */

// priming the working environment
	if c(os) == "Windows" {
		local j /*FILEPATH*/
		// Load the PDF appending application
		quietly do /*FILEPATH*/
	}
	if c(os) == "Unix" {
		local j /*FILEPATH*/
		ssc install estout, replace 
		ssc install metan, replace
	} 
	di in red "J drive is `j'"

// arguments
local ihme_loc_id `1'
local location_id `2'
local in_dir `3'
local out_dir `4'
	
// functions
run /*FILEPATH*/
run /*FILEPATH*/


// locals
local acause_list " "neonatal_preterm" "neonatal_enceph" "neonatal_hemolytic" "neonatal_sepsis" "

// directories
	
// Create timestamp for logs
    local c_date = c(current_date)
    local c_time = c(current_time)
    local c_time_date = "`c_date'"+"_" +"`c_time'"
    display "`c_time_date'"
    local time_string = subinstr("`c_time_date'", ":", "_", .)
    local timestamp = subinstr("`time_string'", " ", "_", .)
    display "`timestamp'"


**************************************************************************

di "pulling in mortality deaths data"
import delimited  /*FILEPATH*/, delim(",") varnames(1) clear

tempfile mort
save `mort'


// format to prep for merge with smr
gen age = ""
replace age = "young" if age_group_id < 9 | age_group == 28
replace age = "old" if age == ""

//Format from long to wide
keep age age_group_id location_id year_id sex_id draw mx 

keep if year_id == 1990 | year_id == 1995 | year_id == 2000 | year_id == 2005 | year_id == 2010 | year_id == 2016

reshape wide mx, i(age age_group_id location_id year_id sex_id) j(draw) 

rename mx* mort_draw_*

save `mort', replace 



// pull in SMR data
di "pulling in SMR data"
import delimited /*FILEPATH*/, clear
tempfile smr_vals
save `smr_vals'

foreach acause of local acause_list {

	use `smr_vals', clear

	di "Acause is `acause'"
	keep if acause == "`acause'"

	di "generating SMR draws"
	forvalues x = 0/999 {
		if mod(`x', 100)==0{
			di in red "working on draw `x'"
		}
		gen smr_draw_`x' = rnormal(mean, std)
	}

	di "format and merge SMR to mort data"
	rename sex sex_id
	gen age = ""
	replace age = "young" if age_start == 0
	replace age = "old" if age_start == 20 
	
	merge 1:m sex_id age using `mort', keep(3)
	drop _merge

	di "generating EMR draws"
	forvalues x = 0/999 {
		gen emr_draw_`x' = mort_draw_`x'*(smr_draw_`x' - 1)
	}

	di "regenerate mean and lower and upper confidence bounds (for DisMod)"
	drop orig_mean mean upper lower smr* mort*
	egen mean = rowmean(emr_draw*)
	fastpctile emr_draw*, pct(2.5 97.5) names(lower upper)
	keep location_id sex_id age_group_id year_id mean upper lower 

	di "save"
	export delimited /*FILEPATH*/, replace 

}
