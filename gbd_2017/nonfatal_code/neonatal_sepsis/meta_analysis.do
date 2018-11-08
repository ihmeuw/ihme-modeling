 


clear all
set more off
set maxvar 32000
ssc install estout, replace 
ssc install metan, replace


if c(os) == "Windows" {
	local j FILEPATH

}
if c(os) == "Unix" {
	local j FILEPATH
		
} 

local acause "`1'"
local grouping "`2'"
local parent_dir "`3'"
local dataset "`4'"
local timestamp "`5'" 

local out_dir FILEPATH 

 
use "`dataset'", clear

keep location_id year location_name super_region_id mean cases sample_size
 
rename mean data_val
gen data_val_se = (data_val * (1-data_val)/sample_size)^0.5
gen data_val_lower = data_val - 1.96*data_val_se
gen data_val_upper = data_val + 1.96*data_val_se
replace data_val_lower=0 if data_val_lower<0
replace data_val_upper=1 if (data_val_upper>1 & data_val_upper!=.)


 
metan data_val data_val_lower data_val_upper, random
 

gen mean = r(ES)
gen lower = r(ci_low)
gen upper = r(ci_upp)
gen se = (mean - lower)/1.96

expand 2, gen(iscopy)
gen sex=2
replace sex=1 if iscopy==1 


 
preserve
keep location_id year sex location_name super_region_id mean lower upper data_val 
 
local summ_out_dir "`out_dir'/summary"
local summ_archive_dir "`summ_out_dir'/_archive"
capture mkdir "`summ_out_dir'"
capture mkdir "`summ_archive_dir'"
local summ_fname "`acause'_`grouping'_summary"

save "`summ_out_dir'/`summ_fname'.dta", replace
export delimited using "`summ_out_dir'/`summ_fname'.csv", replace
export delimited using "`summ_archive_dir'/`summ_fname'_`timestamp'.csv", replace
restore


 

keep location_id year sex mean se 


di in red "generating draws"
forvalues i=1/1000{
	gen draw_`i' = rnormal(mean, se)
}


di in red "saving all draws"
drop mean se

local draw_out_dir = "`out_dir'/draws"
local archive_dir = "`draw_out_dir'/_archive"
capture mkdir "`draw_out_dir'"
capture mkdir "`archive_dir'"
local fname "`acause'_`grouping'_draws"

save "`draw_out_dir'/`fname'.dta", replace
export delimited using "`draw_out_dir'/`fname'.csv", replace
export delimited using "`archive_dir'/`fname'_`timestamp'.csv", replace



