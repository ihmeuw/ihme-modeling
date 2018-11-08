// pull LRI draws


adopath + "/FILEPATH/"
get_draws, gbd_id_type(cause_id) gbd_id(322) source(codem) version_id(484013) age_group_id(4 5) clear
tempfile male_u5
save `male_u5', replace

get_draws, gbd_id_type(cause_id) gbd_id(322) source(codem) version_id(483785) age_group_id(4 5) clear
tempfile female_u5
save `female_u5', replace

get_draws, gbd_id_type(cause_id) gbd_id(322) source(codem) version_id(483782) age_group_id(6 7) clear
tempfile male_over5
save `male_over5', replace

get_draws, gbd_id_type(cause_id) gbd_id(322) source(codem) version_id(484016) age_group_id(6 7) clear
tempfile female_over5
save `female_over5', replace

use `male_u5', clear
append using `male_over5'
append using `female_u5'
append using `female_over5'


save "/FILEPATH/", replace

// apply TB proportions to LRI draws

set more off

use "/FILEPATH/", clear
 
drop cause_id envelope pop sex_name metric_id measure_id

merge m:1 location_id year_id using "/FILEPATH/", keep(3) nogen

forvalues i = 0/999 {
			  replace draw_`i'=draw_`i'*tb_pr_`i'
			}
drop tb_pr_*

save "/FILEPATH/", replace


