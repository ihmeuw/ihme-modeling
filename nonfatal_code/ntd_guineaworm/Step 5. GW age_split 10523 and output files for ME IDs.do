
*pull in the draws from the age model here--the age draw code must be run on the cluster; 


adopath ++ "FILEPATH"
create_connection_string, database(gbd) server(modeling-gbd-db)
    local gbd_str = r(conn_string)
	
*import draws from dismod age split
use "FILEPATHS/draws.dta", clear
*rename draw variable
forvalues i=0/999 {

	rename draw_`i' ageCurve_`i'
	
	}

	*drop year_id variable from draws, the curve is for all years
drop year_id
	
*save file for merging later
save "FILEPATH/draws_2.dta", replace

	
	
*import incidence data 
use "FILEPATH/gw_incidence.dta" , clear
*generate case variable for sex
forvalues i=0/999 {
	quietly {
	
	generate sex_cases_`i'=population * sex_draw_`i'

	}
}
 rename population sex_population
 drop _merge
 
 save "FILEPATH/gw_incidence2.dta", replace

 *** GET THE POPULATION ESTIMATES ***  
get_population, location_id("-1") year_id("1990 1995 2000 2005 2010 2016") age_group_id("2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235") sex_id("1 2") location_set_id("35") clear
	drop process_version_map_id
	bysort location_id year_id: egen totalPop = total(population)
  
  
  
*** MERGE POPULATON, ALL-AGE INCIDENCE ESTIMATES, & AGE/SEX CURVE ***   
	merge m:1 location_id year_id sex_id using "FILEPATH/gw_incidence2.dta", assert(3) 

	drop draw*
	
	
	merge m:1 sex_id age_group_id  using "FILEPATH/draws_2.dta", assert(3) nogenerate
  
save "FILEPATH/merged_draws_age_sex.dta", replace
use "FILEPATH/merged_draws_age_sex.dta", clear

get_location_metadata, location_set_id(35) clear
keep location_id is_estimate

merge 1:m location_id using "FILEPATH/merged_draws_age_sex.dta", assert(3) nogenerate



drop cases_*

drop if is_estimate==0

*apply age split
forvalues i = 0 / 999 {
		quietly {
			
			
			generate casesCurve = ageCurve_`i' * population
			bysort location_id year_id sex_id: egen totalCasesCurve = total(casesCurve)
		
			generate inc_age_draw`i' = endemic * casesCurve * (sex_cases_`i' / totalCasesCurve) / population
			
			drop casesCurve totalCasesCurve
			}
		di "." _continue
		}
	
	
*prepare data for upload

keep location_id year_id age_group_id sex_id measure_id inc_age_draw* 

forvalues i=0/999 {

	rename inc_age_draw`i' draw_`i'

	}


	
generate modelable_entity_id=10523
replace measure_id=6

forvalues i=0/999 {

	quietly replace draw_`i'=0 if draw_`i'==.
	
	}

	
*fix for under 1 yrs should be zero incidence


forvalues i=0/999 {

	quietly replace draw_`i'=0 if age_group_id<5
	
	}
	
	
save "FILEPATH/10523.dta", replace
export delimited using "FILEPATH/me_10523_all.csv", replace



*****generate output for MEIDs: 11646 pain (moderate)*******************************

*duration= 1/12 (1 month of moderate pain for everyone)

use "FILEPATH/10523.dta", clear

drop measure_id

expand 2, generate(measure_id)
replace measure_id= measure_id+5

forvalues i=0/999 {

	quietly replace draw_`i'=draw_`i'*(1/12) if measure_id==5
}



replace modelable_entity_id=11646

save "FILEPATH/11646.dta", replace
export delimited using "FILEPATH/me_11646_all.csv", replace






**ME ID 11647 pain for all 2 months, 30% of cases for 9/12

*duration= 2/12 (2 months of mild pain for everyone, 30% of cases for additional 9 months)=0.392

use "FILEPATH/10523.dta", clear
forvalues i=0/999 {

	quietly replace draw_`i'=draw_`i'*.392
}

replace modelable_entity_id=11647
replace measure_id=5

save "FILEPATH/11647.dta", replace
export delimited using "FILEPATH/me_11647_all.csv", replace



*ME ID 11648 *1month of limited mobility

use "FILEPATH/10523.dta", clear
meforvalues i=0/999 {

	quietly replace draw_`i'=draw_`i'*(1/12)
}

replace modelable_entity_id=11648
replace measure_id=5

save "FILEPATH/11648.dta", replace
export delimited using "FILEPATH/me_11648_all.csv", replace

