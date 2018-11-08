
*pull in the draws from the age model here--the age draw code must be run on the cluster; 

*** BOILERPLATE ***
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		}
		
	
	

	
*** LOAD SHARED FUNCTIONS ***			
	adopath ++ FILEPATH
	run FILEPATH/get_demographics.ado
	run FILEPATH/create_connection_string.ado
	


adopath ++ "FILEPATH"
create_connection_string, database(ADDRESS) server(ADDRESS)
    local gbd_str = r(conn_string)

*import draws
use "FILEPATH/draws.dta", clear
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

*drop non-endemic
drop if endemic==0
*generate case variable for sex
forvalues i=0/999 {
	quietly {
	
	generate sex_cases_`i'=population * sex_draw_`i'

	}
}
 rename population sex_population
 drop _merge
 
 save "FILEPATH/gw_incidence2.dta", replace
 
 clear

 *** GET THE POPULATION ESTIMATES ***  
get_population, location_id("157 165 169 44860 35659 190 200 201 202 204 205 207 211 212 213 214 216 218 435 522 43908 43938 43918 43923 43926 43927 43935 43937") year_id("1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017") age_group_id("2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235") sex_id("1 2")
	bysort location_id year_id: egen totalPop = total(population)
  
  
  
*** MERGE POPULATON, ALL-AGE INCIDENCE ESTIMATES, & AGE/SEX CURVE ***   
	merge m:1 location_id year_id sex_id using "FILEPATH/gw_incidence2.dta", assert(3) keep(3)

	drop draw*
	
	
	merge m:1 sex_id age_group_id  using "FILEPATH/draws_2.dta", assert(3) nogenerate
  
save "FILEPATH/merged_draws_age_sex.dta", replace
use "FILEPATH/merged_draws_age_sex.dta", clear

get_location_metadata, location_set_id(35) clear
keep location_id is_estimate

merge 1:m location_id using "FILEPATH/merged_draws_age_sex.dta", assert(3) nogenerate keep(3)



drop cases_*

drop if is_estimate==0
drop if age_group_id==.

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
*export delimited using "FILEPATH/me_10523_end.csv", replace

use "FILEPATH/10523.dta"

expand 2, gen(new)
replace measure_id=5 if new==1

table measure_id



save "FILEPATH/10523.dta", replace






