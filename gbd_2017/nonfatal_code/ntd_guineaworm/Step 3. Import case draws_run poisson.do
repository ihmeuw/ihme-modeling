
*Draws come from R dataset using beta distribution*


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
		
	
	tempfile ages


	
*** LOAD SHARED FUNCTIONS ***			
	adopath ++ FILEPATH
	run FILEPATH/get_demographics.ado
	run FILEPATH/create_connection_string.ado
	


adopath ++ "FILEPATH"
create_connection_string, database(ADDRESS) server(ADDRESS)
    local gbd_str = r(conn_string)

	
//Step 1. Pull in case data by country-year from dataset from endemic countries, with beta draws from R dataset

insheet using "FILEPATH\beta_draws.csv", clear

*drop any Ethiopia national level location_ids
drop if location_id==179

generate sex_id=3
*both sexes

generate age_group_id=22
*all ages

save "FILEPATH\gw_step1b.dta", replace

use "FILEPATH\gw_step1b.dta", clear
//Step 3. run poisson regression by country

	
	replace cases=. if is_outlier==1
		
	
	levelsof location_id, local(locations) clean
	
	generate predicted = .
	generate predictedSe=.
	generate ln_size=ln(population)
	generate obs_inc=cases/population
	generate ln_obs_inc=ln(cases/population)
	drop v6
	
	local failed 
	
foreach location of local locations {
	quietly poisson cases year_id  if location_id==`location',  offset(ln_size) 
	if `e(converged)'==1  {
	quietly {
			predict temp  if location_id==`location', xb nooffset
			predict tempSe if location_id==`location', stdp nooffset
			
				replace predicted = temp  if location_id==`location'
				replace predictedSe = tempSe  if location_id==`location'

				drop temp tempSe
				}
			di "." _continue
			}
		else {
			local failed `failed' `location'
			di _n "`location' failed to converge"
			}
		}

 
forvalues i = 0 / 999 {
		
		
		quietly replace  draw_`i' = exp(rnormal(predicted, predictedSe)) if missing(cases)
		
		}
		

//Make sure that we have no negative draws (set them to zero)

forvalues i=0/999 {

	quietly replace draw_`i'=0 if draw_`i'<0
	
}


generate exp_pred=exp(predicted)
save "FILEPATH\gw_step3_b.dta", replace




*generate figure to determine how much the model over/under predicts

use "FILEPATH\gw_step3_b.dta", clear
 
forvalues i=0/999 {
	generate cases_`i'=draw_`i'*population
	
	}

egen mean_draw=rmean(cases_0-cases_999)
egen max_cases=rmax(cases_0-cases_999)
egen min_cases=rmin(cases_0-cases_999)
egen max_draw=rmax(draw_0-draw_999)
egen min_draw=rmin(draw_0-draw_999)

generate max_cases_diff=max_cases-cases
generate min_cases_diff=min_cases-cases

*generate case_diff=mean_dcases-cases
*positive case_diff means model over predicts, negative means model under_predicts



save "FILEPATH\gw_step4_b.dta", replace


*generate an output dataset to summarize differences in predicted and observed cases by country, year

use "FILEPATH\gw_step4_b.dta", clear
bysort year_id location_id: egen total_cases_pred=total(mean_draw)
*graph predicted v. observed cases

graph bar (sum) total_cases_pred cases, over(year_id) blabel(total)




//Step 4. Apply sex-split to the data

*test with sex_pop

use "FILEPATH\gw_step4_b.dta", clear 

rename population total_pop
drop sex_id
expand 2, generate(sex_id)
replace sex_id = sex_id + 1

*1=male, 2=female

save "FILEPATH\gw_step4_b2.dta", replace

get_population, location_id("-1") age_group_id("22") sex_id("1 2") location_set_id("35") year_id("1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017") clear
  
  save "FILEPATH\sex_pop_results_b2.dta", replace

  
use "FILEPATH\sex_pop_results_b2.dta", 
  merge 1:1 location_id age_group_id sex_id year_id using "FILEPATH\gw_step4_b2.dta"
  
  drop if year_id<1990
  
generate endemic=0 if _merge==1
replace endemic=1 if _merge==3

*apply sex split here

*population here is sex-specific:
*We take the total incidence multiplied by the proportion of GW disease reported in males (47%) and females (53%) times the total national population to get cases in males and females, respectively
*then we divide the sex-specific case estimate (sex_draw_`i') by the sex-specific population (variable: population) to get the incidence for males and females, respectively


forvalues i=0/999 {
	quietly generate sex_draw_`i'=(draw_`i'*.47)*total_pop if sex_id==1
	quietly replace sex_draw_`i'=(draw_`i'*.53)*total_pop if sex_id==2
	
	quietly replace sex_draw_`i'=sex_draw_`i'/population
	}



save "FILEPATH\gw_incidence.dta", replace


**move on to get_draws and apply age split

