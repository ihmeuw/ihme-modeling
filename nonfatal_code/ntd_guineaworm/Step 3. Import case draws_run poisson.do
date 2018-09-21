
*pull in draws from beta distribution (Steps 1 & 2) and model for years/locations where data are missing or implausible 



adopath ++ "FILEPATH"
create_connection_string, database(gbd) server(modeling-gbd-db)
    local gbd_str = r(conn_string)

//Step 1. Pull in case data by country-year from dataset from endemic countries, with beta draws from R dataset

insheet using "FILEPATH/beta_draws.csv", clear



generate sex_id=3
*both sexes

generate age_group_id=22
*ALL AGES

save "FILEPATH/gw_step1b.dta", replace

use "FILEPATH/gw_step1b.dta", clear
//Step 3. run poisson regression by country

  
	
	replace cases=. if is_outlier==1
			
	levelsof location_id, local(locations) clean
	levelsof sex_id, local(sexes) clean
	generate predicted = .
	generate predictedSe=.
	generate ln_size=ln(population)
	generate obs_inc=cases/population
	generate ln_obs_inc=ln(cases/population)
	drop v6
	
	local failed 

*RUN POISSON MODEL FOR EACH LOCATION SEPARATELY
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

 
 *IF CASE DATA IS MISSING (OR WAS OUTLIERED AND SET TO MISSING) USE THE POISSON MODEL TO IMPUTE DRAWS AND SE FOR UNCERTAINTY
forvalues i = 0 / 999 {
		
		
		quietly replace  draw_`i' = exp(rnormal(predicted, predictedSe)) if missing(cases)
		
		}
		

//Make sure that we have no negative draws (set them to zero)

forvalues i=0/999 {

	quietly replace draw_`i'=0 if draw_`i'<0
	
}


*CHECK THE NUMBER OF CASES PREDICTED FROM THE POISSON FOR DIAGNOSIS OVER-PREDICTION
generate exp_pred=exp(predicted)
save "FILEPATH/gw_step3_b.dta", replace




*generate figure to determine how much the model over/under predicts

use "FILEPATH/gw_step3_b.dta", clear
 
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

twoway(scatter max_cases_diff year_id)
twoway(scatter min_cases_diff year_id)



//drop for years not required for GBD
//drop years not in 1990, 1995, 2000, 2005, 2010, 2016

generate keep=1 if year_id==1990 | year_id==1995 | year_id==2000 | year_id==2005 | year_id==2010 | year_id==2016
drop if missing(keep)

*SAVE RESULTS FOR GBD YEARS 
save "FILEPATH/gw_step4_b.dta", replace


*generate an output dataset to summarize differences in predicted and observed cases by country, year

use "FILEPATH/gw_step4_b.dta", clear
bysort year_id location_id: egen total_cases_pred=total(mean_draw)
*graph predicted v. observed cases

graph bar (sum) total_cases_pred cases, over(year_id) blabel(total)




//Step 4. Apply sex-split to the data


use "FILEPATH", clear 

rename population total_pop
drop sex_id
expand 2, generate(sex_id)
replace sex_id = sex_id + 1

*1=male, 2=female

save "FILEPATH/gw_step4_b2.dta", replace

*GET SEX SPECIFIC POPULATION DATA
get_population, location_id("-1") age_group_id("22") sex_id("1 2") year_id("1990 1995 2000 2005 2010 2016") location_set_id("35") clear
  drop process_version_map_id   
  save "FILEPATH/sex_pop_results_b2.dta", replace

use "FILEPATH/sex_pop_results_b2.dta", 
  merge 1:1 location_id age_group_id sex_id year_id using "FILEPATH/gw_step4_b2.dta"
  
generate endemic=0 if _merge==1
replace endemic=1 if _merge==3

*APPLY SEX PROPORTION TO MALES SEX_ID=1 AND FEMALES SEX_ID=2 ACROSS ALL DRAWS
forvalues i=0/999 {
	quietly generate sex_draw_`i'=(draw_`i'*.47)*total_pop if sex_id==1
	quietly replace sex_draw_`i'=(draw_`i'*.53)*total_pop if sex_id==2
	
	quietly replace sex_draw_`i'=sex_draw_`i'/population
	}


save "FILEPATH/gw_incidence.dta", replace


**move on to get_draws and apply age split

