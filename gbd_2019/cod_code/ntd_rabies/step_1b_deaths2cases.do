* Takes in step_1_deaths2cases.do call per locations and writes out location estimate

*** BOILERPLATE ***		
set more off
set maxvar 32000

local location `1'
local years `2'
local ages `3'
local outDir `4'

local years = subinstr("`years'", "_", " ", .)
local ages  = subinstr("`ages'", "_", " ", .)
local sexes 1 2

tempfile pop

run FILEPATH
run FILEPATH
sleep 100


*** PULL IN POPULTION ESTIMATES ***
get_population, location_id(`location') age_group_id(`ages') sex_id(`sexes') year_id(`years') decomp_step(step4) clear
save `pop'
sleep 100


*** PULL IN DEATH ESTIMATES ***
get_draws, gbd_id_type(cause_id) gbd_id(359) source(codcorrect) status(latest) decomp_step(step4) location_id(`location') age_group_id(`ages') sex_id(`sexes') year_id(`years') measure_id(1) clear 
keep if measure_id == 1
sleep 100

merge 1:1 location_id year_id age_group_id sex_id using `pop', nogenerate

keep location_id year_id age_group_id sex_id draw_* population


* The following line of code translates to Incidence = (Deaths + Survivers [assuming 99% CF]) / population 
* rbinomial will return missing if draw is < 1e-4; therefore assume that deaths equals cases for these VERY small numbers
*** CONVERT DEATHS TO CASES ***
forvalues i = 0/999 {
    quietly replace draw_`i' = (draw_`i' + rnbinomial(draw_`i',.99)) if draw_`i'>=0.0001   
    quietly replace draw_`i' =  draw_`i' / population
    quietly replace draw_`i' = 0 if age_group_id<4
}


*** CREATE PREVALENCE ESTIMATES ***	  
expand 2, generate(measure_id)
replace measure_id = measure_id + 5

forvalues i = 0/999 {	  
    * The following line of code translates to Prevalence = Incidence * 2-weeks duration  
    quietly replace draw_`i' = draw_`i' * (2/52) if measure_id==5
}


*** WRAP UP ***	
gen modelable_entity_id = ADDRESS	
export delimited location_id year_id age_group_id sex_id measure_id modelable_entity_id draw_* using `outDir'/`location'.csv, replace
