

* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off
  
  if c(os)=="Unix" {
	local  = "FILEPATH"
	set odbcmgr ADDRESS
	}
  else {
	local  = "FILEPATH"
	}

adopath + FILEPATH
run "FILEPATH"
run "FILEPATH"
run "FILEPATH"
run "FILEPATH"
run "FILEPATH"
run "FILEPATH"
	
	
* ESTABLISH LOCALS AND DIRECTORIES *	
 

*** ONTO THE MODELLING ***

import delimited FILEPATH, clear case(preserve)
*use FILEPATH", clear
drop if modelled==1
*collapse (mean) efTotal, by(location_id year_id modelled)
tempfile efTest
save `efTest'
*/
use FILEPATHmodelingData2016.dta, clear
*use FILEPATH, clear
drop efTotal - childrenonly 
merge 1:m location_id year_id using `efTest'
capture drop _merge

drop is_estimate 
tempfile data
save `data'

get_location_metadata, location_set_id(35) clear
merge 1:m location_id using `data', assert(1 3)
assert is_estimate==0 if _merge==1
drop if _m==1


gen countryIso = substr(iso3, 1, 3)
bysort countryIso year_id: egen rrMean2010_mean = mean(rrMean2010)
replace rrMean2010 = rrMean2010_mean if missing(rrMean2010)

bysort countryIso year_id: egen rrMean_mean = mean(rrMean)
replace rrMean = rrMean_mean if missing(rrMean)

gen lnRr2010 = ln(rrMean2010)
gen lnRrMean = ln(rrMean)
gen lnMean = ln(meanM)

gen lnTrend = lnRr2010
replace lnTrend = lnRrMean if region_name=="Oceania"



regress score denguePr
predict scorePred
gen deviation = score - scorePred
sum deviation
replace score = scorePred if deviation > abs(`r(min)')
drop scorePred deviation


capture drop denguePrS*
capture drop scoreS*
capture drop efTemp 
capture drop efTest
capture drop random* fixed*

mkspline denguePrS = denguePr, cubic knots(0 .3 .8 1)
mkspline scoreS = score, cubic knots(-2 0 1)

local spaceVar scoreS
local timeVar lnTrend

  
menbreg casesM `spaceVar'* `timeVar' if denguePr>0 /*nid*/, exp(sampleM) intmethod(mvaghermite)  || location_id:

  predict randomModel, reffects reses(randomModelSe) nooffset
  predict fixed, fixedonly fitted nooffset
  predict fixedSe, stdp fixedonly nooffset
 
capture drop predTest 
generate predTest = 0
foreach var of varlist `spaceVar'* {
	replace predTest =  predTest + `var' * _b[`var']
	}
replace predTest = exp(predTest)
scatter predTest score if year_id==2015
  

gen efTemp = randomModel + ln(efTotal)
nbreg efTemp childrenonly
gen efTemp2 = efTemp
replace efTemp2 = efTemp / exp(_b[childrenonly]) if childrenonly==1

metan efTemp2 randomModelSe if modelled==0, wgt(casesM) 
*metan efTemp efSe if modelled==0, wgt(casesM) 
generate random   = `r(ES)'
generate randomSe = `r(seES)' 

*br location_id location_name year_id cases sample_size casesEf sample_sizeEf efTotal childrenonly modelled delphi efTemp random _* if !missing(efTotal)
*sort efTemp
 
*save FILEPATH, replace
 

* CREATE THE 1000 DRAWS *
forvalues i = 0/999 {
  local fixedTemp  = rnormal(0,1)
  local randomTemp = rnormal(0,1)
  
  quietly {
	 
	 generate draw_`i' = exp(rnormal(fixed, fixedSe) + rnormal(random, randomSe)) * ef_`i' * population  
	 replace  draw_`i' = exp(rnormal(fixed, fixedSe) + rnormal(randomModel, randomModelSe)) * ef_`i' * population if inrange(randomModel, random, .) & !missing(mean)
	 replace  draw_`i' = 0 if denguePr==0 
	 
	 
	 }

  di "." _continue
  }

  

  
fastcollapse draw_* population, by(location_id location_name ihme_loc_id yearWindow is_estimate) type(mean)

rename yearWindow year_id



fastrowmean draw_*, mean_var_name(casesMean)
fastpctile draw_*, pct(2.5 97.5) names(casesLower casesUpper)

foreach var of varlist cases* {
  format `var' %12.0fc
  tabstat `var' if is_estimate==1, by(year_id) stat(n mean sum)
  }
*/

replace year_id=2016 if year_id==2015

* BRING IN AGE DISTRIBUTION DATASET *
rename population allAgePop

merge 1:m location_id year_id using FILEPATH, nogenerate
*merge 1:m location_id year_id using FILEPATH, nogenerate
keep if is_estimate==1 & inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016) & age_group_id!=22


keep year_id age_group_id sex_id location_id draw* allAgePop population incCurve is_estimate	
replace incCurve = 0 if age_group_id<4

generate casesCurve = incCurve * population
bysort year_id location_id: egen totalCasesCurve = total(casesCurve)

forvalues i = 0/999 {
  quietly {
  replace draw_`i' = casesCurve * draw_`i' / totalCasesCurve
  replace  draw_`i' = draw_`i' / population
  }
  di "." _continue
  }
  
fastrowmean draw_*, mean_var_name(casesMean)
fastpctile draw_*, pct(2.5 97.5) names(casesLower casesUpper)
foreach var of varlist cases* {
  replace `var' = `var' * population
  format `var' %12.0fc
  tabstat `var' if is_estimate==1, by(year_id) stat(n mean sum)
  tabstat `var' if is_estimate==1, by(age_group_id) stat(n mean sum)
  }
tabstat casesMean if is_estimate==1, by(age_group_id) stat(n mean sum)
tabstat casesMean if is_estimate==1, by(sex_id) stat(n mean sum)
  
preserve
keep year_id location_id age_group_id sex_id draw_*
generate modelable_entity_id = ADDRESS
generate measure_id = 6

export delimited using FILEPATH, replace



* SEQUELA SPLIT *  
  
keep year_id location_id age_group_id sex_id draw_*
tempfile incTemp
save `incTemp', replace

gen id = _n
expand 3
bysort year_id location_id age_group_id sex_id: gen index = _n
generate seq = "mod"  if index==1
replace  seq = "sev"  if index==2
replace  seq = "post" if index==3
generate post = seq=="post"

generate modelable_entity_id = ADDRESS1 if seq == "mod"
replace  modelable_entity_id = ADDRESS2 if seq == "sev"
replace  modelable_entity_id = ADDRESS3 if seq == "post"

generate duration  = 6/365  if seq == "mod"  // Source of duration: Whitehead et al, doi: 10.1038/nrmicro1690
replace  duration  = 14/365 if seq == "sev"  // Source of duration: Whitehead et al, doi: 10.1038/nrmicro1690
replace  duration  = 0.5    if seq == "post" 

local modMu = 0.945 
local sevMu = 0.055 
local postMu = 0.084

local modSigma = 0.074   
local sevSigma = 0.00765 
local postSigma = 0.02 
  
foreach seq in mod sev post {
  local `seq'Alpha = ``seq'Mu' * (``seq'Mu' - ``seq'Mu'^2 - ``seq'Sigma'^2) / ``seq'Sigma'^2
  local `seq'Beta  = ``seq'Alpha' * (1 - ``seq'Mu') / ``seq'Mu'
  } 
  
forvalues i = 0/999 {
  foreach seq in mod sev post {
    local `seq'Pr = rbeta(``seq'Alpha', ``seq'Beta')
	}
  local correction = `modPr' + `sevPr'	
  local modPr = `modPr' / `correction' 
  local sevPr = `sevPr' / `correction' 

  foreach seq in mod sev post {
    quietly replace draw_`i' = draw_`i' * ``seq'Pr'  if seq=="`seq'"
	}
  }
	
  expand 2, gen(measure_id)
  replace measure_id = measure_id + 5
  
  forvalues i = 0/999 {
	quietly replace draw_`i' =  draw_`i' * duration if measure_id==5
	}
	
	
* EXPORT DRAW FILES *		
  
  keep location_id year_id sex_id age_group_id modelable_entity_id measure_id draw_*  
  
  local modN  ADDRESS 
  local sevN  ADDRESS
  local postN ADDRESS
  
  foreach seq in mod sev post {
 	outsheet if modelable_entity_id==``seq'N' using `outDir'/FILEPATH, comma replace
	}
    

	
* UPLOAD RESULTS *
runFILEPATH

foreach seq in mod sev post {
  capture rm `outDir'/inf_`seq'/all_draws.h5
  }
  
local spaceVar scoreS
local timeVar lnRr2010
  
save_results, modelable_entity_id(ADDRESS1) description("Dengue fever (using `spaceVar' & `timeVar')") in_dir("FILEPATH") file_pattern("FILEPATH") mark_best(yes) env("prod")	
save_results, modelable_entity_id(ADDRESS2) description("Severe dengue fever (using `spaceVar' & `timeVar')") in_dir("FILEPATH") file_pattern("FILEPATH") mark_best(yes)	 env("prod")
save_results, modelable_entity_id(ADDRESS3) description("Post-dengue fatigue (using `spaceVar' & `timeVar')") in_dir("FILEPATHt") file_pattern("FILEPATH") mark_best(yes)  env("prod")
	
	

