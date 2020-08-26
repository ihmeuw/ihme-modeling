adopath + FILEPATH
step STEP

tempfile ageMeta data pop covars

odbc load, exec("SELECT age_group_id, age_group_years_start, age_group_years_end FROM age_group") dsn(ADDRESS) clear
save `ageMeta'

import excel FILEPATH, sheet("extraction") firstrow clear

foreach var of varlist * {
	label variable `var' "`=`var'[1]'"
	}
	
drop in 1
drop if missing(measure)
	
destring nid smaller_site_unit location_id *_issue *_start *_end age_demographer mean lower upper standard_error effective_sample_size cases sample_size unit_value_as_published measure_adjustment uncertainty_type_value group is_outlier cv_*, replace

generate sex_id = 1 if sex=="Male"
replace  sex_id = 2 if sex=="Female"
replace  sex_id = 3 if sex=="Both"

generate index = _n

compress
save `data'

levelsof location_id, local(locations) clean


get_population, location_id(`locations') age_group_id(-1) sex_id(1 2) year_id(-1) decomp_step(`step') clear
save `pop'


foreach covar in 49 863 486 {
	get_covariate_estimates, covariate_id(`covar') location_id(`location') decomp_step(`step') clear 
	local name `=covariate_name_short[1]'
	keep location_id year_id age_group_id sex_id mean_value
	rename mean_value `name'
	
	if `covar'!= 49 merge 1:1 location_id year_id age_group_id sex_id using `covars', keep(3) nogenerate 
	save `covars', replace
	}
	
	
get_covariate_estimates, covariate_id(881) location_id(`locations') decomp_step(`step') clear
	local name `=covariate_name_short[1]'
	keep location_id year_id mean_value
	rename mean_value `name'
 
 merge 1:m location_id year_id using `covars', keep(3) nogenerate
 merge 1:1 location_id year_id age_group_id sex_id using `pop', keep(3) nogenerate
 merge m:1 age_group_id using `ageMeta', assert(2 3) keep(3) nogenerate
 
 foreach var of varlist SEV_wash_water HIV_prev_pct SEV_scalar_Diarrhea sdi {
	replace `var' = `var' * population
	}
 
 fastcollapse SEV_wash_water HIV_prev_pct SEV_scalar_Diarrhea sdi population, by(location_id year_id age*) type(sum) append
 replace sex_id = 3 if missing(sex)
 

joinby location_id sex_id using `data'
keep if inrange(year_id, year_start, year_end) & ((age_group_years_end>=age_start & age_group_years_start<=(age_end-age_demographer)) | (age_end==0 & age_start==0 & age_group_years_start==0))

fastcollapse SEV_wash_water HIV_prev_pct SEV_scalar_Diarrhea sdi population, by(index) type(sum)


 foreach var of varlist SEV_wash_water HIV_prev_pct SEV_scalar_Diarrhea sdi {
	replace `var' = `var' / population
	}


merge 1:1 index using `data', keep(3) nogenerate

keep if measure=="relrisk" & coinfection=="hiv"


tempfile append
save `append'

clear

get_covariate_estimates, covariate_id(486) location_id(-1) decomp_step(`step') clear
rename mean_value SEV_scalar_Diarrhea
keep location_id year_id age_group_id sex_id SEV_scalar_Diarrhea
gen toPred = 1


append using `append'

gen lnSevD = ln(SEV_scalar_Diarrhea)
gen lnMean = ln(mean)
gen lnLower = ln(lower)
gen lnUpper = ln(upper)

gen reGroup = string(nid)
replace reGroup = field_citation_value if missing(nid)
destring reGroup, replace

gen ageCat = 0 if age_start==0 & age_end<50
replace ageCat = 1 if age_start==0 & age_end==99
replace ageCat = 2 if age_start>0



merge m:1 age_group_id using `ageMeta', keep(1 3) nogenerate

save FILEPATH, replace

get_location_metadata, location_set_id(35) clear
keep location_id location_name *region*

merge 1:m location_id using  FILEPATH

generate ageMid = (age_start + age_end) / 2
replace  ageMid = (age_group_years_start + age_group_years_end) / 2 if missing(ageMid)

gen lnAgeMid = ln(ageMid)

nbreg mean lnSevD lnAgeMid, vce(cluster reGroup)
predict predMean, xb
predict predSe, stdp
generate predLower = predMean - predSe * invnormal(0.975)
generate predUpper = predMean + predSe * invnormal(0.975)
 
keep if toPred==1


keep location_id year_id age* sex_id pred* SEV_scalar_Diarrhea
save FILEPATH, replace


keep location_id year_id age_group_id sex_id predMean predSe	
rename pred* lnHivRr*
	
compress
save FILEPATH, replace

