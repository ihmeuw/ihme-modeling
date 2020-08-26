** Description: creating matched pairs for crosswalk
** Date: Jan 8, 2019

// Matching inpatient and claims_2000
insheet using "FILEPATH", comma clear

//compute year mean and round it to the nearest integer
gen year_mean=(year_start+year_end)/2	 
gen year=round(year_mean, 1)

// keep necessary variables
keep nid location_id year age_start age_end sex mean standard_error group_review
rename nid nid_inpatient
rename mean mean_inpatient
rename standard_error standard_error_inpatient
tempfile ref
save `ref', replace

insheet using "FILEPATH", comma clear	
gen year=2005 /* the closest year where inpatient data are available for the US */
keep nid location_id year year_start year_end age_start age_end sex mean standard_error
rename nid nid_claims_2000
rename mean mean_claims_2000
rename standard_error standard_error_claims_2000
merge 1:1 location_id year age_start age_end sex using `ref', keep(3) nogen
rename year closest_year
sort location_id year_start year_end age_start age_end sex
order location_id year_start year_end closest_year age_start age_end sex mean_inpatient standard_error_inpatient mean_claims_2000 standard_error_claims_2000

outsheet using "FILEPATH", comma replace

*****************************************************************************************************************************

// Matching inpatient and claims_2010-2016
insheet using "FILEPATH", comma clear

//compute year mean and round it to the nearest integer
gen year_mean=(year_start+year_end)/2	 
gen year=round(year_mean, 1)

// keep necessary variables
keep nid location_id year age_start age_end sex mean standard_error
rename nid nid_inpatient
rename mean mean_inpatient
rename standard_error standard_error_inpatient
tempfile ref
save `ref', replace

insheet using "FILEPATH", comma clear	
gen year=year_start
replace year=2010 if year_start==2011 | year_start==2012 | year_start==2013 | year_start==2014
keep nid location_id year year_start year_end age_start age_end sex mean standard_error group_review
rename nid nid_claims
rename mean mean_claims
rename standard_error standard_error_claims
merge m:1 location_id year age_start age_end sex using `ref', keep(3) nogen
rename year closest_year
sort location_id year_start year_end age_start age_end sex
order nid_inpatient nid_claims location_id year_start year_end closest_year age_start age_end sex mean_inpatient standard_error_inpatient mean_claims standard_error_claims

outsheet using "FILEPATH", comma replace

*********************************************************************************************************************************
// Matching inpatient and surveillance

insheet using "FILEPATH", comma clear

//round age groups to the closest multiple of 5 
gen age_s=round(age_start,1)
gen age_e=round(age_end,1)
//convert from numeric to string
tostring age_s, replace
tostring age_e, replace
//create age categories
gen age_cat=age_s+"-"+age_e		

//explore years
gen year_mean=(year_start+year_end)/2	 
gen year=round(year_mean, 1)
levelsof year  /* data are available for 1990 1995 2000 2005 2010 2015 */

// keep necessary variables
keep nid location_id year age_start age_end age_cat sex mean standard_error group_review
gen year_id=year
gen sex_id=.
replace sex_id=1 if sex=="Male"
replace sex_id=2 if sex=="Female"

// merge on inpatient envelope to compute cases and sample size
merge m:1 location_id year_id age_start sex_id using "FILEPATH", keepusing(sample_size) keep(3)nogen

gen cases=mean*sample_size
tempfile ref
save `ref', replace

// create custom age groups for "30-40" "40-50" "50-60" "60-70"

				forvalues i=30(10)60 {
				preserve
					local k=`i'+5
					keep if age_start>=`i' & age_start<=`k'
					collapse (sum) cases sample_size, by(nid location_id year sex)
					gen age=`i'
					gen age_s=age
					gen age_e=`i'+10
					tostring age_s, replace
					tostring age_e, replace
					gen age_cat=age_s+"-"+age_e		
					tempfile tmp_`i'
					save `tmp_`i'', replace 
				restore
			}
			use "`tmp_30'", clear
			forvalues i=30(10)60 {
				qui append using "`tmp_`i''"
			}

			tempfile custom_age_1
			save `custom_age_1', replace

// create custom age groups for "0-100"

            use `ref', clear
			collapse (sum) cases sample_size, by(nid location_id year sex)
            gen age_cat="0-100"
            tempfile custom_age_2
			save `custom_age_2', replace

// create custom age groups for "25-40"

            use `ref', clear
			keep if age_start>=25 & age_start<=35
			collapse (sum) cases sample_size, by(nid location_id year sex)
            gen age_cat="25-40"
            tempfile custom_age_3
			save `custom_age_3', replace
			
// create custom age groups for "5-15" "15-25" 

				use `ref', clear
				forvalues i=5(10)15 {
				preserve
					local k=`i'+5
					keep if age_start>=`i' & age_start<=`k'
					collapse (sum) cases sample_size, by(nid location_id year sex)
					gen age=`i'
					gen age_s=age
					gen age_e=`i'+10
					tostring age_s, replace
					tostring age_e, replace
					gen age_cat=age_s+"-"+age_e		
					tempfile tmp_`i'
					save `tmp_`i'', replace 
				restore
			}
			use "`tmp_5'", clear
			append using "`tmp_15'"
			tempfile custom_age_4
			save `custom_age_4', replace

// create custom age groups for "40-65"

            use `ref', clear
			keep if age_start>=40 & age_start<=60
			collapse (sum) cases sample_size, by(nid location_id year sex)
            gen age_cat="40-65"
            tempfile custom_age_5
			save `custom_age_5', replace
			
// create custom age groups for "65-100"

            use `ref', clear
			keep if age_start>=65 
			collapse (sum) cases sample_size, by(nid location_id year sex)
            gen age_cat="65-100"
            tempfile custom_age_6
			save `custom_age_6', replace
			
// create custom age groups for "70-100"

            use `ref', clear
			keep if age_start>=70 
			collapse (sum) cases sample_size, by(nid location_id year sex)
            gen age_cat="70-100"
            tempfile custom_age_7
			save `custom_age_7', replace
			
// append files
            
			use `custom_age_1', clear
			append using `custom_age_2'
            append using `custom_age_3'
			append using `custom_age_4'
			append using `custom_age_5'
			append using `custom_age_6'
			append using `custom_age_7'
			drop age age_s age_e
			// calculate mean and standard error
			gen mean=cases/sample_size
			gen standard_error=sqrt(mean*(1-mean)/sample_size)
			// append inpatient for GBD age groups
			append using `ref'
	
	rename mean mean_inpatient
    rename standard_error standard_error_inpatient
	rename nid nid_inpatient
	drop age_start age_end year_id sex_id
	save "FILEPATH", replace
	
			
insheet using "FILEPATH", comma clear

drop if year_start<1970

//round age groups to the closest multiple of 5 
gen age_s=round(age_start,5)
gen age_e=round(age_end,5)
//convert from numeric to string
tostring age_s, replace
tostring age_e, replace
//creat age categories
gen age_cat=age_s+"-"+age_e		

//explore age categories
sort age_start age_end
levelsof age_cat 

//explore years
gen year_mean=(year_start+year_end)/2	 
levelsof year_mean  /* data are available for 1987 1993.5 1995.5 1998.5 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 */
//round year to the closest multiple of 5 
gen year=round(year_mean, 5)
levelsof year  /* data are available for 1985 1995 2000 2010 2015 */

// Match 1987 to the 1990 CF2 data
replace year=1990 if year<1990

// keep necessary variables
keep location_id year year_start year_end age_cat age_start age_end sex mean standard_error nid group_review

// rename variables
rename mean mean_surveillance
rename standard_error standard_error_surveillance
rename nid nid_surveillance

merge m:m location_id year age_cat sex using "FILEPATH", keepusing(mean_inpatient standard_error_inpatient nid_inpatient) keep(3)nogen

sort location_id year_start year_end age_start age_end sex
order nid_inpatient nid_surveillance location_id year_start year_end age_start age_end sex mean_inpatient standard_error_inpatient mean_surveillance standard_error_surveillance

outsheet using "FILEPATH", comma replace
