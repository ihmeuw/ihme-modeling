

* BOILERPLATE *
  set more off
  clear all
  
  if c(os) == "Unix" {
    local j 
    set ""
    }
  else if c(os) == "Windows" {
    local j ""
    }
	

  tempfile hepA covariates template locations
  
/******************************************************************************\
                         BRING IN CASE FATALITY 
\******************************************************************************/  


  metaprop deaths cases, nograph random
  
  local mu `r(ES)'
  local sigma `r(seES)'
  local cfAlpha = (`mu' * (`mu' - `mu'^2 - `sigma'^2) / `sigma'^2) 
  local cfBeta = (`cfAlpha' * (1 - `mu') / `mu') 
  

/******************************************************************************\	  
                         BRING IN SEROPREVALENCE DATA 
\******************************************************************************/


  local files: dir . files ""
  local files: list sort files
  
  import excel using `=word(`"`files'"', wordcount(`"`files'"'))', firstrow clear
  
  
* CLEAN UP DATA *

 
  expand 2 if nid==141754, gen(newTemp)
  foreach var of varlist sample_size effective_sample_size cases {
   replace `var' = `var'/2 if nid==141754
   }
  foreach var of varlist lower upper standard_error {
   replace `var' = . if nid==141754
   }
  replace location_id = 43887 if nid==141754 & newTemp==0
  replace location_id = 43923 if nid==141754 & newTemp==1
  drop newTemp
  
  replace location_id = 98 if nid==236431
  replace location_name = ""  if nid==141754 | nid==236431
  
  assert measure=="prevalence"
  
* CREATE AGE MID-POINT FOR MODELLING *  
  destring age_demographer, replace
  replace age_end = age_end + age_demographer if !missing(age_demographer)
  egen ageMid = rowmean(age_start age_end)
  
* CREATE YEAR MID-POINT FOR MODELLING * 
  gen year_id = floor((year_start + year_end) / 2)
  
* CREATE SEX_ID *
  generate sex_id = (sex=="Male")*1 + (sex=="Female")*2 + (sex=="Both")*3  
  

* ENSURE THAT WE HAVE SAMPLE SIZE AND CASE NUMBERS FOR ALL ROWS *    
  replace cases = mean * effective_sample_size if missing(cases) & missing(sample_size)
  
  generate meanTestSS = abs(mean - cases/sample_size)
  generate meanTestESS = abs(mean - cases/effective_sample_size)

  generate alpha = mean * (mean - mean^2 - standard_error^2) / standard_error^2
  generate beta  = alpha * (1 - mean) / mean
  
  generate exp = effective_sample_size if missing(sample_size) | sample_size==effective_sample_size | (meanTestESS < meanTestSS & !missing(meanTestSS))
  replace  exp = sample_size if missing(exp) & (meanTestESS > meanTestSS & !missing(meanTestESS))
  replace  exp = alpha + beta if missing(exp)
  replace  exp = min(sample_size, effective_sample_size) if missing(exp)
  
  generate index=_n
  levelsof index if exp<=0, local(indicies) clean
  foreach index of local indicies {
    local stop 0
    local count 1
    while `stop'==0 {
      quietly cii `count' `=`count' * `=mean[`index']'', wilson
	  if (`r(lb)' >= `=lower[`index']' & `=mean[`index']' >= 0.5) | (`r(ub)' <= `=upper[`index']' & `=mean[`index']' < 0.5) {
	    local ++stop
		replace cases = `count' * `=mean[`index']' in `index'
		replace exp = `count' in `index'
		}		
	  local ++count
	  }
    }

  generate out = cases
  replace  out = exp * mean if missing(out)

  generate expRound = round(exp)
  generate outRound = round(out)
  
  keep location_id age_start age_end ageMid year_id sex_id mean lower upper standard_error effective_sample_size cases sample_size cv_* exp out expRound outRound is_outlier
  
  local nDataRows = _N
  di _N
  
  save `hepA'
 	
  gen before1980 = "(location_id==" + string(location_id) + " & year_id==" + string(year_id) + ")"
  levelsof before1980 if year_id<1980, clean sep( | ) local(keepBefore80)
  levelsof location_id if year_id<1980, clean sep ( | location_id==) local(toModelPre80)	
  
/******************************************************************************\	
                           PULL IN COVARIATE DATA
\******************************************************************************/

local covIds 57 142 160
local name57 ldi 
local name142 sanitation 
local name160 water

clear
tempfile covariates

foreach covId in `covIds'{
  get_covariate_estimates, covariate_id("`covId'") clear
  keep location_id year_id mean_value
  di "`name`covId''"
  rename mean_value `name`covId''
  cap merge 1:1 year_id location_id using `covariates', nogen
  save `covariates', replace
}
  
* EXTRAPOLATE WATER AND SANITATION VARIABLES OUT PRE-1980 TO MATCH PRE-1980 DATA POINTS *  
  foreach cov in water sanitation {

    bysort location_id (year_id): gen pctChange_`cov' = (`cov'[_n] - `cov'[_n-1]) / `cov'[_n-1]
    bysort location_id: egen `cov'Mean = mean(`cov')

    mixed pctChange_`cov' ldi `cov'Mean if location_id==`toModelPre80' || location_id: ldi `cov'Mean
    predict prChange_`cov'

    gsort location_id -year_id
    by location_id: replace `cov' =  `cov'[_n-1] - `cov'[_n-1] * prChange_`cov'[_n] if year_id<1980 & (location_id==`toModelPre80')
  }

* CREATE MODELLING COVARIATE USING PCA *
  gen lnLdi = ln(ldi)
  gen lnNoWater = ln(1 - water)
  pca lnNoWater lnLdi
  predict lnWL	
	
* CLEAN UP *	
  drop *Change* *Mean	
  keep if year>=1980 | `keepBefore80'
  
  save `covariates', replace
  
/******************************************************************************\	
 CREATE A SKELETON DATASET CONTAINING EVERY COMBINATION OF ISO, AGE, SEX & YEAR
\******************************************************************************/ 

*Get template for predictions
run 

get_location_metadata, location_set_id(8) clear
save `locations', replace

levelsof(location_id), local(location)

run 
get_demographics, gbd_team(cod) clear

local o = 1
gen age_group_id = .
gen year_id = .
gen sex_id = .
gen location_id = .

foreach i in `r(year_ids)'{
  foreach j in `r(age_group_ids)'{
    foreach k in `r(sex_ids)'{
      foreach p in `location'{
        set obs `o'
        replace location_id = `p' in `o'
        replace year_id = `i' in `o'
        replace age_group_id = `j' in `o'
        replace sex_id = `k' in `o'
        local o = `o'+1
      }
    }
  }
}

save `template', replace

odbc load, exec("SEQUEL QUERY") dsn("ADDRESS") clear
egen ageMid = rowmean(age_start age_end)

merge 1:m age_group_id using `template', nogen keep(3)
save `template', replace

use `locations', clear
merge 1:m location_id using `template', nogen keep(3)
save `template', replace

/******************************************************************************\	
         COMBINE HEP A, LOCATION, SKELETON, AND COVARIATE DATASETS
\******************************************************************************/
 
use `hepA', clear

merge m:1 location_id using `template', nogen keep(3)
  
merge m:1 location_id year_id using `covariates', keep(3) nogenerate

generate cfAlpha = `cfAlpha'
generate cfBeta  = `cfBeta'
  
