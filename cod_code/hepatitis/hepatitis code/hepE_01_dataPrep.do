* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off
  
  if c(os) == "Unix" {
    local j 
    set 
    }
  else if c(os) == "Windows" {
    local j 
    }
	
  
tempfile template locations
  
/******************************************************************************\	
 CREATE A SKELETON DATASET CONTAINING EVERY COMBINATION OF ISO, AGE, SEX & YEAR
\******************************************************************************/ 

*Get template for predictions

get_location_metadata, location_set_id(8) clear
save `locations', replace

levelsof(location_id), local(location)

get_demographics, gbd_team() clear

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

odbc load, exec("SQL QUERY") dsn() clear
egen ageMid = rowmean(age_start age_end)

merge 1:m age_group_id using `template', nogen keep(3)
save `template', replace

use `locations', clear
merge 1:m location_id using `template', nogen keep(3)
save `template', replace
   
/******************************************************************************\	
                          ESTIMATE CASE FATALITY
						  
        For males use CFR of non-pregnant people, for females calculate 
  country/age/year-specific CFRs as weighted averages of the pregant and non-
                              pregnant CFRs 
\******************************************************************************/  

  if c(os) == "Unix" {
    local
    set 
    }
  else if c(os) == "Windows" {
    local j 
    }


keep if pregnant < 3 & !missing(deaths) & jaundiced > 0  & deaths < jaundiced & year >= 1980

glm deaths pregnant, family(binomial jaundiced) robust eform

lincom _cons + pregnant * 1
  local nopregCf = r(estimate) 
  local nopregSe = r(se)

lincom _cons + pregnant * 2
  local pregCf = r(estimate) 
  local pregSe = r(se)

keep location_id year_id age_group_id prPreg
expand 2, gen(sex_id)
replace sex_id = sex_id + 1

expand 2 if year_id==2015, gen(dup)
replace year_id=2016 if dup==1
drop dup

merge 1:1 location_id year_id age_group_id sex_id using `template', keep(2 3) nogenerate

replace prPreg = 0 if age_group_id<7 | age_group_id>15 | sex==1 
replace prPreg = prPreg * 40 / 52
	
*ESTIMATE BETA DISTRIUBTION PARAMETERS FOR PROPORTION SYMPTOMATIC (From the Rein et al hep E paper) *
local mu    = 0.198
local sigma = (0.229 - 0.167)/(2 * invnormal(0.975))
local alpha = `mu' * (`mu' - `mu'^2 - `sigma'^2) / `sigma'^2 
local beta  = `alpha' * (1 - `mu') / `mu' 	

replace prPreg = 0 if prPreg==.

* CREATE CASE FATALITY DRAWS *  
forvalues i = 0/999 {
	local pregCfTemp = exp(rnormal(`pregCf', `pregSe'))
	local nopregCfTemp = exp(rnormal(`nopregCf', `nopregSe'))
	local symptomatic = rbeta(`alpha', `beta')
	
	quietly generate cfr_`i' = ((prPreg * `pregCfTemp') + ((1 - prPreg) * `nopregCfTemp')) * `symptomatic' 
	quietly replace  cfr_`i' = 0 if age_group_id < 3 | (!strmatch(region_name, "*Asia")==1 & !strmatch(region_name, "*Africa*")==1 & region_name!="Oceania")
  nois  di "`i'"
}

save "", replace
