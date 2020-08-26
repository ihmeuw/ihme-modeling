* do FILEPATH/ntd_zika/05a_deathPrep.do

*** SET UP ENVIRONMENT, TEMPFILES & LOCALS ***
clear all
set more off

if c(os) == "Unix" {
    local ADDRESS "FILEPATH"
    set odbcmgr ADDRESS
    }
else if c(os) == "Windows" {
    local ADDRESS "FILEPATH"
    }

tempfile appendTemp deaths locationMeta pop age_specific_pop

local inputDir FILEPATH 
local ageFile  `inputDir' FILEPATH    // age Distribution
local rootDir FILEPATH



*** GET FATAL OUTCOMES DATA ***
import excel FILEpATH firstrow clear

egen cases = rowtotal(Suspected Confirmed)
rename Deaths deaths
rename Imported imported
keep location_id deaths

drop if missing(location_id)

levelsof location_id, local(locations)

rename location_id country_id
save `deaths', replace



*** GET LOCATION METADATA (NEED TO BE ABLE TO LINK SUBNATIONALS AND THEIR PARENT COUNTRIES) ***
get_location_metadata, location_set_id(8) clear 
keep if is_estimate==1 | location_type=="admin0" | (location_id==`=subinstr("`locations'", " ", " | location_id==", .)')

split path_to_top_parent, parse(,) destring gen(split)
rename split4 country_id 

keep location_id location_name country_id parent_id location_type ihme_loc_id *region* is_estimate

save `locationMeta'

merge m:1 country_id using `deaths', keep(3) nogenerate
levelsof location_id, local(locations)



*** GET POPULATION ESTIMATES ***
get_population, location_id(`locations') year_id(2015 2016) sex_id(1 2) age_group_id(-1) clear
save `pop'



*** BRING IN DRAW FILES ***
local count 1
foreach location of local locations {
    capture import delimited using `rootDir'/inf_mod/`location'.csv, clear
    if _rc==0 {
        keep if inlist(year_id, 2015, 2016) & measure_id==6
        if `count'>1 append using `appendTemp'
        save `appendTemp', replace
        local ++count
        }
    }

merge 1:1 location_id year_id sex_id age_group_id using `pop', keep(3) nogenerate
merge m:1 location_id using `locationMeta', assert(2 3) nogenerate

egen cases = rowmean(draw_*)
replace cases = cases * population

collapse (sum) cases, by(country_id)
merge 1:1 country_id using `deaths', nogenerate



*** RUN MODEL AND GENERATE PREDICTIONS ***  
bysort country_id: egen anyReportedDeaths = max(deaths>0 & deaths<.)
generate casesRound = round(cases)
meglm deaths , family(binomial casesRound) || country_id:

generate touse = e(sample)
predict random, remeans reses(randomSe)

local mean = exp(_b[_cons])
local se = `mean' * _se[_cons]

local alpha = `mean' * (`mean' - `mean'^2 - `se'^2) / `se'^2 
local beta  = `alpha' * (1 - `mean') / `mean' 



*** APPLY COUNTRY RANDOM EFFECTS TO ALL CHILD SUBNATIONALS ***
sum random
local random = `r(mean)'
local randomSe = `r(sd)'

merge 1:m country_id using `locationMeta'

replace random = `random' if missing(random)
replace randomSe = `randomSe' if missing(randomSe)
generate gammaA = rgamma(`alpha', 1)
generate gammaB = rgamma(`beta', 1)
generate beta_fixed = gammaA / (gammaA + gammaB)
generate alpha = `alpha'
generate beta  = `beta'

replace anyReportedDeaths = 0 if missing(anyReportedDeaths)



keep country_id location_id random* alpha beta is_estimate anyReportedDeaths
save FILEPATH, replace
