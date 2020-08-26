*  do FILEPATH/ntd_zika/04_congenital.do

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

local inputDir FILEPATH 

local rootDir FILEPATH
local allAge FILEPATH            // all-Age Estimate
local birthSexRatio FILEPATH    // prBirthsBySex

tempfile nCongenital locationMeta population appendTemp congenitalPr noDataLocFile dataLocFile zeros no2zeros


*** LOAD SHARED FUNCTIONS ***
adopath + FILEPATH
adopath + FILEPATH
run FILEPATH
run FILEPATH

*** START LOG ***
cap log using "FILEPATH", replace

use `birthSexRatio', clear
drop if sex_id==3 | year<1990
tempfile birthSexRatio
save `birthSexRatio'



*** GET CONGENITAL OUTCOMES DATA ***
get_bundle_data, bundle_id(ADDRESS) decomp_step("iterative") clear
keep if value_keep_row==1
rename year_start year_id
levelsof location_id, local(locations) 
save `nCongenital'



*** GET LOCATION METADATA (NEED TO BE ABLE TO LINK SUBNATIONALS AND THEIR PARENT COUNTRIES) ***
get_location_metadata, location_set_id(ADDRESS) clear 
*keep if is_estimate==1 | location_type=="admin0" | (location_id==`=subinstr("`locations'", " ", " | location_id==", .)')
keep if is_estimate==1 | location_type=="admin0" 
split path_to_top_parent, parse(,) destring gen(split)
rename split4 country_id 

keep location_id location_name country_id parent_id location_type ihme_loc_id *region* is_estimate

levelsof location_id if parent_id==135, local(braSubs) clean

save `locationMeta'



*** BRING IN POPULATION ESTIMATES ***
levelsof location_id, local(locationList) clean
numlist "1980(1)2019"

get_population, location_id("`locationList'") sex_id("1 2") age_group_id("-1") year_id("`r(numlist)'") decomp_step("step4") clear
keep year_id age_group_id sex_id location_id population
keep if !inlist(age_group_id, ADDRESS, ADDRESS, ADDRESS)
save `population'

use `locationMeta', clear
merge 1:m location_id using `nCongenital'

keep if _merge==3 | strmatch(ihme_loc_id, "BRA*") | strmatch(ihme_loc_id, "MEX*") | strmatch(ihme_loc_id, "USA*") | strmatch(ihme_loc_id, "PHL*")

drop _merge
levelsof location_id, local(locations) 



*** GET ZIKA BIRTH DRAWS ***
local i 1
foreach location of local locations {
    quietly {
        capture import delimited using `rootDir'/preg/`location'.csv, clear
        if _rc==0 {
            if `i'>1 append using `appendTemp'
            save `appendTemp', replace
            local ++i
            }
        }
        di " . `location'" _continue
    }

keep if inrange(year_id, 2014, 2019)
collapse (sum) draw_*, by(location_id year_id)

egen zikaBirths = rowmean(draw_*)
drop draw_*



*** MERGE CONGENITAL DATA, ZIKA BIRTH ESTIMATES, & LOCATION META-DATA ***
merge 1:1 location_id year_id using `nCongenital', gen(congenitalMerge)
merge m:1 location_id using `locationMeta', keep(3) nogenerate

generate modLocation = location_id 
replace  modLocation = country_id if strmatch(ihme_loc_id, "MEX*") | strmatch(ihme_loc_id, "USA*") | strmatch(ihme_loc_id, "PHL*")
bysort country_id year_id: egen country_year_ZikaBirths = total(zikaBirths)
replace zikaBirths = country_year_ZikaBirths if inlist(ihme_loc_id, "BRA", "MEX", "USA", "PHL")
set obs `=_N+1'



***RUN MODEL TO ESTIMATE RATE OF CONGENITAL ZIKA SYNDROME AMONG ZIKA BIRTHS ***
mepoisson cases year_id if zikaBirths>0 & ihme_loc_id!="BRA", exp(zikaBirths) || modLocation: 

gen touse = e(sample)
local prCongenital = _b[_cons]
local prCongenitalSe = _se[_cons]
generate prCongenital = `prCongenital'
generate prCongenitalSe = `prCongenitalSe'
predict random, remeans reses(randomSe)



*** SAMPLE RANDOM EFFECTS FOR MISSING LOCATIONS ***
sum random if touse
local random = `r(mean)'
local randomSe = `r(sd)'
sum randomSe if touse
local randomSe = sqrt(`r(sd)'^2 + `randomSe'^2)
replace random = `random' if missing(random)
replace randomSe = `randomSe' if missing(randomSe)

keep location_id year_id country_id prCongenital* random* cases

merge m:1 location_id using `locationMeta', keep(1 3) nogenerate

replace random = `random' if missing(random)
replace randomSe = `randomSe' if missing(randomSe)
replace prCongenital = `prCongenital' if missing(prCongenital)
replace prCongenitalSe = `prCongenitalSe' if missing(prCongenitalSe)

merge 1:m location_id year_id using `population', keep(3) nogenerate

keep if is_estimate==1 & inrange(age_group_id, ADDRESS, ADDRESS)

collapse (sum) population (mean) prCongenital* random* cases, by(location* country_id parent_id *region* ihme_loc_id year_id sex_id)
save `congenitalPr'



*** GET FULL LIST OF ZIKA PREDICION LOCATIONS ***
levelsof location_id, local(zikaLocations) clean 
local zikaLocations: list zikaLocations | braSubs



*** CREATE ZERO DRAWS FOR NON-ZERO AGE GROUPS ***
clear
get_demographics, gbd_team(epi)
local years `r(year_id)'
local allLocations `r(location_id)'
local ages `r(age_group_id)'

set obs 24

generate int age_group_id = .
local i 1
foreach age in `r(age_group_id)' ADDRESS {
    replace age_group_id = `age' in `i'
    local ++i
    }
expand 2, gen(sex_id)
replace sex_id = sex_id + 1

expand 8
bysort age_group_id sex_id: generate year_id = (_n*5) + 1985
replace year_id = 2017 if year_id==2020
replace year_id = 2019 if year_id==2025

forvalues i = 0 / 999 {
    quietly generate draw_`i' = 0
    }

generate measure_id = 5
save `zeros'

drop if age_group_id<ADDRESS | age_group_id==ADDRESS
save `no2zeros'



*** PREP POPULATION FILE ***
get_population, location_id(`zikaLocations') year_id(`years') age_group_id(ADDRESS ADDRESS ADDRESS ADDRESS) sex_id(1 2) decomp_step ("step4") clear
tempfile zikaPop
save `zikaPop'

get_population, location_id(`zikaLocations') year_id(`years') age_group_id(ADDRESS) sex_id(1 2) decomp_step ("step4") clear
drop age_group_id

rename population mergePop

merge 1:m location_id year_id sex_id using `zikaPop', assert(3) nogenerate
replace  mergePop = population if age_group_id==ADDRESS
generate ageMerge = age_group_id
replace  ageMerge = 28 if age_group_id<ADDRESS
save `zikaPop', replace



*** PROCESS ZIKA BIRTH ESTIMATES TO GET CONGENITAL ZIKA SYNDROME ESTIMATES *
foreach location of local zikaLocations {
    import delimited using `rootDir'/preg/`location'.csv, clear
    replace age_group_id = 2
    merge m:1 location_id year_id sex_id using `congenitalPr', keep(3) nogenerate
    merge m:1 location_id year_id sex_id using `birthSexRatio', generate(birthSexMerge)
    keep if location_id == `location'
    
    rename draw_* draw_*_2

    quietly sum draw_0_2
    local drawMean = `r(mean)'
    
    forvalues i = 0 / 999 {
        quietly {
            if `drawMean' > 0 {
                if `i'<=26 {
                    replace draw_`i'_2 = cases if !missing(cases) & year_id>=2016
                    replace draw_`i'_2 = draw_`i'_2 * exp(rnormal(prCongenital, prCongenitalSe) + rnormal(random, randomSe)) if missing(cases) | year_id<2016
                    }
                else {
                    replace draw_`i'_2 = draw_`i'_2 * exp(rnormal(prCongenital, prCongenitalSe) + rnormal(random, randomSe)) 
                    }
                }
            else {
                generate gammaA = rgamma(cases, 1)
                generate gammaB = rgamma(population, 1)
                replace draw_`i'_2 =  prBirthsBySex * population * gammaA / (gammaA + gammaB) if cases>0 & !missing(cases) & year_id>=2016
                replace draw_`i'_2 = 0 if cases==0 | missing(cases) | year_id<2016
                drop gammaA gammaB
                }
            bysort location_id sex_id (year_id): generate draw_`i'_5 = draw_`i'_2[_n-1]
            }
        }

    quietly {
        keep draw* location_id year_id sex_id
        ds draw_*_2
        reshape long `=subinstr("`r(varlist)' ", "_2 ", "_ ", .)', i(location_id year_id sex_id) j(age_group_id)
        rename draw_*_ draw_*
        keep if inlist(year_id, `=subinstr("`years'", " ", ",", .)')

        generate ageMerge = age_group_id
        replace  ageMerge = 28 if age_group_id<ADDRESS
        drop age_group_id
        merge 1:m location_id year_id ageMerge sex_id using `zikaPop', assert(2 3) keep(3) nogenerate

        forvalues i = 0 / 999 {
            quietly replace draw_`i' = draw_`i' / mergePop 
            }
        }
    
    generate measure_id = 5
    expand 2 if age_group_id==ADDRESS, gen(newObs)
    replace age_group_id = ADDRESS if newObs==1
    drop newObs
    
    append using `no2zeros'
    replace location_id = `location' if missing(location_id)
    
    generate modelable_entity_id = ADDRESS
    replace measure_id = 5
    export delimited location_id year_id age_group_id sex_id measure_id modelable_entity_id draw_* using FILEPATH, replace
    }

local nonZikaLocations: list allLocations - zikaLocations

macro dir
use `zeros', clear
generate location_id = .
generate modelable_entity_id = ADDRESS
replace measure_id = 5

foreach location of local nonZikaLocations {
    quietly replace location_id = `location'
    export delimited location_id year_id age_group_id sex_id measure_id modelable_entity_id draw_* using FILEPATH, replace
    }



*** SAVE RESULTS ***
save_results, modelable_entity_id(ADDRESS) description("Congenital Zika Syndrome") in_dir(`rootDir'/congenital) file_pattern(FILEPATH) mark_best(no) birth_prev("yes") env("prod")
