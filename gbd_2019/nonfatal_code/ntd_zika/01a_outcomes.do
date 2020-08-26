/******************************************************************************\     
    Zika Model

    Background: As a newly emerging infectious disease with relatively little data, Zika is a poor fit to be modeled in Dismod. 
     
     Purpose:    This script pulls toghether and manages the Zika data,
                    and estimates incidence and proportions

     Pathway to run on cluster: do FILEPATH/ntd_zika/01a_outcomes.do

\******************************************************************************/

*** "metaprop" function only installed locally: "ssc install metaprop" 

*** BOILERPLATE ***
clear all
set more off, perm
set maxvar 10000

if c(os) == "Unix" {
    local ADDRESS "FILEPATH"
    set odbcmgr ADDRESS
    }
else if c(os) == "Windows" {
    local ADDRESS "FILEPATH"
    }

*** SETUP TEMPFILES & LOCALS ***
tempfile ab 

local inputDir FILEPATH
local dataDir FILEPATH
local inFiles FILEPATH 


*** CREATE SHELL FILE TO STORE VALUES OF ALPHA & BETA ***
clear

set obs `=wordcount("`inFiles'")'
generate outcome   = ""
generate alpha     = .
generate beta      = .
generate multiplier = .
save `ab'

*** LOOP THROUGH OUTCOMES, LOAD FILES & USE METAPROP TO ESTIMATE ALPHA & BETA *
local i 1

foreach inFile of local inFiles {
    local outcome = subinstr(subinstr("`inFile'", "zika_", "", .), ".xlsx", "", .)

    display "`inFile' = `outcome'"

    if "`inFile'" == "FILEPATH1"  | "`inFile'" == "FILEPATH2" {
        import excel "`dataDir'/`inFile'", sheet("Sheet1") firstrow clear

        if "`outcome'"=="gbs" keep if case_diagnostics == "RT-PCR or seroneutralisation"
        drop if missing(sample_size)

        capture destring sample_size, replace force
        capture destring value_case, replace force

        metaprop value_case sample_size, random nograph
        local mean `r(ES)'
        local se   `r(seES)'

        local alpha = `mean' * (`mean' - `mean'^2 - `se'^2) / `se'^2 
        local beta  = `alpha' * (1 - `mean') / `mean' 

        use `ab', clear
        replace outcome = "`outcome'" in `i'
        replace alpha   = `alpha' in `i'
        replace beta    = `beta' in `i'
        save `ab', replace
        }

    else if "`inFile'" == "_asymp" {
        use `ab', clear
        replace outcome = "`outcome'" in `i'
        replace multiplier = . in `i'
        save `ab', replace
        }

    else if "`inFile'" == "inf_mod" {
        use `ab', clear
        replace outcome = "`outcome'" in `i'
        replace multiplier = 1 in `i'
        save `ab', replace
        }

    else {
        use `ab', clear
        replace outcome = "`outcome'" in `i'
        replace multiplier = 5 in `i'
        save `ab', replace
        }

    local ++i
    }

generate modelable_entity_id = ADDRESS1 if outcome=="inf_mod" 
replace  modelable_entity_id = ADDRESS2 if outcome=="gbs_with_NIDs"
replace  modelable_entity_id = ADDRESS3 if outcome=="_asymp" 

preserve

import excel using "FILEPATH", firstrow clear    // prorportion asymptomatic
drop if n_total == 312 
*drop children-only point
collapse (sum) n*, by(location)
metaprop n_symptomatic n_total, random nograph
local mean `r(ES)'
local se   `r(seES)'

local alpha = `mean' * (`mean' - `mean'^2 - `se'^2) / `se'^2 
local beta  = `alpha' * (1 - `mean') / `mean' 

restore

replace alpha = `alpha' if inlist(outcome, "_asymp", "preg")
replace beta  = `beta' if inlist(outcome, "_asymp", "preg")

replace outcome="fatalities" if outcome=="fatalities_with_NIDs"
replace outcome="gbs" if outcome=="gbs_with_NIDs"


*** SAVE THE FILE ***
save FILEPATH, replace 
