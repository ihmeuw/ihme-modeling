** Purpose: Generate weights for splitting N codes in India

** set the prefix for whichever os we're in
if c(os) == "Windows" {
    global j "J:"
}
if c(os) == "Unix" {
    global j "FILEPATH"
    set odbcmgr unixodbc
}

    local source = source in 1
    if "`source'" == "India_MCCD_states_ICD10" | "`source'"=="_India_10_tabulated_MCCD" local n_chapter = 19
    if "`source'" == "India_MCCD_states_ICD9" | "`source'"=="_India_9_tabulated_MCCD" local n_chapter = 17
    local e_chapter = `n_chapter'+1

** PREP DATA FOR WEIGHTS
    ** use "FILEPATH" if regexm(cause, "^(`n_chapter')|(`e_chapter')"), clear
    keep if regexm(cause, "^(`n_chapter')|(`e_chapter')")

    ** make age long
    
    reshape long deaths, i(subdiv cause cause_name frmat sex year) j(age)
    drop if age==1 | age==2
    ** get rid of out-of-format ages 
    bysort age frmat: egen totdeaths = total(deaths)
    drop if totdeaths==0
    drop totdeaths

    tempfile rawdata
    save `rawdata', replace

    ** get SRS region mapping
    merge m:1 location_id using "FILEPATH", keep(1 3) keepusing(srs_region) nogen
    ** Lump the six union territories into the 'South' region because many of the six are in that region. 
    replace srs_region = "South" if regexm(subdiv, "Six Minor")
    if "`source'"=="_India_10_tabulated_MCCD" | "`source'"=="_India_9_tabulated_MCCD" {
        replace srs_region="National"
    }

    keep subdiv cause cause_name sex year age deaths* srs_region
    isid subdiv cause sex year age

    ** SAVE A TEMPLATE OF THE DESIRED FINAL WEIGHT DATA
    ** the very final dataset should be the cartesian prodcut of all stateageyears in the original and the causes that we are developing proportions for
    ** so make a template that has everything that needs a weight
    tempfile data
    save `data', replace
    keep cause year cause_name
    keep if regexm(cause, "^`e_chapter'")
    duplicates drop
    tempfile causeyears
    save `causeyears', replace

    use `data', clear
    keep subdiv year age sex srs_region
    duplicates drop
    tempfile stateageyears
    save `stateageyears', replace

    ** make cartesion product of two datasets
    joinby year using `causeyears'
    tempfile template
    save `template', replace

    ** DROP BAD DATA
    ** get rid of N codes
    use `data', clear
    drop if regexm(cause, "^`n_chapter'")

    ** get rid of incomplete data too
    bysort subdiv year age sex: egen pctdeathsraw = pc(deaths), prop
    bysort subdiv year age sex: egen totpctraw = total(pctdeathsraw)
    bysort subdiv year age sex: egen maxpctraw = max(pctdeathsraw)
    ** drop if there are no deaths in the state-year-age-sex or if all the deaths are in one cause, as this is not good data for making weights
    drop if totpctraw==0 | maxpctraw==1
    drop totpctraw maxpctraw

    ** now merge with the template so everything gets a weight
    merge 1:1 subdiv year age sex cause using `template', nogen

    ** save data for modeling weights
    tempfile mdata
    save `mdata', replace


** CREATE AGE/SEX/YEAR AND REGION/AGE/SEX/YEAR WEIGHTS, APPLY REGION WHERE POSSIBLE

    ** make weights
    collapse (sum) deaths, by(age sex year cause) fast
    bysort age sex year: egen totdeaths = total(deaths)
    gen pctdeathspred = deaths/totdeaths
    count if totdeaths==0 & year!=1980
    assert `r(N)'==0
    keep age sex year pctdeathspred cause
    tempfile asy
    save `asy'

    ** UGH have to make special weights for punjab 1980... it is the only one with sex 9 and has no E codes
    if "`source'"=="India_MCCD_states_ICD9" {
        use `mdata', clear
        keep if subdiv=="Punjab: Urban"
        collapse (sum) deaths, by(subdiv cause) fast
        ** make sure it doesnt match with anything but what we intend - missing age/sex for Punjab 1980
        gen age = 26
        gen sex = 9
        gen year = 1980
        bysort age sex year subdiv: egen totdeaths = total(deaths)
        gen pctdeathspred = deaths/totdeaths
        tempfile punjab
        save `punjab', replace
    }

    use `mdata', clear
    collapse (sum) deaths, by(age sex year srs_region cause) fast
    bysort age sex year srs_region: egen totdeaths = total(deaths)
    gen pctdeathspred = deaths/totdeaths
    drop if totdeaths==0
    keep age sex year srs_region pctdeathspred cause
    tempfile asyr 
    save `asyr', replace

    ** apply weights
    merge 1:m age sex year srs_region cause using `mdata'
    ** revert to age sex year weight if regional weight was missing
    merge m:1 age sex year cause using `asy', update gen(_merge2)
    if "`source'"=="India_MCCD_states_ICD9" {
        ** revert to PUNJAB SPECIAL ATTENTION BECAUSE .. :/
        merge m:1 age sex year subdiv cause using `punjab', update gen(_mergePJ)
        ** make sure everything got a weight
        count if _merge==2 & _merge2 != 4 & _mergePJ != 4
    }
    else {
        ** make sure everything got a weight
        count if _merge==2 & _merge2 != 4        
    }
    assert `r(N)'==0


** FORMAT AND EXPORT RESULTS FOR VISUALIZING

    rename deaths deathsraw
    capture drop totdeaths
    bysort subdiv age sex year: egen totdeaths = total(deathsraw)
    
    ** use this proportion to calculate the predicted deaths
    gen deathspred = totdeaths*pctdeathspred
    su deathspred
    local predsum = `r(sum)'
    su deathsraw
    assert abs(`r(sum)'-`predsum')<1
    keep year subdiv age sex cause cause_name srs_region deathspred deathsraw pctdeathspred pctdeathsraw
    ** divide the dataset long by raw and predicted values for comparison
    reshape long pctdeaths deaths, i(year subdiv age sex cause) j(type) string
    tempfile weights 
    save `weights', replace
    ** export results
    export delimited using "FILEPATH", replace

    use `rawdata', clear
    keep if regexm(cause, "^(`n_chapter')")
    ds deaths cause cause_name , not
    collapse (sum) deaths, by(`r(varlist)') fast
    drop if deaths==0
    rename deaths ncodedeaths
    tempfile splitdata
    save `splitdata', replace

    use `weights', clear
    keep if type=="pred"
    drop deaths
    joinby subdiv sex year age using `splitdata', unmatched(using)
    count if _merge != 3
    assert `r(N)'==0
    drop _merge
    gen deaths = pctdeaths*ncodedeaths
    bysort subdiv sex year age: egen newtotal = total(deaths)
    count if abs(newtotal-ncodedeaths) >1
    assert `r(N)'==0
    drop newtotal ncodedeaths type pctdeaths srs_region
    tempfile split 
    save `split', replace

    use `rawdata', clear
    drop if regexm(cause, "^`n_chapter'")
    append using `split'
    ds deaths, not
    ** now that the formerly bad data was split, there are duplicate observations and we can collapse
    collapse (sum) deaths, by(`r(varlist)') fast
    ** reshape to wide format
    reshape wide deaths, i(subdiv cause cause_name sex year frmat im_frmat) j(age)

    ** make sure everything is in there
    do "format_gbd_age_groups.do"

