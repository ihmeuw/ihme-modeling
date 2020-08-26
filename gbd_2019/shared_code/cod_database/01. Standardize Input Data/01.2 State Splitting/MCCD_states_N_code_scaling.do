** PURPOSE
** Scale N codes down to the proportion of N codes remaining after E codes are subtracted
** Have to use scaling instead of pure subtraction because it isn't consistent
** when N codes are duplicates of E codes and when they aren't

** Set source-specific locals
    ** count on the source being constant, because it is
    local source = source in 1
    if "`source'" == "India_MCCD_states_ICD10" | "`source'"=="_India_10_tabulated_MCCD" local n_chapter = 19
    if "`source'" == "India_MCCD_states_ICD9" | "`source'"=="_India_9_tabulated_MCCD" local n_chapter = 17
    local e_chapter = `n_chapter'+1

** Save data 
    tempfile raw
    save `raw', replace

** Pivot the data in to the number of N code (19) and E code
    gen group = substr(cause, 1, 2)
    ** for clarity - set group to letter
    keep if real(group)==`n_chapter' | real(group)==`e_chapter'
    replace group = "N" if group=="`n_chapter'"
    replace group = "E" if group=="`e_chapter'"
    collapse (sum) deaths1, by(iso3 subdiv location_id sex year group)
    rename deaths1 deaths
    reshape wide deaths, i(iso3 subdiv location_id sex year) j(group) string

** Generate the n_weight as the proportion of N code deaths that have no E code deaths
    gen diff = deathsN-deathsE
    gen n_weight = diff/deathsN
    replace n_weight = 1 if n_weight ==.
    replace n_weight= 0 if n_weight <0
    tempfile n_weights
    save `n_weights', replace

** Merge back onto raw and Scale
    use `raw', clear
    merge m:1 iso3 subdiv location_id sex year using `n_weights', assert(3) keep(3) nogen keepusing(n_weight)
    ** only want to scale N code deaths
    replace n_weight=1 if !regexm(cause, "^`n_chapter'")
    ** scale it
    foreach var of varlist deaths* {
        ** we can multiply all observations by the n_weight because we sent them to 1 if 
        ** not a N code cause
        replace `var' = `var'*n_weight 
    }
    ** don't really want to create any new variables for the original source
    drop n_weight

