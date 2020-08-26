** ****************************************************

** ******************************************************************************************************************
// Prep Stata
    clear all
    set more off, perm
// Establish directories
    if c(os) == "Unix" {
        set mem 10G
        set odbcmgr unixodbc
        global j "FILENAME"
        local cwd = c(pwd)
        cd "~"
        global h = c(pwd)
        cd `cwd'
    }
    else if c(os) == "Windows" {
        global j "FILEPATH"
        global h "FILEPATH"
    }

    global repo_dir "$h/cod-data"

    local hosp_username = c(username)

// Set up connection string for dB
    forvalues dv=4(.1)6 {
        local dvfmtd = trim("`: di %9.1f `dv''")
        cap odbc query, conn("DRIVER={MySQL ODBC `dvfmtd' Unicode Driver};SERVER=SERVER;UID=USER;PWD=PWD")
        if _rc==0 {
            local driver "MySQL ODBC `dvfmtd' Unicode Driver"
            local db_conn_str "DRIVER={`driver'};SERVER; UID=USER; PWD=PWD"
            continue, break
        }
    }

// Source
    global source "`1'"

// Date
    global timestamp "`2'"

// Username
    global username "`3'"

// Inpatient or Inp Otp union
    local facilities "`4'"

    local run_id "`5'"


//  Local for the directory where all the COD data are stored
    // global in_dir "FILENAME"

// Local for the output directory
    // global out_dir "FILEPATH"

    // old global hosp_dir "FILENAME"
    // global hosp_dir "FILENAME"

// program directory
    global prog_dir "$repo_dir/02_programs"

    // hospital repo for MS noise reduction
    global hosp_repo "FILEPATH"

// whether to test one cause
    local test_single_cause 0
    assert `test_single_cause' != 1 if "$timestamp" != "TEST"
    local test_cause "maternal"

// location set and gbd round
    ** at time of writing: for steps 1-5 we use location set 8, afterwards 35
    ** only distinction is the presence of US counties
    ** these locals affect which countries have subnational locations
    local location_set_id = 35
    local gbd_round_id = 4

// prepare fastcollapse
    do "FILEPATH"

// prepare function to get iso3s that are prepped subnationally
    //quietly run "FILEPATH"
// get subnat iso3s
    use "FILEPATH", replace
    levelsof iso3, local(subnat_iso3s) clean

// log output
    capture log close _all
    local log_dir "FILEPATH"
    capture mkdir `log_dir'
    log using "`log_dir'/11_noise_reduction_${source}_${timestamp}", replace

// Refresh temp directory

    capture mkdir "FILENAME"
    // NOTE: All files in this directory get deleted every time this script is ran
    !rm -rf "FILEPATH"
    capture mkdir "FILEPATH"

// read in data
    if `test_single_cause' == 1 {
        use "FILEPATH" if acause == "`test_cause'", clear
    }
    else {
        use "FILEPATH", clear
    }

// **********************************************************************************************************************************************
// STEP 13a: NOISE REDUCTION FOR VR, CANCER REGISTRY, AND DHS
// BECAUSE OF LOW SAMPLE SIZE OR LOW DEATH COUNTS WE HAVE A GREAT LEVEL OF STOCHASTIC VOLATILITY FOR SOME CAUSE-DEMOGRAPHICS. THIS CAUSES HUGE FLEXUATIONS IN THE TREND WHICH LEADS TO

    // For the China data-series we need to prep them together (plus Hong Kong and Macau)
        if inlist("$source", "China_1991_2002", "China_2004_2012") {
            if "$source" == "China_1991_2002" local check_files "FILEPATH"
            if "$source" == "China_2004_2012" local check_files "FILEPATH"
            local check_files "FILEPATH"
            foreach check_file of local check_files {
                capture confirm file "`check_file'"
                local round = 1
                while _rc == 601 {
                    local time = `round' * 5
                    display "`check_file' is missing. It's been `time' minutes. Waiting 5 more..." _newline
                    local round = `round' + 1
                    sleep 300000
                    capture confirm file "`check_file'"
                }
                if _rc == 0 {
                    display "`check_file' found!" _newline
                    append using "`check_file'"
                }
            }
            keep if iso3 == "CHN"
        }

    // For Indonesia, South Africa, Kenya  and Brazil DHS, create 0s for missing years in every subnational unit before smoothing (in square data, does not add rows with no sample size)
        if "$source" == "Other_Maternal" {
            gen flag_age_year = .

            foreach iso of local subnat_iso3s {
                count if regexm(source_label, "DHS") & iso3 == "`iso'"
                if `r(N)' == 0 {
                    continue
                }
                ** Get years and ages
                foreach measure of varlist year age {
                    sum `measure' if iso3 == "`iso'" & location_id != . & regexm(source_label, "DHS")
                    local start_`measure' = `r(min)'
                    local n_`measure's = (`r(max)' - `r(min)') + 1
                }
                local n_ages = ((`n_ages' - 1) / 5) + 1
                preserve
                    keep if iso3 == "`iso'" & location_id != . & regexm(source_label, "DHS")
                    keep location_id NID subdiv
                    duplicates drop
                    tempfile nid_subdiv
                    save `nid_subdiv', replace
                    odbc load, exec(QUERY)
                    replace iso3 = substr(iso3,1,3)
                    expand `n_years'
                    bysort location_id iso3: gen year = _n - 1
                    replace year = year + `start_year'
                    expand `n_ages'
                    bysort location_id iso3 year: gen age = (_n - 1) * 5
                    replace age = age + `start_age'
                    expand 3
                    bysort location_id iso3 year age: gen cause_exp = _n
                    gen acause = "_comm" if cause_exp == 1
                    replace acause = "maternal" if cause_exp == 2
                    replace acause = "maternal_hiv" if cause_exp == 3
                    drop cause_exp
                    gen source = "Other_Maternal"
                    gen list = "Other_Maternal"
                    gen source_type = "Sibling history, survey"
                    gen national = 1
                    gen sex = 2
                    merge m:1 location_id using `nid_subdiv', assert(3) nogen
                    tempfile dhs_demogs
                    save `dhs_demogs', replace
                restore
                capture drop _m
                merge m:1 iso3 location_id NID subdiv region year age sex acause source source_type list national using `dhs_demogs'
                replace flag_age_year = 1 if _m == 2
                replace source_label = "DHS sibling history:  " + iso3 + " " + string(year) if _m == 2
                egen coverage = max(_merge) if iso3 == "`iso'" & location_id != . & regexm(source_label, "DHS"), by(iso3 year age acause sex)
                drop if coverage == 2
                drop coverage
                replace sample_size = .5 if _m == 2
                replace cf_final = 0 if _m == 2
            }
            foreach cf_var in cf_raw cf_corr cf_rd {
                replace `cf_var' = 0 if `cf_var' == .
            }
        }
        capture drop _merge

    // Get region(s) before reading in other datasets - put Greenland + Alaska together and remove larger nations from Caribbean (to be reset later)
        levelsof region if inlist(iso3, "PRI", "HTI", "DOM", "CUB") & ((regexm(source_type, "VR") | regexm(lower(source_type), "vital")) | strmatch(source, "ICD*") | (source == "Other_Maternal" & "$source" != "Other_Maternal")), local(carib) c
        replace region = 9999 if inlist(iso3, "PRI", "HTI", "DOM", "CUB") & ((regexm(source_type, "VR") | regexm(lower(source_type), "vital")) | strmatch(source, "ICD*") | (source == "Other_Maternal" & "$source" != "Other_Maternal"))
        levelsof region if index("${source}","Greenland"), local(grl) c
        replace region = 10000 if index("${source}","Greenland")
        levelsof region, local(source_regs) sep(",")

    // Save population file
        count if (regexm(source_type, "VR") | regexm(lower(source_type), "vital")) & source == "$source"
        local VR = `r(N)'
        count if source == "$source"
        if (`VR'/`r(N)'==1) | inlist("$source", "Cancer_Registry", "Other_Maternal", "Mexico_BIRMM", "Maternal_report", "SUSENAS", "China_MMS_1996_2005", "China_MMS_2006_2012", "China_Child_1996_2012", "China_1991_2002") | strmatch("$source", "ICD*") {
            preserve
                keep iso3
                duplicates drop
                tempfile nr_locations
                save `nr_locations', replace
                do "FILEPATH"
                keep if location_id == . & year >= 1990
                collapse (sum) pop, by(year iso3) fast
                collapse (mean) pop, by(iso3) fast
                merge 1:1 iso3 using `nr_locations', assert(1 3) keep(3) nogen
                tempfile nats
                save `nats', replace
                gen is_subnat_iso3 = 0
                ** save population of national level for subnat iso3 too
                foreach subnat_iso3 of local subnat_iso3s {
                    replace is_subnat_iso3 = 1 if iso3 == "`subnat_iso3'"
                }
                keep if is_subnat_iso3 == 1
                drop is_subnat_iso3
                replace iso3 = iso3 + "_national"
                append using `nats'
                save "FILEPATH", replace
            restore
        }

    // For the VR data-series we need to prep them together (get populations of the countries in this source prior to doing so)
        count if (regexm(source_type, "VR") | regexm(lower(source_type), "vital"))
        if `r(N)'>0  & !inlist("$source", "China_1991_2002", "China_2004_2012") & "$source" != "Other_Maternal" {
        // Append on sources
        levelsof source, local(this_source) clean
        preserve
        odbc load, exec(QUERY)
        levelsof source, local(sources) clean

        restore
        foreach source of local sources {
            capture confirm file "FILEPATH"
            local round = 1
            while _rc == 601 {
                local time = `round' * 5
                display "`source' file missing. It's been `time' minutes. Waiting 5 more..." _newline
                local round = `round' + 1
                sleep 300000
                capture confirm file "FILEPATH"
            }
            if _rc == 0 {
                display "`source' file found!" _newline
                if `test_single_cause' == 1 {
                    preserve
                        use "FILEPATH" if acause == "`test_cause'", clear
                        tempfile append_data
                        save `append_data'
                    restore
                    append using `append_data'
                }
                else {
                    append using "FILEPATH"
                }
            }
        }
        }

    // Run noise reduction
        count if (regexm(source_type, "VR") | regexm(lower(source_type), "vital")) & source == "$source"
        local VR = `r(N)'
        count if source == "$source"
        if (`VR'/`r(N)'==1) | inlist("$source", "Cancer_Registry", "Other_Maternal", "Mexico_BIRMM", "Maternal_report", "SUSENAS", "China_MMS_1996_2005", "China_MMS_2006_2012", "China_Child_1996_2012", "China_1991_2002") | strmatch("$source", "_Marketscan*") | strmatch("$source", "ICD*") {
            // Keep regions we need (adding Alaska to above created Greenland region, making all big Caribbean nations loaded from other sources have changed)
            replace region = 9999 if inlist(iso3, "PRI", "HTI", "DOM", "CUB") & ((regexm(source_type, "VR") | regexm(lower(source_type), "vital")) | strmatch(source, "ICD*") | (source == "Other_Maternal" & "$source" != "Other_Maternal"))
            replace region = 10000 if location_id == 524 & index("${source}","Greenland")
            keep if inlist(region,`source_regs')
            // **********************************************************************************************************************************************
            // AGGREGATE SUBNATIONAL DATASETS TO NATIONAL AGGREGATES TO USE IN NOISE REDUCTION
                if inlist("$source", "England_UTLA_ICD10", "England_UTLA_ICD9", "Brazil_SIM_ICD9", "Brazil_SIM_ICD10", "Japan_by_prefecture_ICD10", "Japan_by_prefecture_ICD9", "Indonesia_SRS_2014") | inlist("$source", "US_NCHS_counties_ICD9", "US_NCHS_counties_ICD10", "South_Africa_by_province", "China_1991_2002", "China_2004_2012") | inlist("$source", "ICD9_detail", "ICD10", "Sweden_ICD9", "Sweden_ICD10", "ICD8_detail","Saudi_Arabia_96_2012") | inlist("$source", "India_MCCD_states_ICD10", "India_MCCD_states_ICD9") | strmatch("$source", "_Marketscan*") | "$source" == "Mexico_BIRMM" {
                    do "FILEPATH"
                }

                // Confirm changes did not create duplicates
                    duplicates tag NID iso3 location_id subdiv age sex year acause source_label, gen(dups)
                    count if dups > 0
                    if `r(N)' > 0 {
                        di in red "Merge has created duplicate observations"
                        save "FILEPATH", replace
                        BREAK
                    }
                    drop dups
            // **********************************************************************************************************************************************
            ** Get rid of _national iso3 if that iso3 is not in the source

            ** store a macro of the national iso3s that got added from append
            levelsof iso3 if regexm(iso3, "_national"), local(nat_iso3s)
            foreach nat_iso3 of local nat_iso3s {
                ** see if that iso3 e.g. ZAF_national is in source_iso3s
                local iso3 = subinstr("`nat_iso3'", "_national", "", .)
                count if source == "$source" & iso3 == "`iso3'"
                local in_source = `r(N)'
                ** if not...
                if `in_source' == 0 {
                    ** get rid of that thing dawg, that doesnt need to be here
                    ** (e.g. this is Zimbabwe_2007, the source iso3s are just "ZWE", and because
                    ** South Africa is in same region and ZAF_national is created for South_Africa_by_province,
                    ** ZAF_national is in the data right now. It doesn't need to be)
                    di in red "dropping `nat_iso3' cause its not in $source"
                    drop if iso3 == "`nat_iso3'"
                }
            }


            save "FILEPATH", replace
            levelsof acause if source == "$source", local(causes)

            // Submit jobs by cause
            foreach cause of local causes {
                // Keep only the cause we are interested in
                    di "Submitting NR for `cause'"
                    count if acause == "`cause'"
                    // only run causes with more than six observations (minimum DoF)
                    if `r(N)'>6 {
                        // Submit job
                            !qsub -P proj_hospital -e FILEPATH -o FILEPATH -l archive -l fthread=4 -l m_mem_free=4G -l h_rt=2:00:00 -q all.q -N "CoD_11_FILEPATH"
                             sleep 1000
                    }
                    else if `r(N)' <= 6 {
                        preserve
                            keep if acause == "`cause'"
                            display in red "`cause' does not have enough observations!"
                            save "FILEPATH", replace
                            capture saveold "FILEPATH", replace
                        restore
                    }
            }

        // Loop through and append regression results back together
            clear
            // sleep to give all jobs time to finish
            sleep 390000
            foreach cause of local causes {
                // Check for file
                    capture confirm file "FILEPATH"
                    // re-send the nr job 3 times if it fails
                    if _rc != 0 {
                        foreach a of numlist 1/3 {
                            if _rc != 0 {
                                display "`cause' not found on try `a', resending for NR and checking again in a minute"
                                !qsub -P proj_hospital -e FILEPATH -o FILEPATH -l archive -l fthread=4 -l m_mem_free=4G -l h_rt=2:00:00 -q all.q -N "CoD_11_FILEPATH"
                                    sleep 60000
                                capture confirm file "FILEPATH"
                            }
                            if _rc == 0 {
                                exit
                            }
                        }
                    }
                    if _rc == 0 {
                        display "FOUND!"
                    }
                    display "Appending in `cause'"
                    capture append using "FILEPATH"
                    if _rc != 0 {
                        sleep 15000
                        append using "FILEPATH"
                    }
            }

            ** NO LONGER DROP NON-SOURCE OBSERVATIONS HERE, DO SO AFTER RAKING
            save "FILEPATH", replace
        }

    // Fix regions we tinkered with
        capture replace region = `carib' if inlist(iso3, "PRI", "HTI", "DOM", "CUB") & ((regexm(source_type, "VR") | regexm(lower(source_type), "vital")) | strmatch(source, "ICD*") | (source == "Other_Maternal" & "$source" != "Other_Maternal"))
        capture replace region = `grl' if index("${source}","Greenland")

// **********************************************************************************************************************************************

// STEP 14: RAKE SUBNATIONAL NOISE REDUCED DATA TO NATIONAL
    if inlist("$source", "England_UTLA_ICD10", "England_UTLA_ICD9", "Brazil_SIM_ICD9", "Brazil_SIM_ICD10", "Japan_by_prefecture_ICD10", "Japan_by_prefecture_ICD9") | inlist("$source", "US_NCHS_counties_ICD9", "US_NCHS_counties_ICD10", "South_Africa_by_province", "China_1991_2002", "China_2004_2012") | inlist("$source", "ICD9_detail", "ICD10", "Sweden_ICD9", "Sweden_ICD10", "ICD8_detail","Saudi_Arabia_96_2012") | inlist("$source", "India_MCCD_states_ICD10", "India_MCCD_states_ICD9") | strmatch("$source", "_Marketscan*") | "$source" == "Other_Maternal" | "$source" == "Mexico_BIRMM" {
        // keep only subnational countries and national aggregates (don't want DHS data)
        //drop if !(regexm(source_type, "VR") | regexm(lower(source_type), "vital")) & !inlist("${source}","China_1991_2002", "China_2004_2012", "Other_Maternal", "Mexico_BIRMM")
        preserve
            keep if index(iso3,"_national")
            keep age iso3 sex year acause deaths_final cf_final sample_size
            rename deaths_final nat_tot
            rename cf_final nat_cf
            rename sample_size nat_sample
            replace iso3 = subinstr(iso3,"_national","",.)
            tempfile smoothed_national
            save `smoothed_national', replace
        restore
        // get aggregated
        drop if index(iso3,"_national")
        egen subnat_tot = total(deaths_final) if location_id != ., by(age iso3 sex year acause)
        egen subnat_sample = total(sample_size) if location_id != ., by(age iso3 sex year acause)
        replace subnat_tot = 0.0001 if subnat_tot == 0
        merge m:1 age iso3 sex year acause using `smoothed_national', keep(1 3)
        replace _m = 1 if location_id == .
        if ("$source" == "Other_Maternal") replace sample_size = sample_size * (nat_sample / subnat_sample) if _m == 3
        replace deaths_final = deaths_final * (nat_tot / subnat_tot) if _m == 3
        replace cf_final = deaths_final / sample_size if _m == 3
        drop _m nat_*
    }
    keep if source == "$source"

// **********************************************************************************************************************************************
// STEP 13b: APPLY VERBAL AUTOPSY NOISE REDUCTION
    count if index(source_type,"VA") | index(lower(source_type),"verbal autopsy")
    if `r(N)' > 0 {
        // Load all VA data
            preserve
            odbc load, exec(QUERY)
            levelsof source, local(VA_sources)
            restore
            ** India SRS & Indonesia SRS are in a silo
            if "$source" == "India_SRS_states_report" {
                ** no sources to append
                local VA_sources
            }
            ** if Indonesia SRS only add those sources
            else if "$source" == "Indonesia_SRS_2014" {
                local VA_sources "Indonesia_SRS_province"
            }
            else if "$source" == "Indonesia_SRS_province" {
                local VA_sources "Indonesia_SRS_2014"
            }
            ** Append final files if they are not in sources that use only their own data
            foreach VA_source of local VA_sources {
                di "Searching for `VA_source' aggregation..."
                capture confirm file "FILEPATH"
                if _rc == 0 {
                    display "    ...APPENDING"
                }
                while _rc != 0 {
                display "    ...searching..."
                    sleep 30000
                    capture confirm file "FILEPATH"
                    if _rc == 0 {
                        display "    ...APPENDING"
                    }
                }
                append using "FILEPATH"
            }
        // Preserve CF prior to noise reduction
            capture gen double cf_before_smoothing = cf_final
            replace cf_before_smoothing = cf_final if cf_before_smoothing == .
        // Generate directories
            !rm -rf "FILEPATH"
            capture mkdir "FILENAME"
            capture mkdir "FILEPATH"
            capture mkdir "FILEPATH"
        // Drop non-VA , add back on after noise reduction
            preserve
                local no_nra = 0
                keep if (index(source_type,"VA") !=1 & index(source_type,"Verbal Autopsy") !=1) & source == "$source"
                count
                if `r(N)' > 0 {
                    local no_nra = 1
                    save "FILEPATH", replace
                    capture saveold "FILEPATH", replace
                }
            restore
            drop if (index(source_type,"VA") !=1 & index(source_type,"Verbal Autopsy") !=1)
        // Prepare metadata and covariates
            preserve
                ** Super region
                odbc load, exec(QUERY)
                replace location_id = . if type == "admin0"
                replace iso3 = substr(iso3,1,3)
                keep iso3 location_id super_region_id
                tempfile sup_regs
                save `sup_regs', replace
                ** Site-specific PfPR
                insheet using "FILEPATH", comma names clear
                rename site subdiv
                rename nid NID
                rename malaria_pfpr site_specific_pfpr
                drop itn
                drop if NID == .
                tempfile sspfpr
                save `sspfpr', replace
                ** National PfPR
                odbc load, exec(QUERY)
                replace location_id = . if type == "admin0"
                replace iso3 = substr(iso3,1,3)
                drop type
                tempfile natpfpr
                save `natpfpr', replace
                ** Malaria geographic groupings
                insheet using "FILEPATH", comma names clear
                keep iso3 group
                duplicates drop
                    // *********************************************************************
                    // Model South Africa, Cape Verde, and Mauritius outside of Africa, and Yemen along with Africa; Also model India by itself
                    replace group = "outside" if group != "africa" | inlist(iso3,"ZAF","CPV","MUS")
                    replace group = "africa" if iso3 == "YEM"
                    replace group = "india" if iso3 == "IND"
                    // *********************************************************************
                tempfile malaria_groups
                save `malaria_groups', replace
            restore
            merge m:1 iso3 location_id using `sup_regs', assert(2 3)
            keep if _merge == 3
            drop _merge
            merge m:1 iso3 year subdiv NID using `sspfpr', keep(1 3) nogen
            merge m:1 iso3 location_id year using `natpfpr', keep(1 3) nogen
            merge m:1 iso3 using `malaria_groups', keep(1 3) nogen
            ** Set thresholds
            gen pfpr_level = site_specific_pfpr if site_specific_pfpr != . & acause == "malaria"
            replace pfpr_level = national_pfpr if site_specific_pfpr == . & acause == "malaria"
            ** Identify PfPR groups
            gen pfpr_group = "SSA_hypoendem" if group == "africa" & pfpr_level < 0.05
            replace pfpr_group = "SSA_mesoendem" if group == "africa" & pfpr_level >= 0.05 & pfpr_level < 0.4
            replace pfpr_group = "SSA_hyperendem" if group == "africa" & pfpr_level >= 0.4 & pfpr_level != .
            replace pfpr_group = "IND_hypoendem" if group == "india" & pfpr_level < 0.05
            replace pfpr_group = "IND_mesoendem" if group == "india" & pfpr_level >= 0.05 & pfpr_level < 0.4
            replace pfpr_group = "IND_hyperendem" if group == "india" & pfpr_level >= 0.4 & pfpr_level != .
            replace pfpr_group = "G_hypoendem" if group == "outside" & pfpr_level < 0.05
            replace pfpr_group = "G_mesoendem" if group == "outside" & pfpr_level >= 0.05 & pfpr_level < 0.4
            replace pfpr_group = "G_hyperendem" if group == "outside" & pfpr_level >= 0.4 & pfpr_level != .
        // Set groupings for noise reduction
            gen nra_group = string(super_region_id) if pfpr_group == ""
            replace nra_group = pfpr_group if pfpr_group != ""
            ** Split off India
            replace nra_group = "IND" if iso3 == "IND" & acause != "malaria"
        // Save datasets by cause and super region for all causes (except malaria, which is grouped by PfPR)
            levelsof acause if source == "$source", local(acauses)
            foreach acause of local acauses  {
                levelsof nra_group if acause == "`acause'" & source == "$source", local(`acause'_groups)
                foreach group of local `acause'_groups {
                    preserve
                    keep if acause == "`acause'" & nra_group == "`group'"
                    ** Save file for regression
                    save "FILEPATH", replace
                    capture saveold "FILEPATH", replace
                    ** Submit noise reduction job

                    !qsub -P proj_hospital -l archive -l fthread=8 -l m_mem_free=40G -l h_rt=48:00:00 -q long.q -N "CoD_11va_FILEPATH"
                    restore
                }
            }

        // Compile results
            clear
            foreach acause of local acauses {
                foreach group of local `acause'_groups {
                    di "Searching for `acause' `group' results..."
                    capture confirm file "FILEPATH"
                    if _rc == 0 {
                        display "    ...COMPLETE"
                    }
                    while _rc != 0 {
                    display "    ...waiting..."
                        sleep 60000
                        capture confirm file "FILEPATH"
                        if _rc == 0 {
                            display "    ...COMPLETE"
                        }
                    }
                    append using "FILEPATH"
                }
            }

        // Return non-VA data to dataset
            if `no_nra' == 1 {
                append using "FILEPATH"
            }

    }
    capture gen double cf_before_smoothing = cf_final

    // Because we add some sources together, we only want to keep the parts that we need to together (aggregator changes source name for years and locations not in original data)
    keep if source == "$source"
    display "$source"
    count

    // save
        compress source source_label subdiv
        save "FILEPATH", replace
        save "FILEPATH", replace

        if "$source" == "_Marketscan_prevalence" {
            save "FILEPATH", replace
            //save "FILEPATH", replace
        }
        if "$source" == "_Marketscan_incidence" {
            save "FILEPATH", replace
           // save "FILEPATH", replace
        }
        // save "FILEPATH", replace
        // save "FILEPATH", replace
        capture log close
