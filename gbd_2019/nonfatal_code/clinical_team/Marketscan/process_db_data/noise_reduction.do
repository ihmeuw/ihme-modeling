** ****************************************************

** ****************************************************

clear all
set more off, perm

** Define J drive (data) for cluster (UNIX) and Windows (Windows)
    if c(os) == "Unix" {
        global j "FILENAME"
        set odbcmgr unixodbc
        set matsize 11000
                local cwd = c(pwd)
                cd "~" 
                global h = c(pwd)
                cd `cwd'
    }
    else if c(os) == "Windows" {
        global j "FILEPATH"
    }

// Arguments
    if "`1'" != "" {
        global data_name "`1'"
        global timestamp "`2'"
        global username "`3'"
        local cause "`4'" 
        local run_id "`5'"
    }

// Manual
    else if "`1'" == "" {
        global data_name "West_Bank_2004_2009"
        global timestamp  "TEST"
        global username "rmbarber"
        local cause "sids"
    }

local cause_log_dir "FILEPATH"
capture mkdir `cause_log_dir'
if "`resubmit'" != "yes" {
    log using "`cause_log_dir'/log_`cause'.smcl", replace
}    
else if "`resubmit'" == "yes" log using "`cause_log_dir'/log_`cause'_resubmit.smcl", replace

global temp_dir "FILEPATH"

** We predict 4 times, so lets just define it once
    cap program drop predict_nr
    program define predict_nr
        version 13
        syntax , sex_id(string)
        ** make CF and standard error predictions
        predict double cf_tmp_`sex_id', xb nooffset
        predict double std_err_`sex_id', stdp
        gen double predicted_cf_`sex_id' = exp(cf_tmp_`sex_id')
        ** manually normalize the standard error using Brad's estimation
        gen double predicted_std_err_`sex_id' = exp(std_err_`sex_id' - 1) * predicted_cf_`sex_id'
        gen double predicted_var_`sex_id' = (predicted_std_err_`sex_id')^2
        ** set predicted variance to ridiculously high value when it is null (this will just get set to the floor anyway, all we need is a non-null value)
        replace predicted_var_`sex_id' = predicted_cf_`sex_id'*10e+90 if predicted_var_`sex_id' == .
        drop cf_tmp* std_err* predicted_std_err*
    end

** For subnational VR, don't have sample sizes under 50 influence prediction (build in exception for maternal data)
    local ss_threshold 50
    if "$data_name" == "Other_Maternal" local ss_threshold 0
    // ((location_id == 0 | !(substr(list,1,3)=="ICD" | list == "China_1991_2002")) | sample_size >= `ss_threshold')
    
    ** Read in data
        capture use "FILEPATH" if acause == "`cause'", clear
        if _rc {
            sleep 10000
            use "FILEPATH" if acause == "`cause'", clear
        }
        
    ** Convert to deaths
        capture drop deaths_final
        foreach var in final {
            gen double deaths_`var' = cf_`var' * sample_size
            replace deaths_`var' = 0 if deaths_`var' == .
        }
        
    ** Don't want 0 sample size
        drop if sample_size == 0
        
    ** If this is Other_Maternal, we only run smoothing on DHS
        if "$data_name" == "Other_Maternal" {
            preserve
            keep if regexm(source_label, "DHS") != 1 & regexm(source_label, "DLHS")!=1 & regexm(source_label, "RHS")!=1
            tempfile other_maternal
            save `other_maternal', replace
            restore
            keep if regexm(source_label, "DHS") | regexm(source_label, "DLHS")| regexm(source_label, "RHS")
        }
        
    ** Merge on restrictions (parse out modelable_entity_id for Marketscan)
        if strmatch("$data_name", "_Marketscan*") {
            split acause, parse("
            drop acause
            rename acause1 acause
            rename acause2 modelable_entity_id
            destring modelable_entity_id, replace
        }
        
    **  re-enforce age restrictions
        recast double age 
        if !strmatch("$data_name", "_Marketscan*") {
            foreach age_var in age {
                replace `age_var' = 0 if `age_var' == 91
                replace `age_var' = .01 if (`age_var' >.009 & `age_var' < .011) | `age_var' == 93
                replace `age_var' = .1 if (`age_var' >.09 & `age_var' < .11) | `age_var' == 94
            }
        }

    ** put ages back
        if !strmatch("$data_name", "_Marketscan*") {
            replace age = 91 if age == 0
            replace age = 93 if age >.009 & age < .011
            replace age = 94 if age >.09 & age < .11
        }
        
    ** Reset under 1 for Marketscan so it is not reference (try 50 if it's here)
        if strmatch("$data_name", "_Marketscan*") {
            replace age = 9999 if age == 0
            replace age = 0 if age == 50
        }

    ** Set location ID to 0 for country
        replace location_id = 0 if location_id == .
        
    ** Identify population
        if !strmatch("FILEPATH", keep(1 3) nogen
        else if strmatch("$data_name", "_Marketscan*") gen pop = 1000001
        
    ** Run smoothing for each country
    count if source == "$data_name" | source == "subnat_agg"
    if `r(N)' > 0 {
    levelsof iso3 if source == "$data_name" | source == "subnat_agg", local(iso3s)
    preserve
    foreach iso3 of local iso3s {
        di "---`iso3'---"
        capture gen double predicted_cf = .
        capture gen double predicted_var = .
        ** ** ** ** ** ** ** ** ** ** ** **
        ** Only region can be relevant for any country
        sum pop if iso3 == "`iso3'"
        local isopop = `r(max)'
        levelsof region if iso3 == "`iso3'", local(reg) c
        keep if region == `reg' & (iso3 == "`iso3'" | !index(iso3,"_national"))
        ** Keep only country if location has 1 million plus population
        if `isopop' >= 1000000  {
            capture confirm file `region_`reg''
            if _rc {
                gen region_data = 1
                tempfile region_`reg'
                save `region_`reg'', replace
            }
            capture drop region_data
            keep if iso3 == "`iso3'"
            local loc_effect ""
        }
        ** ... otherwise keep whole region (not subnationals) -- set the country in question as base
        else if `isopop' < 1000000 {
            egen isonum = group(iso3)
            levelsof isonum if iso3 == "`iso3'", local(baseiso) c
            local loc_effect "ib`baseiso'.isonum"
        }
        ** Add location_id if multiple subnational entities within iso3
        sum location_id
        if `r(min)' != `r(max)' local loc_effect "`loc_effect' i.location_id"
        ** ** ** ** ** ** ** ** ** ** ** **
        foreach sex in 1 2 {
            ** Clear triggers
            local poisson_1_converge
            local nbreg_1_converge
            local poisson_2_converge
            local nbreg_2_converge
            ** Make sure we have enough observations
            count if deaths_final > 0 & sex == `sex' & (location_id == 0 | sample_size >= `ss_threshold')
            local nonzero = `r(N)'
            count if sex == `sex' & (location_id == 0 | sample_size >= `ss_threshold')
            local df = `r(N)'
            if `df' > 6  & `nonzero' > 0 {
            noisily display in red "SMOOTHING SEX `sex' ISO-3 `iso3'"
                ** Run Poisson
                poisson deaths_final i.age i.year `loc_effect' if sex == `sex' & (location_id == 0 | sample_size >= `ss_threshold'), exposure(sample_size) iterate(100)
                local poisson_1_converge = e(converged)
                if `poisson_1_converge'==1 & strmatch("$data_name", "_Marketscan*") predict_nr, sex_id(`sex')
                else if !strmatch("$data_name", "_Marketscan*") {
                    ** Predict if converged
                    if `poisson_1_converge'==1 {
                        mat ini = e(b)
                        noisily display in red "The Poisson converged, attempting negative binomial..."
                        ** Predict deaths from the first Poisson
                        predict_nr, sex_id(`sex')
                        ** Then attempt primed negative binomial
                        capture noisily nbreg deaths_final i.age i.year `loc_effect' if sex == `sex' & (location_id == 0 | sample_size >= `ss_threshold'), from(ini) exposure(sample_size) iterate(100)
                        local nbreg_1_converge = e(converged)
                        if "`nbreg_1_converge"=="1" {
                            noisily display in red "... double success! The primed negative binomial converged."
                            drop *_`sex'
                            ** Predict deaths from the negative binomial
                            predict_nr, sex_id(`sex')
                        }
                    }
                    ** If convergence is not achieved, try adding in data from the rest of the region (or running current region with different base country)
                    if `poisson_1_converge'==0 {
                        noisily display in red "The Poisson did't converge... "
                        if !index("`loc_effect'","isonum") {
                            count if iso3 == "`iso3'"
                            local regress_iso `r(N)'
                            noisily display in red "... adding in region data..."
                            append using `region_`reg''
                            drop if region_data == 1 & (iso3 == "`iso3'" | sex != `sex')
                            count if iso3 == "`iso3'"
                            assert `r(N)' == `regress_iso'
                        }
                        else noisily display in red "... changing base country..."
                        ** Find country with the most non-zeroes, use as base
                        capture drop isonum
                        capture drop isononzero
                        capture drop nonzeroobs
                        egen isonum = group(iso3)
                        gen isononzero = 1 if deaths_final > 0
                        egen nonzeroobs = total(isononzero), by(iso3)
                        sum nonzeroobs
                        assert `r(max)' > 0 & `r(max)' != .
                        levelsof isonum if nonzeroobs == `r(max)', local(freqisos) c
                        local freqiso ""
                        foreach fi of local freqisos {
                            if "`freqiso'" == "" local freqiso `fi'
                        }
                        local loc_effect_nc "ib`freqiso'.isonum"
                        sum location_id
                        if `r(min)' != `r(max)' local loc_effect_nc "`loc_effect_nc' i.location_id"
                        poisson deaths_final i.age i.year `loc_effect' if sex == `sex' & (location_id == 0 | sample_size >= `ss_threshold'), exposure(sample_size) iterate(100)
                        local poisson_2_converge = e(converged)
                        ** Predict if converged
                        if `poisson_2_converge'==1 {
                            mat ini = e(b)
                            noisily display in red "The region Poisson converged, attempting negative binomial..."
                            ** Predict deaths from the second poisson
                            capture drop *_`sex'
                            predict_nr, sex_id(`sex')
                            ** Then attempt negative binomial
                            capture noisily nbreg deaths_final i.age i.year `loc_effect' if sex == `sex' & (location_id == 0 | sample_size >= `ss_threshold'), from(ini) exposure(sample_size) iterate(100)
                            local nbreg_2_converge = e(converged)
                            if "`nbreg_2_converge'"=="1" {
                                noisily display in red "... double success! The primed region negative binomial converged."
                                drop *_`sex'
                                ** Predict deaths from the second negative binomial
                                predict_nr, sex_id(`sex')
                            }
                        }
                        ** Make sure we've disposed of any region data we loaded
                        capture drop if region_data == 1
                        capture drop region_data
                    }
                }
            }
            ** Break if Marketscan does not converge
            if (`df' < 6  | `nonzero' == 0 | "`poisson_1_converge'"=="0") & strmatch("$data_name", "_Marketscan*") {
                noisily display in red "Did not converge."
                gen predicted_cf_`sex' = .
                gen predicted_var_`sex' = .
            }
            ** Fill in average if not
            if (`df' < 6  | `nonzero' == 0 | "`poisson_2_converge'"=="0") & !strmatch("$data_name", "_Marketscan*") {
                noisily display in red "Poisson with region data didn't converge, filling in prediction with average across all location-years"
                ** Get average across years
                keep if iso3 == "`iso3'"
                egen double predicted_cf_`sex' = mean(cf_final), by(age sex)
                replace predicted_cf_`sex' = . if sex != `sex'
                gen double predicted_var_`sex' = (1/sample_size) * predicted_cf_`sex' * (1-predicted_cf_`sex')
            }
        }

        ** bring together predictions for each sex
        capture confirm var predicted_cf_1
        if _rc == 0 replace predicted_cf = predicted_cf_1 if sex == 1
        capture confirm var predicted_cf_2
        if _rc == 0 replace predicted_cf = predicted_cf_2 if sex == 2
        capture confirm var predicted_var_1
        if _rc == 0 replace predicted_var = predicted_var_1 if sex == 1
        capture confirm var predicted_var_2
        if _rc == 0 replace predicted_var = predicted_var_2 if sex == 2
        capture drop predicted_var_* predicted_cf_*

        ** Now save the output for each country (region make sure to just keep the country of interest, whole region may be present)
        keep if iso3 == "`iso3'"
        capture drop isonum isononzero nonzeroobs
        tempfile NRA_`iso3'
        save `NRA_`iso3'', replace
        discard
        restore, preserve
    }
    ** restore

    ** append together all the datasets
        clear
        foreach iso3 of local iso3s {
            append using `NRA_`iso3''
        }
    }
    else if `r(N)' == 0 {
        keep if source == "$data_name"
        gen predicted_cf = .
        gen predicted_var = .
    }
        
    ** bring back in other_maternal datasets that aren't DHS
        if "$data_name" == "Other_Maternal" {
            append using `other_maternal'
        }

    ** now make the metrics we want for the real data (have to break up to retain precision)
        gen double std_err_data = sqrt(1/sample_size*cf_final*(1-cf_final)+1/(4*sample_size^2)*1.96^2)
        gen double variance_data = std_err_data^2
        **
        gen double mean_cf_data_component = cf_final * (predicted_var / (predicted_var + variance_data))
        gen double mean_cf_prediction_component = predicted_cf * (variance_data / (predicted_var + variance_data))
        gen double mean = mean_cf_data_component + mean_cf_prediction_component
        **
        gen double variance_numerator = (predicted_var * variance_data) 
        gen double variance_denominator = (predicted_var + variance_data)
        gen double variance = variance_numerator / variance_denominator
        
    ** fix Marketscan ages
        if strmatch("$data_name", "_Marketscan*") {
            replace age = 50 if age == 0
            replace age = 0 if age == 9999
        }
        
    ** replace the data
        rename cf_final cf_before_smoothing
        rename mean cf_final
        ** For Other Maternal, SUSENAS, and zero CFs replace with the model prediction
        replace cf_final = predicted_cf if (source == "Other_Maternal" & (regexm(source_label, "DHS") | regexm(source_label, "DLHS")| regexm(source_label, "RHS"))) | inlist(source,"Mexico_BIRMM", "Maternal_report","SUSENAS") | sample_size == 0
        ** Use original data if no prediction could be made
        replace cf_final = cf_before_smoothing if predicted_cf==. | predicted_var==.
        
    ** replace deaths variable
        replace deaths_final = cf_final * sample_size
        recast double deaths_final

    ** Set location ID to 0 for country
        replace location_id = . if location_id == 0
        
    ** throw modelable_entity_id back on acause for Marketscan
    if strmatch("$data_name", "_Marketscan*") {
        replace acause = acause + "
        drop modelable_entity_id
    }
        
    ** Save
    ** keep if source == "$data_name" // NOW DO THIS AFTER RAKING
    save "FILEPATH", replace
    capture saveold "FILEPATH", replace

    capture log close
