    noisily di in red "Running in stata version:"
    version
    noisily di in red "Current state:"
    noisily di in red c(seed)

    clear
    set more off
    set matsize 1200

    local model_group "`1'"
    
    local launch_set_id "`2'"

    local temp_dir "`3'"

    local n_draws "`4'"

    local run_cause_id "`5'"
    
    noisily di "`temp_dir' - `model_group' - `launch_set_id' - `run_cause_id'"


    local se_asia_model_group "VA-4"
    local global_model_group "VA-G"
    local matlab_regex "Matlab"
    local ind_srs_regex "VA-SRS-IND"
    local ind_srs_malaria_regex "malaria_IND_SRS"
    local champs "CHAMPS"

    if regexm("`model_group'", "SSA") {
        local ssa 1
    }
    else {
        local ssa 0
    }

    if !(regexm("`model_group'","`matlab_regex'") | regexm("`model_group'","`ind_srs_regex'") | regexm("`model_group'","`ind_srs_malaria_regex'")) {
        local time_series 0
        noisily di "NON-TIME-SERIES VA"
    }
    else {
        local time_series 1
        noisily di "TIME-SERIES VA"
    }
    
    import delimited using "FILEPATH", clear
    
    program define wilson_std_err
        version 13
        args std_error cf sample_size
        gen double `std_error' = sqrt(1 / `sample_size' * `cf' * (1 - `cf') + 1 / (4 * `sample_size'^2) * 1.96^2) / (1 + 1.96^2 / `sample_size')
    end

    wilson_std_err std_error cf sample_size
    assert !missing(std_error)
    
    capture drop deaths
    gen deaths = cf * sample_size

    program define fill_with_average
        version 13
        bysort age_group_id: egen group_deaths = total(deaths)
        bysort age_group_id: egen group_sample_size = total(sample_size)
        gen cf_pred = group_deaths / group_sample_size
        bysort age_group_id: egen ss_pred = mean(sample_size)
        wilson_std_err se_pred cf_pred ss_pred
        drop group_deaths group_sample_size ss_pred
    end

    tempfile master
    save `master', replace

    levelsof sex_id, local(sexes)
    levelsof cause_id, local(causes)

    if "`model_group'" == "`champs'" {
        noisily di in red "This is CHAMPS, running models on all sexes..."
        local sexes 3
    }

    foreach sex of local sexes {
        foreach cause of local causes {
            use `master', clear
            
            if `sex' != 3 {
                keep if sex_id == `sex' & cause_id == `cause'
            } 
            else {
                keep if cause_id == `cause'
            }

            count
            local total = `r(N)'
            count if cf > 0
            local cf_count = `r(N)'

            noisily di in red "Running sex `sex' cause `cause'..."

            if `total' > 6  & `cf_count' > 0 {
                egen subreg = group(nid location_id site_id)

                if `time_series' == 0 {
                    if "`model_group'" != "`se_asia_model_group'" & "`model_group'" != "`global_model_group'" & "`model_group'" != "`champs'" {

                        if `ssa' == 0 {
                            noisily di in red "RANDOM EFFECT ON STUDY-LOCATION"
                            capture noisily mepoisson deaths i.age_group_id, exposure(sample_size) iterate(500) || _all: R.subreg //, trace gradient showtolerance
                            if _rc | e(converged) == 0 {
                                noisily di in red "FAILED TO CONVERGE"
                                noisily di in red "... RANDOM EFFECT FAILED, FIXED EFFECT INSTEAD"
                                noisily poisson deaths i.age_group_id i.subreg, exposure(sample_size) iterate(500)
                            }
                        }
                        else if `ssa' == 1 {
                            noisily di in red "FIXED EFFECT ON STUDY-LOCATION-YEAR"
                            egen subreg_yr = group(nid location_id site_id year_id)
                            noisily poisson deaths i.age_group_id i.subreg_yr, exposure(sample_size) iterate(500)
                        }
                    }
                    else if "`model_group'" == "`se_asia_model_group'" {
                        noisily di in red "SE ASIA, E ASIA, OCEANIA - FIXED EFFECTS ON STUDY-LOCATION"
                        noisily poisson deaths i.age_group_id i.subreg, exposure(sample_size) iterate(500)
                    }
                    else if "`model_group'" == "`global_model_group'" {
                        noisily di in red "GLOBAL VA MODEL - FIXED EFFECT ON STUDY"
                        noisily poisson deaths i.age_group_id i.subreg, exposure(sample_size) iterate(50)
                    }
                    else if "`model_group'" == "`champs'" {
                        noisily di in red "CHAMPS MODEL - FIXED EFFECT ON SEX AND RANDOM EFFECT ON STUDY-LOCATION"
                        capture noisily mepoisson deaths i.age_group_id i.sex_id, exposure(sample_size) iterate(500) || _all: R.subreg
                        if _rc | e(converged) == 0 {
                            noisily di in red "FAILED TO CONVERGE"
                            noisily di in red "... RANDOM EFFECT FAILED, FIXED EFFECT INSTEAD"
                            noisily poisson deaths i.age_group_id i.sex_id i.subreg, exposure(sample_size) iterate(500)
                        }
                    }
                }
                else if regexm("`model_group'", "`matlab_regex'") {
                    noisily di in red "TIME SERIES REGRESSION (AGE + YEAR DUMMIES)"
                    noisily poisson deaths i.age_group_id i.year_id, exposure(sample_size) iterate(500)
                }

                else if regexm("`model_group'","`scd_regex'") | regexm("`model_group'","`ind_srs_regex'") | regexm("`model_group'","`ind_srs_malaria_regex'") {
                    noisily di in red "TIME SERIES REGRESSION (AGE + YEAR DUMMIES)... ALSO FIXED EFFECT ON LOCATION" 
                    noisily poisson deaths i.age_group_id i.year_id i.location_id, exposure(sample_size) iterate(500)
                }
                if e(converged) == 0 {
                    noisily di in red "FAILED TO CONVERGE"
                    fill_with_average
                }
                else {
                    predict double cf_pred, xb nooffset
                    predict double se_pred, stdp
                    replace cf_pred = exp(cf_pred)
                    replace se_pred = exp(se_pred - 1) * cf_pred
                    replace se_pred = sqrt(cf_pred * 10e+91) if se_pred == .
                    replace se_pred = sqrt(cf_pred * 10e+91) if se_pred > sqrt(cf_pred * 10e+91)
                }
                gen fallback_flag = 0
            }
            else {
                if `total' > 0 {
                    noisily di in red "Not enough observations, filling with average..."
                    fill_with_average
                    gen fallback_flag = 1
                }
                else {
                    noisily di in red "No observations, cause/sex created by looping"
                }
            }
            noisily di in red "SAVING RESULTS FOR sex_`sex'_cause_`cause'"
            tempfile sex_`sex'_cause_`cause'
            save `sex_`sex'_cause_`cause'', replace
        }
    }
    
    clear
    foreach sex of local sexes {
        noisily di in red "`sex'"
        foreach cause of local causes {
            noisily di in red "`cause'"
            append using `sex_`sex'_cause_`cause''
        }
    }

    assert !missing(cf_pred)
    assert !missing(se_pred)
    assert !missing(fallback_flag)
    
    gen double cf_post = ((se_pred^2/(se_pred^2 + std_error^2))*cf) + ((std_error^2/(std_error^2 + se_pred^2))*cf_pred)
    gen double var_post = (se_pred^2*std_error^2)/(se_pred^2+std_error^2)


    replace cf = cf_post if cf_post != .

*     count
*     if `r(N)' > 0 & `graph' == 1 {
*         preserve
*             capture mkdir "FILEPATH"
*             capture mkdir "FILEPATH"
*             capture copy "FILEPATH"
*             capture erase "FILEPATH"
*             pdfstart using "FILEPATH"
*             label define sexlbl 1 "Males" 2 "Females"
*             label values sex sexlbl
*             replace age = 0 if age == 91
*             replace age = 0.01 if age == 93
*             replace age = 0.1 if age == 94
*             egen obs_group = group(NID subdiv iso3 year)
*             sort obs_group year sex age
*             levelsof obs_group, local(obs_groups)
*             foreach obs_group of local obs_groups {
*                 levelsof year if obs_group == `obs_group', local(year) c
*                 levelsof iso3 if obs_group == `obs_group', local(iso_use) c
*                 levelsof subdiv if obs_group == `obs_group', local(subdiv_use) c
*                 summ sample_size if obs_group == `obs_group'
*                 local ss = round(`r(sum)')
*                 scatter cf_before_smoothing age if obs_group == `obs_group', by(sex) mcolor(red) msymbol(O) msize(small) connect(l) lcolor(red) || scatter cf_pred age if obs_group == `obs_group', by(sex) mcolor(blue) msymbol(O) msize(small) connect(l) lcolor(blue)  || scatter cf_post age if obs_group == `obs_group', by(sex, title("`iso_use', `subdiv_use' (`year')""`acause' cause fractions [Sample size: `ss']", size(med))) mcolor(black) msymbol(O) msize(small) connect(l) lcolor(black) ytitle("Cause fraction", size(vsmall)) xtitle("Age", size(vsmall)) legend(size(vsmall) symxsize(*.5) symysize(*.5) label(1 "Input") label(2 "Prediction") label(3 "Posterior"))
*                 pdfappend
*             }
*             pdffinish
*             capture erase "FILEPATH"
*             keep NID iso3 location_id national region subdiv year sex age acause cf cf_before_smoothing sample_size cf_pred std_error se_pred var_post
*             save "FILEPATH", replace
*         restore
*     }


    export delimited using "FILEPATH", replace

    exit, clear STATA
** }

