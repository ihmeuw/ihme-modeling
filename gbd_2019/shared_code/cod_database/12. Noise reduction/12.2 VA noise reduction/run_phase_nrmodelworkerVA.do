** Prep
    clear
    set more off

** What super region/PfPR group?
    ** local categ "`5'"
    local model_group "`1'"
    
** Marker for versioning different runs of same dataset
    local launch_set_id "`2'"

** Where to read/write
    local temp_dir "`3'"
    
** quietly {
    noisily di "`temp_dir' - `model_group' - `launch_set_id'"
** Graph?
** local graph 1

    ** lots of special cases
    local se_asia_model_group "VA-4"
    local matlab_regex "Matlab"
    local ind_srs_regex "VA-SRS-IND"
    local ind_srs_malaria_regex "malaria_IND_SRS"
    local scd_regex "India_SCD"
    local idn_va_comp_regex "India_VA_comp"

    if regexm("`model_group'", "SSA") {
        local ssa 1
    }
    else {
        local ssa 0
    }

    if !(regexm("`model_group'","`matlab_regex'") | regexm("`model_group'","`scd_regex'") | regexm("`model_group'","`ind_srs_regex'") | regexm("`model_group'","`ind_srs_malaria_regex'")) {
        local time_series 0
        noisily di "NON-TIME-SERIES VA"
    }
    else {
        local time_series 1
        noisily di "TIME-SERIES VA"
    }

    local row_is_time_series regexm(source,"`matlab_regex'") | regexm(source,"`scd_regex'") | regexm(source,"`ind_srs_regex'")

** ** ** ** **
** Manual run
*local source "India_SRS_Maternal_states"
*local temp_dir "FILEPATH"
*local timestamp "MANUAL"
*local acause "maternal"
*local categ "IND"
** ** ** ** **
    
** Load source dataset
    import delimited using "FILEPATH", clear
    
** Standard error using Wilson method
    gen double std_error = sqrt(1 / sample_size * cf * (1 - cf) + 1 / (4 * sample_size^2) * 1.96^2)
    
** Convert to deaths
    capture drop deaths
    gen deaths = cf*sample_size

** Run regressions by sex (already split by cause and super region)
    tempfile master
    save `master', replace

    levelsof sex_id, local(sexes)
    levelsof cause_id, local(causes)

    foreach sex of local sexes {
        foreach cause of local causes {
            use `master', clear
            
            keep if sex_id == `sex' & cause_id == `cause'
            
            if `time_series' == 0 {
                count if !(`row_is_time_series')
                local total = `r(N)'
                count if cf > 0 & !(`row_is_time_series')
                local cf_count = `r(N)'
            }
            else {
                count
                local total = `r(N)'
                count if cf > 0
                local cf_count = `r(N)'
            }
            ** Continue if meets minumum required observations...
            if `total' > 6  & `cf_count' > 0 {
                noisily di in red "Running sex `sex' cause `cause'..."
                egen subreg = group(nid location_id site_id)

                ** Run random or fixed effects regression on non-time series VAs
                if `time_series' == 0 {
                    ** Attempt random effects if not SE Asia, E Asia, Oceania (will do so for all non-time-series malaria)
                    if "`model_group'" != "`se_asia_model_group'" {

                        drop if `row_is_time_series'
                        ** All causes except malaria, and malaria outside of sub-Saharan Africa
                        if `ssa' == 0 {
                            noisily di in red "RANDOM EFFECT ON STUDY"
                            capture noisily mepoisson deaths i.age_group_id, exposure(sample_size) iterate(50) || _all: R.subreg
                            if _rc | e(converged) == 0 {
                                noisily di in red "... RANDOM EFFECT FAILED, FIXED EFFECT INSTEAD"
                                noisily poisson deaths i.age_group_id i.subreg, exposure(sample_size) iterate(50)
                            }
                        }
                        ** Malaria-specific for Africa
                        else if `ssa' == 1 {
                            noisily di in red "FIXED EFFECT ON STUDY-YEAR"
                            egen subreg_yr = group(nid location_id site_id year_id)
                            noisily poisson deaths i.age_group_id i.subreg_yr, exposure(sample_size) iterate(50)
                        }
                    }
                    ** Or just used fixed for SE Asia to avoid overinfluence of the large VAs in that region
                    else if "`model_group'" == "`se_asia_model_group'" {
                        noisily di in red "SE ASIA, E ASIA, OCEANIA - FIXED EFFECTS ON STUDY"
                        noisily poisson deaths i.age_group_id i.subreg, exposure(sample_size) iterate(50)
                    }
                }
                ** Do seperate regression for time series
                else if regexm("`model_group'", "`matlab_regex'") {
                    noisily di in red "TIME SERIES REGRESSION (AGE + YEAR DUMMIES)"
                    noisily poisson deaths i.age_group_id i.year_id, exposure(sample_size) iterate(50)
                }

                else if regexm("`model_group'","`scd_regex'") | regexm("`model_group'","`ind_srs_regex'") | regexm("`model_group'","`ind_srs_malaria_regex'") {
                    noisily di in red "TIME SERIES REGRESSION (AGE + YEAR DUMMIES)... ALSO RANDOM EFFECT ON STATE)" 
                    noisily poisson deaths i.age_group_id i.year_id i.location_id, exposure(sample_size) iterate(50)
                }
                predict double cf_pred, xb nooffset
                predict double se_pred, stdp
                replace cf_pred = exp(cf_pred)
                replace se_pred = exp(se_pred-1)*cf_pred
            }
            ** ... Or create empty variables for those studies that do not have enough observations
            else {
                noisily di in red "Not enough `sex`sex'' observations"
                gen cf_pred = .
                gen se_pred = .
            }
            noisily di in red "SAVING RESULTS FOR sex_`sex'_cause_`cause'"
            tempfile sex_`sex'_cause_`cause'
            save `sex_`sex'_cause_`cause'', replace
        }
    }
    
** Compile regressions
    clear
    foreach sex of local sexes {
        noisily di in red "`sex'"
        foreach cause of local causes {
            noisily di in red "`cause'"
            append using `sex_`sex'_cause_`cause''
        }
    }
    
** Get posterior
    gen double cf_post = ((se_pred^2/(se_pred^2 + std_error^2))*cf) + ((std_error^2/(std_error^2 + se_pred^2))*cf_pred)
    gen double var_post = (se_pred^2*std_error^2)/(se_pred^2+std_error^2)
    
** Swap in values if not null
    replace cf = cf_post if cf_post != .

* ** Graph & save output for review
*     count
*     if `r(N)' > 0 & `graph' == 1 {
*         preserve
*             capture mkdir "FILEPATH"
*             capture mkdir "FILEPATH"
*             capture copy "FILEPATH" "FILEPATH"
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

** Save

    export delimited using "FILEPATH", replace

    exit, clear STATA
** }

** *********************************************************************************************
** *********************************************************************************************
