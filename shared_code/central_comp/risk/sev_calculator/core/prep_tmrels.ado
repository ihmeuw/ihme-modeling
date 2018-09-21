cap program drop prep_tmrels
program define prep_tmrels, rclass
    syntax, rr_file(string) exp_file(string) location_ids(numlist) year_ids(numlist) age_group_ids(numlist)

    // most risks need tmrels generated on the fly, except for nutrition_iron, pufa, and bmd
    // for those, grab draws
    local risks_w_tmrel_draws = "95, 109, 122"
    return local risks_w_tmrel_draws "`risks_w_tmrel_draws'"

    adopath + "$functions_dir"
    run "FILEPATH/risk_info.ado"

    // first get risk from risk_id, for use with risk_variables
    risk_info, risk_id($rei_id) clear 
    levelsof risk, local(rei) c

    // pull vars
    import excel using "FILEPATH/risk_variables.xlsx", firstrow clear
    keep if risk=="`rei'"

    if !inlist($rei_id, `risks_w_tmrel_draws') {

        // gen tmrel_mid for risks without tmrel draws
        // also get rr scalar, inv_exp, and min_val
        gen tmrel_mid = (((tmred_para2 - tmred_para1)/2) + tmred_para1)/rr_scalar
        gen rei_id = $rei_id
        rename minval min_val
        keep rei_id tmrel_mid rr_scalar inv_exp min_val
        merge 1:m rei_id using `rr_file', keep(3) nogen

    }
    else {

        levelsof minval, local(minval) c
        levelsof inv_exp, local(inv_exp) c
        levelsof rr_scalar, local(rr_scalar) c
        //custom code for diet_pufa from PAF calculator
        if $rei_id == 122 {
            get_draws, gbd_id_field(modelable_entity_id) gbd_id(2436) location_ids(`location_ids') ///
                sex_ids(1 2) age_group_ids(`age_group_ids') status(best) source(epi) clear
            tempfile PUFA
            save `PUFA', replace
            get_draws, gbd_id_field(modelable_entity_id) gbd_id(2439) location_ids(`location_ids')  ///
                sex_ids(1 2) age_group_ids(`age_group_ids') status(best) source(epi) clear
            forvalues i = 0/999 {
                gen shift_`i' = draw_`i' - .07
                drop draw_`i'
            }
            merge 1:1 age_group_id location_id year_id sex_id measure_id using `PUFA', keep(3) nogen
            forvalues i = 0/999 {
                qui replace draw_`i' = .12 - shift_`i' if shift_`i'>=0 & shift_`i'!=.
                rename draw_`i' tmred_mean_`i'
            }
            drop shift*
            gen rei_id = $rei_id
        }
        //custom for iron
        if $rei_id == 95 {
            clear
            tempfile tmred
            save `tmred', replace emptyok
            local file_list : dir "FILEPATH" files "tmrel_*.csv"
            foreach file of local file_list {
                insheet using "FILEPATH/`file'", clear
                qui append using `tmred'
                save `tmred', replace          
            }
            rename tmrel_* tmred_*
            keep if parameter=="mean"
            gen rei_id = $rei_id
        }
        //custom for bmd
        else if $rei_id == 109 {
            insheet using "FILEPATH/tmred.csv", comma double clear
            drop if parameter=="sd"
            drop parameter risk year_id
            gen rei_id = $rei_id
        }

        ** collapse TMRED acorss year and location
        fastcollapse tmred*, type(mean) by(rei_id sex_id age_group_id)
        fastrowmean tmred*, mean_var_name(tmrel_mid)
        drop tmred*
        merge 1:m rei_id sex_id age_group_id using `rr_file', keep(3) nogen

        ** for iron, bmd, and pufa
        if inlist($rei_id,95,122,109) {
            gen inv_exp = `inv_exp'
            gen rr_scalar = `rr_scalar'
            gen min_val = `minval'
        }

    }

    // now add on exposure percentiles
    joinby rei_id using `exp_file', unmatch(master)
    local f = "$tmp_dir/tm_$rei_id.dta"
    save `f', replace
    return local file_path `f'
end
// END
