cap program drop calc_percentiles
program define calc_percentiles, rclass

    /*
    For categorical risks, rr_max is the relative risk found at the highest level of exposure.
    For continuous risks, we simulate individuals  from the exposure distribution and find the
        99th percentile of that -- ie, the worst possible BMI, SBP, etc.
    pull this from what's generated in PAF calculation.
    create max and min percentile cols and fill them in using locals from above
    */

    run "FILEPATH/risk_info.ado"
    risk_info, risk_id($rei_id) draw_type(exposure) clear
    levelsof risk, local(rei) c

    capture confirm file "FILEPATH/`rei'/exposure/exp_max_min.dta" 
    if _rc == 0 {
        di "reading exposure max/min from PAF calc"
        use "FILEPATH/`rei'/exposure/exp_max_min.dta", clear
        keep rei_id min_1_val_mean max_99_val_mean
        rename (rei_id min_1_val_mean max_99_val_mean) (rei_id min_1_exp max_99_exp)
    }
    else {
        clear
        set obs 1
        gen rei_id = $rei_id
        gen min_1_exp = .
        gen max_99_exp = .
    }

    local f = "$tmp_dir/exp_$rei_id.dta"
    save "`f'", replace
    return local file_path `f'
    
end
// END
