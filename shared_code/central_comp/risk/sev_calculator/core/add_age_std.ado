cap program drop add_age_std
program define add_age_std, rclass

    run "$functions_dir/create_connection_string.ado"
    create_connection_string, database(shared)
    local con = r(conn_string)
    keep if inlist(age_group_id,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235)
    
    preserve
    local query = "select age_group_id, age_group_weight_value as weight from shared.age_group_weight where gbd_round_id = 4" 
    odbc load, exec("`query'") `con' clear
    tempfile age_weights
    save `age_weights'
    restore
    
    merge m:1 age_group_id using `age_weights', assert(2 3) keep(3) nogen
    bysort rei_id year_id location_id sex_id: egen wt_scaled = pc(weight), prop // rescale to 1
    replace age_group_id = 27
    foreach var of varlist sev* {
        qui replace `var' = `var' * wt_scaled
    }

    fastcollapse sev* if age_group_id == 27, type(sum) by(sex_id age_group_id location_id rei_id year_id)
    keep if age_group_id == 27

    local f = "$tmp_dir/asr_results_$rei_id.dta"
    save `f', replace
    return local asr_file_path `f'
end
// END
