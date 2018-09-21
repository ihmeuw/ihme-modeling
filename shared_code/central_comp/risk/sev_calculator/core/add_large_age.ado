cap program drop add_large_age
program define add_large_age, rclass

    run "$functions_dir/fastcollapse.ado"
    run "$functions_dir/create_connection_string.ado"

    create_connection_string, database(cod)
    local cod = r(conn_string)

    merge m:1 location_id year_id age_group_id sex_id using "$tmp_dir/pops_$rei_id.dta", keep(1 3) nogen keepusing(pop_scaled)

    replace age_group_id = 1 if inlist(age_group_id,2,3,4,5)
    replace age_group_id = 23 if inlist(age_group_id,6,7)
    replace age_group_id = 24 if inlist(age_group_id,8,9,10,11,12,13,14)
    replace age_group_id = 25 if inlist(age_group_id,15,16,17,18)
    replace age_group_id = 26 if inlist(age_group_id,19,20,30,31,32,235)
    foreach var of varlist sev* {
        qui replace `var' = `var' * pop_scaled
    }
    fastcollapse sev* pop_scaled if inlist(age_group_id,1,23,24,25,26), type(sum) by(sex_id age_group_id location_id rei_id year_id)
    foreach var of varlist sev* {
        qui replace `var' = `var' / pop_scaled
    }
    keep if inlist(age_group_id,1,23,24,25,26)
    drop pop_scaled

    local f = "$tmp_dir/large_age_results_$rei_id.dta"
    save `f', replace
    return local large_age_file_path `f'
end
// END
