cap program drop add_all_age
program define add_all_age, rclass

    run "$functions_dir/fastcollapse.ado"
    run "$functions_dir/get_population.ado"

    merge m:1 location_id year_id age_group_id sex_id using "$tmp_dir/pops_$rei_id.dta", keep(3) nogen keepusing(pop_scaled)   

    replace age_group_id = 22
    foreach var of varlist sev* {
        qui replace `var' = `var' * pop_scaled
    }
    fastcollapse sev* pop_scaled if age_group_id == 22, type(sum) by(sex_id age_group_id location_id rei_id year_id)
    foreach var of varlist sev* {
        qui replace `var' = `var' / pop_scaled
    }
    keep if age_group_id == 22
    drop pop_scaled

    local f = "$tmp_dir/all_age_results_$rei_id.dta"
    save `f', replace
    return local all_age_file_path `f'
end
// END

