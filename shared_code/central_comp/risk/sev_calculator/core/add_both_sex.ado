cap program drop add_both_sex
program define add_both_sex, rclass

    run "$functions_dir/fastcollapse.ado"
    run "$functions_dir/get_population.ado"

    merge m:1 location_id year_id age_group_id sex_id using "$tmp_dir/pops_$rei_id.dta", keep(1 3) nogen keepusing(pop_scaled)   
    replace sex_id = 3
    foreach var of varlist sev* {
        qui replace `var' = `var' * pop_scaled
    }
    fastcollapse sev* pop_scaled if sex_id == 3, type(sum) by(sex_id age_group_id location_id rei_id year_id)
    foreach var of varlist sev* {
        qui replace `var' = `var' / pop_scaled
    }
    keep if sex_id == 3
    drop pop_scaled

    local f = "$tmp_dir/both_sex_results_$rei_id.dta"
    save `f', replace
    return local both_sex_file_path `f'
end
// END

