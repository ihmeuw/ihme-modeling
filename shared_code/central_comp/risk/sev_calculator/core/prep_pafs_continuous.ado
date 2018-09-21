cap program drop prep_pafs_continuous
program define prep_pafs_continuous
    args tmrel_file risks_w_tmrel_draws

    adopath + "$functions_dir"

    if $rei_id == 109 {
        ** pafs before compiling with hip/non-hip causes for merging with rrs
        local file_list : dir "FILEPATH/paf/metab_bmd/" files "paf_*.dta"
        clear
        tempfile pafs
        save `pafs', replace emptyok
        foreach file of local file_list {
            use "FILEPATH/paf/metab_bmd/`file'"
            gen file = "`file'"
            append using `pafs'
            save `pafs', replace           
        }
        rename paf_* draw_*
        keep location_id year_id sex_id age_group_id cause_id rei_id draw_*
    }
    else {
        ** read in all YLL pafs for given risk
        local paf_dir = "FILEPATH/pafs/$paf_version_id/tmp_sev"
        if $testing {
            // tmp just read a few locations while testing
            local files = "`paf_dir'/8.h5 `paf_dir'/7.h5 `paf_dir'/23.h5 `paf_dir'/13.h5 `paf_dir'/14.h5 `paf_dir'/129.h5"
        }
        else {
            local files = "`paf_dir'/*.h5"
        }
        fast_read, input_files("`files'") where("rei_id == $rei_id") num_slots(40) code_dir("$sev_calc_dir") clear
    }
    capture rename paf_* draw_*
    capture drop rei_id
    gen rei_id = $rei_id

    merge m:1 rei_id cause_id sex_id age_group_id using `tmrel_file', keep(3) nogen
    ** scale everything by rr_scalar to units are units of exposure
    replace tmrel_mid = tmrel_mid/rr_scalar if inlist($rei_id, `risks_w_tmrel_draws')
    replace min_1_exp = min_1_exp/rr_scalar
    replace max_99_exp = max_99_exp/rr_scalar
    compress

end
// END
