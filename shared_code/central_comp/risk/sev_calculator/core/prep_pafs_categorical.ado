cap program drop prep_pafs_categorical
program define prep_pafs_categorical
    syntax, rrs(string) location_ids(numlist) age_group_ids(numlist)

    adopath + "$functions_dir"
    run "$sev_calc_dir/core/readers/fast_read.ado"

    ** read in all YLL pafs for given risk
    local paf_dir = "FILEPATH/pafs/$paf_version_id/tmp_sev"
    if inlist($rei_id, $yld_risks) {
        // unless occ_backpain or hearing -- in that case use YLD pafs
        local paf_dir = "`paf_dir'" + "/yld"
    }
    if $testing {
        // if we're testing, just read a few files instead of everything
        local files = "`paf_dir'/8.h5 `paf_dir'/7.h5 `paf_dir'/23.h5 `paf_dir'/13.h5 `paf_dir'/14.h5 `paf_dir'/129.h5"
    }
    else {
        local files = "`paf_dir'/*.h5"
    }

    ** read the pafs
    if $rei_id == 134 {
        // read both male and female pafs for CSA
        fast_read, input_files("`files'") where("rei_id in [244,245]") num_slots(40) code_dir("$sev_calc_dir") clear
    }
    else if $rei_id == 99 {
        // read both SIR and prev pafs for smoking
        fast_read, input_files("`files'") where("rei_id in [165,166]") num_slots(40) code_dir("$sev_calc_dir") clear
    }
    else {
        fast_read, input_files("`files'") where("rei_id == $rei_id") num_slots(40) code_dir("$sev_calc_dir") clear
    }
    capture rename paf_* draw_*
    capture drop rei_id
    gen rei_id = $rei_id

    ** merge on RRs
    merge m:1 rei_id cause_id sex_id age_group_id using `rrs', keep(3) nogen
    compress

end
// END
