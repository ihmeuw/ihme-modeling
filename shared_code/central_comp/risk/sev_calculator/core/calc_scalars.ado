cap program drop calc_scalars
program define calc_scalars, rclass
    
    adopath + "$functions_dir"
    local f = "$tmp_dir/pre_pre_results_$rei_id.dta"
    save `f', replace
    ** for most risks, we just do categorical or continous
    if $rei_id != 108 {
        if $continuous {

            qui gen diff =max_99_exp -tmrel_mid
            // If the exposure type is equal to one, subtract exposure from tmred.
            // because the risk is protective
            qui replace diff=tmrel_mid-min_1_exp if inv_exp==1
            
            // max rr can't be below 1, so if diff is negative, this changes it to 0. 
            // Which causes max_rr to be 1. If diff is positive, it does nothing
            qui replace diff=(abs(diff)+diff)/2

            forvalues i = 0/999 {
                qui replace draw_`i' = 0 if draw_`i' < 0 // no negative pafs
                qui replace draw_`i' = 1 if draw_`i' > 1 // no pafs > 1
                qui gen max_rr_`i' = rr_`i'^(diff)
                // continuous sev is defined as (paf/(1-paf)/(max_rr-1), where max_rr is rowwise mean
                qui gen double sev_`i' = (draw_`i'/(1-draw_`i'))/(max_rr_`i'-1)
                qui replace sev_`i' = 0 if max_rr_`i' <= 1
                qui replace sev_`i' = 1 if sev_`i' > 1 //truncate
            }
            egen median = rowpctile(sev*), p(50)
            forvalues i = 0/999{
                qui replace sev_`i' = median if sev_`i' == .
                qui drop if sev_`i'==. //if paf is 1, ignore
            }
            drop median

        }
        else {
            
            // Categorical SEV is (paf/1-paf) / (rr_max - 1), where rr_max is defined as
            // the rr category with the highest rr draw value for that a/s/c
            forvalues i = 0/999 {
                qui replace draw_`i' = 0 if draw_`i' < 0 // no negative pafs
                qui replace draw_`i' = 1 if draw_`i' > 1 // no pafs > 1
                qui gen double sev_`i' = (draw_`i'/(1-draw_`i'))/(rr_`i'-1)
                qui replace sev_`i' = 0 if rr_`i' <= 1
                qui replace sev_`i' = 1 if sev_`i' > 1 //truncate
            }
            egen median = rowpctile(sev*), p(50)
            forvalues i = 0/999{
                qui replace sev_`i' = median if sev_`i' == .
                qui drop if sev_`i'==. //if paf is 1, ignore
            }
            drop median

        }
    }
    ** bmi is categorical for < 9 and continuous for >= 9 
    else {
        preserve
            keep if inlist(age_group_id,5,7,6,8)
            forvalues i = 0/999 {
                qui gen double sev_`i' = (draw_`i'/(1-draw_`i'))/(rr_`i'-1)
                qui replace sev_`i' = 0 if rr_`i' <= 1
                qui replace sev_`i' = 1 if sev_`i' > 1 // truncate
            }
            egen median = rowpctile(sev*), p(50)
            forvalues i = 0/999{
                qui replace sev_`i' = median if sev_`i' == .
                qui drop if sev_`i'==. // if paf is 1, ignore
            }
            drop median
            tempfile child
            save `child', replace
        restore
        keep if age_group_id > 8
        qui gen diff =max_99_exp-tmrel_mid
        qui replace diff=(abs(diff)+diff)/2
        qui replace diff=(abs(diff)+diff)/2
        forvalues i = 0/999{
            qui replace draw_`i' = 0 if draw_`i' < 0 // no negative pafs
            qui replace draw_`i' = 1 if draw_`i' > 1 // no pafs > 1
            qui gen max_rr_`i' = rr_`i'^(diff)
            // continuous sev is defined as (paf/(1-paf)/(max_rr-1), where max_rr is rowwise mean
            qui gen double sev_`i' = (draw_`i'/(1-draw_`i'))/(max_rr_`i'-1)
            qui replace sev_`i' = 0 if max_rr_`i' <= 1
            qui replace sev_`i' = 1 if sev_`i' > 1 //truncate
        }
        egen median = rowpctile(sev*), p(50)
        forvalues i = 0/999{
            qui replace sev_`i' = median if sev_`i' == .
            qui drop if sev_`i'==. // if paf is 1, ignore
        }
        drop median
        append using `child'
    }
    
    //save RR max
    preserve
        keep age_group_id sex_id rei_id cause_id rr*
        duplicates drop
        save "$base_dir/$paf_version_id/rrmax/draws/$rei_id.dta", replace
        fastrowmean rr*, mean_var_name(max_rr)
        keep age_group_id sex_id rei_id cause_id max_rr
        save "$base_dir/$paf_version_id/rrmax/summary/$rei_id.dta", replace
    restore

    ** right now results are still cause specific, so these are scalars. 
    ** average across cause to get actual sevs
    fastcollapse sev*, type(mean) by(sex_id age_group_id location_id rei_id year_id)

    local f = "$tmp_dir/results_$rei_id.dta"
    save `f', replace
    return local file_path `f'

end
// END
