cap program drop prep_rrs
program define prep_rrs, rclass
    syntax, location_ids(numlist) age_group_ids(numlist) year_ids(numlist)

    adopath + "$functions_dir"
    create_connection_string
    local con = r(conn_string)

    // for backpain and hearing, we need to grab morbidity rrs instead of mortality
    global yld_risks = "130, 132"

    ** pull RRs **
    // read in RR(max) for air_pm, air_hap, drugs_alcohol, occ_hearing, and bmd
    //calculate RR for drugs_illicit_suicide with PAF code
    if inlist($rei_id,86,87,102,109,140,130) {

        if $rei_id == 140 {
            local n = 0
            foreach me in 1977 1978 1976 {
                clear
                set obs 1
                if `me' == 1977 {
                    gen modelable_entity_id = `me'
                    gen sd = ((ln(16.94)) - (ln(3.93))) / (2*invnormal(.975))
                    forvalues i = 0/999 {
                        gen double rr_`i' = exp(rnormal(ln(8.16), sd))
                    }
                }
                if `me' == 1978 {
                    gen modelable_entity_id = `me'
                    gen sd = ((ln(16.94)) - (ln(3.93))) / (2*invnormal(.975))
                    forvalues i = 0/999 {
                        gen double rr_`i' = exp(rnormal(ln(8.16), sd))
                    }
                }
                if `me' == 1976 {
                    gen modelable_entity_id = `me'
                    gen sd = ((ln(10.53)) - (ln(4.49))) / (2*invnormal(.975))
                    forvalues i = 0/999 {
                        gen double rr_`i' = exp(rnormal(ln(6.85), sd))
                    }
                }
                gen cause_id = 721
                expand 2, gen(dup)
                replace cause_id = 723 if dup == 1
                drop dup
                gen parameter = "cat1"
                gen n = 1
                tempfile r
                save `r', replace
                clear
                set obs 235
                gen age_group_id = _n
                keep if inlist(age_group_id,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235)
                gen n = 1
                joinby n using `r'
                keep cause_id age_group_id modelable_entity_id parameter rr*

                expand 2, gen(dup)
                forvalues i = 0/999 {
                    replace rr_`i' = 1 if dup==1
                }
                replace parameter="cat2" if dup==1 
                drop dup
                gen mortality=1
                gen morbidity=1
                gen sex_id = 1
                expand 2, gen(dup)
                replace sex_id = 2 if dup == 1
                drop dup
                local n = `n' + 1
                tempfile `n'
                save ``n'', replace
            }
            clear
            forvalues i = 1/`n' {
                append using ``i''
            }
            gen rei_id = $rei_id
            keep if mortality == 1
            fastcollapse rr*, type(mean) by(rei_id sex_id age_group_id cause_id parameter)
        }

        if inlist($rei_id,86,87,102,130) {

            // pull cause_id and age_group_id from database to merge on
            get_cause_metadata, cause_set_id(3) clear
            keep cause_id acause
            tempfile cause_merge
            save `cause_merge', replace
            //air pm
            if $rei_id == 86 {
                insheet using "$sev_calc_dir/core/data/air_pm_rrmax.csv", clear
            }
            //air hap
            else if $rei_id == 87 {
                insheet using "$sev_calc_dir/core/data/air_hap_rrmax.csv", clear
            }
            //alcohol
            else if $rei_id == 102 {
                insheet using "$sev_calc_dir/core/data/alcohol_rrmax.csv", clear
                reshape wide rr, i(cause_id sex_id age_group_id) j(draw)
                rename rr* rr_*
                expand = 2 if sex_id == 3, generate(expanded)
                replace sex_id = 2 if expanded
                replace sex_id = 1 if sex_id == 3
                drop expanded
            }
            // hearing
            else if $rei_id == 130 {
                insheet using "$sev_calc_dir/core/data/occ_hearing_rrmax.csv", clear
            }
            if inlist($rei_id,86,87) {
                gen sex_id = 1
                expand = 2, generate(expanded)
                replace sex_id = 2 if expanded
                drop expanded
                if $rei_id == 86 {
                    merge m:1 acause using `cause_merge', keep(3) assert(2 3) nogen
                    drop acause  
                }
            }
            if inlist($rei_id,130) {
                forvalues i = 0/999 {
                    gen draw_`i' = mean
                }
            }
            capture rename draw_* rr_*
            gen rei_id = $rei_id
        }
        //BMD
        if $rei_id == 109 {
            insheet using "FILEPATH/RR.csv", comma double clear
            drop risk year_id mortality morbidity parameter
            gen rei_id = $rei_id
            drop acause
        }     
    }

    //for all other risks, use get_draws
    else {
        if $rei_id == 134 { //for CSA read in both male and female
            get_draws, ///
                source(risk) gbd_id_field(rei_id rei_id) gbd_id(244 245) year_ids(`year_ids') ///
                location_ids(`location_ids') age_group_ids(`age_group_ids') sex_ids(1 2) kwargs(draw_type:rr num_workers:40) clear
        }
        else if $rei_id == 99 { // for smoking read in SIR and prevlelance
            get_draws, ///
                source(risk) gbd_id_field(rei_id rei_id) gbd_id(165 166) year_ids(`year_ids')  ///
                location_ids(`location_ids')  age_group_ids(`age_group_ids') sex_ids(1 2) kwargs(draw_type:rr num_workers:40) clear
        }
        else if inlist($rei_id,334,335) { // for lbw/sg read in joint
            get_draws, ///
                source(risk) gbd_id_field(rei_id) gbd_id(339) location_ids(`location_ids') ///
                kwargs(draw_type:rr num_workers:40)  age_group_ids(`age_group_ids') sex_ids(1 2) year_ids(`year_ids') clear
        }
        else {
            get_draws, ///
                source(risk) gbd_id_field(rei_id) gbd_id($rei_id) location_ids(`location_ids') ///
                kwargs(draw_type:rr num_workers:40)  age_group_ids(`age_group_ids') sex_ids(1 2) year_ids(`year_ids') clear
        } 
        gen rei_id = $rei_id
        drop model_version_id modelable_entity_id
        
        // for backpain and hearing, grab morbidity rrs instead of mortality
        if inlist($rei_id, $yld_risks) {
            keep if morbidity == 1
        }
        else {
            keep if mortality == 1
        }

    }

    if inlist($rei_id,334,335) { // for lbw/sg rescale RRs as done in PAF calc
        expand 2 if parameter == "cat55", gen(dup)
        replace parameter = "cat56" if dup == 1
        forvalues i = 0/999 {
            qui replace rr_`i' = 1 if dup == 1
        }
        drop dup
        tempfile all
        save `all', replace
        import delimited using "FILEPATH/nutrition_lbw_preterm_tmrel.csv", clear
        gen preterm = .
        replace preterm = 1 if regexm(modelable_entity_name,"\[0, 24\) wks")
        replace preterm = 2 if regexm(modelable_entity_name,"\[24, 26\) wks")
        replace preterm = 3 if regexm(modelable_entity_name,"\[26, 28\) wks")
        replace preterm = 4 if regexm(modelable_entity_name,"\[28, 30\) wks")
        replace preterm = 5 if regexm(modelable_entity_name,"\[30, 32\) wks")
        replace preterm = 6 if regexm(modelable_entity_name,"\[32, 34\) wks")
        replace preterm = 7 if regexm(modelable_entity_name,"\[34, 36\) wks")
        replace preterm = 8 if regexm(modelable_entity_name,"\[36, 37\) wks")
        replace preterm = 9 if regexm(modelable_entity_name,"\[37, 38\) wks")
        replace preterm = 10 if regexm(modelable_entity_name,"\[38, 40\) wks")
        replace preterm = 11 if regexm(modelable_entity_name,"\[40, 42\) wks")
        gen lbw = .
        replace lbw = 1 if regexm(modelable_entity_name,"\[0, 500\) g")
        replace lbw = 2 if regexm(modelable_entity_name,"\[500, 1000\) g")
        replace lbw = 3 if regexm(modelable_entity_name,"\[1000, 1500\) g")
        replace lbw = 4 if regexm(modelable_entity_name,"\[1500, 2000\) g")
        replace lbw = 5 if regexm(modelable_entity_name,"\[2000, 2500\) g")
        replace lbw = 6 if regexm(modelable_entity_name,"\[2500, 3000\) g")
        replace lbw = 7 if regexm(modelable_entity_name,"\[3000, 3500\) g")
        replace lbw = 8 if regexm(modelable_entity_name,"\[3500, 4000\) g")
        replace lbw = 9 if regexm(modelable_entity_name,"\[4000, 4500\) g")
        drop model* old_id
        joinby sex_id parameter using `all'
        if $rei_id == 334 {
            local child "preterm"
            local max 11
        } 
        if $rei_id == 335 {
            local child "lbw"
            local max 9
        } 
        forvalues k = 1/`max' {
            preserve
                keep if `child' == `k'
                forvalues i = 0/999 {
                    sort location_id year_id age_group_id sex_id cause_id tmrel_`child'
                    bysort location_id year_id age_group_id sex_id cause_id : gen scalar = rr_`i'[1]
                    qui replace rr_`i' = rr_`i' / scalar
                    qui replace rr_`i' = 1 if tmrel_`child' == 1
                    drop scalar
                }
                tempfile `k'
                save ``k'', replace
            restore 
        }
        clear
        forvalues i = 1/`max' {
            append using ``i''
        }
    }

    if $rei_id != 108 {
        ** find max and average across time and space
        if $continuous {
                 ** for continuous risks we average across years and locations 
                 ** before taking rowise mean. Mean_rr is used in calc_scalars
                 fastcollapse rr*, type(mean) by(rei_id sex_id age_group_id cause_id)
        }
        else {
            if !inlist($rei_id,86,87,102,109,130) {
                if inlist($rei_id,315,316,317,318,319,320,321) {
                    // for vaccines RR needs to be inverted
                    forvalues i = 0/999 {
                        qui replace rr_`i' = 1/rr_`i'
                    }
                }
                ** for categorical risks, we need to find the category (usually cat1)
                ** that contains the highest RR draw. We need to do this by outcome, age, and sex.
                ** Another way to phrase this is we're finding the max rr value across space and time
                ** We keep the rr draw columns (unlike continuous)
                ** per unique values of key, we want only rows belonging to the category that has the highest rr draw value
                ** we'll do this by merging on a filtering dataset, that just contains key and parameter (aka category)
                egen key = group(cause_id age_group_id sex_id) 
                preserve
                egen rowwise_max = rowmax(rr*) // top rr draw value per row
                bysort key: egen double max_per_key = max(rowwise_max) // by a/s/c, max rr draw value
                gen top_category = (max_per_key == rowwise_max) // boolean that's 1 if a row contains the top rr draw value in a/s/c
                keep if top_category 
                contract key parameter  // create a filtering dataset that only contains the top category per a/s/c
                drop _freq
                // A possible edge case is if two categories in a a/s/c group both contain the same max draw value
                // We can check for that by asserting there's only 1 category per key
                isid key parameter
                tempfile top_category_per_key
                save `top_category_per_key'
                restore
                // now that we know, for each a/s/c, which category to keep, we can filter using merge
                merge m:1 key parameter using `top_category_per_key',  keep(3) nogen
                drop key
            }

            // same as continuous risks, average rr over time and space
            fastcollapse rr*, type(mean) by(rei_id sex_id age_group_id cause_id)

        }
    }
    // bmi is cat and cont
    else {
        
        tempfile all
        save `all', replace
            keep if parameter != "per unit"
            egen key = group(cause_id age_group_id sex_id) 
            preserve
                egen rowwise_max = rowmax(rr*) 
                bysort key: egen double max_per_key = max(rowwise_max)
                gen top_category = (max_per_key == rowwise_max) 
                keep if top_category 
                contract key parameter
                drop _freq
                isid key parameter
                tempfile top_category_per_key
                save `top_category_per_key'
            restore
            merge m:1 key parameter using `top_category_per_key',  keep(3) nogen
            drop key
            tempfile child
            save `child', replace
        use `all', clear
        keep if parameter == "per unit"
        append using `child'
        fastcollapse rr*, type(mean) by(rei_id sex_id age_group_id cause_id)

    }

    keep rei_id sex_id age_group_id cause_id rr*
    local f = "$tmp_dir/rr_$rei_id.dta"
    save `f', replace
    return local file_path `f'
end
// END