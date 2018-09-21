
// Purpose:    Splitting grouped ages in cancer data

** **************************************************************************
** CONFIGURATION  (AUTORUN)
** **************************************************************************
// Clear memory and set STATA to run without pausing
    clear all
    set more off

// Accept Arguments
    args data_set_group data_set_name data_type

// Load and run set_common function based on the operating system (will load common functions and filepaths relevant for registry intake)
    run "FILEPATH" "registry_input"   // loads globals and set_common function
    set_common, process(4) data_set_group("`data_set_group'") data_set_name("`data_set_name'") data_type("`data_type'")

// set folders
    local metric = r(metric)
    local data_folder = r(data_folder)
    local temp_folder = r(temp_folder)
    local error_folder = r(error_folder)
    local pop_input_data = "`data_folder'/$step_0_pop"
    local split_pop_data = "`data_folder'/$step_4_pop"

// scripts
    local split_population = "$code_for_this_prep_step/split_population.do"
    local merge_with_pop = "$registry_input_code/_registry_common/merge_with_population.do"

// Cause Map, Age Formats, and Weights
    local cause_map "${gbd_cause_map_`data_type'}"
    local im_format_file = "$im_format_map"
    local age_format_file = "$age_format_map"
    local acause_rates = "${age_sex_weights_`data_type'}"
    local age_sex_restrictions_file = "$age_sex_restrictions_file"

** ****************************************************************
** Launch split_population if population is present
**
** ****************************************************************
// check for population file. run function to split ages and sexes if population file exists
    capture confirm file "`pop_input_data'"
    if !_rc do "`split_population'" `data_set_group' `data_set_name' `data_type'

** ****************************************************************
** Get Additional Resources
** ****************************************************************
// Get age formats
    // Im_frmat
        insheet using "`im_format_file'", comma names clear
        tempfile im_frmat_map
        save `im_frmat_map', replace

    // Frmat
        insheet using "`age_format_file'", comma names clear
        tempfile frmat_map
        save `frmat_map', replace

// Get age-sex restrictions
    // load file and keep relevant data
        use "$cause_restrictions", clear
        if "`data_type'" == "mor" local yl_type = "yll"
        if "`data_type'" == "inc" local yl_type = "yld"
        keep acause male female `yl_type'_age_start `yl_type'_age_end
        rename (`yl_type'_age_start `yl_type'_age_end) (age_start age_end)

    // edit age formats
        foreach var in age_start age_end {
            replace `var' = floor(`var'/5) + 6 if `var' >= 5
            replace `var' = 0 if `var' < 5
        }

    // save
        tempfile age_sex_restrictions
        save `age_sex_restrictions', replace

** ****************************************************************
** Part 1: Determine if split is necessary
** ****************************************************************
// Get data
    use "`data_folder'/$step_3_output", clear

// ensure proper variable formats
    recast double `metric'*

// Add missing age categories
    foreach n of numlist 3/6 23/26 91/94{
        capture gen double `metric'`n' = 0
    }

// // DETERMINE IF AGE-SPLITTING IS NEEDED
    // Determine if non-standard age formats are present. Frmat 9 is handled by "age unknown" section, hence is not considered in "non-standard".
        local nonStandard_ageFormat = 0
        count if  !inlist(frmat, 1, 2, 9, 131) | (!inlist(im_frmat, 1, 2, 8, 9) & frmat != 9)
        if r(N) > 0 local nonStandard_ageFormat = 1

    // Determine if data are present for "age unknown"
        local age_unknown_data = 0
        count if !inlist(`metric'26, 0, .)
        if r(N) local age_unknown_data = 1

    // Determine if data are present for aggregate sex
        global sex_needs_split = 0

    // drop "both sex" data if data for individual sexes are present
        bysort registry_id cause* year* coding frmat im_frmat: egen has_dif_sex = count(sex) if sex == 1 | sex == 2
        drop if sex == 3 & !inlist(has_dif_sex, 0 , .)
        drop has_dif_sex

    // count
        count if sex == 3 | sex == 9
        if r(N) > 0 global sex_needs_split = 1

// // Save if no split is needed
if !`nonStandard_ageFormat' & !`age_unknown_data' & !$sex_needs_split {
    // replace missing data with zeros
    foreach n of numlist 3/6 23/26 91/94{
        replace `metric'`n' = 0 if `metric'`n' == .
    }

    // recalculate metric totals
    drop `metric'1
    egen `metric'1 = rowtotal(`metric'*)

    // Collapse remaining under5 and 80+ ages
    gen under5 = `metric'2 + `metric'3 + `metric'4+ `metric'5+ `metric'6 +`metric'91 + `metric'92 +`metric'93 + `metric'94
    gen eightyPlus =  `metric'22 + `metric'23 + `metric'24+ `metric'25
    replace `metric'2 = under5
    replace `metric'22 = eightyPlus
    drop `metric'3 `metric'4 `metric'5 `metric'6 `metric'23 `metric'24 `metric'25 `metric'26 `metric'91 `metric'92 `metric'93 `metric'94
    drop under5 eightyPlus

    // update frmat and im_frmat to reflect changes
    replace frmat = 131
    replace im_frmat = 9

// SAVE
    save_prep_step, process(4)
    capture log close
    exit, clear
}

** ****************************************************************
** Part 2: Create Custom Weights if necessary
** ****************************************************************
// Replace blank cause entries: used to merge data, removed at end of script
    replace cause = cause_name if cause == ""
    replace gbd_cause = "_gc" if gbd_cause == ""

    // Save pre-split values for later verification
        gen obs = _n
        preserve
            keep obs registry_id year_start year_end coding_system cause cause_name gbd_cause acause* `metric'1
            rename `metric'1 preSplit_`metric'1
            tempfile pre_split
            save `pre_split', replace
        restore

        keep obs registry_id cause gbd_cause acause* year* sex frmat im_frmat `metric'*

    // Save a copy of the data to be used later
        tempfile beforeCustom
        save `beforeCustom', replace


// Make custom weights by multiplying the global cancer rate by the population of the data
    use `beforeCustom', clear

    // Keep only data used in creating weights. Preserve the mapped cause and other datum information to be re-attached later
        replace acause1 = gbd_cause if acause1 == "." | acause1 == ""
        keep obs registry_id year* sex cause acause* gbd_cause
        duplicates drop

    // Reshape so that weights will merge with all associated causes. Drop associated causes that are blank
        replace acause1 = gbd_cause if acause1 == ""
        reshape long acause@, i(obs registry_id year* sex cause) j(seq)
        capture _strip_labels*
        replace acause = "" if acause == "."
        drop if acause == ""

    // for sex=3 data, add sex =1 and sex =2  so that weights can be created. NOTE: "both sex" data were dropped at beginning of script if data for individual sexes are present by observation
    if $sex_needs_split {
        preserve
            replace sex = 3 if sex == 9
            keep if sex == 3
            duplicates drop
            tempfile unknown_sex
            save `unknown_sex', replace
        restore
        replace sex = 2 if sex == 3
        append using `unknown_sex'
        replace sex = 1 if sex == 3
        append using `unknown_sex'
    }

    // Create a special map so that garbage codes with alternate causes might be merged with weights
        preserve
            use `cause_map', clear
            keep if regexm(coding_system, "ICD")
            keep cause gbd_cause
            rename (cause gbd_cause) (acause mapped_cause)
            tempfile map_extra_acauses
            save `map_extra_acauses', replace
        restore

    // Adjust the temporary acause based on the map to enable merge with weights
        merge m:1 acause using `map_extra_acauses', keep(1 3)
        replace acause = mapped_cause if _merge == 3 & !regexm(acause, "neo_")
        replace acause = "neo_leukemia" if regexm(acause, "neo_leukemia")
        replace acause = "neo_nmsc" if regexm(acause, "neo_nmsc")
        drop mapped_cause _merge

    // Merge weights file with remaining dataset. Replace any acause entries that failed to merge with "average_cancer"
        merge m:1 sex acause using `acause_rates', keep(1 3)
        egen rate_tot = rowtotal(*rate*)
        replace acause = "average_cancer" if rate_tot == 0 | _merge == 1
        drop *rate* _merge rate_tot
        duplicates drop
        merge m:1 sex acause using `acause_rates', keep(1 3) nogen
        if "`data_type'" == "inc" rename inc_rate* rate*
        else rename death_rate* rate*

    // Replace empty entries with "0", multiply  and collapse weigths to be cause & sex specific
        foreach var of varlist rate* {
            replace `var' = 0 if `var' == .
        }

    // merge with population data
        do "`merge_with_pop'" "`data_folder'"

    // make weights (weights = the expected number of deaths = rate*pop)
        foreach n of numlist 2 7/22 {
            gen double wgt`n' = rate`n'*pop`n'
        }

    // Collapse the weights for each case to equal the average of the weights. Should only affect codes with multiple alternate causes (acauseX) by combining the individual weights of the alternate causes.
        collapse (sum) wgt*, by(obs sex year* cause) fast

    // keep only variables that are relevant for weights
        keep obs sex cause wgt*
        duplicates tag obs sex cause, gen(tag)
        count if tag >1
        if r(N) {
            di "error. duplicates exist when they should not"
            save "`error_folder'/`data_set_name'_`data_type'_duplicated_weights_${today}.dta"
            BREAK
        }
        drop tag

    // reshape
        reshape long wgt@, i(obs sex cause) j(gbd_age)

    // Save
        tempfile cause_wgts
        save `cause_wgts', replace

    // // Add weights for additional sexes if sex=3 data exists, since weights may have only merged onto sex=3 data for a given obs
    if $sex_needs_split {
        // reshape and collapse to create weights by sex
            reshape wide wgt@, i(obs cause gbd_age) j(sex)
            collapse (sum) wgt*, by(obs cause gbd_age) fast

        // regenerate wgt for sex = 3. 
            drop wgt3
            gen wgt3 = wgt1 + wgt2

        // recalculate weights for sex=1 and sex =2 as their proportions of the total number of deaths (weight for sex =3)
            rename (wgt1 wgt2) (orig_wgt1 orig_wgt2)
            gen wgt1 = orig_wgt1/wgt3
            gen wgt2 = orig_wgt2/wgt3
            gen noWgt = 1 if inlist(wgt3, 0, .)
            replace wgt1 = .5 if noWgt == 1
            replace wgt2 = .5 if noWgt == 1
            drop orig* noWgt

        // drop weights for sex =3. save the data so that they can be joined with sex = 3 data and split it into two sexes
            drop wgt3
            reshape long wgt@, i(obs cause gbd_age) j(sex)
            replace wgt = 1 if wgt == .
            rename sex new_sex
            gen sex = 3

        // save
            tempfile sex_split_wgts
            save `sex_split_wgts', replace
    }

** ****************************************************************
** Part 3: Split age and sex
** ****************************************************************
    // restore pre-custom data
        use `beforeCustom', clear

    // preserve total deaths for testing
        capture summ(`metric'1)
        local pre_split_total = r(sum)

    // prepare data for editing
        keep obs cause sex frmat im_frmat `metric'*
        rename `metric'1 metric_total
        reshape long `metric', i(obs cause sex frmat im_frmat metric_total) j(age)
        capture _strip_labels*

    // // Split
    if `nonStandard_ageFormat' {
        // mark those entries that need to be split. Do not split if frmat == 9 (unknown age)
            gen need_split = 1 if  !inlist(frmat, 1, 2, 9, 131) | (!inlist(im_frmat, 1, 2, 8, 9) & frmat != 9)
            replace need_split = 0 if need_split != 1

        // for each format type, mark which age categories need to be split per the corresponding format map. split only those categories. can split multiple age formats at once
        foreach format_type in im_frmat frmat {
            // preserve a copy in case split cannot be performed
            preserve

            // merge with map and reshape
                if "`format_type'" == "im_frmat" merge m:1 im_frmat age using `im_frmat_map', keep(1 3)
                if "`format_type'" == "frmat" merge m:1 frmat age using `frmat_map', keep(1 3)
                reshape long age_specific@, i(obs cause sex frmat im_frmat age `metric' need_split _merge) j(age_split_num)

            // keep one copy of each entry (for totals) and keep one copy each for the new categories created in the split
                keep if (_merge == 1 & age_split_num == 1) | (_merge == 3 & age_specific != .)

            // mark those data that need to be split. skip to next format if no split can occur
                replace need_split = 0 if _merge == 1
                count if need_split == 1
                if !r(N) {
                    restore
                    continue
                }
                else restore, not


            // edit age formats and rename to enable merge with weights
                gen gbd_age = (age_specific / 5) + 6 if _merge == 3 & age_specific >= 5
                replace gbd_age = 2 if _merge == 3 & age_specific < 5
                rename _merge _merge_`format_type'

            // add weights
                merge m:1 obs sex cause gbd_age using `cause_wgts', keep(1 3) nogen
                egen double wgt_tot = total(wgt), by(obs cause sex `format_type' age)
                replace wgt = 1 if wgt_tot == 0 | wgt_tot == . & need_split == 1
                replace wgt = 0 if wgt == . & need_split == 1
                egen double wgt_scaled = pc(wgt), by(obs cause sex `format_type' age) prop

            // replace only those weights that are marked to be split
                replace `metric' = wgt_scaled * `metric' if need_split == 1
                replace age = gbd_age if need_split == 1

            // collapse, then update format types of split data
                keep `metric' obs cause sex frmat im_frmat age
                collapse (sum) `metric', by(obs cause sex frmat im_frmat age) fast
        }

        // rname age formats to original name
            replace im_frmat = 9
            replace frmat = 131 if frmat != 9

        // save copy in case of troubleshooting
            tempfile afterFrmat
            save `afterFrmat', replace
    }

    // Ensure that remaining under5 and 80+ ages are collapsed
        // Alert user if data remains in under1 age groups
            foreach n of numlist 91/94 {
                count if age == `n' & `metric' != 0
                if r(N) > 0 {
                    noisily di in red "Error in im_frmat. `metric' data still exist in `metric'`n' after age/sex split"
                    BREAK
                }
            }

        // combine under5 and 80+
        bysort obs cause sex: egen under5 = total(`metric') if inlist(age, 2, 3, 4, 5, 6, 91, 92, 93, 94)
        replace `metric' = under5 if age == 2
        bysort obs cause sex: egen eightyPlus = total(`metric') if inrange(age, 22, 25)
        replace `metric' = eightyPlus if age == 22
        drop if inlist(age, 3, 4, 5, 6, 23, 24, 25, 91, 92, 93, 94)
        drop under5 eightyPlus

    // // // Redistribute "unknown age" data according to the current distribution of cases/deaths
    if `age_unknown_data' {
        // Remove age-category indication for "unknown age" data
        preserve
            keep obs cause sex im_frmat frmat age `metric'
            keep if age == 26
            rename `metric' unknown_age_`metric'
            drop age
            tempfile unknown_age_data
            save `unknown_age_data', replace
        restore

        // // Recombine data with "unknown age" data
            // Merge with custom weights, calculate weights according to the percent composition of cases/deaths by age category, and redistribute "unknown age" data by weight type (custom or percent composition)
            drop if inlist(age, 1, 26)
            merge m:1 obs sex im_frmat frmat using `unknown_age_data', assert(3) nogen

            // add custom weights where indicated. for datasets with `sex_unknown' data, ignore sex = 1 or sex = 2 data from unknown_age_wgts (hence assert(2 3) below)
            rename age gbd_age
            merge m:1 obs sex cause gbd_age using `cause_wgts', keep(1 3) assert(2 3) nogen
            rename gbd_age age

            // calculate redistributed values and replace cases/deaths with that value
            egen double total_wgt = total(wgt), by(obs sex im_frmat frmat)
            egen double wgt_pc = pc(wgt), by(obs sex im_frmat frmat) prop
            replace wgt = 1 if total_wgt == 0 | total_wgt == .
            gen double distributed_unknown = unknown_age_`metric' * wgt_pc
            replace distributed_unknown = 0 if distributed_unknown == .

            // add redistributed data to the rest of the data
                replace `metric' = `metric' + distributed_unknown
                keep obs cause sex im_frmat frmat age `metric'
                replace frmat = 131

            // save copy for troubleshooting
                tempfile redistributed_unknown_metric
                save `redistributed_unknown_metric', replace
    }

    // // Split Sex = 3 and sex = 9 data
        local percentage_sex_split = 0
        if $sex_needs_split {
            preserve

                // subset
                    replace sex = 3 if sex == 9
                    keep if sex == 3
                    collapse (sum) `metric', by(obs cause sex frmat im_frmat age)

                // merge with weights
                    rename age gbd_age
                    joinby gbd_age sex cause obs using `sex_split_wgts'
                    rename gbd_age age

                // scale data
                    rename `metric' orig_`metric'
                    gen `metric' = wgt * orig_`metric'
                    keep obs cause sex new_sex im_frmat frmat age `metric'

                // reshape to enable later merge
                    reshape wide `metric', i(obs cause sex new_sex im_frmat frmat) j(age)

                // save copy for troubleshooting
                    tempfile sex_disaggregated
                    save `sex_disaggregated', replace

            restore
        }

    // save a copy for troubleshooting
    tempfile redistributed
    save `redistributed', replace

** ****************************************************************
** Part 4: Finalize
** ****************************************************************
    // Reshape
        reshape wide `metric', i(obs cause sex) j(age)
        merge m:1 obs using `pre_split', assert(3) nogen

        // add disaggregated sex
        if $sex_needs_split == 1 {
            replace sex = 3 if sex == 9
            foreach m of varlist `metric'* {
                replace `m' = . if sex == 3
            }
            merge 1:m obs using `sex_disaggregated', update keep(1 3 4) assert(1 3 4) nogen
            replace sex = new_sex if !inlist(sex, 1, 2)
            drop new_sex
            drop if !inlist(sex, 1, 2)
        }

    // Check for errors. If more than 3 metric totals are greater than .0001% different from the original number, alert the user. (Below this fraction errors are likely due to rounding)
        // check rowtotals  (generated specially to handle sex split data)
        egen post_row_total = rowtotal(`metric'2-`metric'22)
        bysort obs: egen postSplit_`metric'1 = total(post_row_total)
        capture count if abs(postSplit_`metric'1 - preSplit_`metric'1) > .00001*preSplit_`metric'1
        if r(N) {
            pause on
            display in red "Error: total `metric' in `data_set_name' are not equivalent in all rows before and after split"
            gen diff_pre_post = abs(postSplit - preSplit)
            gsort -diff_pre_post
            pause
            drop diff_pre_post
            pause off
        }
        drop preSplit_`metric'1 postSplit_`metric'1

        // test sum of all deaths/cases
        egen `metric'1 = rowtotal(`metric'*)
        capture summ(`metric'1)
        local delta = r(sum) - `pre_split_total'
        if (abs(`delta') > 0.00005 * `pre_split_total' & `percentage_sex_split' < .75) | (abs(`delta') > 0.005 * `pre_split_total' & `percentage_sex_split' >= .75)  {
            noisily di in red "ERROR: Total `metric' in `data_set_name' before age_sex split does not equal total after (a difference of `delta' `metric')."
            BREAK
        }

    // Remove causes that were added earlier in the script
        replace cause = "" if cause == cause_name

    // collapse dataset
        collapse (sum) `metric'* , by(registry_id year* coding *cause* acause* sex)

    // Replace frmat and im_frmat variables to reflect current status
        capture drop `metric'26
        gen frmat = 131
        gen im_frmat = 9
        recast float `metric'*
        aorder

    // restore data_set_name
        capture gen data_set_name = "`data_set_name'"

    // SAVE
        save_prep_step, process(4)
        capture log close

** **************************************************************************
** END age_sex_split.do
** **************************************************************************
