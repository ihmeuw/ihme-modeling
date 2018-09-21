
// Purpose:    Splitting grouped ages in cancer data

** **************************************************************************
** Configure and Set Folders
** **************************************************************************
    // Clear memory and set memory and variable limits
        clear all
        set more off

    // Accept Arguments
        args data_set_group data_set_name data_type

    // Load and run set_common function based on the operating system (will load common functions and filepaths relevant for registry intake)
        run "FILEPATH" "registry_input"   // loads globals and set_common function

    // Set common folders (differs if creating weights rather than processing input data)
        if "`data_set_group'/`data_set_name'" == "_parameters/mapping" local generating_parameters = 1
        else local generating_parameters = 0

        if `generating_parameters' {
            set_common, process(3) data_set_group("`data_set_group'") data_set_name("`data_set_name'") data_type("`data_type'")
            local temp_folder = r(temp_folder)
            local data_folder = r(data_folder)
            local pop_input_data = "`temp_folder'/inc_temp_pop_only.dta"
        }
        else {
            set_common, process(4) data_set_group("`data_set_group'") data_set_name("`data_set_name'") data_type("`data_type'")
            local temp_folder = r(temp_folder)
            local data_folder = r(data_folder)
            local pop_input_data = "`data_folder'/$step_0_pop"
        }

    // Additional folders
        local metric = r(metric)
        local im_format_file = "$im_format_map"
        local age_format_file = "$age_format_map"

** ****************************************************************
** GET ADDITIONAL RESOURCES
** ****************************************************************
// // Get age formats
    // Im_frmat
        insheet using "`im_format_file'", comma names clear
        tempfile im_frmat_map
        save `im_frmat_map', replace

    // Frmat
        insheet using "`age_format_file'", comma names clear
        tempfile frmat_map
        save `frmat_map', replace

// // Create other maps
    // Get location population
        use "$ihme_populations", clear
        convert_pop_for_prep
        rename (pop age) (wgt gbd_age)
        keep location_id year sex wgt gbd_age
        tempfile pop_wgts
        save `pop_wgts', replace

    // get location_ids
        import delimited using "$registry_id_table" , clear
        keep registry_id location_id
        tempfile location_ids
        save `location_ids', replace

** ****************************************************************
** SPLIT POPULATION IF NEEDED
**
** ****************************************************************
// Get data
    use "`pop_input_data'", clear

// Add location_id
    merge m:1 registry_id using `location_ids', keep(1 3) assert(2 3) nogen

// check for missing population. if population is missing, save and exit
    drop pop1
    egen pop1 = rowtotal(pop*), missing
    drop if pop1 == 0 | pop1 == .
    count
    if (r(N) == 1 & pop1 == .) | !r(N) {
        save_prep_step, process(4) pop("yes")
        exit, clear
    }

    // if data exists, edit the population data storage type
        recast double pop*

    // // Determine if age-splitting is needed on population
        preserve
            local age_split_pop = 0
            keep im_frmat frmat
            duplicates drop
            count if !inlist(frmat, 1, 2, 3, 131) | !inlist(im_frmat, 1, 2, 8, 9)
            if `r(N)' > 0 local age_split_pop = 1
        restore

        // replace missing data with zeros
            foreach n of numlist 3/6 23/26 91/94{
                capture gen pop`n' = 0
                replace pop`n' = 0 if pop`n' == .
            }

        // If data are present for "age unknown", then those data needs to be split. If data are present for "sex unknown" or "both sex", then those data needs to be split.
            count if pop26 != 0 & pop26 != .
            if r(N) local age_split_pop = 1
            count if !inlist(sex, 1, 2)
            if r(N) local age_split_pop = 1

        // determine if datset contains unknown sex
            count if inlist(sex, 3, 9)
            if r(N) > 0 local split_sex = 1
            else local split_sex = 0

        // Save if no split is needed
        if !`age_split_pop' & !`split_sex'{

            // Collapse remaining under5 and 80+ ages
            gen under5 = pop2 + pop3 + pop4+ pop5+ pop6 + pop91 + pop92 + pop93 + pop94
            gen eightyPlus =  pop22 + pop23 + pop24+ pop25
            replace pop2 = under5
            replace pop22 = eightyPlus
            drop pop3 pop4 pop5 pop6 pop23 pop24 pop25 pop26 pop91 pop92 pop93 pop94
            drop under5 eightyPlus

            // update frmat and im_frmat to reflect changes
            replace frmat = 131
            replace im_frmat = 9
            recast float pop*

            // Save
            sort registry_id sex year_start year_end

        // SAVE
            save_prep_step, process(4) pop("yes")
            exit, clear
        }
        // if split needed, save uid and current population total
        else{
            gen obs = _n
            preserve
                keep obs registry_id year_start year_end pop1
                rename pop1 preSplit_pop1
                tempfile uids
                save `uids', replace
            restore
            keep obs location_id year_start year_end sex frmat im_frmat pop*
            gen year = floor((year_start + year_end) / 2)
            drop year_start year_end
        }

** ******************
** Split Age
** *******************
    // save the dataset total for later comparison
        summ(pop1)
        local pre_split_pop = r(sum)

    // Split
        reshape long pop, i(obs location_id sex year frmat im_frmat) j(age)

        //
        foreach frmat_type in "im_frmat" "frmat" {
            // preserve a copy in case split cannot be performed
            preserve

            // merge with map and reshape
                if "`frmat_type'" == "im_frmat" merge m:1 `frmat_type' age using `im_frmat_map', keep(1 3)
                else if "`frmat_type'" == "frmat" merge m:1 `frmat_type' age using `frmat_map', keep(1 3)
                reshape long age_specific@, i(obs location_id year sex frmat im_frmat age pop need_split _merge) j(age_split_num)
                keep if (_merge == 1 & age_split_num == 1) | (_merge == 3 & age_specific != .)

            // mark those data that need to be split. skip to next format if no split is needed
                replace need_split = 0 if _merge == 1
                count if need_split == 1
                if !r(N) {
                    restore
                    continue
                }
                else restore, not

            // prepare age for merge
                gen gbd_age = (age_specific / 5) + 6 if _merge == 3
                replace gbd_age = 2 if _merge == 3 & age_specific == 0

            // Merge with population data
                rename _merge _merge1
                merge m:1 location_id year sex gbd_age using `pop_wgts', keep(1 3)
                egen wgt_tot = total(wgt), by(obs location_id year sex frmat im_frmat age)
                replace wgt = 1 if wgt_tot == 0
                egen wgt_scaled = pc(wgt), by(obs location_id year sex frmat im_frmat age) prop
                gen  orig_pop = pop
                replace pop = wgt_scaled * orig_pop if need_split == 1
                replace age = gbd_age if need_split == 1

            //collapse
                keep pop obs location_id year sex frmat im_frmat age
                collapse (sum) pop, by(obs location_id year sex frmat im_frmat age) fast
        }
        drop im_frmat frmat

    // Ensure that remaining under5 and 80+ ages are collapsed
        // Alert user if data remains in under1 age groups
            foreach n of numlist 91/94 {
                count if age == `n' & pop != 0
                if r(N) > 0 {
                    noisily di in red "Error in im_frmat. `metric' data still exist in `metric'`n' after age/sex split"
                    BREAK
                }
            }

        // combine under5 and 80+
        bysort obs sex: egen under5 = total(pop) if inlist(age, 2, 3, 4, 5, 6, 91, 92, 93, 94)
        replace pop = under5 if age == 2
        bysort obs sex: egen eightyPlus = total(pop) if inrange(age, 22, 25)
        replace pop = eightyPlus if age == 22
        drop if inlist(age, 3, 4, 5, 6, 23, 24, 25, 91, 92, 93, 94)
        drop under5 eightyPlus

    // // Split Age Unknown
        // mark if age unknown
            capture count if age == 26 & !inlist(pop, 0, .)
            if !r(N) local age_unknown_data = 0
            else {
                local age_unknown_data = 1
                preserve
                    keep obs location_id year sex age pop
                    keep if age == 26
                    drop age
                    rename pop unknown_age
                    tempfile unknown_metric
                    save `unknown_metric', replace
                restore
            }

        // Redistribute if age unknown
            drop if inlist(age, 1, 26)
            if `age_unknown_data' {
                merge m:1 obs location_id year sex using `unknown_metric', assert(3) nogen
                rename age gbd_age
                merge m:1 location_id year sex gbd_age using `pop_wgts', keep(1 3)
                rename gbd_age age
                egen total_wgt = total(wgt), by(obs location_id year sex)
                egen wgt_pc = pc(wgt), by(obs location_id year sex) prop
                replace wgt_pc = 1 if total_wgt == 0
                replace pop = 0 if pop == .
                replace pop = pop + unknown_age * wgt_pc
                keep obs location_id year sex age pop
                tempfile redistributed_unknown_metric
                save `redistributed_unknown_metric', replace
            }

    // // Verify Split
        preserve
        // Reshape wide and merge back with uids
            reshape wide pop, i(obs location_id year sex) j(age)
            drop year

            merge 1:1 obs using `uids', assert(3) nogen
            drop obs

        // Check for errors. If more than 3 population totals are greater than .0001% different from the original number, alert the user. (Below this fraction errors are likely due to rounding)
            egen postSplit_pop1 = rowtotal(pop2 - pop22)
            gen test_diff = abs(postSplit_pop1 - preSplit_pop1)
            capture count if test_diff > .000005*preSplit_pop1
            if r(N) > 3 {
                pause on
                display in red "Error: total population is not equivalent in all rows before and after split"
                pause
                pause off
            }
            drop preSplit_pop1 postSplit_pop1 test_diff

        // restore data if splitting sex
            if `split_sex' restore
            else restore, not

** ******************
** Split Sex Unknown
** *******************
    if `split_sex' {
    // save a copy of the data with no combined sex
        local has_unique_sex = 0
        count if !inlist(sex, 3, 9)
        if r(N) > 0 {
            local has_unique_sex = 1
            preserve
                keep if !inlist(sex, 3, 9)
                tempfile unique_sex
                save `unique_sex', replace
            restore
            keep if inlist(sex, 3, 9)
        }

    // format population weights
        preserve
            use `pop_wgts', clear
            reshape wide wgt, i(year location_id gbd_age) j(sex)
            gen wgt3 = wgt1 + wgt2
            gen w1 = wgt1/wgt3
            gen w2 = 1 - w1
            drop wgt*
            rename gbd_age age
            tempfile split_sex_wgts
            save `split_sex_wgts', replace
        restore
        keep obs location_id year age pop
        merge m:1 year location_id age using `split_sex_wgts', keep(1 3) nogen
        replace w1 = .5 if w1 == .
        replace w2 = .5 if w2 == .
        gen pop1 = pop * w1
        gen pop2 = pop * w2
        drop pop w*
        reshape long pop, i(obs location_id year age) j(sex)

    // merge with data for which sex was not split
        if `has_unique_sex' append using `unique_sex'

    // Reshape wide and merge back with uids
        reshape wide pop, i(obs location_id year sex) j(age)
        drop year

        merge m:1 obs using `uids', assert(3) nogen
        drop obs
    }

** ******************
** Finalize and Save
** *******************
    // Verify totals
        egen pop1 = rowtotal(pop*)
        summ(pop1)
        local post_split_pop = r(sum)
        if abs(`post_split_pop'-`pre_split_pop') > .0005*`pre_split_pop' {
            noisily di "ERROR: post-split total does not match pre-split total"
            if `generating_parameters' {
                pause on
                pause
            }
            else BREAK
        }

    // update frmat and im_frmat to reflect changes
        capture drop frmat im_frmat
        recast float pop*

    // Finalize
    if !`generating_parameters' {
        // restore data_set_name
        capture gen data_set_name = "`data_set_name'"

        // save
        save_prep_step, process(4) pop("yes")
    }
    else save "`temp_folder'/04_age_sex_split_inc_pop.dta", replace

** *************
** Split Pop Finished
** *************
