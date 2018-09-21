** *****************************************************************************    **
** Author: NAME**
** Date created: August 20 2010                                                    **
** *****************************************************************************    **
clear
set more off
pause on
// set the prefix for whichever os we're in
    if c(os) == "Windows" {
        di in red "NO LONGER WORKS ON WINDOWS"
        BREAK
    }
    if c(os) == "Unix" { 
        global j ""
        global h ""
        set odbcmgr unixodbc
    }
    


 // Set up fast collapse
 do "PATH"
 
 // Pass on globals from master_prep.do
    global data_name "`2'"
    global timestamp "`3'"
    global tmpdir "`4'"
    global source_dir "`5'"
    global subdiv "`6'"
    local is_parallel_run 1
    
    local only_frmat_2 = 0
    assert `only_frmat_2' != 1
    local only_frmat_2_filename = "01b_frmat_2_split"
    local only_frmat_2_condition (frmat == 2 | frmat == 1) & sex != 9 & sex != 3

 // Make the temp directory for this particular dataset
    capture mkdir "$tmpdir/02_agesexsplit"    
    capture mkdir "$tmpdir/02_agesexsplit/${data_name}"    
    global temp_dir "$tmpdir/02_agesexsplit/${data_name}"
    
 // Other macros to make the code work
    global wtsrc acause
    global codmod_level acause
    global weight_dir "PATH"
    
    //set logging and write dir on scratch
    local out_dir "PATH" // this is what the parallel master code uses

    // set code directory
    local code_dir "PATH"

    // set input directory
    local frmat_key_dir "PATH"

 // log our efforts
    log using "PATH", replace

    display "data name: $data_name"
    display "timestamp: $timestamp"
    display "tmpdir: $tmpdir"
    display "source_dir: $source_dir"
    
// Only prep the pop file locally. When running in parallel, there should already be a pop from a local run.
// If this happens during parallel runs, there are read/write/corrupt file errors.
if `is_parallel_run' != 1 {
    // Prep the pop_file
        do "`code_dir'/prep/code/dem_wide.do"
        ** Temporary!!! we only split using the national population distribution
        * keep if location_id == .
        ** we don't need env for age-sex splitting
        drop env*
        ** Simulate previous years for pre 1970
        drop if year < 1970
        if strmatch("$data_name","_Australia*") | "$data_name" == "ICD7A" | "$data_name" == "ICD8A" {
            forval y = 1907/1969 {
                expand 2 if year == 1970, gen(exp)
                replace year = `y' if exp == 1
                drop exp
            }
        }
        save "$temp_dir/pop_prepped.dta", replace
}

    // check and makesure the pop file has the 70s, demographics needs them for 10 year mean covariates.
    use "$temp_dir/pop_prepped.dta", replace
    count if year > 1969 & year < 1980
    assert `r(N)' > 0

    //Save causes file
    do "`code_dir'/prep/code/get_restrictions.do"
    tempfile causes
    save `causes', replace
//here
 // Bring in the data
     ** 02a_before_agesex may be used for other datasets besides just India_CRS in the future
    if inlist("${data_name}", "India_CRS", "India_SCD_states_rural") {
        use "$source_dir/$data_name/data/intermediate/02a_before_agesex.dta", clear
    } 
    else {
        use "$source_dir/$data_name/data/intermediate/01_mapped.dta", clear
    }

    ** see documentation for this local for explanation
    if `only_frmat_2' == 1 {
        tempfile all_mapped_data
        save `all_mapped_data'
        // ** we want keep non-detail infant ages in the file, but don't want to split them
        // ** so use the fact that the infant data to split is determined as "im_frmat > 2"
        // assert im_frmat > 0
        // ** then we can change this back later
        // replace im_frmat = -1*im_frmat
        ** only 80+, 85+ will be split, and not sex (simulating the "good" data from GBD2015)
        keep if `only_frmat_2_condition'
    }

    // hack to get deaths23 out of frmat 2
    replace deaths22 = deaths23 + deaths22 if frmat == 2
    replace deaths23 = 0 if frmat == 2

    keep if subdiv=="$subdiv"

 // Do the thing
    count if frmat!=0 | im_frmat>2 | sex==9 | sex==. | sex==3
    if `r(N)'>0 {

    // "do" the ado-files for age splitting, agesex splitting, and storing summary statistics ///
    // (this just loads them into memory so we can use them later)
    quietly do "`code_dir'/age_sex_splitting/code/agesexsplit.ado"
    quietly do "`code_dir'/age_sex_splitting/code/agesplit.ado"
    quietly do "`code_dir'/age_sex_splitting/code/summary_store.ado"


    // erase any old agesplit and agesexsplit files so that are not appended with the new age and sex split files
        ** call any remaining old age and sex split files in the temp directory and store in the local split_files
        local split_files: dir "$temp_dir" files "${data_name}_ages*", respectcase 

        ** loop through the old split files and erase 
        foreach i of local split_files {
            capture erase "$temp_dir/`i'"
        }

        
    // create vectors to store checks along the way
    local maxobs 10000
    global currobs 1
    mata: stage = J(`maxobs', 1, "")
    mata: sex = J(`maxobs', 1, .)
    mata: sex_orig = J(`maxobs', 1, .)
    mata: frmat = J(`maxobs', 1, .)
    mata: frmat_orig = J(`maxobs', 1, .)
    mata: im_frmat = J(`maxobs', 1, .)
    mata: im_frmat_orig = J(`maxobs', 1, .)
    mata: deaths_sum = J(`maxobs', 1, .)
    mata: deaths1 = J(`maxobs', 1, .)
            
    // recalculate deaths1 so the first set of summary statistics will be accurate
    capture drop deaths1
    aorder 
    egen deaths1 = rowtotal(deaths3-deaths94)
    
    
    ** // store preliminary values for deaths_sum and deaths1 before making any other changes to the dataset
    preserve
        aorder
        egen deaths_sum = rowtotal(deaths3-deaths94)
        summary_store, stage("beginning") currobs(${currobs}) storevars("deaths_sum deaths1") sexvar("sex") frmatvar("frmat") im_frmatvar("im_frmat")
    restore
        
    // clean up formatting
    
        ** combine deaths91 and deaths92 and change im_frmat to 2 where relevant
        ** do this because we don't need to keep 0 days (deaths91) and 1-6 days (deaths92) separate
        egen tmp = rowtotal(deaths91 deaths92)
        replace deaths91 = tmp
        replace deaths92 = 0
        drop tmp
        replace im_frmat = 2 if im_frmat == 1

        replace deaths2 = deaths91 if deaths2==0 & im_frmat==8
        replace deaths91 = deaths2 if im_frmat == 8
        foreach var of varlist deaths92 deaths93 deaths94 {
            replace `var' = 0 if im_frmat == 8
        }

        ** for im_frmat = 9, add on deaths3 since im_frmat = 9 is for 0-4 years combined
        replace deaths91 = deaths2 + deaths3 if im_frmat == 9
        
        ** recalculate deaths1
        drop deaths1
        aorder 
        egen deaths1 = rowtotal(deaths3-deaths94)
        
        ** make sure deaths1 = deaths26
        replace deaths26 = deaths1 if frmat == 9
        
        ** drop deaths2! we don't need it because we don't use "age 0" for anything once the ages have been split
        drop deaths2
        
        // get rid of extraneous observations without deaths (this is where we'll drop codmod observations;
        // they'll be appended back on at the end)
        aorder
        capture drop tmp
        egen tmp = rowtotal(deaths*) 
        gen keep = 1 if source == "Other_Maternal" & inlist(iso3, "BRA", "KEN", "ZAF") & regexm(source_label, "DHS")
        drop if tmp == 0 & keep != 1
        drop tmp keep
        
        // fix sex in case it wasn't in the prep code
        replace sex=9 if sex==3 | sex==.

    // check that frmat and im_frmat are designated for all observations
    count if frmat == .
    if `r(N)' != 0 {
        di in red "WARNING: frmat variable is missing values.  Splitting will not work."
        pause
    }
    count if im_frmat == .
    if `r(N)' != 0 {
        di in red "WARNING: im_frmat variable is missing values.  Splitting will not work."
        pause
    }
        
    // check that we actually need to do agesexsplitting; only run the rest of the code if there are observations that need to be split
    // (NOTE: We have to run the lines up to this point for all datasets no matter what because we need to change frmat 1 to frmat 2
    // and make sure that frmats are designated for all datsets.
    count if (sex != 1 & sex != 2) | frmat != 0 | im_frmat != 2 
     if `r(N)' != 0 { 
        // record the total number of deaths in original, un-split file
            capture drop deathsorig_all
            egen deathsorig_all = rowtotal(deaths3-deaths94)

                
            // make a tempfile of the entire dataset so far
            noisily display "Saving allfrmats tempfile"
            tempfile allfrmats
            save `allfrmats', replace

        // store a summary of the deaths_sum and deaths1 at this stage
        egen deaths_sum = rowtotal(deaths3-deaths94)
        summary_store, stage("allfrmats") currobs(${currobs}) storevars("deaths_sum deaths1") sexvar("sex") frmatvar("frmat") im_frmatvar("im_frmat")
        drop deaths_sum

            
        // prepare data for splitting
            ** rename deaths variables to be deaths_1, deaths_5, deaths_10, etc
            rename deaths3 deaths_1
            rename deaths26 deaths_99
            rename deaths91 deaths_91
            rename deaths93 deaths_93
            rename deaths94 deaths_94
            forvalues i = 7/25 {
                local j = (`i'-6)*5
                rename deaths`i' deaths_`j'
            }
            ** because the causes in our weights are codmod causes, rename the "cause" variable here.
            ** we'll switch it back after splitting is done.
            rename cause cause_orig
            
            ** Map to splitting cause (level 3)
                preserve
                    use `causes', clear
                    ** keep if cause_version==2 
                    keep cause_id path_to_top_parent level acause yld_only yll_age_start yll_age_end male female
                    ** make sure age restriction variables are doubles, not floats
                    foreach var of varlist yll_age_start yll_age_end {
                        recast double `var'
                        replace `var' = 0.01 if `var' > 0.009 & `var' < 0.011
                        replace `var' = 0.1 if `var' > 0.09 & `var' < 0.11
                    }
                    ** drop the parent "all_cause"
                    levelsof cause_id if level == 0, local(top_cause)
                    drop if path_to_top_parent =="`top_cause'"
                    replace path_to_top_parent = subinstr(path_to_top_parent,"`top_cause',", "", .)    
                    ** make cause parents for each level
                    rename path_to_top_parent agg_
                    split agg_, p(",")
                    ** take the cause itself out of the path to parent, also map the acause for each agg
                    rename acause acause_orig
                    rename cause_id cause_id_orig
                    forvalues i = 1/5 {
                        rename agg_`i' cause_id
                        destring cause_id, replace
                        merge m:1 cause_id using `causes', keepusing(acause) keep(1 3) nogen
                        rename acause acause_`i'
                        rename cause_id agg_`i'
                        tostring agg_`i', replace
                        replace agg_`i' = "" if level == `i' | agg_`i' == "."
                        replace acause_`i' = "" if level == `i'
                        order acause_`i', after(agg_`i')
                    }
                    rename acause_orig acause
                    rename cause_id_orig cause_id
                    compress
                    tempfile all
                    save `all', replace
                restore
                
            **    make an acause which is the level 3 version of acause if acause is 4 or 5
                merge m:1 acause using `all', keep(1 3) keepusing(level acause_3) nogen
                gen cause = acause
                forvalues i = 4/5 {
                    replace cause = acause_3 if level == `i'
                }
                drop level acause_*
            ** there are too many variables that could possibly be identifying variables in these datasets so generate an 'id' 
            ** variable for use in reshapes
            gen id_variable = iso3 + string(location_id) + string(year) + string(region) + dev_status + string(sex) + cause + cause_orig + acause + source + source_label + source_type + string(national) + subdiv + string(im_frmat) + string(frmat) + string(NID)
            capture replace id_variable = id_variable + string(subnational)
            
            ** load in frmat keys containing information about whether the frmat/age combination needs to be split
                // im_frmats
                preserve
                    insheet using "`frmat_key_dir'/frmat_im_key.csv", comma clear
                    tempfile frmat_im_key
                    save `frmat_im_key', replace
                restore
                
                // other frmats
                preserve
                    insheet using "`frmat_key_dir'/frmat_key.csv", comma clear
                    tempfile frmat_key
                    save `frmat_key', replace
                restore
                
                // frmat 9
                preserve
                    insheet using "`frmat_key_dir'/frmat_9_key.csv", comma clear
                    tempfile frmat_9_key
                    save `frmat_9_key', replace
                restore
            
            ** save all data, post-prep
            noisily display "Saving allfrmats_prepped tempfile"
            tempfile allfrmats_prepped
            save `allfrmats_prepped', replace

            ** make a tempfile of the observations that need to be **age-split only**, with age long
                // keep only the bad formats - we shouldn't ever have im_frmat or frmat missing, but include those checks just in case.
                // exclude sex == 9, because that's dealt with in age-sex-splitting.
                assert frmat != .
                assert im_frmat != .
                keep if (frmat > 0 | im_frmat > 2) & sex != 9

                // drop the age groups we don't need
                capture drop deaths4 deaths5 deaths6 deaths1 deaths92
                
                // reshape to get age long for infants, adults, and frmat 9 separately because it takes too long to do it all at once
                    ** reshape im_frmats
                    preserve
                        // reduce to non-standard im_frmats, excluding them if frmat == 9 (deaths_99 == 0 | deaths_99 == .), 
                        // because we'll deal with these later
                        keep if (im_frmat > 2) & (deaths_99 == 0 | deaths_99 == .) & frmat != 9
                        
                        if `only_frmat_2' == 1{
                            ** not splitting infants, then drop everything
                            assert im_frmat != .
                            keep if im_frmat == .
                        }
                        // we only care about infant deaths, not the other ages or frmat 9
                        aorder
                        drop deaths_1 deaths_5 deaths_10 deaths_15 deaths_20 deaths_25 deaths_30 deaths_35 deaths_40 deaths_45 deaths_50 deaths_55 deaths_60 deaths_65 deaths_70 deaths_75 deaths_80 deaths_85 deaths_90 deaths_95 deaths_99
                        
                        // make age long    
                        reshape long deaths_, i(id_variable) j(age)
                        
                        // merge with key from above to keep only the frmats and age groups that need to be split
                        merge m:1 im_frmat age using `frmat_im_key', keep(3) nogenerate
                        keep if needs_split == 1
                        
                        // save for later
                        tempfile ims_age
                        save `ims_age', replace
                    restore

                    
                    ** now reshape the bad adult frmats
                    preserve
                        // reduce to non-standard frmats, excluding them if frmat == 9 (deaths_99 == 0  deaths_99 == .), 
                        // because we'll deal with these later
                        keep if (frmat > 0) & (deaths_99 == 0 | deaths_99 == .)
                        
                        // we only care about adult deaths, not infant or frmat 9
                        drop deaths_91 deaths_93 deaths_94 deaths_99
                        
                        // make age long
                        reshape long deaths_, i(id_variable) j(age)
                        
                        // merge with key from above to keep only the frmats and age groups that need to be split
                        merge m:1 frmat age using `frmat_key', keep(3) nogenerate
                        keep if needs_split == 1
                        
                        // save for later
                        tempfile adults_age
                        save `adults_age', replace
                    restore
                    
                    ** lastly, reshape frmat = 9
                    preserve
                        // reduce to frmat 9 deaths
                        keep if (deaths_99 != 0 & deaths_99 != .)
                        
                        // we only care about frmat 9 deaths (deaths_99)
                        aorder
                            drop deaths_1 deaths_5 deaths_10 deaths_15 deaths_20 deaths_25 deaths_30 deaths_35 deaths_40 deaths_45 deaths_50 deaths_55 deaths_60 deaths_65 deaths_70 deaths_75 deaths_80 deaths_85 deaths_90 deaths_95 deaths_91 deaths_93 deaths_94
                        
                        // make age long
                        reshape long deaths_, i(id_variable) j(age)
                        
                        // ensure that the frmat is properly labeled
                        replace frmat = 9
                        
                        // merge with key from above to keep only the frmats and age groups that need to be split
                        merge m:1 frmat age using `frmat_9_key', keep(3) nogenerate
                        keep if needs_split == 1
                        
                        // save for later
                        tempfile frmat9_age
                        save `frmat9_age', replace
                    restore
                            
                    // append these tempfiles together - now we have a reshaped dataset with age long
                    clear
                    use `ims_age'
                    append using `adults_age'
                    append using `frmat9_age'
                    rename deaths_ deaths
                        
                    // save what we have so far - this will be the file that gets age-split
                    noisily display "Saving $temp_dir/temp_to_agesplit_$subdiv.dta"
                    save "$temp_dir/temp_to_agesplit_$subdiv.dta", replace
                    
                    // store a summary of the deaths_sum at this stage
                    rename deaths deaths_sum
                    summary_store, stage("temp_to_agesplit_${subdiv}") currobs(${currobs}) storevars("deaths_sum") sexvar("sex") frmatvar("frmat") im_frmatvar("im_frmat")
                    rename deaths_sum deaths

            ** make a tempfile of the observations that need to be **age-sex-split OR sex-split only**, with age long 
            ** (sex is already long)
                // open file with all data, after prepping
                use `allfrmats_prepped', clear

                // keep only the observations that need age-sex-splitting or sex-splitting
                keep if sex == 9

                // drop the age groups we don't need
                capture drop deaths4 deaths5 deaths6 deaths1 deaths92
                
                // reshape to get age long for infants, adults, and frmat 9 separately because it takes too long to do it all 
                // at once
                    ** reshape im_frmats
                    preserve
                        // exclude observations if frmat == 9 (deaths_99 == 0 | deaths_99 == .), because we'll deal with 
                        // these later
                        keep if (deaths_99 == 0 | deaths_99 == .) & frmat != 9
                        
                        // we only care about infant deaths, not the other ages or frmat 9
                        aorder
                        drop deaths_1 deaths_5 deaths_10 deaths_15 deaths_20 deaths_25 deaths_30 deaths_35 deaths_40 deaths_45 deaths_50 deaths_55 deaths_60 deaths_65 deaths_70 deaths_75 deaths_80 deaths_85 deaths_90 deaths_95 deaths_99
                        
                        // make age long            
                        reshape long deaths_, i(id_variable) j(age)
                        
                        // merge with key from above to get values for age_start and age_end
                        merge m:1 im_frmat age using `frmat_im_key', keep(3) nogenerate
                        // save for later
                        tempfile ims_agesex
                        save `ims_agesex', replace
                    restore
                            
                    ** now reshape the bad adult frmats
                    preserve
                        // exclude observations if frmat == 9 (deaths_99 == 0  deaths_99 == .), because we'll 
                        // deal with these later
                        keep if (deaths_99 == 0 | deaths_99 == .) & frmat != 9
                        
                        // we only care about adult deaths, not infant or frmat 9
                        drop deaths_91 deaths_93 deaths_94 deaths_99
                        
                        // make age long
                        reshape long deaths_, i(id_variable) j(age)
                        
                        // merge with key from above to get values for age_start and age_end
                        merge m:1 frmat age using `frmat_key', keep(3) nogenerate
                        
                        // save for later
                        tempfile adults_agesex
                        save `adults_agesex', replace
                    restore
                    
                    ** lastly, reshape frmat = 9
                    preserve
                        // reduce to frmat 9 deaths
                        keep if (deaths_99 != 0 & deaths_99 != .)
                        
                        // we only care about frmat 9 deaths (deaths_99)
                        aorder
                        drop deaths_1-deaths_91
                        
                        // make age long
                        reshape long deaths_, i(id_variable) j(age)
                        
                        // ensure that the frmat is properly labeled
                        replace frmat = 9
                        
                        // merge with key from above to keep only the frmats and age groups that need to be split
                        merge m:1 frmat age using `frmat_9_key', keep(3) nogenerate
                        
                        // save for later
                        tempfile frmat9_agesex
                        save `frmat9_agesex', replace
                    restore
                        
                // append these tempfiles together - now we have a reshaped dataset with age long
                clear
                use `ims_agesex'
                append using `adults_agesex'
                append using `frmat9_agesex'
                rename deaths_ deaths
                // save what we have so far - this will be the file that gets age-sex-split
                noisily display "Saving $temp_dir/temp_to_agesexsplit_$subdiv.dta"
                save "$temp_dir/temp_to_agesexsplit_$subdiv.dta", replace
                
                
                // store a summary of the deaths_sum at this stage
                rename deaths deaths_sum
                
                // Make isopop iso3
                gen isopop = iso3
                
                summary_store, stage("temp_to_agesexsplit_$subdiv_long_name_test") currobs(${currobs}) storevars("deaths_sum") sexvar("sex") frmatvar("frmat") im_frmatvar("im_frmat")
                rename deaths_sum deaths

                        
        // run the actual splitting code!
            ** age-splitting only
            noisily display _newline "Beginning agesplitting"
            
                
                // open file for age-splitting
                use "$temp_dir//temp_to_agesplit_$subdiv.dta", clear
                
                // create a local to hold the $codmod_level causes in this dataset
                capture levelsof cause, local(causes)
                
                ** log using "$temp_dir//debug_agesplit.log", replace

                // IF there are observations in this dataset, loop through each cause and split the ages for that cause
                if ! _rc {
                    foreach c of local causes {
                        noisily di in red "Splitting cause `c'"
                            agesplit, splitvar("deaths") splitfil("$temp_dir/temp_to_agesplit_$subdiv.dta") outdir("$temp_dir") weightdir("$weight_dir") mapfil("$iso3_dir/countrycodes_official.dta") dataname("${data_name}_$subdiv") cause("`c'") wtsrc("$wtsrc")
                        // see the documentation in agesplit.ado for more details on the syntax of this command
                    }
                }

            
            ** age-sex-splitting and sex-splitting only
            noisily display _newline "Beginning agesexsplitting"
            
                // open file for age-sex-splitting
                use "$temp_dir/temp_to_agesexsplit_$subdiv.dta", clear
                
                // create a local to hold the $codmod_level causes in this dataset
                capture levelsof cause, local(causes)
                
                // IF there are observations in this dataset, loop through each cause and split the ages for that cause 
                // i.e when the return code == 0, the capture executed the levelsof command 
                if ! _rc {
                    foreach c of local causes {
                        noisily di in red "Splitting cause `c'" 
                        agesexsplit, splitvar("deaths") splitfil("$temp_dir/temp_to_agesexsplit_$subdiv.dta") ///
                            outdir("$temp_dir") weightdir("$weight_dir") mapfil("$iso3_dir/countrycodes_official.dta") ///
                            /* popfil("$pop_fil")*/ dataname("${data_name}_$subdiv") cause("`c'") wtsrc("$wtsrc")
                        // see the documentation in agesexsplit.ado for more details on the syntax of this command
                    }
                }

            
        // compile the split files!
            ** prepare a dataset for appending
            clear
            set obs 1
            gen split = ""

            ** be more explicit about which files to loop over; then there aren't too many of them
            foreach cause of local causes {
                local f "$temp_dir/${data_name}_${subdiv}_agesplit_`cause'.dta"
                cap confirm file `f'
                if _rc == 0 {
                    append using `f'
                }
            }

            ** mark that these observations have come from the age-splitting code
            replace split = "age"
            

            * ** loop through the age-sex-split files to combine them
            foreach cause of local causes {
                local f "$temp_dir/${data_name}_${subdiv}_agesexsplit_`cause'.dta"
                cap confirm file `f'
                if _rc == 0 {
                    append using `f'
                }
            }
            
            ** mark that these observations have come from the age-sex-splitting code
            replace split = "age-sex" if split == ""
            
            ** mark observations with missing population
            gen nopop_adult = (nopop_10 == 1)
            gen nopop_im = (nopop_1 == 1)
            gen nopop_frmat9 = ((nopop_adult == 1 | nopop_im == 1) & frmat == 9)
            drop nopop_1-nopop_95
            
            ** rename the deaths variables so that they are in WHO format
            rename deaths deaths_orig_split
            rename deaths_1 deaths3
            forvalues i = 5(5)95 {
                local j = (`i'/5) + 6
                rename deaths_`i' deaths`j'
            }
            renpfix deaths_ deaths
            
            ** record original frmats (note: original sexes were recorded in the agesexsplitting algorithm)
            gen frmat_orig = frmat
            gen im_frmat_orig = im_frmat

            ** reset frmat for successfully split observations
            replace frmat = 0 if (needs_split == 1 | sex_orig == 9) & nopop_adult != 1 & nopop_frmat9 != 1     
                // population exists, so adults have been split
            replace im_frmat = 2 if (needs_split == 1 | sex_orig == 9) & nopop_im != 1 & nopop_frmat9 != 1 & `only_frmat_2' != 1
                // population exists, so infants have been split
            
            ** rename cause and cause_orig back to their original names
            drop cause
            rename cause_orig cause

            ** generate a deaths92, deaths4-6, and deaths23-26 since we have those in the pre-age split file `allfrmats'
            generate deaths92 = 0
            forvalues i = 4/6 {
                generate deaths`i' = 0
            }
            
            ** cleanup
            drop if cause == "" & year == .

            ** save compiled split files
            tempfile splits
            save `splits', replace
            noisily display _newline "Saving splits tempfile"
            
            ** store a summary of the deaths_sum at this stage
            preserve
            aorder
            egen deaths_sum = rowtotal(deaths3-deaths94)
            summary_store, stage("splits") currobs(${currobs}) storevars("deaths_sum") sexvar("sex_orig") frmatvar("frmat_orig") im_frmatvar("im_frmat_orig")
            restore
            
        // combine split data with frmats and age groups that did not need to be split
        // NOTE: we will be merging the split files with `allfrmats', using update to replace missing values in the split files 
        // with the values from `allfrmats'.  This means that we need to be careful about exactly which entries in deaths* 
        // are missing and which are zero.
            ** prepare file with split data
            ** NOTE: We need to save a temporary copy of the deaths variables so that we can deal with
            ** frmats that cross the adult/infant divide
                // zero out all deaths* entries that are missing so that they're not accidentally updated 
                // during the merge.  Also create a copy for use in frmats that cross the adult/infant divide
                foreach var of varlist deaths* {
                    replace `var' = 0 if `var' == .
                    gen `var'_tmp = `var'
                }
                
                // number each observation within each group, so we know which entry to replace later on
                bysort iso3 region dev_status location_id NID subdiv source *national *frmat* subdiv list sex year cause acause split: egen obsnum = seq()
                
                // fix deathsorig_all
                // we need to zero out all but one of these entries, so that it doesn't multiply when we collapse (sum) later        
                replace deathsorig_all = 0 if obsnum != 1
                
                // for individual frmats, change to missing one observation for each deaths category that didn't need to 
                // be split. as with deaths_orig_all, we're only changing one observation because we don't want it to 
                // multiply when we collapse
                    ** establish the databases that mark entries that need to be changed to missing as "9999"
                    preserve
                        insheet using "`frmat_key_dir'/frmat_replacement_key.csv", comma clear
                        tempfile replacement_key
                        save `replacement_key', replace
                    restore
                    
                    preserve
                        insheet using "`frmat_key_dir'/frmat_im_replacement_key.csv", comma clear
                        tempfile replacement_im_key
                        save `replacement_im_key', replace
                    restore

                    ** merge on original frmat, split, and obsnum to make sure that only the first observation in each 
                    ** set is replaced with 9999, and only the proper age groups for each frmat are marked.  NOTE that 
                    ** by merging on split (= "age") as well, we're only doing these replacements for observations that 
                    ** have only been age-split.  For age-sex-splitting or sex-splitting, all the post-split observations 
                    ** need to stay as they are.
                    merge m:1 frmat_orig split obsnum using `replacement_key', update replace
                    drop if _m == 2
                    drop _m

                    merge m:1 im_frmat_orig split obsnum using `replacement_im_key', update replace
                    drop if _m == 2
                    drop _m
                    
                    ** FIX FRMATS AND IM_FRMATS THAT CROSS THE ADULT/INFANT DIVIDE
                        // fix deaths3 if im_frmat is 9
                        replace deaths3 = deaths3_tmp if im_frmat_orig == 9
                        
                        // fix infant deaths if frmat is 9
                        forvalues i = 91/94 {
                            replace deaths`i' = deaths`i'_tmp if frmat_orig == 9
                        }
                        
                        // fix infant deaths if im_frmat is 10
                        local adult_deaths 3 7 8
                        foreach i of local adult_deaths {
                            replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 10
                        }

                        // fix infant deaths if im_frmat is 11
                        local adult_deaths 3 7 8 9 10 11 12 13 14 15 16 17
                        foreach i of local adult_deaths {
                            replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 11
                        }
                        
                        // fix infant formats if im_frmat is 05
                        local adult_deaths 3
                        foreach i of local adult_deaths {
                            replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 5
                        }                        
                        // fix infant formats if im_frmat is 06
                        local adult_deaths 3
                        foreach i of local adult_deaths {
                            replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 6
                        }                        
                        // fix infant formats if im_frmat is 12
                        local adult_deaths 3 4
                        foreach i of local adult_deaths {
                            replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 12
                        }                        
                        // fix infant formats if im_frmat is 13
                        local adult_deaths 3
                        foreach i of local adult_deaths {
                            replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 13
                        }                        
                        // fix infant formats if im_frmat is 14
                        local adult_deaths 3
                        foreach i of local adult_deaths {
                            replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 14
                        }                        
                        // fix infant formats if im_frmat is 15
                        local adult_deaths 3 4
                        foreach i of local adult_deaths {
                            replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 15
                        }

                        // get rid of temporary deaths variables now that we've finished fixing frmats that 
                        // cross the infant/adult divide
                        foreach var of varlist deaths*tmp {
                            drop `var'
                        }
                    
                    ** change marked observations to be missing
                    foreach var of varlist deaths* {
                        replace `var' = . if `var' == 9999
                        if `only_frmat_2' == 1 {
                            ** in addition to above, make sure to replace with original for everything that was not 80+
                            replace `var' = . if !inlist("`var'", "deaths22", "deaths23", "deaths24", "deaths25")
                        }
                    }
                    
                // change to missing one observation for each deaths category that wasn't split because of missing 
                // population. as above, we're only changing one observation because we don't want it to multiply 
                // when we collapse.
                foreach var of varlist deaths91-deaths3 {
                    replace `var' = . if nopop_im == 1 & obsnum == 1
                }
                
                foreach var of varlist deaths7-deaths24 {
                    replace `var' = . if nopop_adult == 1 & obsnum == 1
                }
                
                foreach var of varlist deaths91-deaths25 {
                    replace `var' = . if nopop_frmat9 == 1 & obsnum == 1
                }
                
                
            ** combine the split data with pre-split data, updating the split data for age groups that did not need 
            ** to be split.  NOTE that this needs to be done separately for age-split and age-sex-split observations, 
            ** since age-split obs need to be merged on sex, but we can't do this for age-sex-split obs.
                // age-split observations
                    ** prepare `allfrmats' to only include those observations which should have been age-split
                    preserve
                        // prepared data
                        use `allfrmats', clear
                        drop if sex == 9
                        
                        gen frmat_orig = frmat
                        gen im_frmat_orig = im_frmat

                        noisily display "Saving allfrmats_ageonly tempfile"
                        tempfile allfrmats_ageonly
                        save `allfrmats_ageonly', replace
                        
                        // store a summary of the deaths_sum and deaths1 at this stage
                        aorder
                        egen deaths_sum = rowtotal(deaths3-deaths94)
                        summary_store, stage("allfrmats_ageonly") currobs(${currobs}) storevars("deaths_sum deaths1") sexvar("sex") frmatvar("frmat_orig") im_frmatvar("im_frmat_orig")
                    restore
                    
                    ** merge with age-split observations
                    preserve
                        // run the merge
                        drop if sex_orig == 9
                        merge m:1 iso3 year sex cause acause *frmat_orig subdiv location_id NID source source_label source_type using `allfrmats_ageonly', update
                        noisily display "Saving postsplit_ageonly tempfile"
                        tempfile postsplit_ageonly
                        save `postsplit_ageonly', replace
                        
                        // store a summary of the deaths_sum and deaths1 at this stage
                        aorder
                        egen deaths_sum = rowtotal(deaths3-deaths94)
                        summary_store, stage("postsplit_ageonly") currobs(${currobs}) storevars("deaths_sum deaths1") sexvar("sex") frmatvar("frmat_orig") im_frmatvar("im_frmat_orig")
                    restore
                
                    // age-sex-split and sex-split observations
                        ** prepare `allfrmats' to only include those observations which should have been age-sex-split
                        preserve
                            // prepare data
                            use `allfrmats', clear
                            keep if sex == 9
                            
                            gen frmat_orig = frmat
                            gen im_frmat_orig = im_frmat
                            
                            noisily display "Saving allfrmats_agesexonly tempfile"
                            tempfile allfrmats_agesexonly
                            save `allfrmats_agesexonly', replace
                            
                            // store a summary of the deaths_sum and deaths1 at this stage
                            aorder
                            egen deaths_sum = rowtotal(deaths3-deaths94)
                            summary_store, stage("allfrmats_agesexonly") currobs(${currobs}) storevars("deaths_sum deaths1") ///
                                sexvar("sex") frmatvar("frmat_orig") im_frmatvar("im_frmat_orig")
                            rename deaths_sum deaths
                        restore
                            
                    ** ALL observations where sex_orig == 9 should have been split, except when there wasn't population data.
                    ** In this case, we'll need to have multiple observations for the same iso3 year cause; one for each sex for which
                    ** we have data (split or not split)
                        // nopop_im
                        preserve
                            ** determine which obs from the splitfiles weren't split because of nopop_im (we'll deal with nopop for frmat 9 later)
                            keep if sex_orig == 9
                            keep if nopop_im == 1 & nopop_frmat9 != 1
                            
                            ** for these iso3/year/causes, merge with pre-split data so we can recover the deaths
                            keep iso3 location_id NID source source_label source_type year cause cause_name list nopop* national subdiv dev_status region
                            capture duplicates drop
                            merge 1:m iso3 location_id NID source source_label source_type year cause cause_name list national subdiv dev_status region using `allfrmats_agesexonly', keep(3) nogenerate
                            
                            ** zero out everything except the infant deaths that couldn't be split
                            aorder
                            foreach var of varlist deaths1-deaths26 {
                                replace `var' = 0
                            }
                            
                            ** save file
                            tempfile nopop_im
                            save `nopop_im', replace
                        restore
                        
                        // nopop_adult
                        preserve
                            ** determine which obs from the splitfiles weren't split because of nopop_adult (we'll deal 
                            ** with nopop for frmat 9 later)
                            keep if sex_orig == 9
                            keep if nopop_adult == 1 & nopop_frmat9 != 1
                            
                            ** for these iso3/year/causes, merge with pre-split data so we can recover the deaths
                            keep iso3 location_id NID source source_label source_type year cause cause_name list nopop* national subdiv dev_status region
                            capture duplicates drop
                            merge 1:m iso3 location_id NID source source_label source_type year cause cause_name list national subdiv dev_status region using `allfrmats_agesexonly', keep(3) nogenerate
                            
                            ** zero out everything except the adult deaths that couldn't be split
                            aorder
                            foreach var of varlist deaths91-deaths94 {
                                replace `var' = 0
                            }
                            
                            ** save file
                            tempfile nopop_adult
                            save `nopop_adult', replace
                        restore
                        // nopop_frmat9
                        preserve
                            ** determine which split data wasn't split because of either nopop_im or nopop_adult in frmat 9
                            keep if sex_orig == 9
                            keep if nopop_frmat9 == 1
                            
                            ** for these iso3/year/causes, merge with pre-split data so we can recover the deaths
                            keep iso3 location_id NID source source_label source_type year cause cause_name nopop*
                            capture duplicates drop
                            merge 1:m iso3 location_id NID source source_label source_type year cause cause_name using `allfrmats_agesexonly', keep(3) nogenerate
                            
                            ** nothing was split successfully, so don't zero out any deaths in this case
                            aorder
                            
                            ** save file
                            tempfile nopop_frmat9
                            save `nopop_frmat9', replace
                        restore
                        
                        // append these files together so we maintain all the deaths for sex-split observations
                        keep if sex_orig == 9
                        append using `nopop_im'
                        append using `nopop_adult'
                        append using `nopop_frmat9'
                        noisily display "Saving postsplit_agesexonly tempfile"
                        tempfile postsplit_agesexonly
                        save `postsplit_agesexonly', replace
                        
                        // store a summary of the deaths_sum and deaths1 at this stage
                        aorder
                        egen deaths_sum = rowtotal(deaths3-deaths94)
                        summary_store, stage("postsplit_agesexonly") currobs(${currobs}) storevars("deaths_sum deaths1") ///
                            sexvar("sex") frmatvar("frmat_orig") im_frmatvar("im_frmat_orig")
                                
                // combine both age-split and age-sex-split observations
                use `postsplit_ageonly', clear
                append using `postsplit_agesexonly'

            ** since we have multiple observations per group, collapse to get a total count
                // first make sure that the proper things are set to missing; if the original im_frmat was 3, we 
                // don't have any information about deaths94; set it missing
                replace deaths94 = . if im_frmat_orig == 3 & frmat_orig != 9
            
                // collapse sums up missing entries to make 0, so we need to mark the groups where all the observations
                // for a variable are missing
                    ** create a group variable rather than always sorting on all these variables
                    egen group = group(iso3 region dev_status location_id NID subdiv source* *national ///
                        frmat im_frmat subdiv list sex year cause acause), missing
                    
                    ** loop through variables, creating a miss_`var' variable that records whether all the observations within
                    ** the group have missing for the variable `var'
                    foreach var of varlist deaths* {
                        bysort group (`var'): gen miss_`var' = mi(`var'[1])
                    }
                
                // do the collapse, retaining the value for miss_`var'
                collapse (sum) deaths* (mean) miss* nopop*, by(iso3 location_id subdiv source* NID *national frmat im_frmat list sex year cause cause_name acause region dev_status)
                
                // use miss_`var' to inform which variables need to be reverted back to missing
                foreach var of varlist deaths* {
                    di "`var'"
                    replace `var' = . if miss_`var' != 0
                }
                drop miss*
            
            
        // final adjustments and formatting
            ** we might have some observations that don't contain any information (if frmat == 9 & lacking population numbers),
            ** so drop these now
            capture drop tmp
            egen tmp = rowtotal(deaths91-deaths25 deaths26), missing
            drop if tmp == .
            drop tmp

            ** if deaths were in deaths26 when the rest of the obs was in a frmat other than frmat 9, there may be remaining
            ** deaths in deaths26.  We'll RD them using codmod-level age proportions.
                // first create country-year-sex-age proportions by codmod cause
                aorder
                capture drop deaths_known
                
                // find out how many deaths we've ended up with after splitting
                egen deaths_known = rowtotal(deaths3-deaths25 deaths91-deaths94)
                ** replace deaths_known = round(deaths_known)

                capture drop totdeaths
                bysort iso3 location_id year sex $codmod_level source* NID: egen totdeaths = total(deaths_known)
                
                foreach i of numlist 3/25 91/94 {
                    bysort iso3 location_id year sex $codmod_level source* NID: egen num`i' = total(deaths`i')
                    generate codmodprop`i' = num`i'/totdeaths
                
                    ** now redistribute the remaining deaths26
                    generate new_deaths`i' = deaths`i' + (deaths26*codmodprop`i')
                
                    ** replace deaths = newdeaths where frmat is still 9
                    replace deaths`i' = new_deaths`i' if frmat == 9
                }
            ** we don't have any deaths for some country-year-sex-codmod groups (eg A13 in Thailand males 1958), 
            ** so we'll just use global averages
                // first mark those obs where we weren't able to split up deaths26 because that country/year/sex/$codmod_level 
                // group doesn't have any deaths (they will have numtot = 0)
                aorder
                egen numtot = rowtotal(num*)
                    
                // also replace deaths with 0 where theyr'e missing
                foreach i of numlist 3/25 91/94 {
                    replace deaths`i' = 0 if deaths`i' == .
                }
                
                bysort $codmod_level: egen totknown = total(deaths_known)
                foreach i of numlist 3/25 91/94 {
                    bysort $codmod_level: egen totnum`i' = total(deaths`i')
                    generate totprop`i' = totnum`i'/totknown
                    generate othernew`i' = deaths`i' + (deaths26*totprop`i')
                    replace deaths`i' = othernew`i' if frmat == 9 & numtot == 0
                }
                
                // do some final cleanup
                //replace deaths23 = deaths23_tmp if frmat != 9
                //drop deaths23_tmp
                replace frmat = 2 if frmat == 9
                replace deaths26 = 0    
            

            ** recalculate deaths1
            aorder
            capture drop tmp
            egen tmp = rowtotal(deaths3-deaths94)
            replace deaths1 = tmp
            drop tmp
        
        **                
        // warn user if nopop
        count if nopop_adult != 0 & nopop_adult != .
        if r(N) > 0 {
            preserve
            local numobs = r(N)
            egen totadult = rowtotal(deaths3-deaths25)
            summ totadult if nopop_adult != 0 & nopop_adult != .
            noisily di in red "WARNING: Missing adult population numbers.  " ///
                "`numobs' observations and `r(sum)' deaths not split for adults.  "
            keep if nopop_adult != 0 & nopop_adult != .
            keep iso3 location_id year
            duplicates drop
            sort iso3 year
            noisily di in red "The following country-years are missing adult population numbers:"
            noisily list
            restore
        }

        count if nopop_im != 0 & nopop_im != .
        if r(N) > 0 {
            preserve
            local numobs = r(N)
            summ deaths91 if nopop_im != 0 & nopop_im != .
            noisily di in red "WARNING: Missing infant population numbers.  " ///
                "`numobs' observations and `r(sum)' deaths not split for infants.  "
            keep if nopop_im != 0 & nopop_im != .
            keep iso3 location_id year
            duplicates drop
            sort iso3 year
            noisily di in red "The following country-years are missing infant population numbers:"
            noisily list
            restore
        }

        count if nopop_frmat9 != 0 & nopop_frmat9 != .
        if r(N) > 0 {
            preserve
            keep if nopop_frmat9 != 0 & nopop_frmat9 != .
            keep iso3 location_id year
            duplicates drop
            sort iso3 year
            noisily di in red "WARNING: Missing population numbers AND frmat 9.  Big problems. (Deaths in deaths26 have been lost.)  The following country-years are problematic:"
            noisily list
            restore
        }


        // drop variables we don't need
        drop nopop* *orig* codmodprop* num* new_deaths* tot* other* deaths_known

        // store a summary of the deaths_sum and deaths1 and export to .csv
        preserve
            egen deaths_sum = rowtotal(deaths3-deaths94)
            summary_store, stage("end") currobs(${currobs}) storevars("deaths_sum deaths1") sexvar("sex") frmatvar("frmat") im_frmatvar("im_frmat")

            ** export to Stata dataset
            clear
            getmata stage sex sex_orig deaths_sum deaths1 frmat frmat_orig im_frmat im_frmat_orig
            drop if stage == ""
            
            ** calculate useful comparisons
                gen stage_diff = deaths_sum - deaths1
                replace stage_diff = . if regexm(stage, "postsplit")
                
        restore
    }
    else {
        noisily display "All observations are in the desired age and sex formats.  No splitting required."
    }
}
        if "${data_name}"=="Morocco_Health_In_Figures" {
            replace deaths9=0 if source_label=="deaths_people_15_64"
            order deaths*, last sequential
            drop deaths1
            egen deaths1 = rowtotal(deaths3-deaths94)
            order deaths*, last sequential
        }
            
        // Save dataset
        compress
        fastcollapse deaths*, by(iso3 location_id year sex acause cause cause_name NID source source_label source_type list national subdiv region dev_status im_frmat frmat) type(sum)

        ** see documentation for the only_frmat_2 local at top of file for more info
        if `only_frmat_2' == 1 {
            tempfile split_frmat_2
            save `split_frmat_2'
            use `all_mapped_data', clear
            drop if `only_frmat_2_condition'
            append using `split_frmat_2'
            save "`out_dir'/`only_frmat_2_filename'.dta", replace
            save "`out_dir'/_archive/`only_frmat_2_filename'_${timestamp}.dta", replace
        }
        ** this is the normal/main process. In every case for a production upload run, this is the code that runs
        else {
            save "`out_dir'/02_agesexsplit_$subdiv.dta", replace
            save "`out_dir'/_archive/02_agesexsplit_$subdiv_${timestamp}.dta", replace
            
            // Save dataset for compile
            collapse(sum) deaths*, by(iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status im_frmat frmat) fast
            save "`out_dir'/02_agesexsplit_$subdiv_compile.dta", replace
            save "`out_dir'/_archive/02_agesexsplit_$subdiv_compile_${timestamp}.dta", replace
            // US counties are not needed after redistribution. Aggregate to state level and save dataset for compile. 
            preserve
                odbc load, exec("SELECT location_id, location_parent_id FROM shared.location WHERE location_parent_id BETWEEN 523 and 573 OR location_parent_id = 385") dsn(prodcod) clear
                tempfile states
                save `states', replace
            restore

            if inlist(source, "US_NCHS_counties_ICD9", "US_NCHS_counties_ICD10") {
                merge m:1 location_id using `states', keep(3) nogen
                ** Aggregate
                fastcollapse deaths*, by(NID acause dev_status frmat im_frmat iso3 list national region sex source source_label source_type subdiv year location_parent_id) type(sum)
                rename location_parent_id location_id
                
                save "`out_dir'/02_agesexsplit_compile_states.dta", replace
                save "`out_dir'/_archive/02_agesexsplit_compile_states_${timestamp}.dta", replace
            }

        }
        
        
        capture log close
