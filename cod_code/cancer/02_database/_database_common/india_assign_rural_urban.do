*****************************************
** Description: Assigns rural/urban status to India data
** Input(s)/Output(s): USER (directly adjusts active data)
** How To Use: called by */02_database scripts

*****************************************
// accept arguments
    args metrics

// make map of new location_ids
    preserve
        import delimited using "$common_cancer_data/modeled_locations.csv", varnames(1) clear
        keep if parent_id == 163 | location_id == 163
        keep location_id location_name ihme_loc_id
        gen urbanicity = "_urban" if regexm(lower(location_name), "urban")
        replace urbanicity = "_rural" if regexm(lower(location_name), "rural")
        duplicates drop
        tempfile new_lids
        save `new_lids', replace
    restore

// make urbanicity weights
    preserve
        // load unique fraction entries
        use "$registry_input_storage/_documentation/IND_Documentation/urban_rural_fractions.dta", clear
        keep registry_id fraction_urban
        duplicates drop

        // format fractions
        capture count if fraction_urban == .
        assert !r(N)
        gen fraction_rural = 1 - fraction_urban
        reshape long fraction, i(registry_id) j(urbanicity) string

        // save
        tempfile india_fractions
        save `india_fractions', replace
    restore

// merge urbanicity with the dataset
    merge m:1 location_id using `new_lids', keep(1 3)
    count if country_id == 163 & _merge != 3
    assert !r(N)
    drop _merge

// // split data
    preserve
        // Keep data that need to be split
            keep if country_id == 163 & !inlist(urbanicity, "_urban", "_rural")
            drop urbanicity location_id ihme_loc_id

        // generate entry number for later comparison
            gen entry = _n

        // join data with weights (this will create two instances of each datapoint, each joined by a different weight)
            joinby registry_id using `india_fractions'

        // recalculate numbers
            foreach m of varlist `metrics' {
                recast double `m'
                gen orig_`m' = `m'
                replace `m' = fraction*`m'
                bysort entry: egen test_new_`m' = total(`m')
                bysort entry: egen test_old_`m' = mean(orig_`m')
                gen test = abs(test_new_`m'/ test_old_`m')
                count if test != . & abs(1 - test) > 0.00005
                assert !r(N)
                drop test* orig_`m'
            }
            drop fraction entry

        // correct metadata
            replace location_name = location_name + ", " + upper(substr(urbanicity, 2, 1)) + substr(urbanicity, 3, .)
            merge m:1 location_name using `new_lids', keep(1 3) assert(2 3) nogen
            count if !inlist(urbanicity, "_urban", "_rural")
            assert !r(N)
            replace registry_id = registry_id + urbanicity
            drop urbanicity location_name
            gen split_data = 1
            tempfile reaggregated_india_data
            save `reaggregated_india_data', replace
    restore

// Combine with the rest of the data those locations for which data are not already present
    drop urbanicity location_name
    gen not_split = 1
    append using `reaggregated_india_data'
    bysort location_id year sex acause: egen has_data = total(not_split)
    gen extra_data = 1 if country_id == 163 & split_data == 1 & has_data != 0
    drop if extra_data == 1
    drop extra_data has_data not_split split_data

** *******************
** End Subroutine
** ******************
