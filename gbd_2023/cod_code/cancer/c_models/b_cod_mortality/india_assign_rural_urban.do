** *****************************************************************************
** Description: Assigns rural/urban status to India data
** Input(s)/Output(s): N/A (directly adjusts active data)
** How To Use: called by */02_database scripts
** Contributors: INDIVIDUAL_NAME
** *****************************************************************************
pause on

// accept arguments
    args metrics

    tempfile data
    save `data', replace

// make map of location_ids for India, with a column to indicate for each row whether it's for the
// urban or rural part of the state
    preserve
        import delimited using "$common_cancer_data/modeled_locations.csv", varnames(1) clear
        keep if country_id == 163
        keep location_id location_name ihme_loc_id
        gen urbanicity = "_urban" if regexm(lower(location_name), "urban")
        replace urbanicity = "_rural" if regexm(lower(location_name), "rural")
        duplicates drop
        tempfile india_loc_ids
        save `india_loc_ids', replace
    restore

// make map of output registry_ids and make urbanicity weights
  // load a list of locations, registry ids, and the fraction
  // (proportion) of that registry that we'll attribute to the urban part (the
  // rest we attribute to rural)

  use "FILEPATH", clear
  keep registry_index fraction_urban
  duplicates drop
   
   // format fractions
   capture count if fraction_urban == .
   assert !r(N)
   // calculate the rural fraction as 1 - urban
   gen fraction_rural = 1 - fraction_urban
    // pivot from wide to long, i.e., from columns for rural and urban, make rows for rural and urban
    reshape long fraction, i(registry_index) j(urbanicity) string

    // save the fractions
    tempfile india_fractions
    save `india_fractions', replace
    use `data', clear  // 'data' is the data that was in memory when this code file was invoked; it has cases and population, according to the parameters passed in

    // now we're going to split the rows into India and non India
    preserve
        keep if country_id != 163
        export delimited using "FILEPATH", replace
        tempfile india02_non_india_data
        save `india02_non_india_data', replace
    restore

    keep if country_id == 163
    export delimited using "$temp_folder/india03_india_data.csv", replace

// merge urbanicity with the dataset
    merge m:1 location_id using `india_loc_ids', keep(1 3)
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
            joinby registry_index using `india_fractions'
            count if fraction == . 
            assert r(N) == 0

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
            merge m:1 location_name using `india_loc_ids', keep(1 3) assert(2 3) nogen
            count if !inlist(urbanicity, "_urban", "_rural")
            assert !r(N)
            replace registry_index = substr(registry_index, 5, .)
            replace registry_index = "163." + string(location_id) + "." +  substr(registry_index, strpos(registry_index,".") + 1, .)
            drop urbanicity location_name
            gen split_data = 1
            tempfile reaggregated_india_data
            save `reaggregated_india_data', replace
    restore

// Combine with the rest of the data those locations for which data are not already present
    drop urbanicity location_name
    gen not_split = 1
    append using `reaggregated_india_data'
    bysort location_id year_id sex acause: egen has_data = total(not_split)
    gen extra_data = 1 if country_id == 163 & split_data == 1 & has_data != 0
    drop if extra_data == 1
    drop extra_data has_data not_split split_data

//  add back in the non-India data that we removed at the start
    append using `india02_non_india_data'

//  save the data in case it's needed for vetting
export delimited using "FILEPATH", replace

** ***************************************************************************** 
** End Subroutine
** *****************************************************************************