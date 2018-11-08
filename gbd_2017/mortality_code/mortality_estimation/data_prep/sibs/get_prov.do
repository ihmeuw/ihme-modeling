capture program drop get_prov
program define get_prov
    clear
    set mem 15g
    set maxvar 10000
    if c(os)=="Windows" {
         local prefix="FILEPATH"
         local homeprefix="FILEPATH"
        }
    if c(os)=="Unix" {
        local prefix="FILEPATH"
         local homeprefix="FILEPATH"
        }   
    syntax , [iso3(string) subnat(string) mapping(string) province(string) clear] 

    if "`province'" == "True" {
        ** Grab subnational map of counties to provinces
        run "FILEPATH"
        get_locations, subnat_only("`iso3'")
        preserve
            keep if level == 4
            keep ihme_loc_id location_id location_name
            rename location_id parent_id
            tempfile provs
            save `provs', replace
        restore 
        keep if level == 5
        keep ihme_loc_id parent_id
        rename ihme_loc_id county_id
        merge m:1 parent_id using `provs', nogen
        replace county_id = ihme_loc_id
        save `provs', replace

        // Grab the location IDs for subnationals
        import excel using "`mapping'", firstrow clear
        rename ihme_loc_id county_id
        merge 1:1 county_id using `provs', nogen
        drop parent_id
        tempfile master_sub
        save `master_sub'
    }
    else {
        import excel using "`mapping'", firstrow clear
        tempfile master_sub
        save `master_sub'
    }

end
