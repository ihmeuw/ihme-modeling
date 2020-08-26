* do FILEPATH/ntd_chagas/04aFirstSplit.do

* BOILERPLATE *
    clear all
    set maxvar 12000
    set more off
        
 if c(os) == "Unix" {
    local ADDRESS "FILEPATH"
    set odbcmgr ADDRESS
    }
    else if c(os) == "Windows" {
    local ADDRESS "FILEPATH"
    }

    run "FILEPATH"

    tempfile endemic
    
* CREATE DIRECTORIES * 
    local outDir FILEPATH
 
    tokenize "`outDir'", parse(/)
    while "`1'"!="" {
    local path `path'`1'`2'
    capture mkdir `path'
    macro shift 2
    }

    !rm -rf `outDir'/progress
    
    foreach subDir in inputs progress ADDRESS2 ADDRESS3 ADDRESS4 ADDRESS5 ADDRESS6 ADDRESS7 ADDRESS1{
    capture mkdir "`outDir'/`subDir'"
    }

    capture mkdir "`outDir/'/logs"
 

* BUILD LIST OF ENDEMIC LOCATIONS * 
    get_location_metadata, location_set_id(35) clear
    keep if is_estimate==1 
    generate endemic =(strmatch(lower(region_name), "*latin america*") | inlist(ihme_loc_id, "BLZ", "GUY", "SUR"))
    keep location_id endemic
    
    merge 1:m location_id using FILEPATH, nogenerate    // Latin American migrants
    keep if inlist(location_id, 98,99)

    levelsof location_id if endemic==1, local(endemicLocations) clean
    levelsof location_id if missing(draw_0) & endemic!=1, local(zeroLocations) clean
    levelsof location_id if !missing(draw_0) & endemic!=1,    local(nonZeroLocations) clean
    
    preserve
    use FILEPATH clear    // chronic sequela proportion draws
    foreach location in `endemicLocations' `nonZeroLocations' `zeroLocations' {
    quietly save FILEPATH, replace
    di "." _continue
    }
    restore

 foreach location in `endemicLocations' {
        ! qsub -P ADDRESS -l m_mem_free=10G -l fthread=3 -l archive=True -l h_rt=01:00:00 -q long.q -N chagas_`location' -e FILEPATH -o FILEPATH "FILEPATH"
        sleep 100
        }
        
    foreach location in `nonZeroLocations' {
        preserve
        keep if location_id==`location'
        save `outDir'/inputs/migrantPrev_`location'.dta, replace
        restore
        ! qsub -P ADDRESS -l m_mem_free=7G -l fthread=1 -l archive=True -l h_rt=0:10:00 -q all.q -N chagas_`location' -e FILEPATH -o FILEPATH "FILEPATH"
        }
        

    foreach location of local zeroLocations {
    * zero script

    ! qsub -P ADDRESS -l m_mem_free=7G -l fthread=1 -l archive=True -l h_rt=0:42:00 -q long.q -N chagas_`location' -e FILEPATH -o FILEPATH "FILEPATH"
        sleep 100
        }

        log close 