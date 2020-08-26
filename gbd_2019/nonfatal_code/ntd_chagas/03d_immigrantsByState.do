* do FILEPATH/ntd_chagas/03d_immigrantsByState.do

*** BOILERPLATE ***
    clear
    set more off
    
    if c(os) == "Unix" {
        local ADDRESS "FILEPATH"
        }
        
    else if c(os) == "Windows" {
        local ADDRESS "FILEPATH"
        }

    run FILEPATH



*** PULL & PROCESS DATA ON BREAKDOWN OF IMMIGRANTS BY US STATE ***
    import delimited FILEPATH, clear    // state by birth
    
    replace tpop_foreign_born = subinstr( tpop_foreign_born , "," , "", .)
    destring tpop_foreign_born, replace
    
 * 52 is the total value
 * changing to 0 due to stata issue with types 
 
    foreach var of varlist rob_mexico rob_caribbean rob_central_america rob_south_america rob_middle_east {
      replace `var' =  "0" if `var' == "***"
      replace `var' = subinstr( `var' , "," , "", .)
      destring `var', replace force
      replace `var' = round(tpop_foreign_born * `var'[52] / tpop_foreign_born[52]) if `var' == 0
      }

    drop in 52
    
*** CONVERT TO PROPORTIONS BY REGION IN LATIN AMERICA ***    
    foreach var of varlist rob_mexico rob_caribbean rob_central_america rob_south_america  {
      egen pct`var' =  pc(`var')
      replace pct`var' = pct`var' / 100
      }  
      
    replace location_name = trim(location_name)  
    compress  

    tempfile usa
    save `usa'



*** GET LOCATION METADATA AND MERGE TO IMMIGRANT PROPORTIONS FROM ABOVE ***    
    get_location_metadata,  location_set_id(8) clear
    keep if parent_id==102
    keep location_id location_name ihme_loc_id
     
    merge 1:1 location_name using `usa', assert(1 3) keep(3) nogenerate



*** CLEAN UP AND SAVE *** 
    keep location* pct* ihme_loc_id
    reshape long pct, i(location* ihme_loc_id) j(mergeRegion) string

    save FILEPATH, replace
    export delimited FILEPATH, replace
