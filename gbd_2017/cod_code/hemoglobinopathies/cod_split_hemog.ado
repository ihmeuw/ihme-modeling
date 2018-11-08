
cap program drop cod_split_hemog
program define cod_split_hemog
    syntax, parent_cause_id(integer) root_dir(string)

    // Set OS flexibility
    set more off

    
    // load in programs

    run "FILEPATH/get_demographics.ado"
    run "FILEPATH/get_location_metadata.ado"
    run "FILEPATH/get_cod_data.ado"


   
    use "FILEPATH/cod_data_618.dta", clear

    // make temporary space
    cap mkdir "FILEPATH/tmp"
    if _rc {
        !rm -rf "FILEPATH/tmp"
        mkdir "FILEPATH/tmp"
    }
    mkdir "FILEPATH/tmp/scratch" 
    
    
    // save to temporary space
    forvalues i = 0/999 {
        gen draw_`i' = rate
    }
    drop rate
    save "FILEPATH/618_csmr.dta", replace

    // create folders for results
    import delimited using "`root_dir'/input_map.csv", clear
    levelsof cause_id, local(child_cause_ids) clean
    foreach cause of local child_cause_ids {
        mkdir "FILEPATH/`cause'"
    }
    
    local sex_ids 1 2
    
    foreach sex of local sex_ids {
        foreach cause of local child_cause_ids {
            cap mkdir "FILEPATH/`cause'/`sex'"
                if _rc {
                    !rm -rf "FILEPATH/`cause'/`sex'"
                    mkdir "FILEPATH/`cause'/`sex'"
                }
        }
    }   


// loop through demographics
    get_demographics, gbd_team("cod") gbd_round_id(5) clear
    local location_ids = r(location_id) 


    // submit jobs by location/year
    local jobs = ""
    foreach loc of local location_ids {
        local jobs = "`jobs'" + ",hemog_`loc'"
        !qsub -N "hemog_`loc'" -l mem_free=8 -pe multi_slot 4 -P "proj_custom_models" -o "FILEPATH" -e "FILEPATH" "FILEPATH/stata_shell.sh" "`root_dir'/split_loc_year.do" "root_dir(`root_dir') parent_cause_id(`parent_cause_id') location_id(`loc')"
        
        
    }
    local jobs = subinstr("`jobs'",",","",1)


end

cod_split_hemog, parent_cause_id(613) root_dir("FILEPATH/hemog_splits_cod")
