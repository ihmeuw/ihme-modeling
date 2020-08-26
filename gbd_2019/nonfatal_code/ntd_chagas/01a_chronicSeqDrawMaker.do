* do FILEPATH/ntd_chagas/01a_chronicSeqDrawMaker.do

*** BOILERPLATE ***
    clear
    set more off
    
    if c(os) == "Unix" {
        local ADDRESS FILEPATH
        }
    else {
        local ADDRESS FILEPATH
        }

    run FILEPATH
    run FILEPATH
    run FILEPATH
    run FILEPATH
    
    tempfile mergeTemp appendTemp

    
*** CREATE CONNECTION STRING TO SHARED DATABASE ***
    create_connection_string, database(ADDRESS)
    local shared = r(conn_string)
    
*** PULL AGE GROUP DATA ***
    get_age_metadata, age_group_set_id(12) gbd_round_id(6)
    rename age_group_years_start age_start
    rename age_group_years_end age_end
    drop age_group_weight_value
    expand 2, generate(sex_id)
    replace sex_id = sex_id + 1
    tostring sex_id, replace
    save `mergeTemp'
    local dataDir FILEPATH

*** RUN SCRIPTS TO CREATE OUTCOME DRAWS & APPEND ***
    local i = 2
    foreach outcome in afib digest hf {
        do FILEPATH
        
        use `mergeTemp', clear
        merge 1:m age_group_id sex_id using `dataDir'/`outcome'FILEPATH, nogenerate
        
        if "`outcome'"!="afib" append using `appendTemp'
        save `appendTemp', replace
        
        local ++i
        }
        

    foreach var of varlist draw_* {
        quietly replace `var' = 0 if missing(`var')
        }

order  modelable_entity_id outcome age_group_id age_start age_end sex_id

save FILEPATH, replace
export delimited FILEPATH, replace
