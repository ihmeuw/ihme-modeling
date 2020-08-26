* do FILEPATH/ntd_chagas/03c_immigrantsByAgeSex.do

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
    run FILEPATH
    run FILEPATH
    run FILEPATH
    
    tempfile ages appendTemp


*** CREATE CONNECTION STRING TO SHARED DATABASE ***
    noisily di "{hline}" _n _n "Connecting to the database"
        
    create_connection_string, database(ADDRESS)
    local shared = r(conn_string)
    
    
*** PULL AGE GROUP DATA ***
    get_age_metadata, age_group_set_id(12) gbd_round_id(6)
    rename age_group_years_start age_start
    rename age_group_years_end age_end
    drop age_group_weight_value
    generate mergeId = age_group_id
    replace  mergeId = 5 if age_group_id<5
    replace  mergeId = 32 if age_group_id==235

    save `ages'


*** PULL & PROCESS IMMIGRANT AGE/SEX PATTERN DATA *** 
    import delimited FILEPATH, clear    // USA-age-sex-migrants
    
    rename per_fb_male pctAgeSex1
    rename per_fb_female pctAgeSex2
    
    gen mergeId = _n + 4
    replace mergeId = 30 if mergeId==21
    replace mergeId = 31 if mergeId==22
    replace mergeId = 32 if mergeId==23
    collapse (sum) pctAgeSex*, by(mergeId) 
    
    
*** DETERMINE PROPORTIONS BY GBD AGE GROUPS ****
    replace pctAgeSex1 = pctAgeSex1 * (age_end - age_start) / 5 if mergeId==5
    replace pctAgeSex2 = pctAgeSex2 * (age_end - age_start) / 5 if mergeId==5
    replace pctAgeSex1 = pctAgeSex1 / 2 if mergeId==32
    replace pctAgeSex2 = pctAgeSex2 / 2 if mergeId==32
    keep age_group_id pctAgeSex1 pctAgeSex2
    
    reshape long pctAgeSex, i(age_group_id) j(sex_id)
    replace pctAgeSex = pctAgeSex / 100

    
*** SAVE ***
    save FILEPATH, replace
    export delimited FILEPATH, replace
