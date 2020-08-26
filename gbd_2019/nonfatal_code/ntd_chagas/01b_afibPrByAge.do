* do FILEPATH/ntd_chagas/01b_afibPrByAge.do
    
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
    
    tempfile ages ageSex

*** CREATE CONNECTION STRING TO SHARED DATABASE ***
    create_connection_string, database(ADDRESS)
    local shared = r(conn_string)


*** PULL AGE GROUP DATA ***
    get_age_metadata, age_group_set_id(12) gbd_round_id(6)
    rename age_group_years_start age_start
    rename age_group_years_end age_end
    drop age_group_weight_value
    save `ages', replace
    
    expand 2, generate(sex_id)
    replace sex_id = sex_id + 1
    save `ageSex', replace
    
*** PULL IN AFIB OUTCOME DATA ***
    import delimited FILEPATH, bindquote(strict) case(preserve) clear 

*** SORT OUT AGES ***    
    replace age_start = round(age_start-2, 5)
    replace age_start = 95 if age_start>=95
    replace age_end   = age_start + 5

    collapse (sum) value_n*, by(value_chagas age* sex)
    rename sex sex_id
    encode sex_id, gen(sex_id_cat)
    
    merge m:1 age_start using `ages', assert(2 3) keep(3) nogenerate 
    egen age_mid = rowmean(age_start age_end)
    
    *** RUN MODEL ***
    glm value_nafib c.age_mid##c.age_mid i.sex_id_cat i.value_chagas, family(binomial value_ntotal) vce(bootstrap, reps(200)) irls
    
    predict prAfib, xb
    predict prAfibSe, stdp

*** CLEAN UP AND CREATE DRAWS ***    
    reshape wide prAfib* value_n*, i(age_group_id sex_id) j(value_chagas)

    * total where chagas is 1 is ntotal1
    gen chagasPrev = value_ntotal1 / (value_ntotal1 + value_ntotal0)
    gen prPop = value_nafib1 + (value_nafib1==0)

    forvalues i = 0/999 {
        quietly{
            local random = rnormal(0, 1)
            generate draw_`i' = exp(prAfib1 + (`random'*prAfibSe1)) - ((exp(prAfib0 + (`random'*prAfibSe0)) - chagasPrev * exp(prAfib1 + (`random'*prAfibSe1))) / (1 - chagasPrev))
            replace draw_`i' = draw_`i'  * rbeta(prPop * .4, prPop * .6)
            }
        }
    
*** SAVE OUTPUT ***
    sort  sex_id age_group_id 
    keep  sex_id age_group_id age_start age_end draw_*
    order sex_id age_group_id draw_*
    
    generate modelable_entity_id = ADDRESS
    generate outcome = "afib"
    
    save FILEPATH, replace
