* do FILEPATH/ntd_chagas/01c_digestPrByAge.do
    
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
    
    tempfile ages

*** CREATE CONNECTION STRING TO SHARED DATABASE ***
    create_connection_string, database(ADDRESS)
    local shared = r(conn_string)
    
    
*** PULL AGE GROUP DATA ***
    get_age_metadata, age_group_set_id(12) gbd_round_id(6)
    rename age_group_years_start age_start
    rename age_group_years_end age_end
    drop age_group_weight_value
    save `ages', replace

*** PULL IN DIGESTIVE OUTCOME DATA ***    
    import delimited FILEPATH, bindquote(strict) case(preserve) clear 
    
    collapse (sum) value_digest value_totaln, by(age_start)
    gen prdigest = value_digest/value_totaln


*** PREP AGES ***    
    generate age_end = age+9
    replace age_end = 85 if age_end==65

    append using `ages'
    replace age_end = 100 if age_end==125
    
    egen age_mid = rowmean(age_start age_end)
    sort age_mid

    
*** PREP DATA FOR MODELLING ***
    egen nDigest = total(value_digest)
    egen nTotal = total(value_totaln)
    gen totalPr = nDigest / nTotal
    local nTotal = nTotal in 1
    local totalPr = totalPr in 1

    gen rr = prdigest / totalPr


*** RUN MODEL ***
    nl gom3: rr age_mid
    predict g3
    replace g3 = 0 if age_mid<.1

    
*** CREATE DRAWS ***
    drop if missing(age_group_id)
    forvalues i = 0/999 {
        quietly {
            local temp = rbinomial(`nTotal', `totalPr') / `nTotal'
            generate draw_`i' = `temp' * g3
            replace draw_`i' =  0 if missing(draw_`i')
            }
        }


 *** SAVE OUTPUT ***
    keep age_group_id draw_*
    
    expand 2, generate(sex_id)
    replace sex_id = sex_id + 1
    
    expand 2, generate(modelable_entity_id)
    replace modelable_entity_id = modelable_entity_id + ADDRESS
    
     forvalues i = 0 / 999 {
        quietly {
        replace draw_`i' = draw_`i' * 0.6 if modelable_entity_id==ADDRESS
        replace draw_`i' = draw_`i' * 0.4 if modelable_entity_id==ADDRESS
        }
        }
    
    generate outcome = "digest_mild" if modelable_entity_id==ADDRESS
    replace  outcome = "digest_mod"  if modelable_entity_id==ADDRESS
    
     tostring sex_id, replace
     save FILEPATH, replace
