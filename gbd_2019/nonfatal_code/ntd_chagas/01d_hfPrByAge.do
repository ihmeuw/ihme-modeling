* do FILEPATH/ntd_chagas/01d_hfPrByAge.do
    
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
    
    tempfile ages appendTemp
    
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
    expand 2, generate(chagas)
    save `ages'


foreach sex in male female {
    foreach status in negative positive {
        import delimited FILEPATH, bindquote(strict) case(preserve) clear 
        rename id age
        rename v2 noHf
        rename v3 hf
        rename Total total

        gen sex_id = ("`sex'"=="female") + 1
        gen chagas = "`status'"=="positive"
        
        if "`sex'"=="female" | "`status'"=="positive" append using `appendTemp'
        save `appendTemp', replace
        }
    }

gen age_start = round(age-2, 5)
collapse (sum) noHf hf total, by(sex_id chagas age_start)
drop if missing(age_start)


merge 1:1 age_start sex chagas using `ages', assert(2 3) nogenerate
egen age_mid = rowmean(age_start age_end)

replace total = 1 if missing(total)
replace hf = 0 if age_start<20
gen prHf = hf / total

gen sexB = (sex_id - 1.5) * 2
gen chagasB = (chagas - 0.5) * 2

capture drop pred 
capture drop ageS*
mkspline ageS = age_mid, cubic knots(0 25 40) displayknots

export delimited FILEPATH, replace

glm hf i.sex_id i.chagas ageS*, family(binomial total) 
predict pred, xb

meglm hf sexB chagasB || age_mid:, family(binomial total)
predict se, stdp


keep pred se total age_group_id age_mid sex_id chagas prHf
reshape wide pred se total prHf, i(age_group_id age_mid sex_id) j(chagas)

gen prHf = prHf1 - prHf0
drop prHf?

forvalues i = 0/999 {
    local random = rnormal(0, 1)
    generate draw_`i' = invlogit(pred1 + (`random' * se1)) - invlogit(pred0 + (`random' * se0)) 
    }
    
    
egen mean =rowmean(draw_*)
egen lower = rowpctile(draw_*), p(2.5)
egen upper = rowpctile(draw_*), p(97.5)
export delimited FILEPATH, replace
save FILEPATH, replace

keep age_group_id sex_id draw_*
generate modelable_entity_id = ADDRESS
generate outcome = "hf"

tostring sex_id, replace
save FILEPATH, replace
