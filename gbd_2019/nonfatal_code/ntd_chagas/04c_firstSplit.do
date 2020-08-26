
/******************************************************************************\
             SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
    clear all
    set maxvar 12000
    set more off


* PULL IN LOCATION_ID AND ENDEMICITY CATEGORY FROM BASH COMMAND *
 local location "`1'"
 local endemic    "`2'"

* START LOG *
    capture log close
    log using FILEPATH, replace


* LOAD SHARED FUNCTIONS *
    run FILEPATH
    run FILEPATH
    run FILEPATH
    run FILEPATH
    run FILEPATH

* SET UP LOCALS WITH OUTPUT DIRECTORY, MODELABLE ENTITY IDS AND AGE GROUPS *
    get_demographics, gbd_team(epi) clear
    local ages `r(age_group_id)'

    local meid_master        ADDRESS
    local meid_acute         ADDRESS
    local meid_afib          ADDRESS
    local meid_hf            ADDRESS
    local meid_asymp         ADDRESS
    local meid_total         ADDRESS
    local meid_digest_mild   ADDRESS
    local meid_digest_mod    ADDRESS

    local outDir FILEPATH

    tempfile appendTemp mergeTemp prevalence incidence chronic acute



/******************************************************************************\
                        PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/

    * PULL IN DRAWS FROM DISMOD MODELS FOR INCIDENCE AND PREVALENVCE *
        if `endemic'==0 {
        use FILEPATH, clear    // migrant previous locaton
        generate measure_id = 5
        rename draw_* prev_*
        }

        else if inlist(`location', 98, 99) {
        do FILEPATH/ntd_chagas/04d_elimination.do "`location'"
        }

        else {
        get_draws, gbd_id_type(modelable_entity_id) gbd_id(`meid_master') source(epi) location_id(`location') age_group_id(`ages') measure_id(5) decomp_step(iterative) status(best) clear
        rename draw_* prev_*
        }

        capture drop modelable_entity_id
        save `mergeTemp'

        preserve
        rename prev_* draw_*
        keep location_id year_id sex_id age_group_id measure_id draw_*
        save `prevalence'
        restore

        tostring sex_id, replace
        joinby age_group_id sex_id using FILEPATH    // chronic proportion

    * PERFORM DRAW-LEVEL CALCULATIONS *
        forvalues i = 0 / 999 {
            quietly {
                replace draw_`i' = draw_`i' * prev_`i'
                replace draw_`i' = 0 if draw_`i'==0 | missing(draw_`i')
                }
            di "." _continue
            }

     levelsof modelable_entity_id, local(meids) clean
     foreach meid of local meids {
         export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using FILEPATH if modelable_entity_id==`meid', replace
         }

     keep if measure_id==5
     fastcollapse draw_*, by(location_id year_id sex_id age_group_id) type(sum)
     forvalues i = 0 / 999 {
        quietly replace draw_`i' = -1 * draw_`i'
        }
     save `chronic'

    get_draws, gbd_id_type(modelable_entity_id) decomp_step(iterative) gbd_id(`meid_master') source(epi) location_id(`location') age_group_id(`ages') measure_id(6) status(best) clear

        if inlist(`location', 98, 99) | `endemic'==0 {
        if `location' == 98 local eYear 1999
        else if `location' == 99 local eYear 1997
        else local eYear 1980

        forvalues i = 0 / 999 {
            quietly replace draw_`i' = 0    if year_id>=`eYear'
            }
        }


        save `incidence'


    expand 2, gen(newObs)
    replace measure_id = measure_id - newObs

        forvalues i = 0 / 999 {
     quietly {
        local prAcute = rbeta(10, 190)
        replace draw_`i' = draw_`i' * `prAcute' * 6/52 if measure_id==5
        }
        }

    keep location_id year_id sex_id age_group_id measure_id draw_*

    tempfile

    generate modelable_entity_id = `meid_acute'
    export delimited using FILEPATH, replace

    keep if measure_id==5
    forvalues i = 0 / 999 {
        quietly replace draw_`i' = -1 * draw_`i'
        }
    save `acute'

    clear
    append using `prevalence' `incidence'

    replace modelable_entity_id = `meid_total'

    export delimited location_id year_id sex_id age_group_id modelable_entity_id measure_id draw_* using FILEPATH, replace

    keep if measure_id==5
    append using `acute'
    tostring sex_id, replace
    append using `chronic'

    fastcollapse draw_*, by(location_id year_id sex_id age_group_id) type(sum)
    forvalues i = 0 / 999 {
        quietly replace draw_`i' = 0 if draw_`i' < 0
        }
    generate measure_id = 5
    generate modelable_entity_id = `meid_asymp'

    export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using FILEPATH, replace

    file open progress using FILEPATH.txt, text write replace
    file write progress "complete"
    file close progress

    log close

