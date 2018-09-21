cap program drop add_loc_hierarchy
program define add_loc_hierarchy, rclass
    syntax, location_ids(numlist) year_ids(numlist)

    run "$functions_dir/fastcollapse.ado"
    run "$functions_dir/create_connection_string.ado"

    create_connection_string, database(FILEPATH)
    local con = r(conn_string)

    preserve

        // Load location levels
         # delim ;
        odbc load, exec("
            SELECT lhh.level,lhh.location_id,lhh.location_name,lhh.parent_id,lhh.location_type,
            lhh.most_detailed,(SELECT GROUP_CONCAT(location_id)
                FROM shared.location_hierarchy_history
                WHERE location_set_version_id = shared.active_location_set_version(35,4) and parent_id = lhh.location_id) AS child_
            FROM shared.location_hierarchy_history lhh
            WHERE lhh.location_set_version_id = shared.active_location_set_version(35,4)
            GROUP BY lhh.location_id, lhh.location_name, lhh.parent_id, lhh.location_type,
            lhh.most_detailed ORDER BY lhh.sort_order") `con' clear ;
        # delim cr
        keep if child_!=""
        qui summ level
        local max_loc `r(max)'
        tempfile loc_level
        save `loc_level', replace

        // Load location levels for SDI
        # delim ;
        odbc load, exec("
            SELECT lhh.level,lhh.location_id,lhh.location_name,lhh.parent_id,lhh.location_type,
            lhh.most_detailed,(SELECT GROUP_CONCAT(location_id)
                FROM shared.location_hierarchy_history WHERE location_set_version_id = shared.active_location_set_version(40,4)
                and parent_id = lhh.location_id) AS child_
            FROM shared.location_hierarchy_history lhh
            WHERE lhh.location_set_version_id = shared.active_location_set_version(40,4) GROUP BY lhh.location_id, lhh.location_name,
            lhh.parent_id, lhh.location_type, lhh.most_detailed
            ORDER BY lhh.sort_order") `con' clear ;
        # delim cr
        keep if child_!=""
        qui summ level
        local max_sdi `r(max)'
        tempfile sdi_level
        save `sdi_level', replace

    restore
    merge m:1 location_id year_id age_group_id sex_id using "$tmp_dir/pops_$rei_id.dta", keep(1 3) nogen keepusing(pop_scaled)
    tempfile data
    save `data', replace

    //aggregate up normal location hierarchy
    foreach x of numlist `max_loc'(-1)0 {
        use if level==`x' using `loc_level', clear
        count
        if `r(N)'==0 continue
        forvalues i=1/`r(N)' {
            use if level==`x' using `loc_level', clear
            levelsof child_ in `i', local(keep) c
            levelsof location_id in `i', local(parent) c
            use if inlist(location_id,`keep') using `data', clear
            count
            if `r(N)'==0 continue
            bysort year_id age_group_id sex_id rei_id: egen weight = pc(pop_scaled), prop
            foreach sev of varlist sev* {
                qui replace `sev' = `sev' * weight
            }
            fastcollapse sev*, type(sum) by(year_id age_group_id sex_id rei_id)
            gen location_id=`parent'
            ** merge on pop
            merge m:1 location_id year_id age_group_id sex_id using "$tmp_dir/pops_$rei_id.dta", keep(1 3) nogen keepusing(pop_scaled)
            append using `data'
            save `data', replace
        }
    }
    drop pop_scaled

    //aggregate up SDI hierarchy
    merge m:1 location_id year_id age_group_id sex_id using "$tmp_dir/pops_$rei_id.dta", keep(1 3) nogen keepusing(pop_scaled)
    save `data', replace
    foreach x of numlist `max_sdi'(-1)0 {
        use if level==`x' using `sdi_level', clear
        count
        if `r(N)'==0 continue
        forvalues i=1/`r(N)' {
            use if level==`x' using `sdi_level', clear
            levelsof child_ in `i', local(keep) c
            levelsof location_id in `i', local(parent) c
            clear
            set obs 1
            gen loc = "`keep'"
            split loc, p(,) destring
            drop loc
            gen n = 1
            reshape long loc, i(n) j(num)
            levelsof loc, local(keep) c
            use `data', clear
            run "FILEPATH/tag.ado"
            tag location_id, ifin("`keep'") keep
            count
            if `r(N)'==0 continue
            bysort year_id age_group_id sex_id rei_id: egen weight = pc(pop_scaled), prop
            foreach sev of varlist sev* {
                qui replace `sev' = `sev' * weight
            }
            fastcollapse sev*, type(sum) by(year_id age_group_id sex_id rei_id)
            gen location_id=`parent'
            ** merge on pop
            merge m:1 location_id year_id age_group_id sex_id using "$tmp_dir/pops_$rei_id.dta", keep(1 3) nogen keepusing(pop_scaled)
            append using `data'
            save `data', replace
        }
    }
    drop pop_scaled

    local f = "$tmp_dir/loc_results_$rei_id.dta"
    save `f', replace
    return local loc_file_path `f'
end
// END
