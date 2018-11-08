if c(os) == "Windows" {
    global j "J:"
    global h "H:"
}
if c(os) == "Unix" {
    global j "/home/j" 
    set odbcmgr unixodbc
    global h "~"
}

set varabbrev off

do "FILEPATH"

local in_dir "FILEPATH"
local data_dir "FILEPATH"
local out_dir "FILEPATH"

clear
set more off, perm
pause on
set mem 25g

** set options and validate them
local append_vr 1
local adjust_weight_data 0
local make_weights 0
local graph_weights_in_pdf 0

assert inlist(`append_vr', 0, 1)
assert inlist(`adjust_weight_data', 0, 1)
assert inlist(`make_weights', 0, 1)
assert inlist(`graph_weights_in_pdf', 0, 1)

assert `make_weights' == 1 if `graph_weights_in_pdf' == 1

assert `adjust_weight_data' == 1 if `make_weights' == 1 & `append_vr' == 1

** Prep the completeness weights
if `adjust_weight_data' == 1 {
    use "FILEPATH", clear

        gen sex_source_tag = substr(iso3_sex_source, 4, .)
        keep iso3 year sex_source_tag u5_comp_pred trunc_pred

        ** reconcile sex_source tag
        gen keep_tag = 0
        ** keep VR sources
        replace keep_tag = 1 if regexm(sex_source_tag, "_VR")
        replace keep_tag = 1 if year < 2004 & sex_source_tag == "_both_DSP-1996-2000"
        replace keep_tag = 1 if year >= 2004 & sex_source_tag == "_both_SSPC-DC"
        keep if keep_tag == 1
        fastcollapse u5_comp_pred trunc_pred, by(iso3 year) type(mean)
        egen double kid_comp = rowmean(u5_comp_pred trunc_pred)
        replace u5_comp_pred=1 if u5_comp_pred>1
        replace trunc=1 if trunc>1
        replace kid_comp=1 if kid_comp>1
        tempfil comp
        save `comp', replace
}
        
** Get population
if `adjust_weight_data' == 1 {
    do "FILEPATH"
    tempfile pop
    save `pop', replace
}

local use_artificial_old_age 0
assert inlist(`use_artificial_old_age', 0, 1)

local data_file_suffix ""
if `use_artificial_old_age' == 0 {
    local data_file_suffix "_hasold"
}

if `append_vr' == 1 {
    ** Bring in the datasets
    quietly {
        insheet using "FILEPATH", comma names clear
        levelsof vr_sources, local(data_sources)
        levelsof vr_sources if artificial_old_age_generated == 1, local(artificial_old_age_sources)
        noisily di "Confirming files exist ... "

        foreach data_source of local data_sources {
            local is_artificial_age_source 0
            foreach src of local data_sources {
                if "`data_source'" == "`src'" {
                    local is_artificial_age_source 1
                }
            }
            if `is_artificial_age_source' == 1 & `use_artificial_old_age' == 1 {
                confirm file "FILEPATH"
            }
            else {
                confirm file "FILEPATH"
            }
        }
        noisily di "Reading data..."
        foreach data_source of local data_sources {
            noisily di "     ... `data_source'"
            local is_artificial_age_source 0
            foreach src of local data_sources {
                if "`data_source'" == "`src'" {
                    local is_artificial_age_source 1
                }
            }
            if `is_artificial_age_source' == 1 & `use_artificial_old_age' == 1 {
                use "FILEPATH", clear
            }
            else {
                use "FILEPATH", clear
            }
            assert "`data_source'" != "ICD7A"
            if inlist("`data_source'", "_ICD7A", "ICD8A", "ICD9_BTL") {

      replace deaths23 = 0 if frmat == 1 & regexm(acause, "malaria")
                replace deaths24 = 0 if frmat == 1 & regexm(acause, "malaria")
                replace deaths25 = 0 if frmat == 1 & regexm(acause, "malaria")    
                
                replace frmat = 0 if frmat == 1 & regexm(acause, "malaria")
            }


                keep if frmat == 0 | (im_frmat == 1 | im_frmat == 2)
                drop if frmat==9
            tempfile tmp_`data_source'
            save `tmp_`data_source'', replace
        }
        clear
        noisily di "Appending data..."
        foreach data_source of local data_sources {
            noisily di "     ... `data_source'"
            append using `tmp_`data_source''
        }
    }
        
        drop if iso3=="ZAF"
        
    ** Aggregate to national
        replace location_id = .
        fastcollapse deaths*, by(iso3 location_id subdiv year list NID acause sex *frmat source source_label national region cause*) type(sum)
        
        sort iso3 location_id subdiv cause year source NID national sex
        foreach var of varlist deaths* {
            replace `var' = 0 if sex == 3 & (sex[_n-1] == 1 | sex[_n-1] == 2) & (sex[_n-2] == 1 | sex[_n-2] == 2) & `var'[_n-1] !=0 & `var'[_n-2]!=0 
        }
        
        replace sex = 9 if sex ==3

        fastcollapse deaths*, by(iso3 location_id year list acause sex *frmat source source_label national region cause*) type(sum)
            

        capture drop tmp
        egen tmp = rowtotal(deaths3-deaths25 deaths91-deaths94)
        gen tag = (tmp != 0 & deaths26 != 0)
        expand 2 if tag == 1, gen(new)


        replace deaths26 = 0 if tag == 1 & new == 0
        tempfile test
        save `test', replace

        order region iso3 source source_label national frmat im_frmat list sex year cause acause deaths2 deaths3 deaths4 deaths5 deaths6 deaths7 deaths8 deaths9 deaths10 deaths11 deaths12 deaths13 deaths14 deaths15 deaths16 deaths17 deaths18 deaths19 deaths20 deaths21 deaths22 deaths23 deaths24 deaths25 deaths91 deaths92 deaths93 deaths94 deaths1
        foreach var of varlist deaths3-deaths25 deaths91-deaths1 {
            replace `var' = 0 if tag == 1 & new == 1
        }
        replace frmat = 9 if tag == 1 & new == 1
        
        ** cleanup
        drop tmp tag new
        capture drop _merge

        drop if sex==3 | sex==9

        replace iso3 = "CHN" if iso3 == "HKG"
        replace iso3 = "CHN" if iso3 == "MAC"
        

        aorder
        move deaths26 deaths1
        egen deaths_known = rowtotal(deaths3-deaths94)
        replace frmat = 9 if deaths_known == 0 & deaths26 > 0 & deaths26 != .
        drop deaths_known
        

        drop deaths1
        aorder 
        egen deaths1 = rowtotal(deaths3-deaths94)
        drop if deaths1==0 | deaths1==.
        
        egen tot_im = rowtotal(deaths91-deaths94)
        replace deaths2 = tot_im 
        drop tot_im

    ** keep WHO standard age formats
        keep if frmat == 0 | (im_frmat == 1 | im_frmat == 2)
        drop if frmat==9
        
        replace deaths91 = deaths91 + deaths92 if im_frmat == 1
        replace im_frmat = 2 if im_frmat == 1
        drop deaths92
        
        replace deaths3 = deaths3 + deaths4 + deaths5 + deaths6 if frmat == 0
        replace deaths4 = 0 if frmat == 0
        replace deaths5 = 0 if frmat == 0
        replace deaths6 = 0 if frmat == 0


        drop if regexm(cause, "^acause_") & inlist(source, "ICD9_BTL", "ICD10_tabulated")

        drop if year<1970

        drop if inlist(iso3, "HKG", "MAC")
        drop if sex==3 | sex==9
        drop if frmat==9
        drop if deaths1==0 | deaths1==.

        drop if regexm(source, "England_UTLA_ICD") & acause == "cc_code"

        tempfile new_stuff
        save `new_stuff'

        use "FILEPATH", clear
        foreach data_source of local data_sources {
            drop if source == "`data_source'"
        }

        append using `new_stuff'
    ** save
   
        compress
        save "FILEPATH", replace
        tempfile alldat
        save `alldat'
}

** ***************************************************************************

if `adjust_weight_data' == 1 {

    ** MAKE ACAUSE WEIGHTS
    if `append_vr' == 0 {
        use "FILEPATH", clear
        tempfile alldat
        save `alldat'
    }
        replace acause = "cc_code"
        fastcollapse deaths*, by(iso3 location_id acause year sex frmat im_frmat) type(sum)
        tempfile fil2
        save `fil2', replace
            
        replace acause = "all"
        tempfile fil3
        save `fil3', replace
        
    ** collapse to acause
        use `alldat', clear
        drop if acause == "cc_code"
        replace acause = "neo_leukemia_other" if acause == "neo_leukemia_ll"
        replace acause = "neo_leukemia_other" if acause == "neo_leukemia_ml"
        replace acause = "inj_war_war" if acause == "inj_war"
        replace acause = lower(acause)
        fastcollapse deaths*, by(iso3 location_id acause year sex frmat im_frmat) type(sum)
        
    ** Aggregate up the levels
        
        ** prepare the cause list
            preserve
            do "FILEPATH"
            replace male = 1 if inlist(acause, "_gc", "_non", "_sb")
            replace female = 1 if inlist(acause, "_gc", "_non", "_sb")
            replace yll_age_start = 0 if inlist(acause, "_gc", "_non", "_sb")
            replace yll_age_end = 95 if inlist(acause, "_gc", "_non", "_sb")
            keep cause_id path_to_top_parent level acause yld_only yll_age_start yll_age_end male female
            replace yll_age_end = 125 if yll_age_end == 80
            levelsof cause_id if level == 0, local(top_cause)
            drop if path_to_top_parent =="`top_cause'"
            replace path_to_top_parent = subinstr(path_to_top_parent,"`top_cause',", "", .)    
            rename path_to_top_parent agg_
            split agg_, p(",")
            forvalues i = 1/5 {
                replace agg_`i' = "" if level == `i'
            }
            drop agg_5
            tempfile cause
            save `cause', replace
            restore
            replace acause = lower(acause)
        

            merge m:1 acause using `cause', keepusing(cause_id level agg* yld_only) keep(1 3)
            drop if acause=="_u"

            duplicates tag iso3 location_id cause_id acause level agg* year sex frmat im_frmat, gen(ddd)
            assert ddd == 0
            drop ddd
            keep iso3 location_id cause_id acause level agg* year sex frmat im_frmat deaths*
            tempfile orig
            save `orig', replace
            foreach level in 5 4 3 2 {
                use `orig', clear
                local agg_level = `level'-1
                if `level'<=4 append using `level`level''
                if `level'<=4 merge m:1 cause_id using `cause', update keepusing(level agg*) keep(1 3 4 5)
                if `level'<=4 tab cause_id if _merge ==1
                keep if level == `level'
                fastcollapse deaths*, by(iso3 location_id agg_`agg_level' year sex frmat im_frmat) type(sum)
                rename agg_`agg_level' cause_id
                destring cause_id, replace
                gen level = `agg_level'
                tempfile level`agg_level'
                save `level`agg_level'', replace
            }

        use `orig', clear
        gen orig=1
        append using `level4'
        append using `level3'
        append using `level2'
        append using `level1'
        replace orig=0 if orig==.

        drop if level>3 & level!=.


        fastcollapse deaths*, by(iso3 location_id cause_id year sex frmat im_frmat) type(sum)

        ** bring back other variables of interest
        merge m:1 cause_id using `cause', keepusing(acause male female yll_age_start yll_age_end) update keep(1 3 4 5) nogen
        drop cause_id
        ** append these extra files
        append using `fil2'        
        append using `fil3'
        
        save `alldat', replace
        merge m:1 iso3 location_id year sex using `pop', assert (using matched) keep(3) keepusing(pop*) nogen
        if `use_artificial_old_age' == 0 {
            gen deaths23 = 0
            gen deaths24 = 0
            gen deaths25 = 0   
        }
    ** rename variables to actual ages
        foreach var in pop deaths {
                rename `var'91 `var'_91
                rename `var'93 `var'_93
                rename `var'94 `var'_94
                rename `var'3 `var'_1
            forvalues i = 7/25 {
                local j = (`i'-6)*5
                rename `var'`i' `var'_`j'
            }
        }
        capture drop deaths1 deaths2 deaths4 deaths5 deaths6 deaths26

    ** ADJUST FOR COMPLETENESS
        
        merge m:1 iso3 year using `comp', keep(1 3)
        
        codebook iso3 if _merge==1
        codebook year if _merge==1
        ** pause
        drop if _merge==1
        drop _merge
        
        ** child completeness
        foreach i of numlist 91 93 94 1 {
            replace deaths_`i' = deaths_`i' / u5_comp_pred
            }
        ** 5-14 comp adjust
        foreach i of numlist 5 10 {
            replace deaths_`i' = deaths_`i' / kid_comp
            }
        ** adult completeness
        foreach i of numlist 15(5)95 {
            replace deaths_`i' = deaths_`i' / trunc_pred
            }
        
        foreach i of numlist 1 5(5)95 {
            replace deaths_`i' = 0 if frmat != 0
            replace pop_`i' = 0 if frmat != 0
        }
        foreach i of numlist 91 93 94 {
            replace deaths_`i' = 0 if im_frmat != 2
            replace pop_`i' = 0 if im_frmat != 2
        }
        
        ** Save aggregated and completeness adusted file
        fastcollapse pop_* deaths*, by(iso3 location_id acause year sex yll_age_start yll_age_end male female) type(sum)
        save "FILEPATH", replace
}

**    +++++++++++++++++++++++++++++++++++++++++++++++

if `make_weights' == 1 {
    if `adjust_weight_data' == 0 {
        use "FILEPATH", clear
    }
    ** now collapse pop and deaths across all countries and years    
        fastcollapse pop_* deaths_*, by(sex acause yll_age_start yll_age_end male female) type(sum)
        
    ** create rates
        foreach i of numlist 1 5(5)95 91 93 94 {
            generate rate`i' = deaths_`i'/pop_`i'
        }
        
    ** make age long
        drop deaths_* pop_*
        reshape long rate, i(sex acause) j(age)
        rename rate weight
        replace weight = 0 if weight == . | weight<0
        sort sex age
    
    ** Drop data that is unusable per restrictions
        ** note where there were restrictions
            gen is_restriction_violation = 0
            replace yll_age_end = 125 if yll_age_end == .
            replace yll_age_start = 0 if yll_age_start == .
            replace male = 1 if male == .
            replace female = 1 if female == .
            destring yll_age_start yll_age_end male female, replace
        ** drop restricted sexes
            replace is_restriction_violation = 1 if sex == 1 & male == 0
            replace is_restriction_violation = 1 if sex == 2 & female == 0
        ** drop restricted ages
            recast double age
            replace age = 0 if age==91
            replace age = .01 if age==93
            replace age = .1 if age==94
            replace is_restriction_violation = 1 if age<yll_age_start & yll_age_start!=.
            replace is_restriction_violation = 1 if age>yll_age_end & yll_age_end!=.
            replace age = 91 if age==0
            replace age = 93 if age>0.009 & age<0.0111
            replace age = 94 if age>0.099 & age<0.111

            replace weight = 0 if is_restriction_violation == 1

            ** make the weight the minimum non-zero weight for the acause if it is 0 and not a restriction violation
            gen weight_tmp = weight if is_restriction_violation != 1 & weight != 0
            bysort acause: egen min_weight = min(weight_tmp)
            drop weight_tmp
            gen min_replaced = 1 if is_restriction_violation != 1 & (weight == 0 | weight == .)
            replace weight = min_weight if is_restriction_violation != 1 & (weight == 0 | weight == .)
            assert weight == 0 if is_restriction_violation == 1
            assert weight != 0 if is_restriction_violation != 1
            assert weight != .
            drop is_restriction_violation min_weight min_replaced

        reshape wide weight, i(acause age yll* male female) j(sex)
        replace weight1 = 0 if weight1 == .
        replace weight2 = 0 if weight2 == .
        reshape long weight, i(acause age yll* male female) j(sex)
        
     ** save
        quietly levelsof acause if acause != "ntd_ebola", local(causes)
        foreach c of local causes {
            preserve
                drop yll* male female
                keep if acause == "`c'"
                sort sex age
                order acause sex age weight
                save "FILEPATH", replace
                capture saveold "FILEPATH", replace
                if "`c'" == "maternal" {
                    save "FILEPATH", replace
                    capture saveold "FILEPATH", replace
                }
                if "`c'" == "_ntd" {
                    replace acause = "ntd_ebola"
                    gen source_acause = "_ntd"
                    save "FILEPATH", replace
                    capture saveold "FILEPATH", replace
                }
            restore
        }

    if `graph_weights_in_pdf' == 1 {
        ** Make reference group weights
            preserve
                keep if male == 0
                keep if sex==2
                keep if age==40
                keep acause weight
                rename weight weight_f_40
                tempfil f40
                save `f40', replace
            restore, preserve
                keep if sex==1
                keep if age==40
                rename weight weight_m_40
                keep acause weight_m_40
                tempfil m40
                save `m40', replace
            restore, preserve
                keep if yll_age_end<=1
                keep if sex==1
                keep if age==91
                rename weight weight_m_inf
                keep acause weight_m_inf
                tempfil m_infant
                save `m_infant', replace
            restore
                merge m:1 acause using `f40', assert(master matched) nogen
                merge m:1 acause using `m40', assert(master matched) nogen
                merge m:1 acause using `m_infant', assert(master matched) nogen
                replace weight = weight / weight_m_40 if male!=0 & yll_age_end>1
                replace weight = weight / weight_f_40 if male==0 & yll_age_end>1
                replace weight = weight / weight_m_inf if yll_age_end<=1
                drop weight_* yll*
            
        ** plot the weights by age
            quietly do "FILEPATH"
            pdfstart using "FILEPATH"
            foreach c of local causes {
                preserve
                keep if acause == "`c'"
                    replace age = 4 if age == 1
                    replace age = 1 if age == 91
                    replace age = 2 if age == 93
                    replace age = 3 if age == 94
                    label define agelab 1 "enn" 2 "lnn" 3 "pnn" 4 "1" 5 "5" 10 "10" 15 "15" 20 "20" 25 "25" 30 "30" 35 "35" 40 "40" 45 "45" 50 "50" 55 "55" 60 "60" 65 "65" 70 "70" 75 "75" 80 "80+"
                    label values age agelab
                    sort sex age
                    label define sexlab 1 "Male" 2 "Female"
                    label values sex sexlab
                scatter weight age, by(sex, title("Age Split Weights, `c'")) xtitle("Age") ytitle("Weight") connect(l)
                pdfappend 
                restore
            }
            pdffinish
    }

}
