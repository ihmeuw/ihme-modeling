
** Purpose:        Extract SEER survival data for use in YLD estimation.
**
** **************************************************************************
** CONFIGURATION
**
** **************************************************************************
    // Clear memory and set memory and variable limits
        clear all
        set maxvar 32000
        set more off

    // Get date
        local today = date(c(current_date), "DMY")
        global today = string(year(`today')) + "_" + string(month(`today'),"%02.0f") + "_" + string(day(`today'),"%02.0f")
        local time = subinstr("`c(current_time)'",":","",.)


** **************************************************************************
** SET LOCALS AND DETERMINE WHICH STEPS TO RUN
** **************************************************************************
    local survival_dir "$survival_input_storage"

    // SELECT FILES TO EXTRACT
        local SEER_yearly            = 0
        local SEER_5_year            = 0
        local GBD2015_update        = 1

** **************************************************************************
** START LOGGING
** **************************************************************************
    local name ""
    foreach step in step_extract_SEER step_upper_lower step_survival_average step_survival_regression step_survival_glm step_survival_fp step_make_graphs step_extract_NORD step_comparisons {
        local namepart = subinstr("`step'","step_","",.)
        if ``step'' == 1 {
            if "`name'" != "" {
                local name = "`name'_`namepart'"
            }
            else {
                local name = "`namepart'"
            }
        }
    }

    cap mkdir "`survival_dir'/logs"
    cap log close _all
    log using "`survival_dir'/logs/`name'_`today'_`time'.smcl", replace


** **************************************************************************
** RUN PROGRAM
** **************************************************************************
// /////////////////////////////////////////////////////
// CLEAN AND FORMAT YEARLY SURVIVAL DATA FOR 1975-2010
// /////////////////////////////////////////////////////
if `SEER_yearly' {
    // Import headers, format and tempfile

        insheet using "`survivsl_dir'/USA_1970_2010_yearly_survival_by_cause_headers.csv", clear

        // sex
            gen sex=3 if regexm(v1,"Males and Females")==1
            replace sex=1 if regexm(v1,"Males")==1 & sex==.
            replace sex=2 if regexm(v1,"Females")==1 & sex==.

        // cancer
            replace v1=subinstr(v1,"SEERa Relative Survival","",.)
            replace v1=subinstr(v1,word(v1,1),"",.)
            replace v1=subinstr(v1,word(v1,1),"",.)
            replace v1=trim(v1)
            gen cancer=substr(v1,1,strpos(v1,"("))
            replace cancer=subinstr(cancer,word(cancer,-1),"",.)
            replace cancer=trim(cancer)

        // specifics
            replace v1=subinstr(v1,cancer,"",.)
            replace v1=subinstr(v1," (Percent) By Year of Diagnosis All Races, ","",.)
            replace v1=subinstr(v1,"Males and Females","",.)
            replace v1=subinstr(v1,"Females","",.)
            replace v1=subinstr(v1,"Males","",.)
            replace v1=subinstr(v1," Year of Diagnosis","",.)
            replace v1=itrim(v1)
            replace v1=trim(v1)
            compress
            rename v1 specifics
            order cancer sex specifics
            gen id=_n

        // tempfile
            tempfile headers
            save `headers', replace


    // Import yearly data, format, merge on headers and save
        insheet using "`survival_dir'/data/raw/USA_1970_2010_yearly_survival_by_cause_data.csv", clear

        // name years
            rename (v2 v3 v4 v5) (year1975 year1980 year1985 year1990)
            foreach n of numlist 6/21 {
                local yr=`n' + 1989
                rename v`n' year`yr'
            }

        // merge on headers
            gen index=_n
            preserve
            keep if v1>"A"
            gen id=_n
            keep index id
            tempfile idlist
            save `idlist', replace
            restore
            merge 1:1 index using `idlist', assert(1 3) nogen
            replace id=id[_n-1] if id==.
            merge m:1 id using `headers', assert(3) nogen


        // map on gbd causes
            preserve
            insheet using "`survival_dir'/seer_survival_gbd_cause_map.csv", clear
            cap rename (v1 v2 v3 v4) (dataset cancer specifics acause)
            drop if acause=="acause"
            tempfile causemap
            save `causemap', replace
            restore
            gen dataset="yearly"
            joinby dataset cancer specifics using `causemap', nolabel

        // clean up
            drop if v1>"A"
            split v1, p("-")
            rename v11 survival_years
            drop index id cancer specifics v1 v12 dataset
            drop if sex==3
            order acause sex survival
            destring survival, replace
            sort acause sex survival

        // reshape survival percentage long over year
            reshape long year, i(acause sex survival) j(percent)

        // prep to be used as the yearly best file
            rename (year percent survival_years) (survival_best year survived_years)

        // save
            saveold "`survival_dir'/seer_yearly_1_30_years.dta", replace
            tempfile best
            save `best', replace
}

// /////////////////////////////////////////////////////
// CLEAN AND FORMAT 5-YEAR SURVIVAL DATA FOR 1950
// /////////////////////////////////////////////////////
if `SEER_5_year' {
    // Impor 1950 data, map on causes, format and tempfile
        insheet using "`survival_dir'/data/raw/USA_1950_5_year_survival_rate_by_cause.csv", clear

        // expand the sex=3 rows to have one for each sex
            preserve
            keep if sex==3
            expand 2, gen(new)
            replace new=new+1
            drop sex
            rename new sex
            tempfile bysex
            save `bysex', replace
            restore
            drop if sex==3
            append using `bysex'

        // merge with cause map
            gen dataset="1950"
            joinby cancer using `causemap', nolabel
            duplicates drop

        // clean up
            drop specifics dataset
            gen survival_years=5
            order acause cancer sex

        // reshape survival percentage long over year
            reshape long year, i(acause sex) j(percent)

        // prep to be used as the "survival worst" file
            rename year survival_worst
            drop cancer survival_years percent

        // create rows for mesothelioma with surv=0%
            qui count
            local num=`r(N)' + 2
            set obs `num'
            replace acause="neo_meso" if acause==""
            replace survival_worst=0 if _n>(`num'-2)
            replace sex=1 if _n==`num'-1
            replace sex=2 if _n==`num'

        // save
            saveold "`survival_dir'/data/intermediate/seer_1950_5_year.dta", replace
            tempfile worst
            save `worst', replace
}

// /////////////////////////////////////////////////////
// CLEAN AND FORMAT GBD2015 leukemia subtype and mesothelioma update data
// /////////////////////////////////////////////////////
if `GBD2015_update'{
    // Import data
        import delimited using "`survival_dir'/Leukemia_Gallbladder_mesothelioma.csv", clear

    // Format
        destring survival_best, replace
        keep acause sex year survived_years survival_best

    // compress and save
    compress
    save "`survival_dir'/data/intermediate/gbd2015_update.dta", replace
}

** ****
** END
** *****
