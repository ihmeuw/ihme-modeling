capture program drop extract_sib
program define extract_sib

    ** *****************************************************************************************
    clear
    set mem 15g
    set maxvar 10000
    syntax , [iso3(string) year(string) filename(string) subnat(string) mapping(string) province(string) clear] 

    ** Set directories - globals to apply to all do files
    if c(os)=="Windows" {
         local prefix="FILEPATH"
         local homeprefix="FILEPATH"
        }
    if c(os)=="Unix" {
        local prefix="FILEPATH"
         local homeprefix="FILEPATH"
        }
    ** *Set width of age groups and time periods
    global yearblock 5
    global ageblock 5

    ** *****************************************************************************************
    ** SET UP STATA                         
    ** *****************************************************************************************

    clear
    set more off
    pause on    

    ** *****************************************************************************************
    ** 
    ** *****************************************************************************************

    capture erase "FILEPATH"  

    local f: dir "FILEPATH", respectcase
    foreach file of local f {
        capture erase "FILEPATH"
        }
    ** *****************************************************************************************
    ** 1. loop through DHS at the county level or subnational level and save in a temporary folder  
    ** *****************************************************************************************

    if "`subnat'" != "nan"{
        if "`mapping'" == "nan" {
            use caseid v000* v002* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* v024 using "`filename'", clear
            gen prov = "`iso3'"
        }

        else {
            cd "FILEPATH"
            do "get_prov.do"
            get_prov, iso3("`iso3'") subnat("`subnat'") mapping("`mapping'") province("`province'")
            tempfile master_sub
            save `master_sub'   

            use caseid v000* v002* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* v024 `subnat' using "`filename'", clear
            merge m:1 `subnat' using `master_sub', keep(3) nogen
            rename ihme prov
        }

        gen file = "`filename'"
        save "FILEPATH", replace

        local sibfiles: dir "FILEPATH", respectcase
        local count: word count `sibfiles'
        di as result _newline(3) "THERE ARE A TOTAL OF `count' SIBLING HISTORY SURVEYS" _newline(3)
        local numsibfiles: subinstr local sibfiles "sibs" "", all

        di `"FILEPATH"'      
    }
    else {

        use caseid v000* v002* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* v024 using "`filename'", clear
        gen file = "`filename'"
        gen prov = "`iso3'"
        save "FILEPATH", replace

        local sibfiles: dir "FILEPATH", respectcase
        local count: word count `sibfiles'
        di as result _newline(3) "THERE ARE A TOTAL OF `count' SIBLING HISTORY SURVEYS" _newline(3)
        local numsibfiles: subinstr local sibfiles "sibs" "", all

        di `"FILEPATH"'
    }

    ** *****************************************************************************************
    ** 2. FOR EACH FILE IN THE TEMPORARY FOLDER, EXTRACT VARIABLES NEEDED FOR ANALYSIS
    ** *****************************************************************************************

    foreach file of local numsibfiles {

        di "`file'"

        use caseid v000* v002* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* prov using "FILEPATH", clear 
        capture keep caseid mmc1 v000* v002* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm15* prov
        qui count if mmc1 ==.       
        local nosib = r(N)
        display as error "Dropping `nosib' observations from survey `file' because they did not respond to question MMC1"
        drop if mmc1 == .
        drop mmc1

        if v007>99 {                
            replace v007=v007-1900
            }                   
        if("`iso3'" == "ETH") {
            local seconds_change = 2807*24*3600
            foreach var of varlist v008 mm4_* mm8_* {
                qui replace `var' = trunc( (`var'*30*24*3600 + `seconds_change' ) / ( 30*24*3600 ) )
            }
        }

    ** *****************************************************************************************
    ** 3. STANDARDIZE AND CALCULATE BASIC INPUTS FOR ANALYSIS AND RESHAPE SO THAT EACH 
    **      OBSERVATION IS A SIBLING
    ** *****************************************************************************************

    ** for all surveys with CMC:
        forvalues num=1/8 { 
            capture renpfix mm`num'_0 mm`num'_ 
            }               

        capture renpfix mm15_0 mm15_
        capture renpfix mmidx_0 mmidx_    

        ** id
        gen mmidx_0 = 0    
        ** sex
        gen mm1_0 = 2      
        ** alive
        gen mm2_0 = 1      
        ** current age
        gen mm3_0 = v012   
        ** CMC date of birth
        gen mm4_0 = v011   
        ** years ago died
        gen mm6_0 = .      
        ** age at death
        gen mm7_0 = .      
        ** CMC date of death
        gen mm8_0 = .     

        // Reshape so that every observation is a sibling
        reshape long mmidx_ mm1_ mm2_ mm3_ mm4_  mm6_ mm7_ mm8_ mm15_ , i(caseid) j(sibid)  


        drop if mm1_ ==. & mm2_ == . & mm3_==. & mm4_==.

        rename caseid id
        rename v000 wbcode
        rename v002 hhid
        rename v007 yr_interview
        rename v009 month_of_birth
        rename v010 yob_resp
        rename v011 cmc_dob
        rename v012 ageresp
        rename v013 ageresp_cat
        rename v014 data_completeness
        rename mm1_ sex
        replace sex = 0 if sex == 2
        rename mm2_ alive
        rename mm3_ age_sib_ifalive
        rename mm4_ cmc_dob_sib
        rename mm6_ years_since_dead
        rename mm7_ age_sib_ifdead
        rename mm8_ cmc_dod
        rename mm15_ yod_sib
        replace v005 = v005 / 1000000       // Per DHS manual, divide sampling weight by 1000000 before applying. 

        capture rename n n01

        ** CALCULATE BASIC INPUTS 
        // Year of death
        capture drop yod
            ** Primary input: mm8_ : CMC date of death of sibling
            ** Secondary input: v007 - mm6_ : year of interview - number of years ago respondent's sibling died
            ** Tertiary input: mm15_: year of death of sibling (not in all surveys)
        gen yod = int((cmc_dod-1)/12)
        replace yod = yr_interview-years_since_dead if yod==. | (wbcode == "NP3" & yr_interview == 1996)
        capture replace yod = yod_sib if yod==.

        // Year of birth
        capture drop yob
            ** Primary input: mm4_: CMC date of birth of sibling
            ** Secondary input: year of interview - sibling's current age [if alive]
            ** Tertiary input: year of interview - (age at death of sibling + years ago sibling died) [if dead]
        gen yob=int((cmc_dob_sib-1)/12)
        replace yob=yr_interview-age_sib_ifalive if (alive==1 & yob==. ) | (wbcode == "NP3" & yr_interview == 1996)
        replace yob=yr_interview-(age_sib_ifdead+years_since_dead) if alive==0 & yob==. | (wbcode == "NP3" & yr_interview == 1996)

        replace yob = . if yob < 0

        ** *****************************************************************************************
        ** Convert Ethiopia dates to standard calendar
        ** *****************************************************************************************
        if "`iso3'" == "ETH" {
            replace yr_interview = 8 + yr_interview

            qui sum yr_interview
            local max_yr_interview = `r(max)'

            qui sum yod
            local max_yod = `r(max)'

            di "Max year of death is `max_yod'"
            di "Max year interview is `max_yr_interview'"

            if(`max_yod' < `max_yr_interview') {
                replace yob = yob + 7 if country == "Ethiopia" & v006 < 5 & inlist(yr_interview, 2000, 2005, 2010)
                replace yod = yod + 7 if country == "Ethiopia" & v006 < 5 & inlist(yr_interview, 2000, 2005, 2010)

                replace yob = yob + 8 if country == "Ethiopia" & v006 >= 5 & inlist(yr_interview, 2000, 2005, 2010)
                replace yod = yod + 8 if country == "Ethiopia" & v006 >= 5 & inlist(yr_interview, 2000, 2005, 2010)
            }
        }

        // Alive or dead status:
        replace alive = . if alive == 8
        tab alive, miss

        local year_str = substr("`file'",-8,4)
        if strlen("`year'") == 4{
            local year2 = `year' + 1
            gen svy = prov + "_`year'" + "_`year2'"
            }
        else {
            gen svy = prov + "_`year'"
            }
        drop prov
        egen samplesize = total(v005) if sibid == 0 
        sort svy sibid
        by svy: replace samplesize = samplesize[1] if samplesize==.

        capture tostring hhid, replace

        capture keep svy hhid id sibid v005 v006 v008 v021 n01 qline yr_interview sex yod yob alive samplesize ageresp
            capture keep svy hhid id sibid v005 v006 v008 v021 n01 qline yr_interview sex yod yob alive samplesize ageresp
            if _rc != 0 {
                keep hhid id sibid v005 v006 yr_interview v008 v021 sex alive yod yob svy samplesize ageresp
            }

        compress
  
    ** *****************************************************************************************
    ** 4. APPEND SURVEYS TOGETHER AS WE LOOP THROUGH THEM
    ** *****************************************************************************************
    capture append using "FILEPATH"
        preserve
        capture keep hhid id sibid v005 v006 yr_interview v008 v021 n01 qline sex alive yod yob svy samplesize ageresp
            if _rc != 0 {
                keep hhid id sibid v005 v006 yr_interview v008 v021 sex alive yod yob svy samplesize ageresp
            }
        replace hhid = trim(substr(id,1,length(id)-2))
        restore
        save "FILEPATH", replace   

        }           

end
