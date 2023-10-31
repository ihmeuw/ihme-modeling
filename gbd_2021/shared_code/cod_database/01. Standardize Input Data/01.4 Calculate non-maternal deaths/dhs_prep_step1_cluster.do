    capture restore
    clear all
    set mem 1000m

    set more off
    ssc install bygap
     
  // set the prefix for whichever os we're in
    if c(os) == "Unix" {
        set mem 10G
        set odbcmgr unixodbc
        global j "FILEPATH"
        local cwd = c(pwd)
        cd "~"
        global h = c(pwd)
        cd `cwd'
    }
    else if c(os) == "Windows" {
        global j "J:"
        global h "H:"
    }

    global repo_dir "FILEPATH"
// Set the time
    // Get create_timestamp function
    run "FILEPATH"
    // Get time of completion 
    create_timestamp
    global timestamp = "`r(timestamp)'"

** Working directories
    global raw_dir "FILEPATH"
    global log_dir "FILEPATH"
    global cod_out_dir "FILEPATH" 
    global cod_archive "FILEPATH"
    global temp_error_dir "FILEPATH"

** set locals from arguments
    set obs 1
    gen arg = "$arg"
    split arg, parse(-) generate(a)
    destring a1, replace
    global newflag = a1
    global filename = a2                                                                            
    global st_country = a3        
    global st_year = a4
    global st_dir = a5

** start log
capture log close
cd "$log_dir"
local f = subinstr("FILEPATH")
global filename = subinstr("$filename",".DTA","",.)
log using "dp1_`f'.smcl", replace
    
di in red "newflag: $newflag"
di in red "filename: $filename"
di in red "st_country: $st_country"
di in red "st_year: $st_year"
di in red "st_dir: $st_dir"

local year = substr("$filename",19,4)
if "$filename"=="FILEPATH" {
    local year = "2001"
}

if ($newflag == 1) {
capture confirm file `"FILEPATH"'
** "     
dis _rc

if (_rc != 0) local rc = 1
 }
else {
    local rc = 1
}
pause
if `rc'~=0 {    


    if "$filename" == "FILEPATH" {

        use qunion qcluster qnumber qline qweight qintm qinty qintc q105m q105y q105c q106 q201a q204* q205* q206* q207* q208* q209* q210* q211* q212* using "FILEPATH", clear
        
        rename qweight v005
        rename qintm v006
        rename qinty v007
        rename qintc v008
        rename q105m v009
        rename q105y v010
        rename q105c v011
        rename q106 v012
        renpfix q205 mm1
        renpfix q206 mm2
        renpfix q207c mm4
        renpfix q207 mm3
        renpfix q208c mm8
        renpfix q208 mm6
        renpfix q209 mm7
        renpfix q204 mmidx
        renpfix q210_0 q210_
        renpfix q211_0 q211_
        renpfix q212_0 q212_
        
        gen caseid=string(qunion)+string(qcluster)+string(qnumber)+string(qline)
        bysort caseid: gen n=_n
        replace caseid=caseid+string(n)
        gen v013=1 if v012>=15 & v012<=19
        replace v013=2 if v012>=20 & v012<=24
        replace v013=3 if v012>=25 & v012<=29
        replace v013=4 if v012>=30 & v012<=34
        replace v013=5 if v012>=35 & v012<=39
        replace v013=6 if v012>=40 & v012<=44
        replace v013=7 if v012>=45 & v012<=49
        gen v014=.
        gen v000="BGD"
        forvalues i=1/9 {
            gen mm15_0`i'=.
            replace mm2_0`i'=0 if mm2_0`i'==2
            gen mm9_0`i' = 2 if (q210_`i'==1 | q211_`i'==1 | q212_`i'==1)
            }
        forvalues i=10/20 {
            gen mm15_`i'=.
            replace mm2_`i'=0 if mm2_`i'==2
            gen mm9_`i' = 2 if q210_`i'==1 | q211_`i'==1 | q212_`i'==1
            }
        rename q201a mmc1
        qui count if mmc1 ==.
        local nosib = r(N)
        display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
        drop if mmc1 == .                                 
        * Dropping females who did not respond to sibling history module
        drop mmc1 qunion qcluster qnumber qline q210* q211* q212*
        
    }
    
    else if "$filename" == "FILEPATH" {    
        ** Bangladesh 2001 - format weird variable names 
        use qunion qcluster qnumber qline qweight qintm qinty qintc q105m q105y q105c q106 q201a q204* q205* q206* q207* q208* q209* q210* q211* q212* using "FILEPATH", clear
        rename qweight v005
        rename qintm v006
        rename qinty v007
        rename qintc v008
        rename q105m v009
        rename q105y v010
        rename q105c v011
        rename q106 v012
        renpfix q205 mm1
        renpfix q206 mm2
        renpfix q207c mm4
        renpfix q207 mm3
        renpfix q208c mm8
        renpfix q208 mm6
        renpfix q209 mm7
        renpfix q204 mmidx
        renpfix q210_0 q210_
        renpfix q211_0 q211_
        renpfix q212_0 q212_
        
        gen caseid=string(qunion)+string(qcluster)+string(qnumber)+string(qline)
        bysort caseid: gen n=_n
        replace caseid=caseid+string(n)
        gen v013=1 if v012>=15 & v012<=19
        replace v013=2 if v012>=20 & v012<=24
        replace v013=3 if v012>=25 & v012<=29
        replace v013=4 if v012>=30 & v012<=34
        replace v013=5 if v012>=35 & v012<=39
        replace v013=6 if v012>=40 & v012<=44
        replace v013=7 if v012>=45 & v012<=49
        gen v014=.
        gen v000="BGD"
        forvalues i=1/9 {
            gen mm15_0`i'=.
            replace mm2_0`i'=0 if mm2_0`i'==2
            gen mm9_0`i' = 2 if (q210_`i'==1 | q211_`i'==1 | q212_`i'==1)
            }
        forvalues i=10/20 {
            gen mm15_`i'=.
            replace mm2_`i'=0 if mm2_`i'==2
            gen mm9_`i' = 2 if q210_`i'==1 | q211_`i'==1 | q212_`i'==1
            }
        rename q201a mmc1
        qui count if mmc1 ==.
        local nosib = r(N)
        display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
        drop if mmc1 == .                                 

        drop mmc1 qunion qcluster qnumber qline q210* q211* q212*
        
    }

    else if "$filename" == "FILEPATH" {        

        use sstate caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* using "FILEPATH", clear 
        capture keep mmc1 sstate caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15*
        qui count if mmc1 ==.
        local nosib = r(N)
        display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"

        drop if mmc1 == .                                 
        drop mmc1
    }
    
    else if "$filename" == "FILEPATH" {                        
        use caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* using "FILEPATH", clear 

        capture keep caseid mmc1 v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15*
        qui count if mmc1 ==.
        local nosib = r(N)
        display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"

        drop if mmc1 == .                                 
        drop mmc1
    }
    
    else if "$filename" == "FILEPATH" {   

        use caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* using "FILEPATH", clear 
        capture keep caseid mmc1 v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15* 
        qui count if mmc1 ==.
        local nosib = r(N)
        display as error "Dropping `nosib' observations from survey `id' (DOM 2001) because they did not respond to question MMC1"

        drop if mmc1 == .                                 
        drop mmc1
    }
    
    else if "$filename" == "FILEPATH" {    

        use aidsex caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* using "FILEPATH", clear 
        capture keep mmc1 aidsex caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15*

        drop if aidsex == 1                             
        drop aidsex
        qui count if mmc1 ==.
        local nosib = r(N)
        display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
        drop if mmc1 == .                                 
        drop mmc1
    }

    else if "$filename" == "FILEPATH" {        
        ** Nepal 2006 - need to fix Nepali dates */
        use caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* using "FILEPATH", clear 
        local seconds_change = - ( 56*365*24*3600 + 8*30*24*3600 + 15*24*3600 )
        foreach var of varlist v007 v008 v011 mm4* mm8* mm15* {
            qui replace `var' = trunc( (`var'*30*24*3600 + `seconds_change' ) / ( 30*24*3600 ) )
            }
        capture keep caseid mmc1 v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15*
        qui count if mmc1 ==.
        local nosib = r(N)
        display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"

        drop if mmc1 == .                                 
        drop mmc1

        replace v007 = int((v008 - 1)/12) + 1900        
    }
    
    else if "$filename" == "FILEPATH" {
        use qcluster qnumhh qline qweight qintm qinty qintc q103m q103y q103c q104 q801 q804* q805* q806* q807* q808* q809* q810* q811* q812* using "FILEPATH", clear
        rename qweight v005
        rename qintm v006
        rename qinty v007
        rename qintc v008
        rename q103m v009
        rename q103y v010
        rename q103c v011
        rename q104 v012
        renpfix q805 mm1
        renpfix q806 mm2
        renpfix q807c mm4
        renpfix q807 mm3
        renpfix q808c mm8
        renpfix q808 mm6
        renpfix q809 mm7
        renpfix q804 mmidx
        gen caseid=string(qcluster)+string(qnumhh)+string(qline)
        bysort caseid: gen n=_n
        replace caseid=caseid+string(n)
        gen v013=1 if v012>=15 & v012<=19
        replace v013=2 if v012>=20 & v012<=24
        replace v013=3 if v012>=25 & v012<=29
        replace v013=4 if v012>=30 & v012<=34
        replace v013=5 if v012>=35 & v012<=39
        replace v013=6 if v012>=40 & v012<=44
        replace v013=7 if v012>=45 & v012<=49
        gen v014=.
        gen v000="GHA"
        forvalues i=1/9 {
            gen mm15_0`i'=.
            replace mm2_0`i'=0 if mm2_0`i'==2
            gen mm9_0`i' = 2 if q810_0`i'==1 | q811_0`i'==1 | q812_0`i'==1
            }
        forvalues i=10/17 {
            gen mm15_`i'=.
            replace mm2_`i'=0 if mm2_`i'==2
            gen mm9_`i' = 2 if q810_`i'==1 | q811_`i'==1 | q812_`i'==1
            }
        rename q801 mmc1
        qui count if mmc1 ==.
        local nosib = r(N)
        display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
        drop if mmc1 == .                                 

        drop mmc1 qcluster qnumhh qline
    }
    
    else if "$filename" == "FILEPATH" {    
        ** Sudan 1989-1990 survey. Sibling history variables coded differently. Need to standardize. */
        use caseid v000* v005* v006* v008* v007* v009* v010* v011* v012* v013* v014* s91co* s803* s804* s805* s807* s808* s810* s811* using "FILEPATH", clear
        renpfix s91co mmidx
        renpfix s803 mm1
        renpfix s804 mm2
        renpfix s805 mm3
        renpfix s807 mm6
        renpfix s808 mm7
        forvalues i=1/9 {
            gen mm4_0`i'=.
            gen mm5_0`i'=.
            gen mm8_0`i'=.
            gen mm9_0`i'=2 if s810_0`i'==1 | s811_0`i'==1
            gen mm15_0`i'=.
            replace mm2_0`i'=0 if mm2_0`i'==2
            }
        forvalues i=10/16 {
            gen mm4_`i'=.
            gen mm5_`i'=.
            gen mm8_`i'=.
            gen mm9_0`i'=2 if s810_`i'==1 | s811_`i'==1
            gen mm15_`i'=.
            replace mm2_`i'=0 if mm2_`i'==2
            }
        drop s810* s811*    
    }
    
    else if "$filename" == "FILEPATH" {
        use caseid v000* v005* v006* v008* v021 v024 v007* v009* v010* v011* v012* v013* v014* mm* using "FILEPATH", clear

        di in red "South Africa subnational here, $filname"
    
  
        replace caseid = caseid + "p" + string(v024)
        drop v024
        capture keep caseid mmc1 v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15*
        qui count if mmc1 ==.
        local nosib = r(N)
        display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
        drop if mmc1 == . 
        drop mmc1
    }

    else if "$filename" == "FILEPATH" | "$filename" == "FILEPATH" | "$filename" == "FILEPATH" {
        di in red "Kenya subnational here, $filename"
        cap use caseid v000* v001* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* sdist mm* using "FILEPATH", clear
        replace caseid = caseid + "p" + string(sdist) + " f" + "$st_year"
    }
    
    else if "$filename" == "FILEPATH" {
        di in red "Kenya subnational here, $filename"
        cap use caseid v000* v001* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* sdist sregion mm* using "FILEPATH", clear
        replace caseid = caseid + "p" + string(sregion) + " f" + "$st_year"
    }

    
    else if "$filename" == "FILEPATH" {
        di in red "Indonesia subnational here, $filename"
        cap use caseid v000* v001* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* sregmun using "FILEPATH", clear
        rename sregmun kab
        gen prov_num = substr(string(v001), 1, 2)
        destring prov_num, replace
        gen kab_code = prov_num*100 + kab

        preserve
            use "FILEPATH", clear
            keep if source=="DHS" & source_year == 1994 
            keep kab_code prov_mapped source_year
            rename source_year year
            duplicates drop
            tempfile map
            save `map', replace
        restore

        merge m:1 kab_code using `map', keepusing(prov_mapped) keep(1 3)
        assert _m == 3
        replace caseid = caseid + "p" + string(prov_mapped)
        drop prov* kab* prov_mapped 
    }
    
    else if "$filename" == "FILEPATH" {
        di in red "Indonesia subnational here, $filename"
        cap use caseid v000* v001* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* sprov sregmun using "FILEPATH", clear

        capture label drop sprov
        rename sprov prov_num
        rename sregmun kab
        gen kab_code = prov_num*100 + kab

        preserve
            use "FILEPATH", clear
            keep if source=="DHS" & source_year == 1997
            keep kab_code prov_mapped source_year
            rename source_year year
            duplicates drop
            tempfile map
            save `map', replace
        restore

        merge m:1 kab_code using `map', keepusing(prov_mapped) keep(1 3)
        assert _m == 3
        replace caseid = caseid + "p" + string(prov_mapped)
        drop prov* kab* _m 
    }
    else if "$filename" == "FILEPATH" {
        di in red "Indonesia subnational here, $filename"
        cap use caseid v000* v001* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* v024 sregmun using "FILEPATH", clear

        label drop v024
        rename v024 prov_num
        rename sregmun kab
        gen kab_code = prov_num*100 + kab

        preserve
            use "FILEPATH", clear
            keep if source=="DHS" & source_year == 2002 
            keep kab_code prov_mapped source_year
            rename source_year year
            duplicates drop
            tempfile map
            save `map', replace
        restore

        merge m:1 kab_code using `map', keepusing(prov_mapped) keep(1 3)
        assert _m == 3
        replace caseid = caseid + "p" + string(prov_mapped)
        drop prov* kab* _m 
    }
    else if "$filename" == "FILEPATH" {
        di in red "Indonesia subnational here, $filename"
        cap use caseid v000* v001* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* v024 sregmun using "FILEPATH", clear

        capture label drop v024
        rename v024 prov_num
        rename sregmun kab
        gen kab_code = prov_num*100 + kab

        preserve
            use "FILEPATH", clear
            keep if source=="DHS" & source_year == 2007 
            keep kab_code prov_mapped source_year
            rename source_year year
            duplicates drop
            tempfile map
            save `map', replace
        restore

        merge m:1 kab_code using `map', keepusing(prov_mapped) keep(1 3)
        assert _m == 3
        replace caseid = caseid + "p" + string(prov_mapped)
        drop prov* kab* _m 
    }
    else if "$filename" == "FILEPATH"  {
        di in red "Indonesia subnational here, $filename"
        cap use caseid v000* v001* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* v024 sregmun using "FILEPATH", clear

        capture label drop v024
        rename v024 prov_num
        rename sregmun kab
        gen kab_code = prov_num*100 + kab

        preserve
            use "FILEPATH", clear
            keep if source=="DHS" & source_year == 2012
            keep kab_code prov_mapped source_year
            rename source_year year
            duplicates drop
            tempfile map
            save `map', replace
        restore

        merge m:1 kab_code using `map', keepusing(prov_mapped) keep(1 3)
        assert _m == 3
        replace caseid = caseid + "p" + string(prov_mapped)
        drop prov* kab* _m 
    }


    else if "$filename" == "FILEPATH" | "$filename" == "FILEPATH" | "$filename" == "FILEPATH" | "$filename" == "FILEPATH" {
        di in red "Ethiopia subnational here, $filename"
        cap use caseid v000* v001* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* v024 using "FILEPATH", clear
        decode v024, gen(region)
        replace caseid = caseid + "r_" + region
        drop region v024
    }


    else if "$filename" == "FILEPATH" { 
        use qweight qintcg qintyg qintmg q102m q102yg q102cg q103 q605* q606* q607* q607c* q610* q610c* q611* q614* using "FILEPATH", clear
        drop  q607cg_*  q610cg_*
        gen caseid = _n
        tostring caseid, replace
        gen v013 = "15-19" if q103>=15 & q103 <=19
        replace v013 = "20-24" if q103>=20 & q103 <=24
        replace v013 = "25-29" if q103>=25 & q103 <=29
        replace v013 = "30-34" if q103>=30 & q103 <=34
        replace v013 = "35-39" if q103>=35 & q103 <=39
        replace v013 = "40-44" if q103>=40 & q103 <=44
        replace v013 = "45-49" if q103>=45 & q103 <=49
        drop if v013 == ""
        gen v014 = .
        foreach i of numlist 1/30 {
            gen mmidx_`i' = `i'
        }

        foreach i of numlist 1/30 {
            if `i' > 9 {
                replace q607c_`i' = q607c_`i' + 255
                replace q610c_`i' = q610c_`i' + 255
            }
            else {
                replace q607c_0`i' = q607c_0`i' + 255
                replace q610c_0`i' = q610c_0`i' + 255            
            }
        }
            
        rename qweight v005    
        rename qintcg v008
        rename qintyg v007
        rename qintmg v006
        rename q102m v009
        rename q102yg v010
        rename q102cg v011
        rename q103 v012
        renpfix q605_ mm1_
        renpfix q606_ mm2_
        foreach var of varlist mm2* {
            replace `var' = 0 if `var' == 2
        }
        renpfix q607_ mm3_    
        renpfix q607c_ mm4_
        renpfix q610_ mm6_
        renpfix q611_ mm7_     
        renpfix q610c_ mm8_
        foreach i of numlist 1/30 {
            if `i' > 9 {
                gen mm9_`i' = 1 if q614_`i' == 2
                replace mm9_`i' = 2 if q614_`i' == 1
                replace mm9_`i' = q614_`i' if mm9_`i' == .
                drop q614_`i'
            }
            else {
                gen mm9_0`i' = 1 if q614_0`i' == 2
                replace mm9_0`i' = 2 if q614_0`i' == 1
                replace mm9_0`i' = q614_0`i' if mm9_0`i' == .
                drop q614_0`i'
            }
        }
        
        foreach i of numlist 1/30 {
            if `i' > 9 {
                gen mm15_`i' = 2010 -  mm6_`i'
                replace mm15_`i' = 99 if mm6_`i'==99 | mm6_`i'==98
            }
            else {
                gen mm15_0`i' = 2010 -  mm6_0`i' 
                replace mm15_0`i' = 99 if mm6_0`i'==99 | mm6_0`i'==98
            }
        }    
    }

    else if "$filename" == "FILEPATH" {
        cap use caseid v000* v001* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* using "FILEPATH", clear

        replace v007 = v007 + 621
        replace v010 = v010 + 621


        replace v011 = v011 + 255
        foreach i of numlist 1/20 {
            if `i' > 9 {
                replace mm8_`i' = mm8_`i' + 255
                replace mm4_`i' = mm4_`i' + 255
            }
            else {
                replace mm8_0`i' = mm8_0`i' + 255
                replace mm4_0`i' = mm4_0`i' + 255            
            }
        }
    }

    else {
        cap use caseid v000* v001* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* using "FILEPATH", clear
        if (_rc != 0) {
            cap use CASEID V000* V005* V006* V008* V021 V007* V009* V010* V011* V012* V013* V014* MM* using "FILEPATH", clear
            if (_rc != 0) {
                capture use caseid V000* V005* V006* V008* V021 V007* V009* V010* V011* V012* V013* V014* MM* using "FILEPATH", clear
                if (_rc != 0) {
                    clear 
                    set obs 1
                    gen data_name = "$filename"
                    gen iso3 = "$st_country"
                    gen year = "$st_year"
                    gen read_fail = 1
                    gen no_maternal = 0
                    gen note = "Failed to read in data: either no maternal questions, or variables are formatted differently (or both) - investigate"
                    cd "$temp_error_dir"
                    save "FILEPATH", replace
                    exit
                }
            else {
                capture keep caseid MMC1 V000* V005* V006* V008* V021 V007* V009* V010* V011* V012* V013* V014* MMIDX* MM1_* MM2_* MM3* MM4*  MM6* MM7* MM8* MM9* MM15*
                capture rename caseid CASEID
                renpfix V v
                renpfix MMIDX mmidx
                renpfix MMC mmc
                renpfix MM mm
                qui count if mmc1 ==.
                local nosib = r(N)
                display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
                drop if mmc1 == .                                 
                drop mmc1 
            }

            }
            else {
                capture keep CASEID MMC1 V000* V005* V006* V008* V021 V007* V009* V010* V011* V012* V013* V014* MMIDX* MM1_* MM2_* MM3* MM4*  MM6* MM7* MM8* MM9* MM15*
                renpfix V v
                renpfix MMIDX mmidx
                renpfix MMC mmc
                renpfix MM mm
                qui count if mmc1 ==.
                local nosib = r(N)
                display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
                drop if mmc1 == .                                 
                drop mmc1 
            }
        }
        else {
            capture keep caseid mmc1 v000* v001* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15*
            qui count if mmc1 ==.
            local nosib = r(N)
            display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
            drop if mmc1 == .                                 
            drop mmc1
        }
    }
if (_N == 0) {
    clear 
    set obs 1
    gen data_name = "$filename"
    gen iso3 = "$st_country"
    gen year = "$st_year"
    gen read_fail = 0
    gen no_maternal = 1
    gen note = "All maternal questions missing: other variables have might have necessary info - investigate"
    cd "$temp_error_dir"
    save "FILEPATH", replace
    exit
}
else {    
    pause

    if v007>99 {                
        replace v007=v007-1900
    }        

    if ("$filename" == "FILEPATH") {
        capture drop mmidx2*
    }
        
        replace v005 = v005/1000000
        gen country = "$st_country"
        gen filename="$filename"       
        gen surveyyear = substr("$st_year",1,4)

        replace surveyyear = "2001" if "$filename"=="FILEPATH"
        destring surveyyear, replace    

                                        
            forvalues x=1/9 {

                capture gen mm15_0`x' = .
                }
            
        capture renpfix mmidx_0 mmidx_
        capture renpfix mm1_0 mm1_
        capture renpfix mm2_0 mm2_
        capture renpfix mm3_0 mm3_
        capture renpfix mm4_0 mm4_
        capture renpfix mm5_0 mm5_
        capture renpfix mm6_0 mm6_
        capture renpfix mm7_0 mm7_
        capture renpfix mm8_0 mm8_
        capture renpfix mm9_0 mm9_
        capture renpfix mm15_0 mm15_

        
            gen mmidx_0 = 0    
            *sex
            gen mm1_0 = 2      
            *alive
            gen mm2_0 = 1      
            *current age
            gen mm3_0 = v012   
            *CMC date of birth
            gen mm4_0 = v011   
            *years ago died
            gen mm6_0 = .      
            *age at death
            gen mm7_0 = .      
            *CMC date of death
            gen mm8_0 = . 
            *year of death
            gen mm15_0 = .
            
            di in red "FILEPATH"
            capture rename CASEID caseid
            label drop _all
            label values mm*
            reshape long mmidx_ mm1_ mm2_ mm3_ mm4_ mm6_ mm7_ mm8_ mm9_ mm15_, i(caseid) j(mm)
            
            ** Drop observations with no sibling data */
            drop if mm1_ ==. & mm2_ == . & mm3_==. & mm4_==.        
            
            rename caseid id
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
            rename mmidx sibid
            
            replace yr_interview = yr_interview-1900 if yr_interview>1900
            
        capture drop yod

        gen yod = int((cmc_dod-1)/12)
        replace yod = yr_interview-years_since_dead if yod==. | (country == "NPL" & yr_interview == 1996)
        capture replace yod = yod_sib if yod==.

        capture drop yob
        gen yob=int((cmc_dob_sib-1)/12)
        replace yob=yr_interview-age_sib_ifalive if (alive==1 & yob==. )  | (country == "NPL" & yr_interview == 1996)
        replace yob=yr_interview-(age_sib_ifdead+years_since_dead) if alive==0 & yob==.  | (country == "NPL" & yr_interview == 1996)

        replace yob = . if yob < 0
  
                if "$filename"=="" {
                replace yod=mm15_
                replace yod=. if yod>surveyyear
                replace yod=. if yod==9998
                replace yod=surveyyear-mm6_ if yod==.

                replace    mm8_=(yod*12)+6
            }
            

            capture drop mmc*
            

            replace yob = yob + 7 if "$st_country" == "ETH" & v006 < 5 

            replace yob = yob + 8 if "$st_country" == "ETH" & v006 >= 5 

            replace yod = yod + 7 if "$st_country" == "ETH" & v006 < 5 

            replace yod = yod + 8 if "$st_country" == "ETH" & v006 >= 5 


            

        gen male = 1 if sex == 1
        replace male = 0 if sex != 1
        gen female = 1 if sex == 0 
        replace female = 0 if sex != 0 


        gen death = 1 if yod ~=.                        
        replace death = 0 if yod ==.

        replace yob = yob + 1900                        
        replace yod = yod + 1900
        gen aged = yod - yob

        replace aged = . if aged < 0                        

        gen agedcat = 0 if aged < 1                        
        replace agedcat = 1 if aged >= 1 & aged < 5

        forvalues a = 5(5)70 {
            local apn = `a' + 5
            replace agedcat = `a' if aged >= `a' & aged <`apn'
        }
        replace agedcat = 75 if aged>= 75

   
        tab sex, miss                        
  
        tab sex sibid, miss                    

        tab sex death, miss r                

        tab sex agedcat, miss r                    

        replace sex = . if sex!=0 & sex!=1                

        gen age = yr_interview -  yob                                

        gen ageblock = .                         
        forvalues age = 0(5)60   {
            replace ageblock =`age' if age - `age'  < 5 & age - `age' >= 0
            }
        replace ageblock = 45 if ageblock == 50 & sibid==0    


        bysort ageblock: egen males = total(male) if death==0
        sort ageblock males
        by ageblock: replace males = males[1] if males==.

        bysort ageblock: egen females = total(female)
        sort ageblock females
        by ageblock: replace females = females[1] if females==.

        bysort ageblock: gen pctmale = males/(males+females)

        gen rnd = runiform()

        bysort ageblock: replace sex = 1 if sex==. & death==0 & rnd <= pctmale
        bysort ageblock: replace sex = 0 if sex==. & death==0 & rnd > pctmale

        drop males females rnd pctmale



        bysort agedcat: egen males = total(male) if death==1
        sort agedcat males
        by agedcat: replace males = males[1] if males==.

        bysort agedcat: egen females = total(female) if death==1
        sort agedcat females
        by agedcat: replace females = females[1] if females==.

        bysort agedcat: gen pctmale = males/(males+females)

        gen rnd = runiform()
        replace sex = 1 if sex==. & death==1 & rnd <= pctmale
        replace sex = 0 if sex==. & death==1 & rnd > pctmale

        drop males females rnd pctmale male female death aged agedcat age ageblock

        drop if alive > 1

        compress

            gen matdeath=0
            replace matdeath=1 if mm9==2 | mm9_==3 | mm9_==4 | mm9_==5 | mm9_==6
            
            gen matdeathmiss=matdeath
            replace matdeathmiss=. if mm9_==. | mm9_==98 | mm9_==99
        
                    
            keep if sex==0

            
            gen source = "FILEPATH"
            gen type = "FILEPATH"
            
            local surveyyear = surveyyear

            drop if filename == "FILEPATH" & (yod >= 1999 & yod <= 2007)
            drop if filename == "FILEPATH" & (yod >= 1999 & yod <= 2006)
            drop if filename == "FILEPATH" & (yod >= 1999 & yod <= 2006)
            drop if filename == "FILEPATH" & (yod >= 1999 & yod <= 2008 )
            drop if filename == "FILEPATH" & (yod >= 1999 & yod <= 2008 )

            save "FILEPATH", replace

}


}


capture log close

 
