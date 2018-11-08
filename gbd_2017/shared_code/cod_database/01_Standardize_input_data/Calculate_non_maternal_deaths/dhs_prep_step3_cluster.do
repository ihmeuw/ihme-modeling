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

** Working directories
global log_dir "FILEPATH"
global cod_out_dir "FILEPATH"
global cod_archive "FILEPATH"
global used_file "FILEPATH"

** set parameters
set obs 1
gen arg = "$arg"
split arg, parse(-) generate(a) destring
global newflag = a1
local country = a2   

** start log
cap log close
cd "$log_dir"
log using "FILEPATH", replace

di in red "newflag: $newflag"
di in red "country: `country'"


capture erase "$FILEPATH"


insheet using "FILEPATH", clear
keep if country == "`country'"                                                                        

if ($newflag == 1) keep if new==1 

replace year = substr(year,1,4)
levelsof year, local(years)
foreach year of local years {
    capture use "FILEPATH", clear
    if (_rc != 0) continue
    di in red "FILEPATH"
    
    gen newid = id+"-"+filename 


    qui count if alive > 1
    local n = r(N)
    di "Dropping `n' siblings from `country' with unknown alive/dead status"
    drop if alive>1

    qui count if yob==.
    local n = r(N)
    di "Dropping `n' siblings from `country' with no year of birth information"
    drop if yob==.

    qui count if yod==. & alive==0
    local n = r(N)
    di "Dropping `n' dead siblings from `country' with no year of death information"
    drop if yod==. & alive==0

    drop alive



    bysort id_sm: gen bi = _N
    gen alive = 1 if yod == .
    replace alive = 0 if yod != .
    bysort id_sm : egen si_old = sum(alive)

    bysort id_sm : egen si = total(alive) if sex==0 & (yr_interview+1900 - yob)>=15 & (yr_interview+1900 - yob)<=49
    gen gkwt_old = bi/si_old
    sort id_sm si
    by id_sm: replace si=si[1]
    gen gkwt = 1/si
    replace gkwt = 1 if gkwt == .

    gen samplewt = v005
    gen totalwt = samplewt*gkwt
    egen totalwt_total = total(totalwt)
    ** Use sampling weights as well as Gakidou-King (GK) weights. 
    replace totalwt = totalwt/totalwt_total                           
    label var totalwt "FILEPATH"
    drop v005 bi alive si si_old 

    drop if yob > surveyyear
    drop if yod > surveyyear & yod !=. 

    levelsof surveyyear

    egen lastsurvey = max(surveyyear)

    gen cy1 = lastsurvey - 1                        

    local cy1 = cy1[1]

    forvalues x = 2/55 {                        
        local one = `x'-1

        gen cy`x' = cy1 - `one'    
  
        local cy`x' = cy`x'[1]                    
        disp `cy`x''
        drop cy`x'
    }

    drop lastsurvey

    expand 15                            
    bysort id_sm sibid: gen order = _n
    gen calcyear = surveyyear
    gen yrcatstart = calcyear - order + 1

    ********AGE:
    gen age = .                                 

    replace age = yrcatstart - yob

    replace age = . if age < 0                                

    drop if age < 15                                
    drop if age > 59


    gen ageblock = . 
    forvalues age = 0(5)60   {
        replace ageblock =`age' if age - `age'  < 5 & age - `age' >= 0
    }
    replace ageblock = 60 if ageblock > 60 & ageblock != . 
    tab ageblock, gen(dumage) label

    di "Generating country-year blocks"
    gen yearblock = . 

    replace yearblock = yrcatstart
                
    gen twodigyrblock = yearblock - 1900
    
    drop if twodigyrblock==.

    tostring twodigyrblock , gen(stryrblock)

    gen stryrblock3dig = stryrblock if twodigyrblock > 99            
    replace stryrblock3dig = "0"+stryrblock if twodigyrblock < 100
    drop stryrblock
    rename stryrblock3dig stryrblock

    gen cy = country+stryrblock

    destring stryrblock, gen(year)
    replace year = 1900 + year

        
    di "Generating Dead variable"             
 
    gen dead = .

    replace dead = 1 if yod !=. & yod == yrcatstart 
    replace dead = . if yod !=. & yod < yrcatstart
    replace dead = 0 if yod !=. & yod > yrcatstart & yob <= yrcatstart
    replace dead = . if yod != . & yod > yrcatstart & yob > yrcatstart

    replace dead = 0 if yod ==. & yob <= yrcatstart
    drop if dead == . 

    generate dead_mat = 1 if matdeath == 1 & dead == 1

    local keep_vars "dead dead_mat ageblock matdeath yearblock totalwt surveyyear filename matdeathmiss sex type source gkwt samplewt totalwt_total ageblock country"

    local optional_vars "sstate id v001"
    foreach x of local optional_vars {
        capture confirm variable `x'
        if !_rc {
            local keep_vars "`keep_vars' `x'"
        }
    }
    keep `keep_vars'
    
    capture append using "FILEPATH"
    save "FILEPATH", replace
}

copy "FILEPATH", replace

capture log close
