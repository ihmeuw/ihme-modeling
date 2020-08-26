    capture restore
    clear all
    set mem 1000m

    set more off
    ssc install bygap


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

    run "FILEPATH"

    create_timestamp
    global timestamp = "`r(timestamp)'"



global log_dir "FILEPATH"
global cod_out_dir "FILEPATH"
global cod_archive "FILEPATH"


cap log close
cd "$log_dir"
log using "FILEPATH", replace


set obs 1
gen a = "$arg"
destring a, replace
global newflag  = a


di in red "newflag: $newflag"

if ($newflag == 0) {
    capture erase "FILEPATH"
    capture erase "FILEPATH"
}

if ($newflag == 1) {
insheet using "FILEPATH", clear
      keep if new == 1
      levelsof country, local(countries) clean
      local ifstatement = "if "
      local clist = ""
      local count = wordcount("`countries'")
      forvalues x=1/`count' {
            local name`x' = word("`countries'",`x') 
            local name`x' = "`name`x''"
            dis `"`name`x''"'
                  local c`x' = "iso3==" + `"""' + `"`name`x''"' + `"""' + `" | "'
            dis `"`c`x''"'
            local ifstatement = `"`ifstatement'"' + `"`c`x''"'
            dis `"`ifstatement'"'
      }
        local ifstatement = substr(`"`ifstatement'"',1,length(`"`ifstatement'"')-2)
      dis `"`ifstatement'"'
}
** end block

if ($newflag == 1) {
    capture confirm file "FILEPATH"
    if (_rc == 0) {
        use "FILEPATH", clear
        drop `ifstatement' 
        save "FILEPATH", replace
        use "FILEPATH", clear
        drop `ifstatement' 
        save "FILEPATH", replace
    }
}

import delimited using "FILEPATH", clear
if ($newflag == 1) keep if new == 1 

levelsof country, local(countries) clean
foreach country of local countries {
    capture use "FILEPATH", clear
    if (_rc != 0 | _N == 0) continue

    di in red "working on `country'"
    
    gen iso3="`country'"
    
    if iso3 == "BRA" {
        capture label drop sstate
        replace iso3 = iso3 + string(sstate)
    }
    if "`country'" == "ZAF" {
        replace iso3 = iso3 + substr(id,-1,.)
    }    
    if "`country'" == "KEN" {
        split id, p("p")
        tostring v001, replace
        replace v001 = "" if v001 == "."
        replace iso3 = iso3 + id2 + " c" + v001 if regexm(id, "2008_2009")
        replace iso3 = iso3 + id2 if !regexm(id, "2008_2009")
    }    
    if "`country'" == "IDN" {
        replace iso3 = iso3 + substr(id,-2,.)
    }

    if "`country'" == "ETH" {
        split id, p("r_")
        replace iso3 = iso3 + "_" + id2
        drop id1 id2
    }
    if "`country'" == "AFG" {
        drop if filename == ""
    }

    capture drop n
    gen n=1
    gen dead_check=dead
    gen dead_mat_check = dead_mat
    collapse (sum) n dead dead_mat (rawsum) dead_check dead_mat_check [pweight=totalwt], by(iso3 yearblock ageblock)
    rename ageblock agegroup
    rename yearblock year
    rename n popwomen
    rename dead womendeaths
    rename dead_mat matdeaths
    gen pmdf = matdeaths/womendeaths
    gen rate = (matdeaths/popwomen)*100000
    gen ln_rate = ln(rate)
    gen source = "DHS sibling history"
    gen type = "Sibling history"
    capture append using "FILEPATH"
    save "FILEPATH", replace
}
import delimited using "FILEPATH", clear
if ($newflag == 1) keep if new == 1 

levelsof country, local(countries) clean
foreach country of local countries {
    capture use "FILEPATH", clear
    if (_rc != 0 | _N == 0) continue

    di in red "working on `country'"
    
    gen iso3="`country'"
    
    if iso3 == "BRA" {
        capture label drop sstate
        replace iso3 = iso3 + string(sstate)
    }
    if "`country'" == "ZAF" {
        replace iso3 = iso3 + substr(id,-1,.)
    }    
    if "`country'" == "KEN" {
        split id, p("p")
        tostring v001, replace 
        replace v001 = "" if v001 == "."
        replace iso3 = iso3 + id2 + " c" + v001 if regexm(id, "2008_2009")
        replace iso3 = iso3 + id2 if ! regexm(id, "2008_2009")
    }

    if "`country'" == "IDN" {
        replace iso3 = iso3 + substr(id,-2,.)
    } 

    if "`country'" == "ETH" {
        split id, p("r_")
        replace iso3 = iso3 + "_" + id2
        drop id1 id2
    }

    capture drop n
    gen n=1
    gen dead_check=dead
    gen dead_mat_check = dead_mat
    collapse (sum) n dead dead_mat (rawsum) dead_check dead_mat_check [pweight=totalwt], by(iso3 surveyyear yearblock ageblock)
    rename ageblock agegroup
    rename yearblock year
    rename n popwomen
    rename dead womendeaths
    rename dead_mat matdeaths
    gen pmdf = matdeaths/womendeaths
    gen rate = (matdeaths/popwomen)*100000
    gen ln_rate = ln(rate)
    gen source = "DHS sibling history"
    gen type = "Sibling history"
    capture append using "FILEPATH"
    save "FILEPATH", replace
}


** save archived versions
copy "FILEPATH", replace
copy "FILEPATH", replace
  
log close
