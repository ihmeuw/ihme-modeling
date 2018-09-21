**********************************************************************************
**Purpose: Compile all prepped files and save 
***********************************************************************

clear all
set more off
pause off
set trace off
capture restore, not

if c(os) == "Windows" {
	global prefix "J:"
}
if c(os) == "Unix" {
	global prefix "/home/j"
}

** Working directories
global log_dir "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/logs"
global data_dir "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data"
global used_file "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data/DHS_used_file.csv"

** start log
cap log close
cd "$log_dir"
log using "dp4.smcl"

** New data option (0 means run all DHS, 1 means just run new DHS)
set obs 1
gen a = "$arg"
destring a, replace
global newflag  = a

di in red "newflag: $newflag"

** delete the current pooled and unpooled estimates if we are running all maternal
if ($newflag == 0) {
    cap erase "$data_dir/dhs_rates_pooled_check.dta"
    cap erase "$data_dir/dhs_rates_unpooled_check.dta"
}
	
** the following block of code develops an if statement to only keep countries for which there is NEW data to include.
if ($newflag == 1) {
insheet using "$used_file", clear
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
			** " this is just to get the quotes back to normal in notepad++ so it's more readable (David Phillips March 2, 2011)
      }
        local ifstatement = substr(`"`ifstatement'"',1,length(`"`ifstatement'"')-2)
      dis `"`ifstatement'"'
** end block
** "
}

** drop countries that have new files, created above, from the pooled and unpooled summary files of all countries, already in existence
if ($newflag == 1) {
	capture confirm file "$data_dir/dhs_rates_pooled_check.dta"
	if (_rc == 0) {
		use "$data_dir/dhs_rates_pooled_check.dta", clear
		drop `ifstatement' 
		saveold "$data_dir/dhs_rates_pooled_check.dta", replace
        use "$data_dir/dhs_rates_unpooled_check.dta", clear
        drop `ifstatement' 
        saveold "$data_dir/dhs_rates_unpooled_check.dta", replace
	}
}

** for countries that you've just added, re-collapse them, and resave the tabulated dataset, for all countries
insheet using "$used_file", clear
if ($newflag == 1) keep if new == 1
levelsof country, local(countries) clean

foreach country of local countries {
	di in red "working on `country'"
    cap use "$data_dir/bycountry/`country'.dta", clear
    if (_rc != 0 | _N == 0) continue
    
    gen iso3="`country'"
	** Tag Brazil subnational
	if iso3 == "BRA" {
		label drop sstate
		replace iso3 = iso3 + string(sstate)
	}
    ** Tag ZAF provinces
    if "`country'" == "ZAF" {
        replace iso3 = iso3 + substr(id,-1,.)
    }	
	** Tag Kenya provinces
	if "`country'" == "KEN" {
		split id, p("p")
		** keep the cluster variable as a district identifier in the 2008-2009 data
		tostring v001, replace
		replace v001 = "" if v001 == "."
        replace iso3 = iso3 + id2 + " c" + v001 if regexm(id, "2008_2009")
		replace iso3 = iso3 + id2 if ! regexm(id, "2008_2009")
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
    capture append using "$data_dir/dhs_rates_pooled_check.dta"
    saveold "$data_dir/dhs_rates_pooled_check.dta", replace
}
        
** also save an "un-pooled" 
insheet using "$used_file", clear
if ($newflag == 1) keep if new == 1
levelsof country, local(countries) clean

 foreach country of local countries {
	di in red "working on `country'"
    cap use "$data_dir/bycountry/`country'.dta", clear
    if (_rc != 0 | _N == 0) continue
    
    gen iso3="`country'"
    
	** Tag Brazil subnational
	if iso3 == "BRA" {
		label drop sstate
		replace iso3 = iso3 + string(sstate)
	}
    ** Tag ZAF provinces
    if "`country'" == "ZAF" {
        replace iso3 = iso3 + substr(id,-1,.)
    }	
	** Tag Kenya provinces
	if "`country'" == "KEN" {
		split id, p("p")
		** keep the cluster variable as a district identifier in the 2008-2009 data
		tostring v001, replace
		replace v001 = "" if v001 == "."
        replace iso3 = iso3 + id2 + " c" + v001 if regexm(id, "2008_2009")
		replace iso3 = iso3 + id2 if ! regexm(id, "2008_2009")
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
    capture append using "$data_dir/dhs_rates_unpooled_check.dta"
    saveold "$data_dir/dhs_rates_unpooled_check.dta", replace
}

** save archived versions
copy "$data_dir/dhs_rates_unpooled_check.dta" "$data_dir/archive/dhs_rates_unpooled_check_$S_DATE.dta", replace
copy "$data_dir/dhs_rates_pooled_check.dta" "$data_dir/archive/dhs_rates_pooled_check_$S_DATE.dta", replace
  
log close
