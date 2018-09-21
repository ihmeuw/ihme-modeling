*** shock uncertainty on adult age groups ***
*** 10/24/2011

** updating 5/5/2012
** Updated dec 2015 by NAME to use combined war/disaster dataset
clear
clear matrix
cap restore, not
set more off
set memory 8000m
set seed 1234567

	if c(os) == "Windows" {
		global prefix ""
	}
	else {
		global prefix ""
		set odbcmgr unixodbc
	}
 
 local input_folder "FILEPATH"
 global outdir "FILEPATH"
 
** Date
local date = string(date("`c(current_date)'", "DMY"), "%tdDD!_NN!_CCYY")


** STEP 1. IMPORT DATA, GET UNCERTAINTY
use "FILEPATH", clear
rename disaster_rate rate
rename l_disaster_rate l_rate
rename u_disaster_rate u_rate
rename pop total_population
rename best deaths_best
rename lower deaths_low
rename upper deaths_high
//rename acause cause
gen source = "Ebola_WHO"

	
	drop if deaths_best < deaths_low
	
	
		gen sds1 = (deaths_best - deaths_low) / 2
		gen sds2 = (deaths_high - deaths_best) / 2
		egen sds = rowmax(sds1 sds2)
		
		replace deaths_best = rate * tot if sds == 0
		replace deaths_low = l_rate * tot if sds == 0
		replace deaths_high = u_rate * tot if sds == 0
		
		replace sds1 = (deaths_best - deaths_low) / 2 if sds == 0
		replace sds2 = (deaths_high - deaths_best) / 2 if sds == 0
		egen sds_rep = rowmax(sds1 sds2)
		replace sds = sds_rep if sds == 0
		
		// some sds are too large and it pushes up the mean estimate because too many 0 draws are removed;
		// if they are above mean/1.96, then replace with mean/1.96 to ensure that 95% of normal distribution is positive
		gen max_sd = deaths_best / 1.96
		replace sds = max_sd if sds > max_sd
		
		keep if deaths_best != 0
		
	
	
keep year iso3 location_id cause source age sex deaths_best sds
gen id=_n
local nn=_N
tempfile draws
noisily dis in red "deaths drawing"
quietly forvalues j=1/`nn' {
    preserve
                keep if id==`j'
                // mat bb=shock_deaths_best[1]
				mat bb=deaths_best[1]
                mat sds=sds[1]
                local iso=iso3[1]
				local loc_id=location_id[1]
                local yy=year[1]
				local aa=age[1]
				local ss=sex[1]
				local cc=cause[1]
				local src = source[1]
                set seed 1234567
                drawnorm deaths, n(1200) mean(bb) sds(sds) clear
                keep if deaths>0
                set seed 1234567
                sample 1000, count
                gen iso3="`iso'"
				gen location_id = `loc_id'
                gen year=`yy'
				gen age=`aa'
				gen sex="`ss'"
				gen cause="`cc'"
				gen source="`src'"
                gen sim=_n
                cap append using `draws'
                save `draws',replace
                restore
                noisily dis "`j' of `nn' sampling done"
}

use `draws', clear
save "FILEPATH", replace
save "FILEPATH", replace


