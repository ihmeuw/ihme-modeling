********************************************************
** Author: USERNAME
** Date created: August 03, 2009
** Description:
** Combines the reshaped population and death data into one file with one observation
** for each census pair. It formats the reshaped data so that each observation has
** the population at the first census, the population at the second census and the average annual deaths.
**
** NOTE: IHME OWNS THE COPYRIGHT

** Set up Stata

cap program drop combine_reshaped
program define combine_reshaped

clear
set more off
set mem 500m
set maxvar 32000
pause on

********************************************************
** Set parameters

syntax, popdata(string) deathsdata(string) saveas(string)

********************************************************
** Analysis code

use "`popdata'", clear
count 
if (`r(N)'==0) { 
	local pop_count = 0 
} 
else {

	keep if source_type == "CENSUS" | source_type == "pop_registry" | (source_type == "DSP" & dsp_use_ddm == 1)
	local pop_count = `r(N)'
}

use "`deathsdata'", clear
count
if (`r(N)'==0) { 
	local death_count = 0 
} 
else {
	count if source_type != "NA"
	local death_count = `r(N)'
}

if (`pop_count' > 0 & `death_count' > 0) { 

	levelsof id if source_type != "DSP", local(source_types_loc)
	tempfile vrdata
	save `vrdata', replace

	if "`iso3'" != "all" use "`popdata'" if strpos(ihme_loc_id,"`iso3'") != 0, clear
	else use "`popdata'", clear

	keep if source_type == "CENSUS" | source_type == "pop_registry" | (source_type == "DSP" & dsp_use_ddm == 1)

	tempfile master
	save `master'
	tempfile master_new
	save `master_new'
	
	clear 
	tempfile new_pop
	save `new_pop', replace emptyok
	
	foreach stl of local source_types_loc {
		di in red "Processing `stl'"
		use `master', clear
		keep if (source_type=="CENSUS" | source_type == "pop_registry") & ihme_loc_id == substr("`stl'",1,strpos("`stl'", "&&")-1) & sex == substr("`stl'", strpos("`stl'", "&&")+2, strpos("`stl'", "@@") - strpos("`stl'", "&&")-2)
		replace id = "`stl'"
		save `new_pop', replace
		use `master_new', clear
		append using `new_pop'
		save `master_new', replace
	}
	use `master_new', clear
	drop source_type sex
	duplicates drop

	merge 1:1 id pop_years using "`vrdata'"
	drop _merge
	drop if deaths_years == "NA"

	quietly {

	forvalues j = 0/100 {
		rename agegroup`j' agegroupv_`j'
	}

	g allagegroups1 = ""
	g allagegroups2 = ""
	g allagegroupsv = ""
	forvalues j = 0/100 {
		replace allagegroups1 = allagegroups1 + "," + string(agegroup1_`j') if agegroup1_`j' ~= .	
		replace allagegroups2 = allagegroups2 + "," + string(agegroup2_`j') if agegroup2_`j' ~= .	
		replace allagegroupsv = allagegroupsv + "," + string(agegroupv_`j') if agegroupv_`j' ~= .	
	}

	replace allagegroups1 = allagegroups1 + ","
	replace allagegroups2 = allagegroups2 + ","
	replace allagegroupsv = allagegroupsv + ","

	g newagegroups = ""
	forvalues j = 0/100 {
		replace newagegroups = newagegroups + ",`j'" if strpos(allagegroups1,",`j',") ~= 0 & strpos(allagegroups2,",`j',") ~= 0 & strpos(allagegroupsv,",`j',") ~= 0
	}
	replace newagegroups = newagegroups + ","

	replace newagegroups = subinstr(newagegroups,",0,1,5,",",0,5,",1)

	drop agegroup* allagegroups*

	forvalues j = 0/100 {
		g agegroup`j' = .
	}


	gen count = 0
	forvalues j = 0/100 {
		forvalues k = 0/100 {
			replace agegroup`k' = `j' if strpos(newagegroups,",`j',") ~= 0 & count == `k'
		}
		replace count = count + 1 if strpos(newagegroups,",`j',") ~= 0
	}

	drop newagegroups

	forvalues j = 0/100 {
		g newpop1_`j' = .
		g newpop2_`j' = .
		g newdeaths_`j' = .
	}

	}

	foreach iteration in pop1 pop2 deaths {
	forvalues j = 0/50 {
		di "`j'"
		quietly {
		local jplus = `j'+1

		levelsof agegroup`j', local(ag1)
		levelsof agegroup`jplus', local(ag2)
		
		foreach a1 of local ag1 {
			foreach a2 of local ag2 {
				di "A1 AND A2 `a1' `a2'"
				if(`a1' < `a2') {
					local endloc = `a2' - 1
					if "`iteration'" != "deaths" local prefix "_" 
					else local prefix ""
					egen temp`a1'`a2' = rowtotal(`iteration'`prefix'`a1'- `iteration'`prefix'`endloc')
					replace new`iteration'_`j' = temp`a1'`a2' if agegroup`j' == `a1' & agegroup`jplus' == `a2' & new`iteration'_`j' == .
					drop temp`a1'`a2'
				}			
			}

			egen temp`a1'`a2' = rowtotal(`iteration'`prefix'`a1'-`iteration'`prefix'100) 
			replace new`iteration'_`j' = temp`a1'`a2' if agegroup`j' == `a1' & agegroup`jplus' == . & new`iteration'_`j' == .
			drop temp`a1'`a2'
		}
		}
	}
	}

	drop pop1* pop2* deaths0-deaths100


	forvalues j = 0/100 {
		rename newpop1_`j' c1_`j'
		rename newpop2_`j' c2_`j'
		rename newdeaths_`j' vr_`j'		
	}


	gen year1 = substr(pop_years, 1, strpos(pop_years," ") - 1)
	gen year2 = substr(pop_years,strpos(pop_years," ") + 1,.)
	destring year1, replace
	destring year2, replace
	replace year1 = floor(year1)
	replace year2 = floor(year2)
	replace year1 = year1 - 1 if year1 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(year2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
	replace year2 = year2 - 1 if year2 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(year2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
	gen date1 = substr(pop_years, 1, strpos(pop_years," ") - 1)
	gen date2 = substr(pop_years, strpos(pop_years," ") + 1,.)
	destring date1, replace
	destring date2, replace
	g time = date2 - date1

	g tmp_year1 = year1
	g tmp_year2 = year2
	tostring tmp_year1, replace
	tostring tmp_year2, replace

	g new_pop_years = tmp_year1 + " " + tmp_year2
	
	replace pop_years = new_pop_years
	drop new_pop_years tmp_year1 tmp_year2


	g numofvr = wordcount(deaths_years)
	g vryear = deaths_years if numofvr == 1
	destring vryear, replace

	g popyearhat = 1
	forvalues j = 0/100 {
		replace popyearhat = c1_`j'*exp((vryear-date1)*(1/time)*log(c2_`j'/c1_`j')) if numofvr == 1 & (vryear ~= year1 & vryear ~= year2 & vryear ~= (year2-1)) & (vryear-date1) <= (date2-vryear)
		replace popyearhat = c2_`j'*exp(-1*(date2-vryear)*(1/time)*log(c2_`j'/c1_`j')) if numofvr == 1 & (vryear ~= year1 & vryear ~= year2 & vryear ~= (year2-1)) & (vryear-date1) > (date2-vryear)
		

		replace vr_`j' = (vr_`j'/c2_`j')*sqrt(c1_`j'*c2_`j') if numofvr == 1 & (vryear == year2 | vryear == (year2-1))
		replace vr_`j' = (vr_`j'/c1_`j')*sqrt(c1_`j'*c2_`j') if numofvr== 1 & (vryear == year1)
		replace vr_`j' = (vr_`j'/popyearhat)*sqrt(c1_`j'*c2_`j') if numofvr == 1 & (vryear ~= year1 & vryear ~= year2 & vryear ~= (year2-1)) & (vryear-date1) <= (date2-vryear)
	}

	drop year1 year2 date1 date2 numofvr vryear popyearhat

	duplicates drop *, force
	duplicates drop ihme_loc_id sex pop_years source_type, force
	replace ihme_loc_id = ihme_loc_id + "&&" + source_type 

	save "`saveas'", replace
} 
else { 
	clear
	gen temp = . 
	save "`saveas'", replace 
}

di "DONE"

end
