cap program drop orderednaming
program define orderednaming

syntax, data(string) iso3(string) type(string) version(string)


clear
set mem 500m
set more off

use "`data'", clear
di _N

if("`iso3'" ~= "all") {
	keep if ihme_loc_id == "`iso3'"
}

// Preliminary data management
if (_N > 0) {
	g keepit0to = 0
	lookfor DATUM0to
	return list
	local count = 0
	foreach var of varlist `r(varlist)' {
		replace keepit0to = keepit0to + 1 if `var' == .
		local count = `count' + 1
	}
	g numof0to = `count'

	drop if numof0to == keepit0to
	// pause
	drop numof0to keepit0to 

	g keepit = 0
	lookfor DATUM
	return list
	foreach var of varlist `r(varlist)' {
		replace keepit = 1 if `var' ~= .
	}
	
	keep if keepit == 1	

	g dropit = 0
}

if (_N > 0) { 

	capture drop DATUMUNK
	capture drop DATUMTOT
	local startobs = 1

	lookfor plus
	return list

	g pluscount = 0

	foreach var of varlist `r(varlist)' {
		replace pluscount = pluscount + 1 if `var' ~= .
	}

	drop pluscount keepit
	lookfor plus
	return list

	capture: drop openend
	g openend = .
	foreach var of varlist `r(varlist)' {
		di "VAR: `var'"
		local openend = substr("`var'",6,strpos("`var'","plus")-6)
		replace openend = `openend' if `var' ~= .
	}

	lookfor DATUM
	return list

	foreach var of varlist `r(varlist)' {
		di in red "`var'"
		if(strpos("`var'","plus") == 0) {
			local lower = substr("`var'",6,strpos("`var'","to")-6)
			replace `var' = . if openend <= `lower'
		}
	}

	local obser = _N
	qui forvalues j = `startobs'(1)`obser' {
		noisily: di "`j' of `obser'"
		preserve
		keep if _n == `j'
		lookfor DATUM
		return list
		foreach var of varlist `r(varlist)' {
			quietly: levelsof `var', clean local(dropit)

			if("`dropit'" == "") {
				quietly: drop `var' 
			}
		}
		
		levelsof ihme_loc_id, local(countryloc)
		levelsof year, local(yearloc)
		levelsof sex, local(sexloc)
		
		lookfor DATUM0to
		return list
		local vars = "`r(varlist)'"	
		if(strpos("`vars'"," ") ~= 0) {
			foreach v of varlist `vars' {
				local lower = substr("`v'",6,strpos("`v'","to")-6)
				local upper = substr("`v'",strpos("`v'","to")+2,.)
				local diff`v' = `upper'-`lower'
			}
			local diff = 0
			foreach v of varlist `vars' {
				if(`diff`v'' > `diff') {
					local diff = `diff`v''
				}
			}
			foreach v of varlist `vars' {
				if(`diff`v'' == `diff') {
					local vars = "`v'"
				}
			}
		}

		local next = substr("`vars'",strpos("`vars'","to")+2,.)
		
		g agegroup0to4 = "`vars'"
		g DAT0to4 = `vars'

		local next = `next'+1

		forvalues k = 2(1)120 {
			local age = (`k'-1)*5
			local ageplus = `age'+4

			lookfor DATUM`next'to
			return list
			local vars = "`r(varlist)'"

			if(strpos("`vars'"," ") ~= 0) {
				foreach v of varlist `vars' {
					local lower = substr("`v'",6,strpos("`v'","to")-6)
					local upper = substr("`v'",strpos("`v'","to")+2,.)
					local diff`v' = `upper'-`lower'
				}
				local diff = 0
				foreach v of varlist `vars' {
					if(`diff`v'' > `diff') {
						local diff = `diff`v''
					}
				}
				foreach v of varlist `vars' {
					if(`diff`v'' == `diff') {
						local vars = "`v'"
					}
				}
			}

			if("`vars'" == "") {
				g agegroup`age'plus = "DATUM`next'plus"
				capture: g DAT`age'plus = DATUM`next'plus

				if(_rc == 111) {
					replace dropit = 1 
				}
				continue, break
			}
			else {
				local next = substr("`vars'",strpos("`vars'","to")+2,.)
				local nextplus = `next'+4
				g agegroup`age'to`ageplus' = "`vars'"
				g DAT`age'to`ageplus'= `vars' 
				local next = `next'+1
			}
		}

		drop DATUM*
		
		lookfor DAT
		return list
		foreach var of varlist `r(varlist)' {
			local newname = subinstr("`var'","DAT","DATUM",.)
			rename `var' `newname'
		}

		tempfile ob`j'
		save `ob`j'', replace

		restore
	}

	use `ob1', clear
	forvalues j = 2(1)`obser' {
		append using `ob`j''
	}

	tab dropit
	drop if dropit == 1
	drop dropit openend

	lookfor DATUM
	return list

	di "DONE"

}

else { 
	clear
	gen temp = . 
}

end

