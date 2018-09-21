***********************************************************************************
*** under-5 envelope estimation based on qx values from child mortality process ***
*** updated with population estimates ***
***********************************************************************************

clear 
set more off
set seed 1234567
set memory 3000m

di "`1'"
di "`2'"

	if (c(os)=="Unix") {
		global j "FILEPATH"
		global ctmp "FILEPATH"
		set odbcmgr unixodbc
		local ihme_loc_id = "`1'"
		local code_dir "`2'"
		qui do "FILEPATH/get_locations.ado"
	} 
	if (c(os)=="Windows") { 
		global j "FILEPATH"
		qui do "FILEPATH/get_locations.ado"
		local ihme_loc_id = "AFG"
	}
	
	
	** written in run all file
	import delimited "$j/temp/USER/u5_env_locs.csv" 
	keep if level_all == 1
	keep location_id ihme_loc_id
	preserve
	keep if ihme_loc_id == "`ihme_loc_id'"
	levelsof location_id, local(loc_id)
	restore
	keep ihme_loc_id
	tempfile codes
	save `codes', replace
	

tempfile malelts femalelts hmdlts
*** males ***
global working "FILEPATH/mltper_1x1"
local sfiles: dir "$working" files "*.txt", respectcase
foreach file of local sfiles {
	cd "$working"
    infix  year 1-10 str age 11-22 lx 47-55 using `file', clear
	drop if year==.
	gen iso3=reverse(substr(reverse("`file'"),16,.))
	cap append using `malelts'
	save `malelts', replace
}
gen sex="male"
save `malelts', replace

*** females ***
global working "FILEPATH"
local sfiles: dir "$working" files "*.txt", respectcase
foreach file of local sfiles {
	cd "$working"
    infix year 1-10 str age 11-22 lx 47-55 using `file', clear
	drop if year==.
	gen iso3=reverse(substr(reverse("`file'"),16,.))
	cap append using `femalelts'
	save `femalelts', replace
}
gen sex="female"
append using `malelts'
destring age, ignore("+") replace
sort iso3 sex year age
bysort iso3 sex year: gen qx=1-lx[_n+1]/lx
gen agefive=0 if age==0
replace agefive=1 if age>=1 & age<=4
forvalues j=5(5)105 {
	replace agefive=`j' if age>=`j' & age<=`j'+4
}
replace agefive=110 if age==110
sort iso3 sex year agefive age
save `hmdlts'
keep iso3 sex year age agefive lx
keep if age==agefive
keep if year>1949
replace lx=lx/100000
sort iso3 sex year age
bysort iso3 sex year: gen qxfive=1-lx[_n+1]/lx
keep iso3 sex year agefive qxfive
sort iso3 sex year agefive
merge iso3 sex year agefive using `hmdlts'
keep if _merge==3
drop _merge
drop if age>4
tempfile pars
gen lnqx=ln(qx)
gen lnfive=ln(qxfive)
keep if year>1950

sort iso3 sex year age
bysort iso3 sex year: gen qinf=qx[1]
gen lnqinf=ln(qinf)

forvalues j=1/4 {
    preserve
	reg lnqx lnfive if sex=="male" & age==`j'
	clear
	set obs 1
	gen sex="male"
	gen par_lnfive=_b[lnfive]
	gen par_cons=_b[_cons]
	gen age=`j'
	cap append using `pars'
	save `pars',replace
	restore
	preserve
	reg lnqx lnfive if sex=="female" & age==`j'
	clear
	set obs 1
	gen sex="female"
	gen par_lnfive=_b[lnfive]
	gen par_cons=_b[_cons]
	gen age=`j'
	cap append using `pars'
	save `pars',replace
	restore
}


use "FILEPATH/`loc_id'.dta", clear
label drop sim  
replace sim = sim-1
drop sex
drop if sex_id==3
gen sex="male" if sex_id==1
replace sex="female" if sex_id==2
keep ihme_loc_id sex year births sim

	
tempfile births
save `births'

** sims
use "FILEPATH/`ihme_loc_id'_noshocks_sims.dta", clear
rename q_u5 q5
rename simulation sim
tempfile data
save `data'

keep ihme_loc_id sim sex year q_ch
expand 4
sort ihme_loc_id sim sex year
bysort ihme_loc_id sim sex year: gen age=_n
merge m:1 sex age using `pars'
keep if _merge==3
drop _merge
gen lnqch=ln(q_ch)
gen pqx=exp(par_lnfive*lnqch+par_cons)
keep ihme_loc_id sim sex year age pqx q_ch
rename pqx qx
reshape wide qx, i(ihme_loc_id sim sex year) j(age)

gen pv4q1=1-(1-qx1)*(1-qx2)*(1-qx3)*(1-qx4)
egen id=group(ihme_loc_id sim sex year)
cap sum id
local nid=r(max)
tempfile singles adjs
save `singles'

keep if pv4q1<q_ch
local cnum = 10
local citer = 20
local ccat=1
gen crmax = 2 
gen crmin = 0.0000000000000001 
gen cadj = .
gen adj4q1_min = .
gen r_min = .
gen diffc_min = .
while `ccat'<=`citer' {
	local cq=0
	while `cq'<=`cnum' {
		cap: gen y`cq' = .
		replace y`cq' = crmin + `cq'*((crmax-crmin)/`cnum')
		local cqq=`cq'+1
		cap: gen r`cqq' = .
		replace r`cqq'=y`cq'
		local cq=`cq'+1 
		
		cap: gen adj4q1`cqq' = .
		replace adj4q1`cqq' = 1-(1-qx1*(1+r`cqq'))*(1-qx2*(1+r`cqq'))*(1-qx3*(1+r`cqq'))*(1-qx4*(1+r`cqq'))
			
		cap: gen diffc`cqq' = .
		replace diffc`cqq'=abs(adj4q1`cqq'-q_ch) if adj4q1`cqq' != 0
	}	
	
	cap: drop diffc_min
	egen diffc_min = rowmin(adj4q11-adj4q111) 
	
	replace r_min = .
	forvalues cqq = 1/11 {
			replace r_min = r`cqq' if diffc`cqq' == diffc_min 
	}

	count if r_min == .
	if(`r(N)' != 0) {
		noisily: di "`ccat'"
		pause
	}
	
	replace cadj=abs(crmax-crmin)/10
	replace crmax=r_min+2*cadj
	replace crmin=r_min-2*cadj
	replace crmin=0.000000001 if crmin < 0 
		
	local ccat=`ccat'+1
}
forvalues j=1/4 {
	replace qx`j'=qx`j'*(1+r_min) 
}
tempfile qx_less
save `qx_less', replace
********************************************************
use `singles', clear
keep if pv4q1>q_ch
local cnum = 10
local citer = 20
local ccat=1

gen crmin = -2 
gen crmax = -0.0000000000000001 
gen cadj = .
gen adj4q1_min = .
gen r_min = .
gen diffc_min = .

while `ccat'<=`citer' {
	local cq=0
	while `cq'<=`cnum' {
		cap: gen y`cq' = .
		replace y`cq' = crmin + `cq'*((crmax-crmin)/`cnum')
		local cqq=`cq'+1
		cap: gen r`cqq' = .
		replace r`cqq'=y`cq'
		local cq=`cq'+1 
		
		cap: gen adj4q1`cqq' = .
		replace adj4q1`cqq' = 1-(1-qx1*(1+r`cqq'))*(1-qx2*(1+r`cqq'))*(1-qx3*(1+r`cqq'))*(1-qx4*(1+r`cqq'))
		
		cap: gen diffc`cqq' = .
		replace diffc`cqq'=abs(adj4q1`cqq'-q_ch) if adj4q1`cqq' != 0
	}	
	
	cap: drop diffc_min
	egen diffc_min = rowmin(adj4q11-adj4q111)
	
	replace r_min = .
	forvalues cqq = 1/11 {
		replace r_min = r`cqq' if abs(diffc`cqq' == diffc_min) < .00001 & diffc`cqq' != .
	}
		replace cadj=abs(crmax-crmin)/10
	replace crmax=r_min+2*cadj
	replace crmin=r_min-2*cadj
	replace crmax=-0.000000001 if crmax > 0
	
	local ccat=`ccat'+1
}
	forvalues j=1/4 {
	replace qx`j'=qx`j'*(1+r_min)
}
tempfile qx_more
save `qx_more', replace
********************************************************
use `singles', clear
keep if pv4q1 == q_ch
append using `qx_less'
append using `qx_more'
keep ihme_loc_id sim sex year qx*
tempfile adjs
save `adjs'
use `data', clear
keep ihme_loc_id sim sex year sim q_*
rename q_enn qxenn
rename q_lnn qxlnn
rename q_pnn qxpnn
merge 1:1 ihme_loc_id sim sex year using `adjs'

keep if _merge==3
drop _merge
replace year=floor(year)

save `adjs',replace
	*** get DAY specific qx values ***
gen dqxenn=1-(1-qxenn)^(1/7)
gen dqxlnn=1-(1-qxlnn)^(1/21)
gen dqxpnn=1-(1-qxpnn)^(1/337)
gen dqx1=1-(1-qx1)^(1/365)
gen dqx2=1-(1-qx2)^(1/365)
gen dqx3=1-(1-qx3)^(1/365)
gen dqx4=1-(1-qx4)^(1/365)
save `adjs',replace

use `births',clear
keep if ihme_loc_id == "`ihme_loc_id'"

replace births=births/52
expand 52
sort ihme_loc_id sim sex year
bysort ihme_loc_id sim sex year: gen double btime=year+(1/52)*(_n-0.5)
tempfile weeks
*** get the start and end time of each age group for each birth-week cohort
gen double length_enn=7/365
gen double length_lnn=21/365
gen double length_pnn=337/365
forvalues j=1/4 {
   gen double length_`j'=1
}
gen double start_time_enn=btime
gen double start_time_lnn=btime+length_enn
gen double start_time_pnn=btime+length_enn+length_lnn
forvalues j=1/4 {
   gen start_time_`j'=btime+`j'
}
local vars="enn lnn pnn 1 2 3 4"
foreach v of local vars {
	gen end_time_`v'=start_time_`v'+length_`v'
}
*** get the year values for all the time variables
foreach v of local vars {
	gen start_year_`v'=floor(start_time_`v')
	gen end_year_`v'=floor(end_time_`v')
}
gen start_size_enn=births
save `weeks'

**** get deaths and pops:age group by age group
qui foreach v of local vars {
	use `adjs',clear
	keep ihme_loc_id sim sex year dqx`v'
	rename year start_year_`v'
	rename dqx`v' dqx`v'_sy
	merge 1:m ihme_loc_id sim sex start_year_`v' using `weeks'
	drop if _merge==1
	drop _merge
	save `weeks',replace
	use `adjs',clear
	keep ihme_loc_id sim sex year dqx`v'
	rename year end_year_`v'
	rename dqx`v' dqx`v'_ey
	merge 1:m ihme_loc_id sim sex end_year_`v' using `weeks'
	drop if _merge==1
	drop _merge
	
	** get the days spent in the starting year and the ending year
	gen days_`v'_sy=round(length_`v'*365,1) if start_year_`v'==end_year_`v'
	replace days_`v'_sy=round((1-(start_time_`v'-start_year_`v'))*365, 1) if start_year_`v'<end_year_`v' 
	gen days_`v'_ey=round(length_`v'*365,1)-days_`v'_sy if start_year_`v'<end_year_`v'
	replace days_`v'_ey=0 if start_year_`v'==end_year_`v'
*** calculate deaths & pops by day ***
	gen deaths_sy_`v'=(1-(1-dqx`v'_sy)^days_`v'_sy)*start_size_`v'
	gen mid_size_`v'=((1-dqx`v'_sy)^days_`v'_sy)*start_size_`v'
	gen deaths_ey_`v'=(1-(1-dqx`v'_ey)^days_`v'_ey)*mid_size_`v'
	gen end_size_`v'=((1-dqx`v'_ey)^days_`v'_ey)*mid_size_`v'
	gen PY_sy_`v'=(1-(1-dqx`v'_sy)^days_`v'_sy)*start_size_`v'*(days_`v'_sy/2)/365+((1-dqx`v'_sy)^days_`v'_sy)*start_size_`v'*days_`v'_sy/365
	gen PY_ey_`v'=(1-(1-dqx`v'_ey)^days_`v'_ey)*mid_size_`v'*(days_`v'_ey/2)/365+((1-dqx`v'_ey)^days_`v'_ey)*mid_size_`v'*days_`v'_ey/365
	
	if "`v'"=="enn" {
		gen start_size_lnn=end_size_enn
	}
	if "`v'"=="lnn" {
		gen start_size_pnn=end_size_lnn
	}
	if "`v'"=="pnn" {
		gen start_size_1=end_size_pnn
	}
	if "`v'"=="1" {
		gen start_size_2=end_size_1
	}
	if "`v'"=="2" {
		gen start_size_3=end_size_2
	}	
	if "`v'"=="3" {
		gen start_size_4=end_size_3
	}
	save `weeks',replace
}
local vars="enn lnn pnn 1 2 3 4"
foreach v of local vars {
	egen sum_deaths_sy_`v'=sum( deaths_sy_`v'), by(ihme_loc_id sim sex start_year_`v')
	egen sum_deaths_ey_`v'=sum( deaths_ey_`v'), by(ihme_loc_id sim sex end_year_`v')
	egen sum_PY_sy_`v'=sum( PY_sy_`v'), by(ihme_loc_id sim sex start_year_`v')
	egen sum_PY_ey_`v'=sum( PY_ey_`v'), by(ihme_loc_id sim sex end_year_`v')
}
keep ihme_loc_id sim sex start_year* end_year* sum_deaths* sum_PY*
duplicates drop ihme_loc_id sim sex start_year_enn end_year_enn,force
tempfile raw
save `raw'
tempfile deaths
foreach v of local vars {
	use `raw',clear
	keep ihme_loc_id sim sex sum_deaths_sy_`v' start_year_`v' sum_PY_sy_`v'
	rename start_year year
	duplicates drop ihme_loc_id sim sex year, force
	collapse (sum) sum_deaths_sy_`v' sum_PY_sy_`v', by(ihme_loc_id sim sex year)
	rename sum_deaths deaths
	rename sum_PY pys
    gen age="`v'"
	cap append using `deaths'
	save `deaths',replace
	use `raw',clear
	keep ihme_loc_id sim sex sum_deaths_ey_`v' sum_PY_ey_`v' end_year_`v'
	rename end_year year
	duplicates drop ihme_loc_id sim sex year, force
	collapse (sum) sum_deaths_ey_`v' sum_PY_ey_`v', by(ihme_loc_id sim sex year)
	rename sum_deaths deaths
	rename sum_PY pys
	gen age="`v'"
	append using `deaths'
    save `deaths',replace
}
collapse (sum) deaths pys, by(ihme_loc_id sim sex year age)
keep if year>1949 & year<2017

reshape wide deaths pys, i(ihme_loc_id year sex sim) j(age, string)

saveold "FILEPATH/noshock_u5_deaths_`ihme_loc_id'.dta", replace

exit,clear

