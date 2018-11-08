** Create new life-tables

// PREP STATA
	clear
	set more off
	if c(os) == "Unix" {
		global prefix ""
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix ""
	}

global WORKING_DIR "`1'"
	
** functions
qui do "$prefix/get_locations.ado"
qui do "$prefix/get_age_map.ado"
adopath + "$prefix/"

** load in national locations
	
	get_locations, level("country")	
	keep ihme_loc_id location_id
	tempfile nat_locs
	save `nat_locs'

	** load in age map

	get_age_map, type("lifetable")
	keep age_group_id age_group_name_short
	rename age_group_name_short age
	tempfile age_map
	save `age_map'

**********************************************************************
	*************** Select new QX values
	***** This is set to create qx values based on the minimum qx, as well as the mean of the lowest 10.
	**** This currently excludes countries with populaiton below 5 million.
***********************************************************************
** Set the preference of excluding countries under 5 million.  
	local ex _excl
	

** Bring in life table values of interest
import delimited "$WORKING_DIR/data/with_shock_summary_lt.csv", clear

** Make a few permutations of the life tables.

	merge m:1 ihme_loc_id location_id using `nat_locs' 
	keep if _m==3
	drop _merge
  
  rename year_id year
	tempfile lts
	save `lts', replace
	
	local exclude = 1
	if  `exclude'==1 {
		get_population,  location_set_id(21) status("recent") location_id(-1) year_id(-1) sex_id(3) age_group_id(22) clear
    keep location_id year_id population
    rename year_id year
		merge 1:m location_id year using `lts'
		assert _m!=2
		keep if _m==3
		drop _m
		drop if population < 5000000
		keep ihme_loc_id year sex_id age_group_id mx ax qx
		local ex _excl

	}

/*
***************************************************** Minimum risk life expectancy calc*********************************
*/

** merge on old age variable
merge m:1 age_group_id using `age_map'
assert _m==3
drop _m

** save list of locations being used in the run
outsheet using "$WORKING_DIR/data/locations_ages_used.csv", comma replace

drop age_

	** For tracking the rownumber desired. 
	gen cell_num = _n
	count
	local obs = `r(N)'
	capture destring age, replace
	levelsof age, local(ages)
** Set new obs for storing new qx values
	local num_ages: list sizeof ages
	local new_obs = `obs' + `num_ages'
	set obs `new_obs'
	outsheet using "$WORKING_DIR/data/diag_locations_pre_min_selection.csv", comma replace
	gen loc = ihme_loc_id
	replace ihme_loc_id = "min" if ihme_loc_id ==""
	local x = 1
	
** Loop through sex and age, storing the minimum qx in the lowest empty row.

** use sex specific values
		foreach a of local ages {
		
			local t1 = `a'
			local cell = `obs' + `x'
			replace age = `a' in `cell'
			di in red "min for `a'"
			
			** Doing differently for age 110 because qx is 1 for all ages, and you should grab the highest ax value
			if `a' != 110 {
				sum (qx) if age==`a'
				local q = `r(min)' + .0000000001
				replace qx = `q' if ihme_loc_id=="min" & age==`a'
				sum cell_num if qx<`q' & age==`a' 
				local cnum = `r(mean)'
				** Get the sex, mx, and ax which correspond to the lowest qx value for that age-group
				levelsof sex in `cnum', clean local(sex)
				replace sex = `sex' in `cell'
				local mx = mx in `cnum'
				replace mx = `mx' if ihme_loc_id=="min" & age==`a' 
				local ax = ax in `cnum'
				replace ax = `ax' if ihme_loc_id=="min" & age==`a'
				local loc = loc in `cnum'
				replace loc = "`loc'" if ihme_loc_id=="min" & age==`a'
			}
			else {
				sum (ax) if age==`a'
				local ax = `r(max)' - .0000000001
				replace ax = `ax' if ihme_loc_id=="min" & age==`a'
				sum cell_num if ax>`ax' & age==`a' 
				local cnum = `r(mean)'
				** Get the sex, mx, and ax which correspond to the lowest qx value for that age-group
				levelsof sex in `cnum', clean local(sex)
				replace sex = `sex' in `cell'
				local mx = mx in `cnum'
				replace mx = `mx' if ihme_loc_id=="min" & age==`a'
				replace qx = 1 if ihme_loc_id =="min" & age ==`a'
				local loc = loc in `cnum'
				replace loc = "`loc'" if ihme_loc_id=="min" & age==`a'
			}
			
			local x = `x' + 1
		}

** Organize, and outsheet
	keep if ihme_loc_id=="min"
	label drop _all
	rename ihme_loc_id mod
	rename loc ihme_loc_id
	keep mod sex age qx ax mx ihme_loc_id
	keep if qx!=.
	gen sex = "female" if sex_id==2
	replace sex="male" if sex_id==1
	assert sex!=""
	outsheet using "$WORKING_DIR/final/FINAL_min_life_tables_input`ex'.csv", comma replace
	
	

	
**********************************************************************************************************************************
** Make PRELIMINARY life tables (pre-AX correction)
******************************************************************************************************************************	
	
	
** Loop through modules, then sexes, making the life tables based on the qx values.  
local mod "FINAL_min"
		insheet using "$WORKING_DIR/final/`mod'_life_tables_input`ex'.csv", clear
		
		** sex_id and ihme_loc_id are kept for diagnostic reasons but can be dropped now
		drop sex_id
		drop ihme_loc_id
	
		** Set time elapsed.
			gen nn = age[_n+1] - age
		** Set Radix
			gen lx = 1 in 1
			capture gen ax = .
		** Set preliminary ax as time elapse/2 -- this is adjusted below
			replace ax = nn/2
			sum age
			local age_max = `r(max)'
		** Replacing ax as 2.5 in oldes age-group: adjusted below.
			capture replace ax = 2.5 if age==`age_max'
			sort age
		** Calc all lx (number of people living in the interval = number of people in last interval times 1 - prob. of death in last interval)
			replace lx=lx[_n-1]*(1-qx[_n-1]) if _n>1
		** Calc dx (number of deaths in the interval).
			gen dx = lx*qx
		** Dx in terminal age-group is all people.
			replace dx = lx if age==`age_max'
		** Set LX:  years of life lived in the interval =  number alive in the last interval times the time of the interval 
						** plus the number of people who die in the interval times when they die.  
			gen Lx = lx[_n+1]*nn + dx*ax
			replace Lx = ax*lx if age==`age_max'
		** Calc Tx: sum years of life lived.	
			gen Tx = .
		** Solve TX older age-groups
			
		forvalues a = 110(-5)5 {
			egen tmp = sum(Lx) if age>=`a'
			replace Tx = tmp if age==`a'
			drop tmp
		}
** Solve TX younger age-groups
		forvalues a = 0(1)1 {
			egen tmp = sum(Lx) if age>=`a'
			replace Tx = tmp if age==`a'
			drop tmp
		}
			
		** Solve EX, and outsheet by sex, module.  
			gen Ex = Tx/lx
			outsheet using "$WORKING_DIR/`mod'_lt_preAX`ex'.csv", comma replace nolabel
	
	
************************************************************************************************
** Calculate AX values of this life-table
*****************************************************************************
** Loop through sex, module.  


	** Use preliminary life-table calculated above.
		gen cax = .
		gen diffax = .
	** Use iterative approach to solving for an adjusted ax value
		local nage=_N
		sort age
		local stan=0.0001
		local iter=20
		local cats=1
		while `cats'<=`iter' & `stan'>=0.0001 {
			sort age
			replace lx=lx[_n-1]*(1-qx[_n-1]) if _n>1
			replace dx=lx*qx
			replace cax=ax
			replace cax=(-5/24*dx[_n-1]+2.5*dx+5/24*dx[_n+1])/dx if _n>3 & _n<`nage'
			replace diffax=abs(ax-cax)
			cap sum diffax
			local stan=r(max)
			local cats=`cats'+1
			replace ax=cax if _n>3 & _n<`nage'
		}              
		
		sort sex age
		cap drop _m                      
	** Bringing in empirical values for older ages
		merge 1:1 sex age using "/data/ax_par.dta"
		drop if _m==2
		drop _m
		** replace ax values using relationship below given the empirical values for older ages
		gen qx_square=qx^2
		replace ax=par_qx*qx+par_sqx*qx_square+par_con if age>75 & par_con!=.
		sort age
		keep age ax mx
	** Use empirical values for younger age mx based on empirical values. 
		local mx_1 = mx in 1
		
		if `mx_1'>=.107 {
			replace ax = .340 in 1 
			replace ax = 1.3565 in 2
		}
		
		else if `mx_1'<.107 {
			replace ax = .049+2.742*`mx_1' in 1
			replace ax = 1.5865- 2.167*`mx_1' in 2
		}
	
	
		
		** Save new ax values:
		drop mx
		saveold  "$WORKING_DIR/`mod'_AX`ex'.dta", replace
		

** Set AX values for YLL calculations: use actual values for younger age groups (ie, 7 days, 28 days, etc.).
		count 
		local obs = `r(N)' + 2
		set obs `obs'
		local first = `obs' - 1
		local second = `obs' 
		replace age = 7/365 in `first'
		replace age =  28/365 in `second'
		replace ax = .009589 if age==0
		replace ax = .028767 in `first'
		replace ax = .0461644 in `second'
		sort age
		drop if age>80
		outsheet using "$WORKING_DIR/`mod'_ax_values.csv", comma replace
		

************************************************************************************
** Make life tables using AX corrections.  
************************************************************************************

			insheet using "$WORKING_DIR/`mod'_lt_preAX`ex'.csv", clear
			** Change names
			foreach var of varlist * {
				local lab: variable label `var'
				if "`lab'" != "" {
						rename `var' `lab'
				}
				
			}
			
			merge 1:1 age using "$WORKING_DIR/`mod'_AX`ex'.dta", update replace
			
		** Resolve for Lx given updated ax values
			drop _m
			sum age
			local age_max = `r(max)'
			replace  Lx = lx[_n+1]*nn + dx*ax
			
			replace Lx = lx/mx if age==`age_max'
			drop Tx Ex
			gen Tx = .
			forvalues a = 110(-5)5 {
				egen tmp = sum(Lx) if age>=`a'
				replace Tx = tmp if age==`a'
				drop tmp
			}
			forvalues a = 0(1)1 {
				egen tmp = sum(Lx) if age>=`a'
				replace Tx = tmp if age==`a'
				drop tmp
			}
			gen Ex = Tx/lx
			tempfile using
			save `using', replace
			outsheet using "$WORKING_DIR/usable/`mod'_lt_final`ex'.csv", comma replace
		
			capture gen mod = "`mod'"
			keep mod sex age Ex ax
			save "`sex'_`mod'_ex.dta", replace
			outsheet using "$WORKING_DIR/usable/`mod'_ex_final`ex'.csv", comma replace
	
	
