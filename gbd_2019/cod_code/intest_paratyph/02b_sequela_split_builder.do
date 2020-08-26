
*** BOILERPLATE ***
	clear all
	set more off

	if c(os) == "Unix" {
		local j "ADDRESS"
		set odbcmgr unixodbc
		}
	
	else if c(os) == "Windows" {
		local j "ADDRESS"
		}

	tempfile mergingTemp
	local rootDir FILEPATH
	
*** CREATE SEVERITY SPLIT DRAWS FOR TYPHOID ***
	local states_typh inf_mod inf_sev abdom_sev gastric_bleeding
	local states_para inf_mild inf_mod inf_sev abdom_mod
  
  
  
*** CREATE SEVERITY SPLIT DRAWS FOR TYPHOID ***
	local abdom_sev  = 0.17
	local gastric_bleeding = 0.05 * (2/365) * (52/6) 
	local inf_mod    = 0.35 
	local inf_sev    = 0.43 + 0.05 - `gastric_bleeding'

	local sample = 100

	clear
	set obs 4
	gen index = _n
	generate state_typh = ""
	generate alpha_typh = .
	generate beta_typh =.
	local count 1

	foreach state of local states_typh {
		local mu = ``state''
		local sigma = `mu'/4

		replace alpha_typh = `sample' * ``state'' in `count'
		replace beta_typh  = `sample' - alpha  in `count'
		replace state_typh = "`state'" in `count'

		local ++count
		}


	save `mergingTemp', replace
  
  
  
  
*** CREATE SEVERITY SPLIT DRAWS FOR PARATYPHOID ***
	local abdom_mod  = 0.05
	local inf_mild = 0.30 * (1-`abdom_mod')
	local inf_mod  = 0.55 * (1-`abdom_mod')
	local inf_sev  = 0.15 * (1-`abdom_mod')

	clear
	set obs 4
	gen index = _n
	generate state_para = ""
	generate alpha_para = .
	generate beta_para =.
	local count 1

	foreach state of local states_para {
		local mu = ``state''
		local sigma = `mu'/4

		replace alpha_para = `mu' * (`mu' - `mu'^2 - `sigma'^2) / `sigma'^2 in `count'
		  replace beta_para  = alpha * (1 - `mu') / `mu' in `count'
		replace state_para = "`state'" in `count'

		local ++count
		}

	merge 1:1 index using `mergingTemp', nogenerate

	drop index
	

	
*** CREATE DURATION DISTRIBUTION PARAMETERS ***	
	local min 12.05
	local max 67
	local mean 28
	loca lambda 4

	local range = `max' - `min'
	local mode  = (`mean'*`lambda' + 2*`mean' - `min' - `max') / `lambda'

	if `mean' == `mode' local v = (`lambda' / 2 ) + 1
	else local v = ((`mean' - `min') * (2 * `mode' - `min' - `max')) / ((`mode' - `mean') * (`max' - `min'))
	local w = (`v' * (`max' - `mean')) / (`mean' - `min')

	generate v_para = `v' if state_para=="inf_sev"
	generate w_para = `w' if state_para=="inf_sev"
	generate range_para = `range' if state_para=="inf_sev"
	generate min_para = `min' if state_para=="inf_sev"

	generate v_typh = `v' if state_typh!="inf_mod"
	generate w_typh = `w' if state_typh!="inf_mod"
	generate range_typh = `range' if state_typh!="inf_mod"
	generate min_typh = `min' if state_typh!="inf_mod"


	local min 4.2
	local max 24
	local mean 14
	loca lambda 4

	local range = `max' - `min'
	local mode  = (`mean'*`lambda' + 2*`mean' - `min' - `max') / `lambda'

	if `mean' == `mode' local v = (`lambda' / 2 ) + 1
	else local v = ((`mean' - `min') * (2 * `mode' - `min' - `max')) / ((`mode' - `mean') * (`max' - `min'))
	local w = (`v' * (`max' - `mean')) / (`mean' - `min')


	replace v_para = `v' if state_para!="inf_sev"
	replace w_para = `w' if state_para!="inf_sev"
	replace range_para = `range' if state_para!="inf_sev"
	replace min_para = `min' if state_para!="inf_sev"

	replace v_typh = `v' if state_typh=="inf_mod"
	replace w_typh = `w' if state_typh=="inf_mod"
	replace range_typh = `range' if state_typh=="inf_mod"
	replace min_typh = `min' if state_typh=="inf_mod"

	
	
	save `rootDir'/inputs/sequela_splits.dta, replace
  
  
