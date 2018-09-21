
	clear all 
	set more off
	pause on
	capture log close
	cap restore, not
	local test = 0

	if (c(os)=="Unix") {
		global root "FILEPATH"
		global jroot "$root"
		local out_dir "FILEPATH"
		set odbcmgr unixodbc
		local code_dir "`1'"
		local read_ctemp "`2'"
		
		if (`test' == 1) {
			local code_dir "FILEPATH"
			local read_ctemp "1"
		}
		
		if ("`read_ctemp'" == "1") global root "FILEPATH"
		qui do "FILEPATH/get_locations.ado"
	} 
	if (c(os)=="Windows") { 
		global root "J:"
		global jroot "`root'"
		qui do "FILEPATH/get_locations.ado"
	}
	cd "FILEPATH"	
	local date = subinstr("`c(current_date)'", " ", "_", 2)	
	local run_anyway = 0
	
	get_locations, level(estimate)
	keep if level_all == 1
	keep ihme_loc_id region_name location_name
	rename region_name gbdregion
	levelsof ihme_loc_id, local(all_locs) clean
	local num_countries: word count `all_locs'
	tempfile countrycodes
	save `countrycodes', replace
	
	tempfile all 
	local missing ""
	local count = 0
	foreach ihme_loc_id of local all_locs { 
		di in red "`ihme_loc_id'"
		capture use "FILEPATH/`ihme_loc_id'_noshocks.dta", clear 
		if (_rc != 0) {
			di in green "MISSING: `ihme_loc_id'"
			local missing "`missing' `ihme_loc_id'"
		}
		else { 
			local count = `count' + 1
			if (`count' > 1) append using `all'
			quietly save `all', replace
		} 	
	} 
	
	** save list of missing files, break code if there are some missing
	clear
	local len = wordcount("`missing'")
	if (`len' == 0) {
		local len = 1
		local missing "NOTHINGMISSING"
	}
	set obs `len'
	gen ihme_loc_id = ""
	gen row = _n
	local count = 1
	foreach unit of local missing {
		replace ihme_loc_id = "`unit'" if row == `count'
		local count = `count' + 1
	}
	drop row
	outsheet using "`out_dir'/FILEPATH/missing_results.csv", comma replace
	if (`len' > 1 & `run_anyway' != 1) assert 1==2

	** merge on country codes, format
	use `all', clear
	merge m:1 ihme_loc_id using `countrycodes'  
	drop _merge
	order gbdregion location_name ihme_loc_id sex year q_e* q_l* q_n* q_p* q_inf* q_ch* q_u5*
	rename gbdregion region_name

	** Save estimates and save rates of decline
	preserve
	drop change* 
		saveold "`out_dir'/FILEPATH/estimated_enn-lnn-pnn-ch-u5_noshocks.dta", replace 

		saveold "`out_dir'/FILEPATH/estimated_enn-lnn-pnn-ch-u5_noshocks_`date'.dta", replace 

	restore

	preserve
	drop q*

		saveold "`out_dir'/FILEPATH/estimated_enn-lnn-pnn-ch-u5_ratesofchange_noshocks.dta", replace
		saveold "`out_dir'FILEPATH/estimated_enn-lnn-pnn-ch-u5_ratesofchange_noshocks_`date'.dta", replace
	restore
	
	drop change*
	rename q* new_q*
	tempfile new
	save `new', replace
	
	** read in most recent older file to get comparison
	local files : dir "FILEPATH" files "estimated_enn-lnn-pnn-ch-u5_noshocks*.dta"
	local oldfiles : word count `files'
	clear 
	set obs `oldfiles'
	gen fi = ""
	local count = 1
	foreach f of local files {
		replace fi = "`f'" if _n == `count'
		local count = `count' + 1
	}
	gen date = fi
	replace date = subinstr(date,"__","",.)
	replace date = subinstr(date,"estimated_enn-lnn-pnn-ch-u5_noshocks_","",.)
	replace date = subinstr(date,"estimated_enn-lnn-pnn-ch-u5_noshocks","",.)
	replace date = subinstr(date,".dta","",.)
	replace date = subinstr(date,"_","",.)
	replace date = subinstr(date," ","",.)
	gen st_date = date(date,"DMY")
	sort st_date
	keep if _n == _N - 2
	local compfile = fi[1]
	use "FILEPATH/`compfile'", clear
	merge 1:1 ihme_loc_id location_name sex year using `new'
	
	foreach age in enn lnn pnn ch u5 {
		gen rat_`age' = new_q_`age'_med/q_`age'_med
	}
	gen compdate = "`compfile'"
	gen todaydate = "$S_DATE"

	saveold "FILEPATH/comparison_runs.dta", replace

	** log close
	exit, clear

	
	
