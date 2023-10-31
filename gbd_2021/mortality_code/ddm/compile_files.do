** ********************************************************************
** Description: Recompiles formatted, reshaped, and combined deaths/pop
** ********************************************************************

** **********************
** Set up Stata 
** **********************

	clear all
	capture cleartmp 
	set mem 1g
	set more off
	
	set trace on
	
	** Set code directory for Git
	local user "`c(username)'"
	
	global version_id `1'
	global file_name `2'
	global main_dir "FILEPATH"
	local source_dir "FILEPATH"
	local source_out_dir "FILEPATH"
	noi di "`source_dir'"

quietly { 
		cd "`source_dir'"
		noi di "`source_dir'"
		local files: dir "temp" files "$file_name*.dta", respectcase
		clear 
		tempfile temp
		local count = 0 
		foreach f of local files { 
			use "temp/`f'", clear
			if (_N == 0) continue 
			local count = `count' + 1
			if (`count'>1) append using `temp'
			save `temp', replace
		} 
		use `temp', clear
		if ("$file_name" == "FILEPATH"){
			noi di "D03"
			duplicates drop
		}
		noi save "`source_out_dir'/$file_name.dta", replace
}

* DONE