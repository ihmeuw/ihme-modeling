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
	noi di "FILEPATH"

quietly {

		cd "`source_dir'"
		noi di "`source_dir'"

		if strpos("$file_name", "wshock") {
		global file_name_base = regexr("$file_name", "_wshock", "")
		local all_files: dir "temp" files "$file_name_base*.dta", respectcase
		local files
		foreach f of local all_files {
		if strpos("`f'", "_wshock.dta") {
        local files `files' `f'
		}
		}
		}
		else {
		local all_files: dir "temp" files "$file_name*.dta", respectcase
		local files
		foreach f of local all_files {
		if !strpos("`f'", "_wshock.dta") {
        local files `files' `f'
		}
		}
		}

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
		if ("$file_name" == "d03_combined_population_and_deaths" | "$file_name" == "d03_combined_population_and_deaths_wshock"){
			noi di "D03"
			duplicates drop
		}
		noi save "FILEPATH", replace
}

* DONE
