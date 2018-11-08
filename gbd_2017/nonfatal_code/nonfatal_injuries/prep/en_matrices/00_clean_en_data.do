	
// Master script for cleaning all of the data for the EN matrices
	clear
	set more off, perm
	
// Import macros
local check = 99
if `check'==1 {
	if c(os) == "Unix" {
		set odbcmgr unixodbc
	}
	local 1 "FILEPATH"
	local 2 "`1'/FILEPATH"
	local 3 "FILEPATH"
	local 4 "EN_matrices"
	local 5 "FILEPATH"
	local 6 "FILEPATH"
	local 7 0 1 5 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95
	local 8 "FILEPATH"
	local 9 "`1'/FILEPATH"
	local 10 "/FILEPATH"
}
	global prefix `1'
	local inj_dir `2'
	local code_dir `3'
	local step_name `4'
	local cleaned_dir `5'
	local prepped_dir `6'
	local ages `7'
	local data_dir `8'
	local log_dir `9'
	// Directory of general GBD ado functions
	local gbd_ado `10'
	// Step diagnostics directory
	local diag_dir `11'
	
	local expert 0
	local ihme 1
	local chinese_niss 0
	local chinese_icss 0
	local nld 0
	local hdr 0
	local argentina 0

if `expert' {
// a) Clean (and prep) 2010 expert group data
	cap confirm file "`prepped_dir'/FILEPATH.csv"
	do "`code_dir'/FILEPATH.do" $prefix "`inj_dir'" "`prepped_dir'" "`ages'" `code_dir'
}

if `ihme' {
// b) Clean IHME hospital data
		capture ssc install dirlist
		
		local hospital_data_dir "FILEPATH"
	
		** first get the list of sources to grab
		local hospital_dir_list : dir "`hospital_data_dir'" dirs "*"
		
		capture confirm file "`cleaned_dir'/FILEPATH.csv"
		if _rc {
			if c(os) == "Unix" {
				local mem 32.4
				local slots = 100
				local mem = 200
				local name IHME_hosp_EN_data
				! qsub -N `name' -P proj_injuries -o FILEPATH -e FILEPATH -pe multi_slot `slots' -l mem_free=`mem' "FILEPATH.sh" "`code_dir'/FILEPATH.do" "$prefix `code_dir' `cleaned_dir' `log_dir' `gbd_ado' `slots' `diag_dir' `name'"
			}
			else {
				di in red "please clean these data sets by running this code on the cluster, as they may be too large"
				di in red "these data sets need to be cleaned: `sources'"
				BREAK
			}		
		
		}
		else {
			** loop through the sources
			local x=1
			foreach source of local hospital_dir_list {
				local dir_date ""
				local source=upper("`source'")
				di in red "checking source `source'"
				local source_file "`hospital_data_dir'/FILEPATH.dta"
				
				** want to use only sources where the "FILEPATH.dta" file exists
				capture confirm file "`source_file'"
				di "`source_file'"
				
				if !_rc {
					di "`source_file'"
					** get a list of the dates that all of these sources were modified
					if c(os)=="Windows" {
						local source_file = subinstr("`source_file'", "/" , "\", .)
					}
					
					dirlist "`source_file'"
					local dir_date `r(fdates)'
					
					if `x'==1 {
						clear
						set obs 1
						gen source = "`source'"
						gen date = date("`dir_date'","MDY")
						tempfile file_date_list
						save `file_date_list', replace
						local ++x
					}
					
					else {
						use `file_date_list', clear
						set obs `x'
						replace source = "`source'" in `x'
						replace date = date("`dir_date'","MDY") in `x'
						save `file_date_list', replace
						local ++x
					}
			
					}
					** end _rc loop for checking in FILEPATH.do files exist
						
			}
			** end source file loop
			use `file_date_list', clear
			if c(os)=="Windows" {
				local check_file = subinstr("`cleaned_dir'/FILEPATH.csv", "/", "\", .)
			}
			else {
				local check_file = "`cleaned_dir'/FILEPATH.csv"
			}
			
			di "`check_file'"
			
			dirlist "`check_file'"
			return list
			
			gen last_cleaned=date("`r(fdates)'", "DMY")		
			count if last_cleaned < date
			local tot_unp = `r(N)'
			levelsof source if last_cleaned < date, local(sources)
			
			if `tot_unp' > 0 {
				if c(os) == "Unix" {
					local mem 32.4
					local slots = ceil(`mem'/2)
					local mem = `slots' * 2
					local name IHME_hosp_EN_data
					! qsub -N `name' -pe multi_slot `slots' -l mem_free=`mem' "FILEPATH.sh" "`code_dir'/FILEPATH/FILEPATH.do" "$prefix `code_dir' `cleaned_dir' `log_dir' `gbd_ado' `slots' `diag_dir' `name'"
				}
				else {
					di in red "There are `r(N)' hospital data sets from IHME that have not been cleaned"
					di in red "please clean these data sets by running this code on the cluster, as they may be too large"
					di in red "these data sets need to be cleaned: `sources'"
					BREAK
				}
			}
			
			else {
				di "All available IHME hospital datasets have already been cleaned"
			}
		}
}


if `chinese_niss' {		
// c) Clean Chinese data 
	cap confirm file "`cleaned_dir'/FILEPATH.csv"
	if _rc do "`code_dir'/FILEPATH.do" $prefix "`data_dir'"
}	

if `chinese_icss' {
// d) Clean Chinese surveillance data (ICSS)
	cap confirm file "`cleaned_dir'/FILEPATH.csv"
	if _rc do "`code_dir'/FILEPATH.do" $prefix "`code_dir'" "`cleaned_dir'" "`log_dir'"
}

if `nld' {
// e) Clean NLD data
	do "`code_dir'/FILEPATH.do" $prefix "`cleaned_dir'"
}

if `hdr' {
// f) Clean HDR data 
	do "`code_dir'/FILEPATH.do" $prefix "`code_dir'" "`cleaned_dir'" "`log_dir'"
}	

if `argentina' {
// g) Clean Argentina data
	do "`code_dir'/FILEPATH.do" $prefix "`code_dir'" "`cleaned_dir'" "`log_dir'"
}
	
// confirm that cluster job is done before moving to next step	
	local check_file = "`cleaned_dir'/FILEPATH.csv"
	local i = 0
	while `i' == 0 {
		capture confirm file "`check_file'"
		if (!_rc) continue, break
		else sleep 60000
	}
	
	di in red "00_clean_en_data done"
	