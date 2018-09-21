
** Get counts of data in each congenital full model MEs ,and delete 2015 data from the database
** {AUTHOR NAME}
**----------------------------------------------------------

** setup
clear all 
set more off
set maxvar 30000
cap restore, not
cap log close 

	// set prefix for the OS 
	if c(os) == "Windows" {
		global dl "{FILEPATH}"
	}
	if c(os) == "Unix" {
		global dl "{FILEPATH}"
		set odbcmgr unixodbc
	}

	local dir "$dl/{FILEPATH}"

run "$dl/{FILEPATH}/upload_epi_data.ado"
run "$dl/{FILEPATH}/get_epi_data.ado"

local dir "$dl/WORK/12_bundle"
local date {DATE}

log using "$dl/{FILEPATH}/log_full_model_database_contents_`date'.smcl", replace 

local fullmod_bundle_list {BUNDLE ID LIST} 

foreach b in `fullmod_bundle_list' {
 
 ** define the cause name 
 		if `b'==435 {
			local c cong_cleft
			}

 		if `b'==436 {
			local c cong_downs
			}

		if `b'==437 {
			local c cong_turner
			}

		if `b'==438 {
			local c cong_klinefelter 
			}

		if (`b'==439 | `b'==638) {
			local c cong_chromo
			}

		if (`b'==602 | `b'==604 | `b'==606  ) {
			local c cong_msk
			}

		if (`b'==608 | `b'==610 | `b'==612 | `b'==614 ) {
			local c cong_neural
			}

		if (`b'==616 | `b'==618 ) {
			local c cong_urogenital
			}

		if (`b'==620 | `b'==622  `b'==624 | `b'==626 ) {
			local c cong_digestive
			}


		if (`b'==629 | `b'==630 | `b'==632 | `b'==634 |`b'==636 )  {
			local c cong_heart 
			}

	di  "`c' `b'" 

	** get count of data currently in the database
		get_epi_data, bundle_id("`b'") clear 
		count 
			local data_count = r(N)

		di in red "`b' `c' current database: `data_count' data points"
		}

	cap log close 

**delete the data
local bundles_with_data {BUNDLE ID LIST}

foreach b in `bundles_with_data' {
 
	 ** define the cause name 
 		if `b'==435 {
			local c cong_cleft
			}

 		if `b'==436 {
			local c cong_downs
			}

		if `b'==437 {
			local c cong_turner
			}

		if `b'==438 {
			local c cong_klinefelter 
			}

		if `b'==439 {
			local c cong_chromo
			}

			local destination_file "`dir'/{FILEPATH}/deleting_old_`b'_data_`date'.xlsx"
				export excel using "`destination_file'", firstrow(variables) replace sheet("extraction")

				di "uploading `b' `c' data "
				upload_epi_data, bundle_id("`b'") filepath("`destination_file'") clear 
				}





