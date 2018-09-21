
** Purpose: upload final round of lit data for all MEs 
	** {AUTHOR NAME}

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

run "$dl/{FILEPATH}/upload_epi_data.ado"
run "$dl/{FILEPATH}/get_epi_data.ado"

local dir "$dl/{FILEPATH}"

**-----------------------------------------------------
** Upload prepped 2013 lit data
	
	**birth prev models
		local birth_bundle_list {BUNDLE ID LIST}
	
	**full models: 
		local fullmod_bundle_list {BUNDLE ID LIST} 
				

foreach b in `birth_bundle_list' {

	** set cause names 
 
		if `b'==226 {
			local c cong_cleft
			}

		if `b'==227 {
			local c cong_downs
			}
		
		if `b'==228 {
			local c cong_turner
			}
		
		if `b'==229 {
			local c cong_klinefelter
			}

		if `b'==230 {
			local c cong_chromo
			}
				
		if (`b'==603 | `b'==605 | `b'==607) {
			local c cong_msk
			}

		if (`b'==609 | `b'==611 | `b'==613 | `b'==615)  {
			local c cong_neural
			}

		if (`b'==617 | `b'==619)  {
			local c cong_urogenital
			}

		if (`b'==621 | `b'==623 | `b'==625 | `b'==627) {
			local c cong_digestive
			}

		if (`b'==629 | `b'==631 | `b'==633 | `b'==635 |`b'==637 )  {
			local c cong_heart 
			}

		if `b'==639 {
			local c cong_chromo
			}

di in red "`b' `c' data"

** upload 	
	local destination_file "`dir'/{FILEPATH}/lit_data_from_2013_`c'_`b'_prepped_{DATE}.xls"	
			upload_epi_data, bundle_id("`b'") filepath("`destination_file'") clear 

			di in red "`destination_file'"	
			} 
	}


** Also upload to the bundles with registry correction
	local reg_corrections_bundle_list {BUNDLE ID LIST}

		** relating each bundle_id to the corresponding bundle_id with registry correction 
		foreach b in `reg_corrections_bundle_list' {
			local c cong_heart 

			if `b'== 629 {
				local corr_b 702
				}

			if `b'== 631 {
				local corr_b 745
				}

			if `b'== 633 {
				local corr_b 743
				}

			if `b'== 635 {
				local corr_b 746
				}

			if `b'== 637 {
				local corr_b 744
				}

		di in red "`corr_b' `c' data from `b' folder"

	** upload 	
	local destination_file "`dir'/{FILEPATH}/lit_data_from_2013_`c'_`b'_prepped_{DATE}.xls"	
			di in red "`destination_file'"	

			upload_epi_data, bundle_id(`corr_b') filepath("`destination_file'") clear 	
			
			}
