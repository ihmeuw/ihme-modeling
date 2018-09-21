** ***********************************************************************************************
** Description: compiles the data to be used for the age/sex model 
**		(1) compiles CBH data for all sexes
**		(2) compiles VR data for all sexes
**		(3) compiles VR data from Mortality for all sexes
**		(4) compiles population data (with exceptions for India SRS and China WHO data) 
**		(5) calculates risks for VR
**		(6) marks exclusions/outliers
**		(7) marks data types (i.e. age format)
** 		(8) makes transformations
** ***********************************************************************************************

** *************************	
** Set up Stata 
** *************************

	clear all 
	capture cleartmp 
	set more off 
	capture restore, not
	

	if (c(os)=="Unix") {
		local user = "`1'"
		di "`1'"
		di "`user'"
		set odbcmgr unixodbc
		global j "FILEPATH"
		local code_dir  "FILEPATH"
		qui do "FILEPATH"
		adopath + "FILEPATH"
	}
	else { 
		global j "FILEPATH"
		qui do "FILEPATH" 
		adopath + "FILEPATH"
	}
	
	capture log close
	log using "FILEPATH/input_log.log", replace
	
	** directories 
	global cbh_dir				"FILEPATH"
	global save_dir 			"FILEPATH"
	
	** vr files 
	global cod_vr_file			"FILEPATH" 
	global mort_vr_file 		"FILEPATH" 
	
	** population/births files 
	global natl_pop_file 		"FILEPATH" 
	global all_pop_file 		"FILEPATH"
	global births_file 			"FILEPATH"

	** 5q0 files 
	global raw5q0_file 			"FILEPATH"
	global estimate5q0_file		"FILEPATH"
	

	import delimited "FILEPATH", clear
	keep if level_all == 1
	keep ihme_loc_id local_id_2013 region_name
	replace local_id_2013 = "CHN" if ihme_loc_id == "CHN_44533"
	tempfile codes
	save `codes', replace
	
	import delimited "FILEPATH", clear
	keep location_id ihme_loc_id
	tempfile codes2
	save `codes2', replace

	** 4 and 5 star VR locations
	import delimited "FILEPATH", clear
	keep  ihme_loc_id stars
	tempfile four_five_star
	save `four_five_star', replace

** *************************	
** Compile CBH data 
** *************************

	local surveys: dir "$cbh_dir" dirs "*", respectcase

	local non_data_folders = "Skeleton FUNCTIONS archive"	
	
	local missing_files ""
	tempfile temp 
	local count = 0 
	foreach survey of local surveys { 
		di in white "  `survey'" 
		
		local non_dat = 0
		foreach folder of local non_data_folders {
			if (regexm("`survey'","`folder'")) local non_dat = 1
		}
		
		if (`non_dat' == 0) {
			foreach sex in males females both { 
				if ("`survey'" == "DHS-OTHER") {
					foreach fold in "FILEPATH" {
						local survey "FILEPATH`fold'"
						cap use "FILEPATH", clear 
						if _rc != 0 {
							local missing_files "`missing_files' `survey'-`sex'"
						} 
						else {
							local count = `count' + 1 
							gen source2 = "`survey'"
							gen sex = subinstr("`sex'", "s", "", 1) 			
							if (`count' > 1) append using `temp'
							save `temp', replace 
						}
					}
				
				}
				else {
					cap use "FILEPATH", clear 
					if _rc != 0 {
						local missing_files "`missing_files' `survey'-`sex'"
					} 
					else {
						local count = `count' + 1 
						gen source2 = "`survey'"
						gen sex = subinstr("`sex'", "s", "", 1) 			
						if (`count' > 1) append using `temp'
						save `temp', replace 
					}
				}
			} 
		}
	} 
	drop if q5 == . 	


	gen source_y = source
	replace source = source2
	
	** try to get year of survey for later use
	gen survey_year = substr(source_y,-4,4)
	destring survey_year, replace
	
	drop if source == "COD_DHS_2014" & country == "PER"
	
	** which surveys we are missing output files from:
	di "`missing_files'"
	preserve
	clear
	local numobs = wordcount("`missing_files'")
	set obs `numobs'
	gen file = ""
	local count = 1
	foreach missfile of local missing_files {
		replace file = "`missfile'" if _n == `count'
		local count = `count' + 1
	}
	if (`numobs' == 0) {
		set obs 1
		replace file = "no input folders missing files"
	}
	outsheet using "FILEPATH", comma replace
	restore
	
	** format source variable for consistency  
	replace source = "DHS IN" if source == "DHS-OTHER/In-depth" 
	replace source = "DHS SP" if source == "DHS-OTHER/Special" 
	replace source = "DHS" if source == "DHS_TLS" 
	replace source = "IFHS" if source == "IRQ IFHS"
	replace source = "IRN HH SVY" if source == "IRN DHS" 
	replace source = "TLS2003" if source == "East Timor 2003"
	replace source = "MICS3" if source == "MICS" 
	
	** keep appropriate variables and convert everything to q-space 
	replace p_nn = p_enn*p_lnn if p_nn == . 
	gen p_inf = p_nn*p_pnn 
	gen p_ch = p_1p1*p_1p2*p_1p3*p_1p4
	gen p_u5 = 1-q5
	egen deaths_u5 = rowtotal(death_count*)
	keep country year sex source source_y p_enn p_lnn p_nn p_pnn p_inf p_ch p_u5 deaths_u5 survey_year
	rename country iso3
	order iso3 year sex source source_y p_enn p_lnn p_nn p_pnn p_inf p_ch p_u5 deaths_u5 
	
	foreach age in enn lnn nn pnn inf ch u5 { 
		replace p_`age' = 1-p_`age'
	}
	rename p_* q_*
	
	** make consistent the data for males, females, and both - we need all three for the data to be useful to do sex-splitting

	duplicates drop
	bysort iso3 year source_y: egen count = count(year)
	assert count <= 3
	gen withinsex = 1 if count < 3
	replace withinsex = 0 if withinsex == .
	drop count
	isid iso3 source_y year sex
	tempfile cbh
	save `cbh', replace
	
	** get ihme_loc_id if not present
	use `codes', clear
	rename local_id_2013 iso3
	drop if iso3 == ""
	drop if iso3=="NA" 
	merge 1:m iso3 using `cbh'
	drop if _m == 1
	drop if _m == 1
	drop _m
	replace ihme_loc_id = iso3 if ihme_loc_id == ""
	save `cbh', replace
	merge m:1 ihme_loc_id using `codes'
	drop if ihme_loc_id == "IND_4637" | ihme_loc_id == "IND_4638"
	
	** outsheet anything we're not keeping because we didn't get ihme_loc_id's merged
	preserve
	keep if _m == 1
	if (_N == 0) { 
		set obs 1
		replace ihme_loc_id = "ALL IHME_LOC_ID MERGED FROM CBH"
	}
	outsheet using "FILEPATH", comma replace
	restore
	
	keep if _m == 3
	drop _m local_id_2013 iso3 region_name
	
	** save CBH data 
	tempfile cbh
	save `cbh', replace 
	
	
** *************************	
** Compile VR deaths
** *************************
	
** prep COD VR file 
	use "FILEPATH", clear
	collapse (sum) deaths*, by(iso3 location_id year source subdiv im_frmat child_split sex)
	tempfile cod_vr_col
	save `cod_vr_col'

	keep iso3 location_id sex subdiv source year deaths2 deaths3 deaths91 deaths93 deaths94 im_frmat child_split
	
	summ year
	local yearmax = r(max)
	
	gen ihme_loc_id = iso3
	replace ihme_loc_id = iso3 + "_" + string(location_id) if location_id != .
	replace ihme_loc_id = "PRI" if location_id == 385
	drop iso3 location_id
	
	merge m:1 ihme_loc_id using `codes'
	assert _m!=1
	keep if _m == 3
	drop _m
	
	** check that the infant formats haven't changed
	drop if im_frmat == 9 
	summ im_frmat
	assert `r(max)' == 8 
	
	gen source_type = "VR" 
	replace source_type = "DSP" if substr(ihme_loc_id,1,3) == "CHN" & (source == "China_2004_2012" | source == "China_1991_2002")
	drop if inlist(subdiv, "A30","A10","A30.","A70","A80")
	drop if ihme_loc_id == "QAT" & (subdiv != "" & subdiv != ".")
	drop if inlist(source,"UK_deprivation_1981_2000","UK_deprivation_2001_2012")
	drop if ihme_loc_id == "MAR" & source != "ICD10"
	replace subdiv = "" if inlist(subdiv,"A20",".","..")
	gen loc_substr = substr(ihme_loc_id,-4,4)
	replace subdiv = "" if loc_substr == subdiv
	replace loc_substr = substr(ihme_loc_id,-3,3)
	replace subdiv = "" if loc_substr == subdiv
	replace subdiv = "" if inlist(subdiv,"East Midlands","Scotland","South East","South West" ,"Stockholm county","Sweden excluding Stockholm county","Wales","East of England") & strlen(ihme_loc_id) > 3
	replace subdiv = "" if inlist(subdiv,"London","North East","North West","Northern Ireland","West Midlands","Yorkshire and The Humber") & strlen(ihme_loc_id) > 3
	replace subdiv = "" if inlist(subdiv,"Aichika","Akita","Aomori","Chiba","Eastern Cape", "Ehime", "Free State") & strlen(ihme_loc_id) > 3
	replace subdiv = "" if inlist(subdiv, "Fukui", "Fukuoka","Fukushima","Gauteng") & strlen(ihme_loc_id) > 3
	replace subdiv = "" if inlist(subdiv,"Gifu","Gumma","Hiroshima","Hokkaido","Hyogo","Ibaraki","Ishikawa") & strlen(ihme_loc_id) > 3
	replace subdiv = "" if inlist(subdiv,"Iwate","Kagawa","Kagoshima","Kanagawa") & strlen(ihme_loc_id) > 3
	replace subdiv = "" if inlist(subdiv,"Kochi","Kumamoto","KwaZulu-Natal","Kyoto","Limpopo","Mie") & strlen(ihme_loc_id) > 3
	replace subdiv = "" if inlist(subdiv,"Miyagi","Miyazaki","Mpumalanga","Nagano") & strlen(ihme_loc_id) > 3
	replace subdiv = "" if inlist(subdiv,"Nagasaki","Nara","Niigata","Northern Cape","Oita","Okayama") & strlen(ihme_loc_id) > 3
	replace subdiv = "" if inlist(subdiv,"Okinawa","Osaka","Saga","Saitama","Shiga","Shimane","Shizuoka") & strlen(ihme_loc_id) > 3
	replace subdiv = "" if inlist(subdiv,"Tochigi","Tokushima","Tokyo","Tottori","Toyama","Wakayama") & strlen(ihme_loc_id) > 3
	replace subdiv = "" if inlist(subdiv,"Western Cape","Yamagata","Yamaguchi","Yamanashi") & strlen(ihme_loc_id) > 3
	
	preserve 
	keep if subdiv != ""
	if (_N == 0) {
		set obs 1
		replace subdiv = "NO NEW SUBDIV VALUES TO RESOLVE"
	}
	outsheet using "FILEPATH", comma replace
	restore
	
	drop if subdiv != ""

	drop subdiv loc_substr
	

	drop if child_split == 1
	drop child_split
	
	duplicates tag ihme_loc_id year sex, gen(dup)
	
	drop if dup == 1 & (ihme_loc_id == "GEO" | ihme_loc_id == "VIR" | ihme_loc_id== "PHL") 
	drop if source == "Russia_FMD_2012_2013" & year >=2012
	
	drop if source == "Russia_FMD_1999_2011" | source == "Russia_ROSSTAT" 
	
	
	drop if ihme_loc_id =="MNG" & (source=="ICD9_BTL" | source=="Mongolia_2004_2008" | source=="Other_Maternal") & (year==1994 | year==2006 | year==2007)

	drop if dup==1 & source == "Iran_collaborator_ICD10"

	drop dup

	save "/home/j/temp/USER/as_debug.dta", replace
	isid ihme_loc_id year sex

	gen deaths_enn = deaths91 if im_frmat == 1 | im_frmat == 2 | im_frmat ==4
	gen deaths_lnn = deaths93 if im_frmat == 1 | im_frmat == 2 | im_frmat ==4
	gen deaths_pnn = deaths94 if im_frmat == 1 | im_frmat == 2 | im_frmat ==4
	
		** neonatal-post-neonatal split 
	gen deaths_nn = deaths91 + deaths93 if im_frmat == 1 | im_frmat == 2 | im_frmat == 4
	
		** infant-child split 
	gen deaths_inf = deaths2
	gen deaths_ch  = deaths3
	drop im_frmat
	
	** format sex variable 
	tostring sex, replace
	replace sex = "male" if sex == "1" 
	replace sex = "female" if sex == "2" 
	keep ihme_loc_id year sex deaths_* source_type source
	rename source cod_source
	compress	
	
	replace source_type = "DSP3" if source_type == "DSP" & year >=2004 
	replace source_type = "VR2" if source_type == "VR" & ihme_loc_id == "KOR" & year > 1977
	replace source_type = "VR1" if source_type == "VR" & ihme_loc_id == "TUR" & year < 2009
	replace source_type = "VR2" if source_type == "VR" & ihme_loc_id == "TUR" & year >= 2009
	
	
	summ year
	local yearmax = r(max)
	tempfile cod_vr
	save `cod_vr', replace
	
** aggregate up- go up by level
	tempfile add_nats
	local count = 0
	foreach lev in 6 5 4 {
		di "`lev'"
		import delimited "FILEPATH", clear
		local exp = `yearmax' - 1949
		expand `exp'
		bysort ihme_loc_id: gen year = 1949 + _n
		
		merge 1:m ihme_loc_id year using `cod_vr'
		drop if _m == 2
		assert deaths_ch != . if deaths_inf != .
		assert deaths_inf != . if deaths_ch != .

		keep if level == `lev'
		levelsof parent_id, local(parents)
		
		forvalues i = 1950/`yearmax' {
			foreach j of local parents {
				cap assert deaths_ch != . if parent_id == `j' & year == `i'
				if (_rc != 0) drop if parent_id == `j' & year == `i'
			}
		}
		
		foreach age in enn lnn pnn nn inf ch {
			gen miss_`age' = 1 if deaths_`age' == .
		}
		
		duplicates tag ihme_loc_id year sex source_type, gen(dup)
		drop if dup > 0 & cod_source != "Collapsed Subnat" & regexm(ihme_loc_id, "GBR")
		isid ihme_loc_id year sex source_type
		drop dup

		if `lev' != 4{
			collapse (sum) deaths* miss*, by(parent_id year sex source_type cod_source)
		}
		if `lev' ==4{
			collapse (sum) deaths* miss*, by(parent_id year sex source_type)
		}
		
		rename parent_id location_id
		foreach age in enn lnn pnn nn inf ch {
			replace deaths_`age' = . if miss_`age' != 0
		}

		if `lev'==4 gen cod_source = "Collapsed Subnat"
		if `lev' != 4 replace cod_source = "Collapsed Subnat"
		keep year cod_source sex location_id source_type deaths_enn deaths_lnn deaths_pnn deaths_nn deaths_inf deaths_ch
		tempfile tmp
		save `tmp', replace
		
		import delimited "FILEPATH", clear
		keep ihme_loc_id location_id
		merge 1:m location_id using `tmp'
		keep if _m == 3
		drop location_id
		keep year cod_source sex ihme_loc_id source_type deaths_enn deaths_lnn deaths_pnn deaths_nn deaths_inf deaths_ch
		
		append using `cod_vr'
		save `cod_vr', replace
		local count = `count' + 1
	
	}
	

	duplicates tag ihme_loc_id year sex source_type, gen(dup)
	drop if dup > 0 & cod_source == "Collapsed Subnat"
	drop dup
	isid ihme_loc_id year sex source_type
	
	merge m:1 ihme_loc_id using `codes'
	keep if _m == 3
	drop _m
	keep year cod_source sex ihme_loc_id source_type deaths_enn deaths_lnn deaths_pnn deaths_nn deaths_inf deaths_ch
	
	rename deaths_inf deaths_inf_cod
	rename deaths_ch deaths_ch_cod 
	save `cod_vr', replace
	
	use "FILEPATH", clear 
	merge m:1 ihme_loc_id using `codes'
	keep if _m == 3 & year >= 1950
	drop _m region_name
	
	drop if regexm(deaths_footnote, "Fake number") == 1

	** keep VR and SRS 
	gen source_type1 = "SRS" if regexm(source_type, "SRS") == 1 
	replace source_type1 = "VR" if regexm(source_type, "VR") == 1
	replace source_type1 = "DSP" if regexm(source_type, "DSP") == 1
	keep if source_type1 == "VR" | source_type1 == "SRS" | source_type1 == "DSP"
	drop source_type1
	
	** get infant and child deaths 
	gen deaths_inf = DATUM0to0 
	gen deaths_ch = DATUM1to4
	drop if deaths_inf == . | deaths_ch == . 
	keep ihme_loc_id year sex source_type deaths_source deaths_inf deaths_ch 
	rename deaths_source mortality_source
	compress 
	gen neonatal = 0 
	
	replace source_type = "VR" if source_type== "IRN_VR_post2003"  | source_type== "IRN_VR_pre2003"
	replace source_type = "VR" if source_type == "MEX_VR_post2011" | source_type== "MEX_VR_pre2011"
	replace source_type = "VR" if source_type == "VR_post2002" | source_type == "VR_pre2002" 
	replace source_type = "VR2" if source_type=="VR3"
	
	merge 1:1 ihme_loc_id year sex source_type using `cod_vr'
	
	drop if _m == 2 & (regexm(ihme_loc_id, "IND_") | regexm(ihme_loc_id, "CHN_"))
	
	assert deaths_nn <= ((deaths_lnn + deaths_enn)*1.02) if deaths_lnn != . & deaths_enn != .
	assert deaths_nn >= ((deaths_lnn + deaths_enn)*.98) if deaths_lnn != . & deaths_enn != .
	replace deaths_nn = deaths_enn + deaths_lnn if deaths_enn != . & deaths_lnn != .
	
	assert deaths_inf_cod <= ((deaths_nn + deaths_pnn)*1.02) if deaths_nn != . & deaths_pnn != .
	assert deaths_inf_cod >= ((deaths_nn + deaths_pnn)*.98) if deaths_nn != . & deaths_pnn != .
	replace deaths_inf_cod = deaths_nn + deaths_pnn if deaths_nn != . & deaths_pnn != .
	
	replace deaths_enn =  . if (deaths_inf_cod > deaths_inf*1.05 | deaths_inf_cod < deaths_inf*.95) | (deaths_ch_cod > deaths_ch*1.05 | deaths_ch_cod < deaths_ch*.95) 
	replace deaths_lnn =  . if (deaths_inf_cod > deaths_inf*1.05 | deaths_inf_cod < deaths_inf*.95) | (deaths_ch_cod > deaths_ch*1.05 | deaths_ch_cod < deaths_ch*.95) 
	replace deaths_pnn =  . if (deaths_inf_cod > deaths_inf*1.05 | deaths_inf_cod < deaths_inf*.95) | (deaths_ch_cod > deaths_ch*1.05 | deaths_ch_cod < deaths_ch*.95) 
	replace deaths_nn = . if (deaths_inf_cod > deaths_inf*1.05 | deaths_inf_cod < deaths_inf*.95) | (deaths_ch_cod > deaths_ch*1.05 | deaths_ch_cod < deaths_ch*.95) 

	replace deaths_inf = deaths_inf_cod if deaths_nn != .
	replace deaths_ch = deaths_ch_cod if deaths_nn != .
	
	
** generate both sexes
	keep ihme_loc_id sex year source_type deaths_enn deaths_lnn deaths_pnn deaths_inf deaths_ch deaths_nn 
	drop if sex == "both" 
	tempfile temp
	save `temp', replace 
	gen count = 1
	foreach var of varlist deaths* { 
		replace `var' = -99999999 if `var' == . 
	} 
	collapse (sum) deaths* count, by(ihme_loc_id year source_type)
	keep if count == 2
	foreach var of varlist deaths* { 
		replace `var' = . if `var' < 0 
	} 
	
	drop count
	gen sex = "both" 
	append using `temp' 
	bysort ihme_loc_id year: egen count = count(year)
	keep if count >= 3 	
	drop count 	
	
	

	gen type = 1 if deaths_enn != . & deaths_lnn !=. & deaths_pnn != . & deaths_ch != . 
	replace type = 2 if deaths_nn !=. & deaths_pnn != . & deaths_ch != . & type == . 
	replace type = 3 if deaths_inf != . & deaths_ch != . & type == . 

	bysort ihme_loc_id year sex source_type: egen temp = max(type) 
	replace type = temp 
	drop temp 
	replace deaths_enn = . if type > 1
	replace deaths_lnn = . if type > 1
	replace deaths_nn = . if type > 2
	replace deaths_pnn = . if type > 2
	gen deaths_u5 = deaths_inf + deaths_ch
	
** format
	sort ihme_loc_id source_type sex year
	order ihme_loc_id source_type sex year type deaths*
	compress
	tempfile vr_deaths
	save `vr_deaths'

	
** *************************	
** Compile populations
** *************************	

	get_population, location_id(-1) sex_id("1 2 3") age_group_id("28 5") year_id("-1") location_set_id(22) status("recent") clear
	keep location_id sex_id year_id age_group_id population
	rename year_id year
	rename population pop
	gen sex = "male" if sex_id==1
	replace sex = "female" if sex_id ==2
	replace sex = "both" if sex_id==3
	drop sex_id
	gen age_group_name = "ch" if age_group_id == 5
	replace age_group_name = "inf" if age_group_id == 28
	drop age_group_id
	rename pop pop_
	merge m:1 location_id using `codes2'
	keep if _m ==3
	drop _m
	drop location_id
	reshape wide pop_, i(ihme_loc_id sex year) j(age_group_name, string)
	tempfile natl_pop
	save `natl_pop', replace
	
	use "$births_file", clear
	keep ihme_loc_id year sex births
	merge 1:1 ihme_loc_id sex year using `natl_pop'
	drop if _m!=3
	drop _m
	gen source_type = "VR" 
	save `natl_pop', replace
	

	use "$all_pop_file", clear
	keep if source_type == "SRS" | ((ihme_loc_id == "PAK" | ihme_loc_id == "BGD") & source_type == "IHME") |  source_type == "DSP"
	replace source_type = "SRS" if (ihme_loc_id == "PAK" | ihme_loc_id == "BGD" & source_type == "IHME") 
	gen pop_inf = c1_0to0
	gen pop_ch = c1_1to4
	keep ihme_loc_id year sex source_type pop*
	append using `natl_pop'
	gen mergesource = source_type
	tempfile all_pop
	save `all_pop', replace

	
** *************************	
**  Calculate rates from VR/SRS
** *************************	
	
** merge deaths and population 	
	use `vr_deaths', clear
	gen mergesource = source_type
	replace mergesource = "DSP" if regexm(source_type,"DSP") == 1
	replace mergesource = "SRS" if regexm(source_type,"SRS") == 1
	replace mergesource = "VR" if regexm(source_type,"VR") == 1
	drop if ihme_loc_id == "ZAF" & source_type == "VR-SSA"
	drop if ihme_loc_id == "TUR" & (source_type== "VR1" | source_type=="VR2")
	merge 1:1 ihme_loc_id year sex mergesource using `all_pop'
	assert _m !=1
	keep if _m == 3
	drop _m mergesource
	
** calculate qx 
	g q_enn = deaths_enn/births
	g q_lnn = deaths_lnn/(births-deaths_enn)
	g q_nn = 1-(1-q_enn)*(1-q_lnn)
	
	g m_inf = deaths_inf/pop_inf 
	g m_ch = deaths_ch/pop_ch

** 1a0 (from Preston/Heuveline/Guillot demography book) 
	gen ax_1a0=. 
	replace ax_1a0 = 0.330 if sex=="male" & m_inf>=0.107
	replace ax_1a0 = 0.350 if sex=="female" & m_inf>=0.107
	replace ax_1a0 = (0.330 + 0.350)/2 if sex=="both" & m_inf>=0.107
	replace ax_1a0 = 0.045 + 2.684*m_inf if sex=="male" & m_inf<0.107
	replace ax_1a0 = 0.053 + 2.800*m_inf if sex=="female" & m_inf<0.107 
	replace ax_1a0 = (0.045 + 2.684*m_inf + 0.053 + 2.800*m_inf)/2 if sex=="both" & m_inf<0.107	
** 4a1 (from Preston/Heuveline/Guillot demography book) 
	gen ax_4a1=.
	replace ax_4a1 = 1.352 if sex=="male" & m_inf>=0.107
	replace ax_4a1 = 1.361 if sex=="female" & m_inf>=0.107
	replace ax_4a1 = (1.352+1.361)/2 if sex=="both" & m_inf>=0.107
	replace ax_4a1 = 1.651-2.816*m_inf if sex=="male" & m_inf<0.107
	replace ax_4a1 = 1.522-1.518*m_inf if sex=="female" & m_inf<0.107 
	replace ax_4a1 = (1.651-2.816*m_inf + 1.522-1.518*m_inf)/2 if sex=="both" & m_inf<0.107

	gen q_inf = 1*m_inf/(1+(1-ax_1a0)*m_inf)
	g q_ch = 4*m_ch/(1+(4-ax_4a1)*m_ch)
	g q_u5 = 1-(1-q_inf)*(1-q_ch)

	g q_pnn = 1 - (1-q_inf)/(1-q_nn)
	
	gen pop_5 = pop_inf + pop_ch
	keep ihme_loc_id sex year source_type q_enn q_lnn q_nn q_pnn q_inf q_ch q_u5 deaths_* pop_5 
	order ihme_loc_id sex year source_type q_enn q_lnn q_nn q_pnn q_inf q_ch q_u5 deaths_* pop_5 
	

	replace year = floor(year) + 0.5 
	rename source_type source
	drop if q_inf == . & q_ch == . & q_u5 == . 
	tempfile vr
	save `vr', replace
	

	use "FILEPATH", clear
	foreach var in q_enn q_lnn q_nn q_pnn q_inf q_ch q_u5 {
		cap gen `var' = .
	}
	foreach var in deaths_inf deaths_ch deaths_enn deaths_lnn deaths_pnn deaths_nn deaths_u5 pop_5 {
		cap gen `var' = 999999
	}
	tempfile add_agg_ests
	save `add_agg_ests', replace

	

	
** *************************
** Combine all sources
** *************************

	use `vr', clear
	append using `cbh' 	
	append using `add_agg_ests'
	drop if year < 1950	
	
** *************************	
** Mark exclusions 
** *************************	

	gen exclude = 0 	
	
** exclude CBH estimates more than 15 years before the survey
	destring year, replace
	destring survey_year, replace force
	replace exclude = 11 if year < survey_year - 15 & survey_year != .
	drop survey_year
	
	gen broadsource = source
	replace broadsource = "DSP" if regexm(source,"DSP") == 1
	replace broadsource = "VR" if regexm(source,"VR") == 1
	replace broadsource = "SRS" if regexm(source,"SRS") == 1
	
	preserve
	insheet using "$raw5q0_file", clear
	drop if ihme_loc_id == "BGD" & source == "SRS" & (year == 2002.5 | year == 2001.5)
	gen broadsource = source
	replace broadsource = "DSP" if regexm(source,"DSP") == 1
	replace broadsource = "VR" if regexm(source,"VR") == 1
	replace broadsource = "SRS" if regexm(source,"SRS") == 1
	keep if broadsource == "VR" | broadsource == "SRS" | broadsource == "DSP"
	duplicates drop ihme_loc_id year broadsource, force
	drop if outlier == 1 | shock == 1 
	keep ihme_loc_id year source broadsource
	tempfile gpr_vr
	save `gpr_vr', replace
	
	insheet using "$raw5q0_file", clear
	keep if indirect == "direct" 
	keep if shock == 1 
	keep ihme_loc_id year source 
	rename source source_y
	tempfile gpr_cbh_shock
	save `gpr_cbh_shock', replace
	
	insheet using "$raw5q0_file", clear
	keep if indirect == "direct" 
	keep if outlier == 1 
	keep ihme_loc_id source 
	rename source source_y
	duplicates drop 
	tempfile gpr_cbh_survey
	save `gpr_cbh_survey', replace 	
	
	insheet using "$raw5q0_file", clear
	keep if indirect == "direct"
	keep if outlier != 1 & shock != 1 
	keep ihme_loc_id source
	rename source source_y
	duplicates drop 
	tempfile gpr_cbh_keep
	save `gpr_cbh_keep', replace  
	restore 
	
	merge m:1 ihme_loc_id year broadsource using `gpr_vr'
	drop if _m == 2	
	replace exclude = 2 if _m == 1 & (broadsource == "VR" | broadsource == "SRS" | broadsource == "DSP") 
	drop _m 

	merge m:1 ihme_loc_id year source_y using `gpr_cbh_shock' 
	drop if _m == 2 
	replace exclude = 2 if _m == 3
	drop _m 
	
	merge m:1 ihme_loc_id source_y using `gpr_cbh_survey'
	drop if _m == 2
	replace exclude = 2 if _m == 3 
	drop _m 
	
	merge m:1 ihme_loc_id source_y using `gpr_cbh_keep'
	drop if _m == 2
	replace exclude = 2 if _m == 1 & !regexm(source,"VR") & !regexm(source,"SRS") & !regexm(source,"DSP") 
	drop _m
	compress 
	
** Exclude any VR/SRS that is incomplete 
	preserve
	keep if (broadsource == "VR" | broadsource == "SRS" | broadsource == "DSP") & sex == "both"
	keep ihme_loc_id year source broadsource q_u5
	tempfile incomplete
	save `incomplete', replace
	
	insheet using "FILEPATH", clear 
	rename med q5med
	cap destring q5med, replace force
	keep ihme_loc_id year q5med
	merge 1:m ihme_loc_id year using `incomplete' 
	assert _m != 2
	drop if _m == 1 
	drop _m 
	
	replace year = floor(year)
	summ year	
	local min = `r(min)'
	local max = `r(max)' 
	reshape wide q5med q_u5, i(ihme_loc_id source) j(year)
	order ihme_loc_id q5med* q_u5*
	forvalues y=`min'/`max' { 
		local lower = `y' - 4 
		if (`lower' < `min') local lower = `min'
		local upper = `y' + 4 
		if (`upper' > `max') local upper = `max' 
		egen temp1 = rowmean(q5med`lower'-q5med`upper')
		egen temp2 = rowmean(q_u5`lower'-q_u5`upper')
		gen avg_complete`y' = temp2 / temp1 						 
		drop temp*
	} 
	reshape long q5med q_u5 avg_complete, i(ihme_loc_id source) j(year)
	replace year = year + 0.5 
	drop if q_u5 == .
	
		gen complete = q_u5/q5med 	
	
	gen keep = complete < 0.85	
	replace keep = 0 if (avg_complete > 0.85) 	

	replace keep = 1 if (complete > 1.5 | complete < 0.5) 			 

	keep if keep == 1 
	keep ihme_loc_id year source 
	save `incomplete', replace 	
	restore
	
	merge m:1 ihme_loc_id year source using `incomplete'
	replace exclude = 3 if _m == 3 & (broadsource == "VR" | broadsource == "SRS" | broadsource == "DSP") 
	drop _m

foreach age in enn lnn nn pnn inf ch u5 {
	gen exclude_`age' = exclude 
	replace exclude_`age' = 12 if exclude==0 & q_`age' < 0.0001
}

replace exclude_lnn= 8 if ihme_loc_id == "GAB" & year ==1988.5 & (sex=="male" | sex=="both")
replace exclude_lnn= 8 if ihme_loc_id == "KAZ" & year ==1993 & (sex=="male" | sex=="both")


replace exclude_enn = 8 if exclude_enn==0 & ihme_loc_id=="BWA"
replace exclude_enn = 8 if exclude_enn==0 & ihme_loc_id=="ECU" & year<=1990
replace exclude_enn = 8 if exclude_enn==0 & ihme_loc_id=="IND_43885" & year<=1990 & sex=="female"
replace exclude_lnn = 8 if sex=="female" & ihme_loc_id =="IND_43900" & exclude_lnn==0 & source== "IND DHS urban rural"
replace exclude_ch = 8 if sex=="female" & ihme_loc_id =="IND_43900" & exclude_ch==0 & source=="VR" & year<1999
replace exclude_enn = 8 if ihme_loc_id =="MEX_4669" & year<1990 & year>1978 & exclude_enn ==0
replace exclude_enn = 8 if ihme_loc_id == "PAK" & year>=1985 & year<=2000 & exclude_enn==0 & (source== "PAK_IHS_1998-1999" | source=="PAK IHS 2001-2002")


gen exclude_sex_mod = 0 
merge m:1 ihme_loc_id using `four_five_star'

replace exclude_sex_mod = 1 if _m==3 & source!="VR" & !regexm(source, "DSP")
drop _m
drop star


	local identify = "ihme_loc_id year source source_y"

	tempfile almost
	save `almost', replace
	
	keep if sex == "both"
	tempfile tomerge
	save `tomerge', replace

	insheet using "FILEPATH", clear
	keep ihme_loc_id year med
	gen sex = "both"
	merge 1:m ihme_loc_id year using `tomerge'
	sort ihme_loc_id sex year
	by ihme_loc_id: ipolate med year, gen(med_interp) epolate
	gen s_comp = q_u5/med_interp
	drop if _m == 1
	drop med_interp med sex _m
	keep `identify' s_comp
	replace source_y = "999" if source_y == ""
	isid `identify'
	replace source_y = "" if source_y == "999"
	expand 3
	sort `identify'
	by `identify': gen sex = _n
	tostring sex, replace
	replace sex = "male" if sex == "1"
	replace sex = "female" if sex == "2" 
	replace sex = "both" if sex == "3"
	merge 1:1 `identify' sex using `almost'
	keep if _m==3
	drop _m
	
	

	drop if q_enn == . & q_lnn == . & q_nn == . & q_pnn == . & q_inf == . & q_ch == . & q_u5 == .
	
	replace exclude = 10 if withinsex == 1
	
	label values exclude exclude 	
	
	gen prob_enn = q_enn/q_u5 
 	gen prob_lnn = (1-q_enn)*q_lnn/q_u5 
  	gen prob_pnn = (1-q_nn)*q_pnn/q_u5
	gen prob_inf = q_inf/q_u5 	
	gen prob_ch = (1-q_inf)*q_ch/q_u5 				

	

	gen age_type = "inf/ch" if q_enn == . & q_lnn == . & q_pnn == . 
	replace age_type = "nn/pnn/ch" if q_enn == . & q_lnn == . & q_nn != . & q_pnn != . 
	replace age_type = "enn/lnn/pnn/ch" if q_enn != . & q_lnn !=. & q_pnn != . 
	count if age_type == "" 
	assert `r(N)' == 0 
	assert q_nn != . & q_pnn != . & q_ch != . if age_type == "nn/pnn/ch"


	merge m:1 ihme_loc_id using `codes'
	drop if _m == 2
	drop _m local_id_2013
	
	gen real_year = year
	replace year = floor(year)
	merge m:1 ihme_loc_id year sex using `natl_pop'
	keep if _m == 3
	drop _m year source_type
	rename real_year year
	
	order region_name ihme_loc_id year sex source age_type exclude q_* prob_* pop_* births
	replace source_y = "not CBH" if source_y == ""
	isid ihme_loc_id year sex source source_y
	replace source = source + "___" + source_y
	drop source_y
	
	preserve
	use "FILEPATH", clear
	keep ihme_loc_id year sex source_type comp
	keep if source_type == "VR"
	drop if comp == .
	isid ihme_loc_id year sex comp
	collapse (mean) comp, by(ihme_loc_id year)
	keep if comp >= .95
	drop comp
	replace year = year + .5
	tempfile adult_comp
	save `adult_comp', replace
	restore
	
	merge m:1 ihme_loc_id year using `adult_comp'
	replace exclude = 0 if (exclude == 1 | exclude == 7) & regexm(source,"VR") & _m == 3
	drop _m

	drop if (ihme_loc_id == "IND_4841" | ihme_loc_id == "IND_4871") & source == "India SRS Statistical Reports 1995-2013___not CBH"

	
	compress

	local date = subinstr("`c(current_date)'", " ", "_", 2)
	saveold "FILEPATH", replace
	export delimited "FILEPATH", replace
	saveold "FILEPATH", replace

	log close
	exit, clear
	
