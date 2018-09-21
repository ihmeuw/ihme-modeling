** Purpose: Make country-specific adjustments before running age sex splitting.

clear
set more off

// set the prefix for whichever os we're in
	if c(os) == "Windows" {
		global j "J:"
	}
	if c(os) == "Unix" { 
		global j "/home/j"
		set odbcmgr unixodbc
	}
	
 // Pass on globals from master_prep.do
	global data_name "`2'"
	global timestamp "`3'"
	** not used
	global tmpdir "`4'"
	global source_dir "`5'"
	
	log using "$source_dir/$data_name/logs/02a_before_agesex_${timestamp}", replace
	
	if "${data_name}"=="India_CRS" {
		** call urban rural splitting python program, wait for it to finish and use the output
		local resplit = 0
		if `resplit'==1 {
			!python "$j/WORK/03_cod/01_database/02_programs/urban_rural_splitting/code/gen_acause_weights.py"
			!python "$j/WORK/03_cod/01_database/02_programs/urban_rural_splitting/code/split_urban_rural.py"
		}
		import delimited using "$j/WORK/03_cod/01_database/02_programs/urban_rural_splitting/outputs/urban_rural_split_deaths.csv", case(preserve) clear

		keep iso3 subdiv location_id national source source_label source_type NID list frmat im_frmat sex year cause dev_status region cause_name acause deaths1 deaths2 deaths3 deaths4 deaths5 deaths6 deaths7 deaths8 deaths9 deaths10 deaths11 deaths12 deaths13 deaths14 deaths15 deaths16 deaths17 deaths18 deaths19 deaths20 deaths21 deaths22 deaths23 deaths24 deaths25 deaths26 deaths91 deaths92 deaths93 deaths94
		aorder deaths*
		order deaths*, last

		** Drop some data
		drop if acause=="tb" & (inlist(location_id, 43908, 43911, 43916, 43919, 43920, 43923) | inlist(location_id, 43926, 43928, 43931))
		** "Diarrhea- drop all CRS for cases below"
		aorder
		foreach var of varlist deaths7-deaths25 {
			** Females, Andhra PrUSER rural 1983-1995 (age groups 5-9 and above)
			replace `var' = . if acause=="diarrhea" & location_id==43908 & year>=1983 & year<=1995 & sex==2
			** Males/females, Arunachal PrUSER rural 1986-1995 (age groups 5-9 and above)
			replace `var' = . if acause=="diarrhea" & location_id==43909 & year>=1986 & year<=1995
			** Males/females, Delhi rural 1983-1995 (age group 5-9 and above)
			replace `var' = . if acause=="diarrhea" & location_id==43916 & year>=1983 & year<=1995
			** Males, Goa rural 1983, 1989-1995 (age groups 5-9 and above)
			replace `var' = . if acause=="diarrhea" & location_id==43917 & year>=1989 & year<=1995 & sex==1
			** Males/females, Haryana rural 1984-1995 (age groups 5-9 and above)
			replace `var' = . if acause=="diarrhea" & location_id==43919 & year>=1984 & year<=1995
			** Males/females, Maharashtra rural 1983-1995 (age groups 5-9 and above)
			replace `var' = . if acause=="diarrhea" & location_id==43927 & year>=1983 & year<=1995
			** Females, Nagaland rural 1983-1995 (ages 5-9 and above)
			replace `var' = . if acause=="diarrhea" & location_id==43931 & year>=1983 & year<=1995
			** Females, Six Minor Territories rural 1983-1995 (ages 5-9 and above)
			replace `var' = . if acause=="diarrhea" & location_id==44539 & year>=1983 & year<=1995
		}
		
		foreach var of varlist deaths15-deaths25 {
			** Females, Bihar rural 1987-1995 (age groups 45-49 and above)
			replace `var' = . if acause=="diarrhea" & location_id==43911 & year>=1987 & year<=1995 & sex==2
			** Females, Gujarat rural 1983-1995 (age groups 45-49 and above)
			replace `var' = . if acause=="diarrhea" & location_id==43918 & year>=1983 & year<=1995 & sex==2
		}

		** Males/females, Manipur rural 1984-1995
		drop if acause=="diarrhea" & location_id==43928 & year>=1984 & year<=1995

		** Lower respiratory infections:- Drop CRS for cases below
		** Males/females, Andhra PrUSER rural 1988-1995
		drop if acause=="lri" & location_id==43908 & year>=1988 & year<=1995
		** Females, Arunachal PrUSER rural 1986-1994
		drop if acause=="lri" & location_id==43909 & year>=1986 & year<=1994 & sex==2
		** Males/females, Bihar rural 1986-1995
		drop if acause=="lri" & location_id==43911 & year>=1986 & year<=1995
		** Males/females, Delhi rural 1983-1995
		drop if acause=="lri" & location_id==43916 & year>=1983 & year<=1995
		** Males, Gujarat rural 1988-95
		drop if acause=="lri" & location_id==43918 & year>=1988 & year<=1995 & sex==1
		** Males/females, Karnataka rural 1988-95
		drop if acause=="lri" & location_id==43923 & year>=1988 & year<=1995
		** Males, Maharashtra rural 1988-1995
		drop if acause=="lri" & location_id==43927 & year>=1988 & year<=1995 & sex==1
		** Females, Maharashtra rural 1983-95
		drop if acause=="lri" & location_id==43927 & year>=1983 & year<=1995 & sex==2
		** Males/females, Tamil Nadu urban and rural 1992, 1993, 1995
		drop if acause=="lri" & (location_id==43937 | location_id==43901) & inlist(year, 1992, 1993, 1995)

		
		local drop_states `" "Goa" "Gujarat" "Haryana" "Himachal PrUSER" "Karnataka" "Kerala" "Madhya PrUSER" "Maharashtra" "Manipur" "Mizoram" "Orissa" "Punjab" "Rajasthan" "Tamil Nadu" "The Six Minor Territories" "'
		local drop_rural `" "Jammu and Kashmir" "Tripura" "West Bengal" "'
		foreach state of local drop_states {
			drop if acause=="diabetes" & regexm(subdiv, "`state'")
		}
		foreach state of local drop_rural {
			drop if acause=="diabetes" & regexm(subdiv, "`state'") & regexm(subdiv, "Rural")
		}

		** Drop where CRS and MCCD are misaligned. These are systematic drops of the CRS data
		** keeping only '1' or master only drops any location_id sex acause that is in both the referenced stata file and the current dataset
		merge m:1 location_id sex acause using "$source_dir/${data_name}/maps/state_sex_acauses_to_drop.dta", keepusing(location_id sex acause) keep(1) nogen

		save "$source_dir/${data_name}/data/intermediate/02a_before_agesex.dta", replace
		save "$source_dir/${data_name}/data/intermediate/_archive/02a_before_agesex_${timestamp}.dta", replace
	}
	else if "${data_name}"=="India_SCD_states_rural" {
		do "$j/WORK/03_cod/01_database/03_datasets/India_SCD_states_rural/code/02a_before_agesex_India_SCD_states_rural.do"
	}
	else {
		noisily di "Nothing to see here"
	}

	capture log close

