// Prepare EUROCAT prevalence tables for upload to the database
// {AUTHOR NAME}
// November 2016

/* Outline:
	- format data in the prevalence tables downloaded from the EUROCAT website
	- add additional variables necessary for Epi uploader
	- add notes about the differences between the registries

*/

// setup
	clear all
	set more off
	set maxvar 10000

	cap restore, not
	cap log close


// file paths
	local in_dir "{FILEPATH}"
	local out_dir "{FILEPATH}"

	local date {DATE} 

//------------------------------------------------------------------------------
// load data

	// full-member registries
	import excel using "`in_dir'\EUROCAT_PREV_TABLE_INDIV_ALL_REG_COUNTRIES_FULL_MEMBERS_1980_2014_Y2016M11D22.XLSX", clear
		tempfile raw_full
		save `raw_full', replace 

	// associate registries
	import excel using "`in_dir'\EUROCAT_PREV_TABLE_INDIV_ALL_REG_COUNTRIES_ASSOC_MEMBERS_1980_2014_Y2016M11D22.XLSX", clear 
		tempfile raw_assoc
		save `raw_assoc', replace 

	// UK registries; these are actually duplicate with the registries in the above files. 
	 import excel using "`in_dir'\EUROCAT_GBR_PREV_TABLE_COMBINED_UK_1980_2014_Y2016M11D22.XLSX", clear 
		tempfile raw_uk 
		save `raw_uk', replace 
		
//---------------------------------------------------------------------
// basic formatting 

// FOR THE FULL-MEMBER REGISTRIES ONLY 
local file `raw_full'
	use `file', replace 

	// rename vars 
	keep A B C D E F G H P Q R S 
		rename A registry_name_old
		rename B case_name
		rename C year_start
		rename D sample_size

		rename E cases_lb
		rename F cases_fd
		rename G cases_top
		rename H cases_total

		rename P cases_lb_nonchromo
		rename Q cases_fd_nonchromo
		rename R cases_top_nonchromo
		rename S cases_total_nonchromo

		drop in 1/4

		gen rownum_temp = _n
		drop if rownum_temp > 127119 // extra rows with information at the end of the table 

			destring year_start, replace 
			destring cases* sample_size, replace ignore("-" " " "*")
				format case_name %63s
		 		format registry_name %29s

		 		

//-------------------------------------------------------
// make a template dataset with all info we would expect to have if data were "square"
preserve
	drop if case_name =="" & registry_name_old==""
	keep registry_name_old case_name year_start 

		gen temp = _n 
			replace temp =. if case_name != "All Anomalies"
			replace temp = temp[_n-1] if temp >= .

			bysort temp: carryforward registry_name_old, gen(reg_name)
				drop registry_name_old
				rename reg_name registry_name_old

		// cross-join the three indentifier variables into a template dataset 
			keep case_name registry_name_old
			tempfile reg_list1
			save `reg_list1', replace

			clear all
			set obs 35 
				gen temp = _n
				gen year_start = temp + 1979
				drop temp
					tempfile years
					save `years', replace 

			cross using `reg_list1'
				order reg case_name year 
	
	tempfile template1
	save `template1', replace 

restore
//----------------------------------------------
	
	// fill in the registries 
	gen temp = _n 
		replace temp =. if case_name != "All Anomalies"
		replace temp = temp[_n-1] if temp >= .
			bysort temp: carryforward registry_name_old, gen(reg_name)
				drop temp registry_name_old
				rename reg_name registry_name_old

	// fill in the case names 
	gen temp = _n
		replace temp = . if year !=1980 // assumes none of the registry/case names are missing rows for 1980 
		replace temp = temp[_n-1] if temp >=.
			bysort temp: carryforward case_name, gen(case_name2)
				drop temp case_name
				rename case_name2 case_name 

	// merge to the template dataset to find the years that are missing 
	merge m:1 registry_name_old case_name year using `template1'
		order registry case_name year_start 
		sort registry case_name year_start 
			drop rownum_temp _m


		gen file_path = "{FILEPATH}\EUROCAT_PREV_TABLE_INDIV_ALL_REG_COUNTRIES_FULL_MEMBERS_1980_2014_Y2016M11D22.XLSX"

	tempfile square_full_members
	save `square_full_members', replace 

//------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------
// Do the same formatting for the associate registries:
	local file `raw_assoc'
			use `file', replace 

	// rename vars 
	keep A B C D E F G H P Q R S 
		rename A registry_name_old
		rename B case_name
		rename C year_start
		rename D sample_size

		rename E cases_lb
		rename F cases_fd
		rename G cases_top
		rename H cases_total

		rename P cases_lb_nonchromo
		rename Q cases_fd_nonchromo
		rename R cases_top_nonchromo
		rename S cases_total_nonchromo

		drop in 1/4
		gen rownum_temp = _n
				drop if rownum_temp > 82810

			destring year_start, replace 

		destring cases* sample_size, replace ignore("-" " " "*")
			format case_name %70s
			format registry_name %40s // check on this 

//---------------------------------------------------
// make a template dataset with all info we would expect to have if data were "square"
preserve
	drop if case_name =="" & registry_name_old==""
	keep registry_name_old case_name year_start 

		gen temp = _n 
			replace temp =. if case_name != "All Anomalies"
			replace temp = temp[_n-1] if temp >= .

			bysort temp: carryforward registry_name_old, gen(reg_name)
				drop registry_name_old
				rename reg_name registry_name_old

		// cross-join the three indentifier variables into a template dataset 
			keep case_name registry_name_old
			tempfile reg_list2
			save `reg_list2', replace

			clear all
			set obs 35 
				gen temp = _n
				gen year_start = temp + 1979
				drop temp
					tempfile years
					save `years', replace 

			cross using `reg_list2'
				order reg case_name year 
	
	tempfile template2
	save `template2', replace 

restore
//-----------------------------------------
// fill in the registries 
	gen temp = _n 
		replace temp =. if case_name != "All Anomalies"
		replace temp = temp[_n-1] if temp >= .
			bysort temp: carryforward registry_name_old, gen(reg_name)
				drop temp registry_name_old
				rename reg_name registry_name_old

	// fill in the case names 
	gen temp = _n
		replace temp = . if year !=1980 // assumes none of the registry/case names are missing rows for 1980 
		replace temp = temp[_n-1] if temp >=.
			bysort temp: carryforward case_name, gen(case_name2)
				drop temp case_name
				rename case_name2 case_name 

	// merge to the template dataset to find the years that are missing 
	merge m:1 registry_name_old case_name year using `template2'
		order registry case_name year_start 
		sort registry case_name year_start 
			

			drop rownum_temp _m


		gen file_path ="{FILEPATH}\EUROCAT_PREV_TABLE_INDIV_ALL_REG_COUNTRIES_ASSOC_MEMBERS_1980_2014_Y2016M11D22.XLSX"
 
	tempfile square_assoc_members
	save `square_assoc_members', replace 

//---------------------------------------------------------------------------------

// Do same formatting for the UK country-specific data
	// Need to add UK country-wide totals separately because individual registries suppress data below a certain threshold,
	// but the country totals in the country-specific don't suppress these data. 

 
local file `raw_uk'
	use `file', replace 

	// rename vars 
	keep A B C D E F G H P Q R S 
		rename A registry_name_old
		rename B case_name
		rename C year_start
		rename D sample_size

		rename E cases_lb
		rename F cases_fd
		rename G cases_top
		rename H cases_total

		rename P cases_lb_nonchromo
		rename Q cases_fd_nonchromo
		rename R cases_top_nonchromo
		rename S cases_total_nonchromo

		drop in 1/4 

		gen rownum_temp = _n
		drop if rownum_temp > 3185 // extra rows with information at the end of the table 

			destring year_start, replace 

		destring cases* sample_size, replace ignore("-" " " "*")
		 	format case_name %63s
		 	format registry_name %29s

//-------------------------------------------------------
// make a template dataset with all info we would expect to have if data were "square"
preserve
	drop if case_name =="" & registry_name_old==""
	keep registry_name_old case_name year_start 

		gen temp = _n 
			replace temp =. if case_name != "All Anomalies"
			replace temp = temp[_n-1] if temp >= .

			bysort temp: carryforward registry_name_old, gen(reg_name)
				drop registry_name_old
				rename reg_name registry_name_old

		// cross-join the three indentifier variables into a template dataset 
			keep case_name registry_name_old
			tempfile reg_list1
			save `reg_list1', replace

			clear all
			set obs 35 
				gen temp = _n
				gen year_start = temp + 1979
				drop temp
					tempfile years
					save `years', replace 

			cross using `reg_list1'
				order reg case_name year 
	
	tempfile template1
	save `template1', replace 

restore
//----------------------------------------------
	
	// fill in the registries 
	gen temp = _n 
		replace temp =. if case_name != "All Anomalies"
		replace temp = temp[_n-1] if temp >= .
			bysort temp: carryforward registry_name_old, gen(reg_name)
				drop temp registry_name_old
				rename reg_name registry_name_old

	// fill in the case names 
	gen temp = _n
		replace temp = . if year !=1980 // assumes none of the registry/case names are missing rows for 1980 
		replace temp = temp[_n-1] if temp >=.
			bysort temp: carryforward case_name, gen(case_name2)
				drop temp case_name
				rename case_name2 case_name 

	// merge to the template dataset to find the years that are missing 
	merge m:1 registry_name_old case_name year using `template1'
		order registry case_name year_start 
		// sort registry case_name year_start 
			drop rownum_temp _m


	// note that this is only the total UK data (the data that doesn't have as many suppressed values)
		replace registry_name = "United Kingdom (total)"


		gen file_path = "{FILEPATH}\EUROCAT_GBR_PREV_TABLE_COMBINED_UK 1980_2012_Y2015M01D07.XLSX"

	tempfile square_uk
	save `square_uk', replace 


//---------------------------------------------------------------------------------
// append datasets together

	use `square_full_members', clear
		append using `square_assoc_members'
		append using `square_uk'

tempfile appended
save `appended', replace 

//-------------------------------------------------------------------------------------------
//--------------------------------------------------------------------------------------------

// clean "registry_name" variable and merge in location information
use `appended', clear	

	// drop duplicate registries; don't need sums by country 
		drop if regexm(registry_name_old, "%")

	// format the registry names
		replace registry = substr(registry_name_old, 1, length(registry_name_old)-2) if regexm(registry, "[0-9]")
			replace registry = trim(registry_name_old)
			rename registry_name_old registry_name

	// generate location information from the registry names 
		split registry, p(" - ")
			rename registry_name2 country
			rename registry_name1 site_memo
				foreach loc in Portugal Ireland Malta Norway Ukraine Poland Finland Hungary France "Czech Republic" Spain Sweden Italy {
							replace country = "`loc'" if regexm(site_memo, "`loc'")	
							}
				replace country = "United Kingdom" if country =="UK" | regexm(registry, "United Kingdom")
				replace country = "Netherlands" if country == "NL"

					gen location_name = country 

						// merge in other location information from get_locations shared function
						preserve
							 run "{FILEPATH}\get_location_metadata.ado"
							get_location_metadata, location_set_id(9) clear
									duplicates drop location_name, force 
							tempfile locations 
							save `locations', replace 
						restore

						merge m:1 location_name using `locations', keepusing(ihme_loc_id location_id) keep(1 3) nogen

	save "{FILEPATH}\eurocat_before_uk_location_assignments.dta", replace 

	use "{FILEPATH}\eurocat_before_uk_location_assignments.dta", clear 


//-----------------------------------------------------------------
// Mapping UK subnational regions: UPDATED according to new protocol

       replace location_name = "Scotland" if registry_name =="Glasgow - UK"
       		replace location_id = 434 if location_name =="Scotland"

   	replace location_name = "Wales" if registry_name == "Wales - UK"
        	replace location_id = 4636 if location_name == "Wales"

       		preserve
       		import excel using "{FILEPATH}\uk_population_weights_for_EUROCAT_registries_2016_12_23.xlsx", firstrow clear 
       			tostring population, replace 
       			tempfile uk_pops
       			save `uk_pops', replace
       		restore

	gen note_SR =""
       			
			//---------------
       		// "Merseyside & Cheshire - UK"
       			preserve
       			use `uk_pops', clear 
       				keep if Merseyside_Cheshire== 1
       					keep location_id location_name population Merseyside_Cheshire 
       					egen total = total(population)
       					gen weight = population/total
       					gen registry_name = "Merseyside & Cheshire - UK"
       				gen count = _n
       					local n = _N
       				tempfile uk1
       				save `uk1', replace 
       			restore

       	expand `n' if registry_name== "Merseyside & Cheshire - UK"
			bysort registry_name year_start sample_size site_memo country location_name location_id case_name ///
				: gen count = _n
       	merge m:1 registry_name count using `uk1' , keepusing(weight location_name location_id) nogen update replace

       					// want to have duplicated rows for each region
       					foreach var in cases_lb cases_top cases_total cases_lb_nonchromo cases_fd_nonchromo cases_top_nonchromo cases_total_nonchromo sample_size {
       						replace `var' = `var' * weight if registry_name=="Merseyside & Cheshire - UK"
       							}
							replace note_SR = "Split data from the Merseyside & Cheshire registry to underlying administrative areas using population weights" if registry_name=="Merseyside & Cheshire - UK"
       							drop weight count

       		//------------
       		// "East Midlands & South Yorkshire - UK"
       			// South Yorkshire and the counties of Derbyshire, Nottinghamshire, Lincolnshire, Leicestershire, Rutland and Northamptonshire
       			preserve
       			use `uk_pops', clear 
       				keep if East_Midlands_S_Yorkshire==1
       				keep location_id location_name population East_Midlands_S_Yorkshire
       					egen total = total(population)
       					gen weight = population/total
       					gen registry_name = "East Midlands & South Yorkshire - UK"
       				gen count = _n
       					local n = _N
       				tempfile uk2
       				save `uk2', replace 
       			restore

		expand `n' if registry_name== "East Midlands & South Yorkshire - UK"
			bysort registry_name year_start sample_size site_memo country location_name location_id case_name ///
				: gen count = _n
       	merge m:1 registry_name count using `uk2' , keepusing(weight location_name location_id) nogen update replace 

       					// want to have duplicated rows for each region
       					foreach var in cases_lb cases_top cases_total cases_lb_nonchromo cases_fd_nonchromo cases_top_nonchromo cases_total_nonchromo sample_size {
       						replace `var' = `var' * weight if registry_name=="East Midlands & South Yorkshire - UK"
       							}
       				replace note_SR = "Split data from the East Midlands & South Yorkshire registry to underlying administrative areas using population weights" if registry_name=="East Midlands & South Yorkshire - UK"
       					drop weight count

       	//------------
       	// "N W Thames - UK"
       		// North West London, Hertfordshire (East of England region) and Bedfordshire
       		preserve
       			use `uk_pops', clear 
       				keep if NW_Thames==1
       				keep location_id location_name population NW_Thames
       					egen total = total(population)
       					gen weight = population/total
       					gen registry_name = "N W Thames - UK"
       				gen count = _n
       					local n = _N
       				tempfile uk3
       				save `uk3', replace 
       			restore

		expand `n' if registry_name== "N W Thames - UK"
			bysort registry_name year_start sample_size site_memo country location_name location_id case_name ///
				: gen count = _n
       	merge m:1 registry_name count using `uk3' , keepusing(weight location_name location_id) nogen update replace 

       					// want to have duplicated rows for each region
       					foreach var in cases_lb cases_top cases_total cases_lb_nonchromo cases_fd_nonchromo cases_top_nonchromo cases_total_nonchromo sample_size {
       						replace `var' = `var' * weight if registry_name=="N W Thames - UK"
       							}
       				replace note_SR = "Split data from the N W Thames registry to underlying administrative areas using population weights" if registry_name=="N W Thames - UK"
       						drop weight count

       	//------------
       	// "Northern England - UK"
       		 // Northern England: North Cumbria, Northumberland, Newcastle upon Tyne, North Tyneside, Gateshead, South Tyneside, County Durham, Darlington and Tees
       		preserve
       			use `uk_pops', clear 
       				keep if Northern_England==1
       				keep location_id location_name population Northern_England
       					egen total = total(population)
       					gen weight = population/total
       					gen registry_name = "Northern England - UK"
       				gen count = _n
       					local n = _N
       				tempfile uk4
       				save `uk4', replace 
       			restore

		expand `n' if registry_name== "Northern England - UK"
			bysort registry_name year_start sample_size site_memo country location_name location_id case_name ///
				: gen count = _n
       	merge m:1 registry_name count using `uk4' , keepusing(weight location_name location_id) nogen update replace 

       					// want to have duplicated rows for each region
       					foreach var in cases_lb cases_top cases_total cases_lb_nonchromo cases_fd_nonchromo cases_top_nonchromo cases_total_nonchromo sample_size {
       						replace `var' = `var' * weight if registry_name=="Northern England - UK"
       							}
       				replace note_SR = "Split data from the Northern England registry to underlying administrative areas using population weights" if registry_name=="Northern England - UK"
       					drop weight count

       	//-------------
       	// "South West England - UK"
       		preserve
       			use `uk_pops', clear 
       				keep if Southwest_England==1
       				keep location_id location_name population Southwest_England
       					egen total = total(population)
       					gen weight = population/total
       					gen registry_name = "South West England - UK"
       				gen count = _n
       					local n = _N
       				tempfile uk5
       				save `uk5', replace 
       			restore

		expand `n' if registry_name== "South West England - UK"
			bysort registry_name year_start sample_size site_memo country location_name location_id case_name ///
				: gen count = _n
       	merge m:1 registry_name count using `uk5' , keepusing(weight location_name location_id) nogen update replace 

       					// want to have duplicated rows for each region
       					foreach var in cases_lb cases_top cases_total cases_lb_nonchromo cases_fd_nonchromo cases_top_nonchromo cases_total_nonchromo sample_size {
       						replace `var' = `var' * weight if registry_name=="South West England - UK"
       						}
       				replace note_SR = "Split data from the South West England registry to underlying administrative areas using population weights" if registry_name=="South West England - UK"
       					drop weight count

		//-------------- 
		// "Thames Valley - UK"
			// Oxfordshire, Berkshire and Buckinghamshire after 2005; Oxfordshire only before 2005
				preserve
       			use `uk_pops', clear 
       				keep if Thames_Valley==1
       				keep location_id location_name population Thames_Valley
       					egen total = total(population)
       					gen weight = population/total
       					gen registry_name = "Thames Valley - UK"
       				gen count = _n
       					local n = _N
       				tempfile uk6
       				save `uk6', replace 
       			restore

		expand `n' if registry_name== "Thames Valley - UK"
			bysort registry_name year_start sample_size site_memo country location_name location_id case_name ///
				: gen count = _n
       	merge m:1 registry_name count using `uk6' , keepusing(weight location_name location_id) nogen update replace 

       					// want to have duplicated rows for each region
       					foreach var in cases_lb cases_top cases_total cases_lb_nonchromo cases_fd_nonchromo cases_top_nonchromo cases_total_nonchromo sample_size {
       						replace `var' = `var' * weight if registry_name== "Thames Valley - UK"
       						}
       				replace note_SR = "Split data from the Thames Valley registry to underlying administrative areas using population weights" if registry_name== "Thames Valley - UK"
       					drop weight count

       	//--------------
       	// "Wessex - UK"
       		 // the "Wessex" region corresponds to Dorset, Somerset, Wiltshire and Hampshire // http://www.destinationwessex.org.uk/about-wessex.htm
       			preserve
       			use `uk_pops', clear 
       				keep if Wessex==1
       				keep location_id location_name population Wessex
       					egen total = total(population)
       					gen weight = population/total
       					gen registry_name = "Wessex - UK"
       				gen count = _n
       					local n = _N
       				tempfile uk7
       				save `uk7', replace 
       			restore

		expand `n' if registry_name== "Wessex - UK"
			bysort registry_name year_start sample_size site_memo country location_name location_id case_name ///
				: gen count = _n
       	merge m:1 registry_name count using `uk7' , keepusing(weight location_name location_id) nogen update replace 

       					// want to have duplicated rows for each region
       					foreach var in cases_lb cases_top cases_total cases_lb_nonchromo cases_fd_nonchromo cases_top_nonchromo cases_total_nonchromo sample_size {
       						replace `var' = `var' * weight if registry_name== "Wessex - UK"
       						}
       				replace note_SR = "Split data from the Wessex registry to underlying administrative areas using population weights" if registry_name== "Wessex - UK"
       					drop weight count

        	
       	// Two copies of the total UK data, one assigned to each of England and Wales:
        expand 2 if registry_name == "United Kingdom (total)", gen(new)   
           replace location_name = "England" if registry_name == "United Kingdom (total)" & new==0
        		replace location_id = 4749 if registry_name == "United Kingdom (total)" & new==0
     		replace location_name = "Wales" if registry_name == "United Kingdom (total)" & new==1
        		replace location_id = 4636 if registry_name == "United Kingdom (total)" & new==1
        			drop new 
    
  		replace note_SR = "This is total data from all UK EUROCAT member registries, aggregated. Aggregating to all UK registries un-censors some of the cases that were censored due to small numbers of cases. Two copies are uploaded, one assigned to England and one to Wales." if registry_name == "United Kingdom (total)"   

//-------------------------------------------------------------------

	// merge NIDs and citation information onto the registries: NIDs are assigned by country, not by underlying registries 

		// create a mapping file for these
		preserve
			keep registry_name country
			duplicates drop 
			export delimited using "{FILEPATH}\eurocat_nid_mapping2.csv", replace
		restore
				
		preserve
			import excel using "{FILEPATH}\eurocat_nid_mapping2.xls", firstrow clear
				destring nid, replace ignore(" ")
			tempfile nid_map 
			save `nid_map', replace
		restore
			merge m:1 registry_name country using `nid_map', keepusing(nid field_citation_value) nogen 

//------------------------------------------------------------------------------------------------------
// reshape to get data long on cv_includes_chromos
	// easier to subset and then append
	preserve
		drop cases_*nonchromo 
		gen cv_includes_chromos = 1
			tempfile includes_chromos 
			save `includes_chromos', replace
	restore

	drop cases_lb cases_top cases_total cases_fd
	rename *_nonchromo *
		gen cv_includes_chromos=0
			append using `includes_chromos'

	tempfile ready_for_case_map
	save `ready_for_case_map', replace 	

//-----------------------------------------------------------------------------------------------------------
// case mapping for each EUROCAT registry 

	// make a case_name mapping file
	
	preserve
		keep case_name
		duplicates drop
			gen cause = ""
			gen modelable_entity = ""
		export delimited using "{FILEPATH}\eurocat_case_mapping.csv", replace 
	restore
			

use `ready_for_case_map', clear 

	// merge in the cause mapping file 
	preserve
	import excel using "{FILEPATH}\eurocat_case_mapping.xls", firstrow clear
		drop notes E 
		replace modelable = trim(modelable)
			sort cause modelable_entity_name 

			tempfile eurocat_cause_map
			save `eurocat_cause_map', replace 
	restore 

	merge m:1 case_name using `eurocat_cause_map', keepusing(cause modelable_entity_name) nogen

		// need to confirm which case names are totals for each category of birth defects 
		preserve
			keep if modelable=="Total"

				drop modelable
				rename case_name modelable_entity_name 

			tempfile totals
			save `totals', replace
		restore

			// one registry doesn't report orofacial clefts by type, but still want to keep these data:
		replace modelable="Cleft lip +/- cleft palate" if case_name=="Oro-facial clefts" & regexm(registry, "Sicily") 
		drop if modelable=="Total" 

//-------------------------------------------------------------------------

	// NOTE that the England subnational registries have censored live births and terminations below a certian threshold, ///
		// but have included the total case counts when these are above the minimum threshold.
			// Fill in cases_lb with the available information where possible 

			gen cv_includes_top =. 
			gen cv_includes_stillbirths =.

			// if we have total and top counts: fill in with cv_includes_top =1
			gen to_replace1 = 1 if (cases_lb ==. & cases_fd ==. & cases_top !=. & cases_total !=.)
				replace cases_lb = cases_total - cases_top if to_replace1  ==1
				replace cv_includes_top =1 if to_replace1 ==1


			// if we have total and fd counts: fill in with cv_includes_stillbirths =1
			 gen to_replace2 = 1 if (cases_lb ==. & cases_top ==. & cases_fd !=. & cases_total !=.)
				replace cases_lb = cases_total - cases_fd if to_replace2 ==1
				replace cv_includes_stillbirths =1 if to_replace2 ==1 
					 

			// if we only have total counts: fill in with cv_includes_stillbirths =1 & cv_includes_top =1
			gen to_replace3 = 1 if (cases_lb ==. & cases_top ==. & cases_fd ==. & cases_total !=.)
				replace cases_lb = cases_total if to_replace3 ==1
				replace cv_includes_stillbirths =1 if to_replace3 ==1
				replace cv_includes_top =1 if to_replace3 ==1

			drop to_replace*
			replace cause = trim(cause)

tempfile ready_to_collapse
save `ready_to_collapse', replace 

//------------------------------------------------------------------------------

// collapse from case names to modelable entities
use `ready_to_collapse', clear 

		// drop missing values... so they aren't included in the collapse
			drop if sample_size==. 
			
//-----------------
// concatenate the case_names 
egen collapse_group = group(registry_name modelable_entity year_start sample_size site_memo country ///
	 location_name location_id ihme_loc_id nid field_citation_value ///
			cv_includes_chromos cause modelable_entity_name note_SR), missing

		replace case_name = subinstr(case_name, `"""', "", . )
			replace note_SR = subinstr(note_SR, `"""', "", . )
		sort collapse_group modelable_entity_name case_name 
		
		// case_name
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]
	
//---------------------------------------
// add sex splits for cleft lip
		// Sex split = 64% of those with cleft lip (with or without cleft palate)  ///
				// and 43.5% of those with isolated cleft palate are male (Reference NIDs 310647, 310645, and 310649). 
			
			gen sex=""
			gen group =.
			gen specificity=""
			gen group_review=.

		// creating male and female data points for birth prevalence, and using group review to hide the both-sex cleft data 
			// not splitting the counts of terminations or stillbirths by gender, only the live birth counts 
		preserve
		keep if modelable=="Cleft lip +/- cleft palate" & registry != "Sicily - Italy"
			expand 2 if modelable== "Cleft lip +/- cleft palate" , gen(new) // note that the Sicily registry doesn't have the breakdown of cleft types 

			replace sample_size = sample_size/2 

				replace sex = "Male" if new==0
					replace cases_lb = cases_lb * 0.64 if new==0 & case_name == "Cleft lip with or without palate"
					replace cases_lb = cases_lb * 0.435 if new==0 & case_name == "Cleft palate"

				replace sex = "Female" if new == 1
					replace cases_lb = (cases_lb* (1 - 0.64)) if new==1 & case_name == "Cleft lip with or without palate"
					replace cases_lb = (cases_lb * (1 - 0.435)) if new==1 & case_name == "Cleft palate"

			replace cases_top =.
			replace cases_fd =.
			replace cases_total =.

			replace group = 1
			replace specificity="sex split"
			replace group_review =1

		tempfile cleft_sex_split
		save `cleft_sex_split', replace
		restore

		append using `cleft_sex_split'
			// use group review to hide non-split data where split data is available 
			replace specificity = "both sexes" if modelable=="Cleft lip +/- cleft palate" & sex=="" & registry != "Sicily - Italy"
			replace group_review =0 if modelable=="Cleft lip +/- cleft palate" & sex=="" & registry != "Sicily - Italy"
			replace group = 2 if modelable=="Cleft lip +/- cleft palate" & sex=="" & registry != "Sicily - Italy"

		//-------------
		// Test group_review strategy	
			// summarize cases_lb if sex=="Male"
			// summarize cases_lb if sex=="Female"
			// summarize cases_lb if sex=="" & modelable_entity_name=="Cleft lip +/- cleft palate"
		//--------------

//--------------
// fill in cv_subset (currently called cv_underreport)

	gen cv_subset =0
	replace cv_subset =1 if new_case_name =="Coarctation of aorta, Ebstein's anomaly, Pulmonary valve stenosis"
	replace cv_subset =1 if new_case_name =="Common arterial truncus, Hypoplastic left heart, Single ventricle"
	replace cv_subset =1 if new_case_name =="Hypoplastic left heart, Single ventricle"
	replace cv_subset =1 if new_case_name =="Club foot - talipes equinovarus, Craniosynostosis, Skeletal dysplasias §"
	replace cv_subset =1 if new_case_name== "Patau syndrome/trisomy 13"
	replace cv_subset =1 if new_case_name== "Aortic atresia/interrupted aortic arch, Mitral valve anomalies"
	replace cv_subset =1 if new_case_name== "PDA as only CHD in term infants (>=37 weeks)"
	replace cv_subset =1 if new_case_name== "Aortic valve atresia/stenosis §"
	replace cv_subset =1 if new_case_name== "Double outlet right ventricle §"
	replace cv_subset =1 if new_case_name== "Hypoplastic right heart §"
	replace cv_subset =1 if new_case_name== "Hypospadias"
	replace cv_subset =1 if new_case_name== "Indeterminate sex"

		// "Severe CHD" is the EUROCAT total for most of the heart conditions; can drop this 
			drop if regexm(new_case_name, "Severe CHD")

		
drop new 
tempfile ready_to_collapse2
save `ready_to_collapse2', replace

use `ready_to_collapse2', clear
//------------------------------------------------------
// collapse to the modelable entity level
	gen case_name_count = 1 // to keep track of how many cases are included in each modelable entity 

	 collapse (sum) cases_* case_name_count (max) cv_includes_top cv_includes_stillbirths, by(registry_name year_start sample_size site_memo country location_name location_id ihme_loc_id nid field_citation_value ///
	 		cv_includes_chromos cause modelable_entity_name new_case_name sex specificity group group_review note_SR)

	 // drop "other" data that will not be uploaded
	 	drop if modelable_entity=="exclude" | modelable_entity=="other" | modelable_entity=="?"
	 	drop if modelable_entity=="other cardio" | modelable_entity=="other digest" | modelable_entity=="other musculoskeletal"

tempfile collapsed1 
save `collapsed1', replace

//---------------------------------------------------------------------------
// now collapse to the cause level, for the "total" modelable entities

use `ready_to_collapse2', clear 
	keep if inlist(cause, "heart", "neural", "digest", "msk")
	drop collapse_group new_case_name 

	// concatenate the new_case_names again 
	egen collapse_group = group(registry_name year_start sample_size site_memo country ///
	 location_name location_id ihme_loc_id nid field_citation_value ///
			cv_includes_chromos cause note_SR), missing

		replace case_name = subinstr(case_name, `"""', "", . )
			replace note_SR = subinstr(note_SR, `"""', "", . )
		sort collapse_group case_name 
		
		// case_name
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]

		// if one of the ME's within a total has cv_subet =1, then the total has cv_subset =1
		bysort collapse_group: egen cv_subset2 = max(cv_subset)
				replace cv_subset = cv_subset2
				drop cv_subset2

 	collapse (sum) cases_*  (max) cv_includes_top cv_includes_stillbirths, by(registry_name year_start sample_size site_memo country location_name location_id ihme_loc_id nid ///
 		 field_citation_value cause sex specificity group group_review cv_includes_chromos note_SR new_case_name cv_subset)

 		 // fill in ME information
		gen modelable_entity_name=""
 				replace modelable= "Total congenital digestive birth defects" if cause=="digest"
 				replace modelable= "Total congenital heart birth defects" if cause=="heart"
 				replace modelable= "Total congenital musculoskeletal birth defects" if cause=="msk"
 				replace modelable= "Total neural tube defects" if cause=="neural"
 					 	
 	// append the collapsed non-total modelable entities
 	append using `collapsed1'
 	replace cause = trim(cause)

 		tempfile collapsed2
 		save `collapsed2', replace

//--------------------------------------------------------------

	// fill in the bundle_id values	
	preserve
			import excel using "{FILEPATH}\Bundle_to_ME_map_{USERNAME}.xlsx", firstrow clear 
				gen is_birth_prev = 1 if regexm(modelable_entity_name, "irth prevalence")
					replace is_birth_prev =0 if is_birth_prev==.
					keep if modelable_entity_name_old !=""
					replace modelable_entity_name_old =trim(modelable_entity_name_old)

				tempfile bundle_map
				save `bundle_map', replace
			restore

		rename modelable_entity_name modelable_entity_name_old
			replace modelable_entity_name_old =trim(modelable_entity_name_old)
		gen is_birth_prev =1 

	merge m:1 modelable_entity_name_old is_birth_prev using `bundle_map', nogen keep(1 3)

//  ~ drop data that our team does not model ~ 
	drop if inlist(modelable_entity_name_old, "alcohol", "microcephaly")
		drop modelable_entity_name_old

//-------------------------------------------------------------
// calculate means

	gen measure = "prevalence"
	gen mean = cases_lb/sample_size 
	

//-------------------------------------------------------------
// need to fill in other covariates and metadata 

	// add variables in the order of the Epi template 
	
		gen underlying_nid =""
		gen page_num =""
		gen table_num =""
		gen file_path = "{FILEPATH}" 

		gen source_type = "Registry - congenital"
		gen smaller_site_unit = 1 if site_memo != country 
			replace smaller_site_unit = 0 if smaller_site_unit ==.

		replace sex ="Both" if sex==""
		gen sex_issue =0
		gen year_end = year_start 
		gen year_issue =0
		gen age_start = 0
		gen age_issue =0
		gen age_end = 0
		gen age_demographer = 0

		gen lower =.
		gen upper =.
		gen standard_error =. 
		rename cases_lb cases 
		gen effective_sample_size = .

		gen unit_type = "Person"
		gen unit_value_as_published = 1
		gen measure_issue = 0
		gen measure_adjustment = 0 
			replace measure_adj =1 if cv_includes_stillbirths ==1
			replace measure_adj =1 if cv_includes_top ==1
		gen uncertainty_type_value = .
		gen uncertainty_type = "Effective sample size"

		gen representative_name = "Representative for subnational location only" if smaller_site_unit ==0
			replace representative_name = "Nationally and subnationally representative" if smaller_site_unit ==1 // ... most of these should be, because they are population-based 

		gen urbanicity_type = "Mixed/both"
		gen recall_type = "Point"
		gen recall_type_value =.
		gen sampling_type =""
		gen response_rate =""

		gen case_name = modelable_entity_name
		rename new_case_name case_definition 
		gen case_diagnostics = "Cases defined by ICD-10 codes according to EUROCAT protocol"
		
		gen note_modeler =.
		gen extractor = "{USERNAME}"

		rename cases_top prenatal_top
		rename cases_fd prenatal_fd
		gen prenatal_fd_definition = "20 weeks of age for most member registries; see EUROCAT member list online for stillbirth definitions for each member registry" 
			

		gen cv_low_income_hosp =0
		gen cv_1facility_only =0
		gen cv_aftersurgery =0
		gen cv_inpatient =1
		gen cv_prenatal =1
			rename cv_includes_stillbirths cv_livestill
			rename cv_includes_top cv_topnotrecorded
		rename cv_subset cv_under_report


		gen design_effect =.
		gen input_type =.
		gen outlier_type_id =0
		gen seq =.

		// Change "site_memo" var to include registry information 
		egen new_site_memo = concat(registry_name site_memo), punct(" registry; ")
			drop registry_name site_memo 
			rename new_site_memo site_memo


//-------------------------------------------------------------
order bundle_id modelable_entity_name seq underlying_nid nid field_citation_value file_path page_num table_num source_type location_name location_id ihme_loc_id smaller_site_unit site_memo ///
		sex sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer measure mean lower upper standard_error effective_sample_size cases sample_size unit_type unit_value_as_published ///
		measure_issue measure_adjustment uncertainty_type_value uncertainty_type representative_name urbanicity_type recall_type recall_type_value sampling_type design_effect input_type response_rate case_name case_definition case_diagnostics group specificity group_review note_modeler note_SR extractor ///
			cv* prenatal_top prenatal_fd prenatal_fd_definition outlier_type_id

// drop extra vars
	drop country cases_total case_name_count is_2015 is_2016 is_birth_prev


// format "cause" var
	replace cause = "cong_" + cause
		replace cause = trim(cause)
		replace cause = cause + "s" if cause=="cong_down"
		replace cause = cause + "ive" if cause=="cong_digest"
		replace cause = "cong_chromo" if cause =="cong_edpatau"

local date {DATE}
// save the entire file 
		export excel using "{FILEPATH}\EUROCAT_`date'.xlsx", firstrow(variables) sheet(extraction) replace 


// split into bundles and save files for upload 
	 levelsof bundle_id, local(bundles)
		foreach b in `bundles' {

			preserve
			keep if bundle_id==`b'
			local c = cause

				local file  "{FILEPATH}/EUROCAT_`c'_`b'_prepped_`date'"
				export excel using "`file'.xls", firstrow(variables) sheet(extraction) replace 

				levelsof modelable_entity_name, local(me) clean
				di in red "Exported `me' data for upload"
			restore
		}

