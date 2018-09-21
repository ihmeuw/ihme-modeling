** ************************************************************************************************* **
** Purpose: Export a standardized dataset						
** ************************************************************************************************* **
// Set arguments
	args data_name

// Set the prefix for whichever os we're in
	if c(os) == "Windows" {
		global j "J:"
	}
	if c(os) == "Unix" {
		global j "/home/j"
		set odbcmgr unixodbc
	}

// Set the date and time
	local date = c(current_date)
	local today = date("`date'", "DMY")
	local year = year(`today')
	local month = month(`today')
	local day = day(`today')
	local time = c(current_time)
	local time : subinstr local time ":" "", all
	local length : length local month
	if `length' == 1 local month = "0`month'"	
	local length : length local day
	if `length' == 1 local day = "0`day'"
	global date = "`year'_`month'_`day'"
	global timestamp = "${date}_`time'"
	
** ************************************************************************************************* **
// ESTABLISH DIRECTORIES
	// Data source name
		if "$data_name" == "" global data_name `data_name'
	// Output directory
		global out_dir "$j/WORK/03_cod/01_database/03_datasets/${data_name}/data/intermediate"
	// Map directory
		global list_dir "$j/WORK/03_cod/01_database/03_datasets/${data_name}/maps/cause_list/"

** ************************************************************************************************* ** 
// DO A QUICK VARIABLES CHECK
	// All of the following variables should be present
		#delimit ;
		order		
		iso3 subdiv location_id national 
		source source_label source_type NID list 
		frmat im_frmat 
		sex year cause cause_name
		deaths1 deaths2 deaths3 deaths4 deaths5 deaths6 deaths7 deaths8 deaths9 deaths10 deaths11 deaths12 deaths13 deaths14 deaths15 
		deaths16 deaths17 deaths18 deaths19 deaths20 deaths21 deaths22 deaths23 deaths24 deaths25 deaths26 deaths91 deaths92 deaths93 deaths94 ;

	// Drop any variables not in our template of variables to keep
		keep		
		iso3 subdiv location_id national 
		source source_label source_type NID list 
		frmat im_frmat 
		sex year cause cause_name
		deaths1 deaths2 deaths3 deaths4 deaths5 deaths6 deaths7 deaths8 deaths9 deaths10 deaths11 deaths12 deaths13 deaths14 deaths15 
		deaths16 deaths17 deaths18 deaths19 deaths20 deaths21 deaths22 deaths23 deaths24 deaths25 deaths26 deaths91 deaths92 deaths93 deaths94 ;
		#delimit cr


** ************************************************************************************************* **
// SAVE AS FORMATTED AND COMPILED DATA
	// Ensure that deaths can be uniquely identified with a final collapse
	duplicates tag iso3 subdiv location_id national source source_label source_type NID list frmat im_frmat sex year cause cause_name, gen(dup_obs)
	count if dup_obs != 0
	if `r(N)'>0{
		collapse(sum) deaths*, by(iso3 subdiv location_id national source source_label source_type NID list frmat im_frmat sex year cause cause_name) fast
	}
	capture drop dup_obs
	compress
	save "$out_dir/00_formatted.dta", replace
	save "$out_dir/_archive/00_formatted_${timestamp}.dta", replace
	capture saveold "$out_dir/00_formatted.dta", replace
	capture saveold "$out_dir/_archive/00_formatted_${timestamp}.dta", replace
	


