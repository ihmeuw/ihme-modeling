// Clear memory and set memory and variable limits
	clear all
	macro drop _all
	set maxvar 32000
// Set to run all selected code without pausing
	set more off
// Remove previous restores
	cap restore, not
// Define J drive (data) for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		global j "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "FILEPATH"
	}
	do "FILEPATH"
	
// diet information
	local database_dir "FILEPATH"
	local version "FILEPATH" //this is the version of the compiled dataset from the diet_compiler
	local diet_data "FILEPATH"
	local output "FILEPATH" //adjust the versions here

//dynamically set the codefolder between computer, remote desktop and cluster
	if c(os) == "Unix" {
		local codefolder "FILEPATH"
	}
	else if c(os) == "Windows" {
		local codefolder "FILEPATH"
	}

	
//get location metadata
	run "FILEPATH"
	get_location_metadata, location_set_id(22) clear	
	keep if end_date=="." & location_set_id==22 //most recent locations for EPI/Risk set for countries we estimate for that are most detailed subnational
	keep if is_estimate==1 | location_id == 4749
	**keep the maximumed version
	qui sum location_set_version_id
	**keep if location_set_version_id==`r(max)'
	//keep the maximumed version
	qui sum location_set_version_id
	keep if location_set_version_id==`r(max)'
	gen iso3 = ihme_loc_id
	tempfile full_iso
	save `full_iso', replace
	
 	// import data
	use `diet_data', clear
		
	// generate local with relevant risk factor names - those with FAO data and PHVO data for TFA
	local risk_factors diet_energy // diet_grains diet_transfat diet_redmeat diet_milk diet_procmeat diet_nuts diet_fiber diet_calcium_low diet_pufa diet_satfat diet_omega_3 diet_zinc diet_veg diet_legumes diet_fruit diet_ssb //diet_fish

	**di `risk_factors'
	cap drop developed level //no idea what is currently in the diet database
	drop parent_id

	merge m:1 location_id using `full_iso', keep(3) nogen assert(2 3) keepusing(level iso3 parent_id)		
			drop parent_id
	replace location_id = 6 if regexm(iso3, "CHN")
	replace location_id = 130 if regexm(iso3, "MEX")
	replace location_id = 152 if regexm(iso3, "SAU")
	replace location_id = 93 if regexm(iso3, "SWE")
	replace location_id = 102 if regexm(iso3, "USA")
	replace location_id = 67 if regexm(iso3, "JPN")
	replace location_id = 180 if regexm(iso3, "KEN")
	replace location_id = 163 if regexm(iso3, "IND")
	replace location_id = 95 if regexm(iso3, "GBR")
	replace location_id = 135 if regexm(iso3, "BRA")
	replace location_id = 196 if regexm(iso3, "ZAF")
	replace location_id = 11 if regexm(iso3, "IDN")
		drop iso3
	merge m:1 location_id using `full_iso', keep(3) nogen assert(2 3) keepusing(iso3)	
	
	tempfile data
	save `data', replace
		
foreach risk_factor in `risk_factors' {
		di in red "`risk_factor'"
		use `data', clear
		
		keep if ihme_risk == "`risk_factor'"
		// drop fao data
		drop if svy=="FAO" | svy == "PHVO"
		// drop other excluded data
		drop if data_status == "Exclude"
		// drop non-optimal data sources
		drop if cv_FFQ == 1
		drop if cv_cv_hhbs == 1
		drop if is_outlier == 1
		
		summ standard_error, detail
		replace standard_error = `r(p95)' if standard_error == .

		save "FILEPATH", replace
		
		
		local project 			"FILEPATH" 
		local datafile 			"FILEPATH" 
		local sample_interval	10
		local num_sample		2000
		global proportion 1
		local midmesh 0 1 3 10 20 40 60

		local prjfolder		"FILEPATH"
		global prjfolder 	"FILEPATH"
		global datafile		`datafile'
		
		
		cap mkdir "FILEPATH"
			
		use "FILEPATH", clear
		
		// drop region as it will be used for a different value in Dismod ODE
			cap drop region	
			if $proportion == 1 replace parameter_type = "incidence"
		// each survey for diet has not yet been assigned a unique nid. use survey_id in place of nid since survey_id was generated as a unique identifier in the process of cleaning diet data
			rename nid nid_old
			encode svy, gen(nid)

		gen meas_value = mean
		gen meas_stdev = standard_error
		gen x_sex = 0 if sex == "Both"
		replace x_sex = .5 if sex == "Male"
		replace x_sex = -.5 if sex == "Female"
		gen age_lower = age_start
		gen age_upper = age_end
		gen time_lower = year_start
		gen time_upper = year_end
		gen super = "none"
		gen region = "none"
		gen subreg = iso3

		tostring meas_stdev, replace force
		cap gen integrand = parameter_type

				cap gen x_sex = 0
				cap gen x_ones = 1
				local o = _N
				local age_s 0
				di `o'
		qui forval i = 1/3 {
				local o = `o' + 1
				set obs `o'
				replace integrand = "mtall" in `o'
				replace super = "none" in `o'
				replace region = "none" in `o'
				replace subreg = "none" in `o'
				replace time_lower = 2000 in `o'
				replace time_upper = 2000 in `o'
				replace age_lower = `age_s' in `o'
				local age_s = `age_s' + 20
				replace age_upper = `age_s' in `o'
				replace x_sex = 0 in `o'
				replace x_ones = 0 in `o'
				replace meas_value = .01 in `o'
				replace meas_stdev = "inf" in `o'
			}	
				qui sum age_upper
				local maxage = r(max)
				if `maxage' > 90 local maxage 100
				replace age_upper = `maxage' in `o'

				cap keep citation nid iso3 super region subreg integrand time_* age_* meas_* x_* 
		outsheet using "FILEPATH", comma replace



			local studycovs
			foreach var of varlist x_* {
							local studycovs "`studycovs' `var'"
			}
			qui sum age_upper
			global mesh `midmesh' `maxage'
			global sample_interval `sample_interval'
			global num_sample `num_sample'
			global studycovs `studycovs'
			qui		do "FILEPATH"
			qui		do "FILEPATH"
			qui		do "FILEPATH"
			qui		do "FILEPATH"
			
			
		insheet using "FILEPATH", comma clear case
			if $proportion == 1 keep if integrand == "incidence"
			foreach var of local studycovs {
					cap gen `var' = 0
			}
		
		qui {
			gen age = age_lower
			keep if mod(age_lower,5) == 0  | age_lower <4
			drop if age_lower>80
			replace age_upper = 0.01 if age == 0
			replace age_lower = 0.01 if age == 1
			replace age_upper = 0.1 if age == 1
			replace age_lower = 0.1 if age == 2
			replace age_upper = 1 if age == 2
			replace age_lower = 1 if age == 3
			replace age_upper = 5 if age == 3
			replace age_upper = age_upper + 4 if age_upper > 5 | age_lower == 5
			replace age_upper = 4 if age_lower == 1
			replace age_upper = 100 if age_lower == 80
			replace age = age_lower

		}
		outsheet using "FILEPATH", comma replace
			cd "FILEPATH"
			
			! /usr/local/dismod_ode/bin/sample_post.py
			
		insheet using "FILEPATH", comma case clear
			drop if _n < `num_sample' - 1000 + 1
			replace index = _n - 1
		outsheet using "FILEPATH", comma replace

		insheet  name value using "FILEPATH", comma case clear
			drop if _n == 1
			replace value = "1000" if name == "num_sample"
		outsheet using "`FILEPATH", comma replace
		di "1"
			! FILEPATH scale_beta=false
		di "2"
			! FILEPATH data_in.csv value_tmp.csv plain_tmp.csv rate_tmp.csv effect_tmp.csv sample_out.csv data_pred.csv
		di "3"	
			! FILEPATH value_tmp.csv plain_tmp.csv rate_tmp.csv effect_tmp.csv sample_out.csv pred_out.csv
			sleep 2000
		di "4"	
			! FILEPATH 10
		di "5"
			! FILEPATH value_tmp.csv plain_tmp.csv rate_tmp.csv effect_tmp.csv sample_out.csv model_out.csv
			sleep 2000
		di "6"
			! FILEPATH  "`project'"
		di "7"	
	**}
}
		
***************************************
***************************************

// Compile FAO age trend results and apply them
	clear all
	set maxvar 32000
// Set to run all selected code without pausing
	set more off
// Remove previous restores
	cap restore, not
// Define J drive (data) for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		global j "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "FILEPATH"
	}

// call in args
args risk

// log the run
log using "FILEPATH", replace
	
// diet information
	local output "FILEPATH"
	
	//directions for the demographic getting
	local pop_dir "FILEPATH"
	local population "FILEPATH"
	local data "FILEPATH"

//get some functions going
	adopath + "FILEPATH"

use "FILEPATH" if risk == "`risk'", clear
	
		rename year year_id
		count

		**manually adjust risk to assist in mergeing
		replace risk = "diet_fruit" 	if risk == "diet_fruit_sales"
		replace risk = "diet_legumes" 	if risk == "diet_legumes_sales"
		replace risk = "diet_nuts" 		if risk == "diet_nuts_sales"
		replace risk = "diet_procmeat" 	if risk == "diet_procmeat_sales"
		replace risk = "diet_veg"		if risk == "diet_veg_sales"
		replace risk = "diet_ssb"		if risk == "diet_ssb_sales"
		replace risk = "diet_transfat"	if risk == "diet_hvo_sales"
		replace risk = "diet_redmeat"	if risk == "diet_redmeat_sales"
		replace risk = "diet_milk"		if risk == "diet_milk_sales"

		//merge in the predictions
		joinby risk using `data'

		**create a sex_id variable
		gen sex_id = 3

		duplicates drop location_id nid year_id sex_id age_group_id, force
		
		merge m:m location_id year_id sex_id age_group_id using `population', keep(3) nogen keepusing(mean_pop age_group_years*) //we're essentially adding in age and sex by population

		//generate total pop
		bysort location_id year_id : egen total_pop = total(mean_pop)

		//generate total consumption
		forvalues d = 0/999 {
			di in green "Currently on draw `d' for `risk'"
			gen group_consumption_`d' = pred_`d' * mean_pop 
			egen total_consumption_`d' = total(group_consumption_`d'), by(location_id year_id)
				drop group_consumption_`d'
			gen new_exp_mean_`d' = (pred_`d' / (total_consumption_`d' / total_pop)) * fao_draw_`d'
				drop total_consumption_`d' fao_draw_`d' pred_`d'
		}
		drop total_pop mean_pop
		preserve
		egen new_mean = rowmean(new_exp_mean*)
		egen new_se = rowsd(new_exp_mean*)
			drop new_exp_mean*

		gen sex = "Both"

		**manually adjust risk names back
		replace risk = "diet_fruit_sales"		if risk == "diet_fruit"		& sales_data == 1
		replace risk = "diet_legumes_sales" 	if risk == "diet_legumes"	& sales_data == 1
		replace risk = "diet_nuts_sales"		if risk == "diet_nuts" 		& sales_data == 1
		replace risk = "diet_procmeat_sales" 	if risk == "diet_procmeat"	& sales_data == 1
		replace risk = "diet_veg_sales"			if risk == "diet_veg"		& sales_data == 1
		replace risk = "diet_ssb_sales"			if risk == "diet_ssb"		& sales_data == 1
		replace risk = "diet_hvo_sales"			if risk == "diet_transfat"	& sales_data == 1
		replace risk = "diet_redmeat_sales"		if risk == "diet_redmeat"	& sales_data == 1
		replace risk = "diet_milk_sales"		if risk == "diet_milk"		& sales_data == 1
		
		save "FILEPATH", replace


***************************************
***************************************

// Compile FAO age trend results and apply them
	clear all
	macro drop _all
	set maxvar 32000
// Set to run all selected code without pausing
	set more off
// Remove previous restores
	cap restore, not
// Define J drive (data) for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		global j "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "FILEPATH"
	}
	do "FILEPATH"
	
// diet information
	local version "FILEPATH"
	local fao_data "FILEPATH" //this is the compiled, modeled FAO and Sales data
	local output "FILEPATH"
	local inter "FILEPATH"
	local bradmod_dir "FILEPATH"
	
	//directions for the demographic getting
	local pullfreshnums 0
	local pop_dir "FILEPATH"

//get some functions going
	adopath + "FILEPATH"
	run "FILEPATH"
//pull in location_metadata
	get_location_metadata, location_set_id(22) clear	
	keep if end_date=="." & location_set_id==22
	levelsof location_id, local(locations)

clear

** unadjusted
local risk_factors diet_energy diet_grains diet_redmeat diet_fruit diet_milk diet_zinc diet_satfat diet_pufa diet_legumes diet_nuts diet_veg diet_fiber diet_calcium_low diet_omega_3 diet_procmeat diet_ssb diet_transfat // diet_fish

	
//for each risk, bring in the predictions, make a female prediction etc.
//as of 12/18 we are using global age sex estimates.
tempfile data
save `data', replace emptyok
foreach risk of local risk_factors{
		di "risk is `risk' | Develop status= `ddd'"
		
		//note: d3 signifies global trend. d0 is developing and d1 is developed
		
		//get the beta on sex
		import delim using "FILEPATH", clear

		keep if name == "beta_incidence_x_sex"
		local beta_sex= mean[1]
		local lower_sex	=lower_95[1]
		local upper_sex =upper_95[1]
		
		di "Details: `lower_sex' `beta_sex' `upper_sex'"

		
		//bring in the pred_out
		import delim using "FILEPATH", clear
		
		//generate sex specific predictions
		//according to Mehrdad, this is the formula we should be using:
		//Male: pred_med*exp(.5*beta_sex) | Female: pred_med*exp(-.5*beta_sex)
		gen beta_sex = `beta_sex' //just for back up
		gen predFemale = pred_median * exp(.5*`beta_sex')
		gen predMale = pred_median * exp(-.5*`beta_sex')
		gen predBoth = pred_median
		gen risk = "`risk'"
		
		rename pred_median estimate //to prevent the later reshape from screwing up
		append using `data'
		save `data', replace 
		
}

//format the data a bit
	//clean up data space
	rename estimate pred_mean
	gen pred_se = (pred_upper - pred_lower)/(2*1.96)

	keep age_upper age_lower pred_mean pred_se risk //we're dropping upper and lowers until such time we can figure out how to use them
	forvalues d=0/999 {
		gen pred_`d' = rnormal(pred_mean, pred_se)
	}
		drop pred_mean pred_se
	//get the ages in age_group_id
	//because I am super lazy, manually transform to age_group_ids
	gen age_group_id = .
	{
		tostring age_lower, replace force format(%8.0g)
		replace age_group_id =2  if age_lower=="0"
		replace age_group_id =3  if age_lower==".01"
		replace age_group_id =4  if age_lower==".1"
		replace age_group_id =5  if age_lower=="1"
		replace age_group_id =6  if age_lower=="5"
		replace age_group_id =7  if age_lower=="10"
		replace age_group_id =8  if age_lower=="15"
		replace age_group_id =9  if age_lower=="20"
		replace age_group_id =10 if age_lower=="25"
		replace age_group_id =11 if age_lower=="30"
		replace age_group_id =12 if age_lower=="35"
		replace age_group_id =13 if age_lower=="40"
		replace age_group_id =14 if age_lower=="45"
		replace age_group_id =15 if age_lower=="50"
		replace age_group_id =16 if age_lower=="55"
		replace age_group_id =17 if age_lower=="60"
		replace age_group_id =18 if age_lower=="65"
		replace age_group_id =19 if age_lower=="70"
		replace age_group_id =20 if age_lower=="75"
		replace age_group_id =30 if age_lower=="80"
		replace age_group_id =31 if age_lower=="85"
		replace age_group_id =32 if age_lower=="90"
	}
	
	//save the data
	save "FILEPATH", replace
	
	//get the numbers
	local population "FILEPATH"
	if `pullfreshnums'==1{
		get_population, year_id(1980 1981 1982 1983 1984 1985 1986 1987 1988 1989 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016) ///
		location_id(`locations') sex_id(3) age_group_id(2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30) clear
		keep if inrange(age_group_id,2,33)
		keep if sex_id == 3
		rename population mean_pop

		gen age_group_years_start = .
			replace age_group_years_start = 0 if age_group_id == 2
			replace age_group_years_start = 0.01917808 if age_group_id == 3
			replace age_group_years_start = 0.07671233 if age_group_id == 4
			replace age_group_years_start = 1 if age_group_id == 5
			replace age_group_years_start = 5 if age_group_id == 6
			replace age_group_years_start = 10 if age_group_id == 7
			replace age_group_years_start = 15 if age_group_id == 8
			replace age_group_years_start = 20 if age_group_id == 9
			replace age_group_years_start = 25 if age_group_id == 10
			replace age_group_years_start = 30 if age_group_id == 11
			replace age_group_years_start = 35 if age_group_id == 12
			replace age_group_years_start = 40 if age_group_id == 13
			replace age_group_years_start = 45 if age_group_id == 14
			replace age_group_years_start = 50 if age_group_id == 15
			replace age_group_years_start = 55 if age_group_id == 16
			replace age_group_years_start = 60 if age_group_id == 17
			replace age_group_years_start = 65 if age_group_id == 18
			replace age_group_years_start = 70 if age_group_id == 19
			replace age_group_years_start = 75 if age_group_id == 20
			replace age_group_years_start = 80 if age_group_id == 30
		gen age_group_years_end = age_group_years_start + 4
			replace age_group_years_end = 100 if age_group_id == 30
			replace age_group_years_end = 0.01917808 if age_group_id == 2
			replace age_group_years_end = 0.07671233 if age_group_id == 3
			replace age_group_years_end = 1 if age_group_id == 4

		sort location_id year_id age_group_years_start
		bysort location_id year_id age_group_years_start: egen pop_total = sum(mean_pop)
			replace mean_pop = pop_total
			drop pop_total process_version_map_id


		sort year_id location_id age_group_id

		save "`population'", replace
	}
	else{
		use `population', clear
	}

	
//bring in fao estimates
	//format fao data so that it can merge with the pop numbers
	use `fao_data', clear
	append using "FILEPATH", gen(hhbs_data)
	gen standard_error = (variance)^(1/2)

	forvalues d = 0/999 {
		gen fao_draw_`d' = rnormal(mean_value, standard_error)
	}

	rename mean_value exp_mean
	keep location_id year exp_mean standard_error risk sales_data hhbs_data nid fao_draw*
	keep if year>=1980
	
	//interactive coding suggests that there are five datapoints with no se. No idea how it happened, but drop them
	count if standard_error ==.
	if `r(N)' >5{
		di as error "Greater than expected droppage"
		asdf
	}
	drop if standard_error ==.

	replace risk = "diet_grains" if risk == "diet_grain"
	
	//make sure there is no missing data

	duplicates drop
	
	//tempfile the data. Technically, the following steps could all be done at once, but that seems more complicated (I trust merge more than joinby)
	save "FILEPATH", replace

**clean out the directory that results will be saved in
local workdir "FILEPATH"
cd `workdir'

local datafiles: dir "`workdir'" files "*.dta"

foreach datafile of local datafiles {
        rm `datafile'
}

**this list includes the sales data
local risk_factors diet_energy diet_procmeat diet_grains diet_redmeat diet_milk_sales diet_redmeat_sales diet_fruit diet_zinc diet_legumes diet_milk diet_nuts diet_veg diet_calcium_low diet_fiber diet_fruit_sales diet_legumes_sales diet_nuts_sales diet_procmeat_sales diet_veg_sales diet_pufa diet_satfat diet_omega_3 diet_ssb_sales diet_hvo_sales // diet_fish

//Append all the outputs together, get them ready for the diet_compilier
	clear
	foreach risk of local risk_factors{
		append using "FILEPATH"
	}
		
	//save an intermediate full dataset
	save "FILEPATH", replace
	
	cap rename new_mean new_exp_mean

	//clean up the dataset
	keep year_id new_se sex age_group* new_exp_mean location_id risk sales_data hhbs_data nid
	gen year_start= year_id
	gen year_end = year_id
	rename year_id year
	
	**only "Both" should remain
	gen sex_id = 3
	drop sex
	rename sex_id sex
	
	gen age_start = age_group_years_start
	gen age_end = age_group_years_end

	drop age_group_id
	
	//save results
	save "FILEPATH", replace