// Setup
	clear all
	set maxvar 32000
// Set to run all selected code without pausing
	set more off
// Remove previous restores
	cap restore, not
// Define J drive (data) for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		global j "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "J:"
	}
	
	//directions for the demographic getting
	local pullfreshnums 0
	local fao_prep 0
	local pop_dir "$j/FILEPATH/"

// Load in the GBD Functions
	adopath + "$j/FILEPATH"
	run "$j/FILEPATH/get_population.ado"
	
//pull in location_metadata
	get_location_metadata, location_set_id(22) clear	
	levelsof location_id, local(locations)

clear

**this is a local of all of the risk factors
local risk_factors "..."

tempfile data
save `data', replace emptyok

foreach risk of local risk_factors{
		di "risk is `risk'"
		
		//Import the global age pattern specific to each risk factor from DisMod
		import delim using "FILEPATH/`risk'_age_trend/pred_out.csv", clear
		
		rename pred_median estimate //to prevent the later reshape from screwing up
		append using `data'
		save `data', replace 
		
}

//format the data a bit
	//clean up data space
	rename estimate pred_mean
	gen pred_se = (pred_upper - pred_lower)/(2*1.96)

	keep age_upper age_lower pred_mean pred_se risk
	forvalues d=0/999 {
		gen pred_`d' = rnormal(pred_mean, pred_se)
	}
		drop pred_mean pred_se
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
	
	//get the numbers
	local population "`pop_dir'/pops_fao_as.dta"
		get_population, year_id(1980 1981 1982 1983 1984 1985 1986 1987 1988 1989 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017) ///
		location_id(`locations') sex_id(3) age_group_id(2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30) clear

		keep if inrange(age_group_id,2,33)
		keep if sex_id == 3
		rename population mean_pop

		**create age_group_years_start age_group_years_end
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
			drop pop_total

		sort year_id location_id age_group_id

		save "`population'", replace
	}
	else{
		use `population', clear
	}

	
//bring in fao estimates
if `fao_prep' == 1 {
	use `fao_data', clear
	**append on the hhbs
	append using "$/hhbs_for_split.dta", gen(hhbs_data)

	gen standard_error = (variance)^(1/2)

	forvalues d = 0/999 {
		gen fao_draw_`d' = rnormal(mean_value, standard_error)
	}

	rename mean_value exp_mean
	keep location_id year exp_mean standard_error risk sales_data hhbs_data nid fao_draw*
	keep if year>=1980

	duplicates drop
	
	//tempfile the data. 
	save "tosplit.dta", replace
}

**clean out the directory that results will be saved in
local workdir "output"
cd `workdir'

local datafiles: dir "`workdir'" files "*.dta"




**this list includes all risk factors
local risk_factors "..."

//age split the risk factors
	local a = 0

	foreach risk of local risk_factors {

use "FILEPATH/age_sex_splitting/fao/tosplit.dta" if risk == "`risk'", clear	
		rename year year_id
		count

		//merge in the predictions
		joinby risk using `data'

		**create a sex_id variable
		gen sex_id = 3

		duplicates drop location_id nid year_id sex_id age_group_id, force
		
		merge m:m location_id year_id sex_id age_group_id using `population', keep(3) nogen keepusing(mean_pop age_group_years*) 
		
		//generate total pop
		bysort location_id year_id : egen total_pop = total(mean_pop)

		//generate total consumption
		forvalues d = 0/999 {
			di in green "Currently on draw `d' for `risk'"
			replace pred_`d' = 0 if pred_`d' < 0
			gen group_consumption_`d' = pred_`d' * mean_pop 
			egen total_consumption_`d' = total(group_consumption_`d'), by(location_id year_id)
				drop group_consumption_`d'
			gen new_exp_mean_`d' = pred_`d' / (total_consumption_`d' / total_pop) * fao_draw_`d'
				drop total_consumption_`d' fao_draw_`d' pred_`d'
		}
		drop total_pop mean_pop
		preserve
		egen new_mean = rowmean(new_exp_mean*)
		egen new_se = rowsd(new_exp_mean*)
			drop new_exp_mean*

	save "/as_split_`risk'.dta", replace
}
//Append all the outputs together, get them ready for the diet_compilier
	clear
	foreach risk of local risk_factors{
		append using "/as_split_`risk'.dta"
	}
	//save an intermediate full dataset
	save "/as_split_all_v`version'.dta", replace
	
	cap rename new_mean new_exp_mean

	//clean up the dataset
	keep year_id new_se sex age_group* new_exp_mean location_id risk sales_data hhbs_data nid
	gen year_start= year_id
	gen year_end = year_id
	rename year_id year
	
	gen sex_id = 3
	drop sex
	rename sex_id sex
	
	gen age_start = age_group_years_start
	gen age_end = age_group_years_end

	drop age_group_id
	
	//save results
	save "`output'/as_2019.dta", replace
