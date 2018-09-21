// Purpose:	Run linear model (Step 1)

// /////////////////////////////////////////////////
// CONFIGURE ENVIRONMENT
// /////////////////////////////////////////////////
   	** Set code directory for Git
	local user "`c(username)'"

	quietly {
		if c(os) == "Unix" {
			global J FILEPATH
			local code_dir = FILEPATH
            local outdir = FILEPATH
			set more off
			set odbcmgr unixodbc
			cd "`outdir'"
        }
        else {
        	global J FILEPATH
        	local code_dir = FILEPATH
            local outdir = FILEPATH
        } 
	}
	adopath + FILEPATH
    adopath + FILEPATH 
    adopath + FILEPATH
    clear all

    // Toggle whether to generate a new dataset based on DB run
    local gen_new = 1
	
	// Toggle whether to use scaled China data results 
	local use_scaled_chn = 1 
	
	// toggle to use droped Thai data
	local use_droped_thai = 1


// /////////////////////////////////////////////////
// GET COUNTRY LIST
// /////////////////////////////////////////////////
// Include only non-GEN countries (mostly CON) 
// Of CON, include only VR-only or VR plus
get_locations
keep location_id ihme_loc_id
tempfile locs
save `locs' 
keep if regexm(ihme, "IDN")   
keep ihme
tempfile ilocs 
save `ilocs', replace

// Get all subnational locations, along with the total number of subnationals expected in each
get_locations, level(subnational)
drop if level == 4 & (regexm(ihme_loc_id,"CHN") | regexm(ihme_loc_id,"IND"))
drop if ihme_loc_id == "GBR_4749" // Screws everything up
split ihme_loc_id, parse("_")
rename ihme_loc_id1 parent_loc_id
bysort parent_loc_id: gen num_locs = _N
replace parent_loc_id = "CHN_44533" if parent_loc_id == "CHN" // Mainland has all the data
keep ihme_loc_id parent_loc_id parent_id num_locs 
tempfile subnat_locs
save `subnat_locs'
duplicates drop parent_loc_id, force
keep parent_loc_id
rename parent_loc_id ihme_loc_id
tempfile parent_locs
save `parent_locs'

import delimited using FILEPATH, clear varnames(nonames)
rename v1 ihme_loc_id
duplicates drop ihme_loc_id, force
merge 1:1 ihme_loc_id using `locs', keep(2) nogen // We only want to keep the countries that are NOT in the gen_countries list
keep ihme_loc_id
tempfile st_locs
save `st_locs'

// Identify VR-only or VR-plus countries, and only process those
use FILEPATH, clear
keep if regexm(source_type,"VR") | regexm(ihme_loc_id,"CHN") // Keep China subnationals and process them even though we don't have any VR
keep ihme_loc_id
duplicates drop ihme_loc_id, force
merge 1:1 ihme_loc_id using `st_locs', keep(3) nogen // Drop locations that don't have any VR data in our system 
append using `ilocs'
save `st_locs', replace


// /////////////////////////////////////////////////
// GET DATA
// /////////////////////////////////////////////////
	if `gen_new' == 1 {
	
    	get_cod_data, cause_id("298") clear 
		if `use_droped_thai' == 1 { 
			preserve 
			insheet using FILEPATH, clear
			tostring site, replace 
			tostring urbanicity, replace
			replace citation = "." if missing(citation)  
			replace data_id = .  
			tempfile thai 
			save `thai', replace 
			restore
			merge 1:1 cod_source nid citation data_type location_id year age_group sex site representative using `thai' 
			outsheet using FILEPATH, comma replace 
			drop _m
		}		
		merge m:1 location_id using `locs', keep(3) nogen
    	merge m:1 ihme_loc_id using `st_locs', keep(3) nogen // Drop GEN countries from raw data, and higher-level locs from st_locs
        tempfile master
        save `master'
        
        // Aggregate from subnational units to national -- only if they have all possible locations etc.
            merge m:1 ihme_loc_id using `subnat_locs', keep(3) nogen
            bysort parent_loc_id nid year sex age_group_id: gen true_locs = _N
            keep if num_locs == true_locs // If the number of subnationals matches the total possible
            drop num_locs true_locs
            tempfile subnat
            save `subnat'
            
            fastcollapse study_deaths sample_size pop deaths, type(sum) by(parent_loc_id parent_id acause cause_id cause_name cod_source_label description nid citation data_type year age_group_id age_name sex urbanicity_type site) 
			replace data_type = "aggregated_subnat" 
            rename parent_loc_id ihme_loc_id
            rename parent_id location_id
            save `subnat', replace
            
        // Check if the national data already exist
            use `master', clear
            merge m:1 ihme_loc_id using `parent_locs', keep(3) nogen
            merge 1:1 ihme_loc_id year sex age_group_id nid site using `subnat', keep(2) nogen // Keep only data that doesn't exist already
            tempfile new_nat
            save `new_nat' 
    
        use `master', clear
		
		if `use_scaled_chn == 1' {
			keep if regexm(ihme_loc_id,"CHN") & !inlist(ihme_loc_id,"CHN_354","CHN_361","CHN_44533")

			
			preserve
			import delimited FILEPATH, clear

			gen infect_deaths = hiv_deaths + aids_deaths
			drop aids_deaths hiv_deaths
			tempfile infect
			save `infect'
			restore
			
			preserve
			keep if regexm(cod_source_label, "China_2004_2012") 
			keep if age_group_id == 22
			fastcollapse deaths, type(sum) by(year location_name)
			keep if year > 2012
			rename deaths vr_deaths
			merge 1:1 location_name year using `infect', keep(3) nogen
			gen scalar = infect_deaths/vr_deaths 
			replace scalar = 1 if scalar < 1
			fastcollapse scalar, type(max) by(location_name)
			tempfile chn_scalars
			save `chn_scalars'
			restore 
			
			keep if cod_source_label != "China_Infectious" 
			merge m:1 location_name using `chn_scalars', keep(3) nogen
			replace deaths = deaths * scalar
			replace sample_size = sample_size * scalar
			
			drop scalar
			foreach var in nid cf rate {
				replace `var' = .
			}
			foreach var in citation {
				replace `var' = ""
			}
			
			preserve
			fastcollapse study_deaths sample_size pop deaths env, type(sum) by(data_id acause cause_id cause_name cod_source_label description nid citation data_type year age_group_id age_name sex urbanicity_type) 
			gen location_id = 44533
			gen location_name = "China (without Hong Kong and Macao)"
			tempfile mainland
			save `mainland'
			restore
			
			append using `mainland'
			replace cod_source_label = "China DSP data scaled to China_Infectious using ratio between Infectious and China_2004_2012"
			tempfile chn_data
			save `chn_data'

			use `master', clear
			drop if regexm(ihme_loc_id,"CHN") & !inlist(ihme_loc_id,"CHN_354","CHN_361")
			append using `chn_data', force
		}
        append using `new_nat'
        
        // Drop Outliers
            drop if ihme_loc_id == "CHN_361" // Only one datapoint, pretty variable
			
		// Drop non-VR sources that we can't use
			gen exception = 0 
			replace exception = 1 if cod_source_label == "Indonesia_SRS_2014" 
			drop if inlist(data_type,"Surveillance","Verbal Autopsy") & exception == 0
			drop exception
			
		// No HIV deaths before 1981
			drop if year < 1981 
			
		// outliers 
			drop if ihme == "PHL" & (year < 1990 | year == 2004 | year == 2005) 
			drop if ihme == "MNG" & year == 1994 
			drop if ihme == "IRN" & age_group_id >= 19  
			drop if ihme == "ROU" & year < 1990 
			drop if ihme == "GUY" & year == 2000 
			drop if ihme == "BEL" & year == 1992 
			drop if ihme == "ITA" & (year == 2001 | year == 2002) 
			drop if ihme == "RUS" & year < 1985 
			drop if ihme == "SGP" & year>=2004 & year<=2011 
			drop if regexm(ihme,"JPN") & year>=1980 & year<=1985
			
			
			
		
    	outsheet using "`outdir'/test_hiv_dataset_12022015.csv", comma replace
	}
    
    else insheet using "`outdir'/test_hiv_dataset_12022015.csv", comma clear
	
	// Get list of countries with actual data here
		preserve
		duplicates drop ihme_loc_id, force
		keep ihme_loc_id
		tempfile cod_data_locs
		save `cod_data_locs'
		restore
		
    rename (year sex) (year_id sex_id)
	tempfile master
	save `master'


// /////////////////////////////////////////////////
// EDIT PARAMETER FILE
// /////////////////////////////////////////////////
// First, classify countries based on whether they have complete VR or not
insheet using FILEPATH, clear
keep ihme stars 
tempfile star 
save `star', replace 
use `locs', clear 
keep ihme 
merge m:1 ihme using `star' 
gen complete = 0 
replace complete = 1 if _m == 3  
keep ihme complete 
replace complete = 1 if regexm(ihme,"GBR")
tempfile comp_indic 
save `comp_indic', replace




import delimited using "`code_dir'/params.csv", clear 
// Drop GEN countries and countries without CoD VR data from params, and higher-level locs from st_locs
merge 1:1 ihme_loc_id using `st_locs', keep(3) nogen 
merge 1:1 ihme_loc_id using `comp_indic', keep(3) nogen
merge 1:1 ihme_loc_id using `cod_data_locs', keep(3) nogen

// Space-Time Parameters
	// Lambda: Time-weight effect in ST. t_weights_lookup[i][j] = (1-(a_dist/(max_a_dist+1))**self.lambdaa)**3
	//			Essentially, lower lambda expands the width of data used by ST to calculate a single-year estimate
		replace lambda = .1 if complete == 1
		replace lambda = .3 if complete == 0

	// Omega: Age weight in ST:	for i in range(len(self.age_map)+1):
	//	            				a_weights_lookup[i] = 1 / np.exp(self.omega*i)
	//		  A higher omega should narrow the bandwith of ages used to calculate a single-age estimate (since the distance from one point to another would increase much more steeply)
	//			However, omega = 0 should make it so no data is used except for the data in the age group itself
		replace omega = 2 
		*non smooth changes in age data, but we still trust the data 
		replace omega = 5 if inlist(ihme, "BMU", "URY", "MEX_4648", "PAN")
		
	// Zeta: Affects space-weights in ST: sp_weights = {
	//										3: self.zeta,
	//  	               					2: self.zeta*(1-self.zeta),
	//	                   					1: (1-self.zeta)**2,
	//                    					0: 0}
	// 		 Where I believe that 3 is the lowest level, 2 is next highest, 1 is next highest, and 0 is the highest possible
	//			in the location hierarchy, so 3 should be on the same hierarchical level as the country of interest.
	//		 Larger zeta (up to 1) will lead to more weight being placed on data closest in the location hierarchy to the location
	
		replace zeta = .95

// GPR Parameters: See page 9 and 10 in http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.385.4366&rep=rep1&type=pdf
	// Scale: Stretches the x axis of the data in use. Higher scale -> bigger window of data used
		replace scale = 5 if complete == 1
		replace scale = 7 if complete == 0
		
	// Amp: Stretches the y axis -- lower amp allows for less "wiggle" vertically
		// Ex: replace amp = "regional" if ihme_loc_id == "PRK"
		// STP is only location without a regional mad and only a global one so have to set to global
		replace amp = "global" if ihme == "STP"

export delimited using "`outdir'/params.csv", delimit(",") replace


// /////////////////////////////////////////////////
// RUN REGRESSIONS
// /////////////////////////////////////////////////
	use `master', clear
    gen national_type_id = 0
	replace national_type_id = 1 if representative == "Nationally representative only"
	replace national_type_id = 2 if representative == "Representative for subnational location only" 
	
	fastcollapse deaths env pop sample_size, type(sum) by(location_id year_id age_group_id sex_id national_type_id data_type site)
	drop if inlist(age_group_id, 2, 3) 
	
	// adding 2015 THA 
	preserve 
	insheet using FILEPATH, clear  
	tostring site, replace  
	tempfile old_thai 
	save `old_thai', replace 
	restore 
	merge m:1 location year age_group sex using `old_thai'
	drop if _m ==1 & location_id == 18 
	gen cf_2015 = deaths_2015/env_2015 
	replace deaths = cf_2015 * env if location_id == 18 
	drop cf_2015 deaths_2015 sample_size_2015 env_2015 
	drop _m		

	// If you want Death Rate space, uncomment the below
	gen death_rate = (deaths/pop)*100
	summ death_rate if death_rate > 0
	replace death_rate = r(min) if death_rate == 0

	// Calculate sample size
	rename sample_size deaths_ss
	gen sample_size = (deaths_ss/env)*pop
	summ sample_size if sample_size > 0
	replace sample_size = r(min) if sample_size == 0
	gen scalar_ss = deaths_ss / env
	sum scalar_ss 
	local mean_scale = `r(mean)'
	tempfile data
	save `data'

	// Make template
	get_locations, level(lowest) gbd_year(2016) 
	keep location_id ihme_loc_id location_name region_id region_name super_region_id super_region_name level 

	expand 2
	bysort location_id: gen sex_id = _n

	expand 47
	bysort location_id sex_id: gen year_id = _n + 1969

	expand 23
	bysort location_id sex_id year_id: gen age_group_id = _n + 1 if _n<=19 // Generate NN through 75-79 (age groups 2-20) 
	bysort location_id sex_id year_id: replace age_group_id = _n + 10 if _n>=20 & _n<=22 //80...90-94(30,31,32) 
	bysort location_id sex_id year_id: replace age_group_id = 235 if _n ==23 //95+
	

	order location_id ihme_loc_id year_id sex_id age_group_id
	
	drop if year_id < 1981
	
    tempfile master
    save `master'
   
   	merge 1:m location_id year_id age_group_id sex_id using `data', keep(1 3) nogen
	
    // Delete the anchoring point if that is the only data point available for the location -- don't want to pull all estimates to 0 across the time series 
    // Instead remove all data so it'll move to the regional average
    bysort location_id age_group_id sex_id: egen data_count = count(death_rate) 
	gen exception = 0 
	tab ihme
	replace exception = 1 if regexm(ihme, "IDN") 
	replace exception = 1 if inlist(age_group_id,2,3)
    foreach var in deaths death_rate sample_size {
        replace `var' = . if data_count == 1 & exception == 0 
    }
    foreach var in data_type site {
        replace `var' = "" if data_count == 1 & exception ==0
    }
	drop exception
    drop data_count

	// Prep for linear regression
	 
	gen ln_dr = ln(death_rate) // No need to transform zeroes because all death rates are non-zero (many are close, but none have values for DR but not for ln_dr)
	bysort location_id age_group_id sex_id: egen peak_dr = max(ln_dr)
	gen possible_peakyear = year if (ln_dr == peak_dr) & (ln_dr != .)
	bysort location_id age_group_id sex_id: egen peakyear = max(possible_peakyear) 
	bysort location_id sex_id: egen location_peakyear = mean(peakyear)
	bysort region_id sex_id: egen region_peakyear = mean(peakyear)
	bysort super_region_id sex_id: egen sr_peakyear = mean(peakyear)
	egen global_peakyear = mean(peakyear)

	local peaktype "informed"
	if "`peaktype'" == "informed"{ 
		replace peakyear = location_peakyear if peakyear == .
		replace peakyear = region_peakyear if peakyear == .
		replace peakyear = sr_peakyear if peakyear == .
		replace peakyear = global_peakyear if peakyear == .
	}
	else if "`peaktype'" == "uninformed"{
		replace peakyear = 2016 if peakyear == .
	}

	replace peakyear = round(peakyear)

	// Manually set location/age-specific knots based on prior knowledge
    replace peakyear = 2004 if location_id == 22 
	
	

	gen year_relto_peak = year-peakyear

	mkspline spl1 0 spl2 = year_relto_peak

	// Run linear regression 
	
	rreg ln_dr spl1 spl2 i.region_id i.age_group_id i.sex_id 
	
	
	
	predict ln_dr_predicted

	// Flag subnationals
	gen sn_flag = 0
	replace sn_flag = 1 if (national_type_id == 2) | (national_type_id == 0)

	sort location_id age_group_id year_id
	keep location_id year_id age_group_id sex_id national_type_id data_type site sample_size sn_flag death_rate deaths env pop ln_dr ln_dr_predicted 
	*to calculate age_weights THE IDS have to be consecutive integers 
	replace age_group_id = 21 if age_group_id ==30
	replace age_group_id = 22 if age_group_id == 31 
	replace age_group_id = 23 if age_group_id == 32 
	replace age_group_id = 24 if age_group_id == 235
	outsheet using "linear_predictions.csv", comma replace

// END OF DO FILE
