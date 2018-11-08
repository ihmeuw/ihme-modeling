// Author: 	 NAME
// Date: 	1/24/2014
// Purpose:	Run linear model (Step 1)

// /////////////////////////////////////////////////
// CONFIGURE ENVIRONMENT
// /////////////////////////////////////////////////
   	** Set code directory for Git
	local user "`c(username)'"

	quietly {
		if c(os) == "Unix" {
			global J "ADDRESS"
			local code_dir = "FILEPATH"
            local outdir = "FILEPATH"
			set more off
			set odbcmgr unixodbc
			cd "`outdir'"
        }
        else {
        	global J "ADDRESS"
        	local code_dir = "FILEPATH"
            local outdir = "FILEPATH"
        } 
	}
	adopath + "FILEPATH"
    adopath + "FILEPATH"
    clear all

    // Toggle whether to generate a new dataset based on DB run
    local gen_new = 1
	
	// Toggle whether to use new scaled China data results from Austin
	local use_scaled_chn = 1


// /////////////////////////////////////////////////
// GET COUNTRY LIST
// /////////////////////////////////////////////////
// Include only non-GEN countries (mostly CON) 
// Of CON, include only VR-only or VR plus
get_locations
keep location_id ihme_loc_id
tempfile locs
save `locs'

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

import delimited using "FILEPATH", clear varnames(nonames)
rename v1 ihme_loc_id
duplicates drop ihme_loc_id, force
merge 1:1 ihme_loc_id using `locs', keep(2) nogen // We only want to keep the countries that are NOT in the gen_countries list
keep ihme_loc_id
tempfile st_locs
save `st_locs'

// Identify VR-only or VR-plus countries, and only process those
use "FILEPATH", clear
keep if regexm(source_type,"VR") | regexm(ihme_loc_id,"CHN") // Keep China subnationals and process them even though we don't have any VR
keep ihme_loc_id
duplicates drop ihme_loc_id, force
merge 1:1 ihme_loc_id using `st_locs', keep(3) nogen // Drop locations that don't have any VR data in our system
save `st_locs', replace


// /////////////////////////////////////////////////
// GET DATA
// /////////////////////////////////////////////////
	if `gen_new' == 1 {
		/*
		if `use_scaled_chn' == 1 {
			// First, get China data that is scaled from the Infectious Disease to the VR source for all deaths 2010 to 2012, using GBD pops for study population
			insheet using "`outdir'/scaled_china_death_data.csv", comma clear
			drop v1 scalar
			// Generate aggregated China Mainland from subnationals
			preserve
			fastcollapse study_deaths sample_size pop deaths, type(sum) by(data_id acause cause_id cause_name cod_source_label description nid citation data_type year age_group_id age_name sex urbanicity_type) 
			gen location_id = 44533
			gen location_name = "China (without Hong Kong and Macao)"
			tempfile mainland
			save `mainland'
			restore
			append using `mainland'
			tempfile chn_data
			save `chn_data'
		}
		*/
		run "FILEPATH"
    	get_data, cause_ids(298) clear
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
            
            fastcollapse study_deaths sample_size pop deaths, type(sum) by(parent_loc_id parent_id acause cause_id cause_name cod_source_label description nid citation data_type year age_group_id age_name sex urbanicity_type) 
			replace data_type = "aggregated_subnat" 
            rename parent_loc_id ihme_loc_id
            rename parent_id location_id
            save `subnat', replace
            
        // Check if the national data already exist
            use `master', clear
            merge m:1 ihme_loc_id using `parent_locs', keep(3) nogen
            merge 1:1 ihme_loc_id year sex age_group_id nid using `subnat', keep(2) nogen // Keep only data that doesn't exist already
            tempfile new_nat
            save `new_nat' 
    
        use `master', clear
		
		if `use_scaled_chn == 1' {
			keep if regexm(ihme_loc_id,"CHN") & !inlist(ihme_loc_id,"CHN_354","CHN_361","CHN_44533")

			
			preserve
			import delimited "FILEPATH", clear
			// if there are issues with limited use, run the command below
			// import delimited "FILEPATH", clear

			gen infect_deaths = hiv_deaths + aids_deaths
			drop aids_deaths hiv_deaths
			tempfile infect
			save `infect'
			restore
			
			preserve
			keep if regexm(cod_source_label, "China_2004_2012")
			fastcollapse deaths, type(sum) by(year location_name)
			keep if year > 2012
			rename deaths vr_deaths
			merge 1:1 location_name year using `infect', keep(3) nogen
			gen scalar = infect_deaths/vr_deaths
			fastcollapse scalar, type(max) by(location_name)
			tempfile chn_scalars
			save `chn_scalars'
			restore
			
			** preserve
			** // Aggregate all trusted years together, and create scalars
			** keep if year > 2011 // we are most confident in these years
			** fastcollapse pop deaths env sample_size, type(sum) by(ihme_loc_id cod_source_label)
			** keep if cod_source_label == "China_Infectious" | cod_source_label == "China_2004_2012"
			** keep deaths ihme_loc_id cod_source_label
			** replace cod_source_label = "infect" if cod_source_label == "China_Infectious"
			** replace cod_source_label = "vr" if cod_source_label == "China_2004_2012"
			** rename deaths deaths_
			
			** reshape wide deaths_, i(ihme_loc_id) j(cod_source_label) string
			** gen scalar = deaths_infect/deaths_vr
			** keep scalar ihme_loc_id
			** tempfile chn_scalars
			** save `chn_scalars'
			** restore 
			
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
			drop if inlist(data_type,"Surveillance","Verbal Autopsy")
			
		// No HIV deaths before 1981
			drop if year < 1981
		
		
    	outsheet using "`outdir'/test_hiv_dataset_12022015.csv", comma replace
	}
    
    else insheet using "`outdir'/test_hiv_dataset_12022015.csv", comma clear
	// keep if age_group_id == 11 | age_group_id == 12 // Test to see if age weights are causing huge peaks/valleys in things
	
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
insheet using "FILEPATH/input_data.txt", clear
gen comp_count = 0
replace comp_count = 1 if vr == "1" & category == "complete" & year > 1980
bysort ihme_loc_id sex: egen tot_comp = total(comp_count)
gen complete = 0
replace complete = 1 if tot_comp >= 10
duplicates drop ihme_loc_id, force
keep ihme_loc_id complete
tempfile comp_indic
save `comp_indic'

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
		replace omega = 0 if regexm(ihme_loc_id,"USA") // DC results in 30-34 etc. are unduly affected by bell-shaped age patterns in neighboring ages, leading to tripling of estimates re: data. Turn Omega off.

	// Zeta: Affects space-weights in ST: sp_weights = {
	//										3: self.zeta,
	//  	               					2: self.zeta*(1-self.zeta),
	//	                   					1: (1-self.zeta)**2,
	//                    					0: 0}
	// 		 Where I believe that 3 is the lowest level, 2 is next highest, 1 is next highest, and 0 is the highest possible
	//			in the location hierarchy, so 3 should be on the same hierarchical level as the country of interest.
	//		 Larger zeta (up to 1) will lead to more weight being placed on data closest in the location hierarchy to the location
		// replace zeta = .99 if complete == 1
		replace zeta = .95
		replace zeta = .99 if regexm(ihme_loc_id,"USA") // US results seem to be getting pulled by other US results, consider it mostly complete

// GPR Parameters: See page 9 and 10 in http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.385.4366&rep=rep1&type=pdf
	// Scale: Stretches the x axis of the data in use. Higher scale -> bigger window of data used
		replace scale = 5 if complete == 1
		replace scale = 7 if complete == 0
		// replace scale = 15 // Try to see if this fixes huge upticks in ST-GPR in 2014/15 for many countries due to the 1984 anchoring points and log-space modeling
		
	// Amp: Stretches the y axis -- lower amp allows for less "wiggle" vertically
		// Ex: replace amp = "regional" if ihme_loc_id == "PRK"
		// replace amp = "national" if complete == 1 // National amp crashes CRI males among others -- why?

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

	// Convert to CF space
	// gen cf_HIV=dths_final/mean_env

	// If you want Death Rate space, uncomment the below... sorry about the confusing variable naming
	gen death_rate = (deaths/pop)*100
	summ death_rate if death_rate > 0
	replace death_rate = r(min) if death_rate == 0

	// Calculate sample size
	rename sample_size deaths_ss
	gen sample_size = (deaths_ss/env)*pop*1000
	summ sample_size if sample_size > 0
	replace sample_size = r(min) if sample_size == 0
	gen scalar_ss = deaths_ss / env
	sum scalar_ss 
	local mean_scale = `r(mean)'
	tempfile data
	save `data'

	// Make template
    expand_demographics, type(lowest) 
	merge 1:1 location_id year_id age_group_id sex_id using "FILEPATH/agg_env_summary.dta", keep(1 3) 
	count if _m == 1
	if `r(N)' > 0 BREAK
	drop _m
	
	drop if year_id < 1981
	
    tempfile master
    save `master'
   
   	merge 1:m location_id year_id age_group_id sex_id using `data', keep(1 3) nogen
	
	// CoD (via the get_data function) uses the current envelope (HIV-Deleted) to calculate the deaths column by cf * deaths
	// We want to instead use all-cause deaths applied to the HIV cause fraction, as that is actually consistent internally
	// So here, we recalculate total deaths and death rate based on envelope results
	gen cf_hiv = deaths / env
	replace deaths = cf_hiv * enve_mean
	replace death_rate = (deaths/pop)*100
	summ death_rate if death_rate > 0
	replace death_rate = r(min) if death_rate == 0
	
	drop enve_* cf_hiv

	/*
	// Extend to 1970 to help with the incidence adjustment
	// Add anchoring point at 1970 to help guide points to near-0
	// Update: Add anchoring points in 1970-1983 because Chris is adamant that no HIV deaths occurred in any non-GEN countries pre-1984
	// This will make results jump quite suddenly, but alas
    // We actually will keep no data in ENN/LNN, and then constrain the GPR results to 0 after GPR results are produced
	replace death_rate = .0000001 if year_id >= 1970 & year <= 1983 & !inlist(age_group_id,2,3)
	replace sample_size = pop * 1000 * `mean_scale' if year_id >= 1970 & year <= 1983 & !inlist(age_group_id,2,3)
	replace deaths = (death_rate/100) * pop if year_id >= 1970 & year <= 1983 & !inlist(age_group_id,2,3)
	replace data_type = "fake_anchor" if year_id >= 1970 & year <= 1983 & !inlist(age_group_id,2,3)
	replace site = "fake_anchor" if year_id >= 1970 & year <= 1983 & !inlist(age_group_id,2,3)
    */
	
    // Delete the anchoring point if that is the only data point available for the location -- don't want to pull all estimates to 0 across the time series 
    // Instead remove all data so it'll move to the regional average
    bysort location_id age_group_id sex_id: egen data_count = count(death_rate)
    foreach var in deaths death_rate sample_size {
        replace `var' = . if data_count == 1 & !inlist(age_group_id,2,3)
    }
    foreach var in data_type site {
        replace `var' = "" if data_count == 1 & !inlist(age_group_id,2,3)
    }
    drop data_count

	// Prep for linear regression
	// gen lt_cf = logit(cf_HIV)
	gen ln_dr = ln(death_rate) // No need to transform zeroes because all death rates are non-zero (many are close, but none have values for DR but not for ln_dr)
	bysort location_id age_group_id sex_id: egen peak_dr = max(ln_dr)
	gen possible_peakyear = year if (ln_dr == peak_dr) & (ln_dr != .)
	bysort location_id age_group_id sex_id: egen peakyear = max(possible_peakyear)
	bysort region_id: egen region_peakyear = mean(peakyear)
	bysort super_region_id: egen sr_peakyear = mean(peakyear)
	egen global_peakyear = mean(peakyear)

	local peaktype "uninformed"
	if "`peaktype'" == "informed"{
		replace peakyear = region_peakyear if peakyear == .
		replace peakyear = sr_peakyear if peakyear == .
		replace peakyear = global_peakyear if peakyear == .
	}
	else if "`peaktype'" == "uninformed"{
		replace peakyear = 2015 if peakyear == .
	}

	replace peakyear = round(peakyear)

	// Manually set location/age-specific knots based on prior knowledge
    replace peakyear = 2004 if location_id == 22

	gen year_relto_peak = year-peakyear

	mkspline spl1 0 spl2 = year_relto_peak
	// mkspline spl1 1995 spl2 = year

	// Run linear regression
	rreg ln_dr spl1 spl2 i.region_id i.age_group_id i.sex_id
	predict ln_dr_predicted

	// Flag subnationals
	gen sn_flag = 0
	replace sn_flag = 1 if (national_type_id == 2) | (national_type_id == 0)

	sort location_id age_group_id year_id
	keep location_id year_id age_group_id sex_id national_type_id data_type site sample_size sn_flag death_rate deaths env pop ln_dr ln_dr_predicted
	outsheet using "linear_predictions.csv", comma replace

// END OF DO FILE
