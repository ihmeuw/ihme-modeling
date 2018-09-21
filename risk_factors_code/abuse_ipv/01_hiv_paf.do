// *********************************************************************************************************************************************************************
// Purpose:		launch script do file - parallelize calculation of population attributable fraction of HIV prevalence due to intimate partner violence, by groups of 10 draws
** *********************************************************************************************************************************************************************
** RUNTIME CONFIGURATION
** **************************************************************************
// Set preferences for STATA
	// Clear memory and set memory and variable limits
		clear all
		set mem 12g
		set maxvar 32000
	// Set to run all selected code without pausing
		set more off
	// Set graph output color scheme
		set scheme s1color
	// Remove previous restores
		cap restore, not
	// Reset timer (?)
		timer clear	
\
	// Close previous logs
		cap log close
// Set seed for random draws
	set seed 963066483
	
// load shared functions
	adopath + "FILEPATH"
	
// Create macros for settings, files and filepaths
	local ver 4 // increase by 1 for every run
	local healthstate "abuse_ipv"
	//local risk "abuse_ipv_hiv" 
	local acause = "hiv"
	local integrand "incidence"
	local sex "female"
	local years "1990 1995 2000 2005 2010 2016"
	
	local main_dir "FILEPATH"
	local rr_dir "FILEPATH"
	local code_dir "FILEPATH"
	local data_dir "FILEPATH"
	local out_dir "FILEPATH"

// Make a directory for storing intermediate PAF files & for versioning
	cap mkdir "`out_dir'"

// Get ISO3 with subnational location ids
	do "FILEPATH/get_location_metadata.ado"
	get_location_metadata, location_set_id(22) clear
	keep if is_estimate == 1 & most_detailed == 1 

	keep ihme_loc_id location_id location_ascii_name super_region_name super_region_id region_name region_id 
	
	rename ihme_loc_id iso3 
	tempfile country_codes
	save `country_codes', replace

	levelsof location_id, local(locations)
		
// Prepare relative risk for HIV due to IPV [note that relative risks are actually incidence rate ratios (i.e. for HIV incidence, not prevalence)]
	// Meta-analysis 	
		ssc install metan 

		import excel using "`rr_dir'/FILEPATH/rr_component_studies.xlsx", firstrow clear
		keep if RelativeRisk == "IPV-HIV"
		metan EffectSize Lower Upper, random
		local rr r(ES) // 1.59 (1.27, 1.91)
		local upper = r(ci_upp)
		local lower = r(ci_low) 
		
	// 1,000 draws from normal distribution
		local sd = ((ln(`upper')) - (ln(`lower'))) / (2*invnormal(.975))
		clear
		set obs 1
		forvalues d = 0/999 {
			gen rr_`d' = exp(rnormal(ln(`rr'), `sd'))
		}
		
	// Make identifier for merge with Dismod model
		gen x = 1
	
	// Save relative risk draws for PAF calculation
		save "`rr_dir'/prepped/`acause'_rr_draws.dta", replace 
	
// 3.) Parellize by 10 groups of 100 draws
	**  Make local to be filled with the list of each job that must finish before the compilation/formatting code can be launched
		local holdlist ""
	
	foreach iso3 of local locations {
	
			forvalues i = 99(100)999 {
				local draw_num = `i'
				!qsub -N "ipv_hiv`iso3'_`i'" -P proj_custom_models -l mem_free=20G -pe multi_slot 4 ///
				"FILEPATH.sh" ///
				"`code_dir'/02_hiv_paf.do" ///
				"`draw_num' `iso3'"
			}
	}	
