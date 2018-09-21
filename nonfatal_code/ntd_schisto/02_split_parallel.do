// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		USERNAME
// Last updated:	DATE
// Description:	Parallelization of 02_sequela_split for excluded areas

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
	}

// define locals from qsub command
	local date 			`1'
	local step_num 		`2'
	local step_name		`3'
	local location 		`4'
	local code_dir 		`5'
	local in_dir 		`6'
	local out_dir 		`7'
	local tmp_dir 		`8'
	local root_tmp_dir 	`9'
	local root_j_dir 	`10'

	// write log if running in parallel and log is not already open
	cap log using "FILEPATH/sequela_`location'.smcl", replace
	if !_rc local close 1
	else local close 0

	// directory for standard code file
	adopath + FILEPATH

	** demographics
	local years 1990 1995 2000 2005 2010 2016
	local sexes 1 2

	** set meids
	local meids 			1468 1469 1470 1471 1472 1473 1474 1475
	local inf_mild_meid 	1468
	local dia_meid 			1469
	local hem_meid			1470
	local hep_meid			1471
	local asc_meid			1472
	local dys_meid			1473
	local bla_meid			1474
	local hyd_meid			1475

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
** WRITE CODE HERE

**********************************************************************************************************************
** Pull in prevalence estimates
**********************************************************************************************************************	
	import delimited "FILEPATH/species_specific_exclusions.csv", clear
	keep if location_id == `location'

	// does this location need to be split across species?
	local split = 0
	local intestinal = 0
	local urinary = 0	
	if haematobium == "" & mansoni == "" {
		local split = 1
	}
	** if there isn't coinfection, is it intestinal or urinary schistosomiasis
	if `split' == 0 {
		if haematobium == "" {
			local urinary = 1
		}
		if haematobium != "" {
			local intestinal = 1
		}
	}
	** pull in haematobium and mansoni specific draws if there is coinfection
	if `split' == 1 {
		import delimited "FILEPATH/haematobium_`location'.csv", clear
		rename draw_h* draw*
		tempfile haem_draws
		save `haem_draws', replace
		import delimited "FILEPATH/mansoni_`location'.csv", clear
		rename draw_m* draw*
		tempfile mans_draws
		save `mans_draws', replace
		get_draws, gbd_id_field(modelable_entity_id) gbd_id(2797) source(epi) measure_ids(5) location_ids(`location') status(best) clear
		drop if age_group_id == 22 | age_group_id == 27 | age_group_id == 33
		drop model_version_id modelable_entity_id
		tempfile total_prev
		save `total_prev', replace
	}
	** get draws if only one species
	** here we pull in draws, save it as the species-specific prevalence tempfile, and replace with 0s for the irrelevant cause
	else if `split' == 0 {
		get_draws, gbd_id_field(modelable_entity_id) gbd_id(2797) source(epi) measure_ids(5) location_ids(`location') status(best) clear
		drop if age_group_id == 22 | age_group_id == 27
		drop model_version_id modelable_entity_id
		tempfile total_prev
		save `total_prev', replace
		if `urinary' == 1 {
			tempfile haem_draws
			save `haem_draws', replace
			forvalues i = 0/999 {
				qui replace draw_`i' = 0
				tempfile mans_draws
				save `mans_draws', replace
			}
		}
		else if `intestinal' == 1 {
			tempfile mans_draws
			save `mans_draws', replace
			forvalues i = 0/999 {
				qui replace draw_`i' = 0
				tempfile haem_draws
				save `haem_draws', replace
			}
		}
	}

**********************************************************************************************************************
** Get all-age, sex-specific estimates
**********************************************************************************************************************		

** pull in population data
	numlist "164 2/20 30/32 235"
	local ages = "`r(numlist)'"
    ** get the population estimates
	get_population, location_id(`location') sex_id("1 2") age_group_id("`ages'") year_id("`years'") clear               
    keep age_group_id location_id year_id sex_id population
    tempfile pop_env
    save `pop_env', replace

    ** merge envelope to prevalence estimates
    foreach type in haem mans {
    	use ``type'_draws', clear
    	drop if age_group_id == 33 | age_group_id == 27 | age_group_id == 22
    	merge 1:1 age_group_id location_id year_id sex_id using `pop_env', keep(1 3) nogen
    	forvalues i = 0/999 {
    		qui replace draw_`i' = draw_`i' * population
    		qui replace draw_`i' = 0 if age_group_id == 164
    	}
    	fastcollapse draw_* population, by(location_id year_id sex_id) type(sum)
    	forvalues i = 0/999 {
    		qui replace draw_`i' = draw_`i' / population
    	}
    	drop population
    	gen age_group_id = 22
    	save ``type'_draws', replace
    }

**********************************************************************************************************************
** Morbidity
**********************************************************************************************************************		
** set parameters for translating infection (x) to morbidity (y): y = (a + bx^c)/(1+bx^c)
	** diarrhea (S. mansoni and/or japonicum) - van der Werf 2002
	local a_dia = 0.222
	local b_dia = 0.272
	local c_dia = 7.702

** equation 5: same as equation 1 above (with b = (c - 1)/(c + 1))
	** Moderate/severe hepatic disease -- hepatomegaly (MSL - mid sternal level) (S. mansoni and/or japonicum) - van der Werf 2002
	local a_hep = 0.183
	local b_hep = 0.217
	local c_hep = 1.555

** NEW **
	** Moderate/severe hepatic disease -- hepatomegaly (MCL - mid clavical level) (S. mansoni and/or japonicum) - van der Werf 2002
	local a_hep_mcl = 0.143
	local b_hep_mcl = 0.165
	local c_hep_mcl = 1.395

** NEW **
	** splenomegaly (S. mansoni and/or japonicum) - van der Werf 2002
	local a_spl = 0.094
	local b_spl = 0.120
	local c_spl = 1.272

	** Dysuria during last two weeks (S. haematobium) - van der Werf 2003
	local a_dys = 0.30
	local b_dys = 1.94
	local c_dys = 4.23

	** Major hydronephrosis (S. haematobium) van der Werf 2003
	local a_hyd = 0
	local b_hyd = 0.11
	local c_hyd = 1.23

** equation 1 combined with equation 4: y = (a + bx^c)/(1 + bx^c) where b = (c - 1)/(c + 1)
    ** history of haematemesis ˜this year(Ongom and Bradley, 1972)."
	local a_hem = 0
	local b_hem = 0.0133
	local c_hem = 1.027

	** Ascites (S. mansoni and/or japonicum) - van der Werf 2002
	local a_asc = 0
	local b_asc = 0.00249
	local c_asc = 1.005

	** Bladder pathology on ultrasound (S. haematobium) - van der Werf 2004
	local a_bla = 0.033
	local b_bla = 1.35
	local c_bla = 1.78


**********************************************************************************************************************
** Intestinal schistosomiasis (S. mansoni/japonicum/intercalatum/mekongi)
**********************************************************************************************************************		
** pull in file
use `mans_draws', clear
	** Intestinal schistosomiasis morbidity
	foreach seq in dia hep hem asc {
		preserve
		forvalues i = 0/999 {
			qui gen seq_`i' = (`a_`seq'' + `b_`seq'' * draw_`i'^`c_`seq'')/(1 + `b_`seq'' * draw_`i'^`c_`seq'') - `a_`seq'' 
			qui replace seq_`i' = 0 if draw_`i' == 0
			drop draw_`i'
		}
		tempfile `seq'_draws
		save ``seq'_draws', replace
		restore
	}

	** Transform hematemesis lifetime-prevalence to point prevalence, assuming that 1-year-period prevalence
	** equals 1/2*lifetime-prevalence (Ongom and Bradley, 1972) and duration is 2 days.
	preserve
	use `hem_draws', clear
	local duration 2
	local recall 365
	forvalues i = 0/999 {
		qui replace seq_`i' = seq_`i'/2 * `duration'/(`duration' - 1 + `recall')
	}
	save `hem_draws', replace
	restore

	** squeeze cases into species-specific prevalence, if necessary
	foreach seq in dia hep hem asc {
		merge 1:1 sex_id year_id using ``seq'_draws', nogen assert(3)
		rename seq_* `seq'_*
	}
	forvalues i = 0/999 {
		gen any_man_inf_`i' = dia_`i' + hep_`i' + hem_`i' + asc_`i'
	}
	foreach seq in dia hep hem asc {
		forvalues i = 0/999 {
			qui replace `seq'_`i' = `seq'_`i' * draw_`i'/any_man_inf_`i' if any_man_inf_`i' > draw_`i'
		}
		preserve
		keep age_group_id year_id sex_id location_id `seq'*
		save ``seq'_draws', replace
		restore	
	}

**********************************************************************************************************************
** Urinary schistosomiasis (S. haematobium)
**********************************************************************************************************************
** pull in file
use `haem_draws', clear
	** save temp files for each sequelae
	foreach seq in dys hyd bla {
		preserve
		forvalues i = 0/999 {
			qui gen seq_`i' = (`a_`seq'' + `b_`seq'' * draw_`i'^`c_`seq'')/(1 + `b_`seq'' * draw_`i'^`c_`seq'') - `a_`seq'' 
			qui replace seq_`i' = 0 if draw_`i' == 0
			drop draw_`i'
		}
		tempfile `seq'_draws
		save ``seq'_draws', replace
		restore
	}

	** Transform dysuria two-week period prevalence to point prevalence, assuming duration is 14 days
	preserve
	use `dys_draws', clear
	local duration 14
	local recall 14
	forvalues i = 0/999 {
		qui replace seq_`i' = seq_`i'*`duration'/(`duration' - 1 + `recall')
	}
	save `dys_draws', replace
	restore

	** squeeze cases into species-specific prevalence, if necessary
	foreach seq in dys hyd bla {
		merge 1:1 sex_id year_id using ``seq'_draws', nogen assert(3)
		rename seq_* `seq'_*
	}
	forvalues i = 0/999 {
		gen any_haem_inf_`i' = dys_`i' + hyd_`i' + bla_`i'
	}
	foreach seq in dys hyd bla {
		forvalues i = 0/999 {
			qui replace `seq'_`i' = `seq'_`i' * draw_`i'/any_haem_inf_`i' if any_haem_inf_`i' > draw_`i'
		}
		preserve
		keep age_group_id year_id sex_id location_id `seq'*
		save ``seq'_draws', replace
		restore	
	}

	** squeeze cases into overall prevalence, if necessary
	rename draw_* any_h_*
	merge 1:1 sex_id year_id using `mans_draws', nogen assert(3)
	forvalues i = 0/999 {
		gen any_sch_`i' = draw_`i' + any_h_`i'
	}
	foreach seq in dia hep hem asc dys hyd bla {
		merge 1:1 sex_id year_id using ``seq'_draws', nogen assert(3)
	}
	forvalues i = 0/999 {
		gen any_seq_`i' = dia_`i' + hep_`i' + hem_`i' + asc_`i' + dys_`i' + hyd_`i' + bla_`i'
	}
	foreach seq in dia hep hem asc dys hyd bla {
		forvalues i = 0/999 {
			quietly replace `seq'_`i' = `seq'_`i' * any_sch_`i' / any_seq_`i' if any_seq_`i' > any_sch_`i'
		}
		preserve
		keep age_group_id year_id sex_id location_id `seq'*
		save ``seq'_draws', replace
		restore	
	}

**********************************************************************************************************************
** Pull in stage-specific age patterns
**********************************************************************************************************************
	** get draws
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(1465) measure_ids(18) location_ids(1) year_ids(2000) source(epi) status(best) clear
	replace location_id = `location'
	drop if age_group_id == 22 | age_group_id == 27 | age_group_id == 33
	drop year_id
	tempfile stage1_draws
	quietly save `stage1_draws', replace

	get_draws, gbd_id_field(modelable_entity_id) gbd_id(1466) measure_ids(18) location_ids(1) year_ids(2000) source(epi) status(best) clear
	replace location_id = `location'
	drop year_id
	tempfile stage2_draws
	quietly save `stage2_draws', replace

	get_draws, gbd_id_field(modelable_entity_id) gbd_id(1467) measure_ids(18) location_ids(1) year_ids(2000) source(epi) status(best) clear
	replace location_id = `location'
	drop year_id
	tempfile stage3_draws
	quietly save `stage3_draws', replace

	get_covariate_estimates, covariate_id(1110) clear
	keep if location_id == `location'
	keep location_id mean_value year_id
	tempfile mda
	save `mda', replace

** loop through sequelae
	foreach seq in dia bla hep dys hyd hem asc {
		di "working on `seq' draws now"

		if inlist("`seq'", "dia", "bla") {
		local stage = "stage1"
		}
		if inlist("`seq'", "hep", "dys", "hyd") {
		local stage = "stage2"
		}
		if inlist("`seq'", "hem", "asc") {
		local stage = "stage3"
		}
		** pull in draws
		use ``stage'_draws', clear
		** replace with 0s for younger age groups, if age < 15
		if inlist("`seq'","hep","hem","asc") {
			forvalues i = 0/999 {
				quietly replace draw_`i' = 0 if age_group_id < 8
			}
		}
		** replace with 0s for younger age groups, if < 1
		else {
			forvalues i = 0/999 {
				quietly replace draw_`i' = 0 if age_group_id < 5
			}
		}

		** merge in population and overall prevalence estimates, and compute cases by age and sex
		merge 1:m location_id sex_id age_group_id using `pop_env', keepusing(population year_id) assert(3) nogen
		merge m:1 location_id sex_id year_id using ``seq'_draws', nogen assert(3)
		bysort year_id location_id sex_id: egen total_pop = total(population)
		forvalues i = 0/999 {
			** calculate unscaled cases by age, sex, and total
			qui replace draw_`i' = draw_`i' * population
			bysort year_id location_id sex_id: egen total_dis_cases_`i' = total(draw_`i')
			** calculate total cases according to custom model
			qui replace `seq'_`i' = `seq'_`i' * total_pop
			** rescale age and sex-specific cases such that they add up to the total predicted by the custom model, 
			** and convert to prevalence per capita
			qui replace draw_`i' = (draw_`i' * (`seq'_`i'/total_dis_cases_`i'))/population
			qui replace draw_`i' = 1 if draw_`i' > 1
		}
		keep draw_* age_group_id sex_id location_id year_id

		** Effect on chronic symptoms: assume zero incidence in treated cases. Correct for excess mortality
		** among treated cohort (not balanced by incidence), assuming prevalent cases die at a rate of 0.1/year
		** (Kheir MM et al. Am J Trop Med Hyg. 1999 Feb;60(2):307-10). The proportion treated cases are
		** assumed to be the average coverage over the whole period since 2005

		** For post-control years (2010 and 2015), take 2005 results and correct for effect of treatment, only for chronic symptoms
		replace age_group_id = 1 if age_group_id == 164
		tempfile draws
		save `draws', replace
		foreach sex of local sexes {
			use `draws', clear
			keep if sex_id == `sex'
			if inlist("`seq'", "hep", "hem", "asc") {
				merge m:1 year_id using `mda', keep(3) nogen
				preserve
				gsort -age
				keep if year_id == 2005
				forvalues i = 0/999 {
					qui replace draw_`i' = draw_`i' * (1 - mean_value/5) + draw_`i'[_n+1] * mean_value/5 * .9^5 if age_group_id >= 8
					qui replace draw_`i' = 1 if draw_`i' > 1
					replace draw_`i' = 0 if age_group_id == 1
				}
				replace year_id = 2010
				tempfile `seq'_2010_`sex'
				save ``seq'_2010_`sex'', replace
				restore, preserve
				gsort -age
				keep if year_id == 2005
				forvalues i = 0/999 {
					qui replace draw_`i' = draw_`i' * (1 - mean_value/11) + (4*draw_`i'[_n+2]/5 + 1*draw_`i'[_n+3]/5) * mean_value/11 * .9^11 if age_group_id >= 8
					qui replace draw_`i' = 1 if draw_`i' > 1
					replace draw_`i' = 0 if age_group_id == 1			
				}
				replace year_id = 2016
				tempfile `seq'_2016_`sex'
				save ``seq'_2016_`sex'', replace
				restore
				drop if year_id == 2010 | year_id == 2016
				append using ``seq'_2010_`sex''
				append using ``seq'_2016_`sex''
			}
			tempfile draws_`sex'
			save `draws_`sex'', replace
		}
		use `draws_1', clear
		append using `draws_2'
		** fix back the age_group_id fix
		replace age_group_id = 164 if age_group_id == 1
		qui keep age_group_id draw_* year_id sex_id
		rename draw_* seq_*
		save ``seq'_draws', replace
	}


**********************************************************************************************************************
** Populate draws for mild infection without specific sequelae
**********************************************************************************************************************
** pull in total schistosomiasis draws
use `total_prev', clear
foreach seq in dia hep hem asc dys hyd bla {
	merge 1:1 age_group_id year_id sex_id using ``seq'_draws', nogen assert(3)
	forvalues i = 0/999 {
		qui replace draw_`i' = draw_`i' - seq_`i' if !missing(seq_`i')
	}
	drop seq_*
}

forvalues i = 0/999 {
	qui replace draw_`i' = 0 if draw_`i' < 0
}

export delimited "FILEPATH/`location'.csv", replace

foreach cause in dia hep hem asc dys hyd bla {
	use ``cause'_draws', clear
	rename seq_* draw_*
	gen measure_id = 5
	export delimited "FILEPATH/`location'.csv", replace
}

**********************************************************************************************************************
** Finish
**********************************************************************************************************************

// write check here
	file open finished using "FILEPATH/finished_loc`location'.txt", replace write
	file close finished

// close logs
	if `close' log close
	clear

