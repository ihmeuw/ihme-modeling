// Calculate prevalence of sequelae

*** ======================= BOILERPLATE ======================= ***
	clear all
	set more off
	set maxvar 32000
	local user : env USER
	local code_root "FILEPATH`user'"
	local data_root FILEPATH

	local exec_from_args : env EXEC_FROM_ARGS
	capture confirm variable exec_from_args, exact
	if "`exec_from_args'" == "True" {
		local params_dir 		`2'
		local draws_dir 		`4'
		local interms_dir		`6'
		local logs_dir 			`8'
		local location			`10'
	}
	else {
		local params_dir "`data_root'FILEPATH"
		local draws_dir "`data_root'FILEPATH"
		local interms_dir "`data_root'FILEPATH"
		local logs_dir "`data_root'FILEPATH"
		local location 10
	}

	cap log using "`logs_dir'/FILEPATH`location'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	// Source relevant functions
	adopath + FILEPATH
	local gbd_round_id 7
    local decomp_step "step2"

*** ======================= MAIN EXECUTION ======================= ***

	** demographics
	local years 1990 1995 2000 2005 2010 2015 2019 2020 2021 2022
	local sexes 1 2

	** set meids
    local inf_mild_meid 	ADDRESS
	local dia_meid 			ADDRESS
	local hem_meid			ADDRESS
	local hep_meid			ADDRESS
	local asc_meid			ADDRESS
	local dys_meid			ADDRESS
	local bla_meid			ADDRESS
	local hyd_meid			ADDRESS

**********************************************************************************************************************
** Pull in prevalence estimates
**********************************************************************************************************************	
	import delimited "`params_dir'/FILEPATH", clear
	keep if location_id == `location'

	// create var indicating if location needs to be split across species
	local split = 0
	local intestinal = 0
	local urinary = 0	
	if haematobium == "" & mansoni == "" {
		local split = 1
	}
	
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
		import delimited "`draws_dir'/FILEPATH`location'.csv", clear
		rename draw_h* draw*
		tempfile haem_draws
		save `haem_draws', replace
		import delimited "`draws_dir'FILEPATH`location'.csv", clear
		rename draw_m* draw*
		tempfile mans_draws
		save `mans_draws', replace
		get_draws, gbd_id_type(modelable_entity_id) gbd_id(ADDRESS) source(epi) measure_id(5) location_id(`location') status(best) gbd_round_id(`gbd_round_id') decomp_step(`decomp_step') clear
		drop if age_group_id == 22 | age_group_id == 27 | age_group_id == 33
		drop model_version_id modelable_entity_id
		tempfile total_prev
		save `total_prev', replace
	}
	** get draws if loc has only one species
	** pull in draws, save it as the species-specific prevalence tempfile, and replace with 0s for the irrelevant cause
	else if `split' == 0 {
		get_draws, gbd_id_type(modelable_entity_id) gbd_id(ADDRESS) source(epi) measure_id(5) location_id(`location') status(best) gbd_round_id(`gbd_round_id') decomp_step(`decomp_step') clear
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
	numlist "2 3 6/20 30/32 235 238 388 389"
	local ages = "`r(numlist)'"
    ** get the population estimates
	get_population, location_id(`location') sex_id("1 2") age_group_id("`ages'") year_id("`years'") gbd_round_id(`gbd_round_id') decomp_step(`decomp_step') clear               
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
** set parameters for translating infection (x) to morbidity (y): y = (a + bx^c)/(1+bx^c) ** 
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
	** Hematemesis "ever" (S. mansoni and/or japonicum) - van der Werf 2002
	**"Ongom et al. showed that reported history of haematemesis ‘ever’ was only twice as frequent as reported
    ** history of haematemesis ‘this year’ (Ongom and Bradley, 1972)."
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
	** save temp files for each sequela
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
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(ADDRESS) measure_id(18) location_id(1) year_id(2000) source(epi) status(best) gbd_round_id(`gbd_round_id') decomp_step(`decomp_step') clear
	replace location_id = `location'
	drop if age_group_id == 22 | age_group_id == 27 | age_group_id == 33
	drop year_id
	tempfile stage1_draws
	quietly save `stage1_draws', replace

	get_draws, gbd_id_type(modelable_entity_id) gbd_id(ADDRESS) measure_id(18) location_id(1) year_id(2000) source(epi) status(best) gbd_round_id(`gbd_round_id') decomp_step(`decomp_step') clear
	replace location_id = `location'
	drop year_id
	tempfile stage2_draws
	quietly save `stage2_draws', replace

	get_draws, gbd_id_type(modelable_entity_id) gbd_id(ADDRESS) measure_id(18) location_id(1) year_id(2000) source(epi) status(best) gbd_round_id(`gbd_round_id') decomp_step(`decomp_step') clear
	replace location_id = `location'
	drop year_id
	tempfile stage3_draws
	quietly save `stage3_draws', replace

	get_covariate_estimates, covariate_id(ADDRESS) status(best) gbd_round_id(`gbd_round_id') decomp_step(`decomp_step') clear
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
				quietly replace draw_`i' = 0 if age_group_id > 236
			}
		}
		** replace with 0s for younger age groups, if < 1
		else {
			forvalues i = 0/999 {
				quietly replace draw_`i' = 0 if age_group_id < 5
				quietly replace draw_`i' = 0 if age_group_id > 236
			}
		}


		** merge in population and overall prevalence estimates, and compute cases by age and sex
		merge 1:m location_id sex_id age_group_id using `pop_env', keepusing(population year_id) keep(3) nogen
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
					qui replace draw_`i' = draw_`i' * (1 - mean_value/12) + (4*draw_`i'[_n+2]/5 + 1*draw_`i'[_n+3]/5) * mean_value/12 * .9^12 if age_group_id >= 8
					qui replace draw_`i' = 1 if draw_`i' > 1
					replace draw_`i' = 0 if age_group_id == 1			
				}
				replace year_id = 2017
				tempfile `seq'_2017_`sex'
				save ``seq'_2017_`sex'', replace
				restore
				drop if year_id == 2010 | year_id == 2017
				append using ``seq'_2010_`sex''
				append using ``seq'_2017_`sex''
			}
			tempfile draws_`sex'
			save `draws_`sex'', replace
		}
		use `draws_1', clear
		append using `draws_2'
		
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
drop metric_id
** this does not account for any coinfection
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
gen modelable_entity_id = `inf_mild_meid'
export delimited "`draws_dir'/`inf_mild_meid'/`location'.csv", replace

foreach cause in dia hep hem asc dys hyd bla {
	use ``cause'_draws', clear
	rename seq_* draw_*
	gen measure_id = 5
	gen modelable_entity_id = ``cause'_meid'
	export delimited "`draws_dir'/``cause'_meid'/`location'.csv", replace
}

*** ======================= CLOSE LOG ======================= ***
	if `close_log' log close
	exit, STATA clear
