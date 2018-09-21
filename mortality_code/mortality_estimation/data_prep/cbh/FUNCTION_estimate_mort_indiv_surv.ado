** Description: This function estimates early, late, and post-neonatal, child, and under-5 mortality from complete birth history data 
** NOTE: IHME OWNS THE COPYRIGHT
** ************************************************************************
	
// Describe program syntax 
	cap program drop CBH_estimate_mort_indiv_surv
	program define CBH_estimate_mort_indiv_surv
		version 11
		syntax, directory(string) /// set directory 
				sex(string) /// set sex 
				period(integer) /// how long do you want the periods to pool over? (2 is standard for GPR; 5 is standard for age/sex model) 
				neonatal(integer) /// calculate early and late (1) or just overall neonatal (2) 
				save_GBD(integer) /// output for GBD? 
				save_sims(integer) /// save simulations? 
				[subset(integer 0)] /// run function for only one country 
				[subset_country(string)] /// which countries to run the function for (if subset = 1)
				[restart(integer 0)] /// should the function restart part way through the country list 
				[restart_country_survey(string)] /// which country-survey (eg ALB_2008-2009) should the function restart at (if restart = 1) 
				[exclude(integer 0)] /// exclude countries 
				[exclude_country(string)] /// which countries to exclude? (if exclude = 1; note that this doesn't effect anything that has already been run) 
				[shock(integer 0)] /// is there a shock? 
				[shock_start(real 0)] /// in what year does the shock start? (if shock = 1);
				[shock_end(real 0)] // in what year does the shock end? (if shock = 1); note that the period for the shock is (shock_start, shock_end]  


// Obtain list of all countries to combine: 
	quietly { 
	clear 
	local all_files: dir "strSurveyRawDataDir/`sex'/" files "*raw_`sex'.dta", respectcase
	gen filename = "" 
	foreach file of local all_files { 
			count
			local set = `r(N)' + 1 
			set obs `set'
			replace filename = "`file'" if _n == _N 
	} 
	split filename, parse("_")
	drop filename3-filename4 
	rename filename1 country
	sort country
	gen ctry_svy = country + "_" + filename2
	local first_country = country[1]
	
	// allow for restarting 
	if(`restart' == 1) {
		gen index = _n
		bysort country: egen minim = min(index)
		summ minim if country == "`restart_country'"
		drop if index < r(mean)
		drop index
		levelsof country, local(drop_me)

		// delete all these countries from the saving files 
		if (`save_sims' == 0) local fileheaders `" "q5" "country info" "' 
		if (`save_sims' == 1) local fileheaders `" "q5" "q5sims" "country info" "' 
		foreach fileheader of local fileheaders {
			preserve
			capture use "strSurveyOutputDir/`fileheader' - `sex' - `period'.dta", clear
			foreach country in local drop_me {
				drop if country == "`country'"
			}
			saveold "strSurveyOutputDir/`fileheader' - `sex' - `period'.dta", replace
			restore
		}
	}

  	
	// allow for subseting 
	if(`subset' == 1) {
		gen keep = 0 
		foreach cc of local subset_country { 
			replace keep = 1 if country == "`cc'"
		} 
		keep if keep == 1
		drop keep 

		if (`save_sims' == 0) local fileheaders `" "q5" "country info" "' 
		if (`save_sims' == 1) local fileheaders `" "q5" "q5sims" "country info" "' 
		foreach fileheader of local fileheaders {
			foreach cc of local subset_country { 
				preserve
				di "`fileheader'" 
				capture use "strSurveyOutputDir/`fileheader' - `sex' - `period'.dta", clear 	 
				drop if country == "`cc'"
				saveold "strSurveyOutputDir/`fileheader' - `sex' - `period'.dta", replace	
				restore
			} 
		}
	}
	
	// allow for excluding 
	if (`exclude' == 1) { 
		foreach cc of local exclude_country { 
			drop if country == "`cc'" 
		} 
	} 

	levelsof ctry_svy, local(surveys)
	levelsof country, local(countries)
	
	tempfile surveynames
	save `surveynames', replace
 
 	// Loop through countries/surveys
	local total = _N 
	local count = 0 
	cd "strSurveyOutputDir" 

	foreach country of local countries {

		local count = `count' + 1 
		noisily display "*********************************************************************" _newline "STARTING ON `country' (`count' of `total')"
		
		// get surveys in current country from tempfile saved before
		use `surveynames', clear
		levelsof ctry_svy if country == "`country'", local(surveys)
		
		foreach survey of local surveys {

			// load data 
			noisily display "    Loading data..."
			use "strSurveyRawDataDir/`sex'/`survey'_`sex'", clear
		
			// convert year to calendar year from cmc 
			replace year = 1900 + (year-1)/12
			drop tps

			// convert alive dummy 
			noisily display "    Creating dummies..."
			gen alive = !death
			drop death 

			// create calendar dummies 
			if ("`sex'" == "both") { // find the range of the data 
				summ year
				local max_year_real = r(max)
				local min_year_real = r(min)
				local min_month = round((`max_year_real' - trunc(`max_year_real'))*12)+1
				local year_diff = ceil((`max_year_real' - `min_year_real'))
			} 
			else {  // this makes sure that we end up with the same periods for males and females as we do for both sexes combined 
				preserve
				use "strSurveyRawDataDir/both/`survey'_both", clear
				replace year = 1900 + (year-1)/12		
				summ year
				local max_year_real = r(max)
				local min_year_real = r(min)
				local min_month = round((`max_year_real' - trunc(`max_year_real'))*12)+1
				local year_diff = ceil((`max_year_real' - `min_year_real'))	
				restore
			} 	
			
			// This assigns n year periods starting at the most recent (decimal) year value, and counting back by n.  
			// year2 will be the middle of of the n year period
			gen year2 = `max_year_real' - (floor((`max_year_real' - year)/`period') + 0.5)*`period'
			local middle = `period'/2 
								
			if (`shock' == 1) { // if there is a shock year, modify the periods accordingly 
				summ year2 
				if `r(max)'>=`shock_start' {
				gen shock = (year > `shock_start' & year <= `shock_end') // mark the observations in the shock years
				
				// find which period the shock year starts and end in 
				summ year2 if `shock_start' >= (year2 - `middle') & `shock_start' < (year2 + `middle')
				local before_residual = `r(min)'
				summ year2 if `shock_end' > (year2 -`middle') & `shock_end' <= (year2 + `middle')
				local after_residual = `r(max)'
				gen year3 = year2 
				
				// for the period the shock starts in, if the residual is over a year in length, make it it's own period, otherwise add it to the period before
				local length = `shock_start' - (`before_residual' - `middle')
				if (`length' >= 1) replace year3 = (`before_residual' - `middle') + `length'/2 if (year2 <= `before_residual'+0.01 & year2 >= `before_residual'-0.01 & shock == 0 & year <= `shock_start')
				else replace year3 = (`before_residual' - 3*`middle') + (`length'+`period')/2 if (year2 <= `before_residual'+0.01 & year2 >= `before_residual'-0.01 & shock == 0 & year <= `shock_start') | year2 == (`before_residual' - `period')

				// for the period the shock ends in, if the residual is over a year in length, make it it's own period, otherwise add it to the period after 
				local length = (`after_residual' + `middle') - `shock_end' 
				if (`length' >= 1) replace year3 = (`after_residual' + `middle') - `length'/2 if (year2 <= `after_residual'+0.01 & year2 >= `after_residual'-0.01 & shock == 0 & year >= `shock_end')
				else replace year3 = (`after_residual' + 3*`middle') - (`length'+`period')/2 if (year2 <= `after_residual'+0.01 & year2 >= `after_residual'-0.01 & shock == 0 & year >= `shock_end') | year2 == (`after_residual' + `period')		
			
				// assign a time period to the shock 
				replace year3 = `shock_start' + 0.5*(`shock_end' - `shock_start') if shock == 1 
				replace year2 = year3
				drop year3 
				}
				else noisily di "No shock years in this survey"	
			} 		
			
			replace year = year2
			drop year2 
			label var year "mid-point of time interval"
			
			// restrict sample sizes (drops those years where #person-months < 10000 and the year is more than halfway back in the recall period) 
			// only drop years where the the both sexes total # of person months is less than 10,000
			if ("`sex'" == "both") { 
				bysort year: egen count = count(year)
				preserve
					gen drop = 1 if (count < 10000) & (year < (`max_year_real'+`min_year_real')/2)
					keep if drop == 1
					keep year drop
					gen survey = "`survey'"
					gen country = "`country'"
					gen period = `period'
					capture duplicates drop
					tempfile drop_years
					save `drop_years', replace
					capture use "strSurveyOutputDir/drop_years.dta", clear
					if _rc == 0 {
						capture drop if survey == "`survey'" & country == "`country'" & period == `period'
						append using `drop_years'
					}
					compress
					saveold "strSurveyOutputDir/drop_years.dta", replace
				restore
				drop if (count < 10000) & (year < (`max_year_real'+`min_year_real')/2)
				drop count
			} 
			else {  
				preserve
				use "strSurveyOutputDir/drop_years.dta", clear
				keep if survey == "`survey'" & country == "`country'" & period == `period'
				capture levelsof year, local(years)
				restore
				foreach yr of local years {
					drop if year <= `yr'
				}
			} 	
			
			sum year
			local max_year = r(max)	// recalculates years after dropping due to sample size 
			local min_year = r(min)
			
		if (`neonatal' == 1) { 
			// separate early and late neonatal 
			preserve
			keep if age == 0 
			gen temp_id = _n 
			expand 2
			bysort temp_id: replace age = (_n-1)/2 // 0 = early neonatal and 0.5 = late neonatal 
			drop temp_id
			
			drop if nn_death == 1 & age == 0.5 // drop observations for the late neonatal period for individuals who die in the early neonatal period  
			replace alive = 1 if nn_death == 2 & age == 0 // mark an individual who dies in the late neonatal period as alive during the early neonatal period 

			tempfile temp_neonatal
			save `temp_neonatal', replace
			restore
			drop if age == 0 
			append using `temp_neonatal'

			// create age dummies 
			gen age2 = age
			replace age2 = 0 if age == 0 				// early neonatal
			replace age2 = 1 if age == 0.5				// late neonatal
			replace age2 = 2 if age >=  1 & age < 12	// 1-11 months
			replace age2 = 3 if age >= 12 & age < 24	// 12-23 months (age 1)
			replace age2 = 4 if age >= 24 & age < 36	// 24-36 months (age 2)
			replace age2 = 5 if age >= 36 & age < 48	// 36-47 months (age 3)
			replace age2 = 6 if age >= 48 & age < 60	// 48-59 months (age 4)
			capture label drop agelabels 
			label define agelabels 0 "early neonatal" 1 "late neonatal" 2 "post-neonatal" 3 "age 1" 4 "age 2" 5 "age 3" 6 "age 4"
			label values age2 agelabels
			replace age = age2
			drop age2
			label values age agelabels
		} 
		if (`neonatal' == 2) { 
			// create age dummies 
			gen age2 = age
			replace age2 = 1 if age == 0 				// neonatal
			replace age2 = 2 if age >=  1 & age < 12	// 1-11 months
			replace age2 = 3 if age >= 12 & age < 24	// 12-23 months (age 1)
			replace age2 = 4 if age >= 24 & age < 36	// 24-36 months (age 2)
			replace age2 = 5 if age >= 36 & age < 48	// 36-47 months (age 3)
			replace age2 = 6 if age >= 48 & age < 60	// 48-59 months (age 4)
			capture label drop agelabels 
			label define agelabels 1 "neonatal" 2 "post-neonatal" 3 "age 1" 4 "age 2" 5 "age 3" 6 "age 4"
			label values age2 agelabels
			replace age = age2
			drop age2
			label values age agelabels
		} 
			// Obtain probability of survival for each period (i.e. the mean survival across all periods observed in each age group) 
			noisily display "    Proportion of survivors per period..."
			svyset psu [pweight = sample_weight]
			svy: mean alive, over(year age)
			mat mean_est = e(b)
			mat mean_est_transpose = mean_est'
			mat vcov = e(V)
			mat var = vecdiag(vcov)'
			
			// Save count of deaths for each period
			gen _freq = 1 
			gen death_count = (alive == 0)
			collapse (sum) _freq death_count, by(year age)

			// Generate simulations for uncertainty estimates 
			sort year age
			svmat mean_est_transpose, names(p_age) // at this point, there is a dataset that gives the monthly prob of survival for each age-year combination and the sample size (in terms of months observed)
			rename p_age1 pr_age
			forvalues s = 1/1000 {		
				gen p_age`s' = (rbinomial(_freq,pr_age))/_freq if pr_age < 1
				replace p_age`s' = 1 if pr_age==1
			}
			rename _freq pm_count

			gen country = "`country'"
			order country death_count pm_count

			// Adjust probabilities 
				/* We obtain monthly probabilities of death for post-neonatal and ages 2-4; we are assuming that the monthly probability of dying stays constant w/in those months of age. For early and late neonatal, (as well as overall neonatal) we are obtaining the probability of dying over the entire period and there is no adjustment necessary */ 
			replace pr_age = pr_age^11 if age == 2
			replace pr_age = pr_age^12 if age >= 3 & age <= 6

			tempfile probabilities age_years
			sort year age
			save `probabilities', replace
			keep year age death_count pm_count
			sort year age
			save `age_years', replace

			// Compute 5q0 for all simulations 
			forvalues s = 1/1000 {			
				use `probabilities', clear
				drop death_count pm_count
				// keep simulation of interest 
				keep year age p_age`s' 
				// adjust probabilities as above 
				replace p_age`s' = p_age`s'^11 if age == 2
				replace p_age`s' = p_age`s'^12 if age >= 3 & age <= 6
				sort year age
				// reshape wide 
				reshape wide p_age`s', i(year) j(age)
				// calculate 5p0 and 5q0 
				if (`neonatal' == 1) gen p`s'5 = p_age`s'0 * p_age`s'1 * p_age`s'2 * p_age`s'3 * p_age`s'4 * p_age`s'5 * p_age`s'6
				if (`neonatal' == 2) gen p`s'5 = p_age`s'1 * p_age`s'2 * p_age`s'3 * p_age`s'4 * p_age`s'5 * p_age`s'6			
				gen q5_`s' = 1 - p`s'5
				drop p_age`s'* p`s'5
				sort year
				tempfile add
				save `add', replace
				// collect results 
				use `age_years', clear
				merge m:1 year using `add'	
				drop _merge
				save `age_years', replace
			}

			// Combine 5q0 estimates with estimates for other ages 
			merge 1:1 year age using `probabilities'
			drop p_age* // drop age-year simulations 
			rename pr_age p_age
			drop _merge

			reshape wide p_age death_count pm_count, i(year) j(age)
			order country p_age* pm_count* death_count*

			// Calculate 5q0 for mean estimates 
			if (`neonatal' == 1) gen p5 = p_age0 * p_age1 * p_age2 * p_age3 * p_age4 * p_age5 * p_age6
			if (`neonatal' == 2) gen p5 = p_age1 * p_age2 * p_age3 * p_age4 * p_age5 * p_age6
			gen q5 = 1-p5
			
			// Get Standard Deviation using Sims
			egen sd_q5 = rowsd(q5_*)
			
			// Get log(10) Standard Deviation -- this is the space we run the 5q0 estimation model in
			forvalues s = 1/1000 {
				gen log10_q5_`s' = log10(q5_`s')
			}
			egen log10_sd_q5 = rowsd(log10_q5_*)
			egen log10_mean_q5 = rowmean(log10_q5_*)

			// Store 5q0 simulations 
			if (`save_sims' == 1) { 
				preserve
				tempfile sims
				drop p5-q5
				save `sims'
				restore
			}

			// Get upper and lower bounds for q
			preserve				
			mkmat q5_*, matrix(qs)
			matrix q = qs'
			clear
			svmat q

			foreach var of varlist q* {
			_pctile `var', p(2.5, 97.5)
			gen lb_`var' = r(r1)
			gen ub_`var' = r(r2)
			}
			mkmat lb* in 1, matrix(L)
			mkmat ub* in 1, matrix(U)
			matrix LB = L'
			matrix UB = U'
			*matrix SD = S'
			restore

			drop q5_*
			svmat LB, names(lb_q5)
			svmat UB, names(ub_q5)
			rename lb_q51 lb_q5
			rename ub_q51 ub_q5

			// Save results 
			noisily display "    Saving..."
			
			keep country year q5 lb_q5 ub_q5 death_count* pm_count* p_age* sd_q5 log10_sd_q5 log10_mean_q5 
			
		if (`neonatal' == 1) { 
			label variable p_age0 "early neonatal survival" 
				rename p_age0 p_enn 
			label variable p_age1 "late neonatal survival" 
				rename p_age1 p_lnn
			label variable p_age2 "post-neonatal survival" 
				rename p_age2 p_pnn 
			label variable p_age3 "1p1"
				rename p_age3 p_1p1
			label variable p_age4 "1p2"
				rename p_age4 p_1p2
			label variable p_age5 "1p3"
				rename p_age5 p_1p3
			label variable p_age6 "1p4" 
				rename p_age6 p_1p4
				
			label variable pm_count0 "early neonatal person months (unweighted)"
				rename pm_count0 pm_count_enn 
			label variable pm_count1 "late neonatal person months (unweighted)"
				rename pm_count1 pm_count_lnn 
			label variable pm_count2 "post-neonatal person months (unweighted)"
				rename pm_count2 pm_count_pnn 
			label variable pm_count3 "age 1 person months (unweighted)" 
				rename pm_count3 pm_count_1yr
			label variable pm_count4 "age 2 person months (unweighted)" 
				rename pm_count4 pm_count_2yr
			label variable pm_count5 "age 3 person months (unweighted)" 
				rename pm_count5 pm_count_3yr
			label variable pm_count6 "age 4 person months (unweighted)"
				rename pm_count6 pm_count_4yr
				
			label variable death_count0 "early neonatal deaths (unweighted)"
				rename death_count0 death_count_enn 
			label variable death_count1 "late neonatal deaths (unweighted)"
				rename death_count1 death_count_lnn 
			label variable death_count2 "post-neonatal deaths (unweighted)"
				rename death_count2 death_count_pnn 
			label variable death_count3 "age 1 deaths (unweighted)" 
				rename death_count3 death_count_1yr
			label variable death_count4 "age 2 deaths (unweighted)" 
				rename death_count4 death_count_2yr
			label variable death_count5 "age 3 deaths (unweighted)" 
				rename death_count5 death_count_3yr
			label variable death_count6 "age 4 deaths (unweighted)"
				rename death_count6 death_count_4yr
				
			label variable q5 "5q0" 
			label variable lb_q5 "5q0 - lower bound" 
			label variable ub_q5 "5q0 - upper bound" 
		} 
		if (`neonatal' == 2) { 
			label variable p_age1 "neonatal survival" 
				rename p_age1 p_nn 
			label variable p_age2 "post-neonatal survival" 
				rename p_age2 p_pnn 
			label variable p_age3 "1p1"
				rename p_age3 p_1p1
			label variable p_age4 "1p2"
				rename p_age4 p_1p2
			label variable p_age5 "1p3"
				rename p_age5 p_1p3
			label variable p_age6 "1p4" 
				rename p_age6 p_1p4
				
			label variable pm_count1 "neonatal person months (unweighted)"
				rename pm_count1 pm_count_nn 
			label variable pm_count2 "post-neonatal person months (unweighted)"
				rename pm_count2 pm_count_pnn 
			label variable pm_count3 "age 1 person months (unweighted)" 
				rename pm_count3 pm_count_1yr
			label variable pm_count4 "age 2 person months (unweighted)" 
				rename pm_count4 pm_count_2yr
			label variable pm_count5 "age 3 person months (unweighted)" 
				rename pm_count5 pm_count_3yr
			label variable pm_count6 "age 4 person months (unweighted)"
				rename pm_count6 pm_count_4yr
				
			label variable death_count1 "neonatal deaths (unweighted)"
				rename death_count1 death_count_nn 
			label variable death_count2 "post-neonatal deaths (unweighted)"
				rename death_count2 death_count_pnn 
			label variable death_count3 "age 1 deaths (unweighted)" 
				rename death_count3 death_count_1yr
			label variable death_count4 "age 2 deaths (unweighted)" 
				rename death_count4 death_count_2yr
			label variable death_count5 "age 3 deaths (unweighted)" 
				rename death_count5 death_count_3yr
			label variable death_count6 "age 4 deaths (unweighted)"
				rename death_count6 death_count_4yr			
				
			label variable q5 "5q0" 
			label variable lb_q5 "5q0 - lower bound" 
			label variable ub_q5 "5q0 - upper bound" 			
		} 
			
			preserve
			if ("`country'" != "`first_country'") append using "q5 - `sex' - `period'.dta"
			sort country year
			saveold "strSurveyOutputDir/q5 - `sex' - `period'.dta", replace
			restore
			** edit source variable
			tempfile all_data
			save `all_data', replace
			use "strSurveyOutputDir/q5 - `sex' - `period'.dta", clear
				cap gen source = "`survey'"
				if _rc == 0 {
					split source, parse("_")
					drop source source1
					gen source = "$sv" + " " + source2
					drop source2
				}
				if _rc != 0 {
				preserve
				keep if source == "" 
					replace source = "`survey'"
					split source, parse("_")
					drop source source1
					gen source = "$sv" + " " + source2
					drop source2
					tempfile sourced
					save `sourced', replace
				restore
				drop if source == ""
				append using `sourced'
				}
				order country source
				sort country source year
				if (length(country) > 3) {
					replace country = substr(country, 1, 3) + "_" + substr(country, 4, .) if regexm(country, "_") != 1
				}	
			saveold "strSurveyOutputDir/q5 - `sex' - `period'.dta", replace
			use `all_data', clear

			// Only save for output to GBD if sex=="both"
			if("`sex'"=="both" & `save_GBD' == 1) {
				drop lb_q5 ub_q5
				replace q5=q5*1000
				drop if q5 == . 
				rename country iso3
				
				// format person-month variables to be used as the sample size in the data variance calculation as a part of the GPR process
				// first change enn and lnn to months rather than weeks
				cap replace pm_count_enn = pm_count_enn*(1/4)
				cap replace pm_count_lnn = pm_count_lnn*(3/4)
				// assure the enn/lnn breakdown never overlaps with nn reporting (when nn is in the dataset)
				looUSER pm_count_nn
				local nn_exists = r(varlist)
				looUSER pm_count_enn
				local enn_exists = r(varlist)
				if ( "`nn_exists'" != "." & "`enn_exists'" != ".") {
					assert pm_count_enn == . if pm_count_nn != .
					replace pm_count_enn = 0 if pm_count_nn != . 
					replace pm_count_lnn = 0 if pm_count_nn != .
					replace pm_count_nn = 0 if pm_count_nn == .
				}
				
				// now take rowtotal to get the total number of person months across ages
				if ( "`nn_exists'" != "." & "`enn_exists'" != "." ) {
					egen total_pm = rowtotal(pm_count_enn pm_count_lnn pm_count_pnn pm_count_1yr pm_count_2yr pm_count_3yr pm_count_4yr pm_count_nn)
				}
				if ("`nn_exists'" == "." & "`enn_exists'" != "." ) {
					egen total_pm = rowtotal(pm_count_enn pm_count_lnn pm_count_pnn pm_count_1yr pm_count_2yr pm_count_3yr pm_count_4yr)
				}
				if ("`nn_exists'" != "." & "`enn_exists'" == "." ) {
					egen total_pm = rowtotal(pm_count_pnn pm_count_1yr pm_count_2yr pm_count_3yr pm_count_4yr pm_count_nn)
				}
				drop pm*
				// for sourcing
				gen source = "`survey'"
				split source, parse("_")
				drop source source1
				gen source = "$sv" + " " + source2
				drop source2
				order iso3 source
				cd "strSurveyOutputDir/direct 5q0 for GBD - survey"
				saveold "direct 5q0 for GBD - `survey'.dta", replace
				cd "strSurveyOutputDir/"
			}

			// Save sims 
			if (`save_sims' == 1) { 
				clear
				use `sims', clear
				if ("`country'" != "`first_country'") append using "q5sims - `sex' - `period'.dta"
				sort country year
				cd "strSurveyOutputDir/"
				saveold "q5sims - `sex' - `period'.dta", replace
				
			tempfile all_data
			save `all_data', replace
			// for citation
			use "q5sims - `sex' - `period'.dta", clear
				cap gen source = "`survey'"
				if _rc == 0 {
					split source, parse("_")
					drop source source1
					gen source = "$sv" + " " + source2
					drop source2
				}
				if _rc != 0 {
				preserve
				keep if source == "" 
					replace source = "`survey'"
					split source, parse("_")
					drop source source1
					gen source = "$sv" + " " + source2
					drop source2
					tempfile sourced
					save `sourced', replace
				restore
				drop if source == ""
				append using `sourced'
				}
				order country source
				sort country source year
				if (length(country) > 3) {
					replace country = substr(country, 1, 3) + "_" + substr(country, 4, .) if regexm(country, "_") != 1
				}	
			cd "strSurveyOutputDir/"	
			saveold "q5sims - `sex' - `period'.dta", replace
			use `all_data', clear
			} 
			
			// Save country-info 
			clear
			set obs 1
			gen country = "`country'"
			gen min_year_real = `min_year_real'
			gen max_year_real = `max_year_real'
			gen min_month = `min_month'
			gen min_year = `min_year'
			gen max_year = `max_year'
			if ("`country'" != "`first_country'") append using "country info - `sex' - `period'.dta"
			sort country
			cd "strSurveyOutputDir/"
			if (length(country) > 3) {
				replace country = substr(country, 1, 3) + "_" + substr(country, 4, .) if regexm(country, "_") != 1
			}			
			saveold "country info - `sex' - `period'.dta", replace

			noisily display "    DONE!" _newline(2)
		} // close survey loop
	} // close country loop

} // close quietly 
	
	end // end function 
	

