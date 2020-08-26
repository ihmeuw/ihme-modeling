



/******************************************************************************\
                                PREP STATA
\******************************************************************************/

*** BOILERPLATE ***
	clear all
	set maxvar 10000
	set more off

	if c(os)=="Unix" {
		local  = "FILEPATH"
		set odbcmgr ADDRESS
		}
	else {
		local  = "FILEPATH"
		}


*** LOAD SHARED FUNCTIONS ***
	adopath + FILEPATH
	run FILEPATH
	run FILEPATH


*** ESTABLISH LOCALS AND DIRECTORIES ***
	local rootDir /FILEPATH


	tokenize "`rootDir'", parse(/)
	while "`1'"!="" {
		local path `path'`1'`2'
		capture mkdir `path'
		macro shift 2
		}

	capture mkdir FILEPATH
	capture mkdir FILEPATH


	foreach subDir in temp progress ADDRESS {
		!rm -rf `rootDir'/`subDir'
		sleep 2000
		capture mkdir `rootDir'/`subDir'
		}

	*/
	tempfile data efToan inc



	*local  "FILEPATH"
	use FILEPATH, clear
	capture drop _m
	merge m:1 location_id year_id using FILEPATH, nogenerate
	tempfile master
	save `master'

	import delimited using FILEPATH, clear
	keep if year_id>=1980
	merge 1:m location_id year_id using `master', gen(grMerge)
	generate endemic = 1 if denguePr>0 & !missing(denguePr)
	replace  endemic = 0 if denguePr==0
	replace  endemic = 0 if denguePr>0 & grMerge==3 & year_id>=1988 & !missing(denguePr)

*** CREATE CUBIC SPLINES ***

	local scoreEta 10
	local scoreVar score
	quietly sum `scoreVar'
	capture drop scoreG
	generate scoreG = (ln((normal((`scoreVar' - `r(mean)') / `r(sd)') + `scoreEta') / (1 - normal((`scoreVar' - `r(mean)') / `r(sd)') + `scoreEta')) + (ln((1 + `scoreEta') / `scoreEta'))) / (2*(ln((1 + `scoreEta') / `scoreEta')))

	*local trendEta 0.01
	local trendEta 0.01
	local lnTrendEta `trendEta'
	quietly sum lnTrend
	capture drop lnTrendG
	generate lnTrendG = (ln((normal((lnTrend - `r(mean)') / `r(sd)') + `trendEta') / (1 - normal((lnTrend - `r(mean)') / `r(sd)') + `trendEta')) + (ln((1 + `trendEta') / `trendEta'))) / (2*(ln((1 + `trendEta') / `trendEta')))
	*scatter lnTrendG lnTrend

	local deathRateEta 100
	quietly sum deathRate
	capture drop deathRateG
	generate deathRateG = (ln((normal((deathRate - `r(mean)') / `r(sd)') + `deathRateEta') / (1 - normal((deathRate - `r(mean)') / `r(sd)') + `deathRateEta')) + (ln((1 + `deathRateEta') / `deathRateEta'))) / (2*(ln((1 + `deathRateEta') / `deathRateEta')))
	*scatter deathRateG deathRate, xline(0.0001)
	generate lnDeathRateG = ln(deathRateG)

	local lnMrEta 10
	quietly sum lnMr
	capture drop lnMrG
	generate lnMrG = (ln((normal((lnMr - `r(mean)') / `r(sd)') + `lnMrEta') / (1 - normal((lnMr - `r(mean)') / `r(sd)') + `lnMrEta')) + (ln((1 + `lnMrEta') / `lnMrEta'))) / (2*(ln((1 + `lnMrEta') / `lnMrEta')))
	*scatter lnMrG lnMr, xline(-10)

	capture drop lnMrS*
	mkspline lnMrS1 -18 lnMrS2 = lnMr

	gen lnHaqi = ln(haqi)
	gen lnMeanMr = ln(meanMR)

	local lnMeanMrEta 10
	quietly sum lnMeanMr
	capture drop lnMeanMrG
	generate lnMeanMrG = (ln((normal((lnMeanMr - `r(mean)') / `r(sd)') + `lnMeanMrEta') / (1 - normal((lnMeanMr - `r(mean)') / `r(sd)') + `lnMeanMrEta')) + (ln((1 + `lnMeanMrEta') / `lnMeanMrEta'))) / (2*(ln((1 + `lnMeanMrEta') / `lnMeanMrEta')))
	*scatter lnMeanMrG lnMeanMr, xline(-10)

	gen sqrtMr = sqrt(deathRate)
	local sqrtMrEta 0.01
	quietly sum sqrtMr
	capture drop sqrtMrG
	generate sqrtMrG = (ln((normal((sqrtMr - `r(mean)') / `r(sd)') + `sqrtMrEta') / (1 - normal((sqrtMr - `r(mean)') / `r(sd)') + `sqrtMrEta')) + (ln((1 + `sqrtMrEta') / `sqrtMrEta'))) / (2*(ln((1 + `sqrtMrEta') / `sqrtMrEta')))
	*scatter sqrtMrG deathRate, xline(0.0001)

	gen lnYrsSince = ln(yrsSinceIntro + 1)
	gen oneType = (predCat==1)
/******************************************************************************\
                                 MODELLING
\******************************************************************************/

*** RUN THE CORE MODEL ***

	local spaceVar scoreG
	local spaceVarLab (eta = ``=reverse(subinstr(reverse("`spaceVar'"), "G", "", 1))'Eta')
	local timeVar lnTrendG
	local timeVarLab (eta = ``=reverse(subinstr(reverse("`timeVar'"), "G", "", 1))'Eta')
	local miscLab with subnat adjustment for IND, IDN & USA (based on deathRateG, eta=100)

	capture drop random* fixed* efTemp*
	menbreg casesM c.`spaceVar' `timeVar'  if denguePr>0, exp(sampleM) intmethod(mvaghermite) || location_id:

	predict randomModel, reffects reses(randomModelSe) nooffset
	predict fixed, fixedonly fitted nooffset
	predict fixedSe, stdp fixedonly nooffset


*** ESTIMATE CROSSWALK FOR EF DATA FROM CHILDREN-ONLY SAMPLES TO COMMUNITY-WIDE SAMPLES ***
	generate efTemp = randomModel + ln(efTotal)
	replace efTemp = 0 if efTemp<0
	poisson efTemp childrenonly
	generate efTemp2 = efTemp
	replace  efTemp2 = efTemp / exp(_b[childrenonly]) if childrenonly==1


*** RUN META-ANALYSIS OF GLOBAL MEAN EF ***
	metan efTemp2 randomModelSe, wgt(casesM) nograph
	generate random   = `r(ES)'
	generate randomSe = `r(seES)'




*** RUN THE IND MODEL ***
	capture drop efTemp*
	menbreg casesM deathRateG  if denguePr>0, exp(sampleM) intmethod(mvaghermite) || location_id:

	predict randomModelTmp, reffects reses(randomModelSeTmp) nooffset
	predict fixedTmp, fixedonly fitted nooffset
	predict fixedSeTmp, stdp fixedonly nooffset


*** ESTIMATE CROSSWALK FOR EF DATA FROM CHILDREN-ONLY SAMPLES TO COMMUNITY-WIDE SAMPLES ***
	generate efTemp = randomModelTmp + ln(efTotal)
	replace efTemp = 0 if efTemp<0
	poisson efTemp childrenonly
	generate efTemp2 = efTemp
	replace  efTemp2 = efTemp / exp(_b[childrenonly]) if childrenonly==1


*** RUN META-ANALYSIS OF GLOBAL MEAN EF ***
	metan efTemp2 randomModelSeTmp, wgt(casesM) nograph
	replace random   = `r(ES)' if strmatch(ihme_loc_id, "IND*") | ihme_loc_id=="AUS" | strmatch(ihme_loc_id, "IDN*")  | strmatch(ihme_loc_id, "USA*")
	replace randomSe = `r(seES)'  if strmatch(ihme_loc_id, "IND*") | ihme_loc_id=="AUS" | strmatch(ihme_loc_id, "IDN*") | strmatch(ihme_loc_id, "USA*")

	replace fixed = fixedTmp  if strmatch(ihme_loc_id, "IND*") | ihme_loc_id=="AUS" | strmatch(ihme_loc_id, "IDN*") | strmatch(ihme_loc_id, "USA*")
	replace fixedSe = fixedSeTmp  if strmatch(ihme_loc_id, "IND*") | ihme_loc_id=="AUS" | strmatch(ihme_loc_id, "IDN*") | strmatch(ihme_loc_id, "USA*")
	replace randomModel = randomModelTmp  if strmatch(ihme_loc_id, "IND*") | ihme_loc_id=="AUS" | strmatch(ihme_loc_id, "IDN*") | strmatch(ihme_loc_id, "USA*")
	replace randomModelSe = randomModelSeTmp  if strmatch(ihme_loc_id, "IND*") | ihme_loc_id=="AUS" | strmatch(ihme_loc_id, "IDN*") | strmatch(ihme_loc_id, "USA*")


*** PRODUCE ROUGH GLOBAL CASE ESTIMATES FOR MODEL TESTING ***
	capture drop efMean
	fastrowmean ef_*, mean_var_name(efMean)

	capture drop testCases testInc index
	bysort location_id year_id: gen index = _n
	generate testInc = exp(fixed + random)  * efMean
	replace  testInc = 0 if endemic==0
	generate testCases = testInc * population
	tabstat testCases if is_estimate==1 & index==1, by(year_id) stat(n mean sum)
	sum testInc if is_estimate==1 & year_id>=1990, d
	sum testInc if is_estimate==1 & inlist(year_id, 1990, 1995,2000, 2005, 2010, 2015, 2016), d
	tabstat testInc if is_estimate==1 & index==1, by(year_id) stat(n mean sd min p25 p50 p75 p90 max)

	tabdisp ihme_loc_id year_id if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2015) & is_estimate==1, cellvar(testInc)
	*/


/******************************************************************************\
                 SUBMIT LOCATION-SPECIFIC MODEL PROCESSING JOBS
\******************************************************************************/

	keep if is_estimate==1 | location_type=="admin0"
	quietly levelsof location_id, local(locations) clean
	save `inc'


*** CREATE LOCATION-SPECIFIC AGE-DISTRIBUTION FILES ***
	get_demographics, gbd_team(epi) clear

	use FILEPATH, clear
	keep if inlist(year_id, `=subinstr("`r(year_ids)'", " ", ",", .)') & inlist(age_group_id, `=subinstr("`r(age_group_ids)'", " ", ",", .)')
	duplicates drop location_id year_id age_group_id sex_id population incCurve, force

	foreach location of local locations {
		quietly {
			preserve
			keep if location_id==`location'
			save `rootDir'/temp/ageSpecific_`location'.dta, replace
			restore
			}
		di "." _continue
		}



*** CREATE LOCATION-SPECIFIC INCIDENCE ESTIMATE FILES & SUBMIT PARALLEL PROCESS JOBS ***
	use `inc', clear

	foreach location of local locations {
		quietly {
			preserve
			keep if location_id==`location'
			save `rootDir'/temp/inc_`location'.dta, replace
			restore

			! qsub -P ADDRESS -pe multi_slot 8 -N dengue_`location' "FILEPATH" "`location'"
			}
		}



 */

/******************************************************************************\
                         SAVE THE RESULTS (OPTIONAL)
\******************************************************************************/


*** CREATE DATASET OF LOCATIONS TO MARK COMPLETION STATUS ***
	keep location_id
	duplicates drop
	generate complete = 0
	levelsof location_id, local(locations) clean

*** GET READY TO CHECK IF ALL LOCATIONS ARE COMPLETE ***
    local pause 2
	local complete 0
	local incompleteLocations `locations'

	display _n "Checking to ensure all locations are complete" _n


*** ITERATIVELY LOOP THROUGH ALL LOCATIONS TO ASSESS PROGRESS UNTIL ALL ARE COMPLETE ***
	while `complete'==0 {

	* Are all locations complete?
	  foreach location of local incompleteLocations {
		capture confirm file FILEPATH
		if _rc == 0 quietly replace complete = 1 if location_id==`location'
		}

	  quietly count if complete==0

	  * If all locations are complete submit save results jobs
	  if `r(N)'==0 {
		local complete 1

	    display "All locations complete." _n "Processing implied EF files. "

		clear
		foreach location of local locations {
			append using `rootDir'/temp/efImplied_`location'
			}

		*NOTE This file is used in the yellow fever model as an input	
		save FILEPATH, replace

		/*
		display "All locations complete." _n "Submitting save_results. "
		local mark_best no

		run FILEPATH
		save_results, modelable_entity_id(ADDRESS) description("All acute dengue cases (using `spaceVar' `spaceVarLab' & `timeVar' `timeVarLab' `miscLab')") in_dir(`rootDir'/ADDRESS) file_pattern({location_id}.csv) mark_best(`mark_best') env("prod")
		save_results, modelable_entity_id(ADDRESS1) description("Dengue fever (using `spaceVar' `spaceVarLab' & `timeVar' `timeVarLab' `miscLab')") in_dir(`rootDir'/ADDRESS1) file_pattern({location_id}.csv) mark_best(`mark_best') env("prod")
		save_results, modelable_entity_id(ADDRESS2) description("Severe dengue fever (using `spaceVar' `spaceVarLab' & `timeVar' `timeVarLab' `miscLab')") in_dir(`rootDir'/ADDRESS2) file_pattern({location_id}.csv) mark_best(`mark_best') env("prod")
		save_results, modelable_entity_id(ADDRESS3) description("Post-dengue fatigue (using `spaceVar' `spaceVarLab' & `timeVar' `timeVarLab' `miscLab')") in_dir(`rootDir'/ADDRESS3) file_pattern({location_id}.csv) mark_best(`mark_best') env("prod")
		*/
		}


	  * If all locations are not complete, inform the user and pause before checking again
	  else {
	    quietly levelsof location_id if complete==0, local(incompleteLocations) clean
	    display "The following locations remain incomplete:" _n _col(3) "`incompleteLocations'" _n "Pausing for `pause' minutes" _continue

		forvalues sleep = 1/`=`pause'*6' {
		  sleep 10000
		  if mod(`sleep',6)==0 di "`=`sleep'/6'" _continue
		  else di "." _continue
		  }
		di _n

		}
	  }

