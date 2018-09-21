/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER  
Output:           Extrapolate GBD2013 Prevalence Draws to Estimate Prevalence for GBD2016
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "J:"
	}
	

* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2016"
	*model step
	local step 02
	*local root
	local localRoot "FILEPATH"
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*local temporary directory for other things:
	local local_tmp_dir "FILEPATH"
	*directory for output of draws > on ihme/scratch
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

	*directory for standard code files
	adopath + FILEPATH

	** Set locals from arguments passed on to job
	local location "`1'"
	local set "`2'"

	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/extrapolate_log_`set'_`location'_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir


/*====================================================================
                        1: Get GBD Info
====================================================================*/


*--------------------1.1: Demographics

	*Get list of ages/sexes/years for oncho demographics
		use "`tmp_dir'/`set'_preextrapolated_draws_`location'.dta", clear
			levelsof age_group_id,local(onchoages) c
			levelsof sex_id,local(onchosexes)  c
			levelsof year_id,local(onchoyears) sep(,) c
			levelsof outvar,local(outcomes) c

	*Get list of gbd demographics
		get_demographics, gbd_team("epi") clear
			local gbdages="`r(age_group_ids)'"
			local gbdyears="`r(year_ids)'"
			local gbdsexes="`r(sex_ids)'"

*--------------------1.2: Population

	get_population, location_id("`location'") sex_id("`onchosexes'") age_group_id("`gbdages'") year_id("`gbdyears'") clear
		tempfile `set'_pop
		save ``set'_pop', replace	

	* Create population total with all gbd ages + pop totals for odd age groups (28 & 21)
		*Limit to the years present in oncho data to facilitate data merge
			keep if inlist(year_id,`onchoyears')

		*Sum population across age groups included in these composite age groups
			replace age_group_id=28 if inlist(age_group_id,2,3,4)
			replace age_group_id=21 if inlist(age_group_id ,30,31,32,235)
			collapse (sum) population,by (age_group_id year_id sex_id location_id)
			keep if age_group_id==21 | age_group_id==28
			tempfile collapsedpop
			save `collapsedpop', replace
		
		*Append population for these odd age groups to other gbd age groups
			use ``set'_pop', clear
			drop if age_group_id==21 | age_group_id==28
				*drop just in case the database actually did produce these - drop and recalculate
			append using `collapsedpop'
			
		*Save complete population table
			tempfile allpop
			save `allpop', replace
	

/*====================================================================
                        2: Extrapolate to New Age Groups (Prevalence-Space)
====================================================================*/

*--------------------2.1: Convert to Prevalence Space, Prep

	*Merge case draws with population by age-sex
		use "`tmp_dir'/`set'_preextrapolated_draws_`location'.dta", clear
		merge m:1 location_id year_id age_group_id sex_id using `allpop', nogen

	*Calculate prevalence
		forval x=0/999{
			replace cases`x'=cases`x'/population
			rename cases`x' prevalence`x'
		}

	*Duplicate rows of age groups to be extrapolated
		drop if inlist(age_group_id,2,3,4,30,31,32,235) // missing prevlaence, only have population
		expand 4 if age_group_id==21 //age 80+	
		expand 3 if age_group_id==28 //age <1yo
		bysort outvar location_id year_id age_group_id sex_id: gen id=_n

*--------------------2.2: Assume all prevalence below post neonatal is 0 (age definition consistent with GBD2015)

	*Replace the value of the age group
		replace age_group_id=4 if age_group_id==28
		replace age_group_id=3 if age_group_id==4 & id==2
		replace age_group_id=2 if age_group_id==4 & id==3
		forval x=0/999{
			replace prevalence`x'=0 if inlist(age_group_id,2,3)
		}

*--------------------2.3: Assume prevalence for age>80 is same as all age groups (30,31,32,235)

	*Replace the value of the age group
		replace age_group_id=30 if age_group_id==21 & id==1
		replace age_group_id=31 if age_group_id==21 & id==2
		replace age_group_id=32 if age_group_id==21 & id==3
		replace age_group_id=235 if age_group_id==21 & id==4
		
*--------------------2.4: Format and Save
		
		drop if inlist(age_group_id,21,28)
		cap drop id population
		tempfile agefilled
		save `agefilled', replace	
	
/*====================================================================
                        3: Interpolate/Extrapolate to All Years (Prevalence-Space)
====================================================================*/

	use `agefilled', clear
		
*--------------------3.1: Prepare interpolation/extrapolation

	*Make empty rows to fill for 2016
		preserve
			keep if year_id==2013
			forval x=0/999{
				replace prevalence`x'=.
			}
			replace year_id=2016
			tempfile 2016
			save `2016', replace
		restore
		
	*Append new empty rows for 2016
		append using `2016'

	*Set cross-section to be location-age-sex - this value will be interpolated across years
		egen panel = group(location_id age_group_id sex_id outvar)
		tsset panel year_id

	*Fill dataset so there are empty rows for all years
		tsfill, full
		
	*Fill repeated variable values other than prevlance down columns
		bysort panel: egen pansex = max(sex_id)
		bysort panel: egen panloc = max(location_id)
		bysort panel: egen panage = max(age_group_id)
		replace sex_id = pansex
		drop pansex
		replace age_group_id=panage
		drop panage
		replace location_id=panloc
		drop panloc
		bysort panel : replace outvar = outvar[1]
		
*--------------------3.2: Interpolate/Extrapolate

	*Use iploate/epolate
		forvalues i=0/999 {
			bysort panel: ipolate prevalence`i' year_id, gen(draw_`i') epolate
			quietly replace draw_`i' = 0 if draw_`i' < 0
		}

*--------------------3.3: Format for GBD 2016

	*Limit to GBD years, format, save
		egen OKyear=anymatch(year_id),values("`gbdyears'")
		drop if OKyear==0
	
	*Merge with population and convert back to case space
		merge m:1 location_id year_id age_group_id sex_id using `pop', nogen
			*expect 100% merge
			
		forval i=0/999{
			replace draw_`i'=draw_`i'*population
		}
	
	*SAVE
		tempfile filledyear
		save `filledyear', replace
		
/*====================================================================
                        4: Add Missing Uncertainty (Case-Space)
====================================================================*/

*--------------------4.1: Add uncertainty to OCP draws

		* METHODS PER LOC COFFENG (GBD 2010 Expert Group, Former IHME Researcher):
		// Within each draw, multiply the number of cases with visual impairment by a random value,
		// which is defined as the exponent of a normally distributed variable with mean zero and sd 0.1.
		// Use the function rnormal (with mean 0 and sd 0.1) to create the random value and exponentiate it.
		// Within a draw, apply the same randomly drawn value to all country-year-sex-age. This step adds
		// some uncertainty to these estimates (relative sd +/-20%). Do the same for blindness; don't do this
		// for the other sequelae, as these already have uncertainty quantified.

		*1 value of rando per draw - applied to all values within draw for appropriate outcome variables
		
		if "`set'"=="ocp"{
		
			gen rando=.
			local visimpair vicases blindcases
			foreach impair in `visimpair'{
				forvalues i=0/999{
					quietly local rando = rnormal(0,0.1) 
					replace rando=`rando'
					replace draw_`i'=draw_`i'*exp(rando) if outvar=="`impair'"
				}
			}
			
			drop rando

			tempfile ocp_adjusted
			save `ocp_adjusted', replace
			
		}
		
		else {
			tempfile apoc_adjusted
			save `apoc_adjusted', replace
		}


/*====================================================================
                        5: Perform Visual Impairment Split (Case-Space)
====================================================================*/

*--------------------5.1: Split visual impairment cases into moderate and severe

	*Split the cases of visual impairment into moderate and severe cases
		* The fraction of moderate cases should be .8365775  (standard error .0030551)
		* Generate random values using the rnormal function. Within each draw, apply the same randomly drawn fraction to all country-year-sex-age. 
		
		gen vis_rando=.
		expand 2 if outvar=="vicases",gen(new)
		replace outvar="vis_mod" if new==1
		drop new
		expand 2 if outvar=="vicases",gen(new)
		replace outvar="vis_sev" if new==1
		drop new
		
		forvalues i=0/999{
			quietly local vis_rando = rnormal(.8365775, .0030551) 
			replace vis_rando=`vis_rando'
					
			replace draw_`i'= vis_rando * draw_`i' if outvar=="vis_mod"
			replace draw_`i'= (1-vis_rando) * draw_`i' if outvar=="vis_sev"
		}
				
		drop vis_rando panel
		
	*Calculate prevalence
		forval i=0/999{
			replace draw_`i'=draw_`i'/population
			*SET MINIMUM AGE LIMITS BY ZERO-ING DRAWS
			replace draw_`i'=0 if inlist(age_group_id,2,3)
		}
		
		tempfile prefix
		save `prefix',replace



/*====================================================================
                        6: Adjust All Uncertainty per GBD2016 (Prevalence-Space)
====================================================================*/

*--------------------6.1: Prep Locals and SEs

		levelsof outvar,local(outcomes) c
		local parent mfcases
		local outcomes: list parent | outcomes
			*do this so that mfcases comes first
		
		tempfile adjusted
		local t 1

		foreach year in `gbdyears' {
		forvalues sex = 1/2 {
		foreach outcome in `outcomes' {
			
		use `prefix', clear
		keep if sex_id==`sex'
		keep if outvar=="`outcome'"
		keep if year_id==`year'

		*parent				1494	mfcases
		*disfigure_pain_1	1495	osdcases1acute
		*disfigure_1		2620	osdcases1chron
		*disfigure_pain_2	1496	osdcases2acute
		*disfigure_pain_3	2515	osdcases3acute
		*disfigure_3		2621	osdcases3chron
		*vision mod			1497	vis_mod
		*vision sev			1498	vis_sev
		*blind				1499	blindcases


	*Set standard errors of additional errors that we want to include in the draws
		local nodmf_sd = 0.261236 
			** // for predictions at higher geographical level (10-20 villages)
		local trend_sd = 0.0262011
			** // sd of time trend in mf prevalence during MDA at higher geographical level (10-20 villages)

		if ("`outcome'" == "mfcases" | "`outcome'" == "osdcases1acute" | "`outcome'" == "osdcases1chron" | "`outcome'" == "osdcases2acute" | "`outcome'" == "osdcases3acute" | "`outcome'" == "osdcases3chron") {
			local grouping = "cases" 
		}
		if ("`outcome'" == "vis_mod" | "`outcome'" == "vis_sev") {
			local grouping = "_vision_low" 
		}
		if "`outcome'" == "blindcases" {
			local grouping = "_vision_blind"
		}

*--------------------6.2: Add Uncertaity for Mf->Nodule Converstion
	
	*Transform prevalences to logit plane
		forvalues i = 0/999 {
			replace draw_`i' = logit(draw_`i')
		}

	*Add uncertainty due to nod-mf conversion for OCP countries
	*		//if inlist("`iso'","BEN","BFA","CIV","GHA","GIN","GNB","MLI","NER","SEN") | inlist("`iso'","SLE","TGO") {
	*		For GBD 2015, use corresponding lcoation_ids:
		if inlist(location_id,200,201,205,207,208,209,211,213,216) | inlist(location_id,217,218) {		
			forvalues i = 0/999 {
				local z = rnormal()
				replace draw_`i' = draw_`i' + `z' * `nodmf_sd'
			}
		}

	*Calculate mean and sd of draws, and if draws are mf prev, save sd for using with other vars
		egen double mean_draw = rowmean(draw_*)
		egen double sd_draw = rowsd(draw_*)
		if "`outcome'" == "mfcases" {
			preserve
				rename sd_draw sd_draw_mf
				keep age_group_id sd_draw
				tempfile sd_mf_`location'_`year'_`sex'
				save `sd_mf_`location'_`year'_`sex'', replace
			restore
		}
		
	*Normalize draws
		forvalues i = 0/999 {
			replace draw_`i' = (draw_`i' - mean_draw)/sd_draw
		}

	*Add nod-mf conversion uncertainty (reset sd of draws if var is mf-prev; adjust sd if other var)
		merge m:1 age_group_id using `sd_mf_`location'_`year'_`sex'', keepusing(sd_draw_mf) nogen
		replace sd_draw = sqrt(sd_draw^2 - sd_draw_mf^2 + `nodmf_sd'^2)
		replace sd_draw = `nodmf_sd' if sd_draw < `nodmf_sd'

*--------------------6.3: Add Uncertainty for time trend

	*Set year when MDA with ivermectin started
		local start_control 1990		
		**  //"MWI"
			if `location' == 182 {
				local start_control = 1997
			}
		**  //"TCD","NER","TZA"
			if inlist(`location' ,204,213,189) {
				local start_control = 1998
			}
		**  //"CMR","CAF","GNQ","LBR","NGA","UGA"
			if inlist(`location' ,202,169,172,210,214,190) {
				local start_control = 1999
			}
		**  //"COG","ETH","COD"
			if inlist(`location' ,170,179,171) {
				local start_control = 2001
			}
		**  //"AGO","BDI","SSD"
			if inlist(`location' ,168,175,435) {
				local start_control = 2005
			}
            
	*Add time trend uncertainty
		replace sd_draw = sqrt(sd_draw^2 + ((`year'-`start_control') * `trend_sd')^2)

	*Re-expand normalized draws to location and adjusted scale.
		forvalues i = 0/999 {
			replace draw_`i' = (draw_`i' * sd_draw) + mean_draw
			replace draw_`i' = 1 / (1 + exp(-draw_`i'))
			replace draw_`i' = 0 if missing(draw_`i')
		}

		drop mean_draw sd_draw
		cap drop sd_draw_mf
				
	*Save and append
		if `t'>1 append using `adjusted'
		save `adjusted', replace
		local ++t
	
	}
	}
	}


/*====================================================================
                        7: Export Files
====================================================================*/

	** Prepare draws file for export
	
		foreach outcome in `outcomes' {
		
		di as error "Output draws for `outcome'"
		
		quietly{
		use `adjusted', clear
		keep if outvar=="`outcome'"
		
		* Create needed varibles
			gen measure_id=5
			gen modelable_entity_id=.

			*_parent
				replace modelable_entity_id=1494 if "`outcome'"=="mfcases"

			*disfigure_pain_1
				replace modelable_entity_id=1495 if "`outcome'"=="osdcases1acute"

			*oncho disfigure_1
				replace modelable_entity_id=2620 if "`outcome'"=="osdcases1chron"

			*oncho disfigure_pain_3
				replace modelable_entity_id=2515 if "`outcome'"=="osdcases3acute"
				
			*disfigure_3
				replace modelable_entity_id=2621 if "`outcome'"=="osdcases3chron"

			*disfigure_pain_2
				replace modelable_entity_id=1496 if "`outcome'"=="osdcases2acute"
				
			*vision_mod
				replace modelable_entity_id=1497 if "`outcome'"=="vis_mod"
				
			*vis_sev
				replace modelable_entity_id=1498 if "`outcome'"=="vis_sev"
				
			*vision_blind
				replace modelable_entity_id=1499 if "`outcome'"=="blindcases"
					
		* Format structure of output
			keep age_group_id location_id year_id sex_id modelable_entity_id draw* measure_id
		
		*Output
		
			if modelable_entity_id~=.{
				levelsof modelable_entity_id,local(meid) c
				outsheet using "`out_dir'/`meid'_prev/`location'.csv", comma replace
				di as error "Success - meid `meid'"
				sleep 30
			}
			if modelable_entity_id==.{
				di as error "Outcome not modelled"
			}
	}
	}
	


***************************

file open progress using `progress_dir'/location.txt, text write replace
file write progress "complete"
file close progress


log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.
2.
3.


Version Control:


