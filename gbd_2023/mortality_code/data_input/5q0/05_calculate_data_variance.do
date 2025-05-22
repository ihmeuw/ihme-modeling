** Purpose:        This code produces gpr_5q0_input.txt which is the fundamental input dataset for the 5q0 GPR process and prediction  models
**                    This file contains the raw data estimates for 5q0 as well as the standard errors (calculated here) and a dummy for VR

** set up stata
clear
capture cleartmp
capture restore, not
capture log close
macro drop all
set more off
set memory 1000m

** Set locals
global j "FILEPATH"
global version_id = "`1'"
global code_dir = "`2'"

set odbcmgr unixodbc

global version_dir = "FILEPATH"
global input_dir = "FILEPATH"
global output_dir = "FILEPATH"

di "$version_id"


** Prep file for data variance in summary birth histories
	** this file was generated from FILEPATH/extract_sbh_method_pv.do
	** now moved to Stash: /child-mortality/extract_sbh_method_pv.do
	** The do file combines performance and coverage files in FILEPATH
	
	insheet using "FILEPATH", clear
	keep if type == "out" & (method =="MAC" | method =="combined")
	drop if model == "ever-married"
	drop if reftime == "Overall"
	keep reftimecat method sd_res_mean
	gen loweryear = substr(reftime,1,2)
	destring loweryear, replace
	expand 2
	sort method loweryear
	bys method reftime: replace loweryear = loweryear + 1 if _n == 2
	drop reftime
	rename loweryear reftime
	replace sd_res_mean = sd_res_mean/1000
	rename sd_res_mean sdq5_sbh

	gen type = "indirect" if method == "combined"
	replace type = "indirect, MAC only" if method == "MAC"

	tempfile indirect_sd
	save `indirect_sd', replace

** prep under-5 population from the standard mortality group population file
	insheet using  "FILEPATH", clear
	rename year_id year

	gen temp_loc_id = location_id
	tostring temp_loc_id, replace
	replace ihme_loc_id = temp_loc_id if ihme_loc_id==""
	drop temp_loc_id

	keep if age_group_id <= 5 & age_group_id >=2 & sex_id == 3
	collapse (sum) pop, by(ihme_loc_id year)
	rename pop pop0to4
	keep ihme_loc_id year pop0to4
	sort ihme_loc_id year

	tempfile u5pop
	save `u5pop', replace

	** create Andhra Pradesh Old under 5 pops
	keep if ihme_loc_id == "IND_4841" | ihme_loc_id == "IND_4871"
	collapse (sum) pop, by(year)
	gen ihme_loc_id = "IND_44849"
	tempfile ap_old_pop
	save `ap_old_pop', replace

	append using `u5pop'
	save `u5pop', replace

** bring in 5q0 database (vr data has already been adjusted for completeness)
** TEMP_DIR
insheet using "FILEPATH", clear
capture gen biasvar = ""
keep location_id ihme_loc_id year ldi maternal_edu hiv data category corr_code_bias to_correct vr mort mort2 mse pred1b resid pred2resid pred2final ptid source1 re2 adjre_fe reference log10sdq5 biasvar varstqx source_year source type sourcetype graphingsource ctr_re super_region_name region_name

** Assure that only years past 1950 are included in the model
	assert year >= 1950

** format CBH standard error variable
	destring vr, replace force
	destring log10sdq5, replace force
	rename log10sdq5 log10sdq5_cbh

** merge in under-5 population
	gen tempyear = year
	replace year = floor(year)
	replace source_year = year if source_year == .
	#replace source_year = string(year) if source_year == "" | source_year == "."
	merge m:1 ihme_loc_id year using `u5pop'
	drop if _merge == 2
	drop _merge

** Population Adjustments
	replace pop0to4 = pop0to4*.006 if (substr(ihme_loc_id,1,3) == "IND") & (source == "srs" | source == "india srs")
	replace pop0to4 = pop0to4*.003 if ihme_loc_id == "BGD" & regexm(source, "srs") != 0
	replace pop0to4 = pop0to4*.01 if ihme_loc_id == "PAK" & regexm(source, "srs") != 0
	preserve
		insheet using "FILEPATH", clear
		cap rename iso3 ihme_loc_id
		keep if ihme_loc_id == "CHN"
		replace ihme_loc_id = "CHN_44533" if ihme == "CHN"
		keep year prop sourcename ihme_loc_id
		rename sourcename source
		replace source = lower(source)
		tempfile dsp_pop
		save `dsp_pop', replace
	restore
	replace source = "dsp" if strpos(source, "dsp") != 0 & ihme_loc_id == "CHN_44533"
	merge m:1 year source ihme_loc_id using `dsp_pop'
	drop if _m == 2
	drop _m
	replace pop0to4 = pop0to4*prop if ihme_loc_id == "CHN_44533" & strpos(source,"dsp") != 0 & prop != .
	drop prop
** CHN Fertility Survey
	replace pop0to4 = pop0to4*.001 if ihme_loc_id == "CHN_44533" & source == "1 per 1000 survey on fertility and birth control"
** CHN Population Change Survey
	replace pop0to4 = pop0to4*.001 if ihme_loc_id == "CHN_44533" & source == "1 per 1000 survey on pop change"
** CHN 1% Intra-Census Survey
	replace pop0to4 = pop0to4*.01 if ihme_loc_id == "CHN_44533" & source == "1% intra-census survey"
** China province DSP population
	preserve
		use "FILEPATH", clear
		keep year u5pop iso3
		gen source = "china dsp"
		tempfile dsp_prov_pop
		save `dsp_prov_pop', replace
		use "FILEPATH", clear
		merge 1:m iso3 using `dsp_prov_pop'
		drop if _m == 1
		drop _m
		drop iso3
		save `dsp_prov_pop', replace
	restore
	merge m:1 year ihme_loc_id using `dsp_prov_pop'
	replace pop0to4 = u5pop if strpos(source, "dsp") & ihme_loc_id != "CHN_44533"
	drop if _m == 2
	drop _m u5pop

	** go back to half years
	drop year
	rename tempyear year

	** merge the indirect data variance information in preparation for stderr calcs below
	** first need to calculation reftime for the individual datapoints
	destring source_year, replace force
	gen reftime = source_year - year
	replace reftime = 0 if reftime <1
	replace reftime = round(reftime)
	replace reftime = 25 if reftime > 25 & type == "indirect"
	replace reftime = 21 if reftime > 21 & type == "indirect, MAC only"

	** now merging in the indirect std errors
	merge m:1 type reftime using `indirect_sd'
	drop _m
	drop if ihme_loc_id == ""

	expand 3 if ihme_loc_id == "PRK"
	destring mort, replace force
	bysort ihme_loc_id mort: replace mort = mort*1.25 if _n == 2 & ihme_loc_id == "PRK"
	bysort ihme_loc_id: replace mort = mort*0.75 if _n == 3 & ihme_loc_id == "PRK"

** need countries with 0 as 5q0 estimate to be non-zero for the log transformation
	replace mort = .0001 if mort <= 0

*************************
** calculate standard errors
*************************
** first need to convert 5q0 data into Mx space (Binomial distribution)
** the conversion eqn is 5q0 = 1 - e^(-5*5m0) -- solve for 5m0 to get conversion below
	gen m5 = (log(-mort+1))/(-5)

** calculate data variance for 5m0
	gen variance = (m5*(1-m5))/pop0to4 if strpos(cat,"vr") != 0 | cat == "census"

	** convert back to qx space using the delta method
	** here the equation is 5q0' = 5*(exp(-5*5m0))^2
	** need to multiply converted qx*var(mx) to get var(qx)
	replace variance = variance *((5*exp(-5*m5))^2)

	** add in variance for the indirects
	replace variance = sdq5_sbh^2 if sdq5_sbh != .

	** add in log10 standard errors from CBH
	gen log10_var = log10sdq5_cbh^2

	** delta method - convert from log10(q5) variance to q5 variance
	replace variance = log10_var*(mort*log(10))^2 if log10_var != .

	replace variance = . if strpos(source, "rapidmortality")

** *************************************************************************************
** add in variance from complete birth histories to summary birth histories according to the following
** 1. CBH variance from the same survey
** 2. Average CBH variance from the same survey series within a country
** 3. Average CBH variance from the same series within (A) region, (B) super-region, (C) global
** 4. For one-off surveys: Average CBH variance from the same source-type within (A) region, (B) super-region, (C) global
** 5. Average CBH variance of everything else

preserve

	quietly do "$code_dir/SBH_variance_addition.do"
	tempfile sbh_all
	save `sbh_all', replace

restore

merge m:m ihme_loc_id sourcetype super_region_name region_name source source_year graphingsource using `sbh_all'
replace variance = var_tot if var_tot != . & regexm(sourcetype, "SBH")
drop var_tot _m add_var

*******************************************************************************************************************

	** generate stderrs for next part
	gen stderr = sqrt(variance)

****************************
** fill in missing standard errors
****************************
	** track which of the below modifications are made using mod_stderror
	gen mod_stderror = "0. stderror directly calculated using births/ cbh or sbh systems" if stderr != . & category != "other"

	** take max stderror from the non-VR points and use this as the standard error for data points missing a stderror (by country)
	bysort location_id: egen maxstderr = max(stderr) if vr == 0

	bysort location_id: egen maxstderr1 = max(maxstderr)
	replace maxstderr = maxstderr1 if maxstderr == .
	drop maxstderr1

	gen changed = 1 if stderr == .
	replace stderr = maxstderr if stderr == .
	replace mod_stderror = "1. max by country non-VR" if stderr == maxstderr & changed == 1
	drop maxstderr changed

	bysort region_name: egen maxstderr = max(stderr) if vr == 0
	gen changed = 1 if stderr == .
	replace stderr = maxstderr if stderr == .
	replace mod_stderror = "2. max by region non-VR" if stderr == maxstderr & changed == 1
	drop maxstderr changed

   sort ihme_loc_id source1 year

****************************
** Add variance from all survey RE standard deviation by source.type
****************************
	replace variance = stderr ^2

**  add in variance by source.type in qx not log10qx space
	destring varstqx, replace force
	replace variance = variance + varstqx if (category != "vr_unbiased" | (category == "vr_unbiased" & inlist(ihme_loc_id, "DZA", "PHL", "TKM", "ECU")))

	replace variance = variance + varstqx if strpos(source, "rapidmortality")
	replace variance = 0.5*variance if strpos(source, "rapidmortality")

	cap assert variance != . if data == 1
	if (_rc) {
		replace variance = .000005 if variance == . & data == 1
	}
	replace stderr = sqrt(variance)

****************************
** Formatting
****************************

** consolidate the 2 dhs categories (dhs direct and dhs indirect) into a single source category
	replace cat = "dhs" if strpos(cat,"dhs") != 0

** add in papfam in as a source
	replace category = "papfam" if regexm(source, "papfam") | regexm(source, "papchild")

** assert that no observations are missing a mortality estimate
	preserve
	keep if data == 1 & mort == .
	saveold "FILEPATH", replace
	restore

	assert mort != . if data == 1
	sort ihme_loc_id category year

** final formatting
	gen filler = "NA"
	order ihme_loc_id year mort stderr category region_name filler ptid

	** standard logit
	gen logit_mort = log(mort/(1-mort))
	gen logit_var = variance * (1/(mort*(1-mort)))^2
	replace data = 0 if (1/(mort*(1-mort)))==0 & data == 1

	assert variance != . if data == 1

** save final prepped file for GPR process
outsheet super_region_name region_name location_id ihme_loc_id year ldi maternal_edu hiv data category vr mort mort2 logit_mort mse pred1b resid pred2resid pred2final adjre_fe ptid source1 reference log10sdq5 biasvar variance logit_var using "FILEPATH", comma replace

** close log
	capture log close
