**Extract and transform Alcohol_LPC to be useable as a covariate
**By Max Griswold
**2/10/15

clear all
set more off


** Setup to run on either cluster or local machine
	if c(os) == "Unix" {
		set more off
		global prefix 
		set 
	}
	else if c(os) == "Windows" {
		global prefix 
	}

	local pre_clean ""

* Create complete country-year template 
	tempfile template
	quietly adopath + ""
	quietly get_demographics, gbd_team("cov") make_template clear
	save `template'

	get_populations, year_id($year_ids) location_id($location_ids) sex_id(1 2) age_group_id(8 9 10 11 12 13 14 15 16 17 18 19 20 21) clear
	bysort location_id year_id: egen total_pop= sum(pop_scaled)
	replace pop_scaled = total_pop
	duplicates drop location_id year_id, force
	drop sex_id age_group_id total_pop

	merge 1:m location_id year_id using `template', nogen

	save `template', replace

	quietly include ""
	get_location_metadata, location_set_id(9) clear
	keep if level >= 3	
	merge 1:m location_id using `template', keep(2 3) nogen

	replace ihme_loc_id="CHN_44533" if location_id==44533
	duplicates drop location_id year_id, force
	keep location_id location_name ihme_loc_id year_id super_region_id super_region_name region_id region_name pop_scaled
	
	rename ihme_loc_id iso3
	save `template', replace

	clear
	tempfile loop

	**Expand through 1970s
	forvalues i = 1(1)10{
		use `template', clear
		keep if year_id == 1980
		drop pop_scaled
		local year = 1980-`i'
		replace year_id = `year'
		save `loop', replace

		get_populations , year_id(`year') location_id($location_ids) sex_id(1 2) age_group_id(8 9 10 11 12 13 14 15 16 17 18 19 20 21) clear
		bysort location_id year_id: egen total_pop = sum(pop_scaled)
		replace pop_scaled = total_pop
		duplicates drop location_id year_id, force
		drop sex_id age_group_id total_pop

		merge 1:1 location_id year_id using `loop', nogen
		save `loop', replace

		use `template', clear
		append using `loop'
		save `template', replace
	}

** Include education data for spacetime
	use "", clear
	keep if educ_25plus != .
	keep iso3 location_id year sex educ_25plus
	bysort location_id year: egen mean_educ_25plus = mean(educ_25plus)
	drop if sex==1
	drop sex educ_25plus
	rename mean_educ_25plus educ_25plus
	rename year year_id

	tempfile education
	save `education'

** Read WHO data 
	use "", clear
	rename (lpc year country) (WHO_alcohol_lpc year_id location_name)
	drop if year_id < 1970
	keep if beveragetypes=="All types"

	duplicates tag location_name year_id, gen(dup)
	drop if dup>=1 & WHO_alcohol_lpc==.
	duplicates drop year_id location_name, force
	drop dup
	tempfile who

	replace WHO_alcohol_lpc = . if WHO_alcohol_lpc==0
	save `who', replace

	**Add on iso3
	import delimited "", clear 
	rename (displaystring iso) (location_name iso3)
	keep location_name iso3

	merge 1:m location_name using `who', keep(3) nogen
	keep location_name iso3 year_id WHO_alcohol_lpc
	save `who', replace

**Read FAO data
	use "", clear
	rename (country year) (countryname year_id)

** Merge ISO3 identifiers
	merge m:1 countryname using "", keep(3) nogen

** Convert to kilograms
	replace value = . if value==0
	gen value_kg = value * 1000 * 1000

** Calculate value per capita
	merge m:1 iso3 year_id using `template', keep(2 3) nogen
	gen value_kg_pc = value_kg / pop_scaled
	drop if item=="Beverages, Fermented"

** Calculate densities for different alcoholic beverages (assume density of ethanol is 0.789 kg per liter, water is 1.000 kg per liter)
	** Wine
	local den_wine = 0.13*0.789 + 0.87
	** Beer
	local den_beer = 0.05*0.789 + 0.95
	**Liquor
	local den_alcoholic = 0.4*0.789 + 0.6
	
** Convert to liters and mulitply by % alcohol to get Liters pure alcohol per capita
	gen FAO_alcohol_lpc = value_kg_pc / `den_wine' * 0.13 if item=="Wine"
	replace FAO_alcohol_lpc = value_kg_pc / `den_beer' * 0.05 if item=="Beer"
	replace FAO_alcohol_lpc = value_kg_pc / `den_alcoholic' * 0.40 if item=="Beverages, Alcoholic"

	
** Combine the quantities of the different types of alcohol to
	collapse (sum) FAO_alcohol_lpc, by(iso3 year location_id)
	
	drop if iso3==""
	tempfile fao
	save `fao'

**Average FAO data in 5 year windows to generate more consistent trends

tsset location_id year_id
tssmooth ma smoothed_fao_alcohol_lpc = FAO_alcohol_lpc, window(5 1 0)
replace smoothed=. if FAO_alcohol==0

*Start crosswalk between WHO and FAO 
merge 1:1 iso3 year using `who', nogen
merge m:1 iso3 year using `template', keep(3 4 5) nogen update

replace smoothed_fao_alcohol_lpc = . if smoothed_fao_alcohol_lpc == 0
replace WHO_alcohol_lpc= . if WHO_alcohol_lpc == 0

egen average = rmean(WHO_alcohol_lpc smoothed_fao_alcohol_lpc)
gen ln_average = ln(average)

	**Make dummy variables
gen FAO_data = 0
gen WHO_data = 0
gen source = "Average"

tempfile placeholder
tempfile merged
save `merged'
save `placeholder'

replace ln_average = ln(smoothed_fao_alcohol_lpc)
replace FAO_data = 1
replace WHO_data = .
replace source = "FAO"

append using `merged'
save `merged', replace

use `placeholder', clear
replace ln_average = ln(WHO_alcohol_lpc)
replace WHO_data = 1
replace FAO_data = .
replace source = "WHO"

**Outlier some points
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & region_id==199
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & region_id==56
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & region_id==104 & iso3 !="BMU" & iso3 != "CUB" & iso3 != "DOM" & iso3 != "HTI" & iso3 != "JAM" & iso3 != "GRD"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3 == "BMU" & year_id <= 1981
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="FRA"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="AUS"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="DEU"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="ARG"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="IRL"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="ITA"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="LUX"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="CHL"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="VEN"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="PRY"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="IND"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="ARE"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="RWA"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="TZA"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="UGA"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="ZMB"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="BWA"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="LSO"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="SWZ"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="ZWE"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="SVN"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="SVK"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="MKD"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="CZE"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="HRV"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="BIH"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="KAZ"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="PRT"
	replace smoothed_fao_alcohol_lpc = . if source=="FAO" & iso3=="ESP"

	replace WHO_alcohol_lpc = . if source=="WHO" & iso3=="GAB"
	replace WHO_alcohol_lpc = . if source=="WHO" & iso3=="KOR"
	replace WHO_alcohol_lpc = . if source=="WHO" & iso3== "GRD"

append using `merged'

	*Run Mixed effect model, store predicted betas,
	mixed ln_average FAO_data || super_region_id: FAO_data|| region_id: FAO_data || location_id: FAO_data || year_id: FAO_data
	predict re*, reffect
	gen adjust_factor_fao = exp(re1+re3)
	gen adjust_fao = adjust_factor_fao*smoothed_fao_alcohol_lpc
	replace adjust_fao = . if smoothed_fao_alcohol_lpc==.
	drop re1-re8

	mixed ln_average WHO_data|| super_region_id: WHO_data || region_id: WHO_data || location_id: WHO_data || year_id: WHO_data
	predict re*, reffect
	gen adjust_factor_who = exp(re1+re3)
	gen adjust_who = adjust_factor_who*WHO_alcohol_lpc
	replace adjust_who = . if WHO_alcohol_lpc==.
	drop re1-re8

*Replace with adjusted values

	drop if source == "Average"

	replace ln_average = adjust_fao if source=="FAO"
	replace ln_average = adjust_who if source=="WHO"
	rename ln_average data

	tempfile pre_adjust
	save `pre_adjust', replace


**Use lowess estimates as mean to generate variance
	lowess data year, by(location_id) gen(lowess_hat) nograph 


	levelsof(source), local(sources)

	tempfile tmp
	save `tmp'

	gen residual = lowess_hat - data
	gen sd = .

	foreach year of numlist 1970/2015 {
		bysort location_id: egen temp = sd(residual) if inrange(year_id, `year'-5, `year'+5)
		replace sd = temp if year_id == `year'
		drop temp
	}	

	foreach year of numlist 1970/1975 {
		bysort location_id: egen temp = sd(residual) if inrange(year_id, 1970, `year'+10-(`year'-1970))
		replace sd = temp if year_id == `year'
		drop temp
	}

	foreach year of numlist 2010/2015 {
		bysort location_id: egen temp = sd(residual) if inrange(year_id, `year'-10+(2015-`year'), 2015)
		replace sd = temp if year_id == `year'
		drop temp
	}

replace sd = . if data==.

*Final bookkeeping things to prepare to run in ST-GPR
	gen me_name = "drugs_alcohol_lpc"
	gen sex_id=3
	gen age_group_id=22
	gen sample_size=.
	gen variance = sd^2
	rename iso3 ihme_loc_id
	rename sd standard_deviation

	gen nid = 200195 if source=="FAO" 
	replace nid = 238452 if source=="WHO"
	replace nid = 238445 if source=="WHO" & year_id>=1960 & year_id<=1979 & data != .
	replace nid = 238448 if source=="WHO" & year_id>=1980 & year_id<=1999 & data != .
	replace nid = 238452 if source=="WHO" & year_id>=2000 & data != .


save `template', replace

quietly get_covariate_estimates, covariate_name_short("cigarettes_pc") clear
keep location_id year_id mean_value
rename mean_value cigarettes_pc

merge 1:m location_id year_id using `template', nogen keep(3)
save `template', replace

quietly get_covariate_estimates, covariate_name_short("ldi_pc") clear
keep location_id year_id mean_value
rename mean_value ldi_pc
gen ln_ldi_pc = ln(ldi_pc)

merge 1:m location_id year_id using `template', nogen keep(3)
save `template', replace

merge m:1 location_id year_id using `education', nogen keep(1 3)

drop if ihme==""

order ihme year source data smoothed_fao WHO_alcohol adjust_factor_fao adjust_factor_who
sort ihme source year


save "", replace
