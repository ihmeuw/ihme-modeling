
*** BOILERPLATE ***
	clear all
	set more off

	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr ADDRESS
		}
	else if c(os) == "FILEPATH" {
		global prefix "FILEPATH"
		}




*** LOAD SHARED FUNCTIONS ***
	adopath +  "$prefix/FILEPATH"
	run  $prefix/FILEPATH.ado
	run  $prefix/FILEPATH.ado



	tempfile selection data appendTemp endemic parents

*** LOAD IN LOCATION META DATA ***
	get_location_metadata, location_set_id(35) clear
	gen country_id = word(subinstr(path_to_top_parent, ",", " ", .), 4)
	destring country_id, replace
	keep location_id parent_id path_to_top_parent country_id is_estimate location_name location_type *region* ihme_loc_id
	generate mergeId = substr(ihme_loc_id, 1, 4)
	tempfile locMeta
	save `locMeta'

*** LOAD AND PROCESS INCIDENCE ESTIMATES ***


	tempfile appendTemp selection


	import delimited using  $prefix/FILEPATH, clear
	rename best run_id
	rename iso3 mergeId
	merge 1:m mergeId using `locMeta', assert(2 3) nogenerate
	save `selection'

	levelsof run_id, local(models) clean

	local count 1

	foreach model of local models {

		import delimited using "$prefix/FILEPATH", clear
		generate run_id = `model'
		foreach var of varlist gpr* {
			capture destring `var', replace force
			}
		if `count'>1 append using `appendTemp'
		save `appendTemp', replace
		local ++count
		}

	merge m:1 location_id run_id using `selection', assert(1 3) keep(3) nogenerate

	duplicates drop location_id year_id, force

	foreach bound in mean lower upper {
		generate `bound' = gpr_`bound' / 10000
		}

	keep location_id year_id mean lower upper rake_factor
	save `appendTemp', replace




*** GET GEOGRAPHIC RESTRICTIONS ***
	import delimited using "$prefix/FILEPATH", clear
	drop cause_id
	drop if year_id<1980
	tempfile gr
	save `gr', replace


*** MERGE EVERYTHING TOGETHER ***

	use `locMeta', clear
	expand 37
	bysort location_id: gen year_id = 1979 + _n

	merge 1:1 location_id year_id using `appendTemp', assert(1 3) generate(locIncidenceMerge)
	merge 1:1 location_id year_id using `gr', gen(grMerge)

	gen endemic = grMerge==1
	replace endemic=0 if strmatch(ihme_loc_id, "GBR*")
	levelsof location_id if strmatch(ihme_loc_id, "IND_*") & grMerge==1 & (location_id<5000 | location_id==44538), sep(,) clean local(endemic)
	levelsof location_id if inlist(parent_id, `endemic'), clean sep(,) local(endemicUR)
	replace endemic=0 if strmatch(ihme_loc_id, "IND_*") & !(location_id<5000 | location_id==44538) & !inlist(location_id, `endemicUR')


	keep if is_estimate==1 | location_type=="admin0"


*** ESTIMATE SUBNATIONAL RAKING FACTORS ***
	foreach bound in mean lower upper {
		replace  `bound' = 0 if endemic==0
		}

	gen subnatIso = substr(ihme_loc_id, 1, 4) if strmatch(ihme_loc_id, "*_*") & endemic==1
	levelsof country_id if country_id!=location_id & endemic==1, clean sep(,) local(parent_ids)
	save `appendTemp', replace

	levelsof location_id if inlist(country_id, `parent_ids'), local(subLocs) clean
	levelsof year_id, local(years) clean

	get_population, year_id(`years') location_id(`subLocs') sex_id(3) age_group_id(22) clear
	keep location_id year_id population

	merge 1:m location_id year_id using `appendTemp', keep(3) nogenerate

	gen cases = mean * population * endemic

	collapse (sum) cases population, by(year_id country_id subnatIso)

	gen sub = !missing(subnatIso)
	drop subnatIso population

	reshape wide cases, i(year_id country_id) j(sub)
	gen subScaler = cases0 / cases1
	keep year_id country_id subScaler

	save `parents'

	use `appendTemp', clear
	merge m:1 country_id year_id using `parents'

	replace subScaler = 1 if missing(subScaler) | country_id==location_id


	foreach x in mean lower upper {
		replace `x'  = `x' * subScaler
		}

	keep location_id year_id mean lower upper endemic
	save "$prefix/FILEPATH", replace
