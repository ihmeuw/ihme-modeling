
*** BOILERPLATE ***
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		}
	
*** LOAD FUNCTIONS ***

	adopath + FILEPATH 
	adopath + FILEPATH/stata
	

	tempfile uf locationMeta age_groups fullPop inc gr
	
	
	

*** GET LOCATION METADATA ***	
	get_location_metadata, location_set_id(35) clear
	keep location_id ihme_loc_id
	save `locationMeta'


*** PROCESS UNDERREPORTING DATA ***
	use "FILEPATH/vl_underreporting.dta", clear
	keep iso3 uf_*
	egen ufMean = rowmean(uf_*)
	save `uf', replace
	
	
*** GET GEOGRAPHIC RESTRICTIONS ***	
	import delimited using "FILEPATH/leish_v_cc.csv", clear
	drop cause_id 
	drop if year_id<1980
	save `gr', replace	
	
	
*** GET & PROCESS INCIDENCE DATA *** 	
	get_epi_data, bundle_id(53) clear
	replace measure = "incidence" if USERNAME==1
	keep if measure=="incidence"
	
	generate caseTemp = mean * effective_sample_size
	assert !missing(effective_sample_size)
	assert !missing(mean)
	assert !missing(caseTemp)
	assert abs(cases - caseTemp)<=1 if !missing(cases)
	replace cases = caseTemp if missing(cases)
	assert !missing(cases)
	
	drop ihme_loc_id
	merge m:1 location_id using `locationMeta', assert(2 3) keep(3) nogenerate
	generate countryIso = substr(ihme_loc_id, 1, 3)
	generate iso3 = substr(ihme_loc_id, 1, 3)
	
	
	gen isGR =  nid==281876
	bysort isGR: gen index = _n if isGR==0
	sum index if isGR==0
	local count = `r(max)'
		
	generate year_id = floor((year_start + year_end) / 2)
	capture drop sex_id
	generate sex_id = (sex=="Male") + ((sex=="Female") * 2) + ((sex=="Both") * 3)
	
	
	
*** COLLAPSE TO ALL-AGE, BOTH-SEX DATA ***	
	gen allAge = age_start==0 & age_end>70
	bysort location_id year_id nid sex allAge: egen maxAge = max(age_end)
	bysort location_id year_id nid sex allAge: egen minAge = min(age_start)
	bysort location_id year_id nid sex allAge (age_start): gen ageGap = age_start[_n+1] - age_end
	gen complete = minAge==0 & maxAge>70 & (inrange(ageGap, 0, 2) | missing(ageGap))
	drop if complete==0 & allAge==0
	
	replace sample_size = effective_sample_size if missing(sample_size)
	replace cases = sample_size * mean if missing(cases)

	collapse (sum) cases sample_size, by(location_id year_id year_start year_end sex sex_id isGR cv_* is_outlier field_citation_value source_type nid)

	bysort year_start year_end location_id: egen anyMale = max(sex=="Male")
	bysort year_start year_end location_id: egen anyFemale = max(sex=="Female")
	bysort year_start year_end location_id: egen anyBoth = max(sex=="Both")
	
	drop if anyBoth==1 & sex!="Both"
	
	collapse (sum) cases sample_size, by(location_id year_id year_start year_end isGR cv_* is_outlier field_citation_value source_type nid)
	drop if year_id<1980
	generate mean = cases / sample_size
	
	drop if location_id==6 & year_start==2004 & year_end==2012 //this is an aggregate that duplicates year-specific data points
	
	
	merge m:1 location_id using `locationMeta', assert(2 3) keep(3) nogenerate
	generate countryIso = substr(ihme_loc_id, 1, 3)
	generate iso3 = substr(ihme_loc_id, 1, 3)
	
	  

*** MERGE INCIDENCE & UNDERREPORTING DATA ***	
	merge m:1 iso3 using `uf', keep(1 3) nogenerate
		

	
*** ADJUST INCIDENCE DATA FOR UNDERREPORTING ***	
	assert !missing(cases)
	assert !missing(sample_size)
	assert !missing(mean)
	
	generate betaCases  = cases 
	generate betaSample = sample_size
	
	replace  betaCases  = mean * 0.999e+8 if betaSample>0.999e+8
	replace  betaSample = 0.999e+8 if betaSample>0.999e+8
	replace  betaCases  = betaCases + 1.1e-4

	forvalues i = 0 / 999 {
		quietly {
			replace uf_`i' = uf_`i' / ufMean  if (missing(uf_`i') & nid==281876) | inlist(nid, 118120)
			
			generate gammaA = rgamma(betaCases, 1)
			generate gammaB = rgamma((betaSample - betaCases), 1)
			
			generate inc_`i' = uf_`i' * gammaA / (gammaA + gammaB) if nid!=281876
			replace  inc_`i' = 0 if nid==281876
			
			gen inc10k_`i' = inc_`i' * 10000
			
			noisily assert !missing(inc_`i') & inc_`i'>=0
			drop gammaA gammaB
			
			}
		
		di "." _continue
		}
	
	replace ufMean = 1 if  inlist(nid, 118120, 281876)
	gen data = mean * ufMean
	egen variance = rowsd(inc_*)
	replace variance = variance^2
	
	gen data10k = mean * ufMean * 10000
	egen variance10k = rowsd(inc10k_*)
	replace variance10k = variance10k^2
	
	keep year_id location_id nid data* sample_size variance* is_outlier
		
		

*** MERGE IN LOCATION METADATA, AND GEOGRAPHIC RESTRICTIONS ***	
	merge m:1 location_id using `locationMeta', assert(2 3) keep(3) nogenerate
	merge m:1 location_id year_id using `gr', gen(grMerge)

	gen endemic = grMerge==1	
	replace endemic=0 if strmatch(ihme_loc_id, "GBR*")
	replace endemic=1 if inlist(location_id, 43874, 43875, 43882, 43883, 43884, 43885, 43886, 43890, 43899, 43900, 43904, 43905, 43906, 43910, 43911, 43918, 43919, 43920, 43921, 43922, 43926, 43935, 43936, 43940, 43941, 43942)

	

	
*** MARK OUTLIERS ***	
	replace is_outlier = 0 if missing(is_outlier)
	replace is_outlier = 1 if ihme_loc_id=="SSD" & year_id==1989
	replace is_outlier = 1 if ihme_loc_id=="TUR" & inlist(year_id, 1995, 1996)


*** CLEAN UP AND FORMAT FOR ST-GPR ***	
	replace nid = 281876 if endemic==0 & missing(nid)
	replace data = 0 if nid==281876
	replace data10k = 0 if  nid==281876
	replace variance = 0.0001^2 if  nid==281876
	replace variance10k = 0.0001^2 if nid==281876
	
	keep if !missing(data)
	keep year_id location_id data* sample_size variance* nid 

	generate me_name = "ntd_vl"
	generate age_group_id = 22
	generate sex_id = 3

	drop data variance
	rename *10k *
	export delimited using FILEPATH/vlModellingData_4.csv, replace
	
	drop if nid==281876
	export delimited using FILEPATH/vlModellingData_5.csv, replace

