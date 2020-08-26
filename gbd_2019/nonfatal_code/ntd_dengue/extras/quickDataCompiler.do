
tempfile paho wpro gbd2013 bySex byAge

local dataDir FILEPATH



*** APPEND GBD 2013 DATA WITH NEW PAHO AND WPRO DATASETS ***
	foreach region in paho wpro gbd2013 {
	  import delimited using FILEPATH, clear
	  generate sourceFile = "`region'"
	  save ``region'', replace
	  }
	  
	append using `paho' `wpro', force
	
	
*** CLEAN UP MISSING AGES AND SEXES AND DROP NON-SURVEILLENCE DATA SOURCES ***
	replace age_start = 0 if missing(age_start)
	replace age_end = 99 if missing(age_end)
	replace sex = "Both" if missing(sex)

	drop if source_type == "Unidentifiable" | nid==139689

	sort location_id year_start year_end sex age_start age_end

	
*** COLLAPSE SEX-SPECIFIC DATA INTO BOTH-SEX DATA POINTS WHERE POSSIBLE/NEEDED ***	
	preserve
	keep if sex!="Both"

	bysort location_id year_start year_end nid age_start age_end: gen count = _N
	bysort location_id year_start year_end nid age_start age_end (sex): gen sex1 = sex[1]
	bysort location_id year_start year_end nid age_start age_end (sex): gen sex2 = sex[2]
	keep if count==2 & sex1=="Female" & sex2=="Male"

	bysort location_id year_start year_end nid age_start age_end: egen totalCases = total(cases)
	bysort location_id year_start year_end nid age_start age_end: egen totalSampleSize = total(sample_size)

	bysort location_id year_start year_end nid age_start age_end (sex): generate toKeep = _n==1

	keep if toKeep==1

	replace cases = totalCases
	replace sample_size = totalSampleSize
	replace effective_sample_size = sample_size
	replace sex = "Both"

	replace mean = cases/sample_size
	foreach var of varlist lower upper standard_error { 
	  replace `var' = .
	  }
	  
	drop count sex1 sex2 totalCases totalSampleSize toKeep  
	save `bySex', replace

	restore

	drop if sex!="Both"
	append using `bySex'

	
*** COLLAPSE AGE-SPECIFIIC DATA INTO ALL DATA WHERE POSSIBLE/NEEDED ***	
	preserve

	keep if sex=="Both" & (age_start>0 | age_end<99)

	bysort location_id year_start year_end nid (age_start age_end): egen minAge = min(age_start)
	bysort location_id year_start year_end nid (age_start age_end): egen maxAge = max(age_end)
	bysort location_id year_start year_end nid (age_start age_end): gen ageGap = abs(age_end[_n-1] - age_start[_n])
	bysort location_id year_start year_end nid (age_start age_end): egen maxGap = max(ageGap)

	drop if minAge>15 | maxAge<99 | (maxGap>1 & !missing(maxGap))

	bysort location_id year_start year_end nid: egen totalCases = total(cases)
	bysort location_id year_start year_end nid: egen totalSampleSize = total(sample_size)

	bysort location_id year_start year_end nid (age_start age_end): generate toKeep = _n==1

	keep if toKeep==1

	replace cases = totalCases
	replace sample_size = totalSampleSize
	replace effective_sample_size = sample_size
	replace age_start = minAge
	replace age_end = maxAge

	replace mean = cases/sample_size
	foreach var of varlist lower upper standard_error {
	  replace `var' = .
	  }
	 
	drop minAge maxAge ageGap maxGap totalCases totalSampleSize toKeep   
	save `byAge', replace

	restore

	drop if age_start>0 | age_end<99

	append using `byAge'

	
*** DEAL WITH DUPLICATE DATA FOR A GIVEN COUNTRY-YEAR ***	
	drop if missing(mean) & missing(cases)
    assert !missing(cases)
	
	bysort location_id year_start (cases mean): gen toKeep = _n==_N 
	keep if toKeep==1
	drop toKeep

	drop if nid==NID
	
	save "FILEPATH", replace
