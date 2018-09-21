// USERNAME
// Takes the summary data (ME==3943, bundle==450) with all cirrhosis etiology proportions and splits data into four bundles (Hep B, Hep C, EtOH, Other) 

// Import data
	import excel using  "FILEPATH", clear firstrow case(preserve)

// Clean up data
	drop if inlist(nid, 210673, 215243)

	replace case_name = "A" if case_name=="AB" & cases==20 & nid==210729

	replace age_start=38 if nid==210723
	replace age_end=65 if nid==210723

	drop if nid==210739 & case_name=="BO"
	replace cases = 74 if nid==210739 & case_name=="B"
	foreach var of varlist mean upper lower standard_error {
	  replace `var' = . if nid==210739 & case_name=="B"
	  }
	  
	replace location_id = 67 if nid==210739
	replace location_name = "Japan" if nid==210739
	capture replace ihme_loc_id = "JPN" if nid==210739
	replace site_memo = "" if nid==210739
	  
	expand 2 if nid==215245 & case_name=="A", gen(new)
	replace case_name="B" if new==1
	drop new  

// Define locals
	local cases 21 10 115 26 7
	local causes A B C AC BC
	while "`cases'"!="" {
	  gettoken case cases: cases
	  gettoken cause causes: causes
	  replace cases = `case' if nid==215245 & case_name=="`cause'"
	  }
// More clean up
	replace sample_size = 179 if nid==215245
	replace year_issue=0
	replace sex_issue=0
	replace age_issue=0

// Calculations
	replace cases = mean * sample_size if missing(cases)
	replace mean = cases / sample_size if missing(mean)

	generate nEtiologies = length(case_name)
	quietly sum nEtiologies

	forvalues i = 1/`r(max)' {
	  quietly generate etiology_`i' = substr(case_name, `i', 1)
	  }
  
	egen splitGroup = group(nid location_id year_start year_end sex age_start age_end)
	bysort splitGroup: egen maxEtiologies = max(nEtiologies)

	preserve

	keep if inlist(case_name, "A", "B", "C", "O")
	keep splitGroup case_name mean cases
	reshape wide mean cases, i(splitGroup) j(case_name) string
	tempfile mergeTemp
	save `mergeTemp'

	restore

	merge m:1 splitGroup using `mergeTemp', nogenerate assert(3)

	foreach etiology in A B C O {
	  replace cases`etiology' = strmatch(case_name, "*`etiology'*") * cases`etiology'
	  }

	generate index = _n  
	expand nEtiologies  
	bysort index: gen splitIndex = _n

	generate casesSplit = .
	generate newEtiology = ""

	forvalues i = 1/3 {
	  replace newEtiology = etiology_`i' if splitIndex==`i'
	  }
	foreach etiology in A B C O {
	  replace casesSplit = cases`etiology' if newEtiology=="`etiology'"
	  }
	egen casesDenom = rowtotal(casesA casesB casesC casesO)  
	 * replace casesSplit = 
	generate casesNew = cases * casesSplit / casesDenom
	 
	bysort splitGroup newEtiology: egen totalCases = total(casesNew)
	replace cases = totalCases
	bysort splitGroup newEtiology (case_name): gen keep = _n
	keep if keep==1

	replace modelable_entity_name = ""

	local meis 1922 1920 1921 1923
	local etiologies A B C O
	foreach name in "alcohol" "hepatitis B" "hepatitis C" "other cause" {
	  gettoken mei meis: meis
	  gettoken etiology etiologies: etiologies
	  replace modelable_entity_id = `mei' if newEtiology=="`etiology'"
	  replace modelable_entity_name = "Cirrhosis of the liver due to `name' proportion" if newEtiology=="`etiology'"
	  }

	*rename unit_type_value unit_value_as_published
	rename unit_type_value unit_type
	drop data_sheet_filepath - casesNew totalCases keep

	foreach var of varlist mean upper lower standard_error {
	  replace `var' = .
	  }
	replace case_name=""
	replace unit_value_as_published = 1
  
local meis 1922 1920 1921 1923
foreach cause in alcohol hepb hepc other {
  gettoken mei meis: meis
  export excel using "FILEPATH" if modelable_entity_id == `mei', sheet("extraction") firstrow(variables) replace
  }

* the end
