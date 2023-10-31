* Purpose: This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
* Notes: Generate prevalence of CL cases, based on incidence as predicted by DisMod and duration from literature

*** PREP STATA ***
	clear all
	set more off
	
	
*** SET UP DIRECTORIES ***
	local in_dir FILEPATH/inputs
    local code_dir FILEPATH/code
	
	capture mkdir "`tmp_in_dir'"
	
	
	

*** LOAD UNDERREPORTING DATA ***  
	/* Bring in country-specific underreporting factors as assigned by Alvar et al (PLoS Negl Trop Dis 2012).
	(N.B. the figures in the excel source file are color-coded to indicate whether they were originally
	assigned by Alvar et al or by USER, because Alvar et al hadn't assigned any value to a specific country.) */
	
	clear
    tempfile shape
    save `shape', emptyok
    
    import excel using "`in_dir'/underreporting_factors_alvar_2012.xlsx", firstrow clear
    keep iso3 gbd_analytical_region_name cl*
	
*** CONVERT UNDERREPORTING FACTORS TO PROPORTION REPORTED ***      
	generate prop_hi = 1 / cl_underreporting_lo if cl_underreporting_lo!=1
	generate prop_lo = 1 / cl_underreporting_hi if cl_underreporting_hi!=1
	drop cl_underreporting_*

	tempfile uf_temp
	save `uf_temp', replace      


*** ESTIMATE PARAMETERS FOR A BETA DISTRIBUTION THAT CORRESPOND TO THE UPPER AND LOWER BOUNDS ***	

	* LOAD FUNCION TO SOLVE FOR BETA PARAMETERS (USED IN NL FUNCTION BELOW) *
  /* Generate shape parameters for a beta distributions with 2.5 and 97.5 percentiles equal to one divided
	by upper and lower underreporting factors for CL, respectively. One divided by the underreporting factor
	represents the fraction of the true total that is reported in the data. N.B.: lower and upper bounds for
	underreporting factors may not have the same value! */
	include "`code_dir'/01c_estimate_beta_shape.do"
	
	keep prop*
	duplicates drop

	sort *lo *hi
	drop if missing(prop_lo, prop_hi)

	generate double alpha = .
	generate double beta = .

	preserve
	forvalues i = 1/`=_N' {
	  restore, preserve
		keep if _n == `i'
		expand 2, gen(y)
		gsort -y
		
		nl faq @ y, parameters(alpha beta) initial(alpha 2 beta 2)

		replace alpha = [alpha]_b[_cons]
		replace beta = [beta]_b[_cons]
		
		keep if y==1
		drop y
		append using `shape'
		save `shape', replace
	}

	restore, not


*** MERGE ORIGINAL UNDERREPORTING FACTOR DATASET WITH BETA DISTRIBUTION PARAMETER ESTIMATES ***
	use `uf_temp', clear
	merge m:1 prop_lo prop_hi using `shape', keepusing(alpha beta) nogen

	keep iso3 gbd_analytical_region_name alpha beta
	keep if length(iso3) == 3
	sort iso3
	
	
	forvalues i = 0/999 {
		quietly generate uf_`i' = 1 / rbeta(alpha,beta)
		quietly replace uf_`i' = uf_`i' * 3 if iso3 == "SSD" 
		quietly replace uf_`i' = 1 if missing(uf_`i') | gbd_analytical_region_name=="Western Europe"
		}

		
	
  
save	"FILEPATH/cl_underreporting2.dta", replace
