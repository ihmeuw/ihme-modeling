

*** PREP STATA ***
	clear all
	set more off

	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		}
	
	
*** LOAD SHARED FUNCTIONS ***
	run "FILEPATH/get_demographics.ado"
    run "FILEPATH/get_covariate_estimates.ado"
	run "FILEPATH/get_location_metadata.ado"
	run "FILEPATH/get_population.ado"

	  
*** SET UP DIRECTORIES ***
	local in_dir FILEPATH
	local tmp_in_dir FILEPATH
    local code_dir FILEPATH
	

*** LOAD UNDERREPORTING DATA ***  
	/* Bring in country-specific underreporting factors as assigned by Alvar et al (PLoS Negl Trop Dis 2012). */
	
	clear
    tempfile shape
    save `shape', emptyok
    
    import excel using "`in_dir'/underreporting_factors_alvar_2012.xlsx", firstrow clear
    keep iso3 gbd_analytical_region_name vl*
    

*** CONVERT UNDERREPORTING FACTORS TO PROPORTION REPORTED ***      
	generate prop_hi = 1 / vl_underreporting_lo if vl_underreporting_lo!=1
	generate prop_lo = 1 / vl_underreporting_hi if vl_underreporting_hi!=1
	drop vl_underreporting_*

	tempfile uf_temp
	save `uf_temp', replace      


*** ESTIMATE PARAMETERS FOR A BETA DISTRIBUTION THAT CORRESPOND TO THE UPPER AND LOWER BOUNDS ***	

	* LOAD FUNCION TO SOLVE FOR BETA PARAMETERS (USED IN NL FUNCTION BELOW) *
    /* Generate shape parameters for a beta distributions */
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
		quietly replace uf_`i' = 1 if missing(uf_`i') | gbd_analytical_region_name=="Western Europe"
		}

    save "`tmp_in_dir'/vl_underreporting.dta", replace

	  

	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	
