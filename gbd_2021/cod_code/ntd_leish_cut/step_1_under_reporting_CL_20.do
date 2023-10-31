* Purpose: apply UR correction to incidence data
* Notes: reads all incidence data, applies UR, simulates uncertainty

import delimited "FILEPATH/inc20.csv", clear



*** MERGE INCIDENCE & UNDERREPORTING DATA ***
	merge m:1 iso3 using "FILEPATH/cl_underreporting.dta"

	keep if _merge==3
	generate year_id = year_start

*** ADJUST INCIDENCE DATA FOR UNDERREPORTING ***
	assert !missing(cases)
	assert !missing(sample_size)
	replace mean = cases / sample_size if missing(mean)
	assert !missing(mean)

	generate betaCases  = cases
	generate betaSample = sample_size

	replace  betaCases  = mean * 0.999e+8 if betaSample>0.999e+8
	replace  betaSample = 0.999e+8 if betaSample>0.999e+8
	replace  betaCases  = betaCases + 1.1e-4

	egen ufMean = rowmean(uf_*)

	forvalues i = 0 / 999 {
		quietly {
			generate gammaA = rgamma(betaCases, 1)
			generate gammaB = rgamma((betaSample - betaCases), 1)

			generate inc_`i' = uf_`i' * gammaA / (gammaA + gammaB)

			noisily assert !missing(inc_`i') & inc_`i'>=0
			drop gammaA gammaB

			}

		di "." _continue
		}

	gen val = mean * ufMean
	egen variance = rowsd(inc_*)
	replace variance = variance^2

	
	drop uf_* inc_* 
	

	keep if !missing(val)
	

	replace nid = "ADDRESS" if missing(nid)
	replace nid = "ADDRESS" if nid=="NA"
	
	generate me_name = "ntd_leish_cut"


	gen nonZeroVar = variance if val>0
	bysort location_id: egen meanNzVar = mean(nonZeroVar)
	assert (meanNzVar*0.1)>=variance if !missing(meanNzVar) & val==0
	replace variance =  (meanNzVar*0.1) if !missing(meanNzVar) & (meanNzVar*0.1)>=variance
	drop nonZeroVar meanNzVar

	*drop zeros
	drop if val==0
	
	rename mean orig_mean
	rename lower orig_lower
	rename upper orig_upper
	
	drop if location_id==135
	
	
	export delimited using FILEPATH/bundle_ADDRESS.csv, replace
