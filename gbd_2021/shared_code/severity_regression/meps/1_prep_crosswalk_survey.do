** PREP THE DATA FROM THE GBD SURVEY IN ORDER TO CROSSWALK FROM SF-12 SCORES TO GBD DISABILITY WEIGHTS


// open data file
	insheet using "./FILEPATH.csv", comma names clear

// keep just mcs and pcs info (a
	keep   memberloginname   scorepcs scoremcs 
	rename memberloginname 	 id
	rename scorepcs			 pcs
	rename scoremcs			 mcs
	gen    composite = pcs + mcs // analysis is done on composit score

// merge loginids (how we identified which health state they responded to) to get lay decriptions associated with them
	merge m:1 id using "./FILEPATH/lay_descriptions.dta", nogen

// merge on disability weights
	rename conditiondescription laydescriptions
	merge m:1 laydescriptions using "./FILEPATH/desc_id_merge.dta", keep(1 3) nogen //  _m=2 are just unused descriptions, we dont need them
	merge m:1 hhseqid using  "./FILEPATH", keep(1 3) nogen //  _m=2 are just unused descriptions, we dont need them

// calc MAD foreach DW - drop outliers (defined in this case as anything greater than 2 MADS for each health state)
	levelsof hhseqid, local(dws) // split data into Health States
	gen median = .
	foreach dw of local dws {
		summ composite if  hhseqid == `dw', d 
		replace median = `r(p50)' if  hhseqid == `dw'
	}
	gen deviation = abs( composite- median)
	gen MAD = .
	foreach dw of local dws {
		summ deviation if  hhseqid == `dw', d 
		replace MAD = `r(p50)' if  hhseqid == `dw'
	}
	gen outlier = 0
	replace outlier = 1 if deviation > (2*MAD)
	drop if outlier == 1

// for the crosswalk we eventually use the 'mean' across all obs per health state, this is calculated as the random effect assigned to that state in a regression without predictors
	xtmixed composite || hhseqid:
	predict composite_hat_2 , fitted

	set scheme s1color
	
// the scatter below shows the output of this prediction with the data
	scatter mean composite, msize(small) msymbol(O) mcolor(gray*.7) xtitle(Composite SF-12) ytitle(Disability Weight) || scatter mean composite_hat_2, msize(small) msymbol(Oh) legend(off)

// rename and keep only whats needed
	keep composite_hat_2 mean_dw 
	rename composite_hat_2 sf 
	rename mean_dw dw
	drop if sf == .
	
// the resulting file will be used in R to estimate the DW for each SF-12 score in survey datasets. 
	save "./FILEPATH/1_crosswalk_data.dta", replace


	
	
	