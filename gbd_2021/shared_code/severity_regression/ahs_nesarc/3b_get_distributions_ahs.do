
// Get 1000 draws of distributions

cap restore, not

clear all
set mem 300m
set maxvar 30000
set more off


foreach mo in 1 12 {

	// make sure aus means have been compiled from the bootstrap
	insheet using "./FILEPATH/${causename}.csv", comma clear

	// start distributions analysis
	keep if run == 1

	keep code severity hhseqid cause ahs`mo'

	// drop if not tracking in aus 1mo
	drop if ahs`mo' == ""

	// pull 1000 draws from files for each hhseqid
	destring hhseqid, force replace

	preserve
	insheet using "$gbd_dws", clear
	keep hhseqid meandw1- meandw1000
	keep hhseqid meandw1- meandw1000
	tempfile weights
	save `weights', replace
	restore 

	merge m:1 hhseqid using `weights', keep(match master) nogenerate

	// get the mean weight for reporitng later on
	egen mean_DW = rowmean(meandw*)

	// get max number of severities for each cause
	bysort cause: egen maxsev = max(severity)

	// for causes with more than one severity, make a new line for asymptomatic
	expand 2 if severity == 1 & maxsev > 1, gen(asymp)
	replace asymp = 1 if severity == 1 & maxsev == 1 // for those with only one weight we only get asymp
	replace asymp = 0 if asymp == .
	replace severity = 0 if asymp == 1

	// get 1000 draws of the midpoints between draws
	forvalues d = 1/1000 {
		gen 		MID`d'a = 0 if asymp == 1
		gen 		MID`d'b = 0 if asymp == 1
	}

	gen str causetrim = substr(subinstr(cause," ","",.),1,10) // "
	replace causetrim = subinstr(causetrim,"'","",.) // "
	replace causetrim = subinstr(causetrim,"-","",.) // "
	levelsof causetrim, local(list)
	foreach cause of local list {
		preserve
		keep if causetrim == "`cause'"
		count if asymp == 0
		if `r(N)' != 0 {
			sort severity 
			forvalues d = 1/1000 {
				replace     MID`d'a = 0 							  if severity == 1
				replace     MID`d'b = (meandw`d'[2] + meandw`d'[3])/2 if severity == 1
				
				replace     MID`d'a = (meandw`d'[2] + meandw`d'[3])/2 if severity == 2		
				replace     MID`d'b = (meandw`d'[3] + meandw`d'[4])/2 if severity == 2	

				replace     MID`d'a = (meandw`d'[3] + meandw`d'[4])/2 if severity == 3		
				replace     MID`d'b = (meandw`d'[4] + meandw`d'[5])/2 if severity == 3
				
				replace     MID`d'a = (meandw`d'[4] + meandw`d'[5])/2 if severity == 4		
				replace     MID`d'b = (meandw`d'[5] + meandw`d'[6])/2 if severity == 4
				
				replace     MID`d'b = (meandw`d'[5] + meandw`d'[6])/2 if severity == 4
				replace     MID`d'b = (meandw`d'[6] + meandw`d'[7])/2 if severity == 4
				
				replace MID`d'b = 1 if severity == maxsev
			}
		}	
		tempfile `cause' 
		save ``cause'', replace
		restore	
	}
	clear
	gen cause =""
	foreach cause of local list {
		append using ``cause''
	}

	// drop 1000 draws of DW, dont need them anymore 
	drop meandw1-meandw1000

	// make 1000 distribution variables
	forvalues i = 1/1000 {	
		gen dist`i' = .
	}

	// make maxsev 0 if only 1 dw. 
	replace maxsev = 0 if maxsev == 1

	local count = 1
	count if severity == 0
	local num `r(N)'
	local num = `num'*1000

	tempfile pre
	save `pre', replace
	// we now have severity cutoffs for each condition, now for each draw of each condition, we have to get the dristibution out of meps. 
	foreach cause of local list {
		use `pre', clear

		keep if causetrim == "`cause'"
		local c = ahs`mo' // aus1 variable name
		forvalues i = 1/1000 {
			local pctcompl = round(((`count'/6000)*100),.001)
			di in red "computing `cause' distribution `i'/1000"
			di in red "`pctcompl'% complete"
			
			// get severity cutoffs
			sort severity
			local maxsev = maxsev
			local cutoffasymp = 0
			
			// reset cutoffs each time
			forvalues s = 1/5 {
				local cutoff`s'a 
				local cutoff`s'b
			}
			
			if maxsev >= 1 {
				local cutoff1a = MID`i'a[2]
				local cutoff1b = MID`i'b[2]		
			}	
			if maxsev >= 2 {
				local cutoff2a = MID`i'a[3]
				local cutoff2b = MID`i'b[3]		
			}			
			if maxsev >= 3 {
				local cutoff3a = MID`i'a[4]
				local cutoff3b = MID`i'b[4]		
			}			
			if maxsev >= 4 {
				local cutoff4a = MID`i'a[5]
				local cutoff4b = MID`i'b[5]		
			}			
			if maxsev == 5 {
				local cutoff5a = MID`i'a[6]
				local cutoff5b = MID`i'b[6]		
			}			
			
			// now go into the bootstrapped draws with the cutoffs and grab the distribution. 
			preserve
			use "./FILEPATH//`i'//`c'",clear
			count
			local total = `r(N)'
			
			gen DW_diff_data = 1-((1-dw_hat)/(1-dw_s))
			count if DW_diff_data < 0
			local pct0 = `r(N)'/`total'
			
			local j 1
			while `j' <= `maxsev' {
				count  if DW_diff_data >= `cutoff`j'a' & DW_diff_data < `cutoff`j'b'
				local pct`j' = `r(N)'/`total'
				local j = `j' + 1

			}
			
			restore
			
			// fill in the distribution amounts
			local j 0 
			while `j' <= `maxsev' {
				replace dist`i' = `pct`j'' if severity == `j'
				local j = `j' + 1
			}
		
		local count = `count' + 1
			
		}	
		
		tempfile `cause' 
		save ``cause'', replace
	}

	clear
	gen cause = ""
	foreach cause of local list {
		append using ``cause''
	}

	// reporting
	egen dist_mean = rowmean(dist*)
	egen dist_lci  = rowpctile(dist*), p(2.5)
	egen dist_uci  = rowpctile(dist*), p(97.5)

	rename mean_DW ahs`mo'mo_DW

	save "./FILEPATH${date}.dta", replace
	save "./FILEPATH.dta", replace
	
	keep code cause severity hhseqid ahs`mo'mo_DW dist_* 
	order code cause severity hhseqid ahs`mo'mo_DW dist_mean dist_lci dist_uci 
	rename dist* ahs`mo'modist*

	tostring hhseqid, replace force
	replace hhseqid = "?" if hhseqid == "."
	replace hhseqid = "." if hhseqid == "?"

	sort cause severity
	replace ahs`mo'mo_DW = 0 if severity == 0 
	outsheet using "./FILEPATH${date}.csv", comma replace
	outsheet using "./FILEPATH.csv", comma replace
}

