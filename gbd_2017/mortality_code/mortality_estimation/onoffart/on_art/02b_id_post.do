
clear all
set more off
if (c(os)=="Unix") {
	global root "ADDRESS"
}

if (c(os)=="Windows") {
	global root "ADDRESS"
}


// What to run?
local cumulative_pre = 1
local cumulative_post = 1
local cumulative_graph = 1

local pre_conditional = 0
local post_conditional = 0

local dismod_templates "FILEPATH"
local bradmod_dir "FILEPATH"
local store_best_dir "FILEPATH"
local graph_dir "FILEPATH"
local dismod_dir "FILEPATH"

// THIS FILE'S PURPOSE IS TO IDENTIFY A 'BEST' COUNTERFACTUAL FOR SUB-SAHARAN AFRICA

// Initialize pdfmaker
if 0==0 {
	if c(os) == "Windows" {
		global prefix "ADDRESS"
		do "FILEPATH"
	}
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		do "FILEPATH"
		set odbcmgr unixodbc
	}
}

			
if `cumulative_post' {
	*************** POST-BRAD MOD - CUMULATIVE **********************
			
	// STEP 1: pull together the raw study-level conditional mortality rates that we input into dismod

		insheet using "`dismod_dir'/HIV_IDBEST_CUMUL_0_6/data_in.csv", comma names clear
		tempfile d6
		save `d6', replace
		insheet using "`dismod_dir'/HIV_IDBEST_CUMUL_0_12/data_in.csv", comma names clear
		tempfile d12
		save `d12', replace
		insheet using "`dismod_dir'/HIV_IDBEST_CUMUL_0_24/data_in.csv", comma names clear

		append using `d6' `d12'
		
		replace age_lower=age_lower*10
		replace age_upper=age_upper*10
		
		tostring age_lower age_upper, replace
		
		egen cd4_cat2=concat(age_lower age_upper), punct(_)

		replace cd4_cat=cd4_cat2
		
		tempfile raw
		save `raw', replace
		
	// STEP 2: compute the mean conditional mortality rates output from dismod's 1000 draws, for each of the study-specific cd4 categories. Since I ran this ONLY for SSA there are no random effects
		foreach dur in 6 12 24 {
			insheet using `dismod_dir'/HIV_IDBEST_CUMUL_0_`dur'/model_draw2.csv, clear
			gen draw=_n
			keep if draw>4000
			reshape long cd4_, i(draw) j(cat_cd4) string
			reshape wide cd4_, i(cat_cd4) j(draw)
			
			gen time_point=`dur'
			
			egen mean=rowmean(cd4_*)
			egen lower=rowpctile(cd4_*), p(5)
			egen upper=rowpctile(cd4_*), p(95)
			drop cd4_*
			
			rename cat_cd4 cd4_cat
			
			tempfile d`dur'
			save `d`dur'', replace
		}
		
		use `d24', clear
		append using `d6' `d12'
		
	// STEP 3: Merge with raw data and compute the residuals for each study and identify the one(s) that are most negative
		merge 1:m cd4_cat time_point using `raw'
		drop if pubmed_id==.

		// have a look at a graph to see how the fit was
		
		gen residual=meas_value-mean
		gen residual_pct=residual/mean
		
		sort residual_pct
		order pubmed_id time_point residual residual_pct site cohort iso3
		
		sort residual_pct
		order time_point
		
	// DETERMINE WHICH IS/ARE THE BEST SITE(S)	
		drop if pubmed_id==.
		
		sort time_point meas_value
		bysort time_point: gen order_mortality=_n

		sort time_point residual
		bysort time_point: gen order_raw_resid=_n
		
		bysort site cohort iso3: egen best_raw_rank=min(order_raw)
		
		sort time_point residual_pct
		bysort time_point: gen order_pct_resid=_n
		
		bysort site cohort iso3: egen best_pct_rank=min(order_pct)
		
		order order_raw_resid order_pct_resid site time_point cd4_cat meas_value pubmed_id residual residual_pct 

		drop cd4_cat2
		split cd4_cat, parse(_)
		destring cd4_cat1, replace
		
		sort best_raw_rank iso3 site cohort time_point cd4_cat1
		replace site=cohort if site==""
		replace site=iso3 if site==""
		
	// Store best sites
		cd "`store_best_dir'"
		outsheet using best_cumulative.csv, comma replace
}



if `cumulative_graph' {
	
	gen top_performer=0
	
	replace top_performer=1 if ///
	site=="Buhera Hospitals and Health Centres" | ///
	site=="rural clinic, Mbarara" | ///
	site=="HIV/AIDS care clinic in Johannesburg" | ///
	site=="Buhera Health Centres" | ///
	site=="41 healthcare facilities in Kigali City and the western province of Rwanda" | ///
	cohort=="OPERA" | ///
	cohort=="HIMS" | ///
	cohort=="CTAC" | ///
	cohort=="Gaborone Independent" 

	destring age_lower age_upper, force replace
	gen cd4_lower=age_lower*10
	gen cd4_upper=age_upper*10
	egen cd4_point=rowmean(cd4_lower cd4_upper)
		
	sort cd4_point
	
	pdfstart using "`graph_dir'/Residuals.pdf"
	
		foreach point in  6 12 24 {
			preserve
			keep if time_point==`point'
		
			if "`point'"=="6" {
				local title_per "0-6 months"
			}
			
			if "`point'"=="12" {
				local title_per "0-12 months"
			}
			
			if "`point'"=="24" {
				local title_per "0-24 months"
			}
	
			
			twoway rarea lower upper cd4_point, color(blue*.1) || /// CI for estimate
				scatter meas_value cd4_point if  cd4_lower==cd4_upper & top_performer==1, mcol(red) || /// data points for mid point appx
				scatter meas_value cd4_point if  cd4_lower==cd4_upper & top_performer==0, mcol(black) || /// data points for mid point appx
				rcap cd4_lower cd4_upper meas_value if cd4_lower!=cd4_upper & top_performer==1, horizontal color(red) lwidth(vthin) || /// cd4 range for range data
				rcap cd4_lower cd4_upper meas_value if cd4_lower!=cd4_upper & top_performer==0, horizontal color(black) lwidth(vthin) || /// cd4 range for range data
				line mean cd4_point, lcolor(blue) /// line of mean estimate
				xtitle("CD4") ///
				ytitle("Conditional probability") ///
				ylabel( ,angle(0)) ///
				legend(off) ///
				ysc(r(0(.1).4)) ylab(0(.1).4) ///
 				legend(lab (1 "95% CI") lab(2 "Raw - median") lab(3 "Raw - cd4-specific data") lab(4 "Est conditional probability")  order(4 1 2 3)) /// 
				title("`title_super'") subtitle("`title_per'") 
				// graph export "`graph_dir'/data_`point'_`sup'.eps", replace
				
			pdfappend
			
			restore	
		
		}
	
	pdffinish
		
}

