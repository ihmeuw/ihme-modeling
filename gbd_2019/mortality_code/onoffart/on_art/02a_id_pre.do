
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


if `cumulative_pre' {				
		
***************** CUMULATIVE *****************

	**** SET UP FOR BRADMOD

	use "`bradmod_dir'/tmp_cumulative.dta", clear

	// Set up folders in the bradmod folder

		cap mkdir "`dismod_dir'/HIV_IDBEST_CUMUL_0_6"
		cap mkdir "`dismod_dir'/HIV_IDBEST_CUMUL_0_12"
		cap mkdir "`dismod_dir'/HIV_IDBEST_CUMUL_0_24"
			
	// Save just the sub-saharan african sites (and the extra 2 lines at the bottom)

		keep if super=="ssa" | super=="none"
		replace super="none"
		egen cd4_cat=concat(age_lower age_upper), p(_)
		replace age_upper=50 if integrand=="mtall" & _n==_N
		tempfile africa
		save `africa', replace

	// save each of the time periods for data_in files

		// save 0-6
		use "`africa'", clear 
		keep if time_point==6 | integrand=="mtall"
		outsheet using "`dismod_dir'/HIV_IDBEST_CUMUL_0_6/data_in.csv", delim(",") replace 

		// save 0-12
		use "`africa'", clear 
		keep if time_point==12 | integrand=="mtall"
		outsheet using "`dismod_dir'/HIV_IDBEST_CUMUL_0_12/data_in.csv", delim(",") replace 

		// save 0-24
		use "`africa'", clear 
		keep if time_point==24 | integrand=="mtall"
		outsheet using "`dismod_dir'/HIV_IDBEST_CUMUL_0_24/data_in.csv", delim(",") replace 

		
	// save "draw_in" files

		foreach dur in 0_6 0_12 0_24 {
			insheet using "`dismod_dir'/HIV_IDBEST_CUMUL_`dur'/data_in.csv", clear comma names
			drop if pubmed_id==.
			duplicates drop age_lower age_upper, force
			keep age_lower age_upper
			sort age_lower age_upper

			gen integrand="incidence"
			gen meas_value=0
			gen meas_stdev="inf"
			gen subreg="none"
			gen region="none"
			gen super="none"
			gen x_sex=0
			gen sex="total"
			gen x_ones="1"
			gen time_upper=2010
			gen time_lower=1997
			gen cd4="CD4"
			gen lower_10=age_lower*10
			gen upper_10=age_upper*10
			egen row_name=concat(cd4 lower_10 upper_10), p(_)
			drop lower_10 upper_10
			drop cd4
			outsheet using "`dismod_dir'/HIV_IDBEST_CUMUL_`dur'/draw_in.csv", comma names replace
		}

	// Save plain_in, and effect_in value_in and rate_in csvs

		foreach dur in 0_6 0_12 0_24 {
			insheet using "`dismod_templates'//plain_in.csv", clear comma names	
			outsheet using "`dismod_dir'/HIV_IDBEST_CUMUL_`dur'/plain_in.csv", comma names replace
		}
		
		foreach dur in 0_6 0_12 0_24 {
			insheet using "`dismod_templates'//value_in.csv", clear comma names
			outsheet using "`dismod_dir'/HIV_IDBEST_CUMUL_`dur'/value_in.csv", comma names replace
		}
		
		
		foreach dur in 0_6 0_12 0_24 {
			insheet using "`dismod_templates'//effect_in.csv", clear comma names
			drop if inlist(name, "high", "ssa", "other")
			outsheet using "`dismod_dir'/HIV_IDBEST_CUMUL_`dur'/effect_in.csv", comma names replace
		}
		
		
		foreach dur in 0_6 0_12 0_24 {
		insheet using "`dismod_templates'//rate_in.csv", clear comma names
			drop if type=="diota" & age==50
			drop if type=="iota" & age==100
			replace age=50 if type=="chi" & age==100
			replace age=50 if type=="omega" & age==100
			replace age=50 if type=="rho" & age==100
			outsheet using "`dismod_dir'/HIV_IDBEST_CUMUL_`dur'/rate_in.csv", comma names replace
		}

}	



if `pre_conditional' {
******** CONDITIONAL **********

	***************** SET UP FOR BRAD MOD  - CONDITIONAL *****************

	use "`bradmod_dir'/tmp_conditional.dta", clear

	// Set up folders in the bradmod folder

		cap mkdir "`dismod_dir'/HIV_IDBEST_0_6"
		cap mkdir "`dismod_dir'/HIV_IDBEST_7_12"
		cap mkdir "`dismod_dir'/HIV_IDBEST_12_24"
			
	// Save just the sub-saharan african sites (and the extra 2 lines at the bottom)

		keep if super=="ssa" | super=="none"
		replace super="none"
		egen cd4_cat=concat(age_lower age_upper), p(_)
		replace age_upper=50 if integrand=="mtall" & _n==_N
		tempfile africa
		save `africa', replace

	// save each of the time periods for data_in files

		// save 0-6
		use "`africa'", clear 
		keep if time_point == 6 | integrand=="mtall"
		di in red "SR - 6 months"
		outsheet using "`dismod_dir'/HIV_IDBEST_0_6/data_in.csv", delim(",") replace 

		// save 0-12
		use "`africa'", clear 
		keep if time_point == 12 | integrand=="mtall"
		replace time_per="7_12"
		di in red "SR - 12 months"
		outsheet using "`dismod_dir'/HIV_IDBEST_7_12/data_in.csv", delim(",") replace 

		// save 0-24
		use "`africa'", clear 
		keep if time_point == 24 | integrand=="mtall"
		replace time_per="12_24"
		di in red "SR -24 months"
		outsheet using "`dismod_dir'/HIV_IDBEST_12_24/data_in.csv", delim(",") replace 

		
	// save "draw_in" files

		foreach dur in 0_6 7_12 12_24 {
			insheet using "`dismod_dir'/HIV_IDBEST_`dur'//data_in.csv", clear comma names
			drop if pubmed_id==.
			duplicates drop age_lower age_upper, force
			keep age_lower age_upper
			sort age_lower age_upper

			gen integrand="incidence"
			gen meas_value=0
			gen meas_stdev="inf"
			gen subreg="none"
			gen region="none"
			gen super="none"
			gen x_sex=0
			gen sex="total"
			gen x_ones="1"
			gen time_upper=2010
			gen time_lower=1997
			gen cd4="CD4"
			egen row_name=concat(cd4 age_lower age_upper), p(_)
			drop cd4
			outsheet using "`dismod_dir'/HIV_IDBEST_`dur'//draw_in.csv", comma names replace
		}

	// Save plain_in, and effect_in value_in and rate_in csvs

		foreach dur in 0_6 7_12 12_24 {
			insheet using "FILEPATH", clear comma names	
			outsheet using "`dismod_dir'/HIV_IDBEST_`dur'//plain_in.csv", comma names replace
		}
		
		foreach dur in 0_6 7_12 12_24 {
			insheet using "FILEPATH", clear comma names
			outsheet using "`dismod_dir'/HIV_IDBEST_`dur'//value_in.csv", comma names replace
		}
		
		
		foreach dur in 0_6 7_12 12_24 {
			insheet using "FILEPATH", clear comma names
			drop if inlist(name, "high", "ssa", "other")
			outsheet using "`dismod_dir'/HIV_IDBEST_`dur'//effect_in.csv", comma names replace
		}
		
		
		foreach dur in 0_6 7_12 12_24 {
		insheet using "FILEPATH", clear comma names
			drop if type=="diota" & age==50
			drop if type=="iota" & age==100
			replace age=50 if type=="chi" & age==100
			replace age=50 if type=="omega" & age==100
			replace age=50 if type=="rho" & age==100
			outsheet using "`dismod_dir'/HIV_IDBEST_`dur'//rate_in.csv", comma names replace
		}

}

if `post_conditional' {

	*************** POST-BRAD MOD - CONDITIONAL **********************
			
	// STEP 1: pull together the raw study-level conditional mortality rates that we input into dismod

		insheet using "`dismod_dir'/HIV_IDBEST_0_6/data_in.csv", comma names clear
		tempfile d6
		save `d6', replace
		insheet using "`dismod_dir'/HIV_IDBEST_7_12/data_in.csv", comma names clear
		tempfile d12
		save `d12', replace
		insheet using "`dismod_dir'/HIV_IDBEST_12_24/data_in.csv", comma names clear

		append using `d6' `d12'

		replace time_per="6" if time_per=="0_6"
		replace time_per="12" if time_per=="7_12"
		replace time_per="24" if time_per=="12_24"
		destring time_per, replace
		
		tempfile raw
		save `raw', replace
		
	// STEP 2: compute the mean conditional mortality rates output from dismod's 1000 draws, for each of the study-specific cd4 categories. Since I ran this ONLY for SSA there are no random effects
		foreach dur in 0_6 7_12 12_24 {
			insheet using `dismod_dir'/HIV_IDBEST_`dur'//model_draw2.csv, clear
			gen draw=_n
			keep if draw>4000
			reshape long cd4_, i(draw) j(cd4_cat) string
			reshape wide cd4_, i(cd4_cat) j(draw)
			
			rename cd4_cat cat_cd4
			
			egen mean=rowmean(cd4_*)
			egen lower=rowpctile(cd4_*), p(5)
			egen upper=rowpctile(cd4_*), p(95)
			drop cd4_*
			
			if "`dur'"=="0_6" {
				gen time_per=6 
			}
			
			if "`dur'"=="7_12" {
				gen time_per=12 
			}
			
			if "`dur'"=="12_24" {
				gen time_per=24 
			}
			
			rename cat_cd4 cd4_cat
			
			tempfile d`dur'
			save `d`dur'', replace
		}
		
		append using `d0_6' `d7_12'
		
	// STEP 3: Merge with raw data and compute the residuals for each study and identify the one(s) that are most negative
		merge 1:m cd4_cat time_per using `raw'

		// have a look at a graph to see how the fit was
		
		
		gen residual=meas_value-mean
		gen residual_pct=residual/mean
		
		sort residual_pct
		order pubmed_id time_per residual residual_pct site cohort iso3
		
		bysort pubmed_id site cohort iso3: egen has_0_6=mean(time_0_6)
		bysort pubmed_id site cohort iso3: egen has_7_12=mean(time_0_12)
		bysort pubmed_id site cohort iso3: egen has_12_24=mean(time_0_24)
		
		keep if has_0_6==1 & has_7_12==1 & has_12_24==1
		sort residual_pct
		
	// Store best sites
		cd "`store_best_dir'"
		outsheet using best_conditional.csv, comma replace
	
}	

