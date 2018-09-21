	
** **************************************************************************
** PREP STATA
** **************************************************************************
	
	// prep stata
	clear all
	set more off
	set maxvar 32000

	// Set OS flexibility 
	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
		local h "~"
	}
	else if c(os) == "Windows" {
		local j "J:"
		local h "H:"
	}
	sysdir set PLUS "`h'/ado/plus"

	// parse incoming syntax elements
	cap program drop parse_syntax
	program define parse_syntax
		syntax, root_dir(string) date(string) out_dir(string)
		c_local out_dir "`out_dir'"
		c_local date "`date'"
		c_local root_dir "`root_dir'"
	end
	parse_syntax, `0'

** **************************************************************************
** run model. 
** **************************************************************************
	capture mkdir "MKDIR"

	insheet using "`root_dir'/inputs/model_neonatal_deaths_OR.csv", clear
	split citation, parse(_)
	gen name = citation1+ "_" + citation2
	drop citation*
	order name, after(study_id)
	order country, last
	drop note year treatment dxic country

// expand to get a micro dataset
	gen id=_n
	gen dth0 = n - dth
	drop n
	rename dth dth1
	reshape long dth,i(id) j(d_stat)
	drop id
	gen id=_n
	local tot = _N
	forvalues x=1/`tot' {
		expand dth[`x'] if id==`x'
	}
	drop id

// get the neonatal death rates attributed to syphilis by treatment status 
	xi: xtmelogit d_stat i.rx  || study_id:

// 1000 draws for uncertainty
	matrix m = e(b)'
	matrix m = m[1..(rowsof(m)-1),1]
	local covars: rownames m
	local num_covars: word count `covars'
	local betas
	forvalues j = 1/`num_covars' {
		local this_covar: word `j' of `covars'
		local covar_rename=subinstr("`this_covar'","_I","",.)
		local covar_rename=subinstr("`covar_rename'","_","",.)
		local betas `betas' b_`covar_rename'
	}

	matrix C = e(V)
	matrix C = C[1..(colsof(C)-1), 1..(rowsof(C)-1)]
	local tot = _N
	drawnorm `betas', means(m) cov(C)
	compress
	forvalues j=1/1000 {
		qui gen draw`=`j'-1' = b_cons[`j'] + b_rx1[`j']*_Irx_1 + b_rx2[`j']*_Irx_2 + b_rx3[`j']*_Irx_3 
	}
	keep rx draw*
	keep  if rx<3
	bysort rx: gen idn = _n
	keep if idn==1
	drop idn
	gen id_resh=_n
	reshape long draw,i(id_resh) j(bs)
	drop bs id_resh
	bysort rx: gen id_resh= _n
	reshape wide draw,i(id_resh) j(rx) 
	gen nn_dth_rate0= invlogit(draw1) - invlogit(draw0)
	gen nn_dth_rate1= invlogit(draw2) - invlogit(draw0)
	keep nn_dth_rate*
	gen id=_n - 1

	// x=0 is no treated
	// x=1 is inadequately treated 
	reshape long nn_dth_rate , i(id) j(treatment)
	reshape wide nn_dth_rate , i(treatment) j(id)
	
	export delimited using "`out_dir'/models/nn_death_by_treatment.csv", replace
	save "`out_dir'/models/nn_death_by_treatment.dta", replace

	