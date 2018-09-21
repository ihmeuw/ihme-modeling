** ******************************************************************* **
** Purpose: Calculate and store summary statistics for agesexsplitting code
** ******************************************************************* **

** define program name and syntax
capture program drop summary_store
program define summary_store

syntax, stage(string) currobs(integer) storevars(string) sexvar(string) frmatvar(string) im_frmatvar(string)

quietly {
	** make currobs a global rather than a local so it can be brought back into the original program
	global currobs = `currobs'

	** store statistics for all data
	mata: stage[${currobs}, 1] = "`stage'"
	foreach var of local storevars {
		quietly sum `var'
		mata: `var'[${currobs}, 1] = `r(sum)'
		global `stage'_`var' = `r(sum)'
	}
	global currobs = ${currobs} + 1

	** store statistics by sex
	capture levelsof `sexvar', local(sexes)
	foreach s of local sexes {
		mata: stage[${currobs}, 1] = "`stage'"
		mata: `sexvar'[${currobs}, 1] = `s'
		foreach var of local storevars {
			quietly sum `var' if `sexvar' == `s'
			mata: `var'[${currobs}, 1] = `r(sum)'
		}
		global currobs = ${currobs} + 1
	}
	
	** store statistics by frmat and im_frmat
	capture levelsof `frmatvar', local(frmats)
	foreach f of local frmats {
		levelsof `im_frmatvar', local(im_frmats)
		foreach i of local im_frmats {
			mata: stage[${currobs}, 1] = "`stage'"
			mata: `frmatvar'[${currobs}, 1] = `f'
			mata: `im_frmatvar'[${currobs}, 1] = `i'
			foreach var of local storevars {
				quietly sum `var' if `frmatvar' == `f' & `im_frmatvar' == `i'
				mata: `var'[${currobs}, 1] = `r(sum)'
			}
			global currobs = ${currobs} + 1
		}
	}
	
}

end


