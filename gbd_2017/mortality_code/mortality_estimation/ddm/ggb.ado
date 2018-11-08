// This applies the Generalized Growth Balance method

	
cap program drop ggb
program define ggb

syntax, data(string) 

clear
set mem 500m
set more off
use "`data'", clear

forvalues j = 0/100 {
	levelsof agegroup`j', clean local(ag)

	if("`ag'" ~= "") {
		local maxnum = `j'
	}
}

local mplus = `maxnum'+1

forvalues j = `mplus'/100 {
	drop c1_`j' c2_`j' vr_`j' agegroup`j'
}

local maxnumminus = `maxnum'-1



quietly {

forvalues j = 0(1)`maxnum' {
	g pop1aplus_`j' = 0
	forvalues k = `j'(1)`maxnum' {
		// Fast row sum or mean?
		replace pop1aplus_`j' = pop1aplus_`j' + c1_`k' if c1_`k' ~= .
	}
}


forvalues j = 0(1)`maxnum' {
	g pop2aplus_`j' = 0
	forvalues k = `j'(1)`maxnum' {
		replace pop2aplus_`j' = pop2aplus_`j' + c2_`k' if c2_`k' ~= .
	}
}


forvalues j = 0(1)`maxnum' {
	g deathsaplus_`j' = 0

	forvalues k = `j'(1)`maxnum' {
		replace deathsaplus_`j' = deathsaplus_`j' + vr_`k' if vr_`k' ~= .
	}
}

di in red "Generating interval spacing"

gen openend = .
forvalues j = 0/`maxnumminus' {
	local jplus = `j'+1
	replace openend = agegroup`j' if vr_`jplus' == . & openend == .
}
replace openend = agegroup`maxnum' if openend == .


forvalues i = 1/`maxnum' {

	qui {
		gen interval_back`i' = .
		gen interval_ahead`i' = .
		gen agegroup_back`i' = .
		gen agegroup_ahead`i' = .
		
		local iminus = `i' - 1
		local iplus = `i' + 1
		
		forvalues j = 0/`iminus' {

			replace interval_back`i' = `j' if (agegroup`i' - agegroup`j' >= time) &  agegroup`i' != . & agegroup`j' != . & (time <= openend - agegroup`i')

			replace interval_back`i' = `j' if (agegroup`i' - agegroup`j'  <= openend - agegroup`i') &  agegroup`i' != . & agegroup`j' != . & (openend - agegroup`i' < time) & interval_back`i' == . 
			
			replace agegroup_back`i' = agegroup`j' if (agegroup`i' - agegroup`j' >= time) &  agegroup`i' != . & agegroup`j' != . & (time <= openend - agegroup`i')
			replace agegroup_back`i' = agegroup`j' if (agegroup`i' - agegroup`j'  <= openend - agegroup`i') &  agegroup`i' != . & agegroup`j' != . & (openend - agegroup`i' < time) & agegroup_back`i' == . 
		}
		replace interval_back`i' = 0 if interval_back`i' == . & agegroup`i' < time
		replace agegroup_back`i' = 0 if agegroup_back`i' == . & agegroup`i' < time
				
		replace interval_back`i' = `iminus' if interval_back`i' == . & agegroup`i' != . & agegroup`iminus' != . & agegroup`i' != openend // Presumably, only missing if the agegroup interval is always larger than time (i.e. if census is only one year apart)
		replace agegroup_back`i' = agegroup`iminus' if agegroup_back`i' == . & agegroup`i' != . & agegroup`iminus' != . & agegroup`i' != openend
		
		forvalues j = `i' / `maxnum' {
			replace agegroup_ahead`i' = agegroup`j' if (agegroup`j' - agegroup`i' < time) & (agegroup`j' - agegroup`i' < agegroup`i') & agegroup`j' != openend & agegroup`i' != . & agegroup`j' != . 
			replace interval_ahead`i' = `j' if (agegroup`j' - agegroup`i' < time) & (agegroup`j' - agegroup`i' < agegroup`i') & agegroup`j' != openend & agegroup`i' != . & agegroup`j' != . 
		}

		if `iplus' != `mplus' {
			replace interval_ahead`i' = `iplus' if interval_ahead`i' == . & agegroup`i' != . & agegroup`iplus' != . // Presumably, only missing if the agegroup interval is always larger than time (i.e. if census is only one year apart)
			replace agegroup_ahead`i' = agegroup`iplus' if agegroup_ahead`i' == . & agegroup`i' != . & agegroup`iplus' != .
		}
	}
}
drop openend

di in red "Generating aggregated population estimates"
forvalues i = 1/`maxnum' {
	qui {
		gen agg_c1_`i' = 0
		gen agg_c2_`i' = 0
		local iminus = `i' - 1
		forvalues j = 0/`iminus' {
			replace agg_c1_`i' = agg_c1_`i' + c1_`j' if interval_back`i' <= `j'
		}
		forvalues j = `i' / `maxnum' {
			replace agg_c2_`i' = agg_c2_`i' + c2_`j' if interval_ahead`i' >= `j'
		}
		replace agg_c1_`i' = . if agg_c1_`i' == 0
		replace agg_c2_`i' = . if agg_c2_`i' == 0
		gen avgbdayagea_`i' = (1/(agegroup`i' - agegroup_back`i')) * sqrt(agg_c1_`i' * agg_c2_`i') if agg_c1_`i' != . & agg_c2_`i' != .
	}
}

drop agg_c1* agg_c2* interval_* agegroup_back* agegroup_ahead*


forvalues j = 0(1)`maxnum' {
	g pylaplus_`j' = sqrt(pop1aplus_`j'*pop2aplus_`j') if pop1aplus_`j' ~= . & pop2aplus_`j' ~= .
}

forvalues j = 0(1)`maxnum' {
	g popgrowthrateaplus_`j' = ln(pop2aplus_`j'/pop1aplus_`j')/time if pop1aplus_`j' ~= . & pop2aplus_`j' ~= .
}


forvalues j = 0(1)`maxnum' {
	g rhsaplus_`j' = deathsaplus_`j'/pylaplus_`j' if deathsaplus_`j' ~= . & pylaplus_`j' ~= .
}

forvalues j = 1(1)`maxnum' {
	g lhsaplus_`j' = (avgbdayagea_`j'/pylaplus_`j') - popgrowthrateaplus_`j' if avgbdayagea_`j' ~= . & pylaplus_`j' ~= . & popgrowthrateaplus_`j' ~= .
}

forvalues j = 0(1)`maxnum' {
	g obsasmr_`j' = vr_`j'/sqrt(c1_`j'*c2_`j') if vr_`j' ~= . & c1_`j' ~=. & c2_`j' ~= .
}


forvalues j = 1(1)`maxnum' {
	g obs5qa_`j' = (obsasmr_`j'*10)/(2+5*(obsasmr_`j')) if obsasmr_`j' ~= .
}

tempfile beforeregression
save `beforeregression', replace

keep ihme_loc_id pop_years lhsaplus_* rhsaplus_*

g top = 0
g bottom = 0

g lhsmean = 0
g rhsmean = 0

g lhssd = 0
g rhssd = 0

g sserr = 0
g sstot = 0

forvalues j = 1(1)`maxnumminus' {
	local trim_upper = `maxnum'-`j'
	local upper = `trim_upper'-4
	forvalues trim_lower = 1(1)`upper' {
		local num = (`trim_upper'-`trim_lower')+1

		g regslope_`trim_lower'to`trim_upper' = 0
		g intercept_`trim_lower'to`trim_upper' = 0
		g diagnosticggb_`trim_lower'to`trim_upper' = 0
		g orthogregslope_`trim_lower'to`trim_upper' = 0
		g orthogintercept_`trim_lower'to`trim_upper' = 0

		replace lhsmean = 0
		replace rhsmean = 0
		forvalues p = `trim_lower'(1)`trim_upper' {
			replace lhsmean = lhsmean + lhsaplus_`p' 
			replace rhsmean = rhsmean + rhsaplus_`p' 
		} 

		replace lhsmean = lhsmean/`num'
		replace rhsmean = rhsmean/`num'
		
		replace top = 0
		replace bottom = 0
		
		forvalues p = `trim_lower'(1)`trim_upper' {
			replace top = top + (lhsaplus_`p' - lhsmean)*(rhsaplus_`p' - rhsmean)
			replace bottom = bottom + (rhsaplus_`p' - rhsmean)^2 
		} 

		replace regslope_`trim_lower'to`trim_upper' = top/bottom
		replace intercept_`trim_lower'to`trim_upper' = lhsmean - regslope_`trim_lower'to`trim_upper'*rhsmean

		replace sserr = 0
		replace sstot = 0 
		forvalues p = `trim_lower'(1)`trim_upper' {
			replace sserr = sserr + (lhsaplus_`p'-(intercept_`trim_lower'to`trim_upper'+regslope_`trim_lower'to`trim_upper'*rhsaplus_`p'))^2
			replace sstot = sstot + (lhsaplus_`p'-lhsmean)^2  
		} 			

		replace diagnosticggb_`trim_lower'to`trim_upper' = 1 - sserr/sstot
		
		replace lhssd = 0
		replace rhssd = 0
		forvalues p = `trim_lower'(1)`trim_upper' {
			replace lhssd = lhssd + (lhsaplus_`p' - lhsmean)^2
			replace rhssd = rhssd + (rhsaplus_`p' - rhsmean)^2
		} 	
		replace lhssd = lhssd/`num'
		replace rhssd = rhssd/`num'
		replace lhssd = sqrt(lhssd)
		replace rhssd = sqrt(rhssd)

		replace orthogregslope_`trim_lower'to`trim_upper' = lhssd/rhssd
		replace orthogintercept_`trim_lower'to`trim_upper' = lhsmean - orthogregslope_`trim_lower'to`trim_upper'*rhsmean
		g ggb_avgcompleteness_`trim_lower'to`trim_upper' = 1/regslope_`trim_lower'to`trim_upper'


	}
}

}

tempfile afterregression
keep ihme_loc_id pop_years ggb_avgcompleteness_* regslope_* diagnosticggb_* orthogregslope_* intercept_* orthogintercept_*
duplicates drop ihme_loc_id pop_years, force
sort ihme_loc_id pop_years
save `afterregression', replace

use `beforeregression', clear
sort ihme_loc_id pop_years
merge ihme_loc_id pop_years using `afterregression', sort

drop _merge
save `beforeregression', replace

forvalues j = 1(1)`maxnumminus' {
	local trim_upper = `maxnum'-`j'
	local upper = `trim_upper'-4
	forvalues trim_lower = 1(1)`upper' {

		g completenessc1toc2_`trim_lower'to`trim_upper' = exp((orthogintercept_`trim_lower'to`trim_upper')*time)
	}

}

end
