// This applies the Synthetic Extinct Generations method

cap program drop seg
program define seg

syntax, data(string) sex(integer)

clear
set mem 500m
set more off
use "`data'", clear

lookfor agegroup100
return list


if("`r(varlist)'" == "") {
	// Find maximum age group value in the data file.
		forvalues j = 0/100 {
			lookfor agegroup`j'
			return list
			if("`r(varlist)'" ~= "") {
				local maxnum = `j'
			}
		}

		local mplus = `maxnum'+1
		local maxnumminus = `maxnum'-1
}
else {
	// Find maximum age group value in the data file.
		forvalues j = 0/100 {
			levelsof agegroup`j', clean local(ag)
		
			if("`ag'" ~= "") {
				local maxnum = `j'
			}
		}
		
		local mplus = `maxnum'+1
		local maxnumminus = `maxnum'-1
	
	// Drop variables that are missing -- above maximum age
		forvalues j = `mplus'/100 {
			drop c1_`j' c2_`j' vr_`j' agegroup`j'
		}
}

// quietly {
// Set the open-ended interval for the dataset
	g openend = .
	forvalues j = 0/`maxnumminus' {
		local jplus = `j'+1
		replace openend = agegroup`j' if vr_`jplus' == . & openend == .
	}
	replace openend = `maxnum'*5 if openend == .

	forvalues j = 1(1)`maxnum' {
		local jminus = `j'-1
		g avgbday_`j' = sqrt(c1_`jminus'*c2_`j')/(agegroup`j'-agegroup`jminus') if c1_`jminus' ~= . & c2_`j' ~= .
	}
	
** Generate age specific growth rate
forvalues j = 0(1)`maxnum' {
	g agegrowthrate_`j' = ln(c2_`j'/c1_`j')/time if c1_`j' ~= . & c2_`j' ~= .
}

** Generate cumulative growth rate
// In short, we estimate 5da in order to generate observed estimates of 30d10_20d40 in order to match with the appropriate life table level
// We then use the life table to impute life expectancies for the open-ended interval

g cumulgrowthrate_0 = ((agegroup1-agegroup0)/2)*agegrowthrate_0

forvalues j = 1/`maxnum' {
	g cumulgrowthrate_`j' = 0
	local jminus = `j'-1
	g sumgrowth = 0 

	// Aggregate the age-specific growth rates of all ages prior to the current age
	forvalues i = 0(1)`jminus' {
		replace sumgrowth = sumgrowth + agegrowthrate_`i'
	}
	
	replace cumulgrowthrate_`j' = (agegroup`j'-agegroup`jminus')*sumgrowth + (agegroup`j'-agegroup`jminus')*agegrowthrate_`j' / 2
	
	drop sumgrowth
}

** Generate life table deaths 5dx 
// This gets the estimate of your lifetable deaths by applying the cumulated growth rates to the 5-year VR death data
forvalues j = 0/`maxnumminus' {
	g lifetabledeaths5dx_`j' = (vr_`j')*exp(cumulgrowthrate_`j') if vr_`j' ~= .
}

g lifetabledeaths5dx_top = 0
g lifetabledeaths5dx_bottom = 0

g diff1 = 0
g diff2 = 0
local mminus = `maxnum'-2
forvalues j = 0/`mminus' {
	local jplus = `j'+1
	forvalues k = `jplus'/`maxnumminus' {
		// Take the sum of lifetable deaths 5dx in the interval, if the year difference is the largest it can be 
		// Note the age restrictions set for _top and _bottom. This is to feed into the ratio_30d10_20d40
		egen temp = rowtotal(lifetabledeaths5dx_`j'-lifetabledeaths5dx_`k') 
		replace lifetabledeaths5dx_top = temp if agegroup`j' >= 10 & agegroup`k' <= 35 & (`k'-`j') >= diff1
		replace lifetabledeaths5dx_bottom = temp if agegroup`j' >= 40 & agegroup`k' <= 55 & (`k'-`j') >= diff2
		drop temp
		replace diff1 = `k'-`j' if `k'-`j' > diff1 & agegroup`j' >= 10 & agegroup`k' <= 35
		replace diff2 = `k'-`j' if `k'-`j' > diff2 & agegroup`j' >= 40 & agegroup`k' <= 55
	}
}

// Ratio of total deaths of 10-40 age group vs. 40-60 age group. 
g ratio_30d10_20d40 = lifetabledeaths5dx_top/lifetabledeaths5dx_bottom

** Replace it with the median if it's missing
sum ratio_30d10_20d40, detail
local median_ratio = r(p50) // Get median in case the mean is skewed due to outliers
replace ratio_30d10_20d40 = `median_ratio' if ratio_30d10_20d40 == .

tempfile beforemerge
save `beforemerge', replace

** *******************************************************************************
** Grab Coale Demeny Life Table numbers from UN to predict life expectancy for the open age interval

qui do "FILEPATH/fastcollapse.ado"

cd "FILEPATH"
import excel using "FILEPATH", clear firstrow

// Variables: index, name, iso3, location-type, lt_type, cd_type notes
keep if location_type == 4 // Country/area
replace cd_type = "CD West" if lt_type != "Model life tables"
rename iso3 ihme_loc_id

expand 3
bysort cd_type ihme_loc_id: gen n = _n
gen sex = ""
replace sex = "Male" if n == 1
replace sex = "Female" if n == 2
replace sex = "Both" if n == 3

tempfile cd_types
save `cd_types'


// Grab Coale Demeny Life tables
cd "FILEPATH"
import excel using "FILEPATH", clear firstrow sheet(MLT_DB)
drop if age == . // Just a bunch of empty rows
rename Type cd_type
rename Sex sex

replace cd_type = "UN Far Eastern" if cd_type == "UN Far_East_Asian"
replace cd_type = "UN South Asian" if cd_type == "UN South_Asian"

tempfile cd_master
save `cd_master'

// Generate 30d10_20d40 estimates 
keep cd_type sex E0 age lxn dxn

// Collapse to sums of lxn and dxn over sex to generate a "both sexes" measure
fastcollapse lxn dxn, type(mean) by(cd_type E0 age) append flag(both_flag)
replace sex = "Both" if both_flag == 1
drop both_flag

reshape wide lxn dxn, i(cd_type sex E0) j(age)
gen d_30d10 = dxn10 + dxn15 + dxn20 + dxn25 + dxn30 + dxn35
gen d_20d40 = dxn40 + dxn45 + dxn50 + dxn55
gen ratio_lvl = d_30d10 / d_20d40
gen lvl = E0 - 19
keep ratio_lvl cd_type sex lvl
reshape wide ratio_lvl, i(cd_type sex) j(lvl)

// Merge with CD Types
merge 1:m cd_type sex using `cd_types', keep(3) nogen // non-mergers should just be cd_types that aren't used by UN estimates

keep cd_type sex ratio_lvl* ihme_loc_id 

// Expand dataset to match open-ended intervals
expand 8
bysort cd_type sex ihme_loc_id: gen n = _n
gen openend = 55 + (5*n)
drop n 

rename ihme_loc_id iso3_short // For merging

tempfile ratio_master
save `ratio_master'

// Grab formatted open-ended life expectancies
use `cd_master', clear
keep Exn cd_type sex age E0 Txn lxn
keep if age >=60 & age <= 95
gen lvl = E0 - 19
drop E0 

// Generate a both-sexes mean life expectancy
fastcollapse Txn lxn, type(sum) append flag(both_flag) by(cd_type age lvl)
replace sex = "Both" if both_flag == 1
drop both_flag

replace Exn = Txn / lxn if Exn == .
rename Exn lifeexp_lvl
drop Txn lxn

reshape wide lifeexp_lvl, i(cd_type sex age) j(lvl)
rename age openend

tempfile exp_master
save `exp_master'

merge 1:m cd_type sex openend using `ratio_master', keep(3) nogen // Dropping unused cd_types

if "`sex'" == "0" keep if sex == "Both"
if "`sex'" == "1" keep if sex == "Male"
if "`sex'" == "2" keep if sex == "Female"
drop sex

tempfile cd_final
save `cd_final'


** **************************************************************************************
** Merge dataset with CD/UN lifetable estimates, and generate estimates of LE in open-ended interval
use `beforemerge', clear

keep ihme_loc_id pop_years ratio_30d10_20d40 openend deaths_years
gen iso3_short = substr(ihme_loc_id,1,3)

merge m:1 iso3_short openend using `cd_final', keep(1 3) // Only non-mergers should be if their open interval is outside of ours
count if _merge == 1 & openend >= 60 & openend < 100
local error_count = `r(N)'
if `error_count' > 0 {
	noi {
		di in red "At least one country did not merge correctly on iso3"
		levelsof iso3_short if _merge == 1 
		BREAK
	}
}

keep ihme_loc_id pop_years deaths_years ratio_30d10_20d40 openend ratio_lvl* lifeexp_lvl* cd_type

g lifeexpforopeninterval = 0 if openend >= 60 & openend < 100

// Get the life expectancy for an open interval from the Coale Demeny life table data
// Life expectancy is the life expectancy level for that level plus the ratio of level minus 30d10_20d40, over ratio of level minus ratio of the next level up times the differences in life expectancy between the first and second life expectancies. 
// So it takes the life expectancy of the level plus the the difference in life expectancies times the relative distance in between the ratios that our observed is.

forvalues j = 1/80 {
	local jplus = `j'+1
	replace lifeexpforopeninterval = lifeexp_lvl`j'+((ratio_lvl`j'-ratio_30d10_20d40)/(ratio_lvl`j'-ratio_lvl`jplus'))*(lifeexp_lvl`jplus'-lifeexp_lvl`j') if ratio_30d10_20d40 <= ratio_lvl`j' & ratio_30d10_20d40 >= ratio_lvl`jplus'
}

** ********************
// Save diagnostic sheet with sources that fall outside of the ratios
preserve
keep if lifeexpforopeninterval == 0
keep ihme_loc_id pop_years deaths_years cd_type ratio_30d10_20d40 ratio_*
split ihme_loc_id, parse("&&")
outsheet using "$main_dir/ratio_outside.csv", comma replace
restore

replace lifeexpforopeninterval = lifeexp_lvl81 if ratio_30d10_20d40 < ratio_lvl81 & ratio_lvl81 != .
replace lifeexpforopeninterval = lifeexp_lvl1 if ratio_30d10_20d40 > ratio_lvl1 & ratio_30d10_20d40 != . 

keep ihme_loc_id pop_years deaths_years lifeexpforopeninterval
sort ihme_loc_id pop_years
merge 1:1 ihme_loc_id pop_years deaths_years using `beforemerge'
drop _merge

save `beforemerge', replace


** Generate N(a+), the estimate of the synthetic population aged a+

forvalues k = 0/`maxnum' {
	g vr_`k'plus = vr_`k' if agegroup`k' == openend
	g agegrowthrate_`k'plus = agegrowthrate_`k' if agegroup`k' == openend
}

// If open-ended, the na_plus is equal to deaths in following ages * exp(life expectancy * age growth rate following age) - (1/6) * (life expectancy * growth rate) ^2
forvalues k = 0/`maxnum' {
	g na_`k'plus = vr_`k'plus*(exp(lifeexpforopeninterval*agegrowthrate_`k'plus)-((lifeexpforopeninterval*agegrowthrate_`k'plus)^2)/6) if agegroup`k' == openend
	g na_`k' = .
	replace na_`k' = na_`k'plus if agegroup`k' == openend
}

// If open-ended, generate the na estimate for previous age group as na_+ * exp(growth rate * year-gap before agegroup) + deaths * exp(year-gap / 2) 
// So this is the estimated population at k, plus the growth rate between the groups, plus deaths in the half-interval (since the later population has deaths subtracted out from them)
forvalues k = 1/`maxnum' {
	local kminus = `k'-1
	** previously: replace na_`kminus' = na_`k'plus*exp(agegrowthrate_`kminus'*5)+vr_`kminus'*exp(2.5*agegrowthrate_`kminus') if na_`k'plus ~= .
	replace na_`kminus' = na_`k'plus * exp(agegrowthrate_`kminus' * (agegroup`k'-agegroup`kminus')) + vr_`kminus' * exp(((agegroup`k'-agegroup`kminus')/2) * agegrowthrate_`kminus') if na_`k'plus ~= .
}

// Take the number alive at na(k) multiplied by the growth rate / # ages (avg. growth rate over __ years). Plus vr times exp(etc).
// The vr part onwards makes sense as the part in #7. So this is saying that number here now is equal to the number in the older cohort, adjusted for age growth rates, and plus the deaths in x.
// This uses the na * exp(agegrowthrate * difference) as equal to the sum of 5ry (rate of change between y and 5 years from y), where 5 is variable depending on age group.
forvalues k = `maxnumminus'(-1)0 {
	local kplus = `k'+1
	replace na_`k' = na_`kplus'*exp((agegrowthrate_`k')*(agegroup`kplus'-agegroup`k'))+(vr_`k')*exp(((agegroup`kplus'-agegroup`k')/2)*(agegrowthrate_`k')) if na_`k' == . 
}


** Generate SEG age specific completeness ratio

forvalues j = 1/`maxnum' {
	g seg_compratio_`j' = (na_`j')/avgbday_`j' if na_`j' ~= . & avgbday_`j' ~= .
}

** Regress ages on the SEG completeness ratios
	// Orthogonal regression
tempfile beforeregression
save `beforeregression', replace

keep ihme_loc_id pop_years seg_compratio* 


g top = 0
g bottom = 0

g lhsmean = 0
g rhsmean = 0

forvalues j = 0/`maxnumminus' {
	// The trim is up to 5 from the top
	// The lower trim goes from one to the upper
	local trim_upper = `maxnum'-`j'
	local upper = `trim_upper'-1
	forvalues trim_lower = 1/`upper' {
		local num = (`trim_upper'-`trim_lower') + 1	
		
		g diagnosticseg_`trim_lower'to`trim_upper' = 0

		replace lhsmean = 0
		replace rhsmean = 0
		forvalues p = `trim_lower'/`trim_upper' {
			replace lhsmean = lhsmean + seg_compratio_`p' // This adds the age-specific completeness ratio for each age category
			replace rhsmean = rhsmean + `p' // This adds all of the age categories 
		} 

		replace lhsmean = lhsmean/`num' // This takes it over the number of age categories in total (trimmed)
		replace rhsmean = rhsmean/`num'
		
		replace top = 0
		replace bottom = 0
		
		forvalues p = `trim_lower'/`trim_upper' {
			replace top = top + ((seg_compratio_`p' - lhsmean)*(`p' - rhsmean)) // Let's aggregate the difference between the SEG completeness and the mean, vs. the age category vs the RHS mean.
			replace bottom = bottom + (`p' - rhsmean)^2  // This gets the distance squared for ages (aggregated)
		} 

		replace diagnosticseg_`trim_lower'to`trim_upper' = top/bottom 
	
	}
}

tempfile afterregression
keep ihme_loc_id pop_years diagnosticseg_*
duplicates drop ihme_loc_id pop_years, force
sort ihme_loc_id pop_years
save `afterregression', replace

use `beforeregression', clear
sort ihme_loc_id pop_years
merge ihme_loc_id pop_years using `afterregression', sort
drop _merge
save `beforeregression', replace

** Generate the mean SEG completeness ratio

forvalues w = 0/`maxnumminus' {
	local trim_upper = `maxnum'-`w'
	local upper = `trim_upper'-1
	forvalues trim_lower = 1/`upper' {

		g sumcomp = 0
		g numcomp = 0
		
		forvalues j = `trim_lower'/`trim_upper' {
			replace sumcomp = sumcomp + seg_compratio_`j'
			replace numcomp = numcomp + 1
		}

		g seg_avgcompleteness_`trim_lower'to`trim_upper' = sumcomp / numcomp // Divide the summed completenesses by the number of comparisons
		drop sumcomp numcomp

	}
}
// }

di "`maxnum'"

end
