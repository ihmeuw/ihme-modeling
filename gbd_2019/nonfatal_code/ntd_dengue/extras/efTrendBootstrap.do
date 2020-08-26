clear

local yearC 2010

set obs `=ADDRESS'
generate year_id = _n + 1979 
gen yearC = `yearC' - year_id
*mkspline yearS = yearC, knots(0 10 20 30) cubic
tempfile bsPred
save `bsPred'

*use "FILEPATH", clear
*use "FILEPATH", clear

import excel using "FILEPATH", sheet(extraction) firstrow clear


*import excel using "FILEPATH", sheet(extraction) firstrow clear

rename year_start year_id
drop if group_review==0

sort location_id

egen series = group(location_id)

gen yearC = `yearC' - year_id
*gen repRate = logit(1/efTotal)
*mkspline yearS = yearC, knots(0 10 20 30) cubic

  
local i = 0
while `i'<1000 {
  quietly {
  preserve
  bsample, cluster(series)
  append using `bsPred'
  
  *capture mepoisson efTotal yearC || efN: || series:
  capture mepoisson mean yearC ||  series:
  *capture mixed repRate yearC ||  series:
  
 /* if _rc!=0 | _b[yearC]<0 {
    noisily di as error "Bad model" _continue	
    restore
	continue
	}
*/
  noisily di as text "." _continue	
  *generate beta_`i' = _b[yearC]	
  generate ef_`i' =  exp(_b[yearC]*yearC)	
  *generate ef_`i' =  _b[yearC]*yearC
  *generate ef_`i' =  exp(_b[yearS1]*yearS1 + _b[yearS2]*yearS2 + _b[yearS3]*yearS3)
  
  keep if missing(mean)
  save `bsPred', replace
  restore
  local ++i
  }
  }
  
use `bsPred', clear
keep year* ef_*

/*
foreach var of varlist ef_* {
  quietly replace `var' = 1 if year_id>2010
  }
  
  */
egen efMean = rowmean(ef_*)
egen efLower = rowpctile(ef_*), p(2.5)
egen efUpper = rowpctile(ef_*), p(97.5)

*twoway (rarea efLower efUpper year_id, fcolor(gs12) lcolor(gs12)) (line efMean year_id, lcolor(navy)), legend(off)

keep year_id ef_*

egen efMean = rowmean(ef_*)

egen efUpper = rowpctile(ef_*), p(97.5)

egen efLower = rowpctile(ef_*), p(2.5)

 save FILEPATH, replace

*save FILEPATH, replace
