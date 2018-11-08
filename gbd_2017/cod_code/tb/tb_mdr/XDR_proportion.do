
** Description: compute XDR proportions

use "FILEPATH", clear

keep ihme_loc_id region_name super_region_name

rename ihme_loc_id iso3

tempfile iso3

save `iso3', replace

insheet using "FILEPATH", comma names clear

tempfile mdr
save `mdr', replace 

insheet using "FILEPATH", comma names clear

replace country="Czech Republic" if country=="Czechia"

replace country="Curaçao" if country=="CuraÁao"

merge 1:1 country using `mdr', keep(3)nogen

merge 1:1 iso3 using `iso3', keep(3)nogen

drop if mdr_dst==. & xdr==.

collapse (sum) mdr_dst xdr, by (super_region_name)

gen XDR_prop=xdr/mdr_dst

save "FILEPATH", replace


gen year_id=2017

expand 2, gen(new)

replace year_id=1992 if new==1

drop new

replace XDR_prop=0 if year==1992

// Prepare to project back to 1980

expand 2 if year==1992, gen(exp)
replace year=1980 if exp==1
drop exp

// interpolate/extrapolate

egen panel = group(super_region_name)
tsset panel year
tsfill, full

bysort panel: ipolate XDR_prop year, gen(xdr_prop_new) epolate

bysort panel: replace super_region_name = super_region_name[_n-1] if super_region_name==""

keep super_region_name year_id xdr_prop_new

rename xdr_prop_new XDR_prop

save "FILEPATH", replace




