use FILEPATH, clear

bysort location_id year_id: egen maxN = max(n_types)
bysort location_id year_id: gen count = _N

gen maxHx = 1
replace maxHx = maxN if maxN>maxHx
bysort location_id (year_id): replace maxHx = maxHx[_n-1] if maxHx[_n-1]>maxHx & !missing(maxHx[_n-1])


gen is_outlier = maxHx!=n_types

tempfile data
save `data'

get_location_metadata, location_set_id(35) clear
keep location_id location_name ihme_loc_id *region* location_type path_to_top_parent is_estimate

expand 37

bysort location_id: gen year_id = _n + 1979

merge 1:m location_id year_id using `data', nogenerate

keep if is_estimate==1 | location_type=="admin0" | !missing(n_types)

split path_to_top_parent, parse(,) destring
rename path_to_top_parent4 country_id
drop path_to_top_parent?

generate modRegion = "Asia" if strmatch(lower(region_name), "*asia*") | region_name=="Oceania"
replace  modRegion = "Americas" if strmatch(lower(region_name), "*america*") | region_name=="Caribbean"
replace  modRegion = "Africa" if strmatch(lower(region_name), "*africa*")
replace  modRegion = "Europe" if strmatch(lower(region_name), "*europe*")
encode   modRegion, generate(modRegionCode)

mkspline yearS = year_id, cubic nknots(3)
*meologit maxHx yearS* if is_outlier==0 || super_region_id: || country_id: 

meologit maxHx year_id if is_outlier==0 || region_id:  || country_id:
capture drop pred*
predict pred*
predict predXb, xb
predict predFixed*, fixedonly
predict predRandom*, remeans

bysort region_id: egen meanTmp1 = mean(predRandom1)
replace predRandom1 = meanTmp1 if missing(predRandom1)
replace predRandom1 = 0 if missing(predRandom1)
bysort country_id: egen meanTmp2 = mean(predRandom2)
replace predRandom2 = meanTmp2 if missing(predRandom2)
replace predRandom2 = 0 if missing(predRandom2)
drop meanTmp*

replace predXb = predXb + predRandom1 + predRandom2

generate predCat = 1 if predXb<_b[cut1:_cons]
replace  predCat = 2 if predXb>=_b[cut1:_cons] & predXb<_b[cut2:_cons]
replace  predCat = 3 if predXb>=_b[cut2:_cons] & predXb<_b[cut3:_cons]
replace  predCat = 4 if predXb>=_b[cut3:_cons] 



egen predMax = rowmax(pred?)
generate predN = .

forvalues i = 1/4 {
	replace predN = `i' if predMax==pred`i'
	}

forvalues i = 1/3 {		
	gen gt`i' = maxN>`i' if !missing(maxN)	
	}
