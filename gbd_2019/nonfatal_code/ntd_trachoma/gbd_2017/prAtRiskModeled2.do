clear all
set more off

if c(os) == "Unix" {
	global prefix "FILEPATH"
	set odbcmgr ADDRESS
	}
	
else if c(os) == "Windows" {
	global prefix "FILEPATH"
	}

run "$prefixFILEPATH"
run "$prefixFILEPATH"
run "$prefixFILEPATH"

tempfile elim locations pop


import excel "$prefixFILEPATH", sheet("Sheet1") cellrange(B1:D11) firstrow clear
save `elim', replace

* PULL POPULATION DATA *
  get_location_metadata, location_set_id(35) clear
  keep if is_estimate==1 | location_type=="admin0"
  keep location_id location_name ihme_loc_id is_estimate location_type
  generate iso3 = ihme_loc_id 
  
  levelsof iso3 if location_type=="admin0" & is_estimate==0, local(snIsos) clean
  foreach snIso of local snIsos {
    levelsof iso3 if strmatch(iso3, "`snIso'_*") & is_estimate==1, local(`snIso'Isos) clean
	}
	
  save `locations'
  
  levelsof location_id, local(locationList) clean

  get_demographics, gbd_team(cod) clear
  local years `r(year_id)'
  local firstYear = word("`years'", 1)
  local lastYear  = word("`years'", -1)
  
  get_population, age_group_id(22) sex_id(3) location_id(`locationList') year_id(`years') clear
  keep if inrange(year_id, `firstYear', `lastYear')
  
  merge m:1 location_id using `locations', keep(3) nogenerate
  save `pop', replace

import delimited "$prefixFILEPATH", bindquote(strict) clear 
keep yearcode countrycode countrydisplay displayvalue 
drop if displayvalue=="No data available"
rename yearcode year_id
rename countrycode iso3
rename countrydisplay location_name
destring displayvalue, gen(nAtRiskTrachoma) force
replace nAtRiskTrachoma = 0 if displayvalue=="Case management"
gen source="who"

merge 1:1 iso3 year_id using `pop', keep(match master) nogenerate

generate prAtRiskTrachoma = nAtRiskTrachoma / population

keep year_id iso3 source prAtRiskTrachoma displayvalue
tempfile mergingTemp
save `mergingTemp', replace

use "$prefixFILEPATH", clear
replace iso3 = substr(iso3, 1, 3)
bysort iso3 year: gen index = _n
drop if index > 1
drop index
rename year year_id
gen source = "atlas"

quietly sum year_id
local maxYear = `r(max)'

expand `=1 + `lastYear' - `maxYear'' if year_id==`maxYear', gen(newObs)
bysort iso3 newObs: replace year_id = year_id + _n if newObs==1
drop newObs
replace prAtRiskTrachoma=. if year_id>2013


append using `mergingTemp'

replace source="" if year_id>=2014

merge m:1 iso3 using `elim', gen(elimMerge)
replace start = 1998 if iso3=="AUS"
replace start = 2000 if iso3=="VNM"

bysort iso3 (year_id): gen expand = ((target - year_id)>0 & !missing(target) & _n==_N) * 2
expand expand, gen(newObs)
replace year_id = target if newObs==1
replace prAtRiskTrachoma = 0 if newObs==1
replace source = "target" if newObs==1
drop newObs

bysort iso3 (year_id): egen anyWho = max(source=="who")

generate eliminated = inlist(iso3, "DZA", "GHA", "IRN", "LBY", "MEX", "MAR", "OMN", "VNM") | inlist(iso3, "TUR", "SYR", "JOR", "SAU", "ARE", "QAT", "BHR", "KWT") | inlist(iso3, "ZAF", "TUN", "THA", "MNG")

generate prToModel = prAtRiskTrachoma
replace  prToModel = . if year_id > start & anyWho == 1 & source == "atlas"
replace  prToModel = . if year_id == 2013 & anyWho == 1 & source == "atlas"
replace  prToModel = . if year_id >= 2010 & eliminated==1 & prAtRiskTrachoma>0 & source == "atlas"

bysort iso3: egen anyZeros = total(prToModel==0 & year_id<=2013)

expand 2 if eliminated==1 & anyZeros==0 & year_id>=2010, gen(newObs)
replace prToModel = 0 if newObs==1
replace source = "eliminated" if newObs==1
drop newObs

sort iso3 year_id

by iso3: egen sd = sd(prToModel)

save $prefixFILEPATH, replace

use  $prefixFILEPATH, clear

bysort iso3: egen maxPr = max(prToModel)
bysort iso3 (year_id): gen afterMax = sum(maxPr == prToModel)
replace prToModel = maxPr if inlist(iso3, "EGY", "GHA", "MAR", "PAK", "TZA", "SDN") & (((year_id<=start & !missing(start)) | (missing(start) & afterMax==0)))
replace source = "who" if iso3=="EGY" & (year_id<2010 | year_id==2013)
drop if iso3=="EGY" & source == "atlas"
bysort iso3 (year_id): replace prToModel = prToModel[_n-1] if year_id==2013 & missing(prToModel) & prToModel[_n-1]>0 & prToModel[_n+1]==0

bysort iso3: egen meanLID = mean(location_id)
replace location_id = meanLID if missing(location_id)
replace location_id = 6 if iso3=="CHN"
replace location_id = 95 if iso3=="GBR"
replace location_id = 163 if iso3=="IND"
replace location_id = 130 if iso3=="MEX"
bysort iso3 year_id: egen meanPr = mean(prToModel)
bysort iso3 year_id: gen index = _n
drop if index>1
replace prToModel = meanPr

bysort iso3: gen count = _N
tab count
tsset location_id year_id

tssmooth ma temp=prToModel, window(2 1 2)
tssmooth ma maSmooth=temp, window(2 1 2)

tssmooth ma tempB=prToModel, window(3 1 3)
tssmooth ma maSmoothB=tempB, window(3 1 3)
bysort iso3 (year_id): replace maSmoothB = maSmoothB[_n-1] if maSmoothB > maSmoothB[_n-1]

sort iso3 year_id


capture gen gom3Smooth = .

local gom3 AUS BFA BRA CIV CMR EGY ETH GNB LAO MWI SDN SSD YEM

foreach iso of local gom3 {
  di "`iso'"
  quietly {
  capture drop temp
  nl gom3: prToModel year_id if iso3=="`iso'"
  predict temp
  replace gom3Smooth = temp if iso3=="`iso'"
  }
  }
 
gen final = prAtRiskTrachoma if sd==0 | (inlist(iso3, "TCD", "IND", "CHN", "ZMB", "MOZ") & source=="atlas")
replace final = maSmooth if inlist(iso3, "DJI", "GMB", "GTM", "PAK", "OMN", "MAR", "IRN", "LBY", "GHA") 
replace final = maSmoothB if inlist(iso3, "BDI", "DZA", "ERI", "IRQ", "KEN", "KHM", "MEX", "MLI") | inlist(iso3, "MMR", "MRT", "MWI", "NER", "NGA", "NPL", "SEN", "SLB", "TZA") | inlist(iso3,"VNM", "UGA")
replace final = gom3Smooth if !missing(gom3Smooth)


levelsof iso3 if sd>0 & !inlist(iso3, "TCD", "EGY", "IND", "CHN", "ZMB", "MOZ") , local(is) clean


bysort iso3: egen meanFinal = mean(final)
replace final = meanFinal if missing(final)
keep iso3 location_id year_id final
keep if inrange(year_id,`firstYear', `lastYear')

rename final prAtRiskTrachoma


foreach iso of local snIsos {  
  expand `=wordcount("``iso'Isos'") + 1' if iso3=="`iso'", gen(newObs)
  bysort newObs iso3 year_id : gen index = _n
  
  local count 1
  
  foreach subnat in ``iso'Isos' {
	replace location_id = `=subinstr("`subnat'", "`iso'_", "", .)' if index==`count' & iso3=="`iso'" & newObs==1
	replace iso3 = "`subnat'" if index==`count' & iso3=="`iso'" & newObs==1
	local ++count
	}

  drop newObs index
  }
	
  
  drop iso3
  duplicates drop
  
  tsset location_id year_id
  generate laggedPar = L10.prAtRiskTrachoma
  bysort location_id (year_id): replace laggedPar = prAtRiskTrachoma[1] if missing(laggedPar)
  
  save $prefixFILEPATH, replace
  export delimited using $prefixFILEPATH, replace
  
