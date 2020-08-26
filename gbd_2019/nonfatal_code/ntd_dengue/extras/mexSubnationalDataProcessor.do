clear all
set more off

if c(os) == "Unix" {
	global prefix "FILEPATH"
	set odbcmgr ADDRESS
	}
else if c(os) == "Windows" {
	global prefix "FILEPATH"
	}

tempfile master	cases
	
run FILEPATH
run FILEPATH
	
get_location_metadata, location_set_id(35) clear
*use FILEPATH, clear
keep location_id parent_id path_to_top_parent location*name*
split path_to_top_parent, destring parse(,)
rename path_to_top_parent4 country_id
drop path_to_top_parent?
keep if country_id==130 & location_id!=130
keep location_id location*name*
save `master'


use  FILEPATH, clear
keep state year Dengue_both population

replace state = "Yucatan" if state=="Yucat�n"
replace state = "Veracruz de Ignacio de la Llave" if state=="Veracruz"
replace state = "San Luis Potosi" if state=="San Luis Potos�"
replace state = "Queretaro" if state=="Quer�taro"
replace state = "Nuevo Leon" if state=="Nuevo Le�n"
replace state = "Michoacan de Ocampo" if state=="Michoac�n"
replace state = "Mexico" if state=="M�xico"

rename state location_ascii_name 
rename population sample_size
rename year year_id
merge m:1 location_ascii_name  using `master', keep(3) nogenerate

save `cases'

import delimited FILEPATH, case(preserve) clear 
replace state = trim(proper(state))
replace state = "Michoacan de Ocampo" if state=="Michoacan"
replace state = "Veracruz de Ignacio de la Llave" if state=="Veracruz"
rename state location_ascii_name 
rename year year_id
merge m:1 location_ascii_name using `master', keep(3) nogenerate

append using `cases'
save `cases', replace


levelsof location_id if missing(sample_size), local(locations) clean
levelsof year_id if missing(sample_size), local(years) clean

get_population, location_id(`locations') year_id(`years') age_group_id(22) sex_id(3) clear
merge 1:1 location_id year_id using `cases', assert(2 3) nogenerate

rename Dengue_both cases
replace sample_size = population if missing(sample_size)
keep location_name location_id year_id cases sample_size

generate nid = .

replace nid = NID if year_id==1990
replace nid = NID if year_id==1991
replace nid = NID if year_id==1992
replace nid = NID if year_id==1993
replace nid = NID if year_id==1994
replace nid = NID if year_id==1995
replace nid = NID if year_id==1996
replace nid = NID if year_id==1997
replace nid = NID if year_id==1998
replace nid = NID if year_id==1999
replace nid = NID if year_id==2000
replace nid = NID if year_id==2001
replace nid = NID if year_id==2002
replace nid = NID if year_id==2003
replace nid = NID if year_id==2004
replace nid = NID if year_id==2005
replace nid = NID if year_id==2006
replace nid = NID if year_id==2007
replace nid = NID if year_id==2008
replace nid = NID if year_id==2009
replace nid = NID if year_id==2010
replace nid = NID if year_id==2011
replace nid = NID if year_id==2012
replace nid = NID if year_id==2013
replace nid = NID if year_id==2014
*replace nid = if year_id==2015


save FILEPATH, replace


generate source_type = "Case notifications - other/unknown"
rename year_id year_start
generate year_end = year_start
generate age_start = 0
generate age_end = 99
rename cases value_cases
generate ignore = 0

generate gbd_component = "Nonfatal Health Outcomes"
generate cause_id = 357
generate measure_id = 6
generate release = "gbd_2016"
generate not_population_data = 0
generate age_type = "Years"
generate sex = "Both"

foreach var in seq underlying_nid value_mean value_lower value_upper standard_error  mean {
	generate `var' = .
	}

generate urbanicity_type = ""

order seq nid underlying_nid source_type location_id year_start year_end age_start age_end value_mean value_lower value_upper standard_error value_cases sample_size ignore gbd_component cause_id measure_id release not_population_data age_type sex mean urbanicity_type
