clear all
set more off

if c(os) == "Unix" {
	global prefix "/home/j"
	set odbcmgr ADDRESS
	}
else if c(os) == "Windows" {
	global prefix "FILEPATH"
	}

tempfile master	
	
run FILEPATH
run FILEPATH	
	
get_location_metadata, location_set_id(35) clear
keep location_id parent_id path_to_top_parent location*name*
*use "FILEPATH", clear
split path_to_top_parent, destring parse(,)
rename path_to_top_parent4 country_id
drop path_to_top_parent?
keep if country_id==163 & location_id!=163 & parent_id!=163
save `master'

import excel FILEPATH, sheet("Sheet2") firstrow clear
*use "FILEPATH", clear
rename location_name location_ascii_name

replace location_ascii_name = "Jammu and Kashmir" if location_ascii_name == "J & K"
replace location_ascii_name = "Uttarakhand" if location_ascii_name=="Uttrakhand"
replace location_ascii_name = "Chhattisgarh" if location_ascii_name=="Chattisgarh"
replace location_ascii_name = "The Six Minor Territories" if inlist(location_ascii_name,"A& N Island","Chandigarh","D&N Haveli","Daman & Diu","Puduchery")
replace location_ascii_name = "" if location_ascii_name==""

expand 2, gen(urban)
replace location_ascii_name = location_ascii_name + ", Rural" if urban==0
replace location_ascii_name = location_ascii_name + ", Urban" if urban==1 

merge m:1 location_ascii_name using `master'

collapse (sum) cases*, by(location_id location_name)

reshape long cases, i(location*) j(year_id)
save `master', replace

levelsof location_id, local(locations) clean
levelsof year_id, local(years) clean

get_population, location_id(`locations') year_id(`years') age_group_id(22) sex_id(3) clear
merge 1:1 location_id year_id using `master'

keep location_id location_name year_id cases population 
rename year_id year_start
rename population sample_size
generate mean = cases / sample_size

save FILEPATH, replace
