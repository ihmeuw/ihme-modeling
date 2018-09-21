// NAME
// 4.2.2016
// GBD2016 Check ebola age distribution post draws


	clear
	set more off
	
	
	if c(os) == "Unix" {
		use "FILEPATH", clear
		
		collapse (mean) deaths, by(iso3 location_id year age sex cause) fast
		
		save "FILEPATH", replace
	}
	
	else if c(os) == "Windows" {
		do "FILEPATH"
		get_ids, table(age_group) clear
		levelsof age_group_id, local(ids) clean
		foreach id of local ids {
			levelsof age_group_name if age_group_id == `id', local(name) clean
			local l_`id' `id' "`name'"	//"
		}

		
		use "FILEPATH", clear
		
		foreach id of local ids {
			label define L_age `l_`id'', modify
		}
		
		label values age L_age
		
		levelsof year, local(years) clean
		quietly {
			pause on
			foreach year of local years {
				levelsof iso3 if year==`year', local(isos) clean
				foreach iso of local isos {
					tw line deaths age if iso3 == "`iso'" & year == `year', title("`iso' - `year'") xlabel(, valuelabel)
					graph export "FILEPATH", replace
				}
			}
			pause off
		}
	}
		
		
