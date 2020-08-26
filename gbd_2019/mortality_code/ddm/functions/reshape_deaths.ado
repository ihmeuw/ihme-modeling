
** Set up Stata

cap program drop reshape_deaths
program define reshape_deaths

// quietly {

clear
set mem 500m
set maxvar 32000
set more off

********************************************************
** Set parameters

syntax, popdata(string) data(string) saveas(string) iso3(string)

********************************************************
** Analysis code

set seed 12345
g temp = .
tempfile saveastemp
save `saveastemp', replace

use "`popdata'", clear
local pop_count = _N
if (`pop_count' != 0){
	local pop_count = _N
	
	keep if source_type == "CENSUS" | source_type == "pop_registry" | (source_type == "DSP" & dsp_use_ddm == 1)
}

use "`data'", clear

local deaths_count = _N
noi di "DEATHS COUNT: `deaths_count'"

if (`deaths_count' != 0){
	
	drop if (regexm(source_type, "VR") != 1 & regexm(source_type, "CENSUS") != 1) & hh_scaled == 0
}
local deaths_count = _N
noi di "DEATHS COUNT: `deaths_count'"

qui if (`pop_count' != 0 & `deaths_count' != 0) { 
	
	replace ihme_loc_id = ihme_loc_id + "&&" + sex + "@@" + source_type
	drop sex

	* Everything except for DSP gets paired with censuses
	levelsof ihme_loc_id if source_type != "DSP", local(source_types_loc)
	tempfile deathdata
	save `deathdata', replace

	** Load the pop data and extra copies of the census data to match with other national-level sources 
	if("`iso3'" != "all") use "`popdata'" if ihme_loc_id == "`iso3'", clear
	else use "`popdata'", clear
	noi di "before drop all non census"

	keep if source_type == "CENSUS" | source_type == "pop_registry" | (source_type == "DSP" & dsp_use_ddm == 1)
	replace ihme_loc_id = ihme_loc_id + "&&" + sex + "@@" + source_type

	tempfile master
	save `master'
	tempfile master_new
	save `master_new'
	
	clear 
	tempfile new_pop
	save `new_pop', replace emptyok	

	foreach stl of local source_types_loc {
		tokenize "`stl'", parse("&&") // Grab the country iso3
		use `master', clear
		keep if (source_type=="CENSUS" | source_type == "pop_registry") & substr(ihme_loc_id,1,strpos(ihme_loc_id,"&&")-1) == "`1'" & sex == substr("`stl'", strpos("`stl'", "&&")+2, strpos("`stl'", "@@") - strpos("`stl'", "&&")-2)
		replace ihme_loc_id = "`stl'"
		save `new_pop', replace
		use `master_new', clear
		append using `new_pop'
		save `master_new', replace
	}
	use `master_new', clear
	drop sex
	save `master', replace

	clear 
	tempfile beforeshape
	save `beforeshape', emptyok
	tempfile beforefreq
	save `beforefreq', emptyok
	
	** Loop through iso3-sex-sources
	use `master', clear
	levelsof ihme_loc_id, local(ihme_loc_id)
	foreach c of local ihme_loc_id {
		noisily: di "`c'"
		preserve
		keep if ihme_loc_id == "`c'"
		levelsof pop_years, local(c_years)

		foreach cy of local c_years {
			gen year1 = substr("`cy'", 1, strpos("`cy'"," ") - 1)
			gen year2 = substr("`cy'",strpos("`cy'"," ") + 1,.)
			destring year1, replace
			destring year2, replace
			replace year1 = floor(year1)
			replace year2 = floor(year2)
			levelsof year1, local(y1)
			levelsof year2, local(y2)
 			drop year1 year2

			save `beforeshape', replace

			use `deathdata', clear
			keep if ihme_loc_id == "`c'"

			keep if year >= `y1' & year <= `y2'

			if(_N > 1) {

				** First take only the death years with the most frequently occuring source type
				save `beforefreq', replace
				capture drop _freq
				contract source_type
				gsort - _freq
				levelsof source_type if _n == 1, clean local(sdloc)
				use `beforefreq', clear
				keep if source_type == "`sdloc'"
			
				** Then within that source type take only those years from the most frequently occuring source 
				save `beforefreq', replace
				capture drop _freq
				contract deaths_source
				gsort - _freq
				levelsof deaths_source if _n == 1, clean local(vrsource)
				use `beforefreq', clear
				
				if((strpos("`c'","CHNDYB") == 0) | (strpos("`c'","X") != 1)) {
					** Drop deaths_source if the maximum age gap differs from the maximum age gap of the most frequently occuring source
					** Find maximum age gap
					gen max_age_gap = 0
					gen age_gap_tempstore = 0
					forvalues i = 0/99 {
						local iplus = `i' + 1
						replace age_gap_tempstore = agegroup`iplus' - agegroup`i'
						replace max_age_gap = age_gap_tempstore if age_gap_tempstore > max_age_gap & age_gap_tempstore != .
					}
					** Store max age gap for the most frequently occuring source in a local
					bysort ihme_loc_id source_type deaths_source: egen max_temp = max(max_age_gap)
					levelsof max_temp if deaths_source == "`vrsource'", local(age_gap_most_frequent)
					gen most_frequent_age_gap = 0

					cap replace most_frequent_age_gap = `age_gap_most_frequent'
					drop if max_age_gap != most_frequent_age_gap & most_frequent_age_gap != 0 & deaths_source != "`vrsource'"
					drop max_age_gap age_gap_tempstore
				}   
			}

			if(_N == 0) {
				drop *
				set obs 1
				forvalues j = 0/100 {
					g deaths`j' = .
					g agegroup`j' = .			
				}		
				g deaths_years = "NA"
				g ihme_loc_id = "`c'"
				g pop_years = "`cy'"
				g deaths_source = "NA"
				g deaths_footnote = "NA"
				g source_type = "NA"
				g deaths_nid = "NA"
				g deaths_underlying_nid = "NA"
				g sex = substr("`c'",strpos("`c'","&&")+2,strpos("`c'","@@")-strpos("`c'","&&")-2)

			}
			else if(_N == 1) {
				levelsof year, clean local(vr_years)	
				drop year

				g sex = substr("`c'",strpos("`c'","&&")+2,strpos("`c'","@@")-strpos("`c'","&&")-2)
				g pop_years = "`cy'"
				g deaths_years = "`vr_years'"		
			}
			else {		
				
				* Generate number of distinct age groups
				gen age_group_count = 0
				forvalues i = 0/100 {
					replace age_group_count = age_group_count + 1 if !mi(agegroup`i')
				}
				levelsof age_group_count, clean local(num_age_group)
				local num_age_group_count: word count `num_age_group'
				gen num_age_group_count = `num_age_group_count'
				gen at_least_one_more_than_eleven = 0
				foreach var in `num_age_group' {
					replace at_least_one_more_than_eleven = 1 if `var' >= 11
				}
				drop if age_group_count < 11 & num_age_group_count != 1 & at_least_one_more_than_eleven == 1
				drop age_group_count

				** If there are no observations after VR year trimming, drop all, fill variables, and go to next source.
				if (_N == 0){
					drop *
					set obs 1
					forvalues j = 0/100 {
						g deaths`j' = .
						g agegroup`j' = .			
					}		
					g deaths_years = "NA"
					g ihme_loc_id = "`c'"
					g pop_years = "`cy'"
					g deaths_source = "NA"
					g deaths_footnote = "NA"
					g source_type = "NA"
					g deaths_nid = "NA"
					g deaths_underlying_nid = "NA"
					g sex = substr("`c'",strpos("`c'","&&")+2,strpos("`c'","@@")-strpos("`c'","&&")-2)
				}
				else {
					** Compile the sources and footnotes for each year 
					local count_y = 0
					levelsof year, clean local(vr_years)
					levelsof source_type, clean local(stloc)
					
					foreach vary of local vr_years {
						levelsof deaths_source if year == `vary', clean local(vrsource`count_y')	
						levelsof deaths_footnote if year == `vary', clean local(fn`count_y')
						levelsof deaths_nid if year == `vary', clean local(nid`count_y')
						levelsof deaths_underlying_nid if year == `vary', clean local(underlying_nid`count_y')
						local count_y = `count_y'+1
					}
					local count_y = `count_y'-1

					** Determine a kind of least common denominator, i.e. use the coarsest age group from age groups of death 
					** and population data for the census pair and average annual deaths

					g allagegroups = ""
					forvalues q = 0/100 {
						replace allagegroups = allagegroups + "," + string(agegroup`q') if agegroup`q' ~= .
					}
					replace allagegroups = allagegroups + ","
					levelsof allagegroups, local(allagegroups)
					local newagegroups = ""
					forvalues q = 0/100 {
						local inall = 0
						local all = 0
						foreach ag of local allagegroups {
							local all = `all'+1
							if(strpos("`ag'",",`q',") ~= 0) {
								local inall = `inall'+1
							}
						}	
				
						if(`inall' == `all') {
							local newagegroups = "`newagegroups'" + "`q' "
						}
						
					}
					
					local newagegroups = rtrim("`newagegroups'")

					tempfile beforeadd
					save `beforeadd', replace
					clear
					set obs 1
					g id = 1

					gen year1 = substr("`cy'", 1, strpos("`cy'"," ") - 1)
					gen year2 = substr("`cy'",strpos("`cy'"," ") + 1,.)
					destring year1, replace
					destring year2, replace
					replace year1 = floor(year1)
					replace year2 = floor(year2)
					levelsof year1, local(cyear1)
					levelsof year2, local(cyear2)
		 			drop year1 year2
		 			
					forvalues cyr = `cyear1'/`cyear2' {
						g year`cyr' = .
					}
					reshape long year, i(id)
					drop year id
					rename _j year
					drop if strpos("`vr_years'",string(year)) ~= 0
					tempfile afteradd
					save `afteradd', replace
					use `beforeadd', clear
					append using `afteradd'
					
					foreach vcombo1 of local vr_years {
						foreach vcombo2 of local vr_years {
							if(`vcombo1' < `vcombo2') {
								di "`vcombo1' `vcombo2'"
								forvalues j = 0/100 {
									egen vcombotemp = mean(deaths`j') if year == `vcombo1' | year == `vcombo2'
									sort vcombotemp
									replace deaths`j' = vcombotemp[_cons] if ihme_loc_id == "" & year > `vcombo1' & year < `vcombo2'
									drop vcombotemp
								}
								continue, break
							}
						}
					}
					

					summ year if ihme_loc_id ~= ""
					return list
					local minvryear = `r(min)'
					local maxvryear = `r(max)'
					
					local count1 = 0
					local count2 = 0
					
					forvalues cyr = `cyear1'/`cyear2' {
						if(`cyr' < `minvryear') {
							local count1 = `count1' + 1  
						}
						else if(`cyr' > `maxvryear') {
							local count2 = `count2' + 1
						}
					}
					
					local count1 = `count1' + 1
					local count2 = `count2' + 1
					
					forvalues j = 0/100 {
						replace deaths`j' = `count1'*deaths`j' if year == `minvryear' 
						replace deaths`j' = `count2'*deaths`j' if year == `maxvryear'
					}

					replace ihme_loc_id = "`c'" if ihme_loc_id == ""

					collapse (sum) deaths0-deaths100, by(ihme_loc_id)

					forvalues j = 0/100 {
						replace deaths`j' = deaths`j'/(`cyear2'-`cyear1'+1)
					}

					forvalues q = 0/100 {
						g agegroup`q' = ""
					}			
					local count = 0
					foreach newa of local newagegroups {
						replace agegroup`count' = "`newa'"
						destring agegroup`count', replace
						local count = `count'+1	
					}

					forvalues q = 0/100 {
						destring agegroup`q', replace
					}

					g pop_years = "`cy'"
					g deaths_years = "`vr_years'"
					g deaths_source = "`vrsource0'"
					g deaths_footnote = "`fn0'"
					g deaths_nid = "`nid0'"
					g deaths_underlying_nid = "`underlying_nid0'"
					g source_type = "`stloc'"
					g sex = substr("`c'",strpos("`c'","&&")+2,strpos("`c'","@@")-strpos("`c'","&&")-2)

					forvalues couy = 1/`count_y' {
						replace deaths_source = deaths_source + "#`vrsource`couy''" if !regexm(deaths_source,"`vrsource`couy''")
						replace deaths_footnote = deaths_footnote + "#`fn`couy''" if !regexm(deaths_footnote,"`fn`couy''")
						replace deaths_nid = deaths_nid + "#`nid`couy''" if !regexm(deaths_nid,"`nid`couy''") | !regexm(deaths_underlying_nid, "`underlying_nid`couy''")
						replace deaths_underlying_nid = deaths_underlying_nid + "#`underlying_nid`couy''" if !regexm(deaths_underlying_nid,"`underlying_nid`couy''") | !regexm(deaths_nid, "`nid`couy''")
					}
				}
			}

			tempfile aftershape
			save `aftershape', replace
				
			use `saveastemp', clear
			append using `aftershape'
			save `saveastemp', replace
			
			use `beforeshape', clear
		
		}
		restore
	}

	use `saveastemp', clear

	drop temp

	gen id = ihme_loc_id
	replace ihme_loc_id = substr(ihme_loc_id,1,strpos(ihme_loc_id,"&&")-1)

	sort ihme_loc_id pop_years sex
	
} 

save "`saveas'", replace
di "DONE"

end
