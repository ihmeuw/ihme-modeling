** NAME 
** NAME, modified May 2015
** 6/11/2013
** Purpose: format EMDAT data, taking into account length of disaster

clear all
pause on
set more off
cap restore, not

** for saving purposes:
local date = c(current_date)

if c(os)=="Windows" {
	global prefix=""
	}
if c(os)=="Unix" {
	global prefix=""
	}

// pdfstart.ado
cap do "FILEPATH"
local acrobat_distiller_path "FILEPATH"

do "FILEPATH"
create_connection_string, server("modeling-cod-db") database("shared") 
local conn_string `r(conn_string)'

do "FILEPATH"

cd "FILEPATH"

local output_folder "FILEPATH"


** want to make comparison with currently used data? let local "makecomparison" to be yes or no
local makecomparison = "no"
local old_date = "18 Jun 2013"
cap mkdir "FILEPATH"

** dB setup
do "FILEPATH"
create_connection_string, server("modeling-cod-db")
local conn_string `r(conn_string)'

** location_set_version_id
// fix hard-coding
local location_ver = 153 

** prep country codes
	// : Replaced flat file with DB query
	** Subnationals
clear
gen iso3 = ""
tempfile codes_sub
save `codes_sub', replace
foreach sub in CHN GBR MEX IND BRA JPN SAU SWE USA KEN ZAF IDN {
if inlist("`sub'", "JPN") local admin = 4
else if inlist("`sub'", "IND") local admin "4, 12"
else local admin = 3
odbc load, exec("SELECT location_id, location_ascii_name as location_name, region_name, ihme_loc_id FROM shared.location_hierarchy_history loc_hh JOIN shared.location USING (location_id) WHERE loc_hh.ihme_loc_id LIKE '`sub'%' AND loc_hh.location_type_id IN (`admin') AND loc_hh.location_set_version_id = `location_ver'") `conn_string' clear
replace ihme_loc_id = substr(ihme_loc_id, 1, 3)
rename (location_name region_name ihme_loc_id) (countryname gbd_region iso3)
if "`sub'" == "IND" {
replace countryname = subinstr(countryname, "?", "a", .) if regexm(countryname, "Arun\?chal|Bih\?r|Gujar\?t|Hary\?na|Karn\?taka|Mah\?r\?shtra|Megh\?laya|N\?g\?land|R\?jasth\?n|N\?du")
replace countryname = subinstr(countryname, "?", "i", .) if regexm(countryname, "Chhatt\?sgarh|Kashm\?r")
}
else if "`sub'" == "JPN" {
replace countryname = subinstr(countryname, "?", "o", .)
replace countryname = subinstr(countryname, "�", "O", .)
replace countryname = proper(countryname)
}
append using `codes_sub'
save `codes_sub', replace
}

	// Drop to keep countryname unique. There are Distrito Federal in both Mexico and Brazil, thankfully no Brazil DF in data.
	drop if countryname == "Distrito Federal" & iso3 == "BRA"
	save `codes_sub', replace

	** Nationals
	odbc load, exec("SELECT location_id, location_name, region_name, ihme_loc_id, location_type_id FROM shared.location_hierarchy_history WHERE location_type_id IN (2,3,4,8,12) AND location_set_version_id = `location_ver'") `conn_string' clear
	replace ihme_loc_id = substr(ihme_loc_id, 1, 3)
	drop if regexm(location_name, "County") & regexm(region_name, "High-income North America")
	// District of Columbia admin2
	drop if location_id == 891	
	rename (location_name region_name ihme_loc_id) (countryname gbd_region iso3)
	// Drop unnecessary codes
	drop if iso3 == "USA" & (location_type_id == 3 | location_type_id == 4)
	drop if location_type_id == 3 & inlist(iso3, "JPN", "BRA", "MEX", "IND")
	tempfile codes
	save `codes', replace


** subnational population weights
	use "FILEPATH", clear
		keep if sex == "both" & age_group_id == 22
		gen keep = 0
		foreach iso in BRA CHN GBR IDN IND JPN KEN MEX SWE SAU USA ZAF {
			replace keep = 1 if regexm(ihme_loc_id, "`iso'")
		}
		replace keep = 0 if ihme_loc_id == "CHN_44533"
		drop if keep == 0
		drop sex* age* parent_id keep source location_id
		drop if length(ihme_loc_id)==3
		rename pop pop_
		// Drop India state, only use urban rural
			gen iso_length = length(ihme_loc_id)
			drop if regexm(ihme_loc_id, "IND_") & iso_length == 8
			drop iso_length
		// Drop England
			drop if ihme_loc_id == "GBR_4749"
		levelsof ihme_loc_id, local(ids) clean
		bysort ihme_loc_id : assert location_name == location_name[1]
		foreach id of local ids {
			levelsof location_name if ihme_loc_id == "`id'", local(`id'_name) clean
		}
		drop location_name *egion_id level
		reshape wide pop_, i(year) j(ihme_loc_id) string
		foreach iso in BRA CHN GBR IDN IND JPN KEN MEX SWE SAU USA ZAF  {
			looUSER pop_`iso'
			local varlist `r(varlist)'
			egen pop_`iso'_tot = rowtotal(pop_`iso'_*)
			foreach var of local varlist {
				local stub = subinstr("`var'", "pop_", "", 1)
				gen weight_`stub' = `var' / pop_`iso'_tot
			}
		}
		drop *tot pop_*
		reshape long weight_, i(year) j(ihme_loc_id) string
		drop if weight_ == .
		split ihme_loc_id, parse("_")
		rename ihme_loc_id1 iso3
		rename ihme_loc_id2 location_id
		rename weight_ weight
		drop if location_id == "tot"
		destring location_id, replace
		gen gbd_country_iso3 = iso3
		gen location_name = ""
		foreach id of local ids {
			replace location_name = "``id'_name'" if ihme_loc_id == "`id'"
		}

		rename year disyear
		expand 25
		gen type = ""
		bysort disyear location_id : gen indic = _n
			replace type = "Air" if indic == 1
			replace type = "Chemical spill" if indic == 2
			replace type = "Cold wave" if indic == 3
			replace type = "Collapse" if indic == 4
			replace type = "Drought" if indic == 5
			replace type = "Earthquake" if indic == 6
			replace type = "Explosion" if indic == 7
			replace type = "Famine" if indic == 8
			replace type = "Fire" if indic == 9
			replace type = "Flood" if indic == 10
			replace type = "Gas leak" if indic == 11
			replace type = "Heat wave" if indic == 12
			replace type = "Other" if indic == 13
			replace type = "Other Geophysical" if indic == 14
			replace type = "Other hydrological" if indic == 15
			replace type = "Poisoning" if indic == 16
			replace type = "Rail" if indic == 17
			replace type = "Road" if indic == 18
			replace type = "Volcanic activity" if indic == 19
			replace type = "Water" if indic == 20
			replace type = "Wildfire" if indic == 21
			replace type = "legal intervention" if indic == 22
			replace type = "storm" if indic == 23
			replace type = "terrorism" if indic == 24
			replace type = "war" if indic == 25
			replace type = "Cholera" if indic == 26
			replace type = "Measles" if indic == 27
			replace type = "Meningitis" if indic == 28
			replace type = "Measles" if indic == 29

		drop indic
		isid disyear location_id type
		gen _merge = 1

		tempfile subnatl_popweight
		save `subnatl_popweight', replace


	// Find pop-weights to split India states by urbanicity
	get_location_metadata, location_set_id(35) clear
		keep if (location_type == "admin2" | location_type == "urbanicity") & regexm(ihme_loc_id, "IND_")
		keep parent_id location_id
		tempfile india_urbanicity
		save `india_urbanicity', replace

	use "FILEPATH", clear
		// For some reason age_group 22 doesn't have level values
		levelsof ihme_loc_id if regexm(ihme_loc_id, "IND_") & level == 5, local(inds) clean
		gen keep = 0
		foreach iso of local inds {
			qui replace keep = 1 if ihme_loc_id == "`iso'"
		}
		keep if keep == 1
		keep if sex == "both" & age_group_id == 22
		drop sex* age* parent_id keep source location_id location_name
		rename pop pop_
		split ihme_loc_id, parse("_")
		drop ihme_loc_id1
		rename ihme_loc_id2 location_id
		destring location_id, replace

		merge m:1 location_id using `india_urbanicity', assert(3) nogen
		bysort parent_id year : egen total = total(pop)
		gen weight = pop / total
		keep year weight location_id parent_id

		rename year disyear

		** for Bihar flood U/R split
		preserve
			keep if disyear == 1987 & parent_id == 4844
			tempfile bihar_urban_rural
			save `bihar_urban_rural', replace
		restore

		expand 29
		gen type = ""
		bysort disyear location_id : gen indic = _n
			replace type = "Air" if indic == 1
			replace type = "Chemical spill" if indic == 2
			replace type = "Cold wave" if indic == 3
			replace type = "Collapse" if indic == 4
			replace type = "Drought" if indic == 5
			replace type = "Earthquake" if indic == 6
			replace type = "Explosion" if indic == 7
			replace type = "Famine" if indic == 8
			replace type = "Fire" if indic == 9
			replace type = "Flood" if indic == 10
			replace type = "Gas leak" if indic == 11
			replace type = "Heat wave" if indic == 12
			replace type = "Other" if indic == 13
			replace type = "Other Geophysical" if indic == 14
			replace type = "Other hydrological" if indic == 15
			replace type = "Poisoning" if indic == 16
			replace type = "Rail" if indic == 17
			replace type = "Road" if indic == 18
			replace type = "Volcanic activity" if indic == 19
			replace type = "Water" if indic == 20
			replace type = "Wildfire" if indic == 21
			replace type = "legal intervention" if indic == 22
			replace type = "storm" if indic == 23
			replace type = "terrorism" if indic == 24
			replace type = "war" if indic == 25
			replace type = "Cholera" if indic == 26
			replace type = "Yellow Fever" if indic == 27
			replace type = "Meningitis" if indic == 28
			replace type = "Measles" if indic == 29

		drop indic
		isid disyear location_id type
		save `india_urbanicity', replace

	// Get Indian state location_ids
	get_location_metadata, location_set_id(35) clear
		keep if location_type == "admin1" & regexm(ihme_loc_id, "IND_")
		keep location_id location_ascii_name
		rename location_ascii_name countryname
		tempfile india_states
		save `india_states', replace


** bring in online supplement data
	** Andorra, United Arab Emirates, Marshall Islands, and Qatar
		local countries1 = "AND ARE MHL QAT"
		tempfile and_are_mhl_qat
		foreach country of local countries1 {
			import excel "FILEPATH", firstrow clear sheet( "`country'")
			keep if type == "Disaster"
			count
			if `r(N)' > 0 {
				if nid == . continue
				** format: each event is unique, so can add the deaths
				reshape long year_, i(source website type table_number_or_text_section comments nid) j(year)
				rename year_ Total
				drop source website type table_number_or_text_section comments

				collapse (sum) Total, by(year nid)

				** drop observations that don't have any deaths -- these 0s mean that no data were extracted for these years
				drop if Total == 0
				drop if Total < 10

				generate iso3 = "`country'"
				generate countryname = "Andorra" if "`country'" == "AND"
				replace countryname = "United Arab Emirates" if "`country'" == "ARE"
				replace countryname = "Marshall Islands" if "`country'" == "MHL"
				replace countryname = "Qatar" if "`country'" == "QAT"

				cap append using `and_are_mhl_qat'
				save `and_are_mhl_qat', replace
			}
		}

	** Philippines
	import excel "FILEPATH", firstrow clear sheet( "Sheet1")
	keep if type == "disaster"

		** initial formatting
		drop if Specificname == "" & source == "" & website == "" & table == ""
		drop if inlist(Cause, "Malaria", "Measles", "Small pox", "Diarrhoeal/Enteric")
		cap rename NID nid
		drop if nid == .

		** format: make each event is unique, so can add the deaths
		destring year*, replace
			** there are some duplicates: average these
				** these one are not actually duplicates
				drop if Dup == 1 & year_1990 != .
				drop if Dup == 1 & year_1991 != .

				preserve
				keep if Dup != .
				collapse (mean) year_*, by(type Cause Specificname source nid website table_number_or_text_section comments Duplicate)
				tempfile phldup
				save `phldup', replace
				restore

			** for all others, sum
			keep if Dup == .
			collapse (sum) year_*, by(type Cause Specificname source nid website table_number_or_text_section comments Duplicate)
			append using `phldup'

		reshape long year_, i(type Cause Specificname source nid website table_number_or_text_section comments Duplicate) j(year)
		rename year_ Total
		drop type Cause Specificname source website table_number_or_text_section comments Duplicate

		collapse (sum) Total, by(year nid)

		** drop observations that don't have any deaths -- these 0s mean that no data were extracted for these years
		drop if Total == 0
		drop if Total < 10

		generate iso3 = "PHL"
		generate countryname = "Philippines"

	tempfile phl
	save `phl', replace

	append using `and_are_mhl_qat'
	gen source = "Online supplement"

	** merge on country codes information
	merge m:1 iso3 countryname using `codes'
	assert _m != 1
	keep if _m==3
	drop _m
	gen cause = "Natural disaster"
	tempfile supplement
	save `supplement', replace

**import who epidemic data --  3/28/17 -- The disaster_no variable was incorrect in the previous version of the file.
**	 	This mislabeled causes, but has been fixed in the _test_3_28_17 version of the file.
** 		The filename can be changed once it is confirmed that this solved the problem. //  3/28/17
* rename deaths total_deaths
* ** one duplicate, easy enough to drop.
* duplicates drop country_name dis_subtype disaster_no end_date start_date iso total_deaths continent_orig, force
import delimited using "FILEPATH", clear // firstrow // excel formatted the dates weirdly, switching back to csv.
drop population_split _merge start_date_x end_date_x  
rename start_date_y start_date
rename end_date_y end_date
replace country_name = "Guinea-Bissau" if country_name == "Guinea-Bisseau"
rename note source
replace source = "EMDAT" if source == "EM-DAT"
keep cause country_name dis_subtype end_date start_date iso total_deaths continent_orig pop* source disaster_type_orig location 
tempfile epidemic_data
save `epidemic_data', replace

** bring in EMDAT data
	import excel using "FILEPATH", clear firstrow
	generate source = "EMDAT"
	** merge on epidemic causes
	** there are four blank rows that we need to drop (an artifact of scraping)
	duplicates drop country_name dis_subtype disaster_no end_date start_date iso total_deaths continent_orig, force
	** only use Epidemics from research
	drop if disaster_type_orig == "Epidemic"
	append using `epidemic_data'
	replace dis_subtype = cause if regexm(source, "WHO") | source == "Gideon" | regexm(source, "Sidd") | (source == "EMDAT" & cause == "cholera")
	replace dis_type = cause if regexm(source, "WHO") | source == "Gideon" | regexm(source, "Sidd") | (source == "EMDAT" & cause == "cholera")
	* merge 1:1 country_name dis_subtype disaster_no end_date start_date iso total_deaths continent_orig using `epidemic_causes'
	* replace dis_subtype = cause if _m == 3
	* replace dis_type = cause if _m == 3
	* drop _m

	** this shouldn't be split over two years:
	replace end_date = "12/31/1985" if iso  == "BTN" & dis_type == "meningitis" 
	replace end_date = "12/31/1998" if iso  == "CHE" & dis_type == "meningitis" & end_date == "1/6/1999"
	replace end_date = "12/31/1999" if iso  == "CHE" & dis_type == "meningitis" & end_date == "1/6/2000"

	** initial formatting
		** properly name variables
		/*
		rename v1 startdate
		rename v2 enddate
		rename v3 country
		rename v4 iso
		rename v5 location
		rename v6 type
		rename v7 subtype
		rename v8 numkilled
		rename v9 numaffected
		rename v10 estimateddamage
		rename v11 disasternum
		*/

		** drop irrelevant variables, fix some stuff
		drop if dis_type == ""
		* drop if inlist(disaster_group_orig, "Biological", "Extra_terrestrial", "Technological")
		* NOW LIMIT BASED ON TYPE
		replace dis_type = proper(dis_type)
		replace dis_subtype = proper(dis_subtype)
		drop if inlist(dis_type, "Insect Infestation", "Animal Accident") // Biological, but keep epidemics
		drop if inlist(dis_type, "Impact", "Space Weather") // Extraterrestrial
		drop if inlist(dis_type, "Industrial Accident", "Transport Accident", "Miscelleanous Accident", "Miscellaneous Accident") // Technological
		drop A associated_dis* continent_orig insured_losses
		drop total_affected total_dam disaster_no

		rename (dis_type dis_subtype) (type subtype)
		rename total_deaths numkilled
		rename (start_date end_date) (startdate enddate)

		** if number killed is missing, make it equal to 0
		replace numkilled = 0 if numkilled== .

		** drop if number of deaths is less than 10
		drop if numkilled < 10 & disaster_type_orig != "Epidemic"
		drop disaster_type_orig

	** merge on iso3 codes
	rename country_name countryname

		replace countryname = trim(countryname)

		** need to first make some changes to country names so that they'll merge
		** drop the countries that no longer exist
		**  1/14/2016: Fixing some of the below drops
		replace countryname = "Russia" if countryname == "Soviet Union"
		replace countryname = "Germany" if countryname == "Germany Fed Rep" | countryname == "Germany Dem Rep"
		replace countryname = "Portugal" if countryname == "Azores"
		replace countryname = "Spain" if countryname == "Canary Is"
		// split Serbia and Montenegro
		replace countryname = "Serbia" if countryname == "Serbia Montenegro"  & (regexm(location, "Belgrade") | regexm(location, "Novi Sad") | regexm(location, "Belgrade") | regexm(location, "Sokobanja") | regexm(location, "Belgrad") | location == "")
		replace countryname = "Montenegro" if countryname == "Serbia Montenegro"  & (regexm(location, "Bijelo Polje") | regexm(location, "Podgorica") | regexm(location, "Belgrad") )
		replace countryname = "Kosovo" if countryname == "Serbia Montenegro"  & ( regexm(location, "Pristina") | regexm(location, "Kosovo") )
		// Split Czechoslovakia
		replace countryname = "Czech Republic" if regexm(countryname, "Czechoslovakia") // all of these cities are in modern-day Czechoslovakia
		// Split Yugoslavia
		replace countryname = "Macedonia" if countryname == "Yugoslavia"  & (regexm(location, "Skopje") )
		replace countryname = "Montenegro" if countryname == "Yugoslavia"  & (regexm(location, "Montenegro") )
		replace countryname = "Bosnia and Herzegovina" if countryname == "Yugoslavia"  & (regexm(location, "Kakanj") | regexm(location, "Bosnia") )
		replace countryname = "Serbia" if countryname == "Yugoslavia" & (regexm(location, "Belgrade") | regexm(location, "Stalac") | regexm(location, "Resavica") | regexm(location, "Lapovo") | regexm(location, "Aleksina") | regexm(location, "Northeastern") | location == "")
		replace countryname = "Croatia" if countryname == "Yugoslavia" & (regexm(location, "Zagreb") )
		replace countryname = "Slovenia" if countryname == "Yugoslavia" & (regexm(location, "Divaca") | regexm(location, "Jablanica"))

		// 11/18/16 nothing left to drop, all data accounted for
		* drop if inlist(countryname, "Azores", "Soviet Union", "Canary Is", "Czechoslovakia", "Germany Dem Rep", "Germany Fed Rep", "Serbia Montenegro", "Yugoslavia") | regexm(countryname, "Wallis")==1

		** some automated changes
		split countryname, parse( " (")
		split countryname2, parse( ")")
		replace countryname = countryname1+", "+ countryname21 if countryname21 != ""
						// if regexm(countryname2,"China")==1 & countryname1 != "Taiwan"
			replace countryname = countryname1 if countryname1 == "Taiwan"
			replace countryname = countryname21 if countryname1 == "Palestine"
			replace countryname = countryname21+ " "+countryname1 if countryname21=="US"
			replace countryname = "British " + countryname1 if countryname21 == "UK"
		drop countryname1-countryname21

		* replace countryname = subinstr(countryname, "Rep", "Republic",.)
			replace countryname = subinstr(countryname, " Republic", ", Republic of",.) if regexm(countryname,"Moldova")==1
		replace countryname = subinstr(countryname, " P ", " People's ",.)
			replace countryname = subinstr(countryname, " P ", " People's ",.) if regexm(countryname,"Korea Dem")==1
		replace countryname = subinstr(countryname, " Dem ", " Democratic ", .)
		replace countryname = subinstr(countryname, " Dem. ", " Democratic ", .)
		replace countryname = subinstr(countryname, " Is", " Islands",.)
			replace countryname = "Iran, Islamic Republic of" if countryname == "Iran, Islandslamic Republic of"
			replace countryname = subinstr(countryname, "Islandsland", "Island",.)
		replace countryname = subinstr(countryname, "St ", "St. ",.)

		** manual changes
		replace countryname = "The Bahamas" if countryname == "Bahamas" | countryname == "Bahamas, the"
		replace countryname = "Virgin Islands, British" if countryname == "British Virgin Islands"
		replace countryname = "Virgin Islands, U.S." if countryname == "US Virgin Islands"
		replace countryname = "Brunei" if countryname == "Brunei Darussalam"
		replace countryname = "Bosnia and Herzegovina" if countryname == "Bosnia-Hercegovenia"
		replace countryname = "Bolivia" if regexm(countryname, "Bolivia")
		replace countryname = "Cape Verde" if countryname == "Cape Verde Islands" | countryname == "Cabo Verde"
		replace countryname = "Congo" if countryname == "Congo (the)"
		replace countryname = "Cote d'Ivoire" if countryname == "Cte dIvoire"
		replace countryname = "China" if countryname == "China People's Republic"
		replace countryname = "Federated States of Micronesia" if countryname == "Micronesia, Federated States of"
		replace countryname = "The Gambia" if regexm(countryname, "Gambia")
		replace countryname = "Germany, Former Democratic Republic" if countryname == "Germany Democratic Republic"
		replace countryname = "Germany, Former Federal Republic" if countryname == "Germany Fed Republic"
		replace countryname = "Guinea-Bissau" if countryname == "Guinea Bissau"
		replace countryname = "Hong Kong Special Administrative Region of China" if countryname == "Hong Kong, China" | countryname == "Hong Kong Special Administrative Region of China" | countryname == "Hong Kong"
		replace countryname = "Iran" if countryname == "Iran, Islamic Republic of"
		replace countryname = "Iraq" if countryname == "Iraq, Republic of"
		replace countryname = "Laos" if countryname == "Lao People's Democratic Republic, the"
		replace countryname = "Libya" if countryname == "Libyan Arab Jamah"
		replace countryname = "Macao Special Administrative Region of China" if countryname == "Macau"
		replace countryname = "Macedonia" if regexm(countryname, "Macedonia")==1
		replace countryname = "Netherlands" if regexm(countryname, "Netherlands")
		replace countryname = subinstr(countryname, " Fed ", ", Federated ",.)
		replace countryname = "Moldova" if countryname == "Moldova, the, Republic of of"
		replace countryname = "Portugal" if regexm(countryname, "Azores")
		replace countryname = "Philippines" if regexm(countryname, "Philippines")
		replace countryname = "South Korea" if countryname == "Korea Republic" | countryname == "Korea, the Republic of"
		replace countryname = "North Korea" if countryname == "Korea, the Democratic People's Republic of"
		replace countryname = "Reunion" if countryname == "Runion"
		replace countryname = "Russia" if regexm(countryname, "Russian Federation")
		replace countryname = "Saint Kitts and Nevis" if countryname == "St. Kitts and Nevis"
		replace countryname = "Saint Helena" if countryname == "St. Helena"
		replace countryname = "Saint Lucia" if countryname == "St. Lucia"
		replace countryname = "Saint Vincent and the Grenadines" if countryname == "St. Vincent and the Grenadines"
		replace countryname = "Sao Tome and Principe" if countryname == "Sao Tome et Principe"
		replace countryname = "Serbia and Montenegro" if countryname == "Serbia Montenegro"
		replace countryname = "Serbia" if countryname == "Kosovo"
		replace countryname = "South Korea" if countryname == "Korea, Republic of"
		replace countryname = "St. Vincent and the Grenadines" if countryname == "St. Vincent and The Grenadines"
		replace countryname = "Syria" if countryname == "Syrian Arab Republic"
		replace countryname = "Tanzania" if countryname == "Tanzania, United Republic of"
		replace countryname = "The Gambia" if countryname == "Gambia The"
		replace countryname = "United Arab Emirates" if countryname == "United Arab Emirates, the"
		replace countryname = "United Kingdom" if countryname == "United Kingdom of Great Britain and Northern Irela"
		replace countryname = "Venezuela" if countryname == "Venezuela, Bolivarian Republic of"
		replace countryname = "Vietnam" if countryname == "Viet Nam"
		replace countryname = "Yemen" if regexm(countryname, "Yemen")==1
		replace countryname = "Democratic Republic of the Congo" if countryname== "Zaire/Congo Democratic Republic" | countryname == "Congo, the Democratic Republic of the"  | countryname == "Congo (the Democratic Republic of the)"
				replace countryname = "Palestine" if countryname == "Palestine, State of"

		** need to split countryname againt to get rid of extra "the" in some countries
		split countryname, parse (", ")
		replace countryname = countryname1 if countryname2 == "the"
		drop countryname1 countryname2
		replace countryname = "United States" if regexm(countryname, "United States of America")

		** drop the countries that are not in CoD computation
		drop if inlist(countryname, "Anguilla", "Cayman Islands", "Cook Islands", "French Polynesia", "Guadeloupe", "Martinique", "Mayotte") ///
				| inlist(countryname, "Montserrat", "Netherlands Antilles", "New Caledonia", "Niue", "Palau", "Reunion", "Saint Helena", "Saint Kitts and Nevis") ///
				| inlist(countryname, "Tokelau", "Turks and Caicos Islands", "Tuvalu", "Virgin Islands, British", "West Bank", "French Guiana")

		** time to merge on the iso3 codes
		merge m:1 countryname using `codes'

			drop if _m==2
			assert _m==3
			drop _m

		replace iso3 = iso if regexm(source, "WHO") | source == "Gideon" & iso != iso3
	** figure out start and end date
		foreach var of varlist startdate enddate {
			split `var', parse( "/")
				rename `var'1 `var'_day
				rename `var'2 `var'_month
				rename `var'3 `var'_year
		}

		drop startdate enddate

		foreach var of varlist startdate_day-enddate_year {
			destring `var', replace
		}

		** fix Angola 2016 and BanglUSER 1982 cholera:
		replace startdate_year = 2016 if iso3 == "AGO" & type == "Yellow Fever"
		replace startdate_year = 1982 if iso3 == "BGD" & type == "Cholera" & source == "EMDAT" & enddate_year == 1982

	** is it a multi-year conflict?
	gen numyears = enddate_year-startdate_year+1


	** This may be causing us to project disasters into the future. Drop these copies to prevent this.
	duplicates drop
		** if it is a multi-year conflict, divide the number of deaths by length of interval
		**		and interpolate years between start year and end year
		expand numyears if numyears != 1
		sort iso type subtype startdate_year startdate_month startdate_day

		bysort iso type subtype startdate_year startdate_month startdate_day numkilled: gen nn = _n-1
		gen disyear = startdate_year+nn
		replace disyear = enddate_year+nn if disyear == .

		** do some checks
		assert startdate_year == enddate_year if numyears == 1
		assert disyear == startdate_year+nn if startdate_year != .

		** now replace the number killed by the length of the interval
		replace numkilled = numkilled/numyears

			** CHECK HERE TO SEE IF THERE ARE UNDERLYING DIFFERENCES IN THE DATA BETWEEN UPDATE AND PREVIOUS VERSION
			if "`makecomparison'" == "yes" {
				tempfile temp
				save `temp', replace

				preserve
				keep iso3 numkilled disyear
				rename numkilled newtot
				rename disyear year
				collapse (sum) newtot, by(iso3 year)
				merge 1:1 iso3 year using "FILEPATH"
				keep if ihme_indic == 1
				keep iso3 year Total newtot
				rename Total oldtot
				gen absdiff = abs(oldtot-newtot)
				gen percentdiff = (absdiff/oldtot)*100
				count if percentdiff >= 1
				di in red "There are `r(N)' country-years with a >=1% difference between new and currently used TOTAL death numbers"

					** graph
					levelsof iso3, local(isos) clean
					quietly do "FILEPATH"
					pdfstart using "FILEPATH", distexe("`acrobat_distiller_path'")

					foreach iso of local isos {
						tw scatter oldtot year if iso3 == "`iso'" & percentdiff <1 & percentdiff != ., mcol(red) msymb(O) || ///
						scatter oldtot year if iso3 == "`iso'" & percentdiff >=1 & percentdiff != ., mcol(red) msymb(X) || ///
						scatter newtot year if iso3 == "`iso'" & percentdiff <1 & percentdiff != ., mcol(blue) msymb(Th) || ///
						scatter newtot year if iso3 == "`iso'" & percentdiff >=1 & percentdiff != ., mcol(blue) msymb(X)  ///
						title( "Currently used vs. updated disaster death numbers, `iso'") xtitle( "Year") ytitle( "Number of deaths") ///
						legend(label(1 "Currently used, <1% diff") label(2 "Currently used, >=1% diff") label(3 "Updated, <1% diff") label(4 "Updated, >=1% diff")) ///
						note( "X's denote years that have a >=1% diff between new and old") ///
						subtitle( "Before redistrib. of deaths in multi-yr disasters")
						pdfappend
					}
					pdffinish, view
				restore
			} // end `makecomparison'
			***************************
	replace location = "Sumatera Barat" if iso == "IDN" & disyear == 2009 & regexm(location, "Agam, Kepulauan Mentawai, Kota Bukitinggi, Kota Padang, Kota Padangpanjang, Kota Pariaman, Kota Pasaman, Kota Pasaman Barat")
	replace location = "Bihar" if iso == "IND" & disyear == 1987 & subtype == "Riverine Flood"
	drop numyears nn
	tempfile all_emdat_data
	save `all_emdat_data', replace
	
	* ** split the epidemics myself.
	* keep if (inlist(iso3, "CHN", "IDN", "IND", "BRA", "KEN") | (iso3 == "SDN" & population_split == "Yes")) & note != ""
	* merge 1:m iso3 disyear type using `subnatl_popweight'


	** for epidemic testL just use location for subnat-bearing locs
	replace location = "" if !(inlist(iso3, "CHN", "MEX", "GBR", "IDN", "IND", "BRA", "JPN")) & !(inlist(iso3,"SAU","SWE","USA","KEN","ZAF"))

	** add in subnational data for MEX, CHN, UK, IND, BRA, JPN, SAU, SWE, USA
	** duplicate the data that is for these countries, and then assign the duplicates to the subnational locations -- epidemics get special treatment
	use `all_emdat_data', clear
	keep if (inlist(iso3, "CHN", "MEX", "GBR", "IDN", "IND", "BRA", "JPN")|inlist(iso3,"SAU","SWE","USA","KEN","ZAF")) // & note == ""


		** keep only years post 1989 and disasters with more than 300 deaths
		//keep if disyear > 1989
		** split out the province/states
		** if it's missing, or is incomplete (has an ellipses), make it so that it says "not specified"
		replace location = "not specified" if location == ""
		drop if location == "not specified"
		replace location = subinstr(location, " ,", ",", .)
		split location, parse( ",")

		** reshape the data so that each line is an event-region
		rename (location location_id) (mainlocation mainlocation_id)
		generate event_id = _n
		reshape long location, i(event_id) j(area_number)
		drop if location == ""
		replace location = trim(location)
		foreach char in 09 10 11 12 13 {
			replace location = subinstr(location, char(`char'), "", .)
		}

			** if it's not a full province you can figure out, make it missing, and drop it
			replace location = subinstr(location, " ...", "",.)
			replace location = subinstr(location, "province", "", .)
			replace location = subinstr(location, "pr", "", .)
			replace location = subinstr(location, " ", "", 1) if location != "not specified" & !inlist(iso3, "BRA", "GBR", "MEX")
			drop if location == ""


			** fix the location names so you can merge on country-codes; drop the ones you can't figure out
			drop if inlist(location, "C", "G", "Gu", "He", "Ji", "S")


			************************************************************************************8
			************************************************************************************8
			************************************************************************************8
			************************************************************************************8

			tempfile sub_work

			*****************************************************************************
			** IDN
			*****************************************************************************

			preserve
			keep if iso3 == "IDN"

			replace location = "Aceh" if regexm(location, "Aceh")
			replace location = "Sumatera Utara" if regexm(location, "Sumatera Utara")
			replace location = "Sumatera Barat" if regexm(location, "Sumatera Barat")
			replace location = "Riau" if regexm(location, "Riau")
			replace location = "Jambi" if regexm(location, "Jambi")
			replace location = "Sumatera Selatan" if regexm(location, "Sumatera Selatan")
			replace location = "Bengkulu" if regexm(location, "Bengkulu")
			replace location = "Lampung" if regexm(location, "Lampung")
			replace location = "Banka Belitung" if regexm(location, "Banka Belitung")
			replace location = "Kepulauan Riau" if regexm(location, "Kepulauan Riau")
			replace location = "Jakarta" if regexm(location, "Jakarta")
			replace location = "Jawa Barat" if regexm(location, "Jawa Barat")
			replace location = "Jawa Tenga" if regexm(location, "Jawa Tenga")
			replace location = "Yogyakarta" if regexm(location, "Yogyakarta")
			replace location = "Jawa Timur" if regexm(location, "Jawa Timur")
			replace location = "Banten" if regexm(location, "Banten")
			replace location = "Bali" if regexm(location, "Bali")
			replace location = "Nusa Tenggara Barat" if regexm(location, "Nusa Tenggara Barat")
			replace location = "Nusa Tenggara Timur" if regexm(location, "Nusa Tenggara Timur")
			replace location = "Kalimantan Barat" if regexm(location, "Kalimantan Barat")
			replace location = "Kalimantan Tengah" if regexm(location, "Kalimantan Tengah")
			replace location = "Kalimantan Selatan" if regexm(location, "Kalimantan Selatan")
			replace location = "Kalimantan Timur" if regexm(location, "Kalimantan Timur")
			replace location = "Sulawesi Utara" if regexm(location, "Sulawesi Utara")
			replace location = "Sulawesi Tengah" if regexm(location, "Sulawesi Tengah")
			replace location = "Sulawesi Selatan" if regexm(location, "Sulawesi Selatan")
			replace location = "Sulawesi Tenggara" if regexm(location, "Sulawesi Tenggara")
			replace location = "Gorontalo" if regexm(location, "Gorontalo")
			replace location = "Sulawesi Barat" if regexm(location, "Sulawesi Barat")
			replace location = "Maluku" if regexm(location, "Maluku")
			replace location = "Maluku Utara" if regexm(location, "Maluku Utara")
			replace location = "Papua Barat" if regexm(location, "Papua Barat")
			replace location = "Papua" if regexm(location, "Papua")


			save `sub_work', replace
			restore
			drop if iso3 == "IDN"
			append using `sub_work'


			*****************************************************************************
			** CHN
			*****************************************************************************
			preserve
			keep if iso3 == "CHN" | iso3 == "HKG"

			expand 2 if location == "Cangnans" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Fujian" if location == "Cangnans" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Zhejiang" if location == "Cangnans" & iso3 == "CHNG" & multiprov_indic == 2
					drop multiprov_indic
			expand 3 if location == "South"	& iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
				replace location = "Guangdong" if location == "South" & iso3 == "CHN" & multiprov_indic == 1
				replace location = "Hainan" if location == "South" & iso3 == "CHN" & multiprov_indic == 2
				replace location = "Guangxi" if location == "South" & iso3 == "CHN" & multiprov_indic == 3
				drop multiprov_indic
			expand 8 if location == "Centraland Northern China" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Beijing" if location == "Centraland Northern China" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Tianjin" if location == "Centraland Northern China" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Hebei" if location == "Centraland Northern China" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Shaanxi" if location == "Centraland Northern China" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Inner Mongolia" if location == "Centraland Northern China" & iso3 == "CHN" & multiprov_indic == 5
					replace location = "Henan" if location == "Centraland Northern China" & iso3 == "CHN" & multiprov_indic == 6
					replace location = "Hubei" if location == "Centraland Northern China" & iso3 == "CHN" & multiprov_indic == 7
					replace location = "Hunan" if location == "Centraland Northern China" & iso3 == "CHN" & multiprov_indic == 8
					drop multiprov_indic
			expand 8 if location == "Centraland southern parts" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Henan" if location == "Centraland southern parts" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Hubei" if location == "Centraland southern parts" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Hunan" if location == "Centraland southern parts" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Guangdong" if location == "Centraland southern parts" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Guangxi" if location == "Centraland southern parts" & iso3 == "CHN" & multiprov_indic == 5
					replace location = "Hainan" if location == "Centraland southern parts" & iso3 == "CHN" & multiprov_indic == 6
					replace location = "Hong Kong Special Administrative Region of China" if location == "Centraland southern parts" & iso3 == "CHN" & multiprov_indic == 7
					replace location = "Macao Special Administrative Region of China" if location == "Centraland southern parts" & iso3 == "CHN" & multiprov_indic == 8
					drop multiprov_indic
			expand 2 if location == "Dongguancity (Guangdong ); Fujian" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Guangdong" if location == "Dongguancity (Guangdong ); Fujian" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Fujian" if location == "Dongguancity (Guangdong ); Fujian" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 7 if location == "Easternand central China" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Liaoning" if location == "Easternand central China" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Jilin" if location == "Easternand central China" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Heilongjiang" if location == "Easternand central China" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Inner Mongolia" if location == "Easternand central China" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Henan" if location == "Easternand central China" & iso3 == "CHN" & multiprov_indic == 5
					replace location = "Hubei" if location == "Easternand central China" & iso3 == "CHN" & multiprov_indic == 6
					replace location = "Hunan" if location == "Easternand central China" & iso3 == "CHN" & multiprov_indic == 7
					drop multiprov_indic
			expand 2 if location == "Guandongs (China) Peng-hu (Taiwan)" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Guangdong" if location == "Guandongs (China) Peng-hu (Taiwan)" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Taiwan" if location == "Guandongs (China) Peng-hu (Taiwan)" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "GuangxiZhuang Autonomous s; Chongqing municipality" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Guangxi" if location == "GuangxiZhuang Autonomous s; Chongqing municipality" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Chongqing" if location == "GuangxiZhuang Autonomous s; Chongqing municipality" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "Guangzhou(city) (Guangdong and Fujian Provinces)" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Guangdong" if location == "Guangzhou(city) (Guangdong and Fujian Provinces)" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Fujian" if location == "Guangzhou(city) (Guangdong and Fujian Provinces)" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "GuizhouYunnan" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Guizhou" if location == "GuizhouYunnan" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Yunnan" if location == "GuizhouYunnan" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "Liaoning& Jilin Provinces" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Liaoning" if location == "Liaoning& Jilin Provinces" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Jilin" if location == "Liaoning& Jilin Provinces" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "Lijiang-Yanyuanarea (Sichuan-Yunnan Border Region)" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Sichuan" if location == "Lijiang-Yanyuanarea (Sichuan-Yunnan Border Region)" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Yunnan" if location == "Lijiang-Yanyuanarea (Sichuan-Yunnan Border Region)" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "Linxia(Gansu ); Hunan " & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Gansu" if location == "Linxia(Gansu ); Hunan " & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Hunan" if location == "Linxia(Gansu ); Hunan " & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "NinglangYi Zizhixian (Yunnan Sheng ) and Yanyuan Xian county (Sichuan Sheng )" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Yunnan" if location == "NinglangYi Zizhixian (Yunnan Sheng ) and Yanyuan Xian county (Sichuan Sheng )" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Sichuan" if location == "NinglangYi Zizhixian (Yunnan Sheng ) and Yanyuan Xian county (Sichuan Sheng )" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 3 if location == "Nord-Easts" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Liaoning" if location == "Nord-Easts" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Jilin" if location == "Nord-Easts" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Heilongjiang" if location == "Nord-Easts" & iso3 == "CHN" & multiprov_indic == 3
					drop multiprov_indic
			expand 5 if location == "North" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Beijing" if location == "North" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Tianjin" if location == "North" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Hebei" if location == "North" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Shaanxi" if location == "North" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Inner Mongolia" if location == "North" & iso3 == "CHN" & multiprov_indic == 5
					drop multiprov_indic
			expand 5 if location == "NorthChina)" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Beijing" if location == "NorthChina)" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Tianjin" if location == "NorthChina)" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Hebei" if location == "NorthChina)" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Shaanxi" if location == "NorthChina)" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Inner Mongolia" if location == "NorthChina)" & iso3 == "CHN" & multiprov_indic == 5
					drop multiprov_indic
			expand 5 if location == "NorthRegion" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Beijing" if location == "NorthRegion" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Tianjin" if location == "NorthRegion" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Hebei" if location == "NorthRegion" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Shaanxi" if location == "NorthRegion" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Inner Mongolia" if location == "NorthRegion" & iso3 == "CHN" & multiprov_indic == 5
					drop multiprov_indic
			expand 8 if location == "Northand Central" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Beijing" if location == "Northand Central" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Tianjin" if location == "Northand Central" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Hebei" if location == "Northand Central" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Shaanxi" if location == "Northand Central" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Inner Mongolia" if location == "Northand Central" & iso3 == "CHN" & multiprov_indic == 5
					replace location = "Henan" if location == "Northand Central" & iso3 == "CHN" & multiprov_indic == 6
					replace location = "Hubei" if location == "Northand Central" & iso3 == "CHN" & multiprov_indic == 7
					replace location = "Hunan" if location == "Northand Central" & iso3 == "CHN" & multiprov_indic == 8
					drop multiprov_indic
			expand 5 if location == "Northernregions" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Beijing" if location == "Northernregions" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Tianjin" if location == "Northernregions" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Hebei" if location == "Northernregions" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Shaanxi" if location == "Northernregions" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Inner Mongolia" if location == "Northernregions" & iso3 == "CHN" & multiprov_indic == 5
					drop multiprov_indic
			expand 2 if location == "Qinghai; Inner Mongolia" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Qinghai" if location == "Qinghai; Inner Mongolia" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Inner Mongolia" if location == "Qinghai; Inner Mongolia" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "Shaanxiand Gansu Provinces" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Shaanxi" if location == "Shaanxiand Gansu Provinces" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Gansu" if location == "Shaanxiand Gansu Provinces" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "Shantoucity (Guangdong); Fujian " & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Guangdong" if location == "Shantoucity (Guangdong); Fujian " & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Fujian" if location == "Shantoucity (Guangdong); Fujian " & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 5 if location == "Southern" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Guangdong" if location == "Southern" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Guangxi" if location == "Southern" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Hainan" if location == "Southern" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Hong Kong Special Administrative Region of China" if location == "Southern" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Macao Special Administrative Region of China" if location == "Southern" & iso3 == "CHN" & multiprov_indic == 5
					drop multiprov_indic
			expand 5 if location == "Southwestern" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Chongqing" if location == "Southwestern" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Sichuan" if location == "Southwestern" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Guizhou" if location == "Southwestern" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Yunnan" if location == "Southwestern" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Tibet" if location == "Southwestern" & iso3 == "CHN" & multiprov_indic == 5
					drop multiprov_indic
			expand 5 if location == "Southwesterns" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Chongqing" if location == "Southwesterns" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Sichuan" if location == "Southwesterns" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Guizhou" if location == "Southwesterns" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Yunnan" if location == "Southwesterns" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Tibet" if location == "Southwesterns" & iso3 == "CHN" & multiprov_indic == 5
					drop multiprov_indic
			expand 5 if location == "SouthwesternTibet" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Chongqing" if location == "SouthwesternTibet" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Sichuan" if location == "SouthwesternTibet" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Guizhou" if location == "SouthwesternTibet" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Yunnan" if location == "SouthwesternTibet" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Tibet" if location == "SouthwesternTibet" & iso3 == "CHN" & multiprov_indic == 5
					drop multiprov_indic
			expand 2 if location == "Tianshuicities (Gansu ); Sichuan " & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Gansu" if location == "Tianshuicities (Gansu ); Sichuan " & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Sichuan" if location == "Tianshuicities (Gansu ); Sichuan " & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "Yunnan); Guizhou" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Yunnan" if location == "Yunnan); Guizhou" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Guizhou" if location == "Yunnan); Guizhou" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 3 if location == "central" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Henan" if location == "central" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Hubei" if location == "central" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Hunan" if location == "central" & iso3 == "CHN" & multiprov_indic == 3
					drop multiprov_indic
			expand 3 if location == "easternJiangsu" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Liaoning" if location == "easternJiangsu" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Jilin" if location == "easternJiangsu" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Heilongjiang" if location == "easternJiangsu" & iso3 == "CHN" & multiprov_indic == 3
					drop multiprov_indic
			expand 5 if location == "northwesternand central sections" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Shaanxi" if location == "northwesternand central sections" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Gansu" if location == "northwesternand central sections" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Qinghai" if location == "northwesternand central sections" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Ningxia" if location == "northwesternand central sections" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Xinjiang" if location == "northwesternand central sections" & iso3 == "CHN" & multiprov_indic == 5
					drop multiprov_indic
			expand 2 if location == "Dongguancity (Guangdong ); Fujian" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Guangdong" if location == "Dongguancity (Guangdong ); Fujian" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Fujian" if location == "Dongguancity (Guangdong ); Fujian" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "Linxia(Gansu ); Hunan" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Gansu" if location == "Linxia(Gansu ); Hunan" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Hunan" if location == "Linxia(Gansu ); Hunan" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "Shaanxiand Gansu Provinces" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Shaanxi" if location == "Shaanxiand Gansu Provinces" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Gansu" if location == "Shaanxiand Gansu Provinces" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "Shantoucity (Guangdong); Fujian" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Guangdong" if location == "Shantoucity (Guangdong); Fujian" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Fujian" if location == "Shantoucity (Guangdong); Fujian" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "Tianshuicities (Gansu ); Sichuan" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Gansu" if location == "Tianshuicities (Gansu ); Sichuan" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Sichuan" if location == "Tianshuicities (Gansu ); Sichuan" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 2 if location == "Yunnan); Guizhou" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Yunnan" if location == "Yunnan); Guizhou" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Guizhou" if location == "Yunnan); Guizhou" & iso3 == "CHN" & multiprov_indic == 2
					drop multiprov_indic
			expand 15 if location == "easternand northern" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Beijing" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Tianjin" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Hebei" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Shanxi" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Inner Mongolia" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 5
					replace location = "Liaoning" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 6
					replace location = "Jilin" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 7
					replace location = "Heilongjiang" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 8
					replace location = "Shanghai" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 9
					replace location = "Jiangsu" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 10
					replace location = "Zhejiang" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 11
					replace location = "Anhui" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 12
					replace location = "Fujian" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 13
					replace location = "Jiangxi" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 14
					replace location = "Shandong" if location == "easternand northern" & iso3 == "CHN" & multiprov_indic == 15
					drop multiprov_indic
			expand 15 if location == "Southernand eastern regions" & iso3 == "CHN"
				bysort event_id location: generate multiprov_indic = _n
					replace location = "Shanghai" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Jiangsu" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 2
					replace location = "Zhejiang" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 3
					replace location = "Anhui" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 4
					replace location = "Fujian" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 5
					replace location = "Jiangxi" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 6
					replace location = "Shandong" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 7
					replace location = "Henan" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 8
					replace location = "Hubei" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 9
					replace location = "Hunan" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 10
					replace location = "Guangdong" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 11
					replace location = "Guangxi" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 12
					replace location = "Hainan" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 13
					replace location = "Hong Kong Special Administrative Region of China" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 14
					replace location = "Macao Special Administrative Region of China" if location == "Southernand eastern regions" & iso3 == "CHN" & multiprov_indic == 15
					drop multiprov_indic
			expand 2 if location == "Guangxi& Guizhou" & iso3 == "CHN"
				bysort event_id location: gen multiprov_indic = _n
					replace location = "Guangxi" if location == "Guangxi& Guizhou" & iso3 == "CHN" & multiprov_indic == 1
					replace location = "Guizhou" if location == "Guangxi& Guizhou" & iso3 == "CHN" & multiprov_indic == 2


			replace location = "Anhui" if inlist(location, "Anhui ...", "Anhui prov ...", "Anhui provinces ...", "Angui", "Anhuis", "Jinzhai(Anhui Province)", "Sixian", "Tianchang(Anhui") | inlist(location, "Xiaoxianand Fengyang counties (Anhui Province)", "Yuexi") | inlist(location, "Tianchang(Anhui ", "Anhwei")
			replace location = "Angui" if regexm(location, "Anhui")

			replace location = "Beijing" if inlist(location, "NearBeijing")
			replace location = "Beijing" if regexm(location, "Beijing")

			replace location = "Chongqing" if inlist(location, "Beibei", "Bishan", "Chengkou", "Chingquing(municipality)", "Chongqing(Sichuan )", "Chongqingcity", "Chongqings", "Chongquin") | inlist(location, "Chongquing", "Fengjiecounty", "Hongqing", "Kaixian", "Qijiang(Chongqing )", "Yubei", "Yunyang") | inlist(location, "Wanzhou", "Hualong(Chongqing)")
			replace location = "Chongqing" if regexm(location, "Chongqing")

			replace location = "Fujian" if inlist(location, "Fujians", "Cangnanarea", "Fuding", "Fujian)", "Fujina", "Fuqing", "Fuzhou", "Fuzhou(Fujian )") | inlist(location, "Lianjiang", "Ningde", "Ningle", "Putian", "Putian(Fujian Province)", "Quanzhou", "QuanzhouShi", "QuanzhouShi (Province Fujian)") | inlist(location, "Quanzhouand Jiangxi s", "Quanzhoucity (Fujian )", "Quanzou", "Sanming(Fujian )", "Shishicity (Fujian", "Xiamen", "XiamenShi", "Xiapudistricts (Fujian )") | inlist(location, "Yongdingcounty (Fujian )", "Zhangzhou", "ZhangzhouShiqu", "Zhangzhoucities (Fujian )", "Zhanzhouand Xiamen (Longhai County") | inlist(location, "Fujian")
			replace location = "Fujian" if regexm(location, "Fujian")

			replace location = "Gansu" if inlist(location, "Zhouqucounty (Gansu ov", "Awu", "Bayin", "DUSERCounty", "Dingxi", "Gannan(Hezuo Shi)", "Gansu)", "GansuSheng") | inlist(location, "Gansus", "Ganzu", "GuLang counties (Gansu Province)", "Hadapu(Longman district", "JingTai", "Jingmu", "Jiuquen", "Jon?(ZhuoniXian) districts (Gansu )") | inlist(location, "Lanzhou(Gansu )", "LanzhouShi (Gansu Sheng )", "LintanXian", "Longnan(Wudu Xian)", "Luqu", "Mianxian(Gansu )", "MinXian", "MinleXian") | inlist(location, "Minxiancounty (Gansu )", "Ningxia", "NingxiaAutonomous rehion", "ShandanXian", "SunanYugu Zizhixian (Gansu Sheng )", "TianZhu", "Tianshi(Tianshui Shi)", "Tianshui") | inlist(location, "Zhuonicounties (Gansu )", "Longan", "Longnam", "Longman")
			replace location = "Gansu" if regexm(location, "Gansu")

			replace location = "Guangdong" if inlist(location, "Cuang", "Guangdong ovinc", "Guangdon", "Canton", "Chaozhou", "Dongguan", "Dongguan(Guangdong ov.)") | inlist(location, "Foshan", "FoshanMaoming", "Gangdongs", "Guandongs", "Guangdong)", "GuangdongProv.", "GuangdongProvince", "Guangdongs") | inlist(location, "Guangzhou", "GuangzhouProvince", "Gunagdong", "Gunagdongs", "Gungdongs", "Hanwei", "Hechuang", "Heyuan") | inlist(location, "Huilai", "Huizhou", "Jiangmen", "Jieyang") | inlist(location, "Jieyang(Guangdong )", "Lechangdistrict", "Leizhou", "LeizhouPeninsula (Guangdong)") | inlist(location, "Lufeng", "Maomin", "Maoming", "MaomingCity", "Meizhou", "Nantou(Guandong )", "NearMaoming", "Qinghe(Guangdong )") | inlist(location, "Shantou", "Shantou(Guangdong )", "Shanwei", "Shaoguan(Guangdong )", "Shaoguancity (Ruyan county", "Shenzhen", "Shenzhen(Guangdong )", "Sinhui") | inlist(location, "Suixi", "Suixi(Guangdong )", "Xinhui", "Xinsi", "Xuwen", "Xuwencounties (Guangdong )", "Yangjiang", "Yangjiang(Guangdong )") | inlist(location, "Yunfu", "Zhanjiang", "Zhaoqing", "Zhongshan", "Zhongshan(Guangdong )", "Zhongshancounties (Guangdong )", "Zhuhai") | inlist(location, "Guandong", "Guangzhou(city) (Guangdong and Fujian Provinces)")
			replace location = "Guangdong" if regexm(location, "Guangdong")

			replace location = "Guangxi" if inlist(location, "Anchui", "Baise", "Baise(Guangxi )", "Beihei", "Cuangxi", "Fangcheng", "GuangxiRegion", "GuangxiZhuang") | inlist(location, "GuangxiZhuang Autonomous", "GuangxiZhuang autonomous region", "Guangxis", "GuanxiProvinces", "Gunagxi", "Hengxian", "Hepu", "Zhangjiang Yangjiang, Maomin, Jiangmen, Yunfu, Zhaoqing, Shanwei, Shantou, Jieyang, Chaozhou, Meizhou, Huizhou, Dongguan (Guangdong prov.), Nanning, Tianyang, Tiandong, Pingguo, Baise, Ningming, Longan, Hengxian, Yongning, Wuming, Hepu, Qingbei, Fangcheng, Shangsi (Guangxi prov.)") | inlist(location, "Nanning", "Ningming", "Pingguo", "Pingxiangs", "Qingbei", "Qinzhou", "Shangsi(Guangxi ov.)", "Tiandong") | inlist(location, "Tianyang", "Wuming", "Wuzhou", "Yongning", "Yulin(Guangxi )", "Yulin(Guangxi Zhuang)", "YumenShi (Gansu Sheng)") | inlist(location, "GuangxiZhuang Autonomous s; Chongqing municipality") | inlist(location, "Guangxiregion")
			replace location = "Guangxi" if regexm(location, "Guangxi")

			replace location = "Guizhou" if inlist(location, "Jinpingcounty", "GuizhouProv.", "Duyun(Guizhou )", "Duyun(Guizhou Province)", "Fenggangcounties (Guizhou )", "Guizhou", "Guizhourpovinces") | inlist(location, "Guizhous", "Liping", "Luodian", "Luodianand Guiding counties (Guizhou )", "Miao-DongAutonomous Prefecture of Qiandongnan", "Miaocounties (Guizhou )", "Qianxi'nanBouyei", "Rongjiangs") | inlist(location, "Sandu", "Shui", "Shuichgeng", "Southernand Western Guizhou", "Wangmo", "Weining(Guizhou )", "Xishui(Guizhou)", "Zuojiaying(Nayong County- Guizhou )") | inlist(location, "Guizhou s", "GuizhouProvince")
			replace location = "Guizhou" if regexm(location, "Guizhou")

			replace location = "Hainan" if inlist(location, "Ding'an", "Haikou", "Haikou(Hainan )", "HainanIs.", "HainanIsl.", "HainanIsland", "Hainanregion", "Hainans") | inlist(location, "Qionghai", "Sanya", "Wanning", "Wenchang", "Wenchangcounties")
			replace location = "Hainan" if regexm(location, "Hainan")

			replace location = "Hebei" if inlist(location, "Handan(Hebei )", "Hebei", "Level1 = Hebei", "Pingshan(Hebei)", "Shijiazhuang", "Yangyuan", "YuxianCounties (Hebei )") | inlist(location, "ZangbeiCounty", "Hebeis")
			replace location = "Hebei" if regexm(location, "Hebei")

			replace location = "Heilongjiang" if inlist(location, "Shalan(Ningan municipali", "Haihe", "Haping", "Heilongiang", "Heilongjangs", "Heilongjiang)", "Heilongjiangs", "Shalan(Ningan municipality") | inlist(location, "Yichuncity (Heilongjiang )", "Jixi(Heilongjiang)", "Heilongjiang(Coastal areas East China)")
			replace location = "Heilongjiang" if regexm(location, "Heilongjiang")

			replace location = "Henan" if inlist(location, "Henans", "Henen", "Shangqiu", "YuzhouCity (Henan Province)", "Zhengzhou(Henan )", "Zhoukou(Henan )", "centralHenan", "centralHenan ")
			replace location = "Henan" if regexm(location, "Henan")

			replace location = "Hong Kong Special Administrative Region of China" if inlist(location, "Fanling", "HappyValley)", "HongKong", "HongKong (China)", "PatHeung Yuen Long", "Shataukok", "Sheung", "TaKwu Leng") | inlist(location, "TsimSha Tsui (Mount Davis")
			replace location = "Hong Kong Special Administrative Region of China" if iso == "HKG"
			replace iso = "CHN" if iso == "HKG"

			replace location = "Hubei" if inlist(location, "Badong", "Hubai", "Hube", "HubeiProvince", "Hubeis", "Jingzhou", "Shayang", "Shiyan") | inlist(location, "Shiyan(Hubei Province)", "Suizhou(Hubei )", "Wuhan(Hubei )", "Xiangfan", "Xianning", "Xingshancounties (Hubei )", "Zigui") | inlist(location, "Dangyang", "Enshi(Hubei )")
			replace location = "Hubei" if regexm(location, "Hubei")

			replace location = "Hunan" if inlist(location, "Chenzhou(Hunan )", "Chenzhou(Hunan Province)", "Hunan s", "Suiningcounty (Hunan o", "(Hunan)", "Anxiangdistrict (Hunan Province)", "Changsha", "Dayong") | inlist(location, "Dongtingand Boyang Lakes (Hunan )", "Hengnandistricts (Hunan )", "Hengyang", "Hengyang(Hunan )", "Huaihua", "Hunan)", "HunanProvinces") | inlist(location, "Hunana", "Hunans", "Hunhe", "Hunnans", "Iluaihua(Hunan )", "Lingling", "Linwucounty", "Liuyang(Hunan )") | inlist(location, "Loudi", "Shaoyang", "Suiningcounty (Hunan )", "Xangtan", "Xiangxi", "Yanxi", "Yiyang") | inlist(location, "Yongzhou", "Yueyang", "Yueyangareas (Hunan )", "Zhangjiajieand Xiangxi (Longhui county", "Zhuzhou", "Zhuzhouand Chenzhou (Hunan )", "citiesof Hengyang") | inlist(location, "Hanyuancounty (Sichuan )", "Hanyuandistrict (Sichuan )")
			replace location = "Hunan" if regexm(location, "Hunan")

			replace location = "Inner Mongolia" if inlist(location, "Chifeng", "Guyangcounty (Inner Mongolia)", "Hohnut", "Hulunbeir", "InnerMOngolia", "InnerMongolia", "InnerMongolia Autonomous Region (Nei Mongol Zizhiqu)", "InnerMongolia Autonomous region") | inlist(location, "InnerMongolia autonomous region", "InnerMongolia region (Nei Mongol Zizhiqu)", "InnerMongolian Autonomous region", "Lindong(Balin-Zuo Qi)-Tianshan area (Inner Mongolia)", "TUSERiao", "Ulanchabu(Chinese Autonomous Region of Inner Mongolia)", "XiliaoHe", "Xilingol") | inlist(location, "Xingan", "includingBairin Right Banner (Chifeng efecture)")

			replace location = "Jiangsu" if inlist(location, "Baoying", "Gaoyou", "Gaoyou(Jiangsu )", "Guangling", "JIangsus", "JiangsuProvinces", "Jiangsus", "Taizhou(Jiangsu )") | inlist(location, "Taizhouand Lishui)", "Thaizhou(Jiangsu", "Wuxi", "XiangshuiCounty (Jiangsu )", "Xinghua", "Yangzhou", "Yangzhou(Jiangsu)", "easternJiangsu") | inlist(location, "easternJiangsu ", "Jiangsu(South China coast)")
			replace location = "Jiangsu" if regexm(location, "Jiangsu")

			replace location = "Jiangxi" if inlist(location, "Jiangx", "BetweenRuichang and Jiujiang (Jiangxi )", "Ganzhu", "GuUSERcounty (Jiangxi )", "Jaingxi", "Jian", "Jianggxis", "Jiangxi") | inlist(location, "Jiangxis", "Jianxi", "Jiujiang(Jiangxi )", "Jiangxi s", "Lyangarea (Jiangsu )")
			replace location = "Jiangxi" if regexm(location, "Jiangxi")

			replace location = "Jilin" if inlist(location, "Changling(Jilin Sheng)", "Jilan", "Jilins", "Jilis", "northeasternJilin", "Linjian")
			replace location = "Jilin" if regexm(location, "Jilin")

			replace location = "Liaoning" if inlist(location, "BetweenXiuyan and Haicheng (Sanjianfang", "Chaiyang", "Dandongcity", "Fuxin", "Huludaodistricts (Liaoning )", "Jinzhou", "Liaoning", "Liaoning)") | inlist(location, "Shenyang", "Shnoyang", "XiuyanManzu Zizhixian district (Liaoning )", "Zhalanyingzi(Liaoning )", "Liaonin")
			replace location = "Liaoning" if regexm(location, "Liaonging")

			replace location = "Ningxia" if inlist(location, "Shizuishan(Ningxia Hui Autonomous Region)")

			replace location = "Qinghai" if inlist(location, "Yangtse", "Yushuefecture (Qinghai", "QinghaiProvince", "Chengduo", "Delingha(Qinghai)", "Gonghearea (Qinghai)", "Haibei(Qinghai )", "Haidong") | inlist(location, "Haixiefecture (Qinghai )", "HoitTaria (Delhi municipality", "Qinghai)", "Qinghais", "Quinghai", "Xining", "Yushucounties (Quinghai )") | inlist(location, "Yushuefecture (Qinghai )", "Zaduo", "Chengdu", "Chengtu")
			replace location = "Qinghai" if regexm(location, "Qinghai")

			replace location = "Shandong" if inlist(location, "Heze", "Jinan", "Jining", "Liaocheng", "Linyi", "Qingdao", "Shandonf") | inlist(location, "Shandongs", "Shangdongs", "Yantai", "Zaozhuang", "Zibo(Shandong Province)") | inlist(location, "Shangdong", "Hezearea (Shandong )", "Jihan(Shandong )")
			replace location = "Shandong" if regexm(location, "Shandong")

			replace location = "Shanghai" if inlist(location, "Shangai", "Anting(Jiading country)", "Atushi(Jiashi county)", "Huangdu", "Shanghaiarea", "Shanghaimunicipality)", "Shanghais") | inlist(location, "Zhangjiang", "Zhaotun(Qungpu county", "Anting(Jiading county)")
			replace location = "Shanghai" if regexm(location, "Shanghai")

			replace location = "Shaanxi" if inlist(location, "Shaanx", "Taoshi", "near Linfen (Shan", "Huxian", "Ziyang (Shaanxi p", "Ankangcity area (Shaanxi )", "DatongCounties (Shanxi co", "Fenyang") | inlist(location, "Hanzhongcity and 11 surrounding counties (Shaanxi Province)", "Hunyuan", "Jijiagou(Shanxi )", "Liuba(Northwest Shaanxi )", "Nanzheng", "Qixian(Shanxi ", "Shaanxi", "Shaanxis") | inlist(location, "ShanxiProvince (Datong Area)", "Shanxxi", "Shuimo", "Tianzhen", "Wanjia", "Wenshui(Shanxi Province)", "Xixinagcounty", "Yanggao") | inlist(location, "Ziyang(Shaanxi )", "northernShanxi porvince", "DatongCounties (Shanxi ", "Qixian(Shanxi ", "Huashanregion (Shaanxi )")
			replace location = "Shaanxi" if regexm(location, "(Shaanxi)|(Shanxi)")

			replace location = "Sichuan" if inlist(location, "Wencgua", "Wenchuancountry", "Bazhong", "Dazhou", "Lushancounty (Sichuan ", "SichuanProv.", "Aba(Ava efecture)", "Baiyucounty (Sichuan )") | inlist(location, "Beichuan", "Dazhoucity (Sichuan )", "Deyang", "EasternSichuan )", "FuRiver", "Ganzi(Sichuan )", "Ganziecture (Sichuan Province)") | inlist(location, "Guangan", "Guangyuan", "GulinXuyong counties (Sichuan Province)", "Huaping", "Huili", "Jinyang", "Kangdingdistricts (Sichuan )", "Leshancity") | inlist(location, "Leshanefecture. Emeishan city", "Liangshan(Sichuan )", "Lushancounty (Sichuan )", "Lushou","Luzhou", "Mianyang", "Mianyang(Sichuan )", "Mianzhu", "Miyi") | inlist(location, "MountEmei area (Sichuan )", "Moxizhen(Suining Shi- Sichuan )", "Nanchong", "NanpingLevel 2 - level 1 = Sichuan", "Ningnan", "Panzhihuacity (Sichuan )", "Panzhihuadistricts (Sichuan )", "Qingchuanarea (Sichuan )") | inlist(location, "Rulongcounty (Xinlong Xian- Sichuan )", "Shiji(Kangding county-Sichuan)", "Sichuan)", "SichuanProvinces", "Sichuang", "Sichuans", "SouthEast Sichuan ", "SouthWestern Sichuan") | inlist(location, "Weiyuan", "Wenchuan", "Xuanhanregions", "Yaan", "Yajiang", "Yanjiang", "Yibin", "Yibin(Sichuan )") | inlist(location, "Yingxiu(Wenchuan)", "Yongchengcounties (Shichuan )", "nearthe border of Sichan )") | inlist(location, "Aba(Ava efecture", "SouthEast Sichuan")
			replace location = "Sichuan" if regexm(location, "Sichuan")

			replace location = "Tianjin" if inlist(location, "Tianijn")

			replace location = "Tibet" if inlist(location, "Autonomousregion of Tibet", "Damxungcounty (Tibet)", "Daofucounty (Tibetan Autonomous Prefecture of Garze", "Lhasaefecture (Tibet)", "NearNingzhong", "Shannanefectures (Tibet)", "SouthwesternTibet", "Tibetregion") | inlist(location, "Xigaze(Rikaze Shi)", "XizangProvince", "Yangi", "Zhongdian(Diqing efecture)", "Shigatse(Tibet)", "BomiRegion (Eastern Tibet", "Tibet)")
			replace location = "Tibet" if regexm(location, "Tibet")

			replace location = "Xinjiang" if inlist(location, "Altay", "Bachu", "Jiashi(Payzawat)", "Artux", "BachuXian", "BurqinCounty (Xinjiang Uygur Region)", "Dabanchengdistrict (Wulumuqi Xian)", "GUSERiu(Kazak Autonomous Prefecture of Ili") | inlist(location, "Ilidistrict (Xinjiang Uygur Autonomous Region)", "Iliefecture (Xinjiang region)", "Iliefecture (Xinjiang)", "Iliefectures (Xinjiang Uygur autonomous region)", "Jiashi", "JiashiXian counties (Xinjiang Uygur Zizhiqu region)", "JiashiXian(Payzawat)", "Jiashicounty (Xinjiang)") | inlist(location, "Kanas", "Kashi", "Kashi(Xinjiang region)", "KashiShi districts (Xinjiang Uygur Zizhiqu region)", "Kashy( Xinjiang)", "KepingCounty (Xinjiang Region)", "Kepingcounty (Xinjiang)", "MaigaitiXian") | inlist(location, "Nilkacounties", "NorthWestern Xinjiang", "Shawan(Xinjiang Uygur Autonomous region)", "ShufuXian", "Shufuarea", "Shule", "ShuleXian", "SouthernXinjiang") | inlist(location, "Tacheng",  "Toksun", "Toli", "Urumqi", "Winjiang", "XibeAutonoumous County of Qapqal (Chabucha'er Xibo Zizhixian)", "XinjiangAutonomous region)", "XinjiangUighur Autonomos region") | inlist(location, "XinjiangUygur Autonomous Region", "Xinjiangs", "Jiashi Xian(Payzawat), Bachu Xian, Yuepuhu Xian, Yingjisha Xian, Maigaiti Xian, Shule Xian, Shufu Xian, Kashi Shi districts (Xinjiang Uygur Zizhiqu region)", "Yining", "Yuepuhu", "YuepuhuXian", "Yutiancounty (Xinjiang )", "ZhaosuXian (Xinjiang Uygur Zizhiqu)") | inlist(location, "Zhaosucounties (Xinjiang Uygur Autonomous region)", "northwesternXinjinag Uygur Autnomous region", "Alteyregion (Xinjiang)") | inlist(location, "Xingjiang", "YingjishaXian", "Yingjishacounties (Xinhiang autonomus region)", "Pishandistrict (Xinjiang region)")
			replace location = "Xinjiang" if regexm(location, "Xinjiang")

			replace location = "Yunnan" if inlist(location, "Ninglang", "Qujing ", "XinpingCounty (Yunnan ", "Yunn", "Laojinshanmountain", "(Yunan)", "Baoshancity", "Benzilan(Deqin Xian- Yunan") | inlist(location, "Chuxiong", "DaliBai", "Dalidistricts (Yunnan )", "Dayao", "DayaoXian district (Yunnan Sheng )", "Donghua(Yunnan - Lijiang Naxi-Yongsheng Xian )", "Doushaguan-Yanjinarea (Yanjin Xian") | inlist(location, "Eryuan", "Funingcounty (Yunnan )", "Guantun(Yao'an", "Guantun(Yao'ancounty)", "Hapingcounties (Lijian efecture)", "Heqing", "HongheAutonomous  (Yunnan )") |inlist(location, "HuizeCounty", "Jianchuan(Dali ecture)") | inlist(location, "Jinghong(Yunan )", "Kunming", "Kunmingregion (Yunnan )", "Lancang-Menglian-Ximengarea (Yunnan )", "Lanping(Nujiang efecture) - Yunnan Province", "Lijiang", "Lijiang(Yunnan Prov.)", "Longtoushan") | inlist(location, "Longtoushanand Lehong)", "LudianXian (Yunnan Sheng )", "LudianXian (Zhaotong region", "LudianXian county (Yunnan Sheng )", "Mile", "Mojiangcounty (Yunnan )", "Nanhua", "Nujiangefecture (Yunnan )") | inlist(location, "Pingyuan(Yingjiang)", "Pu'er(Yunnan )", "Pu'erand Dehong", "Qiaojia", "Qiubei(Yunnan )", "Qujingefecture", "Shidiancounty (Yunnan )", "Tengchong") | inlist(location, "Weiyuancity (Yongping", "Wudingdistrict (Yunnan )", "XiangyunCounties", "Xiluodou(Yunnan )", "XinpingCounty (Yunnan )", "Yangbi", "YanjinCounty (Yunnan )", "Yiliang(Zhaotong - Yunnan )") | inlist(location, "Yingijangcounty", "Yingjiang(Yunnan )", "Yingjiang(Yunnan Province)", "YingjiangCounty (Yunnan )", "Yongren", "Yongshengcounty (Yunan )", "Yuanmo", "Yunan") | inlist(location, "Yunlong(Yunnan )", "Yunnan (including Dongshan", "Yunnan)", "YunnanProvince near the border of Myanmar", "YunnanProvince)", "YunnanProvinces", "YunnanSheng", "YunnanSheng )", "Yunnans") | inlist(location, "Zhaojiagouvillage (Zhenxiong county-Yunnan )", "Zhaotong", "Zhaotongcity (Ludian county", "Zhaotongefecture (Yunnan )", "southwesterbYunnan") | inlist(location, "(Yunnan)", "Benzilan(Deqin Xian- Yunan ", "Yunan", "southwesterbYunnan ", "Benzilan(Deqin Xian- Yunan", "Baoshancity (Yunnan )", "Laojinshan", "Puladicounty (Gongshan") | inlist(location, "Qiaojiadistrict.", "Qujingcity (Huize district)")
			replace location = "Yunnan" if regexm(location, "Yunan")

			replace location = "Zhejiang" if inlist(location, "Cangnan", "Lishui", "ZhejiangProvince (includ", "Cangnandistrict (Zheijiang )", "Jiangsu(Zhejiang )", "NearHangzhou", "NearYueging (Zheijiang )", "Ningbo") | inlist(location, "Pingyang(Zheijiang )", "Shaoxing(Zheijang )", "Taizhou", "Wenzhou", "Wenzhou(Zheijiang )", "Wenzhou(Zhejiang )", "Wenzhouarea (Zhejiang Province )", "Yangtze") | inlist(location, "Zeijiang", "Zheijangs", "Zheijian", "Zheijiangs", "Zheijiang", "ZhejiangProvince (including Wenzhou", "Zhejiangs") | inlist(location, "Zhejiangs)", "Zhejians", "Cangnans", "Jinagsu", "Hangzhou", "Huzhou(Zhejiang )")
			replace location = "Zhejiang" if regexm(location, "Zhejiang")


			drop if location == "BayanUl Hot-Uliastai area (Xiwuzhumuqin Qi)" & iso3 == "CHN"
			drop if location == "Gixiregion" & iso3 == "CHN"
			drop if location == "Southernand northern sections" & iso3 == "CHN"
			drop if location == "s" & iso3 == "CHN"


			replace location = "Veracruz de Ignacio de la Llave" if location == "Veracruz"
			replace location = "Hidalgo" if location == "Hidalg"
			replace location = "Guerrero" if inlist(location, "Agua Escondida", "Acapulco")
			replace location = "Baja California" if location == "Baja Calif"
			replace location = "Chiapas" if location == "ChiapasState"
			replace location = "Oaxaca" if location == "Acatlande Perez Figueroa"
			replace location = "Oaxaca" if location == "Huautla de Jimenez (Oaxaca)"

			save `sub_work', replace
			restore
			drop if iso3 == "CHN" | iso3 == "HKG"
			append using `sub_work'


			// 
			replace location = "Colima" if location == "Armeria"	
			********************************************************************
			** GBR
			********************************************************************
				** Can't figure these out
				drop if inlist(location, "Ouse") & iso3 == "GBR"
			preserve
			keep if iso3 == "GBR"

			replace location = "Scotland" if location == "Aberdeenshire)"
			replace location = "South West England" if location == "Avon"
			replace location = "Yorkshire and the Humber" if location == "Barnsley"
			replace location = "East of England" if location == "Bedfordshire"
			replace location = "South East England" if location == "Berkshire"
			replace location = "South East England" if location == "Bognor Regis"
			replace location = "East Midlands" if location == "Boston"
			replace location = "South East England" if location == "Buckinghamshire"
			replace location = "North West England" if location == "Burneside"
			replace location = "Yorkshire and the Humber" if location == "Calderdale"
			replace location = "East of England" if location == "Cambridgeshire"
			replace location = "South West England" if location == "Camelford villages)"
			replace location = "Yorkshire and the Humber" if location == "Catcliffe and Whiston Humberside"
			replace location = "North West England" if location == "Cheshire"
			replace location = "North East England" if location == "Cleveland"
			replace location = "Wales" if location == "Clwyd (city of St Asaph : North Wales). Several rivers including: Thames"
			replace location = "South West England" if location == "Cornwall"
			replace location = "South West England" if location == "Cornwall(Kastel Boterel" | location == "Cornwall (Kastel Boterel"
			replace location = "Wales" if location == "Corwen"
			replace location = "Scotland" if location == "Craigellachie"
			replace location = "Scotland" if location == "Cree"
			replace location = "North West England" if location == "Cumbria"
			replace location = "North West England" if location == "Cumbria(Cockermouth" | location == "Cumbria (Cockermouth"
			replace location = "Yorkshire and the Humber" if location == "Darfield"
			replace location = "South East England" if location == "Datford"
			replace location = "East Midlands" if location == "Derbyshire"
			replace location = "East Midlands" if location == "Derwent"
			replace location = "South West England" if location == "Devonshire"
			replace location = "South West England" if location == "Devonshire (Devon)"
			replace location = "South West England" if location == "Devonshire - Southeastern England. Towns: Uckfield" | location == "Devonshire and Cornwall (Southwest England)"
			replace location = "South West England" if location == "Devonshire(Devon?(Stoke Canon England)" | location == "Devonshire (Devon?(Stoke Canon village"
			replace location = "Yorkshire and the Humber" if location == "Dinnington"
			replace location = "Yorkshire and the Humber" if location == "Doncaster"
			replace location = "South East England" if location == "Dorking (Surrey)"
			replace location = "South West England" if location == "Dorset"
			replace location = "South West England" if location == "Dorsetshire"
			replace location = "South East England" if location == "Dover"
			replace location = "Scotland" if location == "Dufftown towns : Moray area ; Aberdeen city ; Aboyne village : Aberdeenshire)"
			replace location = "North East England" if location == "Durham"
			replace location = "North West England" if location == "Earby"
			replace location = "East of England" if location == "East England"
			replace location = "Yorkshire and the Humber" if location == "East Riding"
			replace location = "South East England" if location == "East Sussex"
			replace location = "Yorkshire and the Humber" if location == "East Yorkshire"
			replace location = "South East England" if location == "Edenbridge"
			replace location = "North East England" if location == "Elvington towns (North and East Yorkshire)"
			replace location = "East of England" if location == "Essex"
			replace location = "East of England" if location == "Essex and Kent counties"
			replace location = "Yorkshire and the Humber" if location == "Filey cities)"
			replace location = "South East England" if location == "Folkestonearea" | location == "Folkestone area"
			replace location = "Scotland" if location == "Forres"
			replace location = "Yorkshire and the Humber" if location == "Gilling west); North Yorkshire (York)"
			replace location = "Scotland" if location == "Glasgow"
			replace location = "South West England" if location == "Gloucester"
			replace location = "South West England" if location == "Gloucetershire" | location == "Gloucestershire"
			replace location = "Scotland" if location == "Grampian (Aberdeenshire) - Scotland region"
			replace location = "Scotland" if location == "Grampian(Elgin" | location == "Grampian (Elgin"
			replace location = "Scotland" if location == "Grampians(Roanheads area in Peterhead" | location == "Grampians (Roanheads area in Peterhead"
			replace location = "East Midlands" if location == "Grantham"
			replace location = "East of England" if location == "Great Yarmouth"
			replace location = "North West England" if location == "Greater Manchest"
			replace location = "Yorkshire and the Humber" if location == "Hambleton"
			replace location = "South East England" if location == "Hamphshire"
			replace location = "South East England" if location == "Hampshire"
			replace location = "West Midland" if location == "Hereford and Wor"
			replace location = "West Midland" if location == "Herefordshire"
			replace location = "East of England" if location == "Hertfordshire"
			replace location = "Scotland" if location == "Highland (Wick) - Scotland ; Northamptonshire"
			replace location = "East Midlands" if location == "Horncastle"
			replace location = "Yorkshire and the Humber" if location == "Humberregion" | location == "Humber region"
			replace location = "Yorkshire and the Humber" if location == "Humberside"
			replace location = "South East England" if location == "Isle of Wight"
			replace location = "North West England" if location == "Kendal)"
			replace location = "South East England" if location == "Kent" | location == "Kent (northwest)"
			replace location = "South East England" if location == "Kent (Midlands and North)"
			replace location = "North West England" if location == "Keswick"
			replace location = "North West England" if location == "Lancashire"
			replace location = "South West England" if location == "Larkhill"
			replace location = "Yorkshire and the Humber" if location == "Leeds"
			replace location = "East Midlands" if location == "Leicestershire"
			replace location = "South East England" if location == "Lewes"
			replace location = "West Midlands" if location == "Lichfield"
			replace location = "East Midlands" if location == "Lincoln"
			replace location = "East Midlands" if location == "Lincolnshire"
			replace location = "Scotland" if location == "Lothian(Edinburgh)" | location == "Lothian (Edinburgh)"
			replace location = "East of England" if location == "Louth"
			replace location = "East of England" if location == "Lowestoft"
			replace location = "South East England" if location == "Maidstone"
			replace location = "Yorkshire and the Humber" if location == "Malton"
			replace location = "South East England" if location == "Medway."
			replace location = "North West England" if location == "Merseyside"
			replace location = "Wales" if location == "MidGlamorgan (Bridgend)" | location == "Mid Glamorgan (Bridgend)"
			expand 2 if location == "Midlands" & iso3 == "GBR"
				bysort event_id location: gen mult = _n
				replace location = "West Midlands" if location == "Midlands" & mult == 1
				replace location = "East Midlands" if location == "Midlands" & mult == 2
				drop mult
			expand 2 if location == "Midlands areas" & iso3 == "GBR"
				bysort event_id location: gen mult = _n
				replace location = "West Midlands" if location == "Midlands areas" & mult == 1
				replace location = "East Midlands" if location == "Midlands areas" & mult == 2
				drop mult
			replace location = "Scotland" if location == "Moray(Scotland)" | location == "Moray (Scotland)"
			replace location = "South East England" if location == "Muchelney(Somerset levels); Devon East; Kent (Southern England)" | location == "Muchelney (Somerset levels); Devon East; Kent (Southern England)"
			replace location = "North East England" if location == "Newburn"
			replace location = "East of England" if location == "Norfolk"
			replace location = "East of England" if location == "Norfolkshire"
			replace location = "East Midlands" if location == "Norhtampton"
			expand 3 if location == "North England" & iso3 == "GBR"
				bysort event_id location: gen mult = _n
				replace location = "North East England" if location == "North England" & mult == 1
				replace location = "North West England" if location == "North England" & mult == 2
				replace location = "Yorkshire and the Humber" if location == "North England" & mult == 3
				drop mult
			replace location = "South West England" if location == "North Gloucs"
			replace location = "North East England" if location == "North East"
			replace location = "Wales" if location == "North Wales"
			replace location = "Yorkshire and the Humber" if location == "North Yorkshire"
			replace location = "Yorkshire and the Humber" if location == "North Yorkshire (Ryedale"
			replace location = "Yorkshire and the Humber" if location == "NorthYorkshire (Scarborough" | location == "North Yorkshire (Scarborough"
			replace location = "East Midlands" if location == "Northamptonshire"
			expand 3 if location == "Northern England" & iso3 == "GBR"
				bysort event_id location: gen mult = _n
				replace location = "North East England" if location == "Northern England" & mult == 1
				replace location = "North West England" if location == "Northern England" & mult == 2
				replace location = "Yorkshire and the Humber" if location == "Northern England" & mult == 3
				drop mult
			replace location = "Northern Ireland" if location == "Northern Ireland"
			replace location = "Yorkshire and the Humber" if location == "NorthernEngland - Yorkshire (South and North)" | location == "Northern England - Yorkshire (South and North)"
			replace location = "North East England" if location == "Northumberland"
			replace location = "North East England" if location == "Northumberland(Morpeth" | location == "Northumberland (Morpeth"
			replace location = "Yorkshire and the Humber" if location == "Norton"
			replace location = "East Midlands" if location == "Nottinghamshire"
			replace location = "Yorkshire and the Humber" if location == "OldMalton" | location == "Old Malton"
			replace location = "Northern Ireland" if location == "Omagh"
			replace location = "South East England" if location == "Oxfordshire"
			replace location = "South East England" if location == "Oxfordshire(Oxford)" | location == "Oxfordshire (Oxford)"
			replace location = "Wales" if location == "Pays de Galles"
			replace location = "West Midlands" if location == "Pershore"
			replace location = "Scotland" if location == "Perth & Tayside"
			replace location = "Yorkshire and the Humber" if location == "Pickering"
			replace location = "Yorkshire and the Humber" if location == "Pickering Town)" | location == "Pickering town)"
			replace location = "Wales" if location == "Powys (Mid-Wales)"
			replace location = "Wales" if location == "Rhyl"
			replace location = "Yorkshire and the Humber" if location == "Richmondshire"
			replace location = "North East England" if location == "Rothbury"
			replace location = "Yorkshire and the Humber" if location == "Rotherham"
			replace location = "Scotland" if location == "Rothes"
			replace location = "Scotland" if location == "Ruchill."
			replace location = "Yorkshire and the Humber" if location == "Ryedale"
			replace location = "West Midlands" if location == "Salop"
			replace location = "Yorkshire and the Humber" if location == "Scarborough"
			replace location = "Scotland" if location == "Scotland"
			replace location = "Scotland" if location == "Scotland. River: Exe"
			expand 2 if (location == "Scotland;and England: West Yorkshire" | location == "Scotland; and England: West Yorkshire") & iso3 == "GBR"
				bysort event_id location: gen mult = _n
				replace location = "Scotland" if (location == "Scotland;and England: West Yorkshire" | location == "Scotland; and England: West Yorkshire") & mult == 1
				replace location = "Yorkshire and the Humber" if (location == "Scotland;and England: West Yorkshire" | location == "Scotland; and England: West Yorkshire") & mult == 2
				drop mult
			replace location = "South East England" if location == "Selsey"
			replace location = "Yorkshire and the Humber" if location == "Sheffield"
			replace location = "West Midlands" if location == "Shrophire. Towns:Hull"
			replace location = "West Midlands" if location == "Shropshire"
			replace location = "West Midlands" if location == "Shropshire + Herefordshire"
			replace location = "South West England" if location == "Somerset"
			replace location = "South West England" if location == "Somersetshire"
			expand 2 if location == "South Glamorga (Cardiff) -Wales; Northumberland (Morpeth)" & iso3 == "GBR"
				bysort event_id location: gen mult = _n
				replace location = "Wales" if location == "South Glamorga (Cardiff) -Wales; Northumberland (Morpeth)" & mult == 1
				replace location = "North East England" if location == "South Glamorga (Cardiff) -Wales; Northumberland (Morpeth)" & mult == 2
				drop mult
			replace location = "Wales" if location == "South Wales"
			replace location = "Yorkshire and the Humber" if location == "South Yorkshire"
			replace location = "Wales" if location == "SouthWales"
			expand 4 if (location == "Southernregions" | location == "Southern regions")
				bysort event_id location: gen mult = _n
				replace location = "South West England" if (location == "Southernregions" | location == "Southern regions") & mult == 1
				replace location = "South East England" if (location == "Southernregions" | location == "Southern regions") & mult == 2
				replace location = "Greater London" if (location == "Southernregions" | location == "Southern regions") & mult == 3
				replace location = "East of England" if (location == "Southernregions" | location == "Southern regions") & mult == 4
				drop mult
			replace location = "West Midlands" if location == "Staffordshire"
			replace location = "Yorkshire and the Humber" if location == "Stamford Bridge"
			replace location = "East of England" if location == "Suffolk"
			replace location = "South East England" if location == "Surrey"
			replace location = "South East England" if location == "Sussex"
			replace location = "South East England" if location == "Sussex (East)"
			replace location = "South East England" if location == "Sussex (West and East)"
			replace location = "South East England" if location == "Sussex ; Leatherhead"
			replace location = "Wales" if location == "Swansea)"
			replace location = "Scotland" if location == "Tayside (Angus) (Scotland)"
			replace location = "West Midlands" if location == "Tenbury Wells"
			replace location = "South East England" if location == "Tenterden. Rivers: Uck"
			replace location = "South West England" if location == "Tintagel"
			replace location = "South East England" if location == "Tonbridge Wells"
			replace location = "Yorkshire and the Humber" if location == "Treeton"
			replace location = "North East England" if location == "Tyne and Wear"
			replace location = "North East England" if location == "Tyne and Wear (Northeast of England)"
			replace location = "North East England" if location == "Tyne and Wear (Northeast)"
			replace location = "South West England" if location == "Umberleig)) and Cornwall mainly; other part of South-West England as well: Avon"
			replace location = "East Midlands" if location == "Wainfleet"
			replace location = "East Midlands" if location == "Wakefield"
			replace location = "East Midlands" if location == "Wakefield ; North)"
			replace location = "Wales" if location == "Wales"
			replace location = "West Midlands" if location == "Warwickshire"
			replace location = "Wales" if location == "West Glamorgan (Port Talbot"
			replace location = "West Midlands" if location == "West Midland"
			replace location = "West Midlands" if location == "West Midlands (Central England)"
			replace location = "West Midlands" if location == "West Midlands (Midlands)"
			replace location = "South East England" if location == "West Sussex"
			replace location = "South East England" if location == "West Sussex (South-East England)"
			replace location = "Yorkshire and the Humber" if location == "West Yorkshire"
			replace location = "Yorkshire and the Humber" if location == "West Yorkshire (northern England)"
			replace location = "Yorkshire and the Humber" if location == "WestYorkshire"
			replace location = "Yorkshire and the Humber" if location == "WestYorkshire (North England)" | location == "West Yorkshire (North England)"
			replace location = "Yorkshire and the Humber" if location == "Whiston"
			replace location = "Yorkshire and the Humber" if location == "Whitby"
			replace location = "South East England" if location == "Wight Isl. Kent"
			replace location = "South West England" if location == "Wiltshire"
			replace location = "South West England" if location == "Wiltshire (South West England)"
			replace location = "South West England" if location == "Wiltshire; Wales"
			replace location = "West Midlands" if location == "Worcestershire"
			replace location = "West Midlands" if location == "Worcestershire (Tenbury Wells) - England"
			replace location = "North West England" if location == "Workington"
			replace location = "Wales" if location == "Wrexham-Welshpool-Shrewsburyarea" | location == "Wrexham-Welshpool-Shrewsbury area"
			replace location = "West Midlands" if location == "Wyre Forest"
			replace location = "South East England" if location == "Yalding"
			replace location = "Yorkshire and the Humber" if location == "Yorkshire"
			replace location = "Yorkshire and the Humber" if location == "Yorkshire(West: Leeds" | location == "Yorkshire (West: Leeds"

			save `sub_work', replace
			restore
			drop if iso3 == "GBR"
			append using `sub_work'


			*********************************************************************
			** MEX
			*********************************************************************
				** Can't figure these out
				drop if inlist(location, "Central& Southern Mexico", "La Garza", "Mexico", "North", "Valdivia") & iso3 == "MEX"
			preserve
			keep if iso3 == "MEX"

			replace location = "Guerrero" if location == "Acapulco"
			replace location = "Oaxaca" if location == "Acatlande Perez Figueroa municipality (Oaxaca state)" | location == "Acatlan de Perez Figueroa municipality (Oaxaca state)"
			replace location = "Coahuila" if location == "Acuna"
			replace location = "Guerrero" if location == "Agua Escondida"
			replace location = "Distrito Federal" if location == "Alvaro Obregon"
			replace location = "Michoac�n de Ocampo" if location == "Angangueo"
			replace location = "Colima" if location == "Armeria"
			replace location = "Michoac�n de Ocampo" if location == "Arteaga"
			replace location = "Coahuila" if location == "Arteaga (Coahuila State" | location == "Arteaga (Coahuila state"
			replace location = "Michoac�n de Ocampo" if location == "Arteagaarea (Michoacan)" | location == "Arteaga area (Michoacan)"
			replace location = "M�xico" if location == "Atizapen de Zaragoza (Mexico State)" | location == "Atizapen de Zaragoza (Mexico state)"
			replace location = "Oaxaca" if location == "Atzalan (Veracruz state)); Oaxaca states"
			replace location = "Baja California" if location == "Baja"
			replace location = "Baja California" if location == "Baja California"
			replace location = "Baja California" if location == "BajaCalifornia"
			replace location = "Baja California" if location == "BajaCalifornia State" | location == "Baja California State"
			replace location = "Baja California Sur" if location == "BajaCalifornia Sur" | location == "Baja Calirfornia Sur"
			replace location = "Baja California" if location == "BajaCalifornia state" | location == "Baja California state"
			replace location = "Baja California" if location == "BajaPeninsula" | location == "Baja Peninsula"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Cabo Rojo (Veracruz state)"
			replace location = "Baja California Sur" if location == "Cabo San Lucas"
			replace location = "Veracruz de Ignacio de la Llave" if location == "CaboRojo (Veracruz state)"
			replace location = "Campeche" if location == "Campeche"
			replace location = "Campeche" if location == "Capechestate (Yucatan Peninsula)" | location == "Campeche state (Yucatan Peninsula)"
			replace location = "Michoac�n de Ocampo" if (location == "CentralMexico" | location == "Central Mexico")
			replace location = "M�xico" if location == "Chalcovalley (near Mexico City)" | location == "Chalco valley (near Mexico City)"
			replace location = "Chiapas" if location == "Chiapas"
			expand 2 if location == "Chiapas and Puebla" & iso == "MEX"
				bysort event_id location: gen mult = _n
				replace location = "Chiapas" if location == "Chiapas and Puebla" & mult == 1
				replace location = "Puebla" if location == "Chiapas and Puebla" & mult == 2
				drop mult
			replace location = "Chiapas" if location == "Chiapas states"
			expand 2 if (location == "Chiapas& Oaxaca" | location == "Chiapas & Oaxaca")
				bysort event_id location: gen mult = _n
				replace location = "Chiapas" if (location == "Chiapas& Oaxaca" | location == "Chiapas & Oaxaca") & mult == 1
				replace location = "Oaxaca" if (location == "Chiapas& Oaxaca" | location == "Chiapas & Oaxaca") & mult == 2
				drop mult
			replace location = "Chiapas" if location == "ChiapasState" | location == "Chiapas State"
			replace location = "Chiapas" if location == "Chiapasstate" | location == "Chiapas state"
			replace location = "Chihuahua" if location == "Chichuahua" | location == "Chihuahua State"
			replace location = "Chihuahua" if location == "Chihuahua"
			expand 4 if location == "Chimalapas"
				bysort event_id location: gen mult = _n
				replace location = "Chiapas" if location == "Chimalapas" & mult == 1
				replace location = "Oaxaca" if location == "Chimalapas" & mult == 2
				replace location = "Puebla" if location == "Chimalapas" & mult == 3
				replace location = "Veracruz de Ignacio de la Llave" if location == "Chimalapas" & mult == 4
				drop mult
			replace location = "Veracruz de Ignacio de la Llave" if location == "Chinampa (Veracruz)"
			replace location = "Chiapas" if location == "Chipias"
			replace location = "Nuevo Le�n" if location == "City of Monterrey" | location == "City Of Monterrey"
			replace location = "Coahuila" if location == "Ciudad Acuna"
			replace location = "Baja California" if location == "Ciudad Constitucion (Baja California)"
			replace location = "Chihuahua" if location == "CiudadJuarez" | location == "Ciudad Juarez"
			replace location = "M�xico" if location == "Coacalco"
			replace location = "Coahuila" if location == "Coahuila"
			expand 2 if location == "Coahuila and Nuevo Leon states)"
				bysort event_id location: gen mult = _n
				replace location = "Coahuila" if location == "Coahuila and Nuevo Leon states)" & mult == 1
				replace location = "Nuevo Le�n" if location == "Coahuila and Nuevo Leon states)" & mult == 2
				drop mult
			replace location = "Oaxaca" if location == "Coastal State Oaxaca"
			replace location = "" if location == "Coatepec"
			replace location = "Colima" if location == "Colima"
			replace location = "Colima" if location == "Colima states (Basse Californie)"
			replace location = "Colima" if location == "Colimacity" | location == "Colima city"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Colonia Gavilondo"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Colonia Los Laureles"
			replace location = "Baja California Sur" if location == "Comondu"
			replace location = "Sinaloa" if location == "Concordia"
			replace location = "Colima" if location == "Coquimatlan"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Cordoba"
			replace location = "Sinaloa" if location == "Cosala (Sinaloa state)"
			replace location = "Oaxaca" if location == "Cost"
			replace location = "Guerrero" if location == "CostaChica (Guerrerro state)" | location == "Costa Chica (Guerrerro state)"
			replace location = "Quintana Roo" if location == "Cozumel (Quintana Roo State)" | location == "Cozumel (Quintana Roo state)"
			replace location = "Quintana Roo" if location == "CozumelIsl." | location == "Cozumel Isl."
			replace location = "Chihuahua" if location == "Cuahutemoc"
			replace location = "Guerrero" if location == "Cuajinicuilapa"
			replace location = "M�xico" if location == "Cuantitlan"
			replace location = "Oaxaca" if location == "Cuenca del Rio (Oaxaca state)"
			replace location = "Sinaloa" if location == "Culiacan"
			replace location = "Distrito Federal" if location == "Distrito Federal"
			replace location = "Durango" if location == "Durango"
			replace location = "Durango" if location == "Durango Region" | location == "Durango region"
			replace location = "Durango" if location == "Durango states"
			replace location = "M�xico" if location == "Ecateped"
			replace location = "Baja California" if location == "El Rosario"
			replace location = "Sinaloa" if location == "Elota"
			replace location = "Baja California" if location == "Ensenada"
			replace location = "Sinaloa" if location == "Escuinapa"
			replace location = "Sinaloa" if location == "FromPunta San Telmo to Mazatlan" | location == "From Punta San Telmo to Mazatlan"
			replace location = "Chihuahua" if location == "Guachochi"
			replace location = "" if location == "Guanajuato"
			replace location = "Guerrero" if location == "Guerrero"
			replace location = "Guerrero" if location == "Guerrero States" | location == "Guerrero state" | location == "Guerrero states"
			replace location = "Guerrero" if location == "Guerrerostate"
			replace location = "Hidalgo" if location == "Hidalgo"
			replace location = "Guerrero" if location == "Huamuxtitlan"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Huatsco"
			replace location = "Guerrero" if location == "Igualapa"
			replace location = "Colima" if location == "Ixtlahuacan"
			replace location = "Jalisco" if location == "Jaliscao" | location == "Guadalajara"
			replace location = "Jalisco" if location == "Jalisco"
			replace location = "Jalisco" if location == "JaliscoState" | location == "Jalisco State"
			replace location = "Baja California Sur" if location == "La Paz"
			replace location = "Baja California Sur" if location == "La Paz (Baja California Sur)"
			replace location = "Baja California Sur" if location == "LaPaz"
			replace location = "Sinaloa" if location == "LaReforma area (Sinaloa State)" | location == "La Reforma area (Sinaloa State)"
			replace location = "Colima" if location == "LaYerbabuena (Colima state-Cuauhtemoc)" | location == "La Yerbabuena (Colima state-Cuauhtemoc)"
			replace location = "Michoac�n de Ocampo" if location == "LazaroCardenas (Michoacan state)" | location == "Lazaro Cardenas (Michoacan state)"
			replace location = "Guanajuato" if location == "Leon"
			replace location = "Baja California Sur" if location == "Loreto"
			replace location = "Baja California Sur" if location == "Los Cabos"
			replace location = "Baja California Sur" if location == "LosCabos"
			replace location = "Guerrero" if location == "Malinaltepec municipalities (Guerrero and Oaxaca states)"
			replace location = "Colima" if location == "Manzanillo"
			replace location = "Tamaulipas" if location == "Matamoraz"
			replace location = "Sinaloa" if location == "Mazatlan"
			replace location = "Sinaloa" if location == "MazatlanArea" | location == "Mazatlan Area"
			replace location = "Baja California" if location == "Mexicali (Baja California area)"
			replace location = "Baja California" if location == "Mexicali(Baja California state)" | location == "Mexicali (Baja California state)"
			replace location = "Baja California" if location == "Mexico Baja California"
			replace location = "Distrito Federal" if location == "Mexico City" | location == "Mexico city"
			expand 2 if (location == "Mexico'sBaja California Peninsula" | location == "Mexico's Baja California Peninsula")
				bysort event_id location: gen mult = _n
				replace location = "Baja California" if (location == "Mexico'sBaja California Peninsula" | location == "Mexico's Baja California Peninsula") & mult == 1
				replace location = "Baja California Sur" if (location == "Mexico'sBaja California Peninsula" | location == "Mexico's Baja California Peninsula") & mult == 2
				drop mult
			replace location = "Distrito Federal" if location == "Mexicocity"
			replace location = "Michoac�n de Ocampo" if location == "Michoacan"
			replace location = "Michoac�n de Ocampo" if location == "Michpacan"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Minatitlan"
			replace location = "Nuevo Le�n" if location == "Monterrey"
			replace location = "Morelos" if location == "Morelos"
			replace location = "Morelos" if location == "Morelos State"
			replace location = "Morelos" if location == "Morelos states"
			replace location = "Baja California" if location == "MulogoBaja California"
			replace location = "Coahuila" if location == "Muzquiz"
			replace location = "Chihuahua" if location == "Namiquipa and Nuevo Casas Grandes cities and towns (Chihuahua"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Naranjos"
			replace location = "Sinaloa" if location == "Navolato"
			replace location = "Nayarit" if location == "Nayarit"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Nogales(Veracruz state)" | location == "Nogales (Veracruz state)"
			replace location = "Oaxaca" if location == "NorthPuerto Angel" | location == "North Puerto Angel"
			replace location = "Nuevo Le�n" if location == "Nuevo Leon"
			replace location = "Nuevo Le�n" if location == "Nuevo Mexico"
			replace location = "Nuevo Le�n" if location == "NuevoLeon"
			replace location = "Oaxaca" if location == "Oaxaca"
			replace location = "Oaxaca" if location == "Oaxaca State"
			replace location = "Oaxaca" if location == "Oaxaca States"
			replace location = "Oaxaca" if location == "Oaxaca states"
			replace location = "Oaxaca" if location == "Oaxacaregion" | location == "Oaxaca region"
			replace location = "Oaxaca" if location == "Oaxacastate" | location == "Oaxaca state"
			replace location = "Guanajuato" if location == "Ocampo"
			replace location = "Guerrero" if location == "Ometepec"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Orizaba"
			replace location = "Oaxaca" if location == "Oxaca"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Panuco areas"
			replace location = "Michoac�n de Ocampo" if location == "Patzcuaro"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Perote (Veracruz state)"
			replace location = "Coahuila" if location == "PiedrasNegras" | location == "Piedras Negras"
			replace location = "Jalisco" if location == "Pihuamo (Jalisco state)"
			replace location = "Chiapas" if location == "Pijijiapan"
			replace location = "Quintana Roo" if (location == "Playadel Carmen" | location == "Playa del Carmen")
			expand 2 if location == "Popocatepelt"
				bysort event_id location: gen mult = _n
				replace location = "Puebla" if location == "Popocatepelt" & mult == 1
				replace location = "Morelos" if location == "Popocatepelt" & mult == 2
				drop mult
			replace location = "San Luis Potos�" if location == "Potosi"
			replace location = "Puebla" if location == "Puebla"
			replace location = "Puebla" if location == "Puebla states"
			replace location = "Puebla" if location == "PueblaState" | location == "Puebla State" | location == "Puebla state"
			replace location = "Puebla" if location == "Puerta states"
			expand 4 if location == "Puerto Vallarta (Jalisco state); Manzanillo (Colima state); Michoacan state; Nayarit state"
				bysort event_id location: gen mult = _n
				replace location = "Jalisco" if location == "Puerto Vallarta (Jalisco state); Manzanillo (Colima state); Michoacan state; Nayarit state" & mult == 1
				replace location = "Colima" if location == "Puerto Vallarta (Jalisco state); Manzanillo (Colima state); Michoacan state; Nayarit state" & mult == 2
				replace location = "Michoac�n de Ocampo" if location == "Puerto Vallarta (Jalisco state); Manzanillo (Colima state); Michoacan state; Nayarit state" & mult == 3
				replace location = "Nayarit" if location == "Puerto Vallarta (Jalisco state); Manzanillo (Colima state); Michoacan state; Nayarit state" & mult == 4
				drop mult
			replace location = "Oaxaca" if location == "PuertoEscondido" | location == "Puerto Escondido"
			replace location = "Chiapas" if location == "PuertoMadero; Chiapas state" | location == "Puerto Madero; Chiapas state"
			replace location = "Jalisco" if location == "PuntaFarakkon" | location == "Punta Farakkon"
			replace location = "Quintana Roo" if location == "Quintana Roo"
			replace location = "Quintana Roo" if location == "Quintana Roo s"
			replace location = "Quintana Roo" if location == "QuintanaRoo state" | location == "Quintana Roo state"
			replace location = "Quintana Roo" if location == "Quintanta Roo"
			replace location = "Sinaloa" if location == "Salvador Alvarado"
			replace location = "Baja California Sur" if location == "San Ignacio"
			replace location = "Coahuila" if location == "San Juan de Sabinas"
			replace location = "San Luis Potos�" if location == "San Luis"
			replace location = "San Luis Potos�" if location == "San Luis POtosi"
			replace location = "San Luis Potos�" if location == "San Luis Potosi"
			replace location = "San Luis Potos�" if location == "San Luis Potosi states"
			replace location = "San Luis Potos�" if location == "SanLuis Potosi"
			replace location = "San Luis Potos�" if location == "SanLuis Potosi"
			replace location = "Nuevo Le�n" if location == "Sierrade la Marta" | location == "Sierra de la Marta"
			replace location = "Sinaloa" if location == "Sinalda" | location == "Sinalda"
			replace location = "Sinaloa" if location == "Sinaloa"
			replace location = "Sinaloa" if location == "Sinalda"
			replace location = "Sinaloa" if location == "Sinaloa states"
			expand 2 if location == "Sinaloz y Colima states"
				bysort event_id location: gen mult = _n
				replace location = "Sinaloa" if location == "Sinaloz y Colima states" & mult == 1
				replace location = "Colima" if location == "Sinaloz y Colima states" & mult == 2
				drop mult
			replace location = "Sinaloa" if location == "Sinoloa State"
			replace location = "Chihuahua" if location == "Sonora"
			replace location = "Chihuahua" if location == "Sonora(Basse Californie)" | location == "Sonora (Basse Californie)"
			replace location = "Colima" if location == "Statesof Colima" | location == "States of Colima"
			replace location = "Tabasco" if location == "Tabasco"
			replace location = "Tabasco" if location == "Tabascostate" | location == "Tabasco state"
			replace location = "Tamaulipas" if location == "Tamaulipas"
			replace location = "Tamaulipas" if location == "Tamaulipas (Nuevo Leon state)"
			replace location = "Tamaulipas" if location == "Tamaulipas state"
			replace location = "Tamaulipas" if location == "Tamaulipas states"
			replace location = "Tamaulipas" if location == "Tamaupilas"
			replace location = "Tamaulipas" if location == "Tampico"
			replace location = "Tamaulipas" if location == "Tampulipas"
			replace location = "Baja California" if location == "Tecate"
			replace location = "Colima" if location == "Tecoman municipalities (Colima state)"
			replace location = "Distrito Federal" if location == "Tenochititlan"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Teocelo"
			replace location = "Baja California" if location == "Tijuana"
			replace location = "Yucat�n" if location == "Tizimin (Merida satte"
			replace location = "Guerrero" if location == "Tlacoachisrlahuaca"
			replace location = "Guerrero" if location == "Tlalixtaquilla"
			replace location = "Tlaxcala" if location == "Tlaxcala"
			replace location = "M�xico" if location == "Tlenepantlade Baz" | location == "Tlenepantla de Baz"
			replace location = "Quer�taro" if location == "Toliman"
			replace location = "Jalisco" if location == "Tonila"
			replace location = "Sinaloa" if location == "Topolobampo"
			replace location = "Coahuila" if location == "Torreon"
			replace location = "M�xico" if location == "Tultitlan (Mexico city"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Tuxpan"
			replace location = "Chiapas" if location == "Tuxtla Gutierrez"
			replace location = "Oaxaca" if location == "Tzhuantepec"
			replace location = "" if location == "Valleyof Mexico)" | location == "Valley of Mexico)"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Veracruz"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Veracruz regions"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Veracruz states"
			replace location = "Veracruz de Ignacio de la Llave" if location == "VeracruzState" | location == "Veracruz State"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Veracruzstate" | location == "Veracruz state"
			replace location = "Colima" if location == "Villa de Alvarez"
			replace location = "Coahuila" if location == "Villa de Fuentes"
			replace location = "Chiapas" if location == "Villaflores"
			replace location = "Puebla" if location == "WestPuebla" | location == "West Puebla"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Xalapa"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Xico"
			replace location = "Morelos" if location == "Yautepec"
			replace location = "Veracruz de Ignacio de la Llave" if location == "Yecuatla"
			replace location = "Yucat�n" if location == "Yucata peninsula"
			replace location = "Yucat�n" if location == "Yucatan"
			replace location = "Yucat�n" if location == "YucatanPeninsula" | location == "Yucatan Peninsula"
			replace location = "Zacatecas" if location == "Zacatecas"
			replace location = "Zacatecas" if location == "Zacatecas states"
			replace location = "Jalisco" if location == "Zapotitlan de Vadillo"
			replace location = "Coahuila" if location == "Zaragoza (Coahuila states)"


			save `sub_work', replace
			restore
			drop if iso3 == "MEX"
			append using `sub_work'


			************************************************************************
			** IND
			************************************************************************
			** Not in India
			drop if location == "Abbaspur (Poonch district)" | location == "Abbaspur(Poonch district)" | location == "Yorkshire (West: Leeds"	// In Pakistan
			drop if location == "Athmuqam (Muzaffarad district)" | location == "Athmuqam(Muzaffarad district)"	// In Pakistan
			drop if location == "Dhir Kot" | location == "DhirKot"	// pakistan
			drop if location == "Haegoan"	// Can't find
			drop if location == "Hajira"	// Pakistan
			drop if location == "Hattian"	// Pakistan
			drop if location == "Haveli (Bagh district)" | location == "Haveli(Bagh district)"	// Pakistan
			drop if location == "OmanSea"	// Oman Sea
			drop if location == "Rawalakot"	// Pakistan
			drop if location == "Shovanoliunion of Assasuni Upazila"	// BanglUSER

			preserve
			keep if iso3 == "IND"

			replace location = "Uttar PrUSER, Urban" if location == "Agra"
			replace location = "Gujarat, Urban" if location == "Ahmadabad"
			replace location = "Gujarat, Urban" if location == "Ahmedabad"
			replace location = "Maharashtra" if location == "Ahmednagar"
			replace location = "Uttar PrUSER, Urban" if location == "Ajamgarh"
			replace location = "Maharashtra" if location == "Akoladistricts (Maharasthra state); Rewa"
			replace location = "Himachal PrUSER" if location == "Akpaand Kharo. Shimla"
			replace location = "Kerala, Urban" if location == "Alappuzha"
			replace location = "Kerala" if location == "Alapuzzhadistricts (Kerala state)"
			replace location = "Uttar PrUSER, Urban" if location == "Aligadh"
			replace location = "Uttar PrUSER, Urban" if location == "Aligarh"
			replace location = "Telangana" if location == "Alilabad districts (Andhra PrUSER state)" | location == "Alilabaddistricts (Andhra PrUSER state)"
			replace location = "Madhya PrUSER, Urban" if location == "Alirajpur districts (Madhya PrUSER state); Ajmer" | location == "Alirajpurdistricts (Madhya PrUSER state); Ajmer"
			replace location = "Uttar PrUSER, Urban" if location == "Allahabad"
			replace location = "Uttar PrUSER, Urban" if location == "Allahbad"
			replace location = "Uttarakhand, Urban" if location == "Almora"
			replace location = "Uttarakhand, Urban" if location == "Almora and Pithoragarh) Level 1 = Uttar PrUSER" | location == "Almoraand Pithoragarh) Level 1 = Uttar PrUSER"
			replace location = "Rajasthan, Urban" if location == "Alwar"
			replace location = "Bihar, Urban" if location == "Amarpur"
			replace location = "Uttar PrUSER" if location == "AmbedkarNagar"
			replace location = "Maharashtra, Urban" if location == "Amravati"
			replace location = "Kerala, Urban" if location == "Amravilla (Trivadrum district" | location == "Amravilla(Trivadrum district"
			replace location = "Maharashtra" if location == "Amrawatidistrict"
			replace location = "Gujarat, Urban" if location == "Amreli"
			replace location = "Andhra PrUSER, Urban" if location == "Ananatapur"
			replace location = "Gujarat, Urban" if location == "Anand"
			replace location = "Andhra PrUSER, Urban" if location == "Anantapur"
			replace location = "Andhra PrUSER, Urban" if location == "Anantapur and Kadpa (Andhra PrUSER)" | location == "Anantapurand Kadpa (Andhra PrUSER)"
			replace location = "Jammu and Kashmir, Urban" if location == "Anantnag"
			replace location = "The Six Minor Territories" if location == "AndamanIsl."
			replace location = "The Six Minor Territories" if location == "AndamanIsl. (Golfe du Bengale)"
			replace location = "Andhra PrUSER" if location == "Andra PrUSER"
			replace location = "Andhra PrUSER" if location == "AndhraPrUSER states"
			replace location = "Andhra PrUSER" if location == "AndhraPrUSER)"
			replace location = "Andhra PrUSER" if location == "AndhraPradresh states"
			replace location = "Andhra PrUSER" if location == "AndhraPrUSER" | location == "AndraPrUSER"
			replace location = "Andhra PrUSER" if location == "Andra PrUSER" | location == "Andrha PrUSER"
			replace location = "Andhra PrUSER" if location == "AndrhraPrUSER (Vishakhapatnam"
			replace location = "Assam" if location == "Anglong"
			replace location = "Orissa" if location == "Angul"
			replace location = "Orissa" if location == "Angul(Orissa state)"
			replace location = "Arunachal PrUSER, Rural" if location == "Anini"
			replace location = "Madhya PrUSER" if location == "Anuppur"
			replace location = "Bihar" if location == "Araria"
			replace location = "Arunachal PrUSER" if location == "Arunachal"
			replace location = "Arunachal PrUSER" if location == "Arunachal PrUSER"
			replace location = "Arunachal PrUSER" if location == "ArunachalPrUSER and Island of Majuli state"
			replace location = "Arunachal PrUSER" if location == "ArunachalPrUSER states"
			replace location = "Arunachal PrUSER" if location == "ArunachalPrUSER)"
			replace location = "Arunachal PrUSER" if location == "ArunachalPrUSER"
			replace location = "Bihar, Rural" if location == "Arwal"
			replace location = "Madhya PrUSER" if location == "Ashoknagar"
			replace location = "Assam" if location == "Assam" | location == "Assam."
			replace location = "Assam" if location == "Assam)"
			replace location = "Assam" if location == "Assam states"
			replace location = "Assam" if location == "Assamstate"
			replace location = "Assam" if location == "Assamstates"
			replace location = "Assam" if location == "Assamstate"
			replace location = "Orissa, Rural" if location == "Astaranga"
			expand 3 if location == "Atharmalik(Angul district) (Jajpur district) - Orissa state. Chayyisgarh state and Madhya PrUSER state"
				bysort event_id location: gen mult = _n
				replace location = "Orissa, Rural" if location == "Atharmalik(Angul district) (Jajpur district) - Orissa state. Chayyisgarh state and Madhya PrUSER state" & mult == 1
				replace location = "Chhattisgarh" if location == "Atharmalik(Angul district) (Jajpur district) - Orissa state. Chayyisgarh state and Madhya PrUSER state" & mult == 2
				replace location = "Madhya PrUSER" if location == "Atharmalik(Angul district) (Jajpur district) - Orissa state. Chayyisgarh state and Madhya PrUSER state" & mult == 3
				drop mult
			replace location = "Uttar PrUSER" if location == "Auraiya"
			replace location = "Maharashtra, Urban" if location == "Aurangabad"
			replace location = "Uttar PrUSER, Urban" if location == "Azamgarh"
			replace location = "Uttar PrUSER, Urban" if location == "Azamgarharea (Uttar PrUSER)"
			replace location = "Uttar PrUSER" if regexm(location, "Badaun")
			replace location = "Uttar PrUSER" if location == "Badayu"
			replace location = "Madhya PrUSER, Urban" if location == "Badwani"
			replace location = "Karnataka, Urban" if location == "Bagalkot"
			replace location = "Karnataka, Urban" if location == "Bagalkote"
			replace location = "Uttarakhand" if location == "Bageshwar(new district)"
			replace location = "Uttarakhand" if location == "Bageshwarn districts (Uttarakhand state)" | location == "Bageshwarndistricts (Uttarakhand state)"
			replace location = "Madhya PrUSER" if location == "Bagh"
			replace location = "Uttar PrUSER, Urban" if location == "Bahraich"
			replace location = "Assam" if location == "Baksa"
			replace location = "Assam" if location == "Baksa(Assam)"
			replace location = "Madhya PrUSER" if location == "Balaghat"
			replace location = "Madhya PrUSER" if location == "Balaghat(Madhya PrUSER)"
			replace location = "Orissa, Urban" if location == "Balangir"
			replace location = "Orissa" if location == "Balasore"
			replace location = "Orissa" if location == "Balasore district(Orissa State)" | location == "Balasoredistrict (Orissa State)"
			replace location = "Orissa" if location == "Balasore district(Orissa)" | location == "Balasoredistrict (Orissa)"
			replace location = "Uttar PrUSER" if location == "Balia"
			replace location = "Assam" if location == "Baliabeel"
			replace location = "Orissa, Rural" if location == "Balipatna (Khorda district)" | location == "Balipatna(Khorda district)"
			replace location = "Uttar PrUSER" if location == "Ballia"
			replace location = "Uttar PrUSER" if location == "Balrampur"
			replace location = "Gujarat" if location == "BanasKantha"
			replace location = "Gujarat" if location == "Banaskanda"
			replace location = "Gujarat" if location == "Banaskhanta"
			replace location = "Uttar PrUSER, Urban" if location == "Banda"
			replace location = "Karnataka, Urban" if location == "Bangalore"
			replace location = "Karnataka, Rural" if location == "Banglore Rural" | location == "BangloreRural"
			replace location = "Bihar, Urban" if location == "Banka"
			replace location = "Bihar, Urban" if location == "Banka (Bihar state)" | location == "Banka(Bihar state)" | location == "BangloreRural"
			replace location = "Orissa, Rural" if location == "Banki"
			replace location = "West Bengal" if location == "Bankura"
			replace location = "Gujarat, Rural" if location == "Banni"
			replace location = "Andhra PrUSER, Urban" if location == "Banswada"
			replace location = "Uttar PrUSER" if location == "Barabanki"
			replace location = "Uttar PrUSER" if location == "Barabanki(Uttar PrUSER)"
			replace location = "Orissa, Urban" if location == "Baragarh"
			replace location = "Uttar PrUSER, Urban" if location == "Baraich"
			replace location = "Jammu and Kashmir, Urban" if location == "Baramula"
			replace location = "Uttar PrUSER, Urban" if location == "Bareilly"
			replace location = "Uttar PrUSER, Urban" if location == "Bareli"
			replace location = "Orissa, Urban" if location == "Bargarh"
			replace location = "Orissa, Urban" if location == "Baripada"
			replace location = "Rajasthan" if location == "Barmer"
			replace location = "Assam" if location == "Barpeta"
			replace location = "Assam" if location == "Barpeta.(Assam state)"
			replace location = "Orissa" if location == "Basalore"
			replace location = "West Bengal, Urban" if location == "Basanti"
			replace location = "Uttar PrUSER" if location == "Basti"
			replace location = "Assam, Rural" if location == "Baushkata (Dhubri district" | location == "Baushkata(Dhubri district"
			replace location = "West Bengal" if location == "Bayof Bengal (West Bengal)"
			expand 4 if location == "Bayof Bengal"
				bysort event_id location: gen mult = _n
				replace location = "Tamil Nadu" if location == "Bayof Bengal" & mult == 1
				replace location = "Andhra PrUSER" if location == "Bayof Bengal" & mult == 2
				replace location = "Orissa" if location == "Bayof Bengal" & mult == 3
				replace location = "West Bengal" if location == "Bayof Bengal" & mult == 4
				drop mult
			replace location = "Maharashtra" if location == "Beed"
			replace location = "Bihar, Urban" if location == "Begusarai"
			replace location = "Karnataka" if location == "Belgaum"
			replace location = "Karnataka, Urban" if location == "Bellary"
			replace location = "West Bengal" if location == "Beltali(Voochbehar district"
			replace location = "West Bengal" if location == "BengalOccidental"
			replace location = "West Bengal" if location == "BengaleOccidental"
			replace location = "Assam" if location == "Bengerbhita"
			replace location = "Madhya PrUSER" if location == "Betuldistrict (Madhya PrUSER state)"
			replace location = "Jammu and Kashmir, Rural" if location == "Bhaderwah"
			replace location = "Orissa, Urban" if location == "Bhadrak"
			replace location = "Orissa, Urban" if location == "Bhadrak and Jajpur (Orissa state)" | location == "Bhadrakand Jajpur (Orissa state)"
			replace location = "Bihar, Urban" if location == "Bhagalpur"
			replace location = "Bihar, Urban" if location == "Bhalagpur"
			replace location = "Jammu and Kashmir" if location == "Bhallessa"
			replace location = "Maharashtra" if location == "Bhandara"
			replace location = "Gujarat, Urban" if location == "Bharuch"
			replace location = "Gujarat, Urban" if location == "Bharuch(Gujarat)"
			replace location = "Gujarat, Urban" if location == "Bhavnagar"
			replace location = "Rajasthan, Urban" if location == "Bhilwara"
			replace location = "Madhya PrUSER" if location == "Bhind"
			replace location = "Bihar" if location == "Bhojpur"
			replace location = "West Bengal, Rural" if location == "Bholarhat"
			replace location = "Assam, Rural" if location == "Bhurakata"
			replace location = "Karnataka, Urban" if location == "Bidar"
			replace location = "Bihar" if location == "Bihar"
			replace location = "Bihar" if location == "Biharstate"
			replace location = "Bihar" if location == "Biharstates"
			replace location = "Bihar" if location == "Biharstate"
			replace location = "Karnataka" if location == "Bijapur"
			replace location = "Uttar PrUSER" if location == "Bijnor"
			replace location = "Rajasthan, Urban" if location == "Bikaner"
			replace location = "Assam" if location == "Bilasipara"
			replace location = "Chhattisgarh" if location == "Bilaspur"
			expand 2 if location == "Bilaspurand Mandi districts (Himachal PrUSER)"
				bysort event_id location: gen mult = _n
				replace location = "Chhattisgarh" if location == "Bilaspurand Mandi districts (Himachal PrUSER)" & mult == 1
				replace location = "Chhattisgarh" if location == "Bilaspurand Mandi districts (Himachal PrUSER)" & mult == 2
				drop mult
			replace location = "West Bengal, Urban" if location == "Birbhum"
			replace location = "West Bengal, Urban" if location == "Birbhum district (West Bengal)" | location == "Birbhumdistrict (West Bengal)"
			replace location = "Jharkhand" if location == "Bokaro"
			expand 2 if location == "Bokodistricts (Assam state); Tura"
				bysort event_id location: gen mult = _n
				replace location = "Assam" if location == "Bokodistricts (Assam state); Tura" & mult == 1
				replace location = "Meghalaya, Urban" if location == "Bokodistricts (Assam state); Tura" & mult == 2
				drop mult
			replace location = "Maharashtra, Urban" if location == "Bombay"
			replace location = "Maharashtra, Urban" if location == "Ghatkopar(Bombay)"
			replace location = "Maharashtra, Urban" if location == "Bombay(Assam state)"
			replace location = "Maharashtra, Urban" if location == "Bombay(city) Level 1 = Maharashtra"
			replace location = "Assam, Urban" if location == "Bongaigaon"
			replace location = "Assam, Urban" if location == "Bongaigaondistricts (Assam state)"
			replace location = "Orissa" if location == "Boudh"
			replace location = "Uttar PrUSER, Urban" if location == "Bulandshahar"
			replace location = "Maharashtra" if location == "Buldhana"
			replace location = "Rajasthan" if location == "Bundi"
			replace location = "West Bengal, Urban" if location == "Burdwan"
			replace location = "West Bengal, Urban" if location == "Burdwan and Nadia (West Bengal state)" | location == "Burdwanand Nadia (West Bengal state)"
			replace location = "Madhya PrUSER, Urban" if location == "Burhanpur"
			replace location = "Bihar" if location == "Buxar"
			replace location = "Assam" if location == "Cachar"
			replace location = "West Bengal, Urban" if location == "Calcutta"
			replace location = "Kerala" if location == "Calicut"
			replace location = "West Bengal, Urban" if location == "Canning"
			replace location = "Karnataka" if location == "Chamarajnagar"
			replace location = "Himachal PrUSER" if location == "Chamba"
			replace location = "Uttarakhand" if location == "Chamoli"
			replace location = "Uttar PrUSER" if location == "Chandauli"
			replace location = "Uttar PrUSER" if location == "Chandaulidistricts (Uttar PrUSER)"
			replace location = "Manipur" if location == "Chandel"
			replace location = "The Six Minor Territories, Urban" if location == "Chandigarh"
			replace location = "Maharashtra, Urban" if location == "Chandrapur"
			replace location = "Arunachal PrUSER" if location == "Changlang"
			replace location = "Madhya PrUSER, Urban" if location == "Chattarpur"
			replace location = "Tamil Nadu, Urban" if location == "Chennai"
			replace location = "Madhya PrUSER" if location == "Chhattarpur"
			replace location = "Chhattisgarh" if location == "Chhattisgarh"
			replace location = "Chhattisgarh" if location == "Chhattisgarhstates"
			replace location = "Karnataka" if location == "Chikkaballapur"
			replace location = "Assam, Rural" if location == "Chirakhawa"
			replace location = "Assam" if location == "Chirakhawa opopara" | location == "ChirakhawaTopopara"
			replace location = "Assam" if location == "Chirang"
			replace location = "Uttar PrUSER, Urban" if location == "Chirgaon"
			replace location = "Karnataka" if location == "Chitradurga"
			replace location = "Madhya PrUSER, Urban" if location == "Chitrakut"
			replace location = "Andhra PrUSER" if location == "Chittoor"
			replace location = "Rajasthan, Urban" if location == "Chittorgarh"
			replace location = "Jammu and Kashmir" if location == "Cholglamsarvillage (Ladakh"
			replace location = "Manipur" if location == "Churachandpur"
			replace location = "Rajasthan" if location == "Churu"
			replace location = "West Bengal" if regexm(location, "Coastaldistricts of West Bengal") | location == "Coastaldistricts of West Bengal  (no better info)"
			replace location = "West Bengal" if location == "DarjeelingHills  Level 1 = West Bengal"
			replace location = "Tamil Nadu, Urban" if location == "Coonoor (Tamil Nadu)" | location == "Coonoor(Tamil Nadu)"
			replace location = "Tamil Nadu" if location == "Coty"
			replace location = "Tamil Nadu" if location == "Cuddalore"
			replace location = "Andhra PrUSER, Urban" if location == "Cuddapah"
			replace location = "Orissa, Urban" if location == "Cuttack"
			replace location = "Gujarat" if location == "Dahdod"
			replace location = "Karnataka" if location == "Dakshina"
			replace location = "Madhya PrUSER" if location == "Damoh"
			replace location = "Gujarat" if location == "Dangs"
			replace location = "Gujarat" if location == "Dangsdistricts (Gujarat)"
			replace location = "Bihar, Urban" if location == "Darbhanga"
			replace location = "Bihar" if location == "Darbhangaand Madhubani (Bihar)"
			replace location = "Bihar" if location == "MadhubaniSupaul"
			replace location = "West Bengal, Rural" if location == "Darjeeking" | location == "Darjeeling"
			replace location = "Assam" if location == "Darrang"
			replace location = "Madhya PrUSER" if location == "Datia"
			replace location = "Rajasthan" if location == "Dausa"
			replace location = "Karnataka" if location == "Davanegere"
			replace location = "Karnataka" if location == "Davangere"
			replace location = "Jammu and Kashmir, Rural" if location == "Dawar"
			replace location = "Uttarakhand, Urban" if location == "Dehran Dun (Uttaranchal)" | location == "DehranDun (Uttaranchal)"
			replace location = "Orissa, Rural" if location == "Delanga(Puri district)"
			replace location = "Delhi, Urban" if location == "Delhi"
			expand 2 if (location == "Demajidistrict(Eastern Assam); western parts Meghalaya" | location == "Demajidistrict (Eastern Assam); western parts Meghalaya")
				bysort event_id location: gen mult = _n
				replace location = "Assam" if (location == "Demajidistrict(Eastern Assam); western parts Meghalaya" | location == "Demajidistrict (Eastern Assam); western parts Meghalaya") & mult == 1
				replace location = "Meghalaya" if (location == "Demajidistrict(Eastern Assam); western parts Meghalaya" | location == "Demajidistrict (Eastern Assam); western parts Meghalaya") & mult == 2
				drop mult
			replace location = "Uttar PrUSER, Rural" if location == "Deogarh" | location == "Deoghar"
			replace location = "Uttar PrUSER" if location == "Deoria"
			replace location = "Madhya PrUSER, Urban" if location == "Dewas"
			replace location = "Assam" if location == "Dhakuakhanadistricts (Assam"
			replace location = "Jharkhand, Urban" if location == "Dhanbad"
			replace location = "Orissa, Urban" if location == "Dhankanal"
			replace location = "Tamil Nadu" if location == "Dharmapuri"
			replace location = "Karnataka" if location == "Dharwad"
			replace location = "Assam" if location == "Dhemaji"
			replace location = "Orissa, Urban" if location == "Dhenkanal"
			replace location = "Assam" if location == "Dhermaji"
			replace location = "Gujarat, Rural" if location == "Dholatar(Gujarat state)"
			replace location = "Assam" if location == "Dhubri"
			replace location = "Maharashtra, Urban" if location == "Dhule"
			replace location = "Assam" if location == "Dibrugarh"
			replace location = "Assam" if location == "Dibrugarhdsitrcits (Assam state)"
			replace location = "Assam" if location == "Didrugarh"
			replace location = "Maharashtra, Urban" if location == "Digras"
			replace location = "Madhya PrUSER" if location == "Dindori"
			replace location = "Jammu and Kashmir" if location == "Dodadistricts (Jammu and Kashmir state)"
			replace location = "Rajasthan, Urban" if location == "Doegarh"
			replace location = "Jharkhand, Urban" if location == "Dumka"
			replace location = "Rajasthan" if location == "Dungarpur"
			replace location = "Chhattisgarh" if location == "Durgdistricts (Chhattisgarh state)"
			replace location = "Chhattisgarh" if location == "Durgdistricts (Chhattisgarh)"
			replace location = "Bihar" if location == "EastChamparan"
			replace location = "Andhra PrUSER" if location == "EastGavari"
			replace location = "Andhra PrUSER" if location == "EastGodavari"
			replace location = "Jharkhand" if location == "EastSinghbhum"
			replace location = "Andhra PrUSER" if location == "Eastand West Godavari"
			replace location = "Andhra PrUSER" if location == "EastGodavari"
			replace location = "West Bengal" if location == "EastMidnapore"
			replace location = "Andhra PrUSER" if location == "Eastand West Godavari"
			replace location = "Kerala, Urban" if location == "Ernakulam)"
			replace location = "Tamil nadu" if location == "Erode"
			replace location = "Uttar PrUSER" if location == "Etah"
			replace location = "Uttar PrUSER" if location == "Etawah"
			replace location = "Uttar PrUSER, Urban" if location == "Faizabad"
			replace location = "Uttar PrUSER, Urban" if location == "Farrukabad"
			replace location = "Uttar PrUSER, Urban" if location == "Farrukkhabad"
			replace location = "Uttar PrUSER" if location == "Fatehpur"
			replace location = "Uttar PrUSER" if location == "Firozabad"
			replace location = "" if location == "Fukaldabri"
			replace location = "Karnataka" if location == "Gadag"
			replace location = "Karnataka" if location == "Gadag(Karnataka)"
			replace location = "Karnataka" if regexm(location, "Gadagdistrcits (Karnataka state); Nanded")
			replace location = "Maharashtra" if location == "Gadchiroli"
			replace location = "Orissa" if location == "Gajapathi"
			replace location = "Orissa" if location == "Gajapati"
			replace location = "Uttar PrUSER" if location == "Gajipur"
			replace location = "Tamil Nadu, Urban" if location == "GandhiMandapam"
			replace location = "Gujarat, Urban" if location == "GandhiNagar"
			replace location = "Gujarat, Urban" if location == "Gandhinagar"
			replace location = "Jammu and Kashmir, Rural" if location == "Gandoh"
			replace location = "Rajasthan" if location == "Ganganagar"
			expand 3 if location == "Gangavaram(Coast of Andhra PrUSER); South Odisha; Chhattisgarh"
				bysort event_id location: gen mult = _n
				replace location = "Andhra PrUSER, Rural" if location == "Gangavaram(Coast of Andhra PrUSER); South Odisha; Chhattisgarh" & mult == 1
				replace location = "Orissa" if location == "Gangavaram(Coast of Andhra PrUSER); South Odisha; Chhattisgarh" & mult == 2
				replace location = "Chhattisgarh" if location == "Gangavaram(Coast of Andhra PrUSER); South Odisha; Chhattisgarh" & mult == 3
				drop mult
			replace location = "Orissa" if location == "Ganjam"
			replace location = "Orissa" if location == "Ganjamdistrict (Orissa)"
			replace location = "Jharkhand" if location == "Garhwa"
			replace location = "Uttarakhand" if location == "Garhwal& Kumaon (districts of Nainital"
			replace location = "Meghalaya, Rural" if location == "Garo Hills (Meghalaya state)" | location == "GaroHills (Meghalaya state)"
			replace location = "Uttar PrUSER" if location == "GautamBudh Nagar"
			replace location = "Bihar, Urban" if location == "Gaya"
			replace location = "Uttar PrUSER, Urban" if location == "Ghaziabad"
			replace location = "Uttar PrUSER, Urban" if location == "Ghaziabad (Utar PrUSER)" | location == "Ghaziabad(Utar PrUSER)"
			replace location = "Uttar PrUSER, Urban" if location == "Ghazipur"
			replace location = "Jharkhand" if location == "Giridih"
			replace location = "Goa" if location == "Goa"
			replace location = "Assam" if location == "Goalpara"
			replace location = "Assam" if location == "Goalparadistrict (Assam) Bengal"
			replace location = "Orissa, Rural" if location == "Gobalpur"
			replace location = "Jharkhand" if location == "Godda"
			replace location = "Assam" if location == "Golaghat"
			expand 4 if location == "Golfedu Bengale"
				bysort event_id location: gen mult = _n
				replace location = "Tamil Nadu" if location == "Golfedu Bengale" & mult == 1
				replace location = "Andhra PrUSER" if location == "Golfedu Bengale" & mult == 2
				replace location = "Orissa" if location == "Golfedu Bengale" & mult == 3
				replace location = "West Bengal, Rural" if location == "Golfedu Bengale" & mult == 4
				drop mult
			replace location = "Uttar PrUSER" if location == "Gonda"
			replace location = "Maharashtra" if location == "Gondia"
			replace location = "Bihar" if location == "Gopalganj"
			replace location = "Uttar PrUSER, Urban" if location == "Gorakhpur"
			replace location = "Gujarat" if location == "Gujarat" | location == "Gujurat"
			replace location = "Gujarat" if location == "Gujaratstate"
			replace location = "Gujarat" if location == "Gujaratstate)"
			replace location = "Gujarat" if location == "Gujaratstates"
			replace location = "Gujarat" if location == "GujaratCoast"
			replace location = "Karnataka, Urban" if location == "Gulbarga"
			replace location = "Jharkhand" if location == "Gumla"
			replace location = "Madhya PrUSER" if location == "Guna"
			replace location = "Andhra PrUSER" if location == "Guntardistricts (Andhra PrUSER)"
			replace location = "Andrha PrUSER" if location == "Guntoor(Guntur) and Prakasham districts (Andhra PrUSER)"
			replace location = "Andhra PrUSER" if location == "Guntur"
			replace location = "Andhra PrUSER" if location == "Gunturdistrits (Andhra PrUSER state); Tamil Nadu state"
			replace location = "Madhya PrUSER, Urban" if location == "Gwalior"
			replace location = "Assam" if location == "Hailakandi"
			replace location = "West Bengal, Urban" if location == "Haldibari"
			replace location = "Himachal PrUSER" if location == "Hamirpur"
			replace location = "Rajasthan" if location == "Hanumangarh"
			replace location = "Uttar PrUSER" if location == "Hardoi"
			replace location = "Uttarakhand, Rural" if location == "Harsil(near Gangotri"
			replace location = "Haryana" if location == "Haryana"
			replace location = "Haryana" if location == "Haryanastates"
			replace location = "Haryana" if location == "Haryana'sAmbala and Kurukshetra districts"
			replace location = "Uttar PrUSER, Urban" if location == "Hathras"
			replace location = "Karnataka" if location == "Haveri"
			replace location = "Haryana" if location == "Hayana"
			replace location = "Jharkhand, Urban" if location == "Hazaribagh"
			replace location = "Karnataka, Rural" if location == "Hemavathi"
			replace location = "Himachal PrUSER" if location == "HimachaclPrUSER"
			replace location = "Himachal PrUSER" if location == "HimachelPrUSER"
			replace location = "Himachal PrUSER" if location == "Himachal"
			replace location = "Himachal PrUSER" if location == "HimachalPrUSER"
			replace location = "Himachal PrUSER" if location == "HimachalPrUSER states"
			replace location = "Himachal PrUSER" if location == "HimachalPrUSER)"
			replace location = "Himachal PrUSER" if location == "HimachalPrUSER"
			replace location = "Himachal PrUSER" if location == "Himalayanstate of Himachal PrUSER."
			replace location = "Jammu and Kashmir, Rural" if location == "Himalyanregion of Kashmir and Jammu (Chenab valley)"
			replace location = "Jammu and Kashmir" if location == "Haryanaand Jammu"
			replace location = "Maharasthra" if location == "Hingoli"
			replace location = "Maharasthra" if location == "Holi(Latur and Osmanabad districts"
			replace location = "West Bengal" if location == "Hooghly"
			replace location = "West Bengal" if location == "Hoogly"
			replace location = "Madhya PrUSER, Urban" if location == "Hopal"
			replace location = "Madhya PrUSER, Urban" if location == "Hoshangabad"
			replace location = "West Bengal" if location == "Howrah"
			replace location = "West Bengal" if location == "Howrahdistricts (West Bengal)"
			replace location = "West Bengal" if location == "HowrahandHooghly (West Bengal)"
			replace location = "Telangana, Urban" if location == "Hyberabad" | location == "Hyderabad" | location == "Hyderabad"
			replace location = "Telangana, Urban" if location == "Hyderabad(Andhra PrUSER)"
			replace location = "Kerala" if location == "Idukki"
			replace location = "Manipur, Urban" if location == "Imphal--East"
			replace location = "Manipur, Urban" if location == "Imphal-West"
			replace location = "Jammu and Kashmir" if location == "IndianCachemire)"
			replace location = "" if location == "Isok"
			replace location = "Karnataka, Urban" if location == "JPNagar"
			replace location = "Karnataka, Urban" if location == "JPNagar"
			replace location = "Madhya PrUSER, Urban" if location == "Jabalpur(Madhya PrUSER)" | location == "Jabalpur"
			expand 3 if location == "Jabalpur(Mahya PrUSER); Uttar Prasdesh; Assam"
				bysort event_id location: gen mult = _n
				replace location = "Madhya PrUSER" if location == "Jabalpur(Mahya PrUSER); Uttar Prasdesh; Assam" & mult == 1
				replace location = "Uttar PrUSER" if location == "Jabalpur(Mahya PrUSER); Uttar Prasdesh; Assam" & mult == 2
				replace location = "Assam" if location == "Jabalpur(Mahya PrUSER); Uttar Prasdesh; Assam" & mult == 3
				drop mult
			replace location = "Madhya PrUSER" if location == "Jabalpurand Seoni districts (Madhya PrUSER state)"
			replace location = "Orissa" if location == "Jagatsinghpur"
			replace location = "Orissa" if location == "Jagatsingpurdistricts (Orissa)"
			replace location = "Meghalaya" if location == "JaintaHills district (Meghalaya)"
			replace location = "Rajasthan, Urban" if location == "Jaipur"
			replace location = "Rajasthan" if location == "Jaisalmer"
			replace location = "Orissa" if location == "Jajapur"
			replace location = "Orissa" if location == "Jajapur(Orissa"
			replace location = "Orissa" if location == "Jajpur"
			replace location = "Uttar PrUSER" if location == "Jalaun"
			replace location = "Orissa, Urban" if location == "Jaleshwar(Midnapore"
			replace location = "Rajasthan, Rural" if location == "Jalgaon"
			replace location = "Rajasthan" if location == "Jalgaondistrict"
			replace location = "Rajasthan" if location == "Jalloredistricts (Rajasthan state)"
			replace location = "Maharashtra" if location == "Jalna"
			replace location = "Rajasthan" if location == "Jalore"
			replace location = "West Bengal" if location == "Jalpaiguri"
			replace location = "Jammu and Kashmir" if location == "Jammu"
			replace location = "Jammu and Kashmir" if location == "Jammu& Kashmir state"
			replace location = "Jammu and Kashmir" if location == "Jammu(Kashmir)"
			replace location = "Jammu and Kashmir" if location == "Jammuand Kashmir"
			replace location = "Jammu and Kashmir" if location == "Jammuand Kasmir"
			replace location = "Jammu and Kashmir" if location == "Jammuregion" | location == "Jammuand Cachemire states" | location == "Jammuregion. almost all 10 districts affected"
			replace location = "Gujarat, Urban" if location == "Jamnagar"
			replace location = "Jharkhand" if location == "Jamtara"
			replace location = "Bihar" if location == "Jamui"
			replace location = "Uttar PrUSER, Urban" if location == "Jaunpur"
			replace location = "Bihar" if location == "Jehanabad"
			replace location = "Madhya PrUSER, Urban" if location == "Jhabua"
			replace location = "Uttar PrUSER" if location == "Jhansi"
			replace location = "Uttar PrUSER" if location == "Jhapadistrict"
			replace location = "Jharkhand" if location == "Jharkhand"
			replace location = "Jharkhand" if location == "Jharkhandstates"
			replace location = "Orissa" if location == "Jharsuguda"
			replace location = "Orissa, Rural" if location == "Jhingirital(Orissa)"
			replace location = "Rajasthan" if location == "Jhunjhunu"
			replace location = "Rajasthan, Urban" if location == "Jodhpur"
			replace location = "Assam, Urban" if location == "Jorhat"
			replace location = "Assam, Urban" if location == "Jorhat(Assam)"
			replace location = "Jharkhand, Urban" if location == "Josidih"
			replace location = "Gujarat, Urban" if location == "Junagadh"
			replace location = "Uttar PrUSER" if location == "JyotibaphuleNagar"
			replace location = "Karnataka, Rural" if location == "Kabini(Karnataka)"
			replace location = "Gujarat, Urban" if location == "Kachch-Bhuj"
			replace location = "Andhra PrUSER, Urban" if location == "Kadapa"
			replace location = "Andhra PrUSER, Urban" if location == "Kadapaand Nellore districts (Andhra PrUSER)"
			replace location = "Bihar" if location == "Kaimur"
			replace location = "Orissa, Rural" if location == "Kakatpur"
			replace location = "West Bengal, Rural" if location == "Kakdwip"
			replace location = "Andhra PrUSER, Urban" if location == "Kakinada(Andhra PrUSER state)"
			replace location = "Orissa" if location == "Kalahandi"
			replace location = "Himachal PrUSER, Rural" if location == "Kalpaand Yangthang districts"
			replace location = "Assam" if location == "Kamrup"
			replace location = "Orissa, Urban" if location == "Kanas"
			replace location = "Tamil Nadu" if location == "Kancheepuram"
			replace location = "Orissa" if location == "Kandhamal(Orissa)"
			replace location = "Himachal PrUSER, Urban" if location == "Kangra"
			expand 3 if location == "Kannadadistricts (Karnataka state); Maharashtra state; Kurnool"
				bysort event_id location: gen mult = _n
				replace location = "Karnataka" if location == "Kannadadistricts (Karnataka state); Maharashtra state; Kurnool" & mult == 1
				replace location = "Maharashtra" if location == "Kannadadistricts (Karnataka state); Maharashtra state; Kurnool" & mult == 2
				replace location = "Andhra PrUSER, Urban" if location == "Kannadadistricts (Karnataka state); Maharashtra state; Kurnool" & mult == 3
				drop mult
			replace location = "Uttar PrUSER, Urban" if location == "Kannauj"
			replace location = "Tamil Nadu" if location == "Kanniyakumari"
			replace location = "Uttar PrUSER, Urban" if location == "Kannoj"
			replace location = "Kerala, Urban" if location == "Kannur"
			replace location = "Uttar PrUSER, Urban" if location == "Kanpur"
			replace location = "Uttar PrUSER, Urban" if location == "Kanpur(Uttar PrUSER)"
			replace location = "Uttar PrUSER" if location == "KanpurDehat"
			replace location = "Uttar PrUSER" if location == "KanpurNagar"
			replace location = "Orissa" if location == "Kantapada(Cuttack district)"
			replace location = "Tamil Nadu" if location == "KanyaKumari" | location == "Kanyakumari"
			replace location = "The Six Minor Territories" if location == "Karaikal(Union Territory Pondicherry) (Tamil Nadu state)"
			replace location = "The Six Minor Territories" if location == "Karaikal)"
			replace location = "The Six Minor Territories" if location == "Karaikalregion (Pondicherry)"
			replace location = "Assam" if location == "Karamganj(Assam)"
			replace location = "Assam" if location == "Karbi-Anglong"
			replace location = "Assam" if location == "Karbi-SivsagarnLakhimpur"
			replace location = "Himachal PrUSER, Rural" if location == "Karcham"
			replace location = "Jammu and Kashmir" if location == "Kargildistrict (Kashmir)"
			replace location = "Assam" if location == "Karimganj"
			replace location = "Telangana, Urban" if location == "Karimnagar"
			replace location = "Karnataka" if location == "Karnataka"
			replace location = "Karnataka" if location == "Karnatakastate"
			replace location = "Karnataka" if location == "Karnatakastates"
			replace location = "Tamil Nadu" if location == "Karur"
			replace location = "Karnataka, Urban" if location == "Karwar"
			replace location = "Kerala, Urban" if location == "Kasaragod"
			replace location = "Kerala, Urban" if location == "Kasargode(Kerala State)"
			replace location = "Uttar PrUSER" if location == "KashiramNagar"
			replace location = "Jammu and Kashmir" if location == "Kashmir"
			replace location = "West Bengal, Rural" if location == "Kasiabari"
			replace location = "Uttar PrUSER" if location == "KasiramNagar"
			replace location = "Bihar" if location == "Katihar"
			replace location = "Madhya PrUSER" if location == "Katni"
			replace location = "Uttar PrUSER" if location == "Kaushambi"
			replace location = "Orissa" if location == "Kendraada"
			replace location = "Orissa" if location == "Kendrapara"
			replace location = "Orissa" if location == "Kendraparadistrict (Orissa)"
			replace location = "Orissa, Urban" if location == "Keonjhar"
			replace location = "Kerala" if location == "Kerala"
			replace location = "Kerala" if location == "Kerala(Kollam"
			replace location = "Kerala" if location == "Kerala;Assam"
			replace location = "Kerala" if location == "Keralastate"
			replace location = "Kerala" if location == "Keralastate)"
			replace location = "Himachal PrUSER, Rural" if location == "Khab"
			replace location = "Bihar" if location == "Khagaria"
			replace location = "Bihar" if location == "Khagariadistricts (Northern Bihar state)"
			replace location = "Telangana, Urban" if location == "Khamam"
			replace location = "Telangana, Urban" if location == "Khammam"
			replace location = "Telangana, Urban" if location == "Khammam)of Andhra PrUSER"
			replace location = "Madhya PrUSER" if location == "Khandwa"
			replace location = "Manipur" if location == "KhangabokKhullakpkam Laikai districts (Manipur state)"
			replace location = "Telangana, Urban" if location == "Kharimnagar"
			replace location = "Manipur" if location == "Khatong"
			replace location = "Gujarat, Rural" if location == "Khavda"
			replace location = "Gujarat, Urban" if location == "Kheda"
			replace location = "Madhya PrUSER, Rural" if location == "Khiri"
			replace location = "Orissa" if location == "Khordha"
			replace location = "Jharkhand" if location == "Khunsti"
			replace location = "Orissa" if location == "Khurda"
			replace location = "Maharashtra, Rural" if location == "Killari"
			replace location = "Tamil Nadu, Urban" if location == "Kilpauk"
			replace location = "Himachal PrUSER" if location == "Kinnaur"
			replace location = "Nagaland, Rural" if location == "Kiphere"
			replace location = "Bihar" if location == "Kishanganj"
			replace location = "Jammu and Kashmir, Urban" if location == "Kishtwar"
			replace location = "Karnataka" if location == "Kodagu"
			replace location = "Jharkhand" if location == "Koderma"
			replace location = "Nagaland" if location == "Kohima"
			replace location = "Assam" if location == "Kokrajhar"
			replace location = "Karnataka" if location == "Kolar"
			replace location = "Maharashtra" if location == "Kolhapur"
			replace location = "West Bengal, Urban" if location == "Kolkata"
			replace location = "Kerala, Urban" if location == "Kollam"
			replace location = "Maharashtra" if location == "Konkanregion (Maharashtra)"
			replace location = "Karnataka" if location == "Koppal"
			replace location = "Jammu and Kashmir, Rural" if location == "KoraPani"
			replace location = "Orissa" if location == "Koraput"
			replace location = "Rajasthan, Urban" if location == "Kota"
			replace location = "Kerala" if location == "Kottayam"
			replace location = "Tamil Nadu, Urban" if location == "Kotturpuram"
			replace location = "Kerala, Urban" if location == "Kozhikode"
			replace location = "Andhra PrUSER" if location == "Krishna"
			replace location = "Andhra PrUSER" if location == "Krishna(Andhra PrUSER)"
			replace location = "Andhra PrUSER" if location == "Krishnadistricts (Andhra PrUSER)"
			replace location = "Andhra PrUSER" if location == "Krishnagirin"
			replace location = "Himachal PrUSER" if location == "Kullu"
			replace location = "West Bengal, Urban" if location == "Kultali"
			replace location = "Himachal PrUSER" if location == "Kulu"
			replace location = "Himachal PrUSER" if location == "Kulu(Himachal PrUSER)"
			replace location = "Andhra PrUSER, Urban" if location == "Kurnool"
			replace location = "Uttar PrUSER" if location == "Kushinagar"
			replace location = "Gujarat" if location == "Kutch"
			replace location = "Himachal PrUSER" if location == "Lahauland Spiti"
			replace location = "Manipur" if location == "Laitonjam"
			replace location = "Assam" if location == "Lakhimpu"
			replace location = "Assam" if location == "Lakhimpur"
			replace location = "Assam" if location == "Lakhimpur(Assam state)"
			replace location = "Assam" if location == "Lakhimpur-Kheri"
			replace location = "Assam" if location == "LakhimpurKheri"
			replace location = "Bihar" if location == "Lakhisarai"
			replace location = "Gujarat, Rural" if location == "Lakhpatvillages (Kutch district"
			replace location = "Karnataka, Rural" if location == "Lakshmanathirtha"
			replace location = "Jharkhand" if location == "Latehar"
			replace location = "Maharashtra, Urban" if location == "Latur"
			replace location = "Maharashtra, Urban" if location == "Laturarea"
			replace location = "Mizoram" if location == "Lawngtlai"
			replace location = "Jammu and Kashmir" if location == "LehBus Stand"
			replace location = "Manipur" if location == "LeimapokamBazar Khunpham"
			replace location = "Himachal PrUSER, Rural" if location == "Leo"
			replace location = "Jharkhand" if location == "Lohardaga"
			replace location = "Arunachal PrUSER" if location == "Lohitand Anjaw"
			replace location = "Nagaland" if location == "LUSEReng"
			replace location = "Manipur" if location == "Lourembam"
			replace location = "Uttar PrUSER, Urban" if location == "Lucknow"
			replace location = "Mizoram" if location == "Lunglei"
			replace location = "Bihar" if location == "Madhbani"
			replace location = "Uttar PrUSER, Rural" if location == "Madhepura"
			replace location = "Uttar PrUSER" if location == "Madhubani"
			replace location = "Uttar PrUSER" if location == "Madhubanidistricts (Uttar PrUSER state)"
			replace location = "Madhya PrUSER" if location == "MadhyaPrUSER"
			replace location = "Madhya PrUSER" if location == "MadhyaPrasdeh"
			replace location = "Madhya PrUSER" if location == "MadhyaPrasdesh"
			replace location = "Tamil Nadu, Urban" if location == "Madras"
			replace location = "Tamil Nadu, Urban" if location == "Madurai"
			replace location = "Madhya PrUSER" if location == "MadyhaPrUSER"
			replace location = "Jharkhand, Urban" if location == "Mafhupur"
			replace location = "Madhya PrUSER" if location == "MafyaPrUSER"
			replace location = "Andhra PrUSER, Urban" if location == "Mahaboobnagar"
			replace location = "Telangana, Urban" if location == "Mahabubnagar"
			replace location = "Telangana, Urban" if location == "Mahakalpada(Kendrapara district)"
			replace location = "Maharashtra" if location == "Maharashrta"
			replace location = "Maharashtra" if location == "Maharashstra" | location == "Maharasthra" | location == "Maharasthra"
			replace location = "Maharashtra" if location == "Maharashtra"
			replace location = "Maharashtra" if location == "Maharashtrastate)"
			replace location = "Maharashtra" if location == "Maharashtrastates"
			replace location = "Maharashtra" if location == "Maharastra"
			replace location = "Maharashtra" if location == "Maharastrastate)"
			replace location = "Chhattisgarh" if location == "Mahasamund"
			replace location = "Maharashtra" if location == "Malin"
			replace location = "Telangana" if location == "Mahboobnagar"
			replace location = "Gujarat, Urban" if location == "Mahesana"
			replace location = "Uttar PrUSER" if location == "Mahoba"
			replace location = "Madhya PrUSER, Urban" if location == "Maihar(Madhya PrUSER)"
			replace location = "Uttar PrUSER, Urban" if location == "Mainpuri"
			replace location = "Uttar PrUSER, Urban" if location == "Mainpuri.(Uttar PrUSER) Saini"
			replace location = "Assam, Rural" if location == "Majuli"
			replace location = "Kerala, Urban" if location == "Malappuramand Thiruvananthapuram (Kerala state)"
			replace location = "West Bengal" if location == "Maldadistricts (West Bengal)"
			replace location = "Mizoram" if location == "Mamit"
			replace location = "Himachal PrUSER, Urban" if location == "Mandi"
			replace location = "Madhya PrUSER, Urban" if location == "Mandla"
			replace location = "Karnataka" if location == "Mandya"
			replace location = "Manipur" if location == "Manipur"
			replace location = "Manipur" if location == "Manipurstates"
			replace location = "Orissa, Rural" if location == "Marshagal"
			replace location = "Uttar PrUSER" if location == "Mathura"
			replace location = "West Bengal, Rural" if location == "Mathurapur"
			replace location = "Uttar PrUSER, Urban" if location == "Mau"
			replace location = "Orissa" if location == "Mayurbhang"
			replace location = "Orissa" if location == "Mayurbhanj"
			replace location = "Telangana, Urban" if location == "Medak"
			replace location = "Uttar PrUSER, Urban" if location == "Meerut"
			replace location = "Meghalaya" if location == "Meghalaya"
			replace location = "Meghalaya" if location == "Meghalayastates"
			replace location = "Gujarat, Urban" if location == "Mehsana"
			replace location = "West Bengal, Urban" if location == "Mekhliganj"
			replace location = "Uttar PrUSER, Urban" if location == "Merut"
			replace location = "West Bengal" if location == "MidnaporeEast and West"
			replace location = "Uttar PrUSER" if location == "Midnapurdis.- Level 1 = Uttar PrUSER"
			replace location = "Uttar PrUSER" if location == "Mirzapurdistricts (Uttar PrUSER state); Patna"
			replace location = "Mirozam" if location == "Mizorem" | location == "MizoramState"
			replace location = "Nagaland, Urban" if location == "Mokokchung"
			replace location = "Nagaland, Urban" if location == "Mokokchungdistrict (Nagaland)"
			replace location = "Nagaland" if location == "Mon"
			replace location = "Uttar PrUSER, Urban" if location == "Moradabad"
			replace location = "Madhya PrUSER, Urban" if location == "Morena"
			replace location = "Assam" if location == "Morigaon"
			replace location = "Assam" if location == "Morigaon.Dhubri"
			replace location = "Goa, Urban" if location == "Mormugao"
			replace location = "Uttar PrUSER, Urban" if location == "MujafferNagar"
			replace location = "Maharashtra, Urban" if location == "Mumbaiarea"
			replace location = "Maharashtra, Urban" if location == "Mumbaoi"
			replace location = "Bihar, Urban" if location == "Munger"
			replace location = "Uttar PrUSER, Urban" if location == "Muradabad"
			replace location = "West Bengal" if location == "Murshidabad"
			replace location = "Assam" if location == "Mushakpur"
			replace location = "Uttar PrUSER, Urban" if location == "Muzaffarnagar"
			replace location = "Bihar" if location == "Muzaffarpur"
			replace location = "Jammu and Kashmir" if location == "Muzzafarabad"
			replace location = "Bihar" if location == "Muzzaffarpur"
			replace location = "Karnataka, Urban" if location == "Mysore"
			replace location = "Karnataka, Urban" if location == "Mysore(Karnataka state) Nellore"
			replace location = "Manipur, Urban" if location == "Na-Orem"
			replace location = "Nagaland" if location == "Nagaland"
			replace location = "Assam" if location == "Nagaon"
			replace location = "Tamil Nadu" if location == "Nagapattinam"
			replace location = "Tamil Nadu" if location == "Nagappattinam"
			replace location = "Tamil Nadu, Urban" if location == "Nagore"
			replace location = "Maharashtra" if location == "Nagpurdistricts (Maharashtra state)"
			expand 3 if location == "Naintial(Uttarakhand state); Bihar state; Uttar PrUSER"
				bysort event_id location: gen mult = _n
				replace location = "Uttarakhand" if location == "Naintial(Uttarakhand state); Bihar state; Uttar PrUSER" & mult == 1
				replace location = "Bihar" if location == "Naintial(Uttarakhand state); Bihar state; Uttar PrUSER" & mult == 2
				replace location = "Uttar PrUSER" if location == "Naintial(Uttarakhand state); Bihar state; Uttar PrUSER" & mult == 3
				drop mult
			replace location = "Bihar, Rural" if location == "Nalanda"
			replace location = "Assam, Urban" if location == "Nalbari"
			replace location = "Assam, Urban" if location == "Nalbri"
			replace location = "Andhra PrUSER" if location == "Nalgonda"
			replace location = "Andhra PrUSER" if location == "Nalgondadistricts (Andhra PrUSER state)"
			replace location = "Andhra PrUSER" if location == "Nalgondadistricts (Andhra PrUSER)"
			replace location = "Gujarat, Urban" if location == "Naliya"
			replace location = "Tamil Nadu, Urban" if location == "Namakkal"
			replace location = "West Bengal" if location == "Namkhana"
			replace location = "Maharashtra, Rural" if location == "Nandgavanvillages (Yewatmal district"
			replace location = "Maharashtra" if location == "Nandurbar"
			replace location = "Assam" if location == "Naogoandistrict (Assam state)"
			replace location = "Gujarat" if location == "Narmada"
			replace location = "Gujarat" if location == "Narmadadistricts (Gujarat state)"
			replace location = "Madhya PrUSER" if location == "Narsinghpur"
			replace location = "Maharashtra, Urban" if location == "Nashik"
			replace location = "Maharashtra" if location == "Nasikdistrict (Maharashtra)"
			replace location = "Gujarat" if location == "Nasvari"
			replace location = "Gujarat" if location == "Nasvaridistricts (Gujarat state)"
			replace location = "Gujarat" if location == "Natkharana"
			replace location = "Gujarat, Urban" if location == "Navsari"
			replace location = "Uttar PrUSER, Urban" if location == "Nawabganj(Uttar PrUSER)"
			replace location = "Bihar" if location == "Nawada"
			replace location = "Orissa" if location == "Nayagarh"
			replace location = "Orissa" if location == "Nayagarth"
			replace location = "West Bental" if location == "NearKandi (Murshidabad district"
			replace location = "Karnataka, Rural" if location == "NearThondebhavi (Karnataka)"
			replace location = "Andhra PrUSER, Urban" if location == "Nellore"
			replace location = "Andhra PrUSER, Urban" if location == "Nellore)"
			replace location = "Andhra PrUSER, Urban" if location == "Neloore"
			replace location = "Delhi, Urban" if location == "NewDelhi"
			expand 2 if location == "NewDelhi; Telangana"
				bysort event_id location: gen mult = _n
				replace location = "Delhi, Urban" if location == "NewDelhi; Telangana" & mult == 1
				replace location = "Telangana" if location  == "NewDelhi; Telangana" & mult == 2
				drop mult
			replace location = "The Six Minor Territories, Rural" if location == "NicobarIsl."
			replace location = "West Bengal" if location == "Nijtaraf"
			replace location = "Telangana" if location == "Nizamabad"
			replace location = "Assam" if location == "NorthCachar Hills"
			replace location = "Kerala" if location == "Northerndistricts"
			replace location = "Orissa" if location == "Nuapada"
			replace location = "Orissa" if location == "Odishastate"
			replace location = "Manipur" if location == "Oinam"
			replace location = "Orissa" if location == "Orissa"
			replace location = "Orissa" if location == "Orissastate"
			replace location = "Orissa" if location == "Orissastates"
			replace location = "Maharashtra, Urban" if location == "Osmanabad"
			replace location = "Jharkhand" if location == "Pakur"
			replace location = "Jharkhand" if location == "Palamu"
			replace location = "Rajasthan" if location == "Pali"
			replace location = "Uttar PrUSER, Urban" if location == "PalliaKalan"
			replace location = "Gujarat" if location == "Panchmahal"
			replace location = "Madhya PrUSER, Urban" if location == "Panna"
			replace location = "Tamil Nadu, Urban" if location == "Papanasam"
			replace location = "Maharashtra" if location == "Parbhani"
			replace location = "West Bengal" if location == "Parganas"
			replace location = "Orissa, Urban" if location == "Patamundai"
			replace location = "Gujarat" if location == "Patan"
			replace location = "Kerala" if location == "Pathanamthitta"
			replace location = "West Bengal, Rural" if location == "Patharaima"
			replace location = "Bihar, Urban" if location == "Patna"
			replace location = "Uttarakhand" if location == "PauriGarhwal district"
			replace location = "Assam, Rural" if location == "Peepulbaripart 1 and 2"
			replace location = "Tamil Nadu, Urban" if location == "Perambalur"
			replace location = "Tamil Nadu" if location == "Perambur(Tamil Nadu state)"
			replace location = "Nagaland" if location == "Peren"
			replace location = "Nagaland" if location == "Phek"
			replace location = "Uttar PrUSER" if location == "Pilibhit"
			replace location = "The Six Minor Territories" if location == "Pondicherry"
			replace location = "Jammu and Kashmir" if location == "Poonch(Jammu and Cachemire)" | location == "Qazigund" | location == "Ramsu"
			replace location = "Gujarat" if location == "Porbandar"
			replace location = "Andhra PrUSER" if location == "Prakasam"
			replace location = "The Six Minor Territories" if location == "Puducherryand Karaikal"
			replace location = "Tamil Nadu" if location == "Pudukkottai"
			replace location = "Tamil Nadu" if location == "Pudukottai"
			replace location = "Manipur" if location == "Pukhrambam"
			replace location = "Maharashtra, Urban" if location == "Pune"
			replace location = "Maharashtra, Urban" if location == "Pune(Maharashtra)"
			replace location = "Punjab" if location == "Punjab" | location == "Penjab"
			replace location = "Punjab" if location == "Punjab;northeast India"
			replace location = "Orissa, Urban" if location == "Puri"
			replace location = "Bihar, Urban" if location == "Purnea"
			replace location = "Bihar, Urban" if location == "Purnia"
			replace location = "Uttar PrUSER, Urban" if location == "Raebareli"
			replace location = "Uttar PrUSER, Urban" if location == "RaiBareilly"
			replace location = "Karnataka, Urban" if location == "Raichur"
			replace location = "Maharashtra" if location == "Raigad"
			replace location = "Chhattisgarh" if location == "Raipur"
			replace location = "Madhya PrUSER" if location == "Raisen"
			replace location = "Tamil Nadu, Rural" if location == "Rajanagar"
			replace location = "Rajasthan" if location == "Rajasthan"
			replace location = "Rajasthan" if location == "Rajasthanstate"
			replace location = "Rajasthan" if location == "Rajasthanstates"
			replace location = "Gujarat, Urban" if location == "Rajkot"
			replace location = "Chhattisgarh" if location == "Rajnandgaon"
			replace location = "Rajasthan" if location == "Rajsamand"
			replace location = "Karnataka, Urban" if location == "Ramanagara"
			replace location = "Tamil Nadu, Urban" if location == "Ramanathapuram"
			replace location = "Tamil Nadu, Urban" if location == "Ramanthapuramdistricts (Tamil Nadu state)"
			replace location = "Jharkhand" if location == "Ramgarh"
			replace location = "Uttar PrUSER, Urban" if location == "Rampur"
			replace location = "Uttar PrUSER, Urban" if location == "Rampur.Kinnaur region: Sangla"
			replace location = "Jharkhand" if location == "Ranchi"
			replace location = "Telangana" if location == "Rangareddy"
			replace location = "Assam, Urban" if location == "Rangia"
			replace location = "Madhya PrUSER" if location == "Ratlam"
			replace location = "Maharasthra" if location == "Ratnagiridistrict (Maharasthra)"
			replace location = "Orissa" if location == "Rayagada"
			replace location = "Orissa" if location == "Rayagda"
			replace location = "Madhya PrUSER" if location == "Rewa"
			replace location = "Madhya PrUSER" if location == "Rivaand Katni. Satna"
			replace location = "Bihar" if location == "Rohtas"
			replace location = "Arunachal PrUSER" if location == "Roing& Along (Arunachal PrUSER state)"
			replace location = "Uttarakhand, Urban" if location == "Rudraayag"
			replace location = "Gujarat" if location == "SabarKantha"
			replace location = "Gujarat" if location == "Sabarkanda"
			replace location = "Gujarat" if location == "Sabarkanthadistricts (Gujarat state)"
			replace location = "Orissa, Rural" if location == "Sadarpada"
			replace location = "Assam" if location == "Sadiya"
			replace location = "Madhya PrUSER" if location == "Sagar"
			replace location = "Uttar PrUSER" if location == "Saharanpur"
			replace location = "Bihar, Urban" if location == "Saharsa"
			replace location = "Bihar" if location == "Saharsadistricts (Bihar state)"
			replace location = "Jharkhand, Urban" if location == "Sahebganj"
			replace location = "Madhya PrUSER" if location == "Sahra"
			replace location = "Tamil Nadu, Rural" if location == "Saidapet"
			replace location = "Mizoram" if location == "Saihadistricts (Mizoram state)"
			replace location = "Tamil Nadu" if location == "Salem"
			replace location = "Bihar, Urban" if location == "Samastipur"
			replace location = "Orissa" if location == "Sambalpur"
			replace location = "Orissa" if location == "Sambalpur(Odisha/Orissa)"
			replace location = "Maharashtra" if location == "Sangli"
			replace location = "Manipur" if location == "Sanjenbam(Nambol district)"
			replace location = "Uttar PrUSER" if location == "SantKabir Nagar"
			replace location = "Jharkhand" if location == "Saraikela-Karsaan"
			replace location = "Bihar" if location == "Saran"
			replace location = "Bihar" if location == "Sarandistricts (Bihar state)"
			replace location = "Maharashtra" if location == "Satara"
			replace location = "Madhya PrUSER, Urban" if location == "Satna"
			replace location = "Gujarat" if location == "Saurashtraregion (Gujarat)"
			replace location = "Rajasthan, Urban" if location == "SawaiMadhopur"
			replace location = "Madhya PrUSER, Urban" if location == "Sehore"
			replace location = "Manipur" if location == "Senapati"
			replace location = "Madhya PrUSER, Urban" if location == "Seoni"
			replace location = "Arunachal PrUSER" if location == "Seppa"
			replace location = "Madhya PrUSER" if location == "Shahdol"
			replace location = "Uttar PrUSER, Urban" if location == "Shahjahanpur"
			replace location = "Uttar PrUSER, Urban" if location == "Shahjahapur"
			replace location = "Madhya PrUSER, Urban" if location == "Shajapur"
			replace location = "Bihar" if location == "Sheikhpura"
			replace location = "Bihar" if location == "Shekhpura"
			replace location = "Bihar" if location == "Sheohar(Bihar)"
			replace location = "Madhya PrUSER" if location == "SheopurKalan"
			replace location = "Madhya PrUSER, Urban" if location == "Shivpuri"
			replace location = "Uttar PrUSER" if location == "Shravasti"
			replace location = "Uttar PrUSER" if location == "Shravasti(Uttar PrUSER state)"
			replace location = "Uttar PrUSER" if location == "Shrawasti"
			replace location = "Assam" if location == "Sibasagar"
			replace location = "Assam" if location == "Sibsagadistricts (Assam )"
			replace location = "Uttar PrUSER" if location == "SiddharthNagar"
			replace location = "Madhya PrUSER" if location == "Sidhi"
			replace location = "Rajasthan" if location == "Sikar"
			replace location = "Sikkim" if location == "Sikkim"
			replace location = "Sikkim" if location == "Sikkimstate"
			replace location = "Jharkhand" if location == "Simdega"
			replace location = "Himachal PrUSER" if location == "Simla"
			replace location = "Madhya PrUSER" if location == "Singrauli"
			replace location = "Tamil Nadu" if location == "Sirkali"
			replace location = "Himachal PrUSER" if location == "Sirmaur"
			replace location = "Rajasthan" if location == "Sirohi"
			replace location = "Bihar" if location == "Sitalamathi"
			replace location = "Bihar" if location == "Sitamarhi" | location == "Sitamarhidistricts (Bihar state)"
			replace location = "Uttar PrUSER, Urban" if location == "Sitapur"
			replace location = "Tamil Nadu" if location == "Sivaganga"
			replace location = "Tamil Nadu" if location == "Sivasagar"
			replace location = "Tamil Nadu" if location == "Sivasagar.Districts: Dhemaji"
			replace location = "Bihar" if location == "Siwan"
			replace location = "Himachal PrUSER" if location == "Solan"
			replace location = "Himachal PrUSER" if location == "Solang(near Kullu district"
			replace location = "Maharashtra, Urban" if location == "Solapur"
			replace location = "Jammu and Kashmir, Rural" if location == "Sonamarg(Kashmir)"
			replace location = "Tripura, Urban" if location == "Sonamura(Tripura )"
			replace location = "Orissa" if location == "Sonepur"
			replace location = "Assam" if location == "Sonitpur"
			expand 2 if location == "Sonitpur(Assam state); Begusara"
				bysort event_id location: gen mult = _n
				replace location = "Assam" if location == "Sonitpur(Assam state); Begusara" & mult == 1
				replace location = "Bihar" if location == "Sonitpur(Assam state); Begusara" & mult == 2
				drop mult
			replace location = "Assam" if location == "Sonitpurdistricts (Assam)"
			replace location = "Maharashtra, Rural" if location == "Sostour"
			replace location = "West Bengal" if location == "South24 Parganas"
			replace location = "West Bengal" if location == "South24-Parganas"
			replace location = "Tripura" if location == "Southand West Tripura (Tripura)"
			replace location = "Andhra PrUSER" if location == "Srikakulam"
			replace location = "Tamil Nadu" if location == "Srirangam"
			replace location = "Orissa" if location == "SubarnapurCuttack"
			replace location = "Uttar PrUSER" if location == "Sultanpur"
			replace location = "Himachal PrUSER, Rural" if location == "Sumdo.Khaab"
			replace location = "Bihar" if location == "Supaul"
			replace location = "Gujarat, Urban" if location == "Surat" | location == "Suratdistricts (Gujarat)"
			replace location = "Gujarat" if location == "SurenderNagar districts (Gujarat state)"
			replace location = "Gujarat" if location == "Surendranagar"
			replace location = "Tamil Nadu" if location == "TNilgiris"
			replace location = "Manipur" if location == "Tamenglong"
			replace location = "Tamil Nadu" if location == "TamiNadu state"
			replace location = "Tamil Nadu" if location == "TamilNadu"
			replace location = "Tamil Nadu" if location == "TamilNadu state (Thiruvarur"
			replace location = "Tamil Nadu" if location == "TamilNadu states"
			replace location = "Jammu and Kashmir" if location == "Tangdharin Kupwara district."
			replace location = "West Bengal" if location == "Tanton"
			replace location = "Uttarakhand" if location == "TehriGarhwal"
			replace location = "Uttarakhand" if location == "Tehridistrict (Uttaranchal)"
			replace location = "Telangana" if location == "Telanganaand coastal regions (Hyderabad"
			replace location = "Telangana" if location == "Telenganaregion"
			replace location = "Maharashtra, Urban" if location == "Thane"
			replace location = "Tamil Nadu" if location == "Thanjavur"
			replace location = "Tamil Nadu" if location == "Thanjavur)"
			replace location = "Jammu and Kashmir" if location == "Thathri"
			replace location = "Tamil Nadu" if location == "Thiruvallur"
			replace location = "Tamil Nadu" if location == "Thiruvarur"
			replace location = "Manipur" if location == "Thiyam"
			replace location = "Tamil Nadu, Urban" if location == "Thoothukudi"
			replace location = "Manipur" if location == "Thoubal"
			replace location = "Kerala" if location == "Thrissir(Kerala)"
			replace location = "Madhya PrUSER" if location == "Tikamgarh"
			replace location = "Assam, Urban" if location == "Tinsukia"
			replace location = "Tamil Nadu, Urban" if location == "Tirivarur"
			replace location = "Tamil Nadu" if location == "Tiruchirapalli"
			replace location = "Tamil Nadu" if location == "Tirunelveli"
			replace location = "Tamil Nadu" if location == "TirunelveliKattabo"
			replace location = "Tamil Nadu" if location == "Tirunelvelli"
			replace location = "Tamil Nadu" if location == "Tiruvallur"
			replace location = "Tamil Nadu" if location == "Tiruvarur(Cuddalore coast"
			replace location = "Rajasthan" if location == "Tonk"
			replace location = "Tripura" if location == "Tripura"
			replace location = "Tripura" if location == "TripuraState"
			replace location = "Tripura" if location == "Tripurastates"
			replace location = "Nagaland" if location == "Tuensang"
			replace location = "Karnataka" if location == "Tumkur"
			replace location = "Tamil Nadu, Urban" if location == "Tuticorin"
			replace location = "The Six Minor Territories" if location == "UTPondicherry (Puducherry"
			replace location = "Rajasthan, Urban" if location == "Udaipur"
			replace location = "Rajasthan, Urban" if location == "Udaipur(Rajasthan)"
			replace location = "Assam" if location == "Udalguri"
			expand 2  if location == "Udalguridistricts (Assam state); Bilaspur"
				bysort event_id location: gen mult = _n
				replace location = "Assam" if location == "Udalguridistricts (Assam state); Bilaspur" & mult == 1
				replace location = "Chhattisgarh" if location == "Udalguridistricts (Assam state); Bilaspur" & mult == 2
				drop mult
			replace location = "Rajasthan" if location == "Udaypurdistricts (Rajasthan state)"
			expand 2 if location == "Ukhruldistricts (Manipur state); Dimapur"
				bysort event_id location: gen mult = _n
				replace location = "Manipur" if location == "Ukhruldistricts (Manipur state); Dimapur" & mult == 1
				replace location = "Nagaland" if location == "Ukhruldistricts (Manipur state); Dimapur" & mult == 2
				drop mult
			replace location = "Manipur" if location == "Ulto"
			replace location = "Madhya PrUSER" if location == "Umaria"
			expand 2 if location == "Unadistricts (Himachal PrUSER state); Chatra"
				bysort event_id location: gen mult = _n
				replace location = "Himachal PrUSER"  if location == "Unadistricts (Himachal PrUSER state); Chatra" & mult == 1
				replace location = "Jharkhand"  if location == "Unadistricts (Himachal PrUSER state); Chatra" & mult == 2
				drop mult
			replace location = "Uttar PrUSER" if location == "Unnao"
			replace location = "Uttar PrUSER" if location == "Unnav(Uttar PrUSER state)"
			replace location = "Jammu and Kashmir, Rural" if location == "Uri"
			replace location = "Uttar PrUSER" if location == "UttarPrUSER" | location == "UttarPrUSER state)"
			replace location = "Uttar PrUSER" if location == "UttarPrUSER state"
			replace location = "Uttar PrUSER" if location == "UttarPrUSER states"
			replace location = "Uttar PrUSER" if location == "UttarPrUSER)"
			replace location = "Uttar PrUSER" if location == "UttarPrUSER; Gonda"
			replace location = "Uttar PrUSER" if location == "UttarUSER"
			replace location = "Uttarakhand" if location == "Uttarakhand"
			replace location = "Uttarakhand" if location == "Uttarakhandstate"
			replace location = "Uttarakhand" if location == "Uttaranchal"
			replace location = "Uttarakhand" if location == "Uttarkashi'sBhagirathi valley"
			replace location = "Uttarakhand" if location == "Uttarkashidistrict. Badhkot"
			replace location = "Uttar PrUSER" if location == "UtterPrUSER" | location == "Malpavillage (Pithoragrah district" | location == "Mansunavillage (Uttar PrUSER state)" | location == "Pithoragarhdistrict (Uttarakhand)"
			replace location = "Gujarat, Urban" if location == "Vadodara"
			replace location = "Gujarat, Urban" if location == "Vadodra"
			replace location = "Bihar" if location == "Vaisahli"
			expand 2 if location == "Vaishalidistricts (Bihar state); Banglore Urban"
				bysort event_id location: gen mult = _n
				replace location = "Bihar" if location == "Vaishalidistricts (Bihar state); Banglore Urban" & mult == 1
				replace location = "Karnataka, Urban" if location == "Vaishalidistricts (Bihar state); Banglore Urban" & mult == 2
				drop mult
			replace location = "Bihar" if location == "Vaishall"
			replace location = "Gujarat, Urban" if location == "Valsad"
			replace location = "Kerala, Rural" if location == "Vamanapuram"
			replace location = "Uttar PrUSER, Urban" if location == "Varanasi"
			replace location = "Uttar PrUSER, Urban" if location == "Varanasi.Ankinghat"
			replace location = "Assam" if location == "Varpeta"
			replace location = "Tamil Nadu" if location == "Vellore"
			replace location = "Madhya PrUSER" if location == "Vidisha"
			replace location = "Tamil Nadu" if location == "Villupuram"
			replace location = "Andhra PrUSER" if location == "Visakhapatam"
			replace location = "Andhra PrUSER" if location == "Visakhapatanam"
			replace location = "Andhra PrUSER, Urban" if location == "Visakhapathnamcity"
			replace location = "Andhra PrUSER" if location == "Visakhapatnamdistrcits (Andhra PrUSER)"
			replace location = "Andhra PrUSER" if location == "Visakhapatnamdistricts (Andhra PrUSER)"
			replace location = "Andhra PrUSER" if location == "Vizianagaram"
			replace location = "Manipur" if location == "Wahengkhuman"
			replace location = "Bihar" if location == "Waidhali"
			replace location = "Telangana" if location == "Warangal"
			replace location = "Telangana" if location == "Warangan"
			replace location = "Maharashtra, Urban" if location == "Wardha"
			replace location = "Maharashtra, Urban" if location == "Washim"
			replace location = "Andhra PrUSER" if location == "WesGovari"
			expand 5 if location == "West"
				bysort event_id location: gen mult = _n
				replace location = "Rajasthan" if location == "West" & mult == 1
				replace location = "Goa" if location == "West" & mult == 2
				replace location = "Gujarat" if location == "West" & mult == 3
				replace location = "Maharashtra" if location == "West" & mult == 4
				replace location = "The Six Minor Territories" if location == "West" & mult == 5
				drop mult
			replace location = "West Bengal" if location == "WestBegal"
			replace location = "West Bengal" if location == "WestBegala"
			replace location = "West Bengal" if location == "WestBengal"
			replace location = "West Bengal" if location == "WestBengal and Orissa states"
			replace location = "West Bengal" if location == "WestBengal state"
			replace location = "West Bengal" if location == "WestBengal states"
			replace location = "West Bengal" if location == "WestBengal states)"
			replace location = "West Bengal" if location == "WestBengal)"
			replace location = "West Bengal" if location == "WestBengal."
			replace location = "Bihar" if location == "WestChamparan"
			replace location = "Bihar" if location == "WestChamparan (Bihar state)"
			replace location = "Andhra PrUSER" if location == "WestGodavari"
			replace location = "Andhra PrUSER" if location == "WestGodavari (Andhra PrUSER state)"
			expand 2 if location == "WestSinghbhum districts (Jharkhand state); Bishnupur"
				bysort event_id location: gen mult = _n
				replace location = "Jharkhand" if location == "WestSinghbhum districts (Jharkhand state); Bishnupur" & mult == 1
				replace location = "West Bengal" if location == "WestSinghbhum districts (Jharkhand state); Bishnupur" & mult == 2
				drop mult
			replace location = "Nagaland" if location == "Wokha"
			replace location = "Maharashtra" if location == "Yavatmal"
			replace location = "Arunachal PrUSER" if location == "Yingkiong"
			expand 2 if location == "Zunhebotodistricts (Nagaland state) Ambedkar Nagar"
				bysort event_id location: gen mult = _n
				replace location = "Nagaland" if location == "Zunhebotodistricts (Nagaland state) Ambedkar Nagar" & mult == 1
				replace location = "Uttar PrUSER" if location == "Zunhebotodistricts (Nagaland state) Ambedkar Nagar" & mult == 2
				drop mult
			replace location = "Bihar" if location == "districts:Muzaffarpur"
			replace location = "Kerala, Urban" if location == "districtsof Kannur"
			replace location = "Jammu and Kashmir, Rural" if location == "villagesde Doda"
			replace location = "Jammu and Kashmir" if location == "villagesdu district Kishtwar"


			save `sub_work', replace
			restore
			drop if iso3 == "IND"
			append using `sub_work'


			****************************************************************************************
			** BRA
			****************************************************************************************
			** Can't figure out
			drop if location == "33 state municipalities affected)" | location == "33state municipalities affected)"
			replace location = "" if location == "Aiuba"

			preserve
			keep if iso3 == "BRA"

			replace location = "Acre" if location == "Acre ( mainly Porto Velho" | location == "Acre state"
			replace location = "Alagoas" if location == "Alagoa Grande city (Paraiba State)" | location == "AlagoaGrande city (Paraiba State)"
			replace location = "Alagoas" if location == "Alagoas"
			replace location = "Alagoas" if location == "Alagoas (Quebrangulo)" | location == "Alagoas(Quebrangulo)"
			replace location = "Par�" if location == "Almerim"
			replace location = "Roraima" if location == "Alta Alegre" | location == "AltaAlegre"
			replace location = "Roraima" if location == "Amajari"
			replace location = "Amazonas" if location == "Amazonas"
			replace location = "Amazonas" if location == "Amazonas state"
			replace location = "Amazonas" if location == "Amazonas state (Anama"
			replace location = "Amapa" if location == "Amap�"
			replace location = "Par�" if location == "Anajas (Para state)"
			replace location = "Rio de Janeiro" if location == "Angra dos Reis"
			replace location = "Rio de Janeiro" if location == "Angra dos Reis (Rio de Janeiro state)"
			replace location = "Amazonas" if location == "Anorie"
			replace location = "S�o Paulo" if location == "Apiai (Sao Paulo state)"
			replace location = "Roraima" if location == "Apiau"
			replace location = "Santa Catarina" if location == "Aranangua (Santa Catarina state)"
			replace location = "Amazonas" if location == "Atalaia do Norte"
			replace location = "Bahia" if location == "Bahia"
			replace location = "Bahia" if location == "Bahia (NorthEast provinces)"
			replace location = "Bahia" if location == "Bahia (NorthEast s)"
			replace location = "Bahia" if location == "Bah�a"
			replace location = "Bahia" if regexm(location, "Bah\?a")
			replace location = "Bahia" if location == "Bahia states"
			replace location = "Bahia" if location == "Bahia state)" | location == "Bahia state"
			replace location = "Bahia" if location == "Barro Branco"
			replace location = "Bahia" if location == "Bom Jua (Salvador"
			replace location = "Pernambuco" if location == "Baia Branca (Mage)"
			replace location = "Rio de Janeiro" if location == "Baixada Fluminense and Guaratiba areas (Rio de Janeiro city)"
			replace location = "Rio de Janeiro" if location == "Barra Mansa"
			replace location = "Rio de Janeiro" if location == "Belford Roxo"
			replace location = "Minas Gerais" if location == "Belo Horizonte"
			replace location = "Santa Catarina" if location == "Benedito Novo"
			replace location = "Santa Catarina" if location == "Blumenau"
			replace location = "Roraima" if location == "Boa Vista"
			replace location = "Rio de Janeiro" if location == "Bom Jardim (Rio de Janeiro state)"
			replace location = "Roraima" if location == "Bom-Pim"
			replace location = "Amazonas" if location == "Caapiranga (Amazonas state)"
			replace location = "Rio de Janeiro" if location == "Cachoeiras de Macacu"
			replace location = "S�o Paulo" if location == "Cajati"
			expand 2 if location == "Campina do Sim�o) and Santa Catarina states"
				bysort event_id location: gen mult = _n
				replace location = "Paran�" if location == "Campina do Sim�o) and Santa Catarina states" & mult == 1
				replace location = "Santa Catarina" if location == "Campina do Sim�o) and Santa Catarina states" & mult == 2
				drop mult
			replace location = "Roraima" if location == "Canta"
			replace location = "Par�" if location == "Capitao Poco"
			replace location = "Roraima" if location == "Caracarai"
			replace location = "Minas Gerais" if location == "Caratinga"
			replace location = "Paran�" if location == "Cascavel"
			replace location = "Cear�" if location == "Ceara"
			replace location = "Cear�" if location == "Ceara states"
			replace location = "Piau�" if location == "Cocal (Piaui state)"
			replace location = "Minas Gerais" if location == "Contagem"
			replace location = "Santa Catarina" if location == "Criciuma"
			replace location = "Paran�" if location == "Cruz Machado"
			replace location = "Mato Grosso" if location == "Cuiaba (Mato Grosso province)"
			replace location = "Mato Grosso" if location == "Cuiaba (Mato Grosso )"
			replace location = "Paran�" if location == "Curitiba"
			replace location = "Rio de Janeiro" if location == "Duque de Caxias"
			replace location = "S�o Paulo" if location == "Eldorado (Sao Paulo state)"
			replace location = "Rio de Janeiro" if location == "Engenheiro Paulo De Frontin (Rio de Janeiro)"
			replace location = "Esp�rito Santo" if location == "Espirito Santo"
			replace location = "Esp�rito Santo" if location == "Espirito Santo (Jaguare"
			replace location = "Esp�rito Santo" if location == "Espirito Santo (Leopoldina)"
			replace location = "Esp�rito Santo" if location == "Espirito Santo state"
			replace location = "Esp�rito Santo" if location == "Espirito Santo states"
			replace location = "Santa Catarina" if location == "Florianopolis"
			replace location = "Santa Catarina" if location == "Garuva"
			replace location = "Santa Catarina" if location == "Gaspar"
			replace location = "S�o Paulo" if location == "Glicerio"
			replace location = "Paran�" if location == "Guarapuava"
			replace location = "S�o Paulo" if location == "Guaruja))"
			replace location = "S�o Paulo" if location == "Iguape"
			replace location = "Santa Catarina" if location == "Ilhota"
			replace location = "Cear�" if location == "Inhamuns"
			replace location = "Roraima" if location == "Iracema"
			replace location = "Minas Gerais" if location == "Itacarambi-Januaria-Manga area (Minas Gerais)"
			replace location = "Rio de Janeiro" if location == "Itaipava (Rio de Janeiro)"
			replace location = "S�o Paulo" if location == "Itaoca"
			replace location = "Santa Catarina" if location == "Itapoa"
			replace location = "S�o Paulo" if location == "Itariri"
			replace location = "Pernambuco" if location == "Jaboatao dos Guararapes (Pernambuco state)"
			replace location = "Pernambuco" if location == "Jabotao"
			replace location = "S�o Paulo" if location == "Jacupiranga"
			replace location = "S�o Paulo" if location == "Jales"
			replace location = "Rio de Janeiro" if location == "Japari"
			replace location = "Santa Catarina" if location == "Jaragua do Sul"
			replace location = "S�o Paulo" if location == "Jiundiai (Sao Paulo state)"
			replace location = "S�o Paulo" if location == "Juquia"
			replace location = "Bahia" if location == "Lajedinho (Bahia state)"
			replace location = "Paran�" if location == "Laranjeiras do Sul"
			replace location = "Paran�" if location == "Las Palmas (Parana)"
			replace location = "Santa Catarina" if location == "Luiz Alves"
			replace location = "Roraima" if location == "Macajai"
			replace location = "Rio Grande do Norte" if location == "Macaos"
			replace location = "Alagoas" if location == "Maceio (Alagoas state)"
			replace location = "Rio de Janeiro" if location == "Magaratiba"
			replace location = "Paran�" if location == "Mallet"
			replace location = "Amazonas" if location == "Manaquiri"
			replace location = "Amazonas" if location == "Manaus city (Amazonas state)"
			replace location = "Amazonas" if location == "Manaus)"
			replace location = "Par�" if location == "Maraba"
			replace location = "Par�" if location == "Marcaba"
			replace location = "Amap�" if location == "Maraca"
			replace location = "Maranh�o" if location == "Maranhao"
			replace location = "Mato Grosso" if location == "Mato Grosso"
			replace location = "Mato Grosso do Sul" if location == "Mato Grosso do Sul" | location == "Mato Grosso do Sul"
			replace location = "S�o Paulo" if location == "Maua)"
			replace location = "Paran�" if location == "Medianeira"
			replace location = "Minas Gerais" if location == "Mesquita"
			replace location = "Minas Gerais" if location == "Minas Gerais"
			replace location = "Minas Gerais" if location == "Minas Gerais (Belo Horizonte"
			replace location = "Minas Gerais" if location == "Minas Gerais (Belo Horizonte)"
			replace location = "Minas Gerais" if location == "Minas Gerais State(Contagem"
			replace location = "Minas Gerais" if location == "Minas Gerais and Sao Paulo; Rio de Janeiro"
			replace location = "Minas Gerais" if location == "Minas Gerais states" | location == "Mina Gerais states" | location == "Minas Gerais state)"
			replace location = "Minas Gerais" if location == "Minas Gerais states (Unai)"
			replace location = "Minas Gerais" if location == "Minas Gerias" | location == "Minas" | location == "Minas Gerais (Juiz de Foras)"
			replace location = "Minas Gerais" if location == "Minas Girais"
			replace location = "S�o Paulo" if location == "Miracatu"
			replace location = "Pernambuco" if location == "Moreno"
			replace location = "Minas Gerais" if location == "Muriae (Minas Gerais)"
			replace location = "Rio Grande do Norte" if location == "Natal"
			replace location = "Rio de Janeiro" if location == "Natividade)"
			replace location = "Cear�" if location == "Near Fortaleza (Ceara state)"
			replace location = "Rio de Janeiro" if location == "Niteori" | location == "Niteroi"
			replace location = "Rio de Janeiro" if location == "Niteroi )"
			expand 9 if location == "Nord East"
				bysort event_id location: gen mult = _n
				replace location = "Maranh�o" if location == "Nord East" & mult == 1
				replace location = "Piau�" if location == "Nord East" & mult == 2
				replace location = "Rio Grande do Norte" if location == "Nord East" & mult == 3
				replace location = "Para�ba" if location == "Nord East" & mult == 4
				replace location = "Pernambuco" if location == "Nord East" & mult == 5
				replace location = "Alagoas" if location == "Nord East" & mult == 6
				replace location = "Sergipe" if location == "Nord East" & mult == 7
				replace location = "Bahia" if location == "Nord East" & mult == 8
				replace location = "Pernambuco" if location == "Nord East" & mult == 9
				drop mult
			expand 9 if location == "Nordeste"
				bysort event_id location: gen mult = _n
				replace location = "Maranh�o" if location == "Nordeste" & mult == 1
				replace location = "Piau�" if location == "Nordeste" & mult == 2
				replace location = "Rio Grande do Norte" if location == "Nordeste" & mult == 3
				replace location = "Para�ba" if location == "Nordeste" & mult == 4
				replace location = "Pernambuco" if location == "Nordeste" & mult == 5
				replace location = "Alagoas" if location == "Nordeste" & mult == 6
				replace location = "Sergipe" if location == "Nordeste" & mult == 7
				replace location = "Bahia" if location == "Nordeste" & mult == 8
				replace location = "Pernambuco" if location == "Nordeste" & mult == 9
				drop mult
			replace location = "Roraima" if location == "Normandia"
			replace location = "Ceara" if location == "North East (Ceara)"
			replace location = "Rio de Janeiro" if location == "Nova Friburgo"
			replace location = "Rio de Janeiro" if location == "Nova Friburgo)"
			replace location = "Rio de Janeiro" if location == "Nova Iguacu"
			replace location = "Rio de Janeiro" if location == "Nova Iguacu (Baixada Fluminense region)"
			replace location = "Esp�rito Santo" if location == "Nova Venecia)"
			replace location = "Rio de Janeiro" if location == "Novo Friburgo municipalities (Rio de Janeiro state)"
			replace location = "Minas Gerais" if location == "Ouro Preto)"
			replace location = "Roraima" if location == "Pacaraima"
			replace location = "Mato Grosso" if location == "Para Mato Grosso"
			replace location = "Rio de Janeiro" if location == "Paracambi"
			replace location = "Para�ba" if location == "Paraiba" | location == "Paraiba state"
			replace location = "Paran�" if location == "Parana"
			replace location = "Paran�" if location == "Parana (Campo Largo"
			replace location = "Paran�" if location == "Parana State"
			replace location = "Paran�" if location == "Parana states"
			replace location = "S�o Paulo" if location == "Pariquera Acu"
			replace location = "Rio de Janeiro" if location == "Paty de Alferes"
			replace location = "Pernambuco" if location == "Permnambouc" | location == "Pernambuco)"
			replace location = "Pernambuco" if location == "Pernambouc"
			replace location = "Pernambuco" if location == "Pernambouc (Barreiros) states"
			replace location = "Pernambuco" if location == "Pernambuco" | location == "Pernabuco"
			replace location = "Pernambuco" if location == "Pernambuco region"
			replace location = "Pernambuco" if location == "Pernambuco state"
			replace location = "S�o Paulo" if location == "Peruibe"
			replace location = "S�o Paulo" if location == "Petropolis"
			replace location = "Rio de Janeiro" if location == "Piabanka river. Paulo de Fronty"
			replace location = "Piaui" if location == "Piaui" | location == "Piau�"
			replace location = "Piaui" if location == "Piaui State"
			replace location = "Piaui" if location == "Piaui states"
			replace location = "Paran�" if location == "Pinh�o"
			replace location = "Minas Gerais" if location == "Pirangucu. Santa Rita do Sapucai."
			replace location = "Rio Grande do Sul" if location == "Porto Alegre (Rio Grande do Sul state)"
			replace location = "Rio Grande do Sul" if location == "Porto Alegre and Novo Harmburgo)"
			replace location = "Par�" if location == "Porto de Moz"
			replace location = "S�o Paulo" if location == "Queimados"
			replace location = "Minas Gerais" if location == "Raposos"
			replace location = "Minas Gerais" if location == "Raposos and Riberao das Neves"
			replace location = "Pernambuco" if location == "Recife"
			replace location = "Pernambuco" if location == "Recife city"
			replace location = "Rio de Janeiro" if location == "Resende"
			replace location = "Paran�" if location == "Reserva do Igua�u"
			replace location = "Minas Gerais" if location == "Riberao das Neves (Minas Gerais state)"
			replace location = "Rio de Janeiro" if location == "Rio" | inlist(location, "(1) Rio","(2) Rio","(3) Rio")
			replace location = "Esp�rito Santo" if location == "Rio Banal"
			replace location = "S�o Paulo" if location == "Rio Claro)"
			replace location = "Rio de Janeiro" if location == "Rio De Janeiro (Rio de Janeiro state)"
			replace location = "Rio Grande do Sul" if location == "Rio Grande Do Sul"
			replace location = "Rio Grande do Sul" if location == "Rio Grande Do Sul (Sao Louren?o Do Sul) states"
			replace location = "Rio Grande do Norte" if location == "Rio Grande do Norte" | location == "Rio Grande Do Norte"
			replace location = "Rio Grande do Norte" if location == "Rio Grande do Norte states"
			replace location = "Rio Grande do Sul" if location == "Rio Grande do Sul"
			replace location = "Rio Grande do Sul" if location == "Rio Grande do Sul (Alto Feliz)"
			replace location = "Rio Grande do Sul" if location == "Rio Grande do Sul (Igrejinha"
			replace location = "Rio Grande do Sul" if location == "Rio Grande do Sul state"
			replace location = "Rio Grande do Sul" if location == "Rio Grande do Sul state (Viamao"
			replace location = "Rio Grande do Sul" if location == "Rio Grande do Sul states"
			replace location = "Rio Grande do Sul" if location == "Rio Grande)"
			replace location = "Rio de janeiro" if location == "Rio capital"
			replace location = "Rio de Janeiro" if location == "Rio de Janeiro" | location == "Rio de janeiro"
			replace location = "Rio de Janeiro" if location == "Rio de Janeiro (Campos Dos Goytacazes"
			replace location = "Rio de Janeiro" if location == "Rio de Janeiro (Campos do Goytacazes"
			replace location = "Rio de Janeiro" if location == "Rio de Janeiro (Rio de Janeiro"
			replace location = "Rio de Janeiro" if location == "Rio de Janeiro (Rio de Janeiro state)"
			replace location = "Rio de Janeiro" if location == "Rio de Janeiro (Tres Rios"
			replace location = "Rio de Janeiro" if location == "Rio de Janeiro city area"
			replace location = "Rio de Janeiro" if location == "Rio de Janeiro state"
			replace location = "Rio de Janeiro" if location == "Rio de Janeiro states"
			replace location = "Rio de Janeiro" if location == "Rio de Janiero"
			replace location = "Santa Catarina" if location == "Rio do Sul"
			replace location = "Santa Catarina" if location == "Rio dos Cedros"
			replace location = "Rond�nia" if location == "Rondonia"
			replace location = "Sau Paulo" if location == "San Pablo" | location == "Sao Paulo city  (Sao Paulo)"
			replace location = "S�o Paulo" if location == "San Paulo" | location == "(4) Sao Paulo"
			replace location = "Santa Catarina" if location == "Santa Cartarina"
			replace location = "Santa Catarina" if location == "Santa Catarina"
			replace location = "Santa Catarina" if location == "Santa Catarina (Blumenau"
			replace location = "Santa Catarina" if location == "Santa Catarina state"
			replace location = "Santa Catarina" if location == "Santa Caterina"
			replace location = "Bahia" if location == "Santo Amaro Da Purificado (Central Bahia)"
			replace location = "Minas Gerais" if location == "Santo Antonio Do Rio Abaixo)"
			replace location = "Minas Gerais" if location == "Santo Antonio do Rio Abaixo"
			replace location = "Santa Catarina" if location == "Sao Bonifacio"
			replace location = "Rio de Janeiro" if location == "Sao Goncalo"
			replace location = "Rio de Janeiro" if location == "Sao Jao de Meriti"
			replace location = "Santa Catarina" if location == "Sao Joaquim (Santa Catarina)"
			replace location = "Maranh�o" if location == "Sao Joao da Bastida"
			replace location = "S�o Paulo" if location == "Sao Jose do Rio Preto"
			replace location = "S�o Paulo" if location == "Sao Jose dos Campos"
			replace location = "S�o Paulo" if location == "Sao Jose dos Campos municipalities"
			replace location = "S�o Paulo" if location == "Sao Paulo" | location == "Sau Paulo"
			replace location = "S�o Paulo" if location == "Sao Paulo (Aracatuba"
			replace location = "S�o Paulo" if location == "Sao Paulo (Jundiai"
			replace location = "S�o Paulo" if location == "Sao Paulo (Santos"
			replace location = "S�o Paulo" if location == "Sao Paulo City"
			replace location = "S�o Paulo" if location == "Sao Paulo State(Sao Paulo city )"
			replace location = "S�o Paulo" if location == "Sao Paulo and Campos de Jordao cities. Towns: Volta Redonda"
			replace location = "S�o Paulo" if location == "Sao Paulo city (Sao Paulo)"
			replace location = "S�o Paulo" if location == "Sao Paulo state"
			replace location = "S�o Paulo" if location == "Sao Paulo states"
			replace location = "Rio de Janeiro" if location == "Sapucaia)"
			replace location = "Sergipe" if location == "Sergipe"
			replace location = "Rio de Janeiro" if location == "Seropedica"
			replace location = "Bahia" if location == "South West Bahia"
			replace location = "Rio de Janeiro" if location == "States of Rio de Janeiro"
			replace location = "Paran�" if location == "Sulinain"
			replace location = "Rio de Janeiro" if location == "Sumidaro"
			replace location = "Paran�" if location == "S�o Matheus do Sul"
			replace location = "Rio de Janeiro" if location == "Tangua"
			replace location = "Minas Gerais" if location == "Teofilo Otoni (Minas Gerais)"
			replace location = "Minas Gerais" if location == "Teofilo Otoni (Minas Gerais state)"
			replace location = "Piaui" if location == "Teresina (Piaui State)"
			replace location = "Rio de Janeiro" if location == "Teresopolis" | location == "Teresopolis cities (Rio De Janeiro state)"
			replace location = "Tocantins" if location == "Tocantins"
			replace location = "Rio Grande do Sul" if location == "Torres (Rio Grande do Sul state)"
			replace location = "Roraima" if location == "Uiramuta municipalities (Roraima state)"
			replace location = "Bahia" if location == "Valenca"
			replace location = "Minas Gerais" if location == "Visconde Do Rio Branco"
			replace location = "Minas Gerais" if location == "Visconde do Rio Branco"
			replace location = "Pernambuco" if location == "Vitoria de Santo Antao"
			replace location = "Rio de Janeiro" if location == "Xerem"
			replace location = "Rio de Janeiro" if location == "Xerem (Duque De Caxias)"
			replace location = "Santa Catarina" if location == "cities (Santa Catarina state)"
			replace location = "Rio Grande do Sul" if location == "greater Porto Alegre)"

			save `sub_work', replace
			restore
			drop if iso3 == "BRA"
			append using `sub_work'


			***************************************************************************************
			** JPN
			***************************************************************************************
			** Not in Japan
			drop if location == "China" & iso3 == "JPN"
			drop if location == "efectures"
			drop if location == "Mountainousareas"
			drop if location == "N.A.on the source"
			drop if location == "Naruse(Narusetyoo)"
			drop if location == "Southwestern"
			drop if location == "Yoneyama"

			preserve
			keep if iso3 == "JPN"

			replace location = "Hokkaido" if location == "Abutatyoo"
			replace location = "Aichi" if location == "Aichi"
			replace location = "Akita" if location == "Akita"
			expand 4 if location == "Akita(Honshu Isl.); Hokkaido Isl. ; Niigita; Nagano"
				bysort event_id location: gen mult = _n
				replace location = "Akita" if location == "Akita(Honshu Isl.); Hokkaido Isl. ; Niigita; Nagano" & mult ==1
				replace location = "Hokkaido" if location == "Akita(Honshu Isl.); Hokkaido Isl. ; Niigita; Nagano" & mult == 2
				replace location = "Niagata" if location == "Akita(Honshu Isl.); Hokkaido Isl. ; Niigita; Nagano" & mult == 3
				replace location = "Nagano" if location == "Akita(Honshu Isl.); Hokkaido Isl. ; Niigita; Nagano" & mult == 4
				drop mult
			replace location = "Akita" if location == "Akitaefectures (Tohoku region)"
			replace location = "Kagoshima" if location == "Akune)"
			replace location = "Kagoshima" if location == "Amami-OshimaIsl."
			replace location = "Kagoshima" if location == "Amani"
			replace location = "Aomori" if location == "Aomori"
			replace location = "Aomori" if location == "Aomoriefectures" | location == "AomoriPrefecture - Hokkaido"
			replace location = "Hiroshima" if location == "Asakita(Hiroshima)"
			replace location = "Chiba" if location == "Chiba"
			replace location = "Chiba" if location == "Chiba(Tiba)"
			replace location = "Chiba" if location == "Chibaefecture" | location == "Chibaefecture (Near Tokyo)"
			expand 9 if location == "Chubu"
				bysort event_id location: gen mult = _n
				replace location = "Aichi" if location == "Chubu" & mult == 1
				replace location = "Fukui" if location == "Chubu" & mult == 2
				replace location = "Gifu" if location == "Chubu" & mult == 3
				replace location = "Ishikawa" if location == "Chubu" & mult == 4
				replace location = "Nagano" if location == "Chubu" & mult == 5
				replace location = "Niagata" if location == "Chubu" & mult == 6
				replace location = "Shizuoka" if location == "Chubu" & mult == 7
				replace location = "Toyama" if location == "Chubu" & mult == 8
				replace location = "Yamanashi" if location == "Chubu" & mult == 9
				drop mult
			expand 5 if location == "Chugoku"
				bysort event_id location: gen mult = _n
				replace location = "Hiroshima" if location == "Chugoku" & mult == 1
				replace location = "Okayama" if location == "Chugoku" & mult == 2
				replace location = "Shimane" if location == "Chugoku" & mult == 3
				replace location = "Tottori" if location == "Chugoku" & mult == 4
				replace location = "Yamaguchi" if location == "Chugoku" & mult == 5
				drop mult
			replace location = "Hokkaido" if location == "Datesi"
			replace location = "Hokkaido" if location == "Eastcoast of Hokaido Isl."
			replace location = "Ehime" if location == "Ehime"
			replace location = "Ehime" if location == "Ehimeefectures (Honshu Isl.)"
			replace location = "Shizuoka" if location == "Fujieda(Huziedasi)"
			replace location = "Fukuoka" if location == "Fukuoka" | location == "Fukuokaefecture"
			replace location = "Fukuoka" if location == "Fukuoka(Hiroshima State)"
			replace location = "Fukuoka" if location == "Fukuoka(Hukuoka)"
			replace location = "Fukushima" if location == "Fukushima"
			replace location = "Fukushima" if location == "FukushimaPrefecture" | location == "Fukushimaefecture"
			replace location = "Fukushima" if location == "Fukushimaregions"
			replace location = "Miyagi" if location == "Furukawa(Hurukawasi)"
			replace location = "Miyagi" if location == "Furukawa(Miyagi-Hurukawas)"
			replace location = "Saga" if location == "Genkai(Hukuokasi Nisiku) (Kyushu)"
			replace location = "Gifu" if location == "Gifu"
			replace location = "Gifu" if location == "Gifuefecture; Shimane; Hiroshima"
			replace location = "Gifu" if location == "Gifuregions" | location == "GifuPrefecture"
			replace location = "Gifu" if location == "Gifus"
			replace location = "Gunma" if location == "Gunma"
			replace location = "Tokyo" if location == "HachijoIsl."
			replace location = "Aomori" if location == "Hachinohe(Aomori efecture"
			replace location = "Aomori" if location == "Hachinohe(North-East Honshu)"
			replace location = "Nagano" if location == "Hakuba(Nagano efecture)"
			expand 34 if location == "Hinshu"
				bysort event_id location: gen mult = _n
				replace location = "Aomori" if location == "Hinshu" & mult == 1
				replace location = "Iwate" if location == "Hinshu" & mult == 2
				replace location = "Miyagi" if location == "Hinshu" & mult == 3
				replace location = "Akita" if location == "Hinshu" & mult == 4
				replace location = "Yamagata" if location == "Hinshu" & mult == 5
				replace location = "Fukushima" if location == "Hinshu" & mult == 6
				replace location = "Ibaraki" if location == "Hinshu" & mult == 7
				replace location = "Tochigi" if location == "Hinshu" & mult == 8
				replace location = "Gunma" if location == "Hinshu" & mult == 9
				replace location = "Saitama" if location == "Hinshu" & mult == 10
				replace location = "Chiba" if location == "Hinshu" & mult == 11
				replace location = "Tokyo" if location == "Hinshu" & mult == 12
				replace location = "Kanagawa" if location == "Hinshu" & mult == 13
				replace location = "Niagata" if location == "Hinshu" & mult == 14
				replace location = "Toyama" if location == "Hinshu" & mult == 15
				replace location = "Ishikawa" if location == "Hinshu" & mult == 16
				replace location = "Fukui" if location == "Hinshu" & mult == 17
				replace location = "Yamanashi" if location == "Hinshu" & mult == 18
				replace location = "Nagano" if location == "Hinshu" & mult == 19
				replace location = "Gifu" if location == "Hinshu" & mult == 20
				replace location = "Shizuoka" if location == "Hinshu" & mult == 21
				replace location = "Aichi" if location == "Hinshu" & mult == 22
				replace location = "Mie" if location == "Hinshu" & mult == 23
				replace location = "Shiga" if location == "Hinshu" & mult == 24
				replace location = "Kyoto" if location == "Hinshu" & mult == 25
				replace location = "Osaka" if location == "Hinshu" & mult == 26
				replace location = "Hyogo" if location == "Hinshu" & mult == 27
				replace location = "Nara" if location == "Hinshu" & mult == 28
				replace location = "Wakayama" if location == "Hinshu" & mult == 29
				replace location = "Tottori" if location == "Hinshu" & mult == 30
				replace location = "Shimane" if location == "Hinshu" & mult == 31
				replace location = "Okayama" if location == "Hinshu" & mult == 32
				replace location = "Hiroshima" if location == "Hinshu" & mult == 33
				replace location = "Yamaguchi" if location == "Hinshu" & mult == 34
				drop mult
			replace location = "Hiroshima" if location == "Hiroshima"
			replace location = "Hiroshima" if location == "HiroshimaPrefecture"
			replace location = "Hiroshima" if location == "Hirosima"
			replace location = "" if location == "Hishikari"
			replace location = "Hokkaido" if location == "Hokkaido"
			replace location = "Hokkaido" if location == "HokkaidoIsl." | location == "HokkaisoIsl."
			replace location = "Hokkaido" if location == "Hokkaidoo"
			expand 34 if location == "Honshu"
				bysort event_id location: gen mult = _n
				replace location = "Aomori" if location == "Honshu" & mult == 1
				replace location = "Iwate" if location == "Honshu" & mult == 2
				replace location = "Miyagi" if location == "Honshu" & mult == 3
				replace location = "Akita" if location == "Honshu" & mult == 4
				replace location = "Yamagata" if location == "Honshu" & mult == 5
				replace location = "Fukushima" if location == "Honshu" & mult == 6
				replace location = "Ibaraki" if location == "Honshu" & mult == 7
				replace location = "Tochigi" if location == "Honshu" & mult == 8
				replace location = "Gunma" if location == "Honshu" & mult == 9
				replace location = "Saitama" if location == "Honshu" & mult == 10
				replace location = "Chiba" if location == "Honshu" & mult == 11
				replace location = "Tokyo" if location == "Honshu" & mult == 12
				replace location = "Kanagawa" if location == "Honshu" & mult == 13
				replace location = "Niagata" if location == "Honshu" & mult == 14
				replace location = "Toyama" if location == "Honshu" & mult == 15
				replace location = "Ishikawa" if location == "Honshu" & mult == 16
				replace location = "Fukui" if location == "Honshu" & mult == 17
				replace location = "Yamanashi" if location == "Honshu" & mult == 18
				replace location = "Nagano" if location == "Honshu" & mult == 19
				replace location = "Gifu" if location == "Honshu" & mult == 20
				replace location = "Shizuoka" if location == "Honshu" & mult == 21
				replace location = "Aichi" if location == "Honshu" & mult == 22
				replace location = "Mie" if location == "Honshu" & mult == 23
				replace location = "Shiga" if location == "Honshu" & mult == 24
				replace location = "Kyoto" if location == "Honshu" & mult == 25
				replace location = "Osaka" if location == "Honshu" & mult == 26
				replace location = "Hyogo" if location == "Honshu" & mult == 27
				replace location = "Nara" if location == "Honshu" & mult == 28
				replace location = "Wakayama" if location == "Honshu" & mult == 29
				replace location = "Tottori" if location == "Honshu" & mult == 30
				replace location = "Shimane" if location == "Honshu" & mult == 31
				replace location = "Okayama" if location == "Honshu" & mult == 32
				replace location = "Hiroshima" if location == "Honshu" & mult == 33
				replace location = "Yamaguchi" if location == "Honshu" & mult == 34
				drop mult
			expand 34 if location == "Honshu(Fukushima and Nagano)"
				bysort event_id location: gen mult = _n
				replace location = "Aomori" if location == "Honshu(Fukushima and Nagano)" & mult == 1
				replace location = "Iwate" if location == "Honshu(Fukushima and Nagano)" & mult == 2
				replace location = "Miyagi" if location == "Honshu(Fukushima and Nagano)" & mult == 3
				replace location = "Akita" if location == "Honshu(Fukushima and Nagano)" & mult == 4
				replace location = "Yamagata" if location == "Honshu(Fukushima and Nagano)" & mult == 5
				replace location = "Fukushima" if location == "Honshu(Fukushima and Nagano)" & mult == 6
				replace location = "Ibaraki" if location == "Honshu(Fukushima and Nagano)" & mult == 7
				replace location = "Tochigi" if location == "Honshu(Fukushima and Nagano)" & mult == 8
				replace location = "Gunma" if location == "Honshu(Fukushima and Nagano)" & mult == 9
				replace location = "Saitama" if location == "Honshu(Fukushima and Nagano)" & mult == 10
				replace location = "Chiba" if location == "Honshu(Fukushima and Nagano)" & mult == 11
				replace location = "Tokyo" if location == "Honshu(Fukushima and Nagano)" & mult == 12
				replace location = "Kanagawa" if location == "Honshu(Fukushima and Nagano)" & mult == 13
				replace location = "Niagata" if location == "Honshu(Fukushima and Nagano)" & mult == 14
				replace location = "Toyama" if location == "Honshu(Fukushima and Nagano)" & mult == 15
				replace location = "Ishikawa" if location == "Honshu(Fukushima and Nagano)" & mult == 16
				replace location = "Fukui" if location == "Honshu(Fukushima and Nagano)" & mult == 17
				replace location = "Yamanashi" if location == "Honshu(Fukushima and Nagano)" & mult == 18
				replace location = "Nagano" if location == "Honshu(Fukushima and Nagano)" & mult == 19
				replace location = "Gifu" if location == "Honshu(Fukushima and Nagano)" & mult == 20
				replace location = "Shizuoka" if location == "Honshu(Fukushima and Nagano)" & mult == 21
				replace location = "Aichi" if location == "Honshu(Fukushima and Nagano)" & mult == 22
				replace location = "Mie" if location == "Honshu(Fukushima and Nagano)" & mult == 23
				replace location = "Shiga" if location == "Honshu(Fukushima and Nagano)" & mult == 24
				replace location = "Kyoto" if location == "Honshu(Fukushima and Nagano)" & mult == 25
				replace location = "Osaka" if location == "Honshu(Fukushima and Nagano)" & mult == 26
				replace location = "Hyogo" if location == "Honshu(Fukushima and Nagano)" & mult == 27
				replace location = "Nara" if location == "Honshu(Fukushima and Nagano)" & mult == 28
				replace location = "Wakayama" if location == "Honshu(Fukushima and Nagano)" & mult == 29
				replace location = "Tottori" if location == "Honshu(Fukushima and Nagano)" & mult == 30
				replace location = "Shimane" if location == "Honshu(Fukushima and Nagano)" & mult == 31
				replace location = "Okayama" if location == "Honshu(Fukushima and Nagano)" & mult == 32
				replace location = "Hiroshima" if location == "Honshu(Fukushima and Nagano)" & mult == 33
				replace location = "Yamaguchi" if location == "Honshu(Fukushima and Nagano)" & mult == 34
				drop mult
			expand 34 if location == "Honshu)"
				bysort event_id location: gen mult = _n
				replace location = "Aomori" if location == "Honshu)" & mult == 1
				replace location = "Iwate" if location == "Honshu)" & mult == 2
				replace location = "Miyagi" if location == "Honshu)" & mult == 3
				replace location = "Akita" if location == "Honshu)" & mult == 4
				replace location = "Yamagata" if location == "Honshu)" & mult == 5
				replace location = "Fukushima" if location == "Honshu)" & mult == 6
				replace location = "Ibaraki" if location == "Honshu)" & mult == 7
				replace location = "Tochigi" if location == "Honshu)" & mult == 8
				replace location = "Gunma" if location == "Honshu)" & mult == 9
				replace location = "Saitama" if location == "Honshu)" & mult == 10
				replace location = "Chiba" if location == "Honshu)" & mult == 11
				replace location = "Tokyo" if location == "Honshu)" & mult == 12
				replace location = "Kanagawa" if location == "Honshu)" & mult == 13
				replace location = "Niagata" if location == "Honshu)" & mult == 14
				replace location = "Toyama" if location == "Honshu)" & mult == 15
				replace location = "Ishikawa" if location == "Honshu)" & mult == 16
				replace location = "Fukui" if location == "Honshu)" & mult == 17
				replace location = "Yamanashi" if location == "Honshu)" & mult == 18
				replace location = "Nagano" if location == "Honshu)" & mult == 19
				replace location = "Gifu" if location == "Honshu)" & mult == 20
				replace location = "Shizuoka" if location == "Honshu)" & mult == 21
				replace location = "Aichi" if location == "Honshu)" & mult == 22
				replace location = "Mie" if location == "Honshu)" & mult == 23
				replace location = "Shiga" if location == "Honshu)" & mult == 24
				replace location = "Kyoto" if location == "Honshu)" & mult == 25
				replace location = "Osaka" if location == "Honshu)" & mult == 26
				replace location = "Hyogo" if location == "Honshu)" & mult == 27
				replace location = "Nara" if location == "Honshu)" & mult == 28
				replace location = "Wakayama" if location == "Honshu)" & mult == 29
				replace location = "Tottori" if location == "Honshu)" & mult == 30
				replace location = "Shimane" if location == "Honshu)" & mult == 31
				replace location = "Okayama" if location == "Honshu)" & mult == 32
				replace location = "Hiroshima" if location == "Honshu)" & mult == 33
				replace location = "Yamaguchi" if location == "Honshu)" & mult == 34
				drop mult
			expand 34 if location == "HonshuIsl.; Tokyo; Sendai"
				bysort event_id location: gen mult = _n
				replace location = "Aomori" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 1
				replace location = "Iwate" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 2
				replace location = "Miyagi" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 3
				replace location = "Akita" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 4
				replace location = "Yamagata" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 5
				replace location = "Fukushima" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 6
				replace location = "Ibaraki" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 7
				replace location = "Tochigi" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 8
				replace location = "Gunma" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 9
				replace location = "Saitama" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 10
				replace location = "Chiba" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 11
				replace location = "Tokyo" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 12
				replace location = "Kanagawa" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 13
				replace location = "Niagata" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 14
				replace location = "Toyama" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 15
				replace location = "Ishikawa" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 16
				replace location = "Fukui" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 17
				replace location = "Yamanashi" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 18
				replace location = "Nagano" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 19
				replace location = "Gifu" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 20
				replace location = "Shizuoka" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 21
				replace location = "Aichi" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 22
				replace location = "Mie" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 23
				replace location = "Shiga" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 24
				replace location = "Kyoto" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 25
				replace location = "Osaka" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 26
				replace location = "Hyogo" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 27
				replace location = "Nara" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 28
				replace location = "Wakayama" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 29
				replace location = "Tottori" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 30
				replace location = "Shimane" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 31
				replace location = "Okayama" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 32
				replace location = "Hiroshima" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 33
				replace location = "Yamaguchi" if location == "HonshuIsl.; Tokyo; Sendai" & mult == 34
				drop mult
			expand 34 if location == "HonshuIsland)"
				bysort event_id location: gen mult = _n
				replace location = "Aomori" if location == "HonshuIsland)" & mult == 1
				replace location = "Iwate" if location == "HonshuIsland)" & mult == 2
				replace location = "Miyagi" if location == "HonshuIsland)" & mult == 3
				replace location = "Akita" if location == "HonshuIsland)" & mult == 4
				replace location = "Yamagata" if location == "HonshuIsland)" & mult == 5
				replace location = "Fukushima" if location == "HonshuIsland)" & mult == 6
				replace location = "Ibaraki" if location == "HonshuIsland)" & mult == 7
				replace location = "Tochigi" if location == "HonshuIsland)" & mult == 8
				replace location = "Gunma" if location == "HonshuIsland)" & mult == 9
				replace location = "Saitama" if location == "HonshuIsland)" & mult == 10
				replace location = "Chiba" if location == "HonshuIsland)" & mult == 11
				replace location = "Tokyo" if location == "HonshuIsland)" & mult == 12
				replace location = "Kanagawa" if location == "HonshuIsland)" & mult == 13
				replace location = "Niagata" if location == "HonshuIsland)" & mult == 14
				replace location = "Toyama" if location == "HonshuIsland)" & mult == 15
				replace location = "Ishikawa" if location == "HonshuIsland)" & mult == 16
				replace location = "Fukui" if location == "HonshuIsland)" & mult == 17
				replace location = "Yamanashi" if location == "HonshuIsland)" & mult == 18
				replace location = "Nagano" if location == "HonshuIsland)" & mult == 19
				replace location = "Gifu" if location == "HonshuIsland)" & mult == 20
				replace location = "Shizuoka" if location == "HonshuIsland)" & mult == 21
				replace location = "Aichi" if location == "HonshuIsland)" & mult == 22
				replace location = "Mie" if location == "HonshuIsland)" & mult == 23
				replace location = "Shiga" if location == "HonshuIsland)" & mult == 24
				replace location = "Kyoto" if location == "HonshuIsland)" & mult == 25
				replace location = "Osaka" if location == "HonshuIsland)" & mult == 26
				replace location = "Hyogo" if location == "HonshuIsland)" & mult == 27
				replace location = "Nara" if location == "HonshuIsland)" & mult == 28
				replace location = "Wakayama" if location == "HonshuIsland)" & mult == 29
				replace location = "Tottori" if location == "HonshuIsland)" & mult == 30
				replace location = "Shimane" if location == "HonshuIsland)" & mult == 31
				replace location = "Okayama" if location == "HonshuIsland)" & mult == 32
				replace location = "Hiroshima" if location == "HonshuIsland)" & mult == 33
				replace location = "Yamaguchi" if location == "HonshuIsland)" & mult == 34
				drop mult
			replace location = "Hyogo" if location == "Hyigo"
			replace location = "Hyogo" if location == "Hyogo"
			replace location = "Hyogo" if location == "Hyogoefecture"
			replace location = "Ibaraki" if location == "Ibakaki"
			replace location = "Ibaraki" if location == "Ibaraki"
			replace location = "Ibaraki" if location == "Ibarakiefectures"
			replace location = "Fukuoka" if location == "Iizuka"
			replace location = "Ishikawa" if location == "Ishigaki(Ryukyu Isl.)"
			replace location = "Ishikawa" if location == "Ishikawaregion"
			replace location = "Miyagi" if location == "Ishinomaki"
			replace location = "Miyagi" if location == "Ishinomaki-Onagawaarea (Miyagi efecture) " | location == "Ishinomaki-Onagawaarea (Miyagi efecture)"
			replace location = "Ishikawa" if location == "Isikawa"
			replace location = "Iwate" if location == "Iwate"
			replace location = "Shimane" if location == "Izumo(Shimane)"
			replace location = "Niagata" if location == "Izumozakimati"
			replace location = "Niagata" if location == "Joetsu(Sanwamura)"
			replace location = "Kagoshima" if location == "Kagama"
			replace location = "Kagawa" if location == "Kagawa"
			replace location = "Kagoshima" if location == "Kagoshima" | location == "Kagoshina" | location == "KagoshimaCity"
			replace location = "Kagoshima" if location == "Kagoshima(Shibisan"
			replace location = "Kagoshima" if location == "Kagoshimaefecture"
			replace location = "Kagoshima" if location == "Kagoshimaefectures"
			replace location = "Kagoshima" if location == "Kagoshimaefectures (Chugoku and Shikoku regions)"
			replace location = "Shizuoka" if location == "Kakegawa(Kakegawasi)"
			replace location = "Kanagawa" if location == "Kanagawa"
			replace location = "Kanagawa" if location == "Kanagawaefectures"
			replace location = "Kanagawa" if location == "Kanagawaefectures (Tokyo and surrounding)"
			replace location = "Ishikawa" if location == "Kanazawa"
			expand 7 if location == "Kanto"
				bysort event_id location: gen mult = _n
				replace location = "Gunma" if location == "Kanto" & mult == 1
				replace location = "Tochigi" if location == "Kanto" & mult == 2
				replace location = "Ibaraki" if location == "Kanto" & mult == 3
				replace location = "Saitama" if location == "Kanto" & mult == 4
				replace location = "Tokyo" if location == "Kanto" & mult == 5
				replace location = "Chiba" if location == "Kanto" & mult == 6
				replace location = "Kanagawa" if location == "Kanto" & mult == 7
				drop mult
			replace location = "Tokyo" if location == "Kantoarea (Tokyo)"
			expand 3 if location == "Kariwamura; Nagano and Toyama efecture"
				bysort event_id location: gen mult = _n
				replace location = "Niagata" if location == "Kariwamura; Nagano and Toyama efecture" & mult == 1
				replace location = "Nagano" if location == "Kariwamura; Nagano and Toyama efecture" & mult == 2
				replace location = "Toyama" if location == "Kariwamura; Nagano and Toyama efecture" & mult == 3
				drop mult
			replace location = "Miyagi" if location == "Kashimadai(Kasimadaimati)"
			replace location = "Niagata" if location == "Kasiwazakisi"
			replace location = "Kumamoto" if location == "Kemamoto"
			replace location = "Nagasaki" if location == "Kinkai(Kagoshima"
			replace location = "Hyogo" if location == "Kobe"
			replace location = "Kochi" if location == "Kochi"
			replace location = "Kochi" if location == "Kochicounties (Shikoku"
			replace location = "Kochi" if location == "Kochiefecture"
			replace location = "Kochi" if location == "Kochiefectures (Shikoku island)"
			replace location = "Yamanashi" if location == "Kofu(Yamanashi efecture); Kanto-Koshin region; Sugadaira/NaganoTohoku region"
			replace location = "Saitama" if location == "Koshigaya(Saitama efecture)"
			replace location = "Tokyo" if location == "Kozushima(Koodusimamura)"
			replace location = "Kumamoto" if location == "Kumamoto(Minamata) (Kyushu Island)"
			replace location = "Kumamoto" if location == "Kumamoto(Shizuoka efecture)"
			replace location = "Hiroshima" if location == "Kure"
			replace location = "Hiroshima" if location == "Kuredistricts (Kyushu Isl.)"
			replace location = "Miyagi" if location == "Kurihara(Miyagi-Tukidatetyoo)"
			replace location = "Kyoto" if location == "Kyotango(Kyoto)"
			replace location = "Kyoto" if location == "Kyotio"
			replace location = "Kyoto" if location == "Kyoto"
			replace location = "Kyoto" if location == "Kyotoefecture"
			replace location = "Kyoto" if location == "Kyotoefectures"
			expand 8 if inlist(location, "Kyusgu", "Kyushu", "KyushuIsl.", "KyushuIsl.)", "KyushuIsland")
				bysort event_id location: gen mult = _n
				replace location = "Fukuoka" if inlist(location, "Kyusgu", "Kyushu", "KyushuIsl.", "KyushuIsl.)", "KyushuIsland") & mult == 1
				replace location = "Saga" if inlist(location, "Kyusgu", "Kyushu", "KyushuIsl.", "KyushuIsl.)", "KyushuIsland") & mult == 2
				replace location = "Kumamoto" if inlist(location, "Kyusgu", "Kyushu", "KyushuIsl.", "KyushuIsl.)", "KyushuIsland") & mult == 3
				replace location = "Nagasaki" if inlist(location, "Kyusgu", "Kyushu", "KyushuIsl.", "KyushuIsl.)", "KyushuIsland") & mult == 4
				replace location = "Oita" if inlist(location, "Kyusgu", "Kyushu", "KyushuIsl.", "KyushuIsl.)", "KyushuIsland") & mult == 5
				replace location = "Kagoshima" if inlist(location, "Kyusgu", "Kyushu", "KyushuIsl.", "KyushuIsl.)", "KyushuIsland") & mult == 6
				replace location = "Miyazaki" if inlist(location, "Kyusgu", "Kyushu", "KyushuIsl.", "KyushuIsl.)", "KyushuIsland") & mult == 7
				replace location = "Okinawa" if inlist(location, "Kyusgu", "Kyushu", "KyushuIsl.", "KyushuIsl.)", "KyushuIsland") & mult == 8
				drop mult
			expand 4 if location == "Kyushuand Shikoku area"
				bysort event_id location: gen mult = _n
				replace location = "Ehime" if location == "Kyushuand Shikoku area" & mult == 1
				replace location = "Kagawa" if location == "Kyushuand Shikoku area" & mult == 2
				replace location = "Kochi" if location == "Kyushuand Shikoku area" & mult == 3
				replace location = "Tokushima" if location == "Kyushuand Shikoku area" & mult == 4
				drop mult
			replace location = "Shizuoka" if location == "Makinohra(Haibaratyoo) (Honshu)"
			replace location = "Kagoshima" if location == "Makurazaki"
			replace location = "Nagano" if location == "Matsumoto"
			expand 2 if location == "Matsumotoregion + Tokyo"
				bysort event_id location: gen mult = _n
				replace location = "Nagano" if location == "Matsumotoregion + Tokyo" & mult == 1
				replace location = "Tokyo" if location == "Matsumotoregion + Tokyo" & mult == 2
				drop mult
			replace location = "Ehime" if location == "Matsuyama"
			replace location = "Mie" if location == "Mie"
			expand 3 if location == "Mie-Nara-Sigaarea"
				bysort event_id location: gen mult = _n
				replace location = "Mie" if location == "Mie-Nara-Sigaarea" & mult == 1
				replace location = "Nara" if location == "Mie-Nara-Sigaarea" & mult == 2
				replace location = "Shiga" if location == "Mie-Nara-Sigaarea" & mult == 3
				drop mult
			expand 2 if location == "Mieand Gifu"
				bysort event_id location: gen mult = _n
				replace location = "Mie" if location == "Mieand Gifu" & mult == 1
				replace location = "Gifu" if location == "Mieand Gifu" & mult == 2
				drop mult
			replace location = "Kumamoto" if location == "Minamata"
			replace location = "Nagano" if location == "Minowamachi(Nagano)"
			replace location = "Ibaraki" if location == "Mito"
			replace location = "Mie" if location == "Miyagawa(Mie efecture)"
			replace location = "Miyagi" if location == "Miyagi"
			replace location = "Miyagi" if location == "Miyagiefecture (Honshu): Sendai"
			replace location = "Tokyo" if location == "MiyakeIsl."
			replace location = "Tokyo" if location == "MiyakejimaIsl."
			replace location = "Miyazaki" if location == "Miyazaki"
			replace location = "Tottori" if location == "Mizokuchi(Saihakutyoo-Tottori)"
			replace location = "Chiba" if location == "Mobara"
			replace location = "Iwate" if location == "Morioka(Moriokasi-Iwate)"
			replace location = "Kochi" if location == "Muroto(Shikoku Isl.)"
			replace location = "Nagano" if location == "Nagano"
			replace location = "Nagano" if location == "Naganoefectures" | location == "Naganoefecture (Honshu)"
			replace location = "Niagata" if location == "Nagaoka"
			replace location = "Nagasaki" if location == "Nagaokasi"
			replace location = "Nagasaki" if location == "Nagasaki"
			expand 2 if location == "NagasakiKumamoto efectures"
				bysort event_id location: gen mult = _n
				replace location = "Nagasaki" if location == "NagasakiKumamoto efectures" & mult == 1
				replace location = "Kumamoto" if location == "NagasakiKumamoto efectures" & mult == 2
				drop mult
			replace location = "Okinawa" if location == "NagoBay"
			replace location = "Aichi" if location == "Nagoya"
			replace location = "Ishikawa" if location == "Nanao"
			replace location = "Nara" if location == "Nara"
			replace location = "Miyazaki" if location == "Nango"
			replace location = "Hokkaido" if location == "NearKushiro (Hokkaido)"
			replace location = "Niagata" if location == "Niiagata"
			replace location = "Niagata" if location == "Niigata"
			replace location = "Niagata" if location == "Niigata(Eastern Honshu)"
			replace location = "Niagata" if location == "Niigataefecture"
			replace location = "Niagata" if location == "Niigataefecture (Chuetsu - Honshu): Ojiya"
			replace location = "Ehime" if location == "Niihama"
			replace location = "Tokyo" if location == "Niijima(Niizimamura) islands"
			replace location = "Oita" if location == "Oitaefectures (Kyushu region)"
			replace location = "Niagata" if location == "Ojiya(Odiyasi)"
			replace location = "Nagano" if location == "Okaya"
			replace location = "Okayama" if location == "Okayama"
			replace location = "Okayama" if location == "Okayamaefectures Pacific coast"
			replace location = "Okinawa" if location == "Okinawa"
			replace location = "Okinawa" if location == "Okinawa(Kyushu Isl); Sizuoka"
			replace location = "Okinawa" if location == "OkinawaIsl."
			replace location = "Okinawa" if location == "Okinawaand neigbouring isl."
			replace location = "Okinawa" if location == "Okinawaregion"
			replace location = "Hokkaido" if location == "Okkaido"
			replace location = "Hokkaido" if location == "OkushiriIsland (Hokkaido)"
			replace location = "Osaka" if location == "Osaka"
			replace location = "Tokyo" if location == "Oshima"
			replace location = "Iwate" if location == "Oshu(Iwate-Mizusawasi)"
			expand 2 if inlist(location, "RyikyuIslands", "Ryukyuisland chain", "Ryukyuislands", "RyukyuIslands", "Ryukyus")
				bysort event_id location: gen mult = _n
				replace location = "Okinawa" if inlist(location, "RyikyuIslands", "Ryukyuisland chain", "Ryukyuislands", "RyukyuIslands", "Ryukyus") & mult == 1
				replace location = "Kagoshima" if inlist(location, "RyikyuIslands", "Ryukyuisland chain", "Ryukyuislands", "RyukyuIslands", "Ryukyus") & mult == 2
				drop mult
			replace location = "Niagata" if location == "Sado" | location == "Niigataefecture (Yasuda city"
			replace location = "Niagata" if location == "SadoIsland (Niigata)"
			replace location = "Saga" if location == "Saga"
			replace location = "Saga" if location == "Saga(Kyushu Isl.)"
			replace location = "Shizuoka" if location == "Sagara(Sagaratyoo)"
			replace location = "Tottori" if location == "Saihaku(Saihakutyoo-Tottori district)"
			replace location = "Ehime" if location == "Saijo(Ehime efecture)"
			replace location = "Saitama" if location == "Saitama"
			replace location = "Saitama" if location == "Saitamaefectures (Tokyo)"
			replace location = "Niagata" if location == "Sanjo(Sanzyoosi)"
			replace location = "Miyagi" if location == "Sendai(Miyagi) (Eastern Honshu)"
			replace location = "Shiga" if location == "Shiga"
			replace location = "Shiga" if location == "Shigaefectures"
			expand 4 if inlist(location, "ShihokuIsland", "Shikoku")
				bysort event_id location: gen mult = _n
				replace location = "Ehime" if inlist(location, "ShihokuIsland", "Shikoku") & mult == 1
				replace location = "Kagawa" if inlist(location, "ShihokuIsland", "Shikoku") & mult == 2
				replace location = "Kochi" if inlist(location, "ShihokuIsland", "Shikoku") & mult == 3
				replace location = "Tokushima" if inlist(location, "ShihokuIsland", "Shikoku") & mult == 4
				drop mult
			replace location = "Nagasaki" if location == "Shimabara(Kyushu Isl.)"
			replace location = "Nagasaki" if location == "Shimabara(Nagasaki efecture)"
			replace location = "Shizuoka" if location == "Shimada(Simadasi)"
			replace location = "Tokyo" if location == "Shimana"
			replace location = "Niagata" if location == "Shionomata(Tokamachi district)"
			replace location = "Shizuoka" if location == "Shizuoka"
			replace location = "Shizuoka" if location == "Shizuokaefectures"
			replace location = "Shimane" if location == "Simane"
			replace location = "Shizuoka" if location == "Sizuoka"
			replace location = "Hokkaido" if location == "Soobetutyoo(Hokkaido)"
			replace location = "Iwate" if location == "TIwate"
			replace location = "Iwate" if location == "Tarusawa"
			replace location = "Kumamoto" if location == "Tateyamamachi(Toyama)"
			replace location = "Tochigi" if location == "Tochigi"
			replace location = "Tochigi" if location == "Tochigi(Totigi)"
			replace location = "Aomori" if location == "TohokuDistrict (Aomori efecture)"
			replace location = "Tokushima" if location == "Tokushima"
			replace location = "Tokushima" if location == "Tokushimaefecture (Shikoku Isl.)"
			replace location = "Tokyo" if location == "Tokyo"
			expand 2 if (location == "Tokyo;Fujikawaguchiko" | location == "Tokyo;Fujikawaguchiko")
				bysort event_id location: gen mult = _n
				replace location = "Tokyo" if (location == "Tokyo;Fujikawaguchiko" | location == "Tokyo;Fujikawaguchiko") & mult == 1
				replace location = "Yamanashi" if (location == "Tokyo;Fujikawaguchiko" | location == "Tokyo;Fujikawaguchiko") & mult == 2
				drop mult
			replace location = "Tokyo" if location == "Tokyoarea"
			replace location = "Tokyo" if location == "Tokyoregion"
			replace location = "Tottori" if location == "Tottori"
			replace location = "Toyama" if location == "Toyama"
			replace location = "Nagano" if location == "Toyamaand neighbouring Nagano Prefecture (central and western Japan)"
			replace location = "Niagata" if location == "Tsubame(Yosidamati)"
			replace location = "Ibaraki" if location == "Tsukuba(Ibaraki efecture)"
			replace location = "Ishikawa" if location == "Wajima(Isikawa) region (Noto-hanto)"
			replace location = "Wakayama" if location == "Wakayama"
			replace location = "Shizuoka" if location == "Yaizu(Yaidusi)"
			replace location = "Yamagata" if location == "Yamagata"
			replace location = "Yamagata" if location == "Yamagataefecture"
			replace location = "Yamaguchi" if location == "Yamaguchi"
			replace location = "Niagata" if location == "Yamakoshi(Nagaoka)"
			replace location = "Nara" if location == "Yamoto"
			replace location = "Tokyo" if location == "Yokohama"

			save `sub_work', replace
			restore
			drop if iso3 == "JPN"
			append using `sub_work'

			** SAU
			***************************************************************************************
			preserve
			keep if iso3 == "SAU"

			replace location = "Northern Borders" if location == "Arar"
			replace location = "\'Asir" if location == "Assir"
			replace location = "\'Asir" if location == "Asir"
			replace location = "\'Asir" if location == "Bicha"
			replace location = "Makkah" if location == "Djeddahregions"
			replace location = "Jizan" if location == "Jazan"
			replace location = "Makkah" if location == "Jeddah"
			replace location = "Jizan" if location == "Jizane"
			replace location = "Jizan" if location == "Jizaneregion"
			replace location = "Makkah" if location == "LaMecque"
			replace location = "Makkah" if location == "LaMecque region"
			replace location = "Makkah" if location == "Laith"
			replace location = "Makkah" if location == "Mecca"
			replace location = "Makkah" if location == "Mecque"
			replace location = "Madinah" if location == "Medinaregion"
			replace location = "Najran" if location == "Najran"
			replace location = "Makkah" if location == "Qunfuda"
			replace location = "Riyadh" if location == "Ryad"
			expand 4 if location == "Westernregions"
				bysort event_id location: gen mult = _n
				replace location = "Tabuk" if location == "Westernregions" & mult == 1
				replace location = "Madinah" if location == "Westernregions" & mult == 2
				replace location = "Makkah" if location == "Westernregions" & mult == 3
				replace location = "Bahah" if location == "Westernregions" & mult == 4
				drop mult


			save `sub_work', replace
			restore
			drop if iso3 == "SAU"
			append using `sub_work'



			************************************************************************
			** USA
			************************************************************************
			/* People working on subnationals research, 4.5.2016
			** Not in USA or Unclear
			drop if location == "Ontario"
			drop if location == "northernMexico"
			drop if location == "states"
			drop if location == "midwest"
			drop if location == "CentralMidwestern"
			drop if location == "Richmond" & mainlocation == "North Carolina, Delaware, New York, Richmond, Maine, Rhode Island, Connecticut, Massachusetts, Indiana, Michigan, Ohio, Kentucky"
			drop if location == "Columbia" & inlist(mainlocation, "Tennessee, Columbia, Alabama, Georgia", "Texas, Colorado, Mississippi, Columbia", "Louisiana, Mississippi, Alabama, Texas, New York, Pennsylvania, Columbia, Georgia, Maryland, Tennessee, Virginia")
			drop if location == "FromGeorgia to New York"
			drop if location == "Gulfof Mexico"
			drop if location == "Majuro( Marshall Isl.)"
			drop if location == "Mid-West"
			drop if location == "MiddleWest"
			drop if location == "Midwest"
			drop if location == "Mid-Atlanic"
			drop if location == "Mid-Atlantic"
			drop if location == "North"
			drop if location == "North-East"
			drop if location == "NorthEast"
			drop if location == "Northeasr"
			drop if location == "Northeast"
			drop if location == "Northeast)"
			*/
			preserve
			keep if iso3 == "USA"

			replace location = "Alabama" if location == "AL"
			replace location = "Arkansas" if location == "AR"
			replace location = "Texas" if location == "Abilene(San Antonia & Austin areas"
			replace location = "Wisconsin" if location == "Adams" & regexm(mainlocation, "Wisconsin")
			replace location = "Indiana" if location == "Adams" & regexm(mainlocation, "Indiana")
			replace location = "Arkansas" if location == "Akansas"
			replace location = "Alabama" if location == "Alabama"
			replace location = "Alabama" if regexm(location, "Alabama\(")
			replace location = "Florida" if location == "Alachua"
			replace location = "California" if location == "Alamo(central California)"
			replace location = "Alaska" if location == "Alaska"
			replace location = "Osawatomie" if location == "Allen"
			replace location = "New Hampshire" if location == "Alstead"
			replace location = "California" if location == "Amador"
			replace location = "Texas" if location == "Amarillo(Plains"
			replace location = "Northern Mariana Islands" if location == "AnatahanIsl. (Mariannes Ils.)"
			replace location = "Tennessee" if location == "Anderson"
			replace location = "Wisconsin" if location == "Antigo(Wisconsin)"
			replace location = "Kentucky" if location == "Appalachia" | location == "(Appalachia"
			replace location = "Arkansas" if location == "Aransas"
			replace location = "California" if location == "Arcata"
			replace location = "Arizona" if location == "Arizona" | location == "Arizonastates"
			replace location = "Arizona" if location == "Arkanasas" | location == "Aarkansas"
			replace location = "Arkansas" if location == "Arkansas"
			replace location = "Arkansas" if location == "Arkensas"
			replace location = "Colorado" if location == "Arvada(Colorado)"
			replace location = "California" if location == "Atascadero"
			expand 2 if location == "Atlanta(Georgia state); Alabama"
				bysort event_id location: gen mult = _n
				replace location = "Georgia" if location == "Atlanta(Georgia state); Alabama" & mult == 1
				replace location = "Alabama" if location == "Atlanta(Georgia state); Alabama" & mult == 2
				drop mult
			replace location = "Georgia" if location == "Atlanta(Georgia)"
			replace location = "Illinois" if location == "Auglaize"
			replace location = "Texas" if location == "Austin(Texas)"

			replace location = "The Bahamas" if location == "Bahamas"
				replace iso3 = "BHS" if location == "The Bahamas"
			replace location = "Florida" if location == "BalmBeach county"
			replace location = "Maryland" if location == "Baltimore"
			replace location = "Texas" if location == "Bandera"
			replace location = "Illinois" if location == "Barstow"
			replace location = "Oklahoma" if location == "Bartlesvilleand Dewey"
			replace location = "Georgia" if location == "Bartowcounties (SouthEast"
			replace location = "Texas" if location == "Bastrop(Texas)"
			replace location = "Missouri" if location == "Bates"
			replace location = "Louisiana" if location == "BatonRouge (Louisiana)"
			replace location = "Mississippi" if location == "BaySt. Louis (Mississippi)"
			replace location = "Alabama" if location == "BayouLa Batre"
			replace location = "California" if location == "Bayview"
			replace location = "Tennessee" if location == "Benton"
			replace location = "Massachusetts" if location == "BerkshireCounty"
			replace location = "Texas" if location == "Bexar"
			replace location = "Texas" if location == "Bexar(Texas)"
			replace location = "Mississippi" if location == "Biloxi"
			replace location = "Colorado" if location == "BlackForest (Colorado)"
			expand 2 if location == "Blair(Nebraska); Iowa"
				bysort event_id location: gen mult = _n
				replace location = "Nebraska" if location == "Blair(Nebraska); Iowa" & mult == 1
				replace location = "Iowa" if location == "Blair(Nebraska); Iowa" & mult == 2
				drop mult
			replace location = "Virginia" if location == "BlueRidge Mountain Canyon"
			replace location = "Indiana" if location == "Boone"
			replace location = "Washington" if location == "Boston(Washigton)"
			replace location = "Colorado" if location == "Boulder" | location == "Boulder)"
			replace location = "New Jersey" if location == "BoundBrook"
			replace location = "Nebraska" if location == "Boyd"
			replace location = "Minnesota" if location == "Brainerd(Minnesota)"
			replace location = "Vermont" if location == "Brattleboro"
			replace location = "Texas" if location == "Brazoria"
			replace location = "Texas" if location == "Brooks"
			replace location = "Colorado" if location == "Broomfield"
			replace location = "Florida" if location == "Broward"
			replace location = "Texas" if location == "Brownwood"
			replace location = "New York" if location == "Buffalo"
			replace location = "New York" if location == "Buffaloregion"
			replace location = "New Jersey" if location == "Burlington"
			replace location = "Texas" if location == "Burnet"
			replace location = "Nebraska" if location == "Butler"

			replace location = "Connecticut" if location == "CT"
			replace location = "Georgia" if location == "Cairo"
			replace location = "California" if location == "Calaveras"
			replace location = "Texas" if location == "Calhoun"
			replace location = "California" if location == "California" | location == "Californi"
			replace location = "California" if location == "California)"
			replace location = "California" if location == "Californie"
			replace location = "Kentucky" if location == "Calloway"
			replace location = "California" if location == "Cambria"
			replace location = "Texas" if location == "Cameron"
			replace location = "New Jersey" if location == "CapeMay"
			replace location = "North Carolina" if location == "CarolineDu Nord"
			replace location = "Virginia" if location == "Carolinecounty (Virginia)"
			replace location = "North Carolina" if location == "Carolinedu Nord"
			replace location = "South Carolina" if location == "Carolinedu Sud"
			replace location = "Indiana" if location == "Carroll" & mainlocation == "Carroll, Adams, Cass, Howard, Tipton, Wells, Miami, Boone, Jennings, Clay, Tippecanoe counties (Indiana), Lafayette, Fort WAyne, Decatur, Huntington counties (Ohio), Mercer, Auglaize, Van Wert, Darke, Shelby, Logan, Hamilton, RocUSERd, Montezuma, Celina, Miamitown (Illinois)"
			replace location = "Georgia" if location == "Carroll" & mainlocation == "Douglas, Floyd, Carroll, Atlanta (Georgia state); Alabama, North Carolina,Tennessee states"
			replace location = "Kentucky" if location == "Carrollcounties (Kentucky)"
			replace location = "Nevada" if location == "Carson(Nevada)"
			replace location = "Nevada" if location == "CarsonCity (Nevada)"
			replace location = "Indiana" if location == "Cass"
			replace location = "Oklahoma" if location == "Catoosa(Oklahoma)"
			expand 2 if location == "Catwabacounties (North Carolina); Pensylvania"
				bysort event_id location: gen mult = _n
				replace location = "North Carolina" if location == "Catwabacounties (North Carolina); Pensylvania" & mult == 1
				replace location = "Pennsylvania" if location == "Catwabacounties (North Carolina); Pensylvania" & mult == 2
				drop mult
			replace location = "Iowa" if location == "CedarRapids (Iowa states)"
			replace location = "Illinois" if location == "Celina"
			replace location = "Florida" if location == "CentralFlorida and Florida Panhandle"
			replace location = "Georgia" if location == "CentralGeorgia"
			replace location = "Texas" if location == "Chamberts"
			replace location = "North Carolina" if location == "Charlotteregion (North Carolina)"
			replace location = "Tennessee" if location == "Cheatham"
			replace location = "Oregon" if location == "Chehalus(Western Oregon"
			replace location = "Illinois" if location == "Chicago"
			replace location = "Alabama" if location == "Chilton(Alabama state)"
			replace location = "Washington" if location == "Clallam"
			replace location = "California" if location == "Claremont"
			replace location = "Indiana" if location == "Clay"
			replace location = "Arkansas" if location == "ClayFulton"
			replace location = "Florida" if location == "Claycounties (Florida)"
			replace location = "Texas" if location == "Cleburne(Plains"
			replace location = "Ohio" if location == "Cleveland"
			replace location = "Ohio" if location == "Cleveland"
			replace location = "Alabama" if location == "Coden(Alabama)"
			replace location = "Kansas" if location == "Coffeyville"
			replace location = "Nebraska" if location == "Colfax"
			replace location = "Florida" if location == "Colliercounties (Florida)"
			replace location = "Colorado" if location == "Colorado"
			replace location = "Colorado" if location == "Colorado(Denver)"
			replace location = "Colorado" if location == "ColoradoSpings (Colorado)" | location == "ColoradoSings (Colorado)"
			expand 2 if location == "ColoradoSings(Colorado); New Mexico"
				bysort event_id location: gen mult = _n
				replace location = "Colorado" if location == "ColoradoSings(Colorado); New Mexico" & mult == 1
				replace location = "New Mexico" if location == "ColoradoSings(Colorado); New Mexico" & mult == 2
				drop mult
			replace location = "Georgia" if location == "Colquitt)"
			replace location = "Wisconsin" if location == "Columbia" & mainlocation == "Adams, Columbia, Crawford, Dane, Dodge, Grant, Green, Iowa, Juneau, La crosse, Lafayette, Monroe, Richland, Rock, Sauk, Vernon counties (Wisconsin)"
			replace location = "District of Columbia" if location == "Columbiadistrict"
			replace location = "Connecticut" if location == "Connecticut"
			replace location = "Connecticut" if location == "Connecticut;NorthEast"
			replace location = "Illinois" if location == "CookCounty"
			replace location = "Ohio" if location == "Crawford" & mainlocation == "Huron, Erie, Crawford, Lucas, Sandusky counties (Ohio)"
			replace location = "Wisconsin" if location == "Crawford" & mainlocation == "Adams, Columbia, Crawford, Dane, Dodge, Grant, Green, Iowa, Juneau, La crosse, Lafayette, Monroe, Richland, Rock, Sauk, Vernon counties (Wisconsin)"
			replace location = "Oklahoma" if location == "Creek"
			replace location = "New Jersey" if location == "Cumberland"
			replace location = "Nebraska" if location == "Cuming"
			replace location = "California" if location == "Cutten"

			replace location = "Texas" if location == "Dallas"
			replace location = "Texas" if location == "Dallas-FortWorth area (Texas)"
			replace location = "Wisconsin" if location == "Dane"
			replace location = "Illinois" if location == "Darke"
			replace location = "Alabama" if location == "DauphinIsland"
			replace location = "Tennessee" if location == "Davidson"
			replace location = "Florida" if location == "Daytonabeach (Florida)"
			replace location = "Ohio" if location == "Decatur"
			replace location = "Texas" if location == "DelRio (Texas)"
			replace location = "Delaware" if location == "Delaware"
			replace location = "Colorado" if location == "Denver"
			replace location = "Colorado" if location == "Denver(Colorado)"
			replace location = "Colorado" if location == "Denverarea (Colorado)"
			replace location = "Michigan" if location == "Detroit"
			replace location = "Illinois" if location == "DewittCounties (Illinois)"
			replace location = "Nebraska" if location == "Dodge" & mainlocation == "Boyd, Butler, Colfax, Cuming, Dodge, Merrick, Platte, Sarpy, Saunders, Seward and Stanton (Eastern Nebraska)"
			replace location = "Wisconsin" if location == "Dodge" & mainlocation == "Adams, Columbia, Crawford, Dane, Dodge, Grant, Green, Iowa, Juneau, La crosse, Lafayette, Monroe, Richland, Rock, Sauk, Vernon counties (Wisconsin)"
			replace location = "Georgia" if location == "Douglas"
			replace location = "Iowa" if location == "Dunkerton"

			expand 15 if location == "EasternSeabord"
				bysort event_id location: gen mult = _n
				replace location = "Maine" if location == "EasternSeabord" & mult == 1
				replace location = "New Hampshire" if location == "EasternSeabord" & mult == 2
				replace location = "Massachusetts" if location == "EasternSeabord" & mult == 3
				replace location = "Rhode Island" if location == "EasternSeabord" & mult == 4
				replace location = "Connecticut" if location == "EasternSeabord" & mult == 5
				replace location = "New York" if location == "EasternSeabord" & mult == 6
				replace location = "New Jersey" if location == "EasternSeabord" & mult == 7
				replace location = "Pennsylvania" if location == "EasternSeabord" & mult == 8
				replace location = "Delaware" if location == "EasternSeabord" & mult == 9
				replace location = "Maryland" if location == "EasternSeabord" & mult == 10
				replace location = "Virginia" if location == "EasternSeabord" & mult == 11
				replace location = "North Carolina" if location == "EasternSeabord" & mult == 12
				replace location = "South Carolina" if location == "EasternSeabord" & mult == 13
				replace location = "Georgia" if location == "EasternSeabord" & mult == 14
				replace location = "Florida" if location == "EasternSeabord" & mult == 15
				drop mult
			replace location = "Tennessee" if location == "EasternTennessee"
			replace location = "Kentucky" if location == "Easternand central Kentucky"
			replace location = "Texas" if location == "Eastland"
			replace location = "Texas" if location == "ElPaso (Texas)"
			replace location = "Colorado" if location == "ElPaso counties"
			replace location = "Ohio" if location == "Erie" & mainlocation == "Huron, Erie, Crawford, Lucas, Sandusky counties (Ohio)"
			replace location = "New York" if location == "Erie" & mainlocation == "Erie, Niagara, Genesee, Orleans counties counties (Western New York)"
			replace location = "New Mexico" if location == "Espanola(Nouveau Mexique)"
			replace location = "California" if location == "Eureka"

			replace location = "Illinois" if location == "Fairdale"
			replace location = "California" if location == "Fallbrokregion"
			replace location = "North Dakota" if location == "Fargo"
			replace location = "California" if location == "Ferndale"
			replace location = "Florida" if location == "Florida"
			replace location = "Florida" if location == "Florida)"
			replace location = "Florida" if location == "FloridaKeys"
			replace location = "Florida" if location == "FloridaPanhandle" | location == "FloridaPandhandle"
			replace location = "Florida" if location == "Floride"
			replace location = "Georgia" if location == "Floyd"
			replace location = "Colorado" if location == "FortCollins (Northern Colorado)"
			replace location = "Ohio" if location == "FortWAyne"
			replace location = "Texas" if location == "FortWorth"
			replace location = "Texas" if location == "FortWorth Texas"
			replace location = "California" if location == "Fortuna"
			replace location = "California" if location == "Fortunaand Petrolia areas (North California)"
			replace location = "Kentucky" if location == "Franklin" & mainlocation == "Mercer, Jessamine, Franklin, Calloway, Montgomery (Kentucky), Sharp, Clay Fulton, Randolph, Lauwrence (Arkansas), Obion county (Tennessee), Missouri, Indiana, Illinois"
			replace location = "Missouri" if location == "Franklin" & mainlocation == "Franklin, Jefferson, Gasconade counties (Missouri)"
			replace location = "Missouri" if location == "Franklin" & mainlocation == "Franklin, Jefferson counties (Missouri)"
			replace location = "California" if location == "Fresno"

			replace location = "Georgia" if location == "GA"
			replace location = "Florida" if location == "Gainesville"
			replace location = "Texas" if location == "Galveston"
			replace location = "Texas" if location == "Galveston(Texas)"
			replace location = "Missouri" if location == "Gasconadecounties (Missouri)"
			replace location = "New York" if location == "Genesee"
			replace location = "Texas" if location == "Georgetown"
			replace location = "Georgia" if location == "Georgia"
			replace location = "Georgia" if location == "Georgie(Camilla"
			replace location = "Georgia" if location == "Georgie"
			replace location = "Georgia" if location == "Georgoa"
			replace location = "California" if location == "Glendora"
			replace location = "Texas" if location == "Granburry"
			replace location = "North Dakota" if location == "GrandForks"
			replace location = "Minnesota" if location == "GraniteFalls (Minnesota)"
			replace location = "Texas" if location == "GraniteShoals"
			replace location = "Wisconsin" if location == "Grant" & mainlocation == "Adams, Columbia, Crawford, Dane, Dodge, Grant, Green, Iowa, Juneau, La crosse, Lafayette, Monroe, Richland, Rock, Sauk, Vernon counties (Wisconsin)"
			replace location = "Washington" if location == "GraysHarbor"
			replace location = "Wisconsin" if location == "Green" & mainlocation == "Adams, Columbia, Crawford, Dane, Dodge, Grant, Green, Iowa, Juneau, La crosse, Lafayette, Monroe, Richland, Rock, Sauk, Vernon counties (Wisconsin)"
			replace location = "Massachusetts" if location == "Greenfield"
			replace location = "Vermont" if location == "Guilford(Vermont)"
			replace location = "Louisiana" if location == "GulfCoast Louisiana"
			replace location = "Florida" if location == "GulfCounties"
			replace location = "Mississippi" if location == "Gulfport"

			replace location = "Illinois" if location == "Hamilton"
			replace location = "Washington" if location == "Hanford(Washington)"
			replace location = "Kansas" if location == "Hansas"
			replace location = "Texas" if location == "Happy(Texas)"
			replace location = "Texas" if location == "Harris"
			replace location = "Mississippi" if location == "HarrisonCounty"
			replace location = "Hawaii" if location == "Hawai"
			replace location = "Hawaii" if location == "HawaiIsl."
			replace location = "Hawaii" if location == "Hawaii"
			replace location = "Tennessee" if location == "Henry"
			replace location = "Tennessee" if location == "Hickman"
			replace location = "Texas" if location == "Hidalgo"
			replace location = "New Hampshire" if location == "Hinsdale"
			replace location = "Kansas" if location == "Hoisington(Kansas)"
			replace location = "Texas" if location == "Hondo(Texas)"
			replace location = "Tennessee" if location == "Houston" & regexm(mainlocation, "Tennessee")
			replace location = "Texas" if location == "Houston" & regexm(mainlocation, "Texas")
			replace location = "Texas" if location == "Houston(Texas"
			replace location = "Maryland" if location == "Howard" & mainlocation == "Howard, Prince George counties"
			replace location = "Indiana" if location == "Howard" & mainlocation == "Carroll, Adams, Cass, Howard, Tipton, Wells, Miami, Boone, Jennings, Clay, Tippecanoe counties (Indiana), Lafayette, Fort WAyne, Decatur, Huntington counties (Ohio), Mercer, Auglaize, Van Wert, Darke, Shelby, Logan, Hamilton, RocUSERd, Montezuma, Celina, Miamitown (Illinois)"
			replace location = "Iowa" if location == "Hugo"
			replace location = "California" if location == "HumboldtHill"
			replace location = "California" if location == "HuntingtonBeach (California)"
			replace location = "Ohio" if location == "Huntingtoncounties (Ohio)"
			replace location = "Virginia" if location == "Hurley(VA)"
			replace location = "Ohio" if location == "Huron"

			replace location = "Iowa" if location == "IA"
			replace location = "Iowa" if location == "IA"
			replace location = "Iowa" if location == "IA"
			replace location = "Illinois" if location == "IL"
			replace location = "Indiana" if location == "IN"
			replace location = "Idaho" if location == "Iadaho"
			replace location = "Idaho" if location == "Idaho"
			replace location = "Ohio" if location == "Ihio"
			replace location = "Illinois" if location == "Illinois" | location == "Ilinois"
			replace location = "Illinois" if location == "IllinoisStates"
			replace location = "Illinois" if location == "Illlinois"
			replace location = "Indiana" if location == "Indiana"
			replace location = "Indiana" if location == "Indianaa"
			replace location = "Indiana" if location == "Indiania"
			replace location = "Wisconsin" if location == "Iowa"
			replace location = "Iowa" if location == "Iowastate" | location == "(Iowastate)"

			replace location = "Tennessee" if location == "Jackson" & regexm(mainlocation, "Tennessee")
			replace location = "Wisconsin" if location == "Jackson" & regexm(mainlocation, "Wisconsin")
			replace location = "Nebraska" if location == "Jaksontown (Nebraska)"
			replace location = "Colorado" if location == "Jefferson" & mainlocation == "Park, Jefferson, Larimer counties (Colorado)"
			replace location = "Missouri" if location == "Jefferson" & mainlocation == "Franklin, Jefferson, Gasconade counties (Missouri)"
			replace location = "Alabama" if location == "Jefferson" & mainlocation == "Jefferson, Chilton (Alabama state)"
			replace location = "Washington" if location == "Jefferson" & mainlocation == "Snohomish, Mason, Grays Harbor, Jefferson, Clallam, Skagit and Whatcom (Washingotn states)"
			replace location = "Texas" if location == "Jefferson(Texas)"
			replace location = "Missouri" if location == "Jeffersoncounties (Missouri)"
			replace location = "Indiana" if location == "Jennings"
			replace location = "Kentucky" if location == "Jessamine"
			replace location = "Texas" if location == "JimWells"
			replace location = "Missouri" if location == "Joplin(Missouri state)"
			replace location = "Wisconsin" if location == "Juneau"

			replace location = "Kansas" if location == "KS"
			replace location = "Kentucky" if location == "KY"
			replace location = "Kansas" if location == "Kansas"
			replace location = "Hawaii" if location == "Kauai"
			replace location = "Wisconsin" if location == "Kaukana"
			replace location = "New Hampshire" if location == "Keene"
			replace location = "Texas" if location == "Kenedy"
			replace location = "Texas" if location == "Kennedycounty (Texas)"
			replace location = "Kansas" if location == "Kensas"
			replace location = "Delaware" if location == "Kent"
			replace location = "Kentucky" if location == "Kentuchy"
			replace location = "Kentucky" if location == "Kentuckky(Midwest)"
			replace location = "Kentucky" if location == "Kentuckystates"
			replace location = "California" if location == "Kern"
			replace location = "Oregon" if location == "KlamathFalls (Oregon)"
			replace location = "Texas" if location == "Kleberg"
			replace location = "Tennessee" if location == "Knox"
			replace location = "Texas" if location == "Kress(Texas)"

			replace location = "Louisianna" if location == "LA"
			replace location = "Illinois" if location == "LOgan"
			replace location = "California" if location == "LaConchita"
			replace location = "California" if location == "LaVerne communes (Los Angeles)"
			replace location = "Kansas" if location == "Labette"
			replace location = "California" if location == "LacTahoe region"
			replace location = "Wisconsin" if location == "Lacrosse"
			replace location = "Wisconsin" if location == "Ladysmith(Wisconsin)"
			replace location = "Ohio" if location == "Lafayette" & mainlocation == "Carroll, Adams, Cass, Howard, Tipton, Wells, Miami, Boone, Jennings, Clay, Tippecanoe counties (Indiana), Lafayette, Fort WAyne, Decatur, Huntington counties (Ohio), Mercer, Auglaize, Van Wert, Darke, Shelby, Logan, Hamilton, RocUSERd, Montezuma, Celina, Miamitown (Illinois)"
			replace location = "Colorado" if location == "Lafayette" & mainlocation == "Boulder, Larimer, El Paso counties, Rocky Mountain Front Range and foothills, Lyons, Longmont, Lafayette, Broomfield, Colorado Springs(Colorado); New Mexico"
			replace location = "Wisconsin" if location == "Lafayette" & mainlocation == "Adams, Columbia, Crawford, Dane, Dodge, Grant, Green, Iowa, Juneau, La crosse, Lafayette, Monroe, Richland, Rock, Sauk, Vernon counties (Wisconsin)"
			replace location = "Florida" if location == "Lafayette" & mainlocation == "Gainesville, Alachua, Lafayette, Gulf Counties"
			replace location = "California" if location == "LagunaBeach"
			replace location = "Tennessee" if location == "Lake" & regexm(mainlocation, "Tennessee")
			replace location = "Florida" if location == "Lake" & regexm(mainlocation, "Florida")
			replace location = "Texas" if location == "LakeGranbury area"
			replace location = "Ohio" if location == "Lakecounty (Ohio)"
			replace location = "Colorado" if location == "Lakewood"
			replace location = "Texas" if location == "Lampasas"
			replace location = "California" if location == "LandersCalifornia"
			replace location = "Colorado" if location == "Larimer"
			replace location = "Colorado" if location == "Larimercounties (Colorado)"
			replace location = "Nevada" if location == "LasVegas"
			replace location = "Arkansas" if location == "Lauwrence(Arkansas)"
			replace location = "Texas" if location == "Libertycity (Texas)"
			replace location = "Oklahoma" if location == "Lincoln(Oklahoma)"
			replace location = "Oregon" if location == "Lincolncounties (Oregon coast)"
			replace location = "New Mexico" if location == "Lincolncounty (New Mexico)"
			replace location = "New Jersey" if location == "LittleFalls"
			replace location = "Michigan" if location == "Livingstoncounties"
			replace location = "New Jersey" if location == "LodiBergen County (New Jersey) Tolland"
			replace location = "Illinois" if location == "Logan"
			replace location = "Colorado" if location == "Longmont"
			replace location = "New Mexico" if location == "LosAlamos"
			replace location = "California" if location == "LosAngeles"
			replace location = "California" if location == "LosAngeles (California)"
			replace location = "California" if location == "LosAngeles County (California)"
			replace location = "California" if location == "LosAngeles area (California)"
			replace location = "Louisiana" if location == "Louisiana" | location == "Louisianna"
			replace location = "Louisiana" if location == "Louisiana(New orleans)"
			replace location = "Louisiana" if location == "Louisiana)"
			replace location = "Louisiana" if location == "Louisianastates"
			replace location = "Louisiana" if location == "Louisiane"
			replace location = "Louisiana" if location == "Lousiana"
			replace location = "Oklahoma" if location == "Love"
			replace location = "Ohio" if location == "Lucas" & mainlocation == "Huron, Erie, Crawford, Lucas, Sandusky counties (Ohio)"
			replace location = "Virginia" if location == "Lynchburg(Chesterfield County"
			replace location = "Colorado" if location == "Lyons"

			replace location = "Massachusetts" if location == "MA"
			replace location = "Maryland" if location == "MD"
			replace location = "Michigan" if location == "MI"
			replace location = "Minnesota" if location == "MN"
			replace location = "Missouri" if location == "MO"
			replace location = "Missouri" if location == "MOIllinois"
			replace location = "Mississippi" if location == "MS"
			replace location = "Michigan" if location == "Macomb"
			replace location = "Maine" if location == "Maine"
			replace location = "Maine" if location == "Mainestate"
			replace location = "California" if location == "Malibu"
			replace location = "California" if location == "Malibu(California)"
			replace location = "Hawaii" if location == "Manoavalley"
			replace location = "California" if location == "MareIsland"
			replace location = "California" if location == "Marin"
			replace location = "California" if location == "Marincounty"
			replace location = "Florida" if location == "Marion"
			replace location = "Wisconsin" if location == "Marqutte"
			replace location = "Florida" if location == "Martin"
			replace location = "Maryland" if location == "Maryland"
			replace location = "Washington" if location == "Mason" & mainlocation == "Snohomish, Mason, Grays Harbor, Jefferson, Clallam, Skagit and Whatcom (Washingotn states)"
			replace location = "Illinois" if location == "Mason" & mainlocation == "Mason, Tazewell, LOgan, Will, Dewitt Counties (Illinois)"
			replace location = "Massachusetts" if location == "Massachusets"
			replace location = "Massachusetts" if location == "Massachusett"
			replace location = "Massachusetts" if location == "Massachusetts"
			replace location = "Massachusetts" if location == "Massaschsetts"
			replace location = "Oklahoma" if location == "Maud"
			replace location = "Hawaii" if location == "Mauiislands"
			replace location = "California" if location == "Mendocino"
			replace location = "California" if location == "Merced"
			replace location = "Illinois" if location == "Mercer" & mainlocation == "Carroll, Adams, Cass, Howard, Tipton, Wells, Miami, Boone, Jennings, Clay, Tippecanoe counties (Indiana), Lafayette, Fort WAyne, Decatur, Huntington counties (Ohio), Mercer, Auglaize, Van Wert, Darke, Shelby, Logan, Hamilton, RocUSERd, Montezuma, Celina, Miamitown (Illinois)"
			replace location = "Kentucky" if location == "Mercer" & mainlocation == "Mercer, Jessamine, Franklin, Calloway, Montgomery (Kentucky), Sharp, Clay Fulton, Randolph, Lauwrence (Arkansas), Obion county (Tennessee), Missouri, Indiana, Illinois"
			replace location = "Nebraska" if location == "Merrick"
			replace location = "Oklahoma" if location == "Miami" & regexm(mainlocation, "Oklahoma")
			replace location = "Indiana" if location == "Miami" & regexm(mainlocation, "Indiana")
			replace location = "Florida" if location == "Miami-Dade"
			replace location = "Texas" if location == "Miamiand Commerce"
			replace location = "Illinois" if location == "Miamitown(Illinois)"
			replace location = "Michigan" if location == "Michigan"
			replace location = "Michigan" if location == "Mid-Atlantic;Michigan"
			replace location = "Maryland" if location == "Midwest;Ohio Valley; New England; Northeast; Maryland"
			replace location = "Wisconsin" if location == "Milwaukee"
			replace location = "Minnesota" if location == "Minesota"
			replace location = "Minnesota" if location == "Minissotta"
			replace location = "Minnesota" if location == "Minnesota"
			expand 2 if location == "Minnesota;Alabama; Midwest"
				bysort event_id location: gen mult = _n
				replace location = "Minnesota" if location == "Minnesota;Alabama; Midwest" & mult == 1
				replace location = "Alabama" if location == "Minnesota;Alabama; Midwest" & mult == 2
				drop mult
			replace location = "Minnesota" if location == "Minnesotastates"
			replace location = "Minnesota" if location == "Minnessota"
			replace location = "Mississippi" if location == "MississIppi"
			replace location = "Mississippi" if location == "Mississipi"
			replace location = "Mississippi" if location == "Mississippi"
			replace location = "Mississippi" if location == "Mississippi)"
			replace location = "Mississippi" if location == "Mississippistate"
			replace location = "Mississippi" if location == "Mississippistates"
			replace location = "Mississippi" if location == "Mississippivalley"
			replace location = "Mississippi" if location == "Mississppi"
			replace location = "Missouri" if location == "Missouri"
			replace location = "Missouri" if location == "Missouri(Midwest)"
			replace location = "Missouri" if location == "Missourri"
			replace location = "Alabama" if location == "Mobile"
			replace location = "Modesto" if location == "Modesto(California)" | location == "Stanislaus"
			replace location = "New Jersey" if location == "Monmouth"
			replace location = "Wisconsin" if location == "Monroe" & mainlocation == "Adams, Columbia, Crawford, Dane, Dodge, Grant, Green, Iowa, Juneau, La crosse, Lafayette, Monroe, Richland, Rock, Sauk, Vernon counties (Wisconsin)"
			replace location = "Wisconsin" if location == "Monroe" & mainlocation == "Kaukana, Waupaca, Adams, Jackson, Juneau, Marqutte, Milwaukee, Monroe, POrtage, Sauk, Waushara counties (Wisconsin)"
			replace location = "Florida" if location == "Monroe" & mainlocation == "Miami-Dade, Monroe, Broward, Collier counties (Florida)"
			replace location = "Arizona" if location == "MontLemmon"
			replace location = "Montana" if location == "Montana"
			replace location = "Montana" if location == "Montana(Rockies)"
			replace location = "Montana" if location == "Montania"
			replace location = "California" if location == "Montereycounties (California)"
			replace location = "Illinois" if location == "Montezuma"
			replace location = "Kansas" if location == "Montgomery"
			replace location = "Kentucky" if location == "Montgomery(Kentucky)"
			expand 2 if location == "Moore(Oklahoma state); Missouri"
				bysort event_id location: gen mult = _n
				replace location = "Oklahoma" if location == "Moore(Oklahoma state); Missouri" & mult == 1
				replace location = "Missouri" if location == "Moore(Oklahoma state); Missouri" & mult == 2
				drop mult
			replace location = "New Jersey" if location == "Morris(Sussex county"
			replace location = "California" if location == "MorroBay"
			replace location = "California" if location == "Myrtletown"

			replace location = "North Carolina" if location == "NC"
			replace location = "New Hampshire" if location == "NH"
			replace location = "Louisiana" if location == "NO"
			replace location = "New York" if location == "NY"
			replace location = "Georgia" if location == "Nand C Georgia"
			replace location = "California" if location == "Napa"
			replace location = "California" if location == "NapaValley"
			replace location = "Florida" if location == "Naples(Collier County"
			replace location = "Arizona" if location == "NarrowCanyon (Arizona)"
			expand 4 if location == "Nashville(Arkansas); Van (Texas); Delmond (South Dakota); Colorado"
				bysort event_id location: gen mult = _n
				replace location = "Arkansas" if location == "Nashville(Arkansas); Van (Texas); Delmond (South Dakota); Colorado" & mult == 1
				replace location = "Texas" if location == "Nashville(Arkansas); Van (Texas); Delmond (South Dakota); Colorado" & mult == 2
				replace location = "South Dakota" if location == "Nashville(Arkansas); Van (Texas); Delmond (South Dakota); Colorado" & mult == 3
				replace location = "Colorado" if location == "Nashville(Arkansas); Van (Texas); Delmond (South Dakota); Colorado" & mult == 4
				drop mult
			replace location = "California" if location == "NearYountville (Napa valley"
			replace location = "Nebraska" if location == "Nebarska"
			replace location = "Nebraska" if location == "Nebraska"
			expand 2 if location == "Nebraskaand Kansas"
				bysort event_id location: gen mult = _n
				replace location = "Nebraska" if location == "Nebraskaand Kansas" & mult == 1
				replace location = "Kansas" if location == "Nebraskaand Kansas" & mult == 2
				drop mult
			replace location = "Kentucky" if location == "Nelson"
			replace location = "Kansas" if location == "Neosho"
			replace location = "Nevada" if location == "Nevada"
			replace location = "New York" if location == "New-York"
			replace location = "Texas" if location == "NewBraunfels"
			expand 6 if location == "NewEngland"
				bysort event_id location: gen mult = _n
				replace location = "Connecticut" if location == "NewEngland" & mult == 1
				replace location = "Maine" if location == "NewEngland" & mult == 2
				replace location = "Massachusetts" if location == "NewEngland" & mult == 3
				replace location = "New Hampshire" if location == "NewEngland" & mult == 4
				replace location = "Rhode Island" if location == "NewEngland" & mult == 5
				replace location = "Vermont" if location == "NewEngland" & mult == 6
				drop mult
			replace location = "Maine" if location == "NewEngland (Maine)"
			replace location = "New Hampshire" if location == "NewHalpshire"
			replace location = "New Hampshire" if location == "NewHampshire"
			replace location = "New Hampshire" if location == "NewHampshire)"
			replace location = "Iowa" if location == "NewHartford"
			replace location = "New Jersey" if location == "NewJersey"
			replace location = "New Jersey" if location == "NewJersey)"
			expand 2 if location == "NewJerseyn New York"
				bysort event_id location: gen mult = _n
				replace location = "New Jersey" if location == "NewJerseyn New York" & mult == 1
				replace location = "New York" if location == "NewJerseyn New York" & mult == 2
				drop mult
			replace location = "New Mexico" if location == "NewMexico"
			replace location = "New Mexico" if location == "NewMexico (West and Centre of USA)"
			replace location = "New Mexico" if location == "NewMexique"
			replace location = "Louisiana" if location == "NewOrleans" | location == "New Orleans"
			replace location = "New York" if location == "NewYork"
			replace location = "New York" if location == "NewYork +"
			replace location = "" if location == "NewYork states; Midwest"
			expand 6 if location == "NewYork; Boston (Massachusset); North Carolina; Philadelphia; Atlanta; Delaware"
				bysort event_id location: gen mult = _n
				replace location = "New York" if location == "NewYork; Boston (Massachusset); North Carolina; Philadelphia; Atlanta; Delaware" & mult == 1
				replace location = "Massachusetts" if location == "NewYork; Boston (Massachusset); North Carolina; Philadelphia; Atlanta; Delaware" & mult == 2
				replace location = "North Carolina" if location == "NewYork; Boston (Massachusset); North Carolina; Philadelphia; Atlanta; Delaware" & mult == 3
				replace location = "Pennsylvania" if location == "NewYork; Boston (Massachusset); North Carolina; Philadelphia; Atlanta; Delaware" & mult == 4
				replace location = "Georgia" if location == "NewYork; Boston (Massachusset); North Carolina; Philadelphia; Atlanta; Delaware" & mult == 5
				replace location = "Delaware" if location == "NewYork; Boston (Massachusset); North Carolina; Philadelphia; Atlanta; Delaware" & mult == 6
				drop mult
			replace location = "New York" if location == "Newyork"
			replace location = "New York" if location == "Niagara"
			replace location = "Hawaii" if location == "Nihau"
			expand 2 if location == "North+ South Carolina" | location == "North+ South Dakota"
				bysort event_id location: gen mult = _n
				replace location = "North Carolina" if location == "North+ South Carolina" | location == "North+ South Dakota" & mult == 1
				replace location = "South Carolina" if location == "North+ South Carolina" | location == "North+ South Dakota" & mult == 2
				drop mult
			replace location = "North Carolina" if location == "NorthCarolina"
			replace location = "North Carolina" if location == "NorthCarolina (South Est regions)"
			replace location = "North Carolina" if location == "NorthCarolina states"
			replace location = "North Carolina" if location == "NorthCaroline"
			replace location = "Idaho" if location == "NorthCentral Idaho"
			replace location = "North Carolina" if location == "NorthCraolina"
			replace location = "North Dakota" if location == "NorthDAkota states"
			replace location = "North Dakota" if location == "NorthDakota"
			replace location = "North Dakota" if location == "NorthDakota states"
			replace location = "Texas" if location == "NorthTexas"
			replace location = "California" if location == "Northand Center California"
			expand 2 if location == "Northand South Carolina"
				bysort event_id location: gen mult = _n
				replace location = "North Carolina" if location == "Northand South Carolina" & mult == 1
				replace location = "South Carolina" if location == "Northand South Carolina" & mult == 2
				drop mult
			replace location = "Massachusetts" if location == "Northbridge"
			replace location = "North Carolina" if location == "Northcarolina"
			replace location = "Illinois" if location == "Northeast;Chicago"
			replace location = "Michigan" if location == "Northeast;Michigan"
			replace location = "California" if location == "NorthernCalifornia; Napa"
			replace location = "North Carolina" if location == "NorthernCleveland"
			replace location = "Florida" if location == "NorthernFlorida"
			replace location = "Hawaii" if location == "NorthernKauai (Hawai)"
			replace location = "Mississippi" if location == "NorthernMississippi"
			replace location = "North Carolina" if location == "NoryhCarolina"
			replace location = "New Mexico" if location == "Nouveau-Mexique"
			replace location = "Texas" if location == "Nueces"

			replace location = "Ohio" if location == "OH"
			replace location = "Hawaii" if location == "Oahu"
			replace location = "Hawaii" if location == "Oahu(Hawai)"
			replace location = "California" if location == "Oakland" & mainlocation == "Oakland"
			replace location = "New Jersey" if location == "Oakland" & mainlocation == "Keene, Alstead, Stoddard, Unity, Hinsdale, Walpole (Cheshire County, New Hampshire), Brattleboro, Guilford (Vermont), Berkshire County, Pittsfield, Greenfield, Northbridge, Worcester and Southbridge (Massachusetts), Spring Lake, Wayne, Little Falls, Oakland, Pompton Lakes, Westwood, Bound Brook, Lodi Bergen County (New Jersey) Tolland, eastern Windham, western Hartford Counties (Connecticut)"
			replace location = "Michigan" if location == "Oakland" & mainlocation == "Wayne, Oakland, Macomb, Livingston counties"
			replace location = "Tennessee" if location == "Obion"
			replace location = "Tennessee" if location == "Obioncounty (Tennessee)"
			expand 2 if location == "Oceancounties (New Jersey state); Virginia"
				bysort event_id location: gen mult = _n
				replace location = "New Jersey" if location == "Oceancounties (New Jersey state); Virginia" & mult == 1
				replace location = "Virginia" if location == "Oceancounties (New Jersey state); Virginia" & mult == 2
				drop mult
			replace location = "California" if location == "Oceano"
			replace location = "Ohio" if location == "Ohio"
			replace location = "Ohio" if location == "OhioValley"
			replace location = "Ohio" if location == "Ohiostate)"
			replace location = "Oklahoma" if location == "Ohklahoma"
			replace location = "Oklahoma" if location == "Okhahoma"
			replace location = "Oklahoma" if location == "Okhlahoma"
			replace location = "Oklahoma" if location == "Oklahoma"
			replace location = "Oklahoma" if location == "OklahomaCity" | location == "Oklahomacity"
			replace location = "Oklahoma" if location == "Oklahomastate"
			replace location = "Oklahoma" if location == "Oklahomastates"
			replace location = "Washington" if location == "Olympia(Washington state)"
			replace location = "California" if location == "Orange"
			replace location = "California" if location == "Orange(California)"
			replace location = "Oregon" if location == "Orego"
			replace location = "Oregon" if location == "Oregon"
			replace location = "Oregon" if location == "Oregon(West)"
			replace location = "Oregon" if location == "Oregonstates"
			replace location = "California" if location == "Organge"
			replace location = "California" if location == "Organgecounty"
			replace location = "New York" if location == "Orleanscounties counties (Western New York)"
			replace location = "Washington" if location == "Orting"
			replace location = "Washington" if location == "Orting"
			replace location = "Oklahoma" if location == "Osawatomie"
			replace location = "Illinois" if location == "Osborn(Illinois)"
			replace location = "Florida" if location == "Osceola"
			replace location = "Washington" if location == "Oso(near Seattle)"
			replace location = "Kentucky" if location == "Owensboro(Kentucky)"

			replace location = "Pennsylvania" if location == "PA"
			replace location = "Wisconsin" if location == "POrtage"
			expand 2 if location == "PacificNorthwest"
				bysort event_id location: gen mult = _n
				replace location = "Oregon" if location == "PacificNorthwest" & mult == 1
				replace location = "Washington" if location == "PacificNorthwest" & mult == 2
				drop mult
			replace location = "California" if location == "PalmSings (California)"
			replace location = "Colorado" if location == "Park"
			replace location = "Texas" if location == "Parker.Marble Falls area"
			replace location = "Iowa" if location == "Parkesburg"
			replace location = "Florida" if location == "Pasadena"
			replace location = "California" if location == "Pasadenaarea (California)"
			replace location = "Mississippi" if location == "Pascagoula"
			replace location = "California" if location == "PasoRobles"
			replace location = "Oklahoma" if location == "Paynecounties (Oklahoma)"
			replace location = "North Dakota" if location == "Pembina"
			replace location = "Pennsylvania" if location == "Pennsylvania"
			replace location = "Pennsylvania" if location == "Pennsylvanie"
			replace location = "Pennsylvania" if location == "Pennsylvannia"
			replace location = "Pennsylvania" if location == "Pennsyvania"
			replace location = "Pennsylvania" if location == "Pennyslvania"
			replace location = "Pennsylvania" if location == "Pensylvania"
			replace location = "Pennsylvania" if location == "Pensylvanniastates"
			replace location = "Tennessee" if location == "Perry"
			replace location = "Tennessee" if location == "Pickett"
			replace location = "Kentucky" if location == "Pikecounty (Kentucky)"
			replace location = "California" if location == "PineHills"
			replace location = "Massachusetts" if location == "Pittsfield"
			drop if location == "Plains"
			drop if location == "Plains)"
			replace location = "Florida" if location == "PlamBeach counties (Florida state)"
			replace location = "Nebraska" if location == "Platte"
			replace location = "New Jersey" if location == "PomptonLakes"
			replace location = "Oklahoma" if location == "Pottawatomie"
			replace location = "Maryland" if location == "PrinceGeorge counties"
			replace location = "Alaska" if location == "PrinceWilliam Sound area (Alaska)"
			replace location = "Colorado" if location == "Pueblo(Colorado)"

			replace location = "Rhode Island" if location == "RHodeIsland"
			replace location = "Rhode Island" if location == "RI"
			replace location = "Arkansas" if location == "Randolph"
			replace location = "Mississippi" if location == "Rankincounty (Mississippi)"
			replace location = "Texas" if location == "Refugio"
			replace location = "Nevada" if location == "Reno" & mainlocation == "Napa, Sonoma, Mendocino, Marin, Solano (San Francisco region), Los Angeles (California), Reno, Truckee, Carson (Nevada)"
			replace location = "Oklahoma" if location == "Reno" & mainlocation == "Oklahoma city, St Louis, Reno, Amarillo (Plains, midwest, Northeast)"
			replace location = "Rhode Island" if location == "RhodeIs."
			replace location = "Rhode Island" if location == "RhodeIsland"
			replace location = "Rhode Island" if location == "RhodeIslands" | location == "Rhodeislands"
			replace location = "Rhode Island" if location == "RhodesIsland"
			replace location = "Rhode Island" if location == "RhondeIsland"
			replace location = "Wisconsin" if location == "Richland" & mainlocation == "Adams, Columbia, Crawford, Dane, Dodge, Grant, Green, Iowa, Juneau, La crosse, Lafayette, Monroe, Richland, Rock, Sauk, Vernon counties (Wisconsin)"
			replace location = "Virginia" if location == "Richmond" & mainlocation == "Richmond, Lynchburg (Chesterfield County, Virginia state), South Carolina, North Carolina states"
			replace location = "California" if location == "RioDell (Offshore Northern California)"
			replace location = "Texas" if location == "RioGrande"
			replace location = "California" if location == "Riverside"
			replace location = "California" if location == "Riversidecounty"
			replace location = "Illinois" if location == "Rochelle(Illinois)"
			replace location = "Wisconsin" if location == "Rock" & mainlocation == "Adams, Columbia, Crawford, Dane, Dodge, Grant, Green, Iowa, Juneau, La crosse, Lafayette, Monroe, Richland, Rock, Sauk, Vernon counties (Wisconsin)"
			replace location = "Illinois" if location == "RocUSERd"
			drop if location == "Rockies"
			replace location = "Missouri" if location == "Rockville(Missouri)"
			replace location = "Colorado" if location == "RockyMountain Front Range and foothills"

			expand 2 if location == "S& N Carolina"
				bysort event_id location: gen mult = _n
				replace location = "South Carolina" if location == "S& N Carolina" & mult == 1
				replace location = "North Carolina" if location == "S& N Carolina" & mult == 2
				drop mult
			replace location = "Alabama" if location == "SAlabama"
			replace location = "South Carolina" if location == "SC"
			replace location = "South Dakota" if location == "SD"
			replace location = "South Dakota" if location == "SD"
			replace location = "California" if location == "Sacaramento(California)"
			replace location = "California" if location == "Sacrament"
			replace location = "Utah" if location == "SaltLake City (Utah"
			replace location = "Texas" if location == "SanAngelo"
			replace location = "Texas" if location == "SanAntonio"
			replace location = "California" if location == "SanBernardino"
			replace location = "California" if location == "SanBernardino (California)"
			replace location = "California" if location == "SanBernardino Counties (California)"
			replace location = "California" if location == "SanDiego"
			replace location = "California" if location == "SanDiego County (California)" | location == "SanDiego county (California)"
			replace location = "California" if location == "SanDiego( California); Oregon"
			replace location = "California" if location == "SanFrancisco"
			replace location = "California" if location == "SanFrancisco Bay"
			replace location = "California" if location == "SanJoaquin"
			replace location = "California" if location == "SanJoaquin valley (California)"
			replace location = "California" if location == "SanLuis Obispo"
			replace location = "California" if location == "SanMateo"
			replace location = "Texas" if location == "SanPatricio"
			replace location = "Ohio" if location == "Sanduskycounties (Ohio)"
			expand 2 if location == "SanMarcos Municipality (Texas); Claremore (Oklahoma)"
				bysort event_id location: gen mult = _n
				replace location = "Texas" if location == "SanMarcos Municipality (Texas); Claremore (Oklahoma)" & mult == 1
				replace location = "Oklahoma" if location == "SanMarcos Municipality (Texas); Claremore (Oklahoma)" & mult == 2
				drop mult
			replace location = "California" if location == "SantaBarbara"
			replace location = "California" if location == "SantaBarbara (California)"
			replace location = "California" if location == "SantaBarbara counties"
			replace location = "California" if location == "SantaBarbara counties (California)"
			replace location = "California" if location == "SantaCruz (California)"
			replace location = "Florida" if location == "Sarasota(Florida)"
			replace location = "Nebraska" if location == "Sarpy"
			replace location = "Wisconsin" if location == "Sauk"
			replace location = "Nebraska" if location == "Saunders"
			replace location = "Washington" if location == "Seattle"
			replace location = "Florida" if location == "Seminole"
			replace location = "Florida" if location == "Seminolecounties (Florida)"
			replace location = "Nebraska" if location == "Sewardand Stanton (Eastern Nebraska)"
			replace location = "Ohio" if location == "Shadyside(Ohio state)"
			replace location = "Arkansas" if location == "Sharp"
			replace location = "California" if location == "ShastaCounty (California)"
			replace location = "California" if location == "ShastaLake"
			replace location = "Oklahoma" if location == "Shawnee"
			replace location = "Illinois" if location == "Shelby"
			replace location = "California" if location == "SierraNevada (California)"
			replace location = "New Jersey" if location == "SingLake"
			replace location = "Wisconsin" if location == "Sirentown (Northwestern Wisconsin)"
			replace location = "Washington" if location == "Skagitand Whatcom (Washingotn states)"
			replace location = "Louisiana" if location == "Slidell"
			replace location = "Washington" if location == "Snohomish"
			replace location = "California" if location == "Solano(San Francisco region)"
			replace location = "California" if location == "Solanocounties"
			replace location = "California" if location == "Sonoma"
			drop if location == "South-Westregions"
			replace location = "California" if location == "SouthBay"
			replace location = "California" if location == "SouthCalifornia"
			replace location = "South Carolina" if location == "SouthCarolina"
			replace location = "South Carolina" if location == "SouthCarolina (Midwest"
			replace location = "South Carolina" if location == "SouthCarolina states"
			replace location = "South Dakota" if location == "SouthDakota"
			replace location = "" if location == "SouthEast"
			replace location = "" if location == "SouthEast"
			replace location = "" if location == "SouthEast"
			replace location = "Texas" if location == "SouthEast)"
			replace location = "Ohio" if location == "SouthOhio"
			expand 2 if location == "Southand NOrth Carolina"
				bysort event_id location: gen mult = _n
				replace location = "South Carolina" if location == "Southand NOrth Carolina" & mult == 1
				replace location = "North Carolina" if location == "Southand NOrth Carolina" & mult == 2
				drop mult
			replace location = "South Carolina" if location == "Southcarolina"
			drop if location == "Southeast"
			replace location = "Arizona" if location == "SouthernArizona"
			replace location = "California" if location == "SouthernCalifornia"
			replace location = "California" if location == "SouthernCalifornia (San Diego)"
			replace location = "Kansas" if location == "SouthernKansas"
			replace location = "Mississippi" if location == "SouthernMississippi"
			drop if location == "SouthernPlains to New England"
			replace location = "Arkansas" if location == "Southernand eastern Arkansas"
			replace location = "Georgia" if location == "SouthwestGeorgia"
			replace location = "Louisiana" if location == "St.Bernard Parish (Lousiana)"
			replace location = "Oklahoma" if location == "StLouis"
			replace location = "California" if location == "Stanislau(Northern California)"
			replace location = "Texas" if location == "Starr"
			replace location = "Tennessee" if location == "Stewart(Tennessee)"
			replace location = "New Hampshire" if location == "Stoddard"
			replace location = "Virginia" if location == "Suffolk(Virginia)"
			replace location = "Arizona" if location == "Summerhaven"
			replace location = "Tennessee" if location == "Sumner"
			replace location = "Florida" if location == "Sumter"
			expand 2 if location == "Sussexcounties (Delaware state); Atlantic"
				bysort event_id location: gen mult = _n
				replace location = "Delaware" if location == "Sussexcounties (Delaware state); Atlantic" & mult == 1
				replace location = "New Jersey" if location == "Sussexcounties (Delaware state); Atlantic" & mult == 2
				drop mult

			replace location = "Texas" if location == "TExas"
			replace location = "Tennessee" if location == "TN"
			replace location = "Texas" if location == "TX"
			replace location = "Washington" if location == "Tacoma"
			replace location = "California" if location == "Tahoe(California)"
			replace location = "Tennessee" if location == "Tannesse"
			replace location = "Texas" if location == "Tarrant"
			replace location = "Illinois" if location == "Tazewell"
			replace location = "Oklahoma" if location == "Tecumseh"
			replace location = "California" if location == "Templeton"
			replace location = "Tennessee" if location == "TenNessee"
			replace location = "Tennessee" if location == "Tenneessee"
			replace location = "Tennessee" if location == "TenneesseeValley"
			replace location = "Tennessee" if location == "Tennesse"
			replace location = "Tennessee" if location == "Tennessee"
			replace location = "Tennessee" if location == "TennesseeValley"
			replace location = "Tennessee" if location == "Tennesseestate"
			replace location = "Tennessee" if location == "Tennesseestates"
			replace location = "Texas" if location == "Texas"
			replace location = "Texas" if location == "Texas)"
			replace location = "Texas" if location == "TexasPanhandle (Hutchinson County)"
			replace location = "Texas" if location == "Tilden"
			replace location = "Oregon" if location == "Tillamook"
			replace location = "Indiana" if location == "Tippecanoecounties (Indiana)"
			replace location = "Indiana" if location == "Tipton"
			replace location = "Nevada" if location == "Truckee"
			replace location = "Oklahoma" if location == "Tulsa"
			expand 2 if location == "Tumarecounties (California state); Utah"
				bysort event_id location: gen mult = _n
				replace location = "California" if location == "Tumarecounties (California state); Utah" & mult == 1
				replace location = "Utah" if location == "Tumarecounties (California state); Utah" & mult == 2
				drop mult
			replace location = "Alabama" if location == "Tuscaloosa(Alabama region)"

			replace location = "New Hampshire" if location == "Unity"
			replace location = "Utah" if location == "Utah"
			replace location = "Texas" if location == "Utopia"

			replace location = "Virginia" if location == "VA"
			replace location = "Vermont" if location == "VT"
			replace location = "California" if location == "Vallejo"
			replace location = "Illinois" if location == "VanWert"
			replace location = "California" if location == "Ventura"
			replace location = "California" if location == "Ventura(California)"
			replace location = "California" if location == "Venturacounty"
			replace location = "California" if location == "Venturacounty (California)"
			replace location = "Vermont" if location == "Vermont"
			replace location = "Missouri" if location == "Vernon.Papinville"
			replace location = "Wisconsin" if location == "Vernoncounties (Wisconsin)"
			replace location = "Mississippi" if location == "Vicksburg(Warren county"
			replace location = "Texas" if location == "Victoria"
			replace location = "Virginia" if location == "Virgina"
			replace location = "Virginia" if location == "Virginia"
			replace location = "Virginia" if location == "Virginiastate)"
			replace location = "Virginia" if location == "Virginiastates"
			replace location = "Virginia" if location == "Virginie"
			replace location = "Virginia" if location == "Virnginia"
			replace location = "Florida" if location == "Volusiacounties"
			replace location = "Florida" if location == "Volusioa"

			replace location = "Washington" if location == "WA"
			replace location = "Wisconsin" if location == "WI"
			replace location = "West Virginia" if location == "WV"
			replace location = "Wyoming" if location == "WY"
			replace location = "Wisconsin" if location == "Waisconsin"
			replace location = "New Hampshire" if location == "Walpole(Cheshire County"
			replace location = "North Dakota" if location == "Walshcounties (North Dakota)"
			replace location = "Tennessee" if location == "Warrencounties (Tennessee)"
			replace location = "Missouri" if location == "Warrensburg(Missouri)"
			replace location = "Washington" if location == "Washington"
			replace location = "District of Columbia" if location == "WashingtonDC"
			replace location = "District of Columbia" if location == "WashingtonDC"
			replace location = "District of Columbia" if location == "WashingtonDC"
			replace location = "District of Columbia" if location == "WashingtonDC"
			replace location = "District of Columbia" if location == "WashingtonDC"
			replace location = "District of Columbia" if location == "WashingtonDC"
			replace location = "District of Columbia" if location == "WashingtonDC"
			replace location = "District of Columbia" if location == "WashingtonDC"
			replace location = "District of Columbia" if location == "WashingtonDC area"
			expand 2 if location == "WashingtonDC; Boston"
				bysort event_id location: gen mult = _n
				replace location = "District of Columbia" if location == "WashingtonDC; Boston" & mult == 1
				replace location = "Massachusetts" if location == "WashingtonDC; Boston" & mult == 2
				drop mult
			replace location = "Washington" if location == "Washingtonstate)"
			replace location = "Washington" if location == "Washingtonstates" | location == "Washingtons"
			replace location = "Wisconsin" if location == "Waupaca"
			replace location = "Oklahoma" if location == "Waurika"
			replace location = "Wisconsin" if location == "Wausharacounties (Wisconsin)"
			replace location = "Mississippi" if location == "Waveland"
			replace location = "New Jersey" if location == "Wayne" & mainlocation == "Keene, Alstead, Stoddard, Unity, Hinsdale, Walpole (Cheshire County, New Hampshire), Brattleboro, Guilford (Vermont), Berkshire County, Pittsfield, Greenfield, Northbridge, Worcester and Southbridge (Massachusetts), Spring Lake, Wayne, Little Falls, Oakland, Pompton Lakes, Westwood, Bound Brook, Lodi Bergen County (New Jersey) Tolland, eastern Windham, western Hartford Counties (Connecticut)"
			replace location = "Michigan" if location == "Wayne" & mainlocation == "Wayne, Oakland, Macomb, Livingston counties"
			replace location = "Illinois" if location == "Waynecounty (Illinois)"
			replace location = "Indiana" if location == "Wells"
			replace location = "Nevada" if location == "Wells- Nevada"
			replace location = "Texas" if location == "WestTexas"
			replace location = "West Virginia" if location == "WestVirgina"
			replace location = "West Virginia" if location == "WestVirginia"
			replace location = "Kansas" if location == "WesternKansas"
			replace location = "Louisiana" if location == "WesternLouisiana"
			replace location = "Pennsylvania" if location == "WesternPennsylvania"
			replace location = "West Virginia" if location == "Westvirginia"
			replace location = "New Jersey" if location == "Westwood"
			replace location = "Colorado" if location == "WheatRidge"
			replace location = "New Mexico" if location == "WhiteRock"
			replace location = "Texas" if location == "WichitaFalls (Texas)"
			replace location = "Illinois" if location == "Will"
			replace location = "Texas" if location == "Willacycounties (Texas state)"
			replace location = "Texas" if location == "Williamson"
			replace location = "Kansas" if location == "Wilsonand Woodson (Kansas)"
			replace location = "Wisconsin" if location == "Wisconsin"
			expand 2 if location == "Wisconsinand Illinois"
				bysort event_id location: gen mult = _n
				replace location = "Wisconsin" if location == "Wisconsinand Illinois" & mult == 1
				replace location = "Illinois" if location == "Wisconsinand Illinois" & mult == 2
				drop mult
			replace location = "Wisconsin" if location == "Wisconsinstates"
			replace location = "Massachusetts" if location == "Worcesterand Southbridge (Massachusetts)"
			replace location = "Washington" if location == "Wsahington"
			replace location = "Wyoming" if location == "Wyomin(Mid-West)"
			replace location = "Wyoming" if location == "Wyoming"

			replace location = "Ohio" if location == "Xenia(Greene county"
			replace location = "Arizona" if location == "Yarnell(Arizona)"
			replace location = "Alabama" if location == "Yazoo(Mississippi); Alabama"
			replace location = "California" if location == "Yoosemiteregion"
			replace location = "California" if location == "YosemiteValley (California)"

			replace location = "Connecticut" if location == "easternWindham"
			replace location = "Illinois" if location == "illinois"
			replace location = "Louisiana" if location == "louisiana"
			replace location = "Arizona" if location == "nearTucson (Arizona)"
			replace location = "California" if location == "region(California)"
			replace location = "Texas" if location == "texas"
			replace location = "Vermont" if location == "vermont"
			replace location = "Washington" if location == "washington"
			replace location = "Connecticut" if location == "westernHartford Counties (Connecticut)"
			replace location = "North Carolina" if location == "westernLIncoln"

			save `sub_work', replace
			restore
			drop if iso3 == "USA"
			append using `sub_work'



			*************************************************************************************
			** KEN
			*************************************************************************************
			preserve
			keep if iso3 == "KEN"

			expand 2 if location == "Samburu(Upper Eastern); Mandera"
				bysort event_id location : gen mult = _n
				replace location = "Samburu" if location == "Samburu(Upper Eastern); Mandera" & mult == 1
				replace location = "Mandera" if location == "Samburu(Upper Eastern); Mandera" & mult == 2
				drop mult

			expand 3 if location == "Samburudistricts  (Upper Eastern reion); Wajir district (North Eastern); Muranga district (Centarl region)"
				bysort event_id location : gen mult = _n
				replace location = "Samburu" if location == "Samburudistricts  (Upper Eastern reion); Wajir district (North Eastern); Muranga district (Centarl region)" & mult == 1
				replace location = "Wajir" if location == "Samburudistricts  (Upper Eastern reion); Wajir district (North Eastern); Muranga district (Centarl region)" & mult == 2
				replace location = "Murang'a" if location == "Samburudistricts  (Upper Eastern reion); Wajir district (North Eastern); Muranga district (Centarl region)" & mult == 3
				drop mult

			replace location = "Busia" if inlist(location, "Bussiaregion")

			replace location = "Garissa" if regexm(location, "Garissa")

			replace location = "Kalifi" if inlist(location, "Kifili", "Kalifi districts")

			replace location = "Kisumu" if regexm(location, "Kisumu")

			replace location = "Kwale" if regexm(location, "Kwale")

			replace location = "Lamu" if regexm(location, "Lamu")

			replace location = "Mandera" if regexm(location, "Mandera")

			replace location = "Marsabit" if regexm(location, "Marsabit")

			replace location = "Meru" if regexm(location, "Meru")

			replace location = "Migori" if regexm(location, "Migori")

			replace location = "Mombasa" if regexm(location, "Mombasa")

			replace location = "Nairobi" if regexm(location, "Nairobi")

			replace location = "Samburu" if regexm(location, "Samburu")
			replace location = "Samburu" if location == "Sambru"

			replace location = "Murang'a" if regexm(location, "Murang'a")

			replace location = "TaitaTaveta" if regexm(location, "^Taita")

			replace location = "TanaRiver" if regexm(location, "^Tana")

			replace location = "TharakaNithi" if regexm(location, "Tharaka")

			replace location = "TransNzoia" if regexm(location, "^TransNz")

			replace location = "Turkana" if regexm(location, "Turkana")

			save `sub_work', replace
			restore
			drop if iso3 == "KEN"
			append using `sub_work'


			************************************************************************8
			** ZAF
			************************************************************************8
			preserve
			keep if iso3 == "ZAF"

			expand 2 if location == "Gauteng(Eastern Cape)"
				bysort event_id location : gen mult = _n
				replace location = "Gauteng" if location == "Gauteng(Eastern Cape)" & mult == 1
				replace location = "Eastern Cape" if location == "Gauteng(Eastern Cape)" & mult == 2
				drop mult

			expand 2 if inlist(location, "KwazuluNatal (Free state)", "MpumalangaKwa-Zulu Natal and Guateng Provinces")
				bysort event_id location : gen mult = _n
				replace location = "KwaZulu-Natal" if inlist(location, "KwazuluNatal (Free state)", "MpumalangaKwa-Zulu Natal and Guateng Provinces") & mult == 1
				replace location = "Free State" if inlist(location, "KwazuluNatal (Free state)", "MpumalangaKwa-Zulu Natal and Guateng Provinces") & mult == 2
				drop mult

			expand 2 if location == "easternand northern Cape Region"
				bysort event_id location : gen mult = _n
				replace location = "Eastern Cape" if location == "easternand northern Cape Region" & mult == 1
				replace location = "Northern Cape" if location == "easternand northern Cape Region" & mult == 2
				drop mult

			replace location = "Eastern Cape" if regexm(location, "(EasternCape)|(Easterncape)|(Eastern Cape)")

			replace location = "Free State" if regexm(location, "FreeState")
			replace location = "Free State" if location == "OrangeFree"

			replace location = "Gauteng" if regexm(location, "Gauteng")
			replace location = "Gauteng" if location == "Johannesburg"

			replace location = "KwaZulu-Natal" if regexm(location, "(KwaZulu)|(Kwazulu)|(kwaZulu)")
			replace location = "KwaZulu-Natal" if regexm(location, "Natal") | location == "N.Natal" | location == "NorthernNatal"

			replace location = "Mpumalanga" if regexm(location, "Mpumalanga")

			replace location = "North-West" if regexm(location, "NorthWest")

			replace location = "Northern Cape" if regexm(location, "NorthernCape")

			replace location = "Western Cape" if regexm(location, "(CapeTown)|(Capetown)")
			replace location = "Western Cape" if regexm(location, "Westerncape")

			save `sub_work', replace
			restore
			drop if iso3 == "ZAF"
			append using `sub_work'


			****************************************************************************************
			** SWE
			****************************************************************************************
			preserve
			keep if iso3 == "SWE"

			replace location = "Sweden except Stockholm" if inlist(location, "Avesta", "Bollnas", "Goteborg", "Mora")

			save `sub_work', replace
			restore
			drop if iso3 == "SWE"
			append using `sub_work'


			******************************************************************************************
			******************************************************************************************
			******************************************************************************************
			******************************************************************************************

			/* Get rid of any accented characters
			ASCII table
			dec | char
			225 | �
			227 | �
			233 | �
			237 | �
			243 | �
			244 | �
			250 | �
			*/
			replace location = subinstr(location, char(225), "a", .)
			replace location = subinstr(location, char(227), "a", .)
			replace location = subinstr(location, char(233), "e", .)
			replace location = subinstr(location, char(237), "i", .)
			replace location = subinstr(location, char(237), "i", .)
			replace location = subinstr(location, char(243), "o", .)
			replace location = subinstr(location, char(244), "o", .)
			replace location = subinstr(location, char(250), "u", .)

			
		** get number of provinces affected, distribute deaths across them
		bysort event_id: gen nn = _n
		bysort event_id: egen number_provinces_affected = max(nn)
		replace numkilled = numkilled/number_provinces_affected
		drop nn number_provinces_affected area_number event_id

		rename mainlocation_id location_id
		replace countryname = location
		drop location
		rename mainlocation location

		** add on iso3 & location_id codes
		drop iso3 location_id
		merge m:1 countryname using `codes_sub'
			replace _m = 3 if countryname == "The Bahamas" | countryname == "Taiwan" // keep Bahamas & Taiwan
			replace iso3 = "BHS" if countryname == "The Bahamas"
			replace location_id = 106 if iso3 == "BHS"
			replace iso3 = "TWN" if countryname == "Taiwan"
		keep if _m!=2
			replace iso3 = iso if _m == 1

		** bring together with all the other data (at the national level)
		append using `all_emdat_data'
		drop if _m == . & (inlist(iso3, "CHN", "MEX", "GBR", "IND", "BRA", "JPN") | inlist(iso3,"SAU","SWE","USA","KEN","ZAF")) // & note == ""
		replace _m = 4 if _m == .

	** formatting
  replace iso3 = "IND" if iso3 == "Ind"
	replace type = type + " - " + subtype if type != subtype
	drop  subtype start* end*
	replace type = "Earthquake" if regexm(type, "Earthquake")==1
	replace type = "Wildfire" if regexm(type, "Wildfire")
	replace type = "Volcanic activity" if regexm(type, "Volcanic")
	replace type = "Cold wave" if regexm(type, "(Cold Wave)|(Severe Winter Conditions)")
	replace type = "Drought" if regexm(type, "Drought")
	replace type = "Famine" if regexm(type, "Complex Disasters")
	replace type = "Heat wave" if regexm(type, "Heat Wave")
	replace type = "Flood" if regexm(type, "Flood")
	replace type = "storm" if regexm(type, "Storm")
	replace type = "Other hydrological" if regexm(type, "(Avalanche)|(Subsidence)")
	replace type = "Other Geophysical" if regexm(type, "(Landslide)|(Rockfall)") | type == "Mass Movement (Dry) - --"
	order countryname iso3 gbd_region disyear, first
	order numkilled, last
	compress


	** make the dataset wide
		** first need to collapse the number killed by iso3-disaster year - type
		collapse (sum) numkilled, by(countryname iso3 location_id gbd_region disyear type _m source)
		** split unknown subnationals
		preserve
			// Import researched subnational info for; based off leftover unmatched (_m==1) obs. from above merge
			import excel using "FILEPATH", clear firstrow

			// First clean up spelling, then match with `codes_sub'. Some obs. are still unknown
			rename CorrectedSubnational countryname
			rename ISO3 iso3
			rename Region gbd_region
			rename Year disyear
			rename Event type
			rename Deaths numkilled
			keep countryname iso3 gbd_region disyear type numkilled
			// Split multi-location events
			gen id = _n
			replace countryname = "n/a unknown" if countryname == ""
			split countryname, parse(";")
			drop countryname
			reshape long countryname, i(iso3 disyear gbd_region type numkilled id) j(multi_indic)
			replace countryname = trim(countryname)
			drop if countryname == ""
			bysort id : replace multi_indic = _N
			replace numkilled = numkilled / multi_indic
			drop multi_indic
			// Clean-up countrynames
			replace countryname = proper(countryname)
			// Add commas to India urbanicity
			foreach urb in Rural Urban {
				replace countryname = subinstr(countryname, " `urb'", ", `urb'", 1) if iso3 == "IND"
			}
			replace countryname = "WestPokot" if countryname == "Westpokot"
			replace countryname = "TanaRiver" if countryname == "Tanariver"
			replace countryname = "TaitaTaveta" if countryname == "Taitataveta"
			replace countryname = "Murang'a" if regexm(countryname, "^Murang")
			replace countryname = "HomaBay" if countryname == "Homabay"
			replace countryname = "Uttar PrUSER" if countryname == "UttarprUSER"
			replace countryname = subinstr(countryname, "Madya", "Madhya", 1)
			replace countryname = subinstr(countryname, "Jammu And Kashmir", "Jammu and Kashmir", 1)
			replace countryname = "Gujarat, Rural" if countryname == "Gujarat , Rural"
			replace countryname = subinstr(countryname, "Andrha", "Andhra", 1)
			replace countryname = subinstr(countryname, "Andra", "Andhra", 1)
			replace countryname = "Jammu and Kashmir" if countryname == "Kashmir"
			replace countryname = subinstr(countryname, "Chandigarh", "Chhattisgarh", 1)
			replace countryname = "Veracruz de Ignacio de la Llave" if countryname == "Veracruz"
			replace countryname = subinstr(countryname, " Do ", " do ", 1) if iso3 == "BRA"
			replace countryname = "Rio de Janeiro" if countryname == "Rio De Janeiro"
			replace countryname = "Michoacan de Ocampo" if countryname == "Michoacan De Ocampo"
			replace countryname = "Puebla" if countryname == "Pueblo"
			replace countryname = "District of Columbia" if countryname == "District Of Columbia"
			replace countryname = subinstr(countryname, "Kwazulu", "KwaZulu", 1)

			merge m:1 countryname using `codes_sub', keep(1 3)
				gen split = 0 if _m == 3
				replace split = 1 if _m == 1
				drop _merge

			drop if numkilled == 0
			gen source = "EMDAT"
			tempfile sub_researched
			save `sub_researched', replace

		restore
		// Drop the observations that were researched above
		drop if _m == 1
		gen split = 0
		append using `sub_researched'
		drop _merge

		
		// Split remaining subnationals
		
		
		preserve
			keep if split == 1

			collapse (sum) numkilled, by(iso3 disyear gbd_region type source) fast

			merge 1:m iso3 disyear type using `subnatl_popweight', keep(3) assert(2 3) nogen

			replace numkilled = numkilled * weight

			collapse (sum) numkilled, by(iso3 location_id disyear type gbd_region source) fast
			gen split = 0

			tempfile subnatl_estimate
			save `subnatl_estimate', replace

		restore
			drop if split == 1
			append using `subnatl_estimate'
			assert split == 0
			drop split

	replace source = "EMDAT" if type != "Meningitis" & type != "Cholera" & source == ""
	// FINAL COLLAPSE
		collapse (sum) numkilled, by(iso3 location_id disyear type source) fast
		isid iso3 location_id disyear type source

	rename disyear year
	rename type cause
	* generate source = "EMDAT"
	** epidemics didn't come from
	* replace source = "Gideon/WHO" if inlist(cause, "Cholera", "Meningitis") & iso3 != "BGD" & year != 1982 & year != 1983
	gen nid = .
	replace nid = 13769 if source == "EMDAT"
	tempfile disaster
	save `disaster', replace

** quick changes to famine
replace iso3 = "SSD" if iso3 == "SDN" & inlist(year, 1983, 1984, 1985) & cause == "Famine"
** fix ZAF subnational epidemic
replace location_id = 485 if iso3 == "ZAF_485"
replace iso3 = "ZAF" if iso3 == "ZAF_485"
** GBR 434...same thing as with ZAF...
replace location_id = 434 if iso3 == "GBR_434"
replace iso3 = "GBR" if iso3 == "GBR_434"

** append sources together
append using `supplement'
drop Total location_type_id gbd_region
compress

** append India epidemics that were split
* preserve
* 	import delimited using "FILEPATH", clear
* 	tempfile ind_sub_splits_epidemics
* 	save `ind_sub_splits_epidemics', replace
* restore
* append using `ind_sub_splits_epidemics'

** 1977 AP cyclone
preserve
	keep if year == 1978 & iso3 == "IND"
	expand 2, gen(new)
	replace location_id = 43872 if new == 1
	replace numkilled = 14204 if new == 1
	replace year = 1977 if new == 1
	* replace total_deaths = 14204 if new == 1
	replace cause = "Cyclone" if new == 1
	replace source = "EMDAT" 
	drop if new != 1
	drop new
	tempfile ind_77
	save `ind_77', replace
restore
drop if year == 1977 & iso3 == "IND" & location_id == 43872
append using `ind_77'

** Sumatera Barat earthquake
replace location_id = 4711 if iso3== "IDN" & year == 2009 & location_id == 11 & numkilled == 1323

** epidmeics column names
* replace cause = type if cause == "" & type != ""
* replace year = disyear if year == . & disyear != .
* replace numkilled = total_deaths if numkilled == . & total_deaths != .
assert source != ""

** Bihar 1987 flood (1200 in EMDAT)
preserve
	keep if year == 1987 & iso3== "IND" & location_id == 43911 & cause == "Flood"
	replace numkilled = 1200
	expand 2, gen (new)
	replace location_id = 43875 if new == 1
	rename year disyear
	merge 1:1 disyear location_id using `bihar_urban_rural', keep(3) nogen
	rename disyear year
	replace numkilled = numkilled * weight
	replace source = "EMDAT"
	replace nid = 13769
	replace cause = "Flood"
	tempfile bihar_87
	save `bihar_87', replace
restore
drop if year == 1987 & iso3== "IND" & location_id == 43911
append using `bihar_87'

** 2004 tusnami for Six Minor Territories
preserve
	keep if year == 2004 & iso3 == "IND" & inlist(location_id, 44538, 44539, 44540) & cause == "Earthquake"
	reshape wide numkilled, i(iso3 year cause source) j(location_id)
	gen six_minor_R_prop = numkilled44539 / numkilled44538
	gen six_minor_U_prop = numkilled44540 / numkilled44538
	replace numkilled44538 = 1310
	replace numkilled44539 = numkilled44538 * six_minor_R_prop
	replace numkilled44540 = numkilled44538 * six_minor_U_prop
	reshape long
	tempfile six_minor_quake
	save `six_minor_quake', replace
restore
drop if year == 2004 & iso3 == "IND" & inlist(location_id, 44538, 44539, 44540) & cause == "Earthquake"
append using `six_minor_quake'

** append all split epidemics -- just meningitis and cholera
preserve
	import delimited using "FILEPATH", clear
	rename year_id year
	replace cause = proper(cause)
	tempfile splits_epidemics
	save `splits_epidemics', replace
restore
append using `splits_epidemics'

** fix IDN 2004 tsunami --- why is this still a problem!?!
replace numkilled = 82956 if iso3 == "IDN" & location_id == 11 & year== 2004 & cause == "Earthquake"
replace location_id = 4710 if iso3 == "IDN" & location_id == 11 & year== 2004 & cause == "Earthquake" & numkilled == 82956


** save
keep iso3 location_id year cause source numkilled nid countryname
order iso3 location_id year cause source, first
save "`output_folder'/formatted_type_specific_disaster_deaths.dta", replace
save "`output_folder'/archive/formatted_type_specific_disaster_deaths_`date'.dta", replace
