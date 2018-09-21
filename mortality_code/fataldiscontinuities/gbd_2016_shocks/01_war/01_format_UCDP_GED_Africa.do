// Author:	NAME
// Date:		10.3.2014
// Date Modified: 11.6.2015 by NAME
// Purpose: To use the UCDP disaggregated dataset for Africa in order to reduce death attribution errors when >1 country is inolved in a conflict-year

	** set up stata
clear all
set more off
set mem 4g
if c(os) == "Windows" {
	global j "PATH"
}
if c(os) == "Unix" {
	global j "/FILEPATH"
	set odbcmgr unixodbc
}

global outdir "FILEPATH"
// AT: for later connection to db
do "$FILEPATH/create_connection_string.ado"
		create_connection_string, server("modeling-cod-db") database("cod")
		local conn_string `r(conn_string)'

// bring in file
	import delimited "FILEPATH", varnames(1) // GBD 2016
	drop v*

/* Format GBD 2016 UCDP_GED data */
drop if deaths_a == .				// AT 12/01/16 : choice of variable is arbitrary: all rows od deaths_* and *_est contain . as their value. Dropping by any of them is equivalent.
destring year deaths_* *_est, replace

// Fix conflict_dset_id
	split conflict_dset_id, p("-")
	replace conflict_dset_id = conflict_dset_id2
	replace conflict_dset_id = conflict_dset_id1 if conflict_dset_id2 == ""
	drop conflict_dset_id1 conflict_dset_id2

// keep on variables that may be needed and sum of death numbers
	keep deaths_a deaths_b deaths_civilians deaths_unknown best_est high_est low_est year type_of_violence conflict_dset_id conflict_name dyad_dset_id dyad_name side_a side_a_dset_id side_b side_b_dset_id country gwno ihme_loc_id
	collapse (sum) deaths_a deaths_b deaths_civilians deaths_unknown best_est high_est low_est, by(year type_of_violence conflict_dset_id conflict_name dyad_dset_id dyad_name side_a side_a_dset_id side_b side_b_dset_id country gwno ihme_loc_id)

	rename gwno gwnoloc

	// tagging duplicates for observations where >1 country is present
	duplicates tag year type_of_violence conflict_dset_id dyad_dset_id conflict_name, gen(dup)

	// only keep data points with >1 country involved
	tab dup
	keep if dup>0

	// expand dataset in order to divide deaths among the country/locations listed for deathsa/b/civ
		bysort year type_of_violence conflict_dset_id dyad_dset_id conflict_name: gen n=_n
		generate indic =3 if dup > 0
		replace indic = 1 if dup == 0
		expand indic if dup >0

		rename (deaths_a deaths_b deaths_civilians best_est high_est low_est) (deada deadb deadciv best high low)

	// create ratios to divide unkowns between the locations
		bysort year type_of_violence conflict_dset_id dyad_dset_id conflict_name n: gen n2=_n

		foreach var of varlist deada deadb deadciv {
			generate ratio_`var' = `var'/ (deada + deadb + deadciv) if dup > 0
			replace ratio_`var' = 0 if (deada + deadb + deadciv) == 0 & `var' == 0 & dup > 0
			}

	// generate a deaths varaible for best_estimate that takes into account the unkown deaths when >1 country is present
			generate deaths = deada + (ratio_deada*deaths_unknown)  if n2==1 & dup > 0
			replace deaths = deadb + (ratio_deadb*deaths_unknown) if n2==2 & dup > 0
			replace deaths = deadciv + (ratio_deadciv*deaths_unknown) if n2==3 & dup > 0

		// deaths_best consistency check
		gen totdeath = deada+deadb+deadciv+deaths_unknown
		// assert totdeath == best if dup > 0
			// 27 contradictions out of 1092

		// if the best estimate doesn't match up to the total death (deada+deadb+deadciv+deaths_unknown), then I'm replacing the deaths estimate by multiplying the best deaths estimate with the ratio of deaths for sidea/b/civilians when >1 country is present
		generate deaths2 = best*ratio_deada if n2 == 1 & totdeath != best & dup > 0
		replace deaths2 = best*ratio_deadb if n2 == 2 & totdeath != best & dup > 0
		replace deaths2 = best*ratio_deadciv if n2 == 3 & totdeath != best & dup > 0
		replace deaths = deaths2 if totdeath != best & dup > 0
		drop totdeath deaths2

	// in order to split the low and high estimates, use the ratio of deaths attributed to the location/best
		gen deaths_low = low*(deaths/best)
		gen deaths_high = high*(deaths/best)

	// code deaths variable if only deaths_unknown are present
		replace deaths = 12345 if deada == 0 & deadb == 0 & deadciv == 0 & deaths_unknown > 0 & dup >0
		replace deaths = 123456 if deada == 0 & deadb == 0 & deadciv == 0 & deaths_unknown == 0 & dup > 0
		// 162 instances

	// Look at how deaths are distributed based on type of conflict when >1 country was involved
		// Since the number of civilians dead depends on type of the conflict, it makes more sense to use the total number of deaths for sidea/b/civ across all observations in the same type of conflict when >1 country was involved and create ratios of deathsa/b/civ. We can then use this ratio to divide the deaths where death estimates for deada/b/civ are 0
		bysort type_of_violence: egen totdeada = total(deada) if dup > 0
		bysort type_of_violence: egen totdeadb = total(deadb) if dup > 0
		bysort type_of_violence: egen totdeadciv = total(deadciv) if dup > 0

		generate totratio_deada = totdeada / (totdeada + totdeadb + totdeadc)
		generate totratio_deadb = totdeadb/ (totdeada + totdeadb + totdeadc)
		generate totratio_deadciv = totdeadciv / (totdeada + totdeadb + totdeadc)

		// if deaths==12345, meaning all deaths are unkown, split the best/low/high estimates between sidea/b/civ based on the totratios that were just created
		// if deaths==123456, meaning only best estimates are available, split the best/low/high estimates between sidea/b/civ based on the totratios that were just created
		replace deaths_low = totratio_deada * low if inlist(deaths, 12345, 123456) & n2 == 1
		replace deaths_high = totratio_deada * high if inlist(deaths, 12345, 123456) & n2 == 1
		replace deaths = totratio_deada * best if inlist(deaths, 12345, 123456) & n2 == 1

		replace deaths_low = totratio_deadb * low if inlist(deaths, 12345, 123456) & n2 == 2
		replace deaths_high = totratio_deadb * high if inlist(deaths, 12345, 123456) & n2 == 2
		replace deaths = totratio_deadb * best if inlist(deaths, 12345, 123456) & n2 == 2

		replace deaths_low = totratio_deadciv * low if inlist(deaths, 12345, 123456) & n2 == 3
		replace deaths_high = totratio_deadciv * high if inlist(deaths, 12345, 123456) & n2 == 3
		replace deaths = totratio_deadciv * best if inlist(deaths, 12345, 123456) & n2 == 3

		drop *ratio* totdead*

		** Country name fixes
			replace country = "Cambodia" if country == "Cambodia (Kampuchea)"
			replace country = "Democratic Republic of the Congo" if country == "DR Congo (Zaire)"
			replace country = "Cote d'Ivoire" if country == "Ivory Coast"
			replace country = "Myanmar" if country == "Myanmar (Burma)"
			replace country = "United States" if country == "United States of America"
			replace country = "Zimbabwe" if country == "Zimbabwe (Rhodesia)"

		tempfile data
		save `data', replace


		// Get ISO3

		odbc load, clear `conn_string' exec("SELECT ihme_loc_id, location_name FROM shared.location_hierarchy_history WHERE location_set_version_id = 39 AND location_type = 'admin0'")
		//AT: trying above... it works! get rid of this line...// odbc load, exec("SELECT ihme_loc_id, location_name FROM shared.location_hierarchy_history WHERE location_set_version_id = 39 AND location_type = 'admin0'") dsn(cod) clear
		rename (ihme_loc_id location_name) (iso3 country)

		merge 1:m country using `data', keep(3) assert (1 3)
		replace iso3 = ihme_loc_id if ihme_loc_id != ""
		rename (deaths deaths_low deaths_high country) (dbest dlow dhigh country_name)


		// collapse
		collapse (sum) dbest dlow dhigh, by( year type_of_violence conflict_dset_id conflict_name dyad_dset_id dyad_name side_a side_a_dset_id side_b side_b_dset_id iso3 country_name gwnoloc)
		drop if dbest==0 & dlow==0 & dhigh==0

		// Add nid
		gen nid = 268262 // for UCDP_GED_1989_2015_V_5_0_2016_Y2016M10D24

	// save
	saveold "$outdir/Africa.dta", replace
