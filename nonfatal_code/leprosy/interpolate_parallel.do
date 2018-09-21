** USERNAME
	
clear all
set more off
set maxvar 32767
set max_memory 1073741824000
if c(os) == "Unix" {
	global j "FILEPATH"
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	global j "FILEPATH"
}

// parse incoming syntax elements
cap program drop parse_syntax
program define parse_syntax
	syntax, location_id(string) sex_id(string) measure_id(string) out_dir(string) [modelable_entity_id(string) in_dir(string) ref_year(string)]
	c_local location_id "`location_id'"
	c_local sex_id "`sex_id'"
	c_local measure_id "`measure_id'"
	c_local modelable_entity_id = "`modelable_entity_id'"
	c_local in_dir = "`in_dir'"
	c_local out_dir = "`out_dir'"
	c_local ref_year = real("`ref_year'")
end
parse_syntax, `0'


adopath + "FILEPATH"
get_demographics, gbd_team("epi")

** read in Epi years and store in MATA
cd "`in_dir'"
foreach year of global year_ids {
	if "`in_dir'" != "" {
		import delimited using "`measure_id'_`location_id'_`year'_`sex_id'.csv", asdouble varname(1) clear
	}
	else {
		get_draws, gbd_id_field(modelable_entity_id) gbd_id(`modelable_entity_id') measure_ids(`measure_id') location_ids(`location_id') year_ids(`year') age_group_ids(2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21) sex_ids(`sex_id') source(epi) clear
	}
	sort age_group_id
	mata age = st_data(.,("age_group_id"))
	unab draws_`year' : draw*
	mata draws_`year' = .
	mata draws_`year' = st_data(., st_varindex(tokens("`draws_`year''")))
	mata mean_draws_`year' = rowsum(draws_`year') / cols(draws_`year')
}

** intermediate years
qui {
	foreach y in 2011 2012 2013 2014 {
		local year_gap = `y' - 2010
		mata draws_`y' = draws_2010 :* exp((ln(mean_draws_2015 :/ mean_draws_2010) :* (`year_gap') :/ (5)))
		mata _editmissing(draws_`y',0)
	}
	
	foreach y in 2006 2007 2008 2009 {
		local year_gap = `y' - 2005
		mata draws_`y' = draws_2005 :* exp((ln(mean_draws_2010 :/ mean_draws_2005) :* (`year_gap') :/ (5)))
		mata _editmissing(draws_`y',0)
	}
	
	foreach y in 2001 2002 2003 2004 {
		local year_gap = `y' - 2000
		mata draws_`y' = draws_2000 :* exp((ln(mean_draws_2005 :/ mean_draws_2000) :* (`year_gap') :/ (5)))
		mata _editmissing(draws_`y',0)
	}
	
	foreach y in 1996 1997 1998 1999 {
		local year_gap = `y' - 1995
		mata draws_`y' = draws_1995 :* exp((ln(mean_draws_2000 :/ mean_draws_1995) :* (`year_gap') :/ (5)))
		mata _editmissing(draws_`y',0)
	}
	
	foreach y in 1991 1992 1993 1994 {
		local year_gap = `y' - 1990
		mata draws_`y' = draws_1990 :* exp((ln(mean_draws_1995 :/ mean_draws_1990) :* (`year_gap') :/ (5)))
		mata _editmissing(draws_`y',0)
	}
	
** back to 1980, assuming 1990 - 1995 trend or a reference year if one was chosen
	forvalues y = 1980/1989 {
		local year_gap = `y' - 1990
		mata draws_`y' = draws_1990 :* exp((ln(mean_draws_`ref_year' :/ mean_draws_1990) :* (`year_gap') :/ (`ref_year' - 1990)))
		mata _editmissing(draws_`y',0)
	}

}

get_demographics, gbd_team("cod")

cd "`out_dir'/interpolated"

foreach year of global year_ids {
	clear
	getmata age_group_id=age (draw_*)=draws_`year', double
	forvalues draw = 1/1000 {
		local new_draw = `draw' - 1
		rename draw_`draw' draw_`new_draw'
	}
	format draw_* %16.0g
	gen location_id = real("`location_id'")
	gen measure_id = real("`measure_id'")
	gen sex_id = real("`sex_id'")
	gen year_id = real("`year'")
	outsheet age_group_id location_id measure_id sex_id year_id draw_* using "`measure_id'_`location_id'_`year'_`sex_id'.csv", comma replace
}


clear mata

