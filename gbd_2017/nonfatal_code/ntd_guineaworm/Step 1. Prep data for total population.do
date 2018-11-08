*update population numbers for original data


*** BOILERPLATE ***
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		}
		
	




	
*** LOAD SHARED FUNCTIONS ***			
	adopath ++ FILEPATH
	run FILEPATH/get_demographics.ado
	run FILEPATH/create_connection_string.ado
	


adopath ++ FILEPATH
create_connection_string, database(ADDRESS) server(ADDRESS)
    local gbd_str = r(conn_string)

//Step 1. Pull in case data by country-year from dataset from endemic countries

insheet using "FILEPATH/GW_custom_data.csv", clear

*drop the population variable (sample size)

drop sample_size
drop sex
generate age_group_id=22
rename year_start year_id

duplicates drop
table year_id

save "FILEPATH/input_correct.dta", replace

 get_population, location_id("157 165 169 44860 35659 190 200 201 202 204 205 207 211 212 213 214 216 218 435 522 43908 43938 43918 43923 43926 43927 43935 43937") age_group_id("22") sex_id("3") year_id("1984 1985 1986 1987 1988 1989 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017") clear
 
  save "FILEPATH/input_results_1.dta", replace
  
  use "FILEPATH/input_results_1.dta", 

  
  merge 1:1 location_id age_group_id year_id using "FILEPATH/input_correct.dta"
 
 drop age_group_id
 drop sex_id
 
 *write to a csv file
 
 outsheet  location_id year_id cases is_outlier cases population using "FILEPATH/gw_pop_corr.csv" , comma replace
