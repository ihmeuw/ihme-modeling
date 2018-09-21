*update population numbers to be used as denominator
adopath ++ "FILEPATH"
create_connection_string, database(gbd) server(modeling-gbd-db)
    local gbd_str = r(conn_string)

//Step 1. Pull in case data by country-year from dataset from endemic countries
*PULL IN DATA
insheet using "FILEPATH/GW_apr11.csv", clear

*drop the population variable (sample size) for both ages and sexes

drop sample_size
drop sex
generate age_group_id=22
rename year_start year_id

save "FILEPATH/input_correct.dta", replace

*get population for GW endemic locations
 get_population, location_id("157 165 169 179 35659 190 200 201 202 204 205 207 211 212 213 214 216 218 435 522 43908 43938 43918 43923 43926 43927 43935 43937") age_group_id("22") sex_id("3") year_id("1984 1985 1986 1987 1988 1989 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016") clear
  drop process_version_map_id
 *save populations to file
  save "FILEPATH/input_results_1.dta", replace
  
 *pull populations from file to merge with input data
  use "FILEPATH/input_results_1.dta", 
  merge 1:1 location_id age_group_id year_id using "FILEPATH/input_correct.dta"
 
 drop age_group_id
 drop sex_id
 
 *write to a csv file
 
 outsheet  location_id year_id cases is_outlier cases population using gw_pop_corr.csv , comma replace
