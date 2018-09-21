** ****************************************************
** Author: NAME 
** Modified: NAME, 30 Dec 2015 to compile only inj_disaster & inj_war deaths from VR
** Purpose: Prepare and compile all final Cause of Death sources for mortality team
** Date Created: 3/21/2014
** ****************************************************

** ***************************************************************************************************************************** **
** set up stata
clear all
set more off
set mem 4g
if c(os) == "Windows" {
	global j ""
}
if c(os) == "Unix" {
	global j ""
	set odbcmgr unixodbc
}

** ***************************************************************************************************************************** **
** define globals
global vrdir "FILEPATH"
global date = c(current_date)

** database setup
do "FILEPATH"
create_connection_string, server("modeling-cod-db") database("cod")
local conn_string `r(conn_string)'


