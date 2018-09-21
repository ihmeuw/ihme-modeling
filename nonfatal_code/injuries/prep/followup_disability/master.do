***********************************************************************************************************
* username
* DATE

***********************************************************************************************************
***********************************************************************************************************

cap cleartmp

clear
set mem 1g
set more off
cap restore, not
cap log close



***************************************************************************************************
* SET UP MACROS

// install needed functions
	// ssc install sf36
	// ssc install carryforward
	
// set OS
	if c(os) == "Windows"   global j "FILEPATH"
	if c(os) == "Unix" 		global j "FILEPATH"

// today's date
	global date `c(current_date)'
	
// define locations 
// If these files move, this is the only place you should have to change the directory, all the other code runs off this path
	local  path 		"FILEPATH" 
	global raw 			"`path'/FILEPATH"
	global prepped 		"`path'/FILEPATH"
	global output 		"`path'/FILEPATH"
	global int 			"`path'/FILEPATH"
	global logs 		"`path'/FILEPATH"
	global code 		"`path'/FILEPATH"
	
	global ncodes tGS829 tGS909 tGS958 tGS959 tGS907 tGS908 tN10 tN11 tN12 tN14 tN19 tN1a tN1b tN20 tN21 tN22 tN23 tN25 tN26 tN29 tN2a tN2b tN2c tN2d tN3 tN30 tN31 tN4 tN5 tN6 tN7 tN8 tN9 // RB JULY012013
	
***************************************************************************************************
* LOG WORK

	log using "${logs}//${date}_LOG.smcl", replace
	
***************************************************************************************************
* 1) DATA CLEANING

	// DUTCH LIS 
	do "${code}//1) clean DUTCH.do"
	