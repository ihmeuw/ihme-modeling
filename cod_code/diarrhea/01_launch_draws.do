/// Launches cholera draw creation code '02_dobydraw_2016.do' ///
// Internal IHME filepath has been replaced with "FILEPATH" //

clear all
set more off

set maxvar 8000
** Set directories
	global j "/home/j"
	set odbcmgr unixodbc

local username USERNAME

//// Launch parallel do files ////

forvalues value = 1(1)1000{
	! qsub -P proj_rfprep -N "cholera_draw_`value'" -pe multi_slot 8 -l mem_free=8 -o FILEPATH/`username'/output ///
		"FILEPATH/stata_shell.sh" ///
		"FILEPATH/02_dobydraw_2016.do" ///
		"`value'" 
}

