/// Launches cholera draw creation code 03_dobydraw_2017.do

clear all
set more off

set maxvar 8000
** Set directories
	global j "filepath"
	set odbcmgr unixodbc

local username username

//// Launch parallel do files ////

//global draws 
//global n 1000
global n 10


forvalues value = 701(1)730 {
	!qsub -N "cholera_draw_`value'" -l fthread=2 -l m_mem_free=4G -P ihme_general -q all.q -l archive=TRUE  ///
		-o /filepath/`username'/output -e /filepath/`username'/errors ///
		"/filepath/stata_shell.sh" ///
		"/filepath/03_dobydraw_2019.do" ///
		"`value'" 
}
