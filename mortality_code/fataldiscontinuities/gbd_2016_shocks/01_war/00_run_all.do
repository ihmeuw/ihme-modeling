// Author:NAME	
// Date:		4/5/2012
// Edited: 2/26/2015 by NAME to add a rough estimate of causes between war, terrorism, and legal intervention
// Purpose:	Update all conflict datasets

clear all
set more off

cd "FILEPATH"
global outdir "FILEPATH"
qui {
	do "01_format_UCDP_GED_Africa.do"
	noi: di "01 complete"
	do "02_format_UCDP_wardeaths.do"
	noi: di "02 complete"
	do "03_format_UCDP_one-sided_conflict_deaths.do"
	noi: di "03 complete"
	do "04_format_UCDP_non-state_deaths.do"
	noi: di "04 complete"
	do "05_format_pre-1989_ucdp_battles.do"
	noi: di "05 complete"
	do "07_format_IISS.do"
	noi: di "07 complete"
	do "08_format_2014_online_death_sources.do"
	noi: di "08 complete"
	do "09_format_robert_strauss_center.do"
	noi: di "09 complete"
	do "10_format_VR_war_deaths.do"
	noi: di "10 complete"
	do "11_combine_all_death_sources.do"
	noi: di "11 complete"
}

di "War Database Update Complete"
