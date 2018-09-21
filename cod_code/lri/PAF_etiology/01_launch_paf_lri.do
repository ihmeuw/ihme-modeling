// The purpose of this file is to launch parallel jobs to calculate lri PAFs ///
set more off
clear all


qui do "F/get_location_metadata.ado"	
get_location_metadata, location_set_id(9) clear
keep if most_detailed == 1 & is_estimate==1
levelsof location_id, clean
global location_ids `r(levels)'
global year_ids 1990 1995 2000 2005 2010 2016
global sex_ids 1 2

// Launch job for each location and year
	foreach location_id of global location_ids {
				foreach year of global year_ids {
					! qsub -P proj_rfprep -N "PAF_LRI_`location_id'_`year'" -pe multi_slot 4 -l mem_free=8 -o FILEPATH ///
					"FILEPATH/stata_shell.sh" ///
					"FILEPATH/lri_paf_calculation.do" ///
					"`location_id' `year'"
					
				}
	}
