
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	set type double, perm
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		local cluster 1 
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		local cluster 0
	}
	// directory for standard code files
		adopath + FILEPATH

	

		// base directory on J 
		local root_j_dir `1'
		// base directory on clustertmp
		local root_tmp_dir `2'
		// timestamp of current run (i.e. 2014_01_17) 
		local date `3'
		// step number of this step (i.e. 01a)
		local step_num `4'
		// name of current step (i.e. first_step_name)
		local step_name `5'
		// directory for steps code
		local code_dir `6'
		local location_id `7'

	
//Loop over every year and sex for given location 
	//year_ids
		local year_ids "1990 1995 2000 2005 2010 2016"
	//sex_ids
		local sex_ids "1 2"

	foreach year_id in `year_ids' {
		foreach sex_id in `sex_ids' {

		** bring in proportion of people with hearing loss with hearing aids 
		use FILEPATH clear 
		keep age_group_id aids* //drop vars that needed to be kept for diagnostics
		tempfile aids
		save `aids', replace

		** bring in prevalence information
		use FILEPATH, clear 
		drop *db* //drop vars that needed to be kept for diagnostics
		tempfile envelope
		save `envelope', replace


	use `aids', clear
	merge 1:1 age_group_id using `envelope', nogen
	tempfile working_temp
	save `working_temp'
	
	** make a place to save adjusted information
	keep age_group_id
	tempfile adjusted_temp
	save `adjusted_temp', replace

	** adjust hearing envelope with hearing aids data 

		local draw 0
		quietly {
			forvalues draw=0/999 {

				noisily di in red "DRAW `draw'! `step_name' - `year_id' sex `sex_id'"
				
				use `working_temp', replace

				** category by category apply the adjustment
					
					** convert hearing aid coverage to prevalence (severity prevalence * hearing aid coverage)
						local sev 20
						forvalues sev=20(15)95 {
							gen prev_aids_sev_`sev'_draw_`draw' = draw_`draw'_`sev' * aids_sev_`sev'_draw_`draw'
						}					
					
					** make sure hearing aid coverage is never over 95% for a given severity
						local sev 20
						forvalues sev=20(15)95 {
							gen prev_95pct_`sev'= draw_`draw'_`sev' * .95
							replace prev_aids_sev_`sev'_draw_`draw' = prev_95pct_`sev' if aids_sev_`sev'_draw_`draw' > 0.95
						}

						gen adjusted_`draw'_20 = draw_`draw'_20 - prev_aids_sev_20_draw_`draw'
				
					** 35-50
						gen adjusted_`draw'_35 = draw_`draw'_35 - prev_aids_sev_35_draw_`draw'
						replace adjusted_`draw'_20 = adjusted_`draw'_20 + prev_aids_sev_35_draw_`draw'
					
					** 50-65
						gen adjusted_`draw'_50 = draw_`draw'_50 - prev_aids_sev_50_draw_`draw'
						replace adjusted_`draw'_35 = adjusted_`draw'_35 + prev_aids_sev_50_draw_`draw'			
					
					** 65-80
						gen adjusted_`draw'_65 = draw_`draw'_65 - prev_aids_sev_65_draw_`draw'
						replace adjusted_`draw'_50 = adjusted_`draw'_50 + prev_aids_sev_65_draw_`draw'	

					** 80-95 NOT  ADJUSTED)
						gen adjusted_`draw'_80 = draw_`draw'_80  
					** 95+ NOT ADJUSTED
						gen adjusted_`draw'_95 = draw_`draw'_95 

				save `working_temp', replace
					
			}
		}
		
		use `working_temp', clear
		
	** save draws in intermediate location
		format adj* %16.0g
		cap mkdir FILEPATH
		save FILEPATH, replace
		count

	

		//Next sex	
		}
	//Next year 
	}
