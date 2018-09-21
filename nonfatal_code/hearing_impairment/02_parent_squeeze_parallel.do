
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

	
		
		   	//load prevalence draws
		 		  	get_draws, gbd_id_field(modelable_entity_id) source(epi) gbd_id(`meid') measure_ids(5) location_ids(`location_id') clear
					keep if inlist(age_group_id, `age_ids_list')

					//Make sure all required ages are present 
					tab age_group_id
					if `r(r)' != `num_age_ids' di in red "MEID `meid' MISSING 1 OR MORE AGE_GROUP_IDS" BREAK 


			//save temp file 
				tempfile `healthstate'_draws 
				save ``healthstate'_draws', replace
			}
		

//Loop over every year and sex for given location 
	//year_ids
		local year_ids "1990 1995 2000 2005 2010 2016"
	//sex_ids
		local sex_ids "1 2"

	foreach year_id in `year_ids' {
		foreach sex_id in `sex_ids' {

			
			local i 0 
			foreach db in 0_19db 20_34db 35db 35_49db 50_64db 65_79db 80_94db 95db {
				use `_hearing_`db'_draws' if year_id == `year_id' & sex_id == `sex_id', clear
				rename draw* draw*_`db'
				if `i' == 0 tempfile working_temp 
				else merge 1:1 age_group_id using `working_temp', nogen
				save `working_temp', replace
				local ++ i 
				}

	

		** loop through each draw and calculate mutually exclusive, discrete (non-cumulative) prevalence estimates (ex 20-35 db, instead of 20+ dB prevalence)		
		
			local draw 0
			quietly {
				forvalues draw = 0/999 {
					noisily di in red "Draw `draw'!! `step_name' loop 1 ~~~ year `year_id' sex `sex_id'"
		
					use `working_temp', clear		
					
					** (1) squeeze 35+, 0-19, and 20-34 to sum up to 1 
					
						gen total= draw_`draw'_0_19db + draw_`draw'_20_34db + draw_`draw'_35db
						
						foreach db in 0_19db 20_34db 35db {
							gen draw_`draw'_`db'_squeeze1 = draw_`draw'_`db' * (1/total)
						}
						
						drop total						
					
					** (2) squeeze categorical results into 35+ parent envelope (0-19 and 20-35 not a part of the squeeze)
						
						gen total= draw_`draw'_35_49db + draw_`draw'_50_64db + draw_`draw'_65_79db + draw_`draw'_80_94db + draw_`draw'_95db
						

						foreach db in 35_49db 50_64db 65_79db 80_94db 95db {
							gen draw_`draw'_`db'_prop = draw_`draw'_`db' * (1/total)
							gen draw_`draw'_`db'_squeeze2 = draw_`draw'_`db'_prop * draw_`draw'_35db_squeeze1
							}
						
						drop total						
					
					** rename prevalence estimates for the categories of interest
						gen draw_`draw'_20 = draw_`draw'_20_34db_squeeze1
						gen draw_`draw'_35 = draw_`draw'_35_49db_squeeze2
						gen draw_`draw'_50 = draw_`draw'_50_64db_squeeze2
						gen draw_`draw'_65 = draw_`draw'_65_79db_squeeze2
						gen draw_`draw'_80 = draw_`draw'_80_94db_squeeze2
						gen draw_`draw'_95 = draw_`draw'_95db_squeeze2
					
					
					save `working_temp', replace
				}
			}
			
			use `working_temp', replace

			
		** save draws in intermediate location
			format draw* %16.0g
			save FILEPATH, replace
	
