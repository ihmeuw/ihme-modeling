


// Hearing aid coverage by severity in Norway
	insheet using FILEPATH, comma clear
		


		
			gen cases = mean*sample_size
			collapse (sum) cases sample_size, by(age_start sev_thresh)
			gen mean = cases/sample_size
		gen logit_mean = logit(mean)
			noisily regress logit_mean age_start i.sev_thresh
			use `demog', clear
		predict logit_pred_mean, xb
		predict logit_pred_se, stdp
		gen pred_mean = invlogit(logit_pred_mean)
		
		gen pred_se = pred_mean*(1-pred_mean)*logit_pred_se 
		sort sev age_start
		keep sev age_start pred_mean pred_se age_group_id
		order sev age_start pred_mean pred_se
		tempfile means
		save `means', replace
		levelsof age_group_id, local(ages)
		levelsof sev, local(sevs)
		foreach sev of local sevs {
			clear
			gen age_group_id = .
			tempfile `sev'_dists
			save ``sev'_dists', replace
			foreach age of local ages {
				use `means' if age_group_id == `age' & sev == `sev', clear
				local M = pred_mean
				local SE = pred_se
				local N = `M'*(1-`M')/`SE'^2
				local a = `M'*`N'
				local b = (1-`M')*`N'
				clear
				set obs 1000
				gen sev_`sev'_ = rbeta(`a',`b')
				gen num = _n-1
				gen age_group_id = `age'
				reshape wide sev_`sev'_, i(age_group_id) j(num)
				append using ``sev'_dists'
				save ``sev'_dists', replace
			}
		
			cap save FILPATH, replace //will still run if running locally 
		}
	
	





	//load hearing aid coverage draws for Norway 
			get_draws, gbd_id_field(modelable_entity_id) source(epi) gbd_id(2411) measure_ids(5) location_ids(90) clear
			keep if inlist(age_group_id, `age_ids_list')
			rename draw* Norway_draw*
			tempfile Norway_hearing_aid_coverage
			save `Norway_hearing_aid_coverage', replace 
	//load hearing aid coverage draws for country of interest 
 		  	get_draws, gbd_id_field(modelable_entity_id) source(epi) gbd_id(2411) measure_ids(5) location_ids(`location_id') clear
 		  	keep if inlist(age_group_id, `age_ids_list')
			tempfile country_hearing_aid_coverage
			save `country_hearing_aid_coverage', replace 


	** CALCULATE RATIO
	*************************************
		merge 1:1 year_id sex_id age_group_id using `Norway_hearing_aid_coverage', nogen 
			forvalues draw = 0/999 {
				gen ratio_draw_`draw' = draw_`draw' / Norway_draw_`draw'
				}
			save `country_hearing_aid_coverage', replace 

//Loop over every year and sex for given location 
	//year_ids
		local year_ids "1990 1995 2000 2005 2010 2016"
	//sex_ids
		local sex_ids "1 2"

	foreach year_id in `year_ids' {
		foreach sex_id in `sex_ids' {


		use `country_hearing_aid_coverage' if year_id == `year_id' & sex_id == `sex_id', clear 

		//Call in Norway severity specific hearing aid coverage 
		forvalues sev=20(15)65 {
			merge 1:1 age_group_id using "FEILPATH", nogen
			}

			
			local draw 0	
				forvalues draw = 0/999 {
						di in red "Draw `draw'! `step_name'"
						
						forvalues sev=20(15)65 {
							gen aids_sev_`sev'_draw_`draw' = ratio_draw_`draw' * sev_`sev'_`draw'
							}
						
						// Assume no correction for deafness
						gen aids_sev_95_draw_`draw'=0

				}		
				
				


			format *draw* %16.0g
			save FEILPATH, replace
			
	
		//Next sex	
		}
	//Next year 
	}

