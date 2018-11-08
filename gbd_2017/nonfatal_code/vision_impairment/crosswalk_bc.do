
		
		local user = "`c(username)'"
		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
        local date = subinstr("`date'", " " , "_", .)
		local step_num "03"
		local step_name "crosswalk_bc_presenting"
		local hold_steps ""
		local code_dir "FILEPATH"
		local in_dir "FILEPATH"
		local root_j_dir "FILEPATH"
		local root_tmp_dir "FILEPATH"
		
		}

		local i = 0
		foreach bundle in 347 348 297 721 722 723 296 724 {
			get_epi_data, bundle_id(`bundle') clear
			if `i' == 0 tempfile getepidata
			if `i' > 0 append using `getepidata', force
			save `getepidata', replace 
			local i = `i' + 1
			}
			save FILEPATH, replace

	
	drop if seq==. | year_start==.
	drop if extractor=="USER" & input_type=="group_review"
	replace group_review = 0 if strpos(note_modeler,"parent - combined severities split via crosswalk") & group_review==.
	replace group_review = 1 if strpos(note_modeler,"Split from combined severities via crosswalk") & group_review==.
	save `getepidata', replace 
	save FILEPATH, replace

//Get demographics. Super regions will be used for regression 
			get_location_metadata, location_set_id(9) clear 
			keep location_id super_region_id super_region_name
			drop if super_region_id == . 
			duplicates drop location_id, force
			tempfile super_regions
			save `super_regions', replace
			save FILEPATH, replace

			get_ids, table(age_group) clear 
			
			keep if inlist(age_group_id,1,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235) //5yr ranges 
			gen age_group_years_start = substr(age_group_name,1,2)
			replace age_group_years_start = "0" if age_group_id==1
			replace age_group_years_start = "5" if age_group_id==6
			destring age_group_years_start, replace
			gen age_group_years_end = substr(age_group_name,7,8)
			replace age_group_years_end = "4" if age_group_id==1
			replace age_group_years_end = "9" if age_group_id==6
			replace age_group_years_end = "99" if age_group_id==235
			destring age_group_years_end, replace
			gen age_group_mid = (age_group_years_start + age_group_years_end) / 2
			tempfile gbd_ages
			save `gbd_ages', replace
			save FILEPATH, replace
			
			

tempfile epi_data
save `epi_data'
// reshape data to have best corrected and presenting from same nid, location_id, modelable_entity_id, age, sex, and year


			drop if cv_self_report==1 | is_outlier == 1 | group_review == 0

			replace site_memo="." if site_memo==""
			replace extractor="." if extractor==""
			

			local unique_vars "modelable_entity_id severity location_id age_start age_end nid sex year_start cv_best_corrected site_memo urbanicity_type"
			foreach var in `unique_vars' {
				count if missing(`var')
				if `r(N)' > 0 di in red "missing values for `var' - isid will break "
			}

			sort `unique_vars'
			
			tempfile data_all_vars
			save `data_all_vars'
			
			keep `unique_vars' mean cases sample_size
			order `unique_vars' mean cases sample_size


			tempfile data
			save `data'


** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **

		// AGGREGATE SEXES
			count if cases == 0
			local countdown = `r(N)'
			while `countdown' != 0 {
				quietly {
					count if cases == 0
					local prev_count = `r(N)'
					sort `unique_vars'
					local all_sex_group_vars = subinstr("`unique_vars'", "sex ", "", 1)
					egen all_sex_group = group(`all_sex_group_vars')
					levelsof all_sex_group, local(groups)
					foreach group of local groups {
						replace sex = "Both" if cases[_n-1] == 0 & all_sex_group == `group' & all_sex_group[_n-1] == `group'
						replace sex = "Both" if cases == 0 & all_sex_group == `group'
						replace sex = "Both" if cases[_n+1] == 0 & all_sex_group == `group' & all_sex_group[_n+1] == `group'
					}
					collapse (sum) cases sample_size, by(`unique_vars') fast
				}
				count if cases == 0
				local countdown = `prev_count' - `r(N)'
			}
		// AGGREGATE AGE GROUPS
			foreach age_tail in front rear {
				count if cases == 0
				local countdown = `r(N)'
				while `countdown' != 0 {
					quietly {
						count if cases == 0
						local prev_count = `r(N)'
						sort `unique_vars'
						local all_age_group_vars = subinstr("`unique_vars'", "age_start age_end ", "", 1)
						egen all_age_group = group(`all_age_group_vars')
						levelsof all_age_group, local(groups)
						foreach group of local groups {
							if "`age_tail'" == "front" {
								replace age_end = age_end[_n+1] if cases == 0 & all_age_group == `group' & all_age_group[_n+1] == `group'
								replace age_start = age_start[_n-1] if cases[_n-1] == 0 & all_age_group == `group' & all_age_group[_n-1] == `group'
							}
							else if "`age_tail'" == "rear" {
								replace age_start = age_start[_n-1] if cases == 0 & all_age_group == `group' & all_age_group[_n-1] == `group'
								replace age_end = age_end[_n+1] if cases[_n+1] == 0 & all_age_group == `group' & all_age_group[_n+1] == `group'
							}
						}
						collapse (sum) cases sample_size, by(`unique_vars') fast
					}
					count if cases == 0
					local countdown = `prev_count' - `r(N)'
				}
			}
			gen mean = cases/sample_size
			drop cases
			
			tempfile collapsed 
			save `collapsed' 

		** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **

			local reshape_vars = subinstr("`unique_vars'", "cv_best_corrected ", "", 1)
			local reshape_vars = subinstr("`reshape_vars'", "modelable_entity_id ", "", 1)
			drop modelable_entity_id
			reshape wide mean, i(`reshape_vars' sample_size) j(cv_best_corrected) 			
				rename mean1 best_corrected
				rename mean0 presenting
				
			tempfile reshaped
			save `reshaped'

		

		use `reshaped', clear 
		gen ratio = best_corrected/presenting
		
		
				keep if best_corrected!=.
				keep if presenting!=.
								if `cluster' == 0 save FILEPATH, replace
				drop if best_corrected>presenting

			tempfile bc_presenting_envelope
			save `bc_presenting_envelope'

			use `bc_presenting_envelope', clear
			merge m:1 location_id using `super_regions', keep(3) nogen


			tempfile x
			save `x', replace

			levelsof super_region_name, local(supers)
			foreach region of local supers {
				local region_short = substr("`region'", 1, 10)
				local region_short=subinstr("`region_short'", " ", "", .)
				local region_short=subinstr("`region_short'", "-", "", .)
				gen `region_short'=0
				replace `region_short'=1 if super_region_name=="`region'"
			}

	
			gen logit_bc = logit(best_corrected)
			gen logit_pres = logit(presenting)
			gen bc = invlogit(logit_bc)
			gen pres = invlogit(logit_pres)
			replace bc = 0 if bc==. & best_corrected==0
			replace pres = 0 if pres==. & presenting==0

			if `cluster' == 0 save FILEPATH, replace

			tempfile temp
			save `temp', replace
			clear
		
	
					get_ids, table(covariate) clear 
					levelsof covariate_name if regexm(covariate_name, "LDI") | regexm(covariate_name, "access")
				//Query covariates 
					get_covariate_estimates, covariate_id(57)  clear
						rename mean LDI_pc
						keep location_id year_id LDI_pc
						tempfile covs 
						save `covs', replace
					get_covariate_estimates, covariate_id(1099)  clear 
						rename mean haqi
						keep location_id year_id haqi
						merge 1:1 location_id year_id using `covs', nogen 
						save `covs', replace
					
		// Run regressions
		
			quietly {
			clear
			gen age_group_id = .
			tempfile xwalked_pres
			save `xwalked_pres', replace
			*local vision_severity blindness
			foreach vision_severity in blindness mod severe  {    //imp
				noisily di in red "** ** ** ** ** ** ** ** ** ** ** **"
				noisily di in red "Regressing `vision_severity'"
				noisily di in red "** ** ** ** ** ** ** ** ** ** ** **"
				use `temp', clear
				if "`vision_severity'" == "blindness" keep if inlist(severity,"DVB")
				if "`vision_severity'" == "mod" keep if inlist(severity,"MOD")
				if "`vision_severity'" == "severe" keep if inlist(severity,"SEV")
				* if "`vision_severity'" == "imp" keep if inlist(severity,"IMP")
				gen age_group_mid = (age_start+age_end)/2
	
				rename year_start year_id
				merge m:1 location_id year_id using `covs', keep(3)

				gen log_ldi = log(LDI_pc)
				cap gen exp_hsa = exp(health_system_access)

				gen female = (sex=="Female")
				gen male = (sex=="Male")
				gen sex_both =(sex == "Both")

			

					
						noisily regress logit_pres logit_bc age_group_mid LDI_pc haqi LatinAmer NorthAfri SouthAsia Southeast SubSahara
						
				use `data', clear
				if "`vision_severity'" == "blindness" keep if inlist(severity,"DVB")
				if "`vision_severity'" == "mod" keep if inlist(severity,"MOD")
				if "`vision_severity'" == "severe" keep if inlist(severity,"SEV")
				
				drop modelable_entity_id
				drop cases

				local reshape_vars = subinstr("`unique_vars'", "cv_best_corrected ", "", 1)
				local reshape_vars = subinstr("`reshape_vars'", "modelable_entity_id ", "", 1)
				reshape wide mean, i(`reshape_vars' sample_size) j(cv_best_corrected) 			
				rename mean1 best_corrected
				rename mean0 presenting
				
				levelsof nid if presenting!=. , local(todrop) 
				foreach nid of local todrop{
					drop if nid==`nid'
				}
				drop presenting
				
				gen logit_bc = logit(best_corrected)
				
				merge m:1 location_id using `super_regions', keep(3) nogen
				levelsof super_region_name, local(supers)
				foreach region of local supers {
					local region_short = substr("`region'", 1, 10)
					local region_short=subinstr("`region_short'", " ", "", .)
					local region_short=subinstr("`region_short'", "-", "", .)
					gen `region_short'=0
					replace `region_short'=1 if super_region_name=="`region'"
				}
				rename year_start year_id
				merge m:1 location_id year_id using `covs', keep(3) nogen
				rename year_id year_start
				
				gen age_group_mid = (age_start + age_end)/2
				
				

				predict logit_pres, xb
				predict pred_se, stdp
				gen pres = invlogit(logit_pres)
				 
				drop if pres==.
				
				gen diagcode_suffix = severity
				append using `xwalked_pres'
				save `xwalked_pres', replace
		
