// Purpose: Calculate Region-Duration-CD4-Sex-Age-specific mortality estimates for HIV patients on ART

clear all
set more off

** Set code directory for Git
global user "`c(username)'"

if (c(os)=="Unix") {
	global root FILEPATH
	global code_dir FILEPATH
}

if (c(os)=="Windows") {
	global root FILEPATH
	global code_dir FILEPATH
}

**** SET UP ****
	cd "$code_dir"
	global dismod_dir FILEPATH
	global dismod_type = "20150319" // Options: standard, or an alternate date (20150319) that corresponds to the DisMod version you want
	global remove = 1 //remove old outputs
	
	global km_pre = 1				// Prep stages for all KM data
	global km_full_brad = 1		// Run Bradmod for the full set of data
	global hr_pre = 1				// Run sex meta-analysis and prep age data for BradMod
	global hr_brad = 1			// Run BradMod to synthesize age hazard ratios
	global hr_post = 1				// Reference hazard ratios to region-specific mean ages and format
	global study_level = 1			// split and subtract background mortality at study level and prep result for bradmod
	global study_level_final = 1    //compile study level bradmod and save for spectrum
	
	// Archive toggles (do we want to archive this run, and if so, how should we save?
	global archive = 1
	local date: display %td_CCYY_NN_DD date(c(current_date), "DMY")
	global date = subinstr(trim("`date'"), " " , "-", .)
	local comment = ""
	if "`comment'" != "" global date = "$date_`comment'"
	global bradmod_archive = "data_in.csv change_post.csv data_omega.csv data_pred.csv data_tmp.csv effect_tmp.csv info_out.csv model_draw2.csv model_out.csv plain_tmp.csv plot_post.pdf rate_omega.csv rate_tmp.csv sample_out.csv sample_split.csv sample_stat.csv stat_post.pdf value_tmp.csv"
	global bradmod_remove = subinstr("$bradmod_archive","data_in.csv","",.)

****Remove old outputs. Can't do this to some Dismod excels since it uses "updates" the last one*** 
if $remove == 1 { 
	if $km_pre == 1 {  
		local prepd_dir FILEPATH 
		local pct_male_dir FILEPATH
		local tmp_dir FILEPATH
		! rm "`prepd_dir'/HIV_extract_HR_2015.xlsx" 
		! rm "`prepd_dir'/HIV_extract_KM_2015.xlsx" 
		! rm "`pct_male_dir'/pct_male_med_age.csv"
		! rm "`tmp_dir'/KM_forsplit.dta"
	} 
	if $hr_pre == 1 { 
		local hr_dir FILEPATH
		! rm "`hr_dir'/sex_hazard_ratios.dta" 
		! rm "`hr_dir'/sex_hazard_ratios.csv" 
		foreach sup in ssa high other{ 
			*the code below removes the dismod output but not the input
			! rm "$dismod_dir/HIV_HR_`sup'/data_in.csv"
		} 
	}
	if $hr_post == 1 { 
		local hr_dir FILEPATH
		! rm "`hr_dir'/age_hazard_ratios.dta" 
		! rm "`hr_dir'/age_hazard_ratios.csv" 	
	} 
	if $study_level == 1 {  
		local br_dir FILEPATH 
		*CONTAINS THE DISMOD ODE MODEL FOLDER NAMES I.E HIV_KM_1_0_6 ECT
		use FILEPATH, clear 
		levelsof brad, local(runtypes)
		! rm "`br_dir'/split_km.dta" 
		! rm "`br_dir'/km_rates.dta" 
		! rm "`br_dir'/tmp_conditional.dta"  
		foreach folder in `runtypes'{ 
			! rm "$dismod_dir/`folder'/data_in.csv"
		}
	} 
	if $study_level_final == 1 { 
		local adj_dir FILEPATH
		! rm "`adj_dir'/mortality_probs_new_model.dta" 
	}
} 



**** KAPLAN MEIER ****
// We take kaplan meier data on mortality and loss to follow up that has been extracted from literature (region-duration-sex-age-CD4-specific). These are collected as cumulative probabilities from initiation on therapy. We first standardize the extractions and apply a LTFU correction factor. Then we sex-age split the data,if not sex-age specific, at the study level using HRs from the meta analysis below and run the data through dismod (new dismod, or 'bradmod'), "tricking" dismod to treat CD4 categories as ages.  From  dismod we obtain conditional mortality probabilities for the sex-age-duration-periods used by Spectrum.

	// Prepare KM data for dismod; cleaning the data - applying exclusion criteria; applying a correction factor for differential mortality in LTFU. This is prepping the data for splitting and some cleaning that will go into running the full model
	** everything entered as 0-6 0-12 0-24 cumulative probabilities
	if $km_pre == 1 {
		do $code_dir/00_prep_data.do
		do $code_dir/01_prep_KM.do
		// note: this also calls on the file 
		** 01a_adjust_survival_for_ltfu
		if $archive == 1 {
			// Archive pct male_med
			local pct_male_dir FILEPATH
			! cp "`pct_male_dir'/pct_male_med_age.csv" "`pct_male_dir'/archive/pct_male_med_age_$date.csv"
			
			// Archive conditional and cumulative tmp probabilities and prep km for split 
			local tmp_dir FILEPATH
			! cp "`tmp_dir'/KM_forsplit.dta" "`tmp_dir'/archive/KM_forsplit_$date.dta" 
		}
	}

	**** HAZARD RATIOS ****
// Since we want our final output to be age and sex specific as well, we use hazard ratios (Also extracted from literature) to adjust our region-duration-cd4-specific mortality estimates. First we use the stata command 'metan' to generate sex hazard ratios. Then we use 'bradmod' to synthesize the age hazard ratios, and output the hazard ratios for spectrum's age groups, referenced to the region-specific mean age from the studies.

	if $hr_pre == 1 {
		// Run sex meta analysis
		do $code_dir/05_sex_meta_analysis.DO .
		
		// Prep age data for bradmod
		do $code_dir/06_age_prep_bradmod
		
		if $archive == 1 {
			// Archive sex HRs
			local hr_dir FILEPATH
			! cp "`hr_dir'/sex_hazard_ratios.dta" "`hr_dir'/archive/sex_hazard_ratios_$date.dta"
			! cp "`hr_dir'/sex_hazard_ratios.csv" "`hr_dir'/archive/sex_hazard_ratios_$date.csv"
		}
	}
	
	
	if $hr_brad == 1 {
		// The previous code automatically updates the 'data_in' file, but you may also need to manually update effect_in (random effects), plain_in (smoothing), rate_in (mesh points), value_in (# draws), pred_in (specify intervals to predict)
	
		local runtypes "HIV_HR_ssa HIV_HR_high HIV_HR_other"
		
		// Remove results from previous run
			foreach folder in `runtypes' {
				foreach fff of global bradmod_remove {
					// Remove the results from the previous run
					cap rm "$dismod_dir/`folder'/`fff'"
				}
			}
		
		// Launch BradMod jobs
			cd "$code_dir"
			local job_count = 0
			foreach runtype in `runtypes' {
				! qsub -N brad_`runtype' -e /share/temp/sgeoutput/$user/errors -o /share/temp/sgeoutput/$user/output "run_all.sh" "$code_dir/launch_dismod_ode.do" "`runtype' $dismod_type"
				local ++job_count
			}
		
		// Check that BradMod has finished before proceeding
			local i = 0
			while `i' == 0 {
				local counter = 0
				foreach runtype in `runtypes' {
					capture confirm file "$dismod_dir/`runtype'/model_draw2.csv"
					if !_rc local ++counter
				}
				
				di "Checking `c(current_time)': `counter' of `job_count' ID Best jobs finished"
				if (`counter' == `job_count') local i = `i' + 1
				else sleep 30000
			}
			
		sleep 30000 // Just to give the hr jobs more time to write -- had issues with draw files being empty
			
		// Archive, if desired
			if $archive == 1 {
				foreach runtype in `runtypes' {
					cap mkdir "$dismod_dir/`runtype'/archive_$date"
					foreach fff of global bradmod_archive {
						! cp "$dismod_dir/`runtype'/`fff'" "$dismod_dir/`runtype'/archive_$date/`fff'"
					}
				}
			}
	}
		
		
	// With dismod output, reference hazard ratios to region-specific mean ages and format data for analysis
	if $hr_post == 1 {
		do $code_dir/07_age_seperate_post_bradmod
		
		if $archive == 1 {
			local hr_dir FILEPATH
			! cp "`hr_dir'/age_hazard_ratios.dta" "`hr_dir'/archive/age_hazard_ratios_$date.dta"
			! cp "`hr_dir'/age_hazard_ratios.csv" "`hr_dir'/archive/age_hazard_ratios_$date.csv"			
		}
	}
	
	// Split data at study level, if needed, using HRs calculated above and subtract background mort, also will prep the csvs need for bradmod run
	if $study_level == 1 {
		do $code_dir/01b_split_km 
		do $code_dir/01c_subtractback 
		do $code_dir/01d_prep_km_forBramod 
		do $code_dir/03_prep_full_KM
		
		if $archive == 1 {
			local br_dir FILEPATH
			! cp "`br_dir'/split_km.dta" "`br_dir'/archive/split_km_$date.dta"
			! cp "`br_dir'/km_rates.dta" "`br_dir'/archive/km_rates_$date.dta" 
			! cp "`br_dir'/tmp_conditional.dta" "`br_dir'/archive/tmp_conditional_$date.dta"
		}
	}
	
	
	
	// Run Bradmod now for all the major regions
	if $km_full_brad == 1 {
		*CONTAINS THE DISMOD ODE MODEL FOLDER NAMES I.E HIV_KM_1_0_6 ECT
		use FILEPATH, clear 
		levelsof brad, local(runtypes)
		
		// Remove results from previous run
			foreach folder in `runtypes' {
				foreach fff of global bradmod_remove {
					// Remove the results from the previous run
					cap rm "$dismod_dir/`folder'/`fff'"
				}
			}
		
		// Launch BradMod jobs
			cd "$code_dir"
			local job_count = 0
			foreach runtype in `runtypes' {
				! qsub -N brad_`runtype' -e /share/temp/sgeoutput/$user/errors -o /share/temp/sgeoutput/$user/output "run_all.sh" "$code_dir/launch_dismod_ode.do" "`runtype' $dismod_type"
				local ++job_count
			}
		
		// Check that BradMod has finished before proceeding
			local i = 0
			while `i' == 0 {
				local counter = 0
				foreach runtype in `runtypes' {
					capture confirm file "$dismod_dir/`runtype'/model_draw2.csv"
					if !_rc local ++counter
				}
				
				di "Checking `c(current_time)': `counter' of `job_count' Full BradMod jobs finished"
				if (`counter' == `job_count') local i = `i' + 1
				else sleep 30000
			}
			
			sleep 30000 // Just to give the BradMod jobs more time to write -- had issues with draw files being empty
			
		// Archive, if desired
			if $archive == 1 {
				foreach runtype in `runtypes' {
					cap mkdir "$dismod_dir/`runtype'/archive_$date"
					foreach fff of global bradmod_archive {
						! cp "$dismod_dir/`runtype'/`fff'" "$dismod_dir/`runtype'/archive_$date/`fff'"
					}
				}
			}
			
	}
	
// final analysis for the study level model
	if $study_level_final == 1 {

		// Save in spectrum input format
		// NOTE: Archiving of draws happens in this code file, for ease of use
		do $code_dir/10b2_postbrad_savespec
		
		if $archive == 1 {
			local adj_dir FILEPATH
			
			! cp "`adj_dir'/mortality_probs_new_model.dta" "`adj_dir'/archive_adj_mort/mortality_probs_new_model_$date.dta"
			
			
		}
	}	
