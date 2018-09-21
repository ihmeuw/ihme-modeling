

*** BOILERPLATE ***
	clear all
	set more off

	if c(os) == "Unix" {
		local j "FILENAME"
		}
	else if c(os) == "Windows" {
		local j "FILENAME"
		}

	run FILENAME/build_cod_dataset.ado
	run FILENAME/select_xforms.ado
	run FILENAME/run_best_model.ado
	run FILENAME/process_predictions.ado


*** CREATE THE DATASET ***
	local cause_id 351
	local covariate_ids 57 118 142 160 1099 1110
	local get_model_results gbd_team(epi) model_version_id(95817) measure_id(5)
	local saveto FILENAME/schisto.dta
	local minAge 5
	
	// use this if want new covariates or if there's been a data refresh
	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') get_model_results(`get_model_results') saveto(`saveto', replace) clear


	adopath + "FILENAME"
	get_location_metadata, location_set_id(8) clear
	preserve
	keep if parent_id == 196
	levelsof location_id, local(sa_subs)
	restore
	keep if parent_id == 135
	levelsof location_id, local(brazil_subs)
	use FILENAME/schisto.dta, clear

*** RUN BEST MODEL AS MIXED EFFECTS ***


	generate lnPrev_22 = ln(prevalence_95817_22 + 0.000001)
    generate logitWater = logit(water_prop)
    generate logLDI = ln(LDI_pc)
    gen lnPrev = ln(prevalence_95817 + 0.000001)

    ** generate cases
    gen cases = prevalence_95817 * population

   	forvalues i = 5/15 {
   		preserve
	   	keep lnPrev lnPrev_22 location_id age_group_id sex_id year_id
	   	replace year_id = year_id + `i'
	   	drop if year_id > 2016
	   	local expand = `i' + 1
	   	expand `expand' if year_id == 1980 + `i'
	   	bysort year_id location_id sex_id age_group_id: replace year_id = year_id - _n + 1 if year_id == 1980 + `i'
	   	rename lnPrev lnPrev_lag`i'
	   	rename lnPrev_22 lnPrev_22_lag`i'
	   	tempfile lag`i'
	   	save `lag`i'', replace
	   	restore
	   	merge m:m year_id location_id sex_id age_group_id using `lag`i'', nogen keep(1 3)
   	}

    gen south_africa = 0
    foreach loc of local sa_subs {
    	replace south_africa = 1 if location_id == `loc'
    }

	preserve
	import delimited "FILENAME/schisto_gr_map.csv", clear
	keep ihme_loc_id location_name location_id v4
	rename v4 endemicity
	tempfile endemic
	save `endemic', replace
	restore
	** merge on to current dataset
	merge m:1 location_id using `endemic', keepusing(endemic) nogen
	replace endemic = 0 if endemic == .
	gen brazil_sub = 0
	foreach loc of local brazil_subs {
		replace brazil_sub = 1 if location_id == `loc' & endemic == 1
	}


run_best_model nbreg study_deaths lnPrev_lag5 south_africa brazil_sub i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult
test [lndelta]_cons = 1
test [lnalpha]_cons = 1


	
*** SUBMIT JOBS TO CREATE DRAWS AND SAVE RESULTS ***
	process_predictions `cause_id', link(ln) random(no) min_age(5) multiplier(envelope) endemic(endemic) description("fixed effects model with south africa and brazil sub indicator")

