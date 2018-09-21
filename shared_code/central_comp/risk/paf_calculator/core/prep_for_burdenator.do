// *************************************************************************************************
// *************************************************************************************************
/// Purpose:	Compile PAFs, apply direct dismod PAFs, calculate joint PAFs, apply mediation, and PAFs of 1
//				save as [location_id]_[year_id].dta
// *************************************************************************************************
// *************************************************************************************************

** *************************************************************************************************
** STATA
** *************************************************************************************************

	clear
	set more off
	set maxvar 32000
	pause on
	
** *************************************************************************************************
** WORKSPACE
** *************************************************************************************************

	cap ssc install moremata
	cap ssc install rsource
	
	// get arguments from qsub
	local location_id = "`1'"
	local year_ids "`2'"
	local year_ids : subinstr local year_ids "_" " ", all
	local risk_version = "`3'"
	local mvids = "`4'"
	local mvids : subinstr local mvids ";" ",", all
	local gbd_round_id "`5'"
	local code_dir = "`6'"
	local out_dir = "`7'"
	local n_draws = "`8'"

	// load functions
	adopath + "FILEPATH"
	adopath + "`code_dir'/helpers"
	run "`code_dir'/core/paf_calc_categ.do"

	create_connection_string, server(ADDRESS)
    local epi_string = r(conn_string)

** *************************************************************************************************
** MAKE TEMPFILES FROM SQL QUERIES
** *************************************************************************************************

** MAPPING FILES ----------------------------------------------------------------------------------

	noi di "`mvids'"
	# delim ;
	odbc load, exec("
		SELECT model_version_id, rei_id, rei, rei_name 
		FROM epi.model_version 
		LEFT JOIN epi.modelable_entity_rei using (modelable_entity_id)
		LEFT JOIN shared.rei using (rei_id) 
		WHERE model_version_id in (`mvids')") `epi_string' clear;
	# delim cr
	levelsof rei_id, local(mvid_reids)
	tempfile match
	save `match', replace

	get_cause_metadata, cause_set_id(3) gbd_round_id(`gbd_round_id') clear
	keep acause cause_id
	tempfile causes
	save `causes', replace

	get_ids, table(age_group) clear
	tempfile ages
	save `ages', replace

	get_demographics, gbd_team("epi") gbd_round_id(`gbd_round_id') clear
	local gbd_ages `r(age_group_ids)'

** RISK HIERARCHY ----------------------------------------------------------------------------------

	get_rei_metadata, rei_set_id(2)  gbd_round_id(`gbd_round_id') clear
	levelsof rei_id if most_detailed == 0, local(parents)
	gen children = ""
	foreach parent of local parents {
		levelsof rei_id if parent_id == `parent', local(child) sep(,)
		replace children = "`child'" if rei_id == `parent'
	}
	sort sort_order
	keep level rei_id rei_name parent_id most_detailed children
	drop if children==""
	replace children=subinstr(children,"169,","",.) if rei_id==169
	tempfile h_orig
	save `h_orig', replace
	drop if rei_id==203
	drop if rei_id==169
	tempfile h
	save `h', replace

	use `h', clear
	qui summ level
	local max `r(max)'
	qui summ level
	local min `r(min)'

** BEHAVIORAL ----------------------------------------------------------------------------------
	
	get_rei_metadata, rei_set_id(2)  gbd_round_id(`gbd_round_id') clear
	levelsof rei_id if regexm(path_to_top_parent,"169,203,") & most_detailed == 1, local(child) sep(,)
 	keep if rei_id == 203
	gen all_my_most_detailed_children = "`child'"
	keep rei_id rei_name all_my_most_detailed_children
	tempfile behav_h
	save `behav_h', replace

** ENVIRONMENTAL ----------------------------------------------------------------------------------

	get_rei_metadata, rei_set_id(2)  gbd_round_id(`gbd_round_id') clear
	levelsof rei_id if regexm(path_to_top_parent,"169,202,") & most_detailed == 1, local(child) sep(,)
 	keep if rei_id == 202
	gen all_my_most_detailed_children = "`child'"
	keep rei_id rei_name all_my_most_detailed_children
	tempfile env_h
	save `env_h', replace

** ALL ----------------------------------------------------------------------------------

	get_rei_metadata, rei_set_id(2)  gbd_round_id(`gbd_round_id') clear
	levelsof rei_id if most_detailed == 1, local(child) sep(,)
 	keep if rei_id == 169
	gen all_my_most_detailed_children = "`child'"
	keep rei_id rei_name all_my_most_detailed_children
	replace all_my_most_detailed_children=subinstr(all_my_most_detailed_children,"141,142","105",.)
	replace all_my_most_detailed_children=subinstr(all_my_most_detailed_children,"244,245","134",.)
	tempfile all_h
	save `all_h', replace

** *************************************************************************************************
** READ IN ALL PAF FILES FOR THAT LOCATION/YEAR
** *************************************************************************************************

** READ/APPEND/RENAME/CAP ----------------------------------------------------------------------------------

	local n = 0
	di "`mvid_reids'"
	foreach model of local mvid_reids {
		get_draws, gbd_id_field(rei_id) gbd_id(`model') year_ids(`year_ids') location_ids(`location_id') ///
			sex_ids(1 2) age_group_ids(`gbd_ages') kwargs(draw_type:paf) source(risk) ///
			n_draws(`n_draws') resample("True") gbd_round_id(`gbd_round_id') clear
		noi di c(current_time) + ": rei `model' read"
		gen mort_type = ""
		replace mort_type = "yld" if measure_id == 3
		replace mort_type = "yll" if measure_id == 4
		keep model_version_id location_id year_id sex_id age_group_id mort_type cause_id paf*
		gen modelable_entity_id = .
		order model_version_id location_id year_id sex_id age_group_id mort_type cause_id
		local n = `n' + 1
		tempfile `n'
		save ``n'', replace
	}
	clear
	forvalues i = 1/`n' {
		append using ``i''
	}
	duplicates drop
	merge m:1 model_version_id using `match', assert(3) keep(3) nogen
	
** ADD CHILD BMI  ----------------------------------------------------------------------------------

	noi di c(current_time) + ": add child BMI PAFs"
	preserve	

		risk_info, risk("metab_bmi") draw_type(exposure) gbd_round_id(`gbd_round_id') clear
		keep if regexm(parameter,"cat")
		keep me_id parameter
		rename me_id modelable_entity_id
		tempfile M
		save `M', replace
		get_draws, gbd_id_field(rei_id) gbd_id(108) year_ids(`year_ids') location_ids(`location_id') ///
			sex_ids(1 2) age_group_ids(`gbd_ages') kwargs(draw_type:exposure num_workers:14) source(risk) ///
			gbd_round_id(`gbd_round_id') n_draws(`n_draws') resample("True") clear
		keep if measure_id == 18
		drop parameter
		merge m:1 modelable_entity_id using `M', keep(3) nogen
		forvalues i = 0/`=`n_draws'-1' {
			bysort location_id year_id age_group_id sex_id: egen scalar = total(draw_`i')
			replace draw_`i' = draw_`i' / scalar if scalar > 1
			drop scalar
		}
		bysort location_id age_group_id year_id sex_id: gen level = _N
		levelsof level, local(ref_cat) c
		local ref_cat = `ref_cat' + 1
		drop level
		fastcollapse draw*, type(sum) by(location_id year_id age_group_id sex_id) append flag(dup)
		replace parameter = "cat`ref_cat'" if dup == 1
		forvalues i = 0/`=`n_draws'-1' {
			replace draw_`i' = 1 - draw_`i' if dup == 1
		}
		drop dup
		rename draw_* exp_*
		tempfile exp
		save `exp', replace

		get_draws, gbd_id_field(rei_id) gbd_id(108) year_ids(2005) location_ids(`location_id') ///
			sex_ids(1 2) age_group_ids(`gbd_ages') kwargs(draw_type:rr num_workers:14) source(risk) ///
			n_draws(`n_draws') resample("True") gbd_round_id(`gbd_round_id') clear
		drop if parameter == "per unit"
		replace location_id = `location_id'
		drop year_id
		duplicates drop
		joinby age_group_id parameter sex_id location_id  using `exp'

		levelsof parameter, c
		local L : word count `r(levels)'
		forvalues i = 0/`=`n_draws'-1' {
			qui gen tmrel_`i' = 0
			replace tmrel_`i' = 1 if parameter=="cat`L'"
		}

		cap drop rei_id
		gen rei_id = 108
		calc_paf_categ exp_ rr_ tmrel_ paf_, by(age_group_id rei_id location_id sex_id year_id cause_id modelable_entity_id mortality morbidity)
		forvalues i = 0/`=`n_draws'-1' {
			qui replace paf_`i' = 0 if paf_`i' == .
			qui replace paf_`i' = 1 if paf_`i' > 1
			qui replace paf_`i' = -.999999 if paf_`i' < -1
		}
		mortality_expand mortality morbidity 
		keep cause_id location_id year_id sex_id age_group_id mortality morbidity paf_*
		gen rei_id = 108
		gen modelable_entity_id = .
		gen model_version_id = .
		gen mort_type = "yll"
		replace mort_type = "yld" if morbidity == 1
		drop mortality morbidity
		tempfile childbmi_pafs
		save `childbmi_pafs', replace
		
	restore
	append using `childbmi_pafs'

** KNEE AND HIP SEQUELA PREP ----------------------------------------------------------------------------------

	noi di c(current_time) + ": prep osteo PAFs"
	preserve

		keep if inlist(cause_id,2141,2145)
		joinby cause_id using "FILEPATH/osteo.dta"
		drop cause_id
		tempfile s
		save `s', replace
		odbc load, exec("SELECT sequela_id, cause_id FROM epi.sequela_old_dd341") `epi_string' clear
		merge 1:m sequela_id using `s', keep(3) nogen
		duplicates drop
		tempfile osteo
		save `osteo', replace

		** read in seqeula and scale to COMO
		levelsof sequela_id, local(ss) c
		local x = 0
		foreach s of local ss {
			get_draws, location_ids(`location_id') year_ids(`year_ids') gbd_id(`s') age_group_ids(`gbd_ages') ///
				gbd_id_field(sequela_id) measure_ids(3) gbd_round_id(`gbd_round_id') source(como) status(203) ///
				sex_ids(1 2) n_draws(`n_draws') resample("True") kwargs(num_workers:14) clear
			local x = `x' + 1
			tempfile `x'
			save ``x'', replace
		}
		clear
		forvalues n = 1/`x' {
			append using ``n''
		}
		rename draw_* seq_*
		gen cause_id = 628 // osteoarthritis
		tempfile s
		save `s', replace

		get_draws, location_ids(`location_id') year_ids(`year_ids') gbd_id(628) age_group_ids(`gbd_ages') ///
			gbd_id_field(cause_id) measure_ids(3) gbd_round_id(`gbd_round_id') source(como) status(203) ///
			sex_ids(1 2) n_draws(`n_draws') resample("True") kwargs(num_workers:14) clear
		renpfix draw_ yld_
		tempfile c_osteo
		save `c_osteo', replace

		joinby location_id year_id sex_id age_group_id cause_id using `s'
		** re-scale sequela to COMO
		forvalues i = 0/`=`n_draws'-1' {
			qui bysort age_group_id location_id sex_id year_id: egen total = total(seq_`i')
			qui replace seq_`i' = (seq_`i' * yld_`i')/total
			drop total
		}
		drop yld*
		joinby location_id year_id sex_id age_group_id sequela_id using `osteo'
		forvalues i = 0/`=`n_draws'-1' {
			qui gen double yld_`i' = seq_`i' * paf_`i'
		}
		fastcollapse yld_*, type(sum) by(age_group_id location_id sex_id year_id cause_id rei_id)
		append using `c_osteo'

		gen denominator = .
		replace denominator = (rei_id == .)
		fastfraction yld*, by(location_id year_id sex_id cause_id age_group_id) denominator(denominator) prefix(paf_) 
		keep if rei_id != .
		keep rei_id year_id sex_id cause_id age_group_id paf*
		rename paf_yld_* paf_*
		gen location_id = `location_id'
		gen mort_type = "yld"
		tempfile osteo_pafs
		save `osteo_pafs', replace
		
	restore
	drop if inlist(cause_id,2141,2145)
	append using `osteo_pafs'

** RESHAPE BY YLL/YLD ---------------------------------------------------------------

	keep age_group_id rei_id location_id sex_id year_id cause_id modelable_entity_id mort_type paf*
	rename paf_* paf__*
	unab vars : paf__*
	local vars : subinstr local vars "__" "_@_", all
	duplicates drop
	reshape wide `vars', i(age_group_id rei_id location_id sex_id year_id cause_id modelable_entity_id) j(mort_type) string
	tempfile data
	save `data', replace

** *************************************************************************************************
** SQUEEZE UNSAFE SEX/HIV AND DRUG USE/HIV PAFS SO ADD TO 1
** *************************************************************************************************

	** apply direct DisMod --> PAFs (SqUeezes so adds to 1)
	** squeeze propotion hiv due to other, proportion hiv due to sex, and proportion hiv due to intravenous drug use
	** then apply direct PAF of unsfe sex-HIV and drug use -HIV
	noi di c(current_time) + ": prep direct PAFs for HIV"

	** build up cause id list
	get_cause_metadata, cause_set_id(3) gbd_round_id(`gbd_round_id') clear
	keep if regexm(acause,"hiv_") & level == 4
	keep cause_id
	gen n = 1
	tempfile hiv
	save `hiv', replace

	local MEs 16447 16448 16449 // Proportion HIV
	local x=0
	foreach me of local MEs { 
		get_draws, gbd_id_field(modelable_entity_id) gbd_id(`me') location_ids(`location_id') ///
			year_ids(`year_ids') age_group_ids(`gbd_ages') source(epi) gbd_round_id(`gbd_round_id') ///
			sex_ids(1 2) n_draws(`n_draws') resample("True") clear
		local x = `x' + 1
		tempfile `x'
		save ``x'', replace	
	}
	clear
	forvalues i = 1/`x' {
		append using ``i''
	}
	drop measure_id model_version_id
	** re-scale HIV models
	forvalues i = 0/`=`n_draws'-1' {
		bysort location_id year_id age_group_id sex_id: egen scalar = total(draw_`i')
		replace draw_`i' = draw_`i' / scalar
		rename draw_`i' paf_yll_`i'
		gen double paf_yld_`i' = paf_yll_`i'
		drop scalar
	}

	gen n = 1
	preserve
		** after scaling keep the proportion HIV due to sex
		keep if modelable_entity_id == 16448 // Proportion HIV due to sex, modelable_entity_id
		gen rei_id = 170 // Unsafe sex, rei_id
		replace modelable_entity_id = .
		joinby n using `hiv'
		tempfile HIV
		save `HIV', replace
	restore
	** after scaling keep the proportion HIV due to IV drug use
	keep if modelable_entity_id == 16447 // Proportion HIV due to intravenous drug use, modelable_entity_id
	gen rei_id = 138 // Drug use dependence and blood borne viruses, rei_id
	replace modelable_entity_id = .
	joinby n using `hiv'
	duplicates drop
	tempfile IV
	save `IV', replace

** *************************************************************************************************
** FRACTION OF HOMICIDE AGAINST FEMALES DUE TO IPV
** *************************************************************************************************

	** inj_homicide_gun, inj_homicide_knife, inj_homicide_other
	get_cause_metadata, cause_set_id(3) gbd_round_id(`gbd_round_id') clear
	keep if regexm(acause,"inj_homicide_") & level == 4
	keep cause_id
	gen n = 1
	tempfile INJ
	save `INJ', replace

	// Fraction of homicide against females due to IPV
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(16435) location_ids(`location_id') ///
		year_ids(`year_ids') sex_ids(2) age_group_ids(`gbd_ages') source(epi) gbd_round_id(`gbd_round_id') ///
		n_draws(`n_draws') resample("True") clear
	drop modelable_entity_id model_version_id measure_id
	rename draw_* paf_yll_*
	forvalues i = 0/`=`n_draws'-1' {
		gen double paf_yld_`i' = paf_yll_`i'
	}
	gen n = 1
	joinby n using `INJ'
	gen rei_id = 168 // Intimate partner violence (direct PAF approach), rei_id
	tempfile IPV
	save `IPV', replace

** *************************************************************************************************
** FPG CATEGORIAGAL 
** *************************************************************************************************

	** diabetes prevalence and FPG - we want to pull the dismod results
	noi di c(current_time) + ": prep diabetes - TB"
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(16441) location_ids(`location_id') year_ids(`year_ids') ///
		measure_ids(5) age_group_ids(`gbd_ages') source(epi) gbd_round_id(`gbd_round_id') ///
		sex_ids(1 2) n_draws(`n_draws') resample("True") clear
	gen parameter="cat1"
	expand 2, gen(dup)
	replace parameter = "cat2" if dup == 1
	forvalues i = 0/`=`n_draws'-1' {
		qui replace draw_`i' = 1 - draw_`i' if dup == 1
	}
	drop dup modelable_entity_id model_version_id measure_id
	renpfix draw_ exp_
	tempfile exp
	save `exp', replace

	get_draws, gbd_id_field(rei_id) gbd_id(142) location_ids(`location_id') year_ids(2005) ///
		age_group_ids(`gbd_ages') kwargs(draw_type:rr num_workers:14) source(risk) gbd_round_id(`gbd_round_id') ///
		sex_ids(1 2) n_draws(`n_draws') resample("True") clear
	replace location_id = `location_id'
	drop year_id
	duplicates drop
	joinby age_group_id parameter sex_id location_id  using `exp'

	** generate TMREL
	levelsof parameter, c
	local L : word count `r(levels)'
	forvalues i = 0/`=`n_draws'-1' {
		qui gen tmrel_`i' = 0
		qui replace tmrel_`i' = 1 if parameter=="cat`L'"
	}

	gen rei_id = 142 // FPG categorical
	calc_paf_categ exp_ rr_ tmrel_ paf_, by(age_group_id rei_id location_id sex_id year_id cause_id mortality morbidity)

	** expand mortality and morbidity
	mortality_expand mortality morbidity 
	gen mort_type = ""
	replace mort_type="yll" if mortality==1 & morbidity==0
	replace mort_type="yld" if mortality==0 & morbidity==1
	drop mortality morbidity

	keep age_group_id rei_id sex_id cause_id mort_type year_id paf*
	gen location_id = `location_id'
	gen modelable_entity_id = .
	rename paf_* paf__*
	unab vars : paf__*
	local vars : subinstr local vars "__" "_@_", all
	reshape wide `vars', i(age_group_id rei_id location_id sex_id year_id cause_id modelable_entity_id) j(mort_type) string

** *************************************************************************************************
** APPEND ALL, HIV,  IV, IPV, AND FPG 
** *************************************************************************************************

	append using `HIV'
	capture append using `IV'
	append using `IPV'
	append using `data'

	keep modelable_entity_id rei_id cause_id location_id year_id age_group_id sex_id paf*
	order modelable_entity_id rei_id cause_id location_id year_id age_group_id sex_id 

	** make sure we keep most detailed ages not any dismod aggregates that might come through
	keep if inlist(age_group_id,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235)

	** Make sure only female IPV comes through, and ipv should only have ages 15+
	drop if inlist(rei_id,135,167,201,168) & sex_id == 1 
	drop if inlist(rei_id,135,167,201,168) & !inlist(age_group_id,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235)
	drop if inlist(rei_id,170) & !inlist(age_group_id,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235)

	** drop latent tb
	drop if cause_id == 954 

	save `data', replace

** *************************************************************************************************
** ADJUST SHS AND APPEND 
** *************************************************************************************************

	noi di c(current_time) + ": adjust SHS for smoking"
	preserve
		keep if rei_id==100 // tempfile SHS
		tempfile shs
		save `shs', replace
	restore

	** aggregate smoking (smoking_direct (99) = smoking_direct_sir (165) + smoking_direct_prev (166)) so we can adjust SHS
	keep if inlist(rei_id,165,166)
	foreach var of varlist paf* {
		qui replace `var' = log(1 - `var')
	}
	fastcollapse paf*, type(sum) by(age_group_id location_id sex_id year_id cause_id modelable_entity_id)
	foreach var of varlist paf* {
		qui replace `var' = 1 - exp(`var')
		qui replace `var' = 1 if `var' == . 
	}
	tempfile smoking_direct
	save `smoking_direct', replace

	noi di c(current_time) + ": pull smoking exposure" // Smoking Prevalence
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(8941) location_ids(`location_id') year_ids(`year_ids') ///
		age_group_ids(`gbd_ages') source(epi) gbd_round_id(`gbd_round_id') ///
		sex_ids(1 2) n_draws(`n_draws') resample("True") clear
	renpfix draw_ smk_prev_
	tempfile smoking_prev
	save `smoking_prev', replace

	use `smoking_direct', clear // Smoking PAF
	renpfix paf_yll_ smk_paf_yll_
	renpfix paf_yld_ smk_paf_yld_
	merge 1:1 age_group_id cause_id year_id sex_id using `shs', keep(2 3) nogen
	merge m:1 age_group_id year_id sex_id using `smoking_prev', keep(2 3) nogen
	
	** Replace missing with 0s (ages that are too young for smoking burden)
	forvalues i = 0/`=`n_draws'-1' {
		qui replace smk_paf_yll_`i' = 0 if smk_paf_yll_`i' == .
		qui replace smk_paf_yld_`i' = 0 if smk_paf_yld_`i' == .
		qui replace smk_prev_`i' = 0 if smk_prev_`i' == .
	}
	
	** Adjust PAF
	** SHS = SHS PAF among non-smokers * (non-smoker population) * (burden not attributble to smoking)
	forvalues i=0/`=`n_draws'-1' {
		qui replace paf_yll_`i' = paf_yll_`i' * ((1 - smk_paf_yll_`i') * (1 - smk_prev_`i'))	
		qui replace paf_yld_`i' = paf_yld_`i' * ((1 - smk_paf_yld_`i') * (1 - smk_prev_`i'))
	}
	drop smk_paf_* smk_prev_*
	keep if rei_id==100 // keep just SHS
	cap levelsof age_group_id, local(ages) sep(,) c
	cap levelsof cause_id, local(causes) sep(,) c
	tempfile shs_adj
	save `shs_adj', replace

	** append to full data set
	use `data', clear
	cap drop if rei_id == 100 & inlist(cause_id,`causes') & inlist(age_group_id,`ages') // drop unadjusted SHS PAF
	append using `shs_adj'

** *************************************************************************************************
** PREP FOR PAFS OF 1
** *************************************************************************************************

	** Drop PAFs of 1 if they came through before joint calculation
	merge m:1 rei_id age_group_id sex_id cause_id using "FILEPATH/all_attrib.dta", keep(1) nogen
	foreach x of varlist paf* {
	  qui replace `x' = 0 if missing(`x') 
	}
	keep modelable_entity_id rei_id cause_id location_id year_id age_group_id sex_id paf*
	foreach var of varlist paf* {
		qui replace `var' = 0 if `var' == .
		qui replace `var' = 1 if `var' > 1
		qui replace `var' = -.999999 if `var' < -1
	}
	** prep PAFs of 1 
	noi di c(current_time) + ": agg PAFs of 1"

	use "FILEPATH/all_attrib.dta", clear
	gen location_id = `location_id'
	tempfile A
	save `A', replace

** *************************************************************************************************
** JOINT PAFS AND MEDIATION
** *************************************************************************************************

	noi di c(current_time) + ": begin joint PAF calculation"

** JOINT PAF CALC ----------------------------------------------------------------------------------

	foreach x of numlist `max'(-1)`min' {
		use if level==`x' using `h', clear
		count
		if `r(N)'==0 continue
		forvalues i=1/`r(N)' {
			use if level==`x' using `h', clear
			levelsof children in `i', local(keep) c
			levelsof rei_id in `i', local(parent) c
			use if inlist(rei_id,`keep') using `data', clear

			** mediate
			if `parent'==104 {
				merge m:1 rei_id cause_id using "FILEPATH/mediation_metab.dta", keep(1 3) assert(1 3)
			}

			cap confirm var mediate_0
			if _rc==0 {
				forvalues i = 0/`=`n_draws'-1' {
					qui replace mediate_`i' = .99999999999999999 if mediate_`i'==1
					qui replace mediate_`i' = .00000000000000001 if mediate_`i'==0
					qui replace paf_yll_`i' = paf_yll_`i' * mediate_`i' if _merge==3
					qui replace paf_yld_`i' = paf_yld_`i' * mediate_`i' if _merge==3
				}
				drop _merge mediate*
			}
			** aggregate
			foreach var of varlist paf_yll* paf_yld* {
				qui replace `var' = log(1 - `var')
			}

			fastcollapse paf_yld* paf_yll*, type(sum) by(age_group_id location_id sex_id year_id cause_id)
			** Exponentiate and complement.
			foreach var of varlist paf* {
				qui replace `var' = 1 - exp(`var')
				qui replace `var' = 1 if `var' == . 
			}

			gen rei_id=`parent'

			append using `data'
			save `data', replace
		}

	}

** BEHAV ----------------------------------------------------------------------------------

	noi di c(current_time) + ": begin behavioral calculation"

	use `behav_h', clear
	replace all_my_most_detailed_children = subinstr(all_my_most_detailed_children,","," ",.)
	levelsof all_my_most_detailed_children, local (risks) c
	levelsof rei_id, local(PARENT) c

	local DF = 0
	foreach risk of local risks {
		use if rei_id==`risk' using `data', clear
		local DF = `DF' + 1
		tempfile `DF'
		save ``DF'', replace
	}
	clear
	forvalues i = 1/`DF' {
		qui append using ``i''
	}
	merge m:1 rei_id cause_id using "FILEPATH/mediation_behav.dta", keep(1 3) assert(1 3)
	forvalues i = 0/`=`n_draws'-1' {
		qui replace mediate_`i' = .99999999999999999 if mediate_`i'==1
		qui replace mediate_`i' = .00000000000000001 if mediate_`i'==0
		qui replace paf_yll_`i' = paf_yll_`i' * mediate_`i' if _merge==3
		qui replace paf_yld_`i' = paf_yld_`i' * mediate_`i' if _merge==3
	}
	drop _merge mediate*

	** aggregate
	foreach var of varlist paf_yll* paf_yld* {
		qui replace `var' = log(1 - `var')
	}
	fastcollapse paf_yld* paf_yll*, type(sum) by(age_group_id location_id sex_id year_id cause_id)
	foreach var of varlist paf* {
		qui replace `var' = 1 - exp(`var')
		qui replace `var' = 1 if `var' == . 
	}

	gen rei_id=`PARENT'
	tempfile B
	save `B', replace

** ALL ----------------------------------------------------------------------------------

	noi di c(current_time) + ": begin all calculation"
	use `all_h', clear
	replace all_my_most_detailed_children = subinstr(all_my_most_detailed_children,","," ",.)
	levelsof all_my_most_detailed_children, local (risks) c
	levelsof rei_id, local(PARENT) c

	local DF = 0
	foreach risk of local risks {
		use if rei_id==`risk' using `data', clear
		local DF = `DF' + 1
		tempfile `DF'
		save ``DF'', replace
	}
	clear
	forvalues i = 1/`DF' {
		qui append using ``i''
	}
	merge m:1 rei_id cause_id using "FILEPATH/mediation_all.dta", keep(1 3) assert(1 3)
	forvalues i = 0/`=`n_draws'-1' {
		qui replace mediate_`i' = .99999999999999999 if mediate_`i'==1
		qui replace mediate_`i' = .00000000000000001 if mediate_`i'==0
		qui replace paf_yll_`i' = paf_yll_`i' * mediate_`i' if _merge==3
		qui replace paf_yld_`i' = paf_yld_`i' * mediate_`i' if _merge==3
	}
	drop _merge mediate*

	** aggregate
	foreach var of varlist paf* {
		qui replace `var' = log(1 - `var')
	}
	fastcollapse paf_yld* paf_yll*, type(sum) by(age_group_id location_id sex_id year_id cause_id)
	foreach var of varlist paf* {
		qui replace `var' = 1 - exp(`var')
		qui replace `var' = 1 if `var' == . 
	}
	gen rei_id=`PARENT'

** APPEND EVERYTHING ----------------------------------------------------------------------------------

	append using `B'
	append using `data'
	save `data', replace
	noi di c(current_time) + ": joint PAF calculation complete"

** *************************************************************************************************
** ADD PAFS OF 1
** *************************************************************************************************

	** append PAFs of 1
	noi di c(current_time) + ": append PAFs of 1"

	** Make sure PAFs of 1 are also set for aggregates
	merge m:1 rei_id age_group_id sex_id cause_id using `A', keep(1) nogen
	append using `A'
	foreach var of varlist paf* {
		qui replace `var' = 0 if `var' == .
		qui replace `var' = 1 if `var' > 1
		qui replace `var' = -.999999 if `var' < -1
	}

	keep rei_id age_group_id sex_id cause_id year_id paf*
	cap drop paf_*_m* paf_*_l* paf_*_u*
	gen location_id = `location_id'
	drop if cause_id==.
	order location_id year_id rei_id cause_id sex_id age_group_id
	foreach yid of local year_ids {
		preserve
			keep if year_id == `yid' | year_id == .
			replace year_id = `yid'
			save "FILEPATH/pafs/`risk_version'/`location_id'_`yid'.dta", replace
		restore
	}

	noi di c(current_time) + ": SAVED!"

// end
