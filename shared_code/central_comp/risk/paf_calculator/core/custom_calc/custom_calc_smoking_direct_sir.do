// *************************************************************************************************
// *************************************************************************************************
// Purpose:	
	** SIR = (C-N) / (S-N*)
	** C = lung cancer mortality rate by country in current pop
	** N = lung cancer mortality rate of never-smokers in current pop  (use CPS II for all, except China & Asia Pacific high income)
	** S = lung cancer mortality rate of life-long smokers in reference pop
	** N*= lung cancer mortality rate for never-smokers in reference pop 
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
	set seed 370566

	// pull arguments from qsub
	local risk = "`1'"
	local rei_id = "`2'"
	local location_id = "`3'"
	local sex_id = "`4'"
	local year_ids : subinstr local 5 "_" " ", all
	local gbd_round_id "`6'"
	local n_draws "`7'"
	local code_dir = "`8'"
	local out_dir = "`9'"
	
	adopath + "FILEPATH"
	adopath + "`code_dir'/helpers"
	run "`code_dir'/core/paf_calc_categ.do"

	create_connection_string, server(ADDRESS)
    local epi_string = r(conn_string)

  	get_demographics, gbd_team("epi") gbd_round_id(`gbd_round_id') clear
	local gbd_ages `r(age_group_ids)'

** *************************************************************************************************
** 
** *************************************************************************************************

** PULL EXPOSURES  ----------------------------------------------------------------------------------

	noi di c(current_time) + ": pulling pop"
    get_population, year_id("`year_ids'") location_id("`location_id'") sex_id("`sex_id'") ///
     	age_group_id("`gbd_ages'") gbd_round_id("`gbd_round_id'")
    drop process_version_map_id
	tempfile pops 
	save `pops', replace

	** Calculate death rate
	noi di c(current_time) + ": pulling lung cancer deaths"
	get_draws, location_ids(`location_id') year_ids(`year_ids') gbd_id(426) age_group_ids(`gbd_ages') ///
		gbd_id_field(cause_id) measure_ids(1) gbd_round_id(`gbd_round_id') source(codcorrect) ///
		sex_ids(`sex_id') kwargs(num_workers:10) n_draws(`n_draws') resample("True") status(66) clear
	merge 1:1 location_id year_id age_group_id sex_id using `pops', assert(2 3) keep(3) nogen
	forvalues i = 0/`=`n_draws'-1' {
		gen double c_`i' = draw_`i' / population * 100000
		drop draw_`i'
	}
	tempfile c_deaths
	save `c_deaths'
			
	** Bring in smoker deaths (s)
	noi di c(current_time) + ": pull in S"
	use "FILEPATH/impact_ratio_s.dta", clear
	rename sex sex_id
	drop age
	tempfile s_deaths
	save `s_deaths'
				
	** Bring in background (non-smoking) deaths (n and n*)
	noi di c(current_time) + ": pull in N"
	use "FILEPATH/impact_ratio_n.dta", clear
	rename sex sex_id
	drop age
	** China is location_id (6) subnationals: children
	if inlist(`location_id',354,361,491,492,493,494,495,496) | inlist(`location_id',497,498,499,500,501,502,503,504,505) | ///
		inlist(`location_id',506,507,508,509,510,511,512,513,514) | inlist(`location_id',515,516,517,518,519,520,521) {
		keep if whereami_id=="CHN"
		gen location_id = `location_id'
	}
	** R1 is High-Income Asia Pacific (65)
	** locations 66,67,68,69
	** Japan (67) subnationals: children
	else if inlist(`location_id',66,68,69) | inlist(`location_id',35424,35425,35426,35427,35428,35429,35430) | ///
		inlist(`location_id',35431,35432,35433,35434,35435,35436,35437,35438) | inlist(`location_id',35439,35440, ///
		35441,35442,35443,35444,35445) | inlist(`location_id',35446,35447,35448,35449,35450,35451,35452) | ///
		inlist(`location_id',35453,35454,35455,35456,35457,35458,35459) | inlist(`location_id',35460,35461, ///
		35462,35463,35464,35465) | inlist(`location_id',35466,35467,35468,35469,35470) {
		keep if whereami_id=="R1"
		gen location_id = `location_id'
	}		
	else {
		keep if whereami_id=="G"
		gen location_id = `location_id'
	}
	tempfile n_deaths
	save `n_deaths'
			
	** Merge together and calculate impact ratio
	noi di c(current_time) + ": calculate impact ratio"
	clear
	use `c_deaths'
	merge m:1 age_group_id sex_id using `s_deaths', keep(3) nogen
	merge m:1 age_group_id sex_id using `n_deaths', keep(3) nogen
	forvalues i = `n_draws'/999 {
		cap drop c_`i' s_`i' n_`i' n_star_`i'
	}

	forvalues draw = 0/`=`n_draws'-1' {
		qui gen double exp_`draw' = ((c_`draw' - n_`draw') / (s_`draw' - nstar_`draw')) * (nstar_`draw' / n_`draw')
		** Cap SIR at values observed in studies.
		qui replace exp_`draw' = 0 if exp_`draw' < 0
		qui replace exp_`draw' = 1 if exp_`draw' > 1
	}

	cap drop risk
	gen risk = "`risk'"
	expand 2, gen(dup)
	gen parameter = "cat1" if dup == 0
	replace parameter = "cat2" if dup == 1
	forvalues i = 0/`=`n_draws'-1' {
		qui replace exp_`i' = 1 - exp_`i' if dup == 1
	}
	drop dup
	tempfile exp
	save `exp', replace

** PULL RRS  ----------------------------------------------------------------------------------

	noi di c(current_time) + ": get relative risk draws"
	get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') location_ids(`location_id') year_ids(`year_ids') ///		
		status(best) kwargs(draw_type:rr num_workers:10) gbd_round_id(`gbd_round_id') source(risk) ///
		sex_ids(`sex_id') n_draws(`n_draws') resample("True") clear
	drop year_id 
	duplicates drop
	replace location_id = `location_id'
	noi di c(current_time) + ": relative risk draws read"
	joinby age_group_id parameter sex_id using `exp'

** GENERATE TMREL ----------------------------------------------------------------------------------

	bysort age_group_id year_id sex_id cause_id mortality morbidity: gen level = _N
	levelsof level, local(tmrel_param) c
	drop level
	forvalues i = 0/`=`n_draws'-1' {
		qui gen tmrel_`i' = 0
		qui replace tmrel_`i' = 1 if parameter=="cat`tmrel_param'"
	}

** PAF CALC ----------------------------------------------------------------------------------

	noi di c(current_time) + ": calc PAFs"
	cap drop rei_id
	gen rei_id = `rei_id'
	calc_paf_categ exp_ rr_ tmrel_ paf_, by(age_group_id rei_id location_id sex_id year_id cause_id modelable_entity_id mortality morbidity)
	forvalues i = 0/`=`n_draws'-1' {
		qui replace paf_`i' = 0 if paf_`i' == .
		qui replace paf_`i' = 1 if paf_`i' > 1
		qui replace paf_`i' = -.999999 if paf_`i' < -1
	}
	noi di c(current_time) + ": PAF calc complete"

	** expand mortlality and morbidity
	mortality_expand mortality morbidity 
	levelsof mortality, local(morts)

	keep cause_id location_id year_id sex_id age_group_id mortality morbidity paf_*
	gen rei_id = `rei_id'
	gen modelable_entity_id = .
	order modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id mortality morbidity
	noi di c(current_time) + ": saving PAFs"
	foreach mmm of local morts {
		if `mmm' == 1 local mmm_string = "yll"
		else local mmm_string = "yld"
		outsheet modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id paf_* using ///
			 "`out_dir'/`risk'/paf_`mmm_string'_`location_id'_`sex_id'.csv" ///
			  if mortality == `mmm', comma replace
		no di "saved: `out_dir'/`risk'/paf_`mmm_string'_`location_id'_`sex_id'.csv"
	}

// END
