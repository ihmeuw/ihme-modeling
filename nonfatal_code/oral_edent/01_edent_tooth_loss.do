// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// PREP STATA
	clear
	set more off
	set maxvar 3200
	if c(os) == "Unix" {
		global prefix "{FILEPATH}"
		set odbcmgr unixodbc
		set mem 2g
	}
	else if c(os) == "Windows" {
		global prefix "{FILEPATH}"
		set mem 2g
	}

// Temp directory
	local tmp_dir "`1'"

// Reading parent model id num
	local edent_id `2'

// ME_id of results
	local child_id `3'

//  ME_id of asymptomatic results
	local asymp_id `4'

// location_id
	local loc `5'


// ****************************************************************************
// Log work
	capture log close
	log using "`tmp_dir'/`child_id'/00_logs/`loc'_draws.smcl", replace

// Load in necessary function
run "{FILEPATH}/get_draws.ado"

// Get symptomatic and asymptomatic prevalence and incidence
	foreach year in {YEAR IDS} {
		foreach sex in {SEX IDS} {
			foreach metric in {METRIC IDS} {
				// Pull in draws of oral_edent
				get_draws, gbd_id_field(modelable_entity_id) gbd_id(`edent_id') measure_ids(`metric') source("{SOURCE}") location_ids(`loc') year_ids(`year') sex_ids(`sex') age_group_ids({AGE GROUP IDS}) status(best) clear
				gen metric = "prevalence_incidence"
				// merge on the tooth loss symptomatic proportion that was created in 00_master
				merge m:1 metric using "`tmp_dir'/`child_id'/tooth_loss_split.dta", assert(3) nogen
				// get symptomatic, by multiplying the draws by the symptomatic proportion
				forval y = 0/999 {
					gen symp_`y' = draw_`y'*prop_`y'
				}
				drop prop* metric
				// get asymptomatic, by subtracting the symptomatic from the total
				preserve
					forval y = 0/999 {
						replace draw_`y' = draw_`y' - symp_`y'
					}
					drop symp*
					outsheet using "`tmp_dir'/`asymp_id'/01_draws/`metric'_`loc'_`year'_`sex'.csv", comma names replace
				restore
				drop draw*
				renpfix symp draw
				outsheet using "`tmp_dir'/`child_id'/01_draws/`metric'_`loc'_`year'_`sex'.csv", comma names replace
			}
		}
	}


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
