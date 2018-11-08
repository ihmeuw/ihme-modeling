// **********************************************************************
// Purpose:        This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Description:   Calculate incidence of leprosy by country-year-age-sex, using cases reported to WHO and
//                      age-patterns from dismod. Produce incidence for every year in 1890-2015, and sweep forward
//                      with ODE to arrive at prevalence predictions.
// FILEPATH/02_inc_prev_ode_1663_1664_parallel.do 22 1987 FILEPATH/02_inc_prev_ode.do

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}

	// Define arguments
	if "`1'" != "" {
		local location_id `1'
		local min_year `2'
		local tmp_dir `3'
	}
	else if "`1'" == "" {
		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
		local date = subinstr("`date'"," ","_",.)
		local location_id 7
		local min_year 1987
		local tmp_dir "FILEPATH"
	}

// *********************************************************************************************************************************************************************
// Import functions
  // directory for standard code file
  adopath + FILEPATH
  adopath + FILEPATH

// Get IHME loc ID for location
	use "`tmp_dir'/loc_iso3.dta" if location_id == `location_id', clear
	levelsof iso3, local(iso) c

// Get files we will need
	** Disfigurement envelope
	use "`tmp_dir'/g2d_env_draws.dta", clear
	tempfile g2d_env_draws
	save `g2d_env_draws', replace
	** Population
	use "`tmp_dir'/pops.dta", clear
	tempfile pops
	save `pops', replace
	** Disfigurement proportions
	use "`tmp_dir'/gd_prop_draws.dta", clear
	tempfile gd_prop_draws
	save `gd_prop_draws', replace

// *********************************************************************************************************************************************************************
    forvalues year = 1987/2017 {

      display "`iso' `year': calculating disability"

    ** Pull draw files for leprosy incidence in both sexes and merge with age-pattern draws of G2D among incident
    ** leprosy cases and the total G2D envelope draws.
      foreach sex in male female {
        quietly insheet using "FILEPATH/incidence_`iso'_`year'_`sex'.csv", clear double

        format %16.0g age draw*

        if "`sex'" == "male" {
          generate int sex_id = 1
        }
        else {
          generate int sex_id = 2
        }

        generate location_id = `location_id'
        generate int year_id = `year'

        tempfile lep_inc_`sex'
        quietly save `lep_inc_`sex'', replace
      }

      use `lep_inc_male', clear
      append using `lep_inc_female'
	  recast double age
	  replace age = 0.01 if age > 0.009 & age < 0.011
	  replace age = 0.1 if age > 0.09 & age < 0.11
	  merge m:1 age using "`tmp_dir'/age_map.dta", assert(3) nogen

      quietly merge m:1 location_id year_id using `g2d_env_draws', keepusing(env*) assert(2 3) keep(3) nogen
      quietly merge 1:1 location_id year_id age_group_id sex_id using `pops', keepusing(mean_pop) assert(2 3) keep(3) nogen
      quietly merge 1:1 age_group_id sex_id using `gd_prop_draws', keepusing(draw_gd*) assert(3) nogen

    ** Calculate age-sex-specific prevalence of G2D among incident leprosy cases, while squeezing it in the G2D envelope.
    ** The resulting draws reflect uncertainty in leprosy incidence, overall prevalence of G2D among incident cases, and
    ** uncertainty in relative differences between ages and sexes in G2D prevalences among incident cases.

      forvalues i = 0/999 {

      ** Calculate total number of incident G2D cases based on incident leprosy cases and envelope
        rename draw_`i' draw_lepr_`i'
        quietly replace draw_lepr_`i' = draw_lepr_`i' * mean_pop
        ** draw_lepr_`i' now contains number of incident leprosy cases
        quietly summarize draw_lepr_`i'
        ** r(sum) now contains the total number of incident leprosy cases
        quietly replace env_`i' = env_`i' * r(sum)
        ** env_`i' now contains the total number of G2D cases among incident leprosy cases

      ** Calculate age-sex-specific incident G2D cases, based on proportional age pattern and envelope
        quietly replace draw_gd2_`i' = draw_lepr_`i' * draw_gd2_`i'
        ** draw_gd2_`i' now contains unscaled, age/sex-specific number of G2D cases among incident leprosy cases
        quietly summarize draw_gd2_`i'
        ** r(sum) now contains the unscaled total number of incident G2D cases
        quietly replace draw_gd2_`i' = draw_gd2_`i' * env_`i' / r(sum)
        quietly replace draw_gd2_`i' = 0 if missing(draw_gd2_`i')
        quietly replace draw_gd2_`i' = draw_lepr_`i' if draw_gd2_`i' > draw_lepr_`i'
        ** draw_gd2_`i' now contains the final age/sex-specific number of incident G2D cases

      ** Calculate age-sex-specific incident G1D cases, based on prevalence of G1D among incident leprosy
      ** cases that don't have G2D.
        quietly replace draw_gd1_`i' = draw_gd1_`i' * (draw_lepr_`i' - draw_gd2_`i')

      ** Convert to incidence rates to national level
        quietly replace draw_gd1_`i' = draw_gd1_`i' / mean_pop
        quietly replace draw_gd2_`i' = draw_gd2_`i' / mean_pop

      }


    ** Write sex-specific files to clustertmp
      foreach sequela in disfigure_1 disfigure_2 {
      foreach sex in male female {
        preserve
        if "`sex'" == "male" {
          quietly keep if sex == 1
        }
        else {
          quietly keep if sex == 2
        }
        if "`sequela'" == "disfigure_1" {
          keep age draw_gd1_*
          rename draw_gd1_* draw_*
        }
        if "`sequela'" == "disfigure_2" {
          keep age draw_gd2_*
          rename draw_gd2_* draw_*
        }

      ** Write incidence file (write to final folder so that it can be uploaded simultaneously with prevalence)
        outsheet using "FILEPATH/incidence_`iso'_`year'_`sex'.csv", comma replace

		** Write initial prevalence for first year of G2D
		if `year' == `min_year' & "`sequela'" == "disfigure_2" {
			forvalues x = 0/999 {
			  ** Prevalence at start of each age category
				quietly generate double prev_start_`x' = 0 if age == 0
				quietly replace prev_start_`x' = prev_start_`x'[_n-1] + (1 - prev_start_`x'[_n-1]) * (1 - exp(-(age - age[_n-1]) * draw_`x'[_n-1])) if age > 0

			  ** Prevalence halfway each age category (half-year correction)
				quietly replace prev_start_`x' = prev_start_`x' + (1 - prev_start_`x') * (1 - exp(-(age[_n+1] - age)/2 * draw_`x')) if age < 95
				quietly replace prev_start_`x' = prev_start_`x' + (1 - prev_start_`x') * (1 - exp(-(100 - age)/2 * draw_`x')) if age == 95
				quietly drop draw_`x'
				quietly rename prev_start_`x' draw_`x'
			}

			keep draw_* age
			format %16.0g draw_* age
			outsheet using "FILEPATH/prevalence_`iso'_`min_year'_`sex'.csv", comma replace
		}

      ** Write prevalence file for G1D only, assuming duration of 6 months (current treatment duration)
      ** G2D is calculated in next part of code with ODE solver because duration is lifelong.
        if "`sequela'" == "disfigure_1" {
          forvalues i = 0/999 {
            quietly replace draw_`i' = draw_`i' * 0.5
          }
          outsheet using "FILEPATH/prevalence_`iso'_`year'_`sex'.csv", comma replace
        }
        restore
      }
      }
    }

// *********************************************************************************************************************************************************************
