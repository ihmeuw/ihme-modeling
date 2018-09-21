/* **************************************************************************
NEONATAL HEMOLYTIC MODELING
PART 2: G6PD
Part B: Birth prevalence of Kernicterus due to G6PD deficiency
6.9.14

The hemolytic modeling process is described in detail in the README in the 
hemolytic code folder.  This script completes the second and final step of modeling 
kernicterus due to G6PD deficiency: taking total g6pd prevalence and multiplying
by scalar values to get kernicterus birth prevalence.

Copied from README:
B. G6PD
	1. We use estimates of the birth prevalence of G6PD (g6pd_birth_prev) from the dismod outputs of congenital models of this condition. (appended together in 02_append_g6pd)

	2. The proportion of children with G6PD who go on to get EHB is 0.0013 (0.00085, 0.002) (Bhutani).  We multiply the birth prevalence of G6PD by this value:

			g6pd_ehb_prev = g6pd_birth_prev * 0.0013 (0.00085, 0.002)

	3. We know that living in less-developed countries increases the risk of EHB.  Thus, we multiply the above value by 2.45 (Brown and Mah) in country-years with an NMR greater than 15:

		g6pd_ehb_prev = g6pd_ehb_prev * 2.45 if NMR>15

	4. Similar to step 4.d in part A, we now multiply this birth prevalence by some proportion of children that go on to develop kernicterus.  Unlike for Rh disease, this proportion is NMR-dependent (SOURCE??):

									g6pd_ehb_prev * 0.23 (0.099, 0.361) if NMR <5
				g6pd_kern_prev   =  g6pd_ehb_prev * 0.35 (0.12, 0.58) if 5<= NMR <15
									g6pd_ehb_prev * 0.438 (0.255, 0.621) if NMR >=15


	This concludes the G6PD portion of hemolytic modeling.
		
******************************************************************************/

	clear all
	set graphics off
	set more off
	set maxvar 32000


	/*  //////////////////////////////////////////////
			WORKING DIRECTORY
	////////////////////////////////////////////// */ 

	// discover root 
		if c(os) == "Windows" {
			local j /*FILEPATH*/
			// Load the PDF appending application
			quietly do /*FILEPATH*/
		}
		if c(os) == "Unix" {
			local j /*FILEPATH*/
			ssc install estout, replace 
			ssc install metan, replace
		} 
		
	// functions
	quietly do /*FILEPATH*/
	adopath + /*FILEPATH*/
		
	// directories 
		local working_dir /*FILEPATH*/
		local in_dir /*FILEPATH*/

		local out_dir /*FILEPATH*/
		capture mkdir "`out_dir'"
		
		local plot_dir "`out_dir'/time_series"
		capture mkdir "`plot_dir'"
		
	// Create timestamp for logs
		local c_date = c(current_date)
		local c_time = c(current_time)
		local c_time_date = "`c_date'"+"_" +"`c_time'"
		display "`c_time_date'"
		local time_string = subinstr("`c_time_date'", ":", "_", .)
		local timestamp = subinstr("`time_string'", " ", "_", .)
		display "`timestamp'"
		
		
	
/* ///////////////////////////////////////////////////////////
// G6PD BIRTH PREVALENCE CALCULATIONS
///////////////////////////////////////////////////////////// */

	local plot_prev= 0

	//1. get NMR data ready (we use it as a scale to determine what countries get which scalars)
		// import data
		use /*FILEPATH*/, clear //NMR data
		keep ihme_loc_id sex year q_nn_med
		keep if year>=1980
		rename q_nn_med nmr

		// convert from probability to NMR
		replace nmr = nmr*1000
		replace sex = "1" if sex == "male"
		replace sex = "2" if sex == "female" 
		replace sex = "3" if sex == "both"
		destring sex, replace
		drop if sex==3

		//year is at midear in this file, switch it to beginning of the year
		replace year = year - 0.5
		tempfile nmr
		save `nmr'

		// merge on additional lcoation data
		get_location_metadata, location_set_id(21) gbd_round_id(4) clear
		tempfile locations
		save `locations'

		merge 1:m ihme_loc_id using `nmr', keep(3) nogen // _merge == 1 are global, super region and regions

		// save prepped nmr template
		keep location_name location_id sex year nmr
		tempfile neo
		save `neo'
	


	//2. bring in prevalence data - only the most recent file from that directory 
	//	NOTE: this will only have values for GBD years 
	di "importing data"
	cd "`in_dir'"
	local files: dir . files "*_prev.csv"
	local files: list sort files 
	import delimited using `=word(`"`files'"', wordcount(`"`files'"'))', clear
	
		
	//3. add nmr data, merge==2 will be non-gbd years and sex==3, merge == 1 is global, superregion, region and mortality-specific locations
		merge 1:1 location_id year sex using `neo', keep(3) nogen
		
	
	//3. Define scalars [see citations above]
	// we need to multiply these draws by the following scalars(with uncertainty):
	// 1. the proportion of g6pd bUSERs who will develop EHB: 
	// 		0.0013 (0.00085, 0.002)
	// 	  However, in high-nmr countries, that number gets multiplied by this value:
	// 		2.45, (1.44, 4.16)
	// 2. The proportion of EHB bUSERs who will go on to develop kernicterus:
	//		0.23 (0.099, 0.361) if nmr<5
	//		0.35 (0.12, 0.58) if 5=<nmr<15
	//		0.438 (0.255, 0.621) if 15=<nmr
	//
	// To get proper uncertainty bounds for both of these, we define the parameters
	// of a beta distribution for those bounded between 0 and 1 and a gamma distribution,
	// for those bounded between 0 and infinity, and draw from that distribution to get 
	// the scalar of use when looping through draws.

		di in red "calculating scalar parameters"
		local baseline_ehb_scalars 0.0013 0.00085 0.002
		local high_ehb_scalars 2.45 1.44 4.16
		local low_kern_scalars 0.23 0.099 0.361
		local mid_kern_scalars 0.35 0.12 0.58
		local high_kern_scalars 0.438 0.255 0.621

		foreach scalar_type in baseline_ehb high_ehb low_kern mid_kern high_kern{
			di in red "finding scalar parameters for `scalar_type'"
			local scalar_mean : word 1 of ``scalar_type'_scalars'
			local scalar_lower : word 2 of ``scalar_type'_scalars'
			local scalar_upper : word 3 of ``scalar_type'_scalars'
			local scalar_se = (`scalar_upper' - `scalar_lower')/(2*1.96)
			if `scalar_mean'<1{
				di in red "generating beta parameters"
				local count_n = (`scalar_mean' * (1-`scalar_mean') ) / `scalar_se'^2
				local `scalar_type'_alpha = `count_n' * `scalar_mean'
				local `scalar_type'_beta = `count_n' * (1-`scalar_mean')
				di in red "for `scalar_type', scalar_se is `scalar_se', count is `count_n', alpha is ``scalar_type'_alpha', beta is ``scalar_type'_beta'"
			}
			else{
				di in red "generating gamma parameters"
				local `scalar_type'_k = (`scalar_mean'/`scalar_se') ^2
				local `scalar_type'_theta = (`scalar_se'^2) / `scalar_mean'
				di in red "for `scalar_type', scalar_se is `scalar_se', k is ``scalar_type'_k', theta is ``scalar_type'_theta'"
			}
		}
	
	//4. Loop through draws, running the multiplication:
	// 		g6pd_kern_prev = g6pd_prev * baseline_scalar [*high_scalar if nmr>15]
		
		di in red "calculating kernicterus prevalence"
		quietly{
			forvalues i=0/999{
				if mod(`i', 100)==0{
							di in red "working on number `i'"
				}
				//pull ehb scalars
				local baseline_ehb_scalar = rbeta(`baseline_ehb_alpha', `baseline_ehb_beta')
				local high_ehb_scalar = rgamma(`high_ehb_k', `high_ehb_theta')
				
				//get baseline ehb prevalence
				gen ehb_draw_`i' = draw_`i' * `baseline_ehb_scalar'
				
				//multiply by higher value if necessary
				replace ehb_draw_`i' = ehb_draw_`i' * `high_ehb_scalar' if nmr>15
				
				//pull kern scalars
				foreach scalar_type in low mid high{
					local `scalar_type'_kern_scalar = rbeta(``scalar_type'_kern_alpha', ``scalar_type'_kern_beta')
				}
				
				//convert to kernicterus prevalence
				gen kernicterus_draw_`i' = ehb_draw_`i' * `low_kern_scalar' if nmr<5
				replace kernicterus_draw_`i' = ehb_draw_`i' * `mid_kern_scalar' if (nmr>=5 & nmr<15)
				replace kernicterus_draw_`i' = ehb_draw_`i' * `high_kern_scalar' if nmr>15
				
			}
		}

//5. Save ehb and kern, get summary stats

	foreach disease_type in ehb kernicterus{
		//all draws
		preserve
			keep year sex location_id location_name `disease_type'_draw*
			rename `disease_type'_draw* draw*
			save /*FILEPATH*/, replace	
			export delimited using /*FILEPATH*/, replace
		
		//summary stats	
			
			egen mean = rowmean(draw*)
			fastpctile draw*, pct(2.5 97.5) names(lower upper)
			drop draw*

			sort location_name location_id year sex 
			save /*FILEPATH*/, replace
			export delimited using /*FILEPATH*/, replace
		restore
		
	}


/* ///////////////////////////////////////////////////////////
// PLOTTING
///////////////////////////////////////////////////////////// */

	if `plot_prev'==1{

		di in red "plotting results"
		use /*FILEPATH*/, clear
		
		// for ylabeling 
		qui sum mean
		local max_val = r(max)

		// get rid of 'Asir (stata thinks it's a local)
		replace location_name = "Asir" if location_name == "'Asir" // "
		replace location_name = "Asaka" if location_name == "?saka"

		pdfstart using /*FILEPATH*/
		
		levelsof location_name, local(location_name_list)
		
		foreach location_name of local location_name_list{
					
			di in red "plotting for `location_name'"
					
			twoway (line mean year if location_name == "`location_name'", lcolor(purple) lwidth(*1.75)  ) ///
				   (line  lower year if location_name == "`location_name'", lcolor(purple) lpattern(dash) lwidth(*1.75)  ) ///
					(line  upper year if location_name == "`location_name'", lcolor(purple) lpattern(dash) lwidth(*1.75)  ), ///
					by(sex) ///
					title("G6PD Kernicterus Prev, `location_name'") ///
					legend(order(1 2) label(1 "Birth Prevalence of G6PD Kernicterus") label(2 "95%CI")) ///
					xlabel(1990(10)2016)
			pdfappend
			}
				
		pdffinish, view

	}









