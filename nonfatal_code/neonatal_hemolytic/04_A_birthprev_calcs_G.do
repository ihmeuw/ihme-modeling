/* **************************************************************************
NEONATAL HEMOLYTIC MODELING
PART 4: Other
Part A: Birth prevalence of Kernicterus due to all other causes
6.9.14

The hemolytic modeling process is described in detail in the README in the 
hemolytic code folder.  This script completes the first and only step of modeling 
kernicterus due to other birth conditions: taking the sum of preterm, g6pd, and 
rh disease ehb birth prevalences, subtracting this number from one to get the prevalence 
of all other hemolytic conditions, and multiplying this prevalence by scalar values
 to get kernicterus birth prevalence.

Copied from README:
D. Other
	1. BUSERs that don't have any of the three conditions above still have some probability of developing EHB and kernicterus.   We begin by summing the birth prevalences of rh disease from part A, G6PD from part B, and preterm births from part C, 	and subtracting this from 1 to get the birth prevalence of all other births:

		other_birth_prev = 1 - (rh_birth_prev +  g6pd_birth_prev + preterm_birth_prev)

	2. The proportion of other children who go on to have EHB is 0.00038 (0.00033, 0.00163).  We multiply other_birth_prev by 	  this value: 

			other_ehb_prev = other_birth_prev * 0.00038 (0.00033, 0.00163)

	3. We know that living in less-developed countries increases the risk of EHB.  Thus, we multiply the above value by 2.45 	in country-years with an NMR greater than 15:

		other_ehb_prev = other_ehb_prev * 2.45 if NMR>15 

	4. Like for G6PD, we muliply this birth prevalence by some NMR-dependent proportion to determine children who go on to 		have kernicterus (these values are the same as for G6PD):

								  	other_ehb_prev * 0.23 (0.099, 0.361) if NMR <5
			other_kern_prev   =     other_ehb_prev * 0.35 (0.12, 0.58) if 5<= NMR <15
									other_ehb_prev * 0.438 (0.255, 0.621) if NMR >=15

	This concludes the other portion of hemolytic modeling.
		
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
	adopath + /*FILEPATH*/
	

	run /*FILEPATH*/

	// directories
		local working_dir  /*FILEPATH*/
		local in_dir /*FILEPATH*/
		local out_dir /*FILEPATH*/
		local plot_dir /*FILEPATH*/
		
		// three input directories; one from each of the other causes
		local rh_disease_dir /*FILEPATH*/
		local g6pd_dir /*FILEPATH*/
		local preterm_dir /*FILEPATH*/
		
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

	local plot_prev= 1

	//1. get NMR data ready (we use it as a scale to determine what countries get which scalars)
		// import data
	use /*FILEPATH*/, clear
		keep ihme_loc_id sex year q_nn_med
		keep if year>=1980
		rename q_nn_med nmr

		preserve
			import delimited /*FILEPATH*/, clear
			tempfile converter
			save `converter', replace
		restore

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
		get_location_metadata, location_set_id(9) gbd_round_id(4) clear 
		tempfile locations
		save `locations'		
		drop ihme_loc_id 
		merge 1:1 location_id using `converter', keep(3) 
		drop _merge
		

		merge 1:m ihme_loc_id using `nmr', keep(3)
		drop _merge 

		// save prepped nmr template
		keep ihme_loc_id location_name location_id sex year nmr
		tempfile neo
		save `neo'






	//2. bring in ehb prevalence data 
		di in red "importing rh disease data"
		use "`rh_disease_dir'", clear 
		drop if sex==3
		keep if year==1990 | year==1995 | year==2000 | year==2005 | year==2010 | year==2016
		rename draw_* rh_disease_draw_*
		
		di in red "merging on g6pd data"
		//merge will only include dismod year vals
		merge 1:1 location_id year sex using "`g6pd_dir'", nogen
		rename draw_* g6pd_draw_*
		
		di in red "merging on preterm data"
		merge 1:1 location_id year sex using "`preterm_dir'", keep(3) nogen
		rename draw_* preterm_draw_* 

	//3. add nmr data, merge==2 will be non-gbd countries
		merge 1:1 location_id year sex using `neo', keep(3)
	
	//3. Define scalars
	// we need to multiply these draws by the following scalars(with uncertainty):
	// 1. the proportion of g6pd bUSERs who will develop EHB: 
	// 		0.00038 (0.00033, 0.00163)
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
		local baseline_ehb_scalars 0.00038 0.00033 0.00163
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
	
	//4. Loop through draws, running the calculation:
		
		di in red "calculating kernicterus prevalence"
		//quietly{
			forvalues i=0/999{
				if mod(`i', 100)==0{
							di in red "working on number `i'"
				}
				
				di "generate 'other' prevalence"
				gen other_draw_`i' = 1- (rh_disease_draw_`i' + g6pd_draw_`i' + preterm_draw_`i')
				drop rh_disease_draw_`i' g6pd_draw_`i' preterm_draw_`i'
				
				di "pull ehb scalars"
				local baseline_ehb_scalar = rbeta(`baseline_ehb_alpha', `baseline_ehb_beta')
				local high_ehb_scalar = rgamma(`high_ehb_k', `high_ehb_theta')
				
				di "get baseline ehb prevalence"
				gen ehb_draw_`i' = other_draw_`i' * `baseline_ehb_scalar'
				drop other_draw_`i'
				
				di "multiply by higher value if necessary"
				replace ehb_draw_`i' = ehb_draw_`i' * `high_ehb_scalar' if nmr>15
				
				di "pull kern scalars"
				foreach scalar_type in low mid high{
					local `scalar_type'_kern_scalar = rbeta(``scalar_type'_kern_alpha', ``scalar_type'_kern_beta')
				}
				
				di "convert to kernicterus prevalence"
				gen kernicterus_draw_`i' = ehb_draw_`i' * `low_kern_scalar' if nmr<5
				replace kernicterus_draw_`i' = ehb_draw_`i' * `mid_kern_scalar' if (nmr>=5 & nmr<15)
				replace kernicterus_draw_`i' = ehb_draw_`i' * `high_kern_scalar' if nmr>15
				
			}
		//}
		
	//5. Save ehb and kern, get summary stats

	foreach disease_type in ehb kernicterus {
		di "all draws"
		preserve
			keep ihme_loc_id year sex location_id `disease_type'_draw*
			rename `disease_type'_draw* draw*
			save /*FILEPATH*/, replace	
			export delimited /*FILEPATH*/,  replace
		
		di "summary stats"
			
			egen mean = rowmean(draw*)
			fastpctile draw*, pct(2.5 97.5) names(lower upper)
			drop draw*

			sort location_id year sex 
			save /*FILEPATH*/, replace
			export delimited /*FILEPATH*/, replace
		restore
		
	}


/* ///////////////////////////////////////////////////////////
// PLOTTING
///////////////////////////////////////////////////////////// */

	if `plot_prev'==1{

		di in red "plotting results"
		use /*FILEPATH*/, clear
		
		//for ylabeling 
		qui sum mean
		local max_val = r(max)

		pdfstart using /*FILEPATH*/
		
		levelsof ihme_loc_id, local(ihme_loc_id_list)
		
		foreach ihme_loc_id of local ihme_loc_id_list{
					
			di in red "plotting for `ihme_loc_id'"
					
			line mean year if ihme_loc_id == "`ihme_loc_id'", lcolor(purple) lwidth(*1.75) || line  lower year if ihme_loc_id == "`ihme_loc_id'", lcolor(purple) lpattern(dash) lwidth(*1.75) || line  upper year if ihme_loc_id == "`ihme_loc_id'", lcolor(purple) lpattern(dash) lwidth(*1.75) by(sex) title("Other Kern, `ihme_loc_id'") xtitle("Year") ytitle("Prevalence") legend(order(1 2) label(1 "Birth Prevalence of Other Kernicterus") label(2 "95%CI")) xlabel(1990(10)2016)
			pdfappend
		}
				
		pdffinish, view

	}









