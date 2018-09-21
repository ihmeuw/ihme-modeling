 /* **************************************************************************
NEONATAL HEMOLYTIC MODELING
PART 1: RH DISEASE
Part B: Rhogam adjusted rh-incompatibility
6.9.14

The hemolytic modeling process is described in detail in the README in the 
hemolytic code folder.  This script completes the second step of modeling 
kernicterus due to Rh Disease: adjusting for the coverage of Rhogam, a drug that 
prevents Rh disease.  

Copied from the README:
2. Rhogam Adjustment
	a. We have data on the number of Rhogam doses (rhogam_doses) given to each developing country in 2010.  We make the
		following assumptions:

		--if a country's neonatal mortality rate (NMR) is less than 5, Rhogam coverage was 100% 
			(irrespective of what the rhogam data list says)
		--if the country's NMR is greater than 5 and it is shown receiving Rhogam, it got the listed
			number of doses in 2010
		-- if the country's NMR is greater than 5 and it is shown receiving no doses of Rhogam, or it
			is not on the list, it got no doses of Rhogam in 2010.

		And, finally: the proportion of rhogam doses to birth stayed constant over time
		(we make this assumption out of necessity, since we only have Rhogam data for one year)

	b. Using the pos_to_neg_count numbers we generated in step 1, we keep only the values for 2010 and sum across years 
		and subnationals to get a single count for each country in 2010. We call this 'raw_births'.

	c. Find the proportion of these raw_births who received Rhogam.  This country-specific fraction will be used as a 
		multiplier for all sex-years:

		rhogam_prop = rhogam_doses / raw_births

	d. Apply these country-specific multipliers to every sex-year, subject to the assumptions outlined in part a.:

									0 if NMR < 5
	rhogam_adjusted_pregnancies  =  rh_incompatible_count * (1-rhogam_prop) if NMR>=5 and data avaiable from Rhogam list
									rh_incompatible_count count if NMR>5 and no data given on Rhogam list


	This gives us the number of births with Rh incompatibility who did not receive Rhogam.
	
	NOTE ON SUMMARY STATISTICS: 
	We run this analysis on the draw level. To get summary statistics, we take the mean, 2.5th, and 97.5th
	percentile of the draws.  However, mathematically, the mean of the rhogam-adjusted pregnancy draws will
	not equal:
		(the mean value of our rh-incompatible pregnancy counts) * (the mean value of our rhogam draw)
		
	(having a non-constant denominator messes with the arithmetic).  To get an appropriate mean measure, then, 
	we replace our 'mean' value in the summary stats with the result of the equation above.  
	
******************************************************************************/

clear all
set more off
set graphics off
set maxvar 32000


/*  //////////////////////////////////////////////
		WORKING DIRECTORY
////////////////////////////////////////////// */

// priming the working environment 
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
	di in red "J drive is `j'"
	
// load functions
quietly do /*FILEPATH*/

adopath + /*FILEPATH*/


// set directories 	
	local working_dir = /*FILEPATH*/
	local out_dir /*FILEPATH*/
	local plot_dir /*FILEPATH*/
	
// Create timestamp for logs
    local c_date = c(current_date)
    local c_time = c(current_time)
    local c_time_date = "`c_date'"+"_" +"`c_time'"
    display "`c_time_date'"
    local time_string = subinstr("`c_time_date'", ":", "_", .)
    local timestamp = subinstr("`time_string'", " ", "_", .)
    display "`timestamp'"
		
	
/* ///////////////////////////////////////////////////////////
// RHOGAM ADJUSTMENT
///////////////////////////////////////////////////////////// */

	//plot at the end?
	local plot_adjustments=0
	
	// prep: get NMR data ready (we use it as a scale to determine what countries do/do not get rhogam-adjusted)
		// import data
		use /*FILEPATH*/, clear
		keep ihme_loc_id sex year q_nn_med
		keep if year>=1980
		rename q_nn_med nmr
		// convert from probability to NMR
		replace nmr = nmr*1000
		replace sex = "1" if sex == "male"
		replace sex = "2" if sex == "female" 
		replace sex = "3" if sex == "both"
		destring sex, replace
		// year is at midear in this file, switch it to beginning of the year
		replace year = year - 0.5
		
		// merge on location_id 
		tempfile nmr
		save `nmr'
		
		
		get_location_metadata, location_set_id(21) gbd_round_id(4) clear 
		tempfile locations
		save `locations'
		merge 1:m ihme_loc_id using `nmr', keep(3) nogen // _merge == 1 are global, super region and regions

		keep location_id sex year nmr
		tempfile neo
		save `neo'
	
	/* /////////////
	1. Rhogam Dose Data 
	//////////////// */
		local rhogam_dir = /*FILEPATH*/
		
		import delimited using "`rhogam_dir'", clear
		rename rh_immunoglobulin rhogam_doses
		keep location_name rhogam_doses

			// some location_names don't quite match 
			replace location_name = "Moldova" if location_name == " Moldova"
			replace location_name = "Congo" if location_name == "Congo "
			replace location_name = "The Gambia" if location_name == "Gambia"

		merge 1:m location_name using `locations', keep(3) nogen
		drop if level == 4 
		keep location_name rhogam_doses location_id 
		tempfile rhogam
		save `rhogam', replace
	
	
	/* /////////////////////////////////////////
	2. Rh-incompatible pregnancies from part A
	/////////////////////////////////////////// */
	
		local rh_prev_dir /*FILEPATH*/
		use "`rh_prev_dir'", clear

		// merge on location get_location_metadata
		merge m:1 location_id using `locations', keep(3) nogen
		
		//take the mean of the draws for plotting, later
		egen rh_incompatible_count_mean = rowmean(draw_*)
		
		//this rhogam data is only for 2010.  Thus, we need to generate a ratio
		// of rhogam doses/pregnancies for 2010 that we can then apply to all other years. 
	
		//first: generate a dataset that's just national-level, both-sexes incompatible pregnancy counts for 2010
			// gen parent_iso3 = substr(iso3, 1,3)
			
			preserve
				keep if level==3 & sex==3 & year==2010
				//this merge won't be perfect: those iso3's that have an nmr<5 in 2010 will be _m==1.  That's ok.
				merge 1:1 location_id using `rhogam', nogen
				
				//now loop through draws
				quietly{
					forvalues i=0/999{
						if mod(`i', 100)==0{
								di in red "generating 2010 rhogam props for `i'"
						}
						//the values with NMR<5 will get filled with '.' here
						gen rhogam_prop_draw_`i' = rhogam_doses / draw_`i'
						//sometimes there are more rhogam doses delivered to a country
						// than we think there were Rh-incompatible pregnancies.
						// in this step, we adjust those proportions to one.
						// since '.' has a value of 'infinity' in the dataset, this 
						// also had the nice effect of changing those '.' for NMR<5 
						// countries to 1.
						replace rhogam_prop_draw_`i'=1 if rhogam_prop_draw_`i'>1
						
						drop draw_`i'
					}
				}
			
				tempfile nat_allsex_births
				save `nat_allsex_births', replace
			restore
	
	// merge the proportion dataset onto the original, then apply the proportion you got 
	// in the last step to each country-sex-subnat combination to get Rhogam-adjusted
	// birth numbers.  The relationship is: adjusted pregnancies = rh-incompatible pregnancies * (1-rhogam_prop).
	
		//merge proportions onto pregnancy counts
		replace ihme_loc_id = substr(ihme_loc_id, 1, 3) // this is changing all subnat locations to match their three-character national id
		merge m:1 ihme_loc_id using `nat_allsex_births', keep(3) nogen // _merge != 3 are superregions and regions

		//also merge nmr data onto this dataset, so we can adjust for subnats with nmr<5
		// 
		merge 1:1 location_id year sex using `neo', keep(3) nogen 
		
		quietly{
			forvalues i = 0/999{
				if mod(`i', 100)==0{
					di in red "generating rhogam adjusted pregnancies for draw `i'"
				}
				//make sure that places with nmr<5 have complete coverage
				replace rhogam_prop_draw_`i' = 1 if nmr<5
				//generate rhogam_adjusted_pregnancies
				replace draw_`i' = draw_`i' * (1- rhogam_prop_draw_`i')
				drop rhogam_prop_draw_`i'
			}
		}
		
		//save all draws
		preserve
			keep location_id year sex births draw_*
			save /*FILEPATH*/, replace
			export delimited using /*FILEPATH*/, replace
		restore
	
	
		//get summary stats
		fastpctile draw_*, pct(2.5 97.5) names(lower upper)
		egen mean = rowmean(draw_*)
	
		drop draw_*
		
		export delimited using /*FILEPATH*/, replace
		save /*FILEPATH*/, replace
	
	
	///visualizations: see what the adjustment did
	
	if `plot_adjustments'==1{
		
		use /*FILEPATH*/, clear
		
		drop if rhogam_doses==0 | rhogam_doses==.
		
		sort ihme_loc_id year sex
		drop if sex==3 
	
		qui sum mean
		local max_val = r(max)

		pdfstart using /*FILEPATH*/
		levelsof location_name, local(location_name_list)
				
		foreach location_name of local location_name_list{
					
			di in red "plotting for `location_name'"
			
			line mean year if location_name == "`location_name'", lcolor(purple) lwidth(*1.75) || line lower year if location_name == "`location_name'", lcolor(purple) lpattern(dash) lwidth(*1.75) ||line upper year if location_name == "`location_name'", lcolor(purple) lpattern(dash) lwidth(*1.75) || line rh_incompatible_count_mean year if location_name == "`location_name'", lcolor(lime) lwidth(*1.75)	by(sex)	title("Rhogam-Adjusted Pregs, `location_name'") legend(order(1 4) label(1 "Adjusted Pregnancy Count")  label(4 "Raw Pregnancy Count"))
			pdfappend
		}
			
		pdffinish, view
			
	}
	
	
	
	
