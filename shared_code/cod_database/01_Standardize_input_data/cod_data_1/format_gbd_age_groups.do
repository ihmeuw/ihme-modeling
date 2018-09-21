** ********************************************************************************************* **
// Purpose: Sum specific age groups and verify death counts and WHO age formats
** ********************************************************************************************* **

// Fill in the deaths variables 
aorder
// Make sure all deaths variables are present in data
	forvalues i = 1/26 {
		capture gen deaths`i' = 0
	}
	aorder
// Collapse deaths3-deaths6
	egen deaths3to6 = rowtotal(deaths3-deaths6)
	replace deaths3 = deaths3to6
	drop deaths3to6
	forvalues i = 4/6 {
		replace deaths`i' = 0
	}
// collapse deaths23-deaths25
	egen deaths23to25 = rowtotal(deaths23-deaths25)
	replace deaths23 = deaths23to25
	drop deaths23to25
	forvalues i = 24/25 {
		replace deaths`i' = 0
	}
// recode frmat <2 to 2 since we collapsed the above
	replace frmat = 2 if frmat < 2
// rename im_deaths to deaths91-deaths94
	forvalues i = 1/4 {
		capture gen im_deaths`i' = .
		capture gen deaths9`i' = 0
		drop im_deaths`i'
	}
// set im_frmat to 8 for countries that don't report any im_deaths
	bysort iso3 year location_id source NID national: egen tot_deaths2 = total(deaths2)	
	egen tmp = rowtotal(deaths91 deaths92 deaths93 deaths94)
	bysort iso3 year location_id source NID national: egen tot_tmp = total(tmp)
	replace im_frmat = 8 if tot_tmp==0 & tot_deaths2!=0
	drop tot_* tmp
//  collapse deaths92
	aorder
	egen enn = rowtotal(deaths91-deaths92)
	replace deaths91 = enn
	drop enn
	replace deaths92 = 0
	replace im_frmat = 2 if im_frmat == 1

// set deaths2 to 0 if the country also reports im_deaths
	aorder
	capture drop im_tot
	egen im_tot = rowtotal(deaths91-deaths94)
	replace deaths2 = 0 if im_tot > 0
	drop im_tot
// move deaths2 to deaths91 for observations where im_frmat = 8
		replace deaths2 = 0 if im_frmat == 8 & deaths91 > 0 & deaths91 ~= .
		replace deaths91 = deaths2 if im_frmat == 8 & deaths2 ~= 0 & deaths2 ~= .
		replace deaths2 = 0 if im_frmat == 8 & deaths91 > 0 & deaths91 ~= .
// now make a final deaths2 variable
	egen im_tot = rowtotal(deaths91 deaths93 deaths94)
	replace deaths2 = im_tot
	drop im_tot
// separate age-missing observation for age-sex splitting
	egen tmp = rowtotal(deaths3-deaths25 deaths91-deaths94)
	** tag deaths26 observations which are in line with deaths in other ages
	gen tag = 1 if tmp!=0 & deaths26>0 & deaths26!=.
	expand 2 if tag==1, gen(new)
	replace deaths26 = 0 if tag==1 & new==0
	replace frmat = 9 if tag == 1 & new == 1
	replace im_frmat = 8 if tag == 1 & new == 1
	replace deaths1 = deaths26 if tag == 1 & new == 1
	** zero out deaths in other ages in the new deaths26 observation
	foreach var of varlist deaths2-deaths25 deaths91-deaths94 {
		replace `var' = 0 if tag==1 & new==1
	}
	drop tmp tag new
	** now check that rows that only have deaths26 are assigned to frmat9 for age-sex splitting
	egen tmp = rowtotal(deaths2-deaths25 deaths91-deaths94)
	replace frmat = 9 if tmp==0 & tmp!=. & deaths26!=0 & deaths26!=.
	replace im_frmat = 8 if tmp==0 & tmp!=. & deaths26!=0 & deaths26!=.
	drop tmp
// recalculate deaths1
	 capture drop deaths1
	 foreach i of numlist 2/26 91/94 {
		capture gen deaths`i' = 0
	 }
	 aorder
	 egen deaths1 = rowtotal(deaths3-deaths94)
	 
	 