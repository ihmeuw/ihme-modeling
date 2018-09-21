** ************************************************************************************************* **
** Author:                            NAME 
** Date created:                             6/18/2015    
** Purpose: Format  Ebola_WHO data for the CoD prep code                        
**
** ************************************************************************************************* **

** PREP STATA**
    clear all

    set more off
    ssc install bygap
    
    ** set the prefix for whichever os we're in
    if c(os) == "Windows" {
        global j ""
    }
    if c(os) == "Unix" {
        global j ""
        set odbcmgr unixodbc
    }

    local date = c(current_date)
	local today = date("`date'", "DMY")
	local year = year(`today')
	local month = month(`today')
    
** *********************************************************************************************
**
** What we call the data: same as folder name
    local data_name "Ebola_WHO"

** Establish data import directories 
    local jdata_in_dir "FILEPATH"
    local cod_in_dir "FILEPATH"

 quietly {
 	do "FILEPATH"
 	do "FILEPATH"
 }
    
** *********************************************************************************************
**
** If appending multiple: best practice is to loop the import, cleaning, cause, year, sex, and age processes together.
** IMPORT **
    ** Import from PATH 
    ** In this dataset use the corrected deaths as they have some consideration of "uncertainty" around the prediction
	import excel "FILEPATH", clear
	**drop in 3/52	**  4.1.2016: We want to use the raw deaths because we are applying correction factors in the draws step.
	* keep in 1/25
    
    rename * var_*
    rename (var_A var_B var_C) (location_name year notes)
    ** No data in CIV
    drop if location_name == "Cote d'Ivoire" & year == "1994"
    drop if location_name == ""
    
    foreach var of varlist var_* { 
        replace `var' = subinstr(`var', "?", "", .)
        local sex = substr(`var', 1, 1) in 1
        local age = subinstr(`var', "Missing", "NS", 1) in 2
        local age = subinstr("`age'", "-", "_", 1)
        local age = subinstr("`age'", "+", "pl", 1)
        local age = subinstr("`age'", " ", "_", 1)
        rename `var' `sex'`age'
    }
    drop in 1/2
    reshape long A M F, i(location_name year notes) j(age)string
    ** Drop where we have duplicate data
    gen tag = 1 if notes == ""
    replace A = "X" if tag == 1
	
    replace tag = 2 if inlist(notes,"No age/sex breakdown", "No age or sex", "No age or gender")
    replace A = "X" if (tag == 2 & A == "0")
    replace M = "X" if tag == 2 & (M == "" | M == "0")
    replace F = "X" if tag == 2 & (F == ""| F == "0")
    ** drop if tag == 2 & (inlist(notes,"No age/sex breakdown", "No age or sex", "No age or gender")) & age != "NO_AGE"
   
    replace tag = 3 if notes == "No age"
    replace A = "X" if tag == 3 & (A != "0")
	replace M = "X" if tag == 3 & M == "0"
	replace F = "X" if tag == 3 & F == "0"
	** drop if tag == 3 & notes == "No age" & age != "NO_AGE"
	
	drop notes
    rename (A M F) (deathsA deathsM deathsF)
    reshape long deaths, i( location_name year age tag) j(sex)string
    drop if deaths == "X"
    destring deaths, replace
    ** Clean
    reshape wide deaths, i(location_name sex year tag) j(age)string
    rename deathsNO_AGE deathsNS
	destring year, replace
    gen NID = 229822
    tempfile data
    save `data', replace
    

** Format another dataset
    foreach year of numlist 2014 2015{
        ** Method A based on sex- and age-group-specific CFRs
         import excel "FILEPATH", sheet("Inferred confirmed deaths A") clear 
         
    ** INITIAL CLEANING **
        ** Remove unnecessary rows and columns from data
        ** grab totals for checking later
        **local total_deaths = C in 9
        **local total_deaths_missing = C in 11
        ** now drop these unnecessary rows
        drop in 6/11    
        
        ** Create tag variable to match above data source
         ** tag is 1 beause there is age breakdown
        gen tag = 1
        gen redistribute = 0
        
        ** Replace country name with ISO3
        rename A location_name
        ** replace iso3 = "LBR" if iso3 == "Liberia"
        ** replace iso3 = "SLE" if iso3 == "Sierra Leone"
        ** replace iso3 = "GIN" if iso3 == "Guinea"
        
        ** Create a stub list for reshaping later
        local ages ""   
        ** Rename all death variables so we can reshape long by age and sex
        foreach var in B C D E F G H I J K L M N O P Q R S T U V W X Y Z AA AB AC AD AE AF AG AH AI AJ AK AL AM AN AO AP AQ AR AS AT AU {
            local sex = substr(`var', 1, 1) in 1
            local age = subinstr(`var', "Missing", "NS", 1) in 2
            local age = subinstr("`age'", "-", "_", 1)
            local age = subinstr("`age'", "+", "pl", 1)
            
            ** if "`sex'" == "M" { // Reshape stubs are same for M and F
                ** local ages "`ages' deaths`age'"
            ** }
            
            destring `var', replace force
            rename `var' deaths`sex'`age'
        }
         ** these rows used to be variable headers
        drop in 1/2   
        reshape long deaths, i(location_name) j(sex)s
        gen age = substr(sex, 2, .)
        replace sex = substr(sex, 1, 1)

        ** Reshape long by sex
        ** reshape long `ages', i(iso3) j(sex) string

        gen year = `year'
        
        ** Format to append the other data (only go to 95+)
        reshape wide deaths, i(location_name sex year) j(age)string
        gen deaths95pl = deaths95_99 + deaths100pl
        drop deaths100pl deaths95_99
        gen NID = 208315
        order location_name sex year deathsNS deaths* tag NID

        append using `data'
        save `data', replace
    }
   
    
	
    
** Format additional Ebola shocks data -  3.10.2016
	import delimited "`cod_in_dir'/WHO_ebola_pre2014.csv", clear varnames(1)
	
	** Variable formatting
	rename deaths deathsNS
	rename country location_name
	expand 2 if year == "2001-2002", gen(new)
	replace deathsNS = deathsNS/2 if year == "2001-2002"
	replace year = "2001" if year == "2001-2002" & new == 0
	replace year = "2002" if year == "2001-2002" & new == 1
	drop new
	gen sex = "A"
	** no age and/or sex
	gen tag = 2	
	
	drop if deathsNS == 0 | deathsNS == .
	
	replace location_name = "South Sudan" if location_name == "Sudan"
	
	keep year sex location_name deathsNS tag
	replace year = substr(year,1,4)
	destring year, replace
	
	collapse (sum) deathsNS, by(location_name year tag sex) fast
	
    gen NID = 243597
	
	append using `data'
    save `data', replace


** Format another dataset for non-West Africa 2014 ebola deaths
    import excel using "FILEPATH", clear sheet("Confirmed Deaths")
    ** only variables with relevant data
    keep A B C O    
    rename A location_name
    rename B year
    rename C deathsNS
    rename O deaths45_49

    drop in 1/2

    destring deathsNS, replace
    destring deaths45_49, replace
    destring year, replace
    gen location_id = .
    ** 1 USA death is in Texas
    replace location_id = 566 if location_name == "USA"  

    gen sex = ""
    ** 1 USA death is male
    replace sex = "M" if location_name == "USA"  
	replace sex = "A" if sex == ""

    gen tag = .
    ** age and sex specified
    replace tag = 1 if location_name == "USA"    
    **No age or sex breakdown
    replace tag = 2 if tag == . 

    gen NID = 245181
	
	drop if location_name == "" | year == .
	

    append using `data'
	
	
** Format appended data
	egen total = rowtotal(deaths*)
	drop if total == 0
	
** Add correct iso3s
    gen iso3 = ""
    replace iso3 = "CIV" if location_name == "Cote d'Ivoire"
    replace iso3 = "COD" if location_name == "Democratic Republic of Congo"
    replace iso3 = "GAB" if location_name == "Gabon"
    replace iso3 = "GIN" if location_name == "Guinea"
    replace iso3 = "LBR" if location_name == "Liberia"
	replace iso3 = "COG" if location_name == "Congo"
    replace iso3 = "MLI" if location_name == "Mali"
    replace iso3 = "NGA" if location_name == "Nigeria"
    replace iso3 = "COG" if location_name == "Republic of Congo"
    replace iso3 = "SLE" if location_name == "Sierra Leone"
    replace iso3 = "ZAF" if location_name == "South Africa (ex-Gabon)"
    replace iso3 = "SSD" if location_name == "South Sudan"
    replace iso3 = "SDN" if location_name == "Sudan"
    replace iso3 = "UGA" if location_name == "Uganda"
    replace iso3 = "USA" if location_name == "USA"

    replace location_id = 205 if location_name == "Cote d'Ivoire"
    replace location_id = 171 if location_name == "Democratic Republic of Congo"
    replace location_id = 173 if location_name == "Gabon"
    replace location_id = 208 if location_name == "Guinea"
    replace location_id = 210 if location_name == "Liberia"
	replace location_id = 170 if location_name == "Congo"
    replace location_id = 211 if location_name == "Mali"
    replace location_id = 214 if location_name == "Nigeria"
    replace location_id = 170 if location_name == "Republic of Congo"
    replace location_id = 217 if location_name == "Sierra Leone"
    replace location_id = 196 if location_name == "South Africa (ex-Gabon)"
    replace location_id = 435 if location_name == "South Sudan"
    replace location_id = 522 if location_name == "Sudan"
    replace location_id = 190 if location_name == "Uganda"
    assert location_id != .
    
	** Duplicates check between sources
		** Duplicates edits
		drop if year == 2001 & iso3 == "COG" & NID == 243597
		drop if year == 2002 & iso3 == "COG" & NID == 243597
		drop if year == 2003 & iso3 == "COG" & NID == 243597
		drop if year == 1994 & iso3 == "GAB" & NID == 243597
		drop if year == 1996 & iso3 == "GAB" & NID == 243597
		drop if year == 2001 & iso3 == "GAB" & NID == 243597
		drop if year == 2002 & iso3 == "GAB" & NID == 243597
		drop if year == 2000 & iso3 == "UGA" & NID == 243597
		drop if year == 2012 & iso3 == "UGA" & NID == 243597
		
	duplicates tag iso3 year sex, gen(dup)
	count if dup > 0
	if `r(N)' > 0 {
		sort iso3 year
		br if dup > 0
		display "Make changes above"
	}

	tempfile testing_at
	save `testing_at', replace

	drop dup
    
    ** Reshape long by age
    reshape long deaths, i(year location_name sex tag NID) j(age) string
    
    ** Replace age with age_frmat values to reshape deaths to wide proper
    replace age = "2" if age == "0"
    replace age = "3" if age == "1_4"
    replace age = "7" if age == "5_9"
    replace age = "8" if age == "10_14"
    replace age = "9" if age == "15_19"
    replace age = "10" if age == "20_24"
    replace age = "11" if age == "25_29"
    replace age = "12" if age == "30_34"
    replace age = "13" if age == "35_39"
    replace age = "14" if age == "40_44"
    replace age = "15" if age == "45_49"
    replace age = "16" if age == "50_54"
    replace age = "17" if age == "55_59"
    replace age = "18" if age == "60_64"
    replace age = "19" if age == "65_69"
    replace age = "20" if age == "70_74"
    replace age = "21" if age == "75_79"
    replace age = "22" if age == "80_84"
    replace age = "23" if age == "85_90"
    replace age = "24" if age == "90_94"
    replace age = "25" if age == "95pl"
    
 *    ** go to GBD age groups
 *    gen age_group_id = age
 *    replace age_group_id = "-1" if age_group_id == "NS"
 *    destring age_group_id, replace
 *    replace age_group_id = 4 if age_group_id == 2
	* replace age_group_id = 5 if age_group_id == 3
	* replace age_group_id = age_group_id - 1 if (age_group_id >= 7) & (age_group_id <= 22)

	* tempfile testing_at_2
	* save `testing_at_2', replace
	
 *    ** redistribute 80+ deaths. (age == 21 in GBD format)
    
 *    ** get the populations of each of the countries in our data.
	*     ** Population for 1970 - 2016
	* 	get_population, location_id("-1") sex_id("3") age_group_id("30 31 32 235") year_id("-1") clear // want both sexes together
	* 	rename population pop_
	* 	drop process_version_map_id
	* 	rename year_id year
	* 	reshape wide pop_, i(location_id year) j(age_group_id)
	* 	tempfile population
	* 	save `population', replace

	* 	************** get_location_metadata was broken when I was working in this, so my workaround was to use a db query and type in the
	* 	**					loc_ids above.
	* 	* get_location_metadata, location_set_id(35) version_id(153) clear
	* 	* keep if location_type == "admin0"
	* 	* keep location_id ihme_loc_id
	* 	* rename ihme_loc_id iso3
	* 	* isid iso3
	* 	* tempfile locations
	* 	* save `locations', replace

 *    **get the fraction of the 80+ population in each of 80-84, 85-89, 90-94, and 95+ age groups
 *    use `testing_at_2', clear
 *    drop age
 *    replace age_group_id = 999 if age_group_id == -1
 *    reshape wide deaths, i(year location_id sex total iso3) j(age_group_id)
 *    merge m:1 year location_id using `population'
 *    keep if _m == 3
 *    egen pop_80_plus = rowtotal(pop_*)
 *    ** reassign the 80+ deaths
 *    foreach i of numlist 30 31 32 235 {
 *    	gen pct_`i' = pop_`i' / pop_80_plus
 *    	gen deaths`i' = pct_`i' * deaths21
 *    }
 *    drop _m pct*

 *    tempfile testing_at_3
 *    save `testing_at_3', replace
	
	* ** go back to the format we had around line 312, using age for the remaining splitting...
	* reshape long deaths, i(year location_id sex total iso3) j(age_group_id)
 *     ** go to WHO age groups
 *    gen age = age_group_id
 *    	replace age = age + 1 if (age >= 6) & (age <= 21)
 *    	replace age = 2 if age_group_id == 4
 *    	replace age = 3 if age_group_id == 5
 *    tostring age, replace
 *    replace age = "NS" if age == "999"
 
 

    * no-age deaths get redistributed below
    replace age = "26" if age == "NS"

    * drop age_group_id
    * drop _m pct*
    ** Reshape age to wide with correct deaths varnames
    reshape wide deaths, i(location_name sex year NID) j(age) string
    
** YEAR (year) **
    // Make year long if needed
    // Generate a year value that is appropriate for each observation
    // year (numeric) - whole years only
    //replace year = substr(year, 1,4)
	//destring year, replace

    /*  3/15/2016: Can't remember where these month breakdowns came from, but we have proportions in West Africa by total deaths per year from NAME, used those proportions instead.
    ** Split Guinea & Sierra Leone deaths proportionately by number of months in 2014 and 2015
    ** Guinea: 28 Feb 2014 to 25 Apr 2015. 10/14 in 2014, 4/14 in 2015
    expand 2 if iso3 == "GIN", gen(id)
    foreach var of varlist deaths* {
        replace `var' = `var' * (10/14) if iso3 == "GIN" & id == 0
        replace `var' = `var' * (4/14) if iso3 == "GIN" & id == 1
        replace year = 2015 if iso3 == "GIN" & id == 1
    }
    drop id
    
    ** Sierra Leone: 21 May 2014 to 25 Apr 2015. 7/11 in 2014, 4/11 in 2015
    expand 2 if iso3 == "SLE", gen(id)
    foreach var of varlist deaths* {
        replace `var' = `var' * (7/11) if iso3 == "SLE" & id == 0
        replace `var' = `var' * (4/11) if iso3 == "SLE" & id == 1
        replace year = 2015 if iso3 == "SLE" & id == 1
    }
    drop id
    */

** SEX (sex) **
    // Make sex long if needed
    // Make sex=1 if Male, sex=2 if Female, sex=9 if Unknown
	replace sex = "1" if sex == "M"
	replace sex = "2" if sex == "F"
	replace sex = "9" if sex == "A" | sex == ""
	destring sex, replace
	assert inlist(sex, 1,2,9)
	
** COLLAPSE
	collapse (sum) deaths* total, by(iso3 location_name location_id year sex tag NID) fast
    
/*  3.17.2016: This dataset is now a shock so we don't need cc_code, this cc_code process is messing up some of the deaths with unknown age and sex.
*/
** We can't calculate cc_code where for age unknown.  Redistribute these deaths according to the age pattern
	preserve
		** Get the proportions from data that has no unknown ages
		order year location_name iso3 sex deaths3 deaths7 deaths8 deaths9 deaths1* deaths20-deaths25 deaths26
		rename deaths26 redistribute
		keep if tag == 1 & redistribute == 0
		collapse (sum) deaths*, by(sex tag redistribute)
		egen total = rowtotal(deaths3-deaths25)
		** Create combined average for unknown sex
			set obs 3
			replace sex = 9 if sex == .
			carryforward redistribute tag, replace
			foreach var of varlist deaths* total {
				cap sum `var'
				replace `var' = `r(sum)' if `var' == .
			}
			
		foreach var of varlist deaths* {
			gen double pct_`var' = `var'/total
		}
		keep sex pct*
		tempfile pct
		save `pct', replace
	restore
	** Apply proportions to data
	rename deaths26 redistribute
	preserve
		keep if tag != 1 | redistribute != 0
		merge m:1 sex using `pct', keep(1 3) nogen
		foreach var of varlist deaths* {
			replace `var' = `var' + (redistribute * pct_`var')
		}
		drop redistribute pct* total
		tempfile deaths_rd
		save `deaths_rd', replace
	restore
	drop if tag != 1 | redistribute != 0 
	append using `deaths_rd'
	
	** Now redistribute unknown sex
	preserve
	keep if sex != 9
	collapse (sum) deaths*, by(sex)
	foreach var of varlist deaths* {
		cap sum `var'
		local sum_`var' = `r(sum)'
		gen pct_`var' = `var' / `sum_`var''
	}
	keep sex pct*
	save `pct', replace
	** Apply sex proportions
	restore, preserve
	keep if sex == 9
	expand 2, gen(new)
	replace sex = 1 if new == 0
	replace sex = 2 if new == 1
	assert inlist(sex,1,2)
	drop new
	merge m:1 sex using `pct', nogen assert(3)
	foreach var of varlist deaths* {
		replace `var' = `var' * pct_`var'
	}
	drop pct_*
	save `deaths_rd', replace
	restore
	drop if sex == 9
	append using `deaths_rd'
	assert inlist(sex,1,2)
	
	** More duplicates created due to sex splitting
	duplicates tag iso3 year sex, gen(dup)
	drop if dup == 1 & NID == 243597
	drop dup
	isid iso3 year sex
	
	
	save "FILEPATH", replace

	** ----------------------------------------------------------------------------------------------------- **
**
