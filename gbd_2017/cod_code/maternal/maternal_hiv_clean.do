** Master file for the maternal analysis
** 
** 
** 
**

set more off
	if c(os) == "Unix" {
		local jdrive "ADRESS"
		local hdrive "ADRESS"
		local clustertemp "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local jdrive "J:"
		local hdrive "H:"
	}

// set globals for best models
	global acause = "maternal"
	global codem = "4331"
	global hivdate = "2014_04_08"

// define folder locations
	local parent_dir "FILEPATH"
	local in_dir "FILEPATH"
		cap mkdir "FILEPATH"
	local out_dir "FILEPATH"
		cap mkdir "FILEPATH"
	local temp_dir "FILEPATH"
	local code_dir "FILEPATH"

// HIV correction
			// define the date of the run (YYYY_MM_DD, e.g. 2014_01_09)
				local paf_file_date = "2017_09_18"
				local births_file "`FILEPATH"
				local hivprev_dir "FILEPATH"  
			
			// bring in countrycodes for merging
			use "`FILEPATH", clear
			drop if iso3 == ""
			tempfile countries
			save `countries', replace
			
			// Bring in the Spectrum output file, merge with births and calculate HIV prevalence in pregnancy by age, country and year	
				insheet using "`births_file'", comma names clear
				merge 1:1 iso3 year age using "FILEPATH"
					drop if age <=5 | age >= 50
					drop if year > 2013 | year < 1980
						tab iso3 if _m == 1
					foreach iso in IND MOZ MDA {
						replace hiv_births = 0 if (hiv_births == . & iso3 == "`iso'")
					}
					replace hiv_births = 0 if age == 10
					drop if hiv_births == .
					drop _m
				merge m:1 iso3 using `countries', keepusing(location_id location_name gbd_region_name gbd_superregion_name)
					drop if _m != 3
					drop _m
				destring births, replace
					replace births = births * 1000
				gen hiv_prev = hiv_births / births
					tab age if hiv_prev == .
					replace hiv_prev = 0 if hiv_prev == .
				save "FILEPATH", replace
									
					
		// 2b. Bring in the PAF data from lit review, format it correctly, fill in missing numbers and then meta-analyze the relative risks
			// PAF data from literature review. Based on analysis of studies in Calver C and Ronsmans C "The contribution of HIV to pregnancy-related mortality: a systematic review and meta-analysis. AIDS 2013, 27: 1631-1639
				import excel using "FILEPATH", firstrow clear
				
				// drop those studies with problems
					drop if author == "Figueroa-Damian"				// study only reports death during pregnancy - no postpartum mortality
					drop if author == "Ticconi"						// study is focused on those presenting with suspected malaria - HIV+ group is therefore not representative of HIV+ population
	
				// for the remainder, process to get in long format the counts for the 2x2 matrix
					drop if HIV_pos_persons == . | disaggregation == "separate"
					keep author pub_year study_site year* HIV*
						drop HIV_prev_pregnant
					tostring pub_year, replace
					gen label  = author + " - " + pub_year + " - " + study_site
					gen study_id = _n
					
					// calculate py for all those where it is missing assuming 40 weeks pregnancy + 6 weeks pp risk period
					foreach n in pos neg {
						gen HIV_`n'_nodeath = HIV_`n'_persons - HIV_`n'_deaths
						replace HIV_`n'_py = HIV_`n'_persons * 46/52 if HIV_`n'_py == .
						replace HIV_`n'_mortrt = HIV_`n'_deaths / HIV_`n'_py
						gen HIV_`n'_alive = HIV_`n'_persons - HIV_`n'_deaths
					}
					
					// try straight metan
					sort label
						label var author "Author"
						label var pub_year "Year"
						label var study_site "Country"
					// without random effects
					metan HIV_pos_deaths HIV_pos_nodeath HIV_neg_deaths HIV_neg_nodeath, noint lcols(author pub_year study_site) astext(75) texts(120) double xlabel(1,10) xtick(0.1,1,10) boxopt(mcolor(forest_green) msymbol(triangle)) pointopt(msymbol(smtriangle) mcolor(gold) msize(tiny)) sortby(label) diamopt(lcolor(forest_green) lwidth(thin)) ciopt(lcolor(black) lwidth(thin)) 
					graph export "FILEPATH", replace
					// with random effects added
					metan HIV_pos_deaths HIV_pos_nodeath HIV_neg_deaths HIV_neg_nodeath, random noint lcols(author pub_year study_site) astext(75) texts(120) double xlabel(1,10) xtick(0.1,1,10) boxopt(mcolor(forest_green) msymbol(triangle)) pointopt(msymbol(smtriangle) mcolor(gold) msize(tiny)) sortby(label) diamopt(lcolor(forest_green) lwidth(thin)) ciopt(lcolor(black) lwidth(thin)) 
					graph export "FILEPATH", replace					
				// keep the random effects version and make 1000 draws and save
				keep label
				return list
				gen RR = r(ES)
				gen RR_lower = r(ci_low)
				gen RR_upper = r(ci_upp)
				gen RR_sd = (RR - RR_lower) / 1.96
				drop label
				collapse (mean) RR*
				expand 1000 in 1
				gen RR_d = rnormal(RR,RR_sd)
				gen n = _n - 1
				gen dummy = "dummy"
				keep RR_d n dummy
				reshape wide RR_d, i(dummy) j(n)
				save "FILEPATH", replace
		
		
		// Bring back the HIV prevalence in pregnancy, merge the RR_draws, calculate PAFs
		use "FILEPATH", clear
			gen dummy = "dummy"
		joinby dummy using "FILEPATH"
			codebook iso3
			codebook year
			codebook age
			codebook hiv_prev
			forvalues i = 0(1)999 {
				gen draw_`i' = (hiv_prev * (RR_d`i' - 1))/((hiv_prev * (RR_d`i' - 1))+1)
			}
		egen paf_mean = rowmean(draw_*)
		order iso3 age year paf_mean hiv_prev
		
		// find those countries where paf is high. for countries that rely on pmdf data (mostly SSA), we need to subtract from the total the number of incidental hiv deaths and identify those that are maternal and add them back. 
		// for those countries that rely on VR data, incidental HIV deaths are already excluded. HIV deaths during pregnancy are also not mapped to maternal.
		// In the former case, we must subtract incidental HIV deaths from the total. In the latter, we must determine the number of HIV-related maternal deaths and add them to the total. This will be an important distinction when paf is high. 
			tab gbd_region if paf_mean >= 0.02, missing
			tab gbd_region if paf_mean >= 0.01, missing
			preserve
			keep if paf_mean >= 0.01
			drop if strpos(gbd_region,"Africa")
			sort paf_mean
			tab iso3, missing
			restore
			
			// identify which countries will have hiv deaths added to the prediction vs. those where it will be subtracted (VR vs. VA/DHS)
			gen add = 1											
				replace add = 0 if paf_mean >= 0.01	| strpos(gbd_region,"Africa") 
				// all those country-age-years with PAF > 1% should instead have hiv deaths subtracted
																// others that should be subtracted include HTI, PNG, KHM, MMR
			foreach iso3 in ARG ATG BHS BLZ BRB DMA DOM GRD GUY HND JAM LCA MMR PAN PRT SUR THA TTO UKR VCT {
				replace add = 1 if iso3 == "`iso3'"	// but not those that rely on VR
			}
				
		keep iso3 age year draw*
		outsheet using "FILEPATH", comma names replace	
					
