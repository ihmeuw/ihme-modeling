
// Settings
			// Clear memory and set memory and variable limits
				clear all
				set mem 5G
				set maxvar 32000

			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color

			// Define J drive (data) for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix "ADDRESS"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "ADDRESS"
				}
			
			// Close any open log file
				cap log close
				
			
	
// locals
    local notifications "FILEPATH"
	local temp_dir "ADDRESS"
	local intermediate_dir "ADDRESS"
	local out_dir "ADDRESS"
	local GBD_2016 "ADDRESS"
		
** **********************************
// Prep data
** **********************************

** get population data
	  
		adopath + "ADDRESS/"
		get_population, location_id("-1") year_id(-1) sex_id("1 2") age_group_id("-1") location_set_id(22) clear
		rename population mean_pop
		merge m:1 age_group_id using "FILEPATH"
		tempfile pop
		save `pop', replace
		
		// create 65+ custom age group
		use `pop', clear
		
		// keep necessary age groups
		keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
		// drop aggregate locations
		drop if inlist(location_id,1, 4, 5, 9, 21, 31, 32, 42, 56, 64, 65, 70, 73, 96, 100, 103, 104, 120, 124, 134, 137, 138, 158, 159, 166, 167, 174, 192, 199)
		// merge on iso3
		merge m:1 location_id using "FILEPATH", keepusing(ihme_loc_id) keep(3)nogen
		rename ihme_loc_id iso3
			** generate custom age groups... 
			preserve
				qui keep if age_group_id>=18
				collapse (sum) mean_pop, by(iso3 year_id sex sex_id)
				qui gen age=65
				tempfile tmp_65
				save `tmp_65', replace
			restore
		// create under 5 age group
			preserve
			    qui keep if age_group_id<=5
				collapse (sum) mean_pop, by(iso3 year_id sex sex_id)
				qui gen age=0
				tempfile tmp_0
				save `tmp_0', replace
			restore
		// create custome age groups 5-15, 15-25, ..., 55-65	
			drop if age_group_id<=5
			drop if age_group_id>=18
			/* replace age_group_name ="80 to 99" if age_group_name=="80 plus" */
			split age_group_name,p( to )
			gen age=age_group_name1
			destring age, replace
			forvalues i=5(10)55 {
				preserve
					local k=`i'+5
					keep if age>=`i' & age<=`k'
					collapse (sum) mean_pop, by(iso3 year sex sex_id)
					gen age=`i'
					tempfile tmp_`i'
					save `tmp_`i'', replace 
				restore
			}
			//append the files
			use "`tmp_0'", clear
			forvalues i=5(10)55 {
				qui append using "`tmp_`i''"
			}
			
			append using "`tmp_65'"
						
			qui drop if year<1990
			rename year_id year
			rename mean_pop pop
			
			//save
			
	        save "FILEPATH", replace
	
** Prep age weights for WHO age groups...

		// generate under 5 custom age group
		use "FILEPATH", clear
			preserve
			qui keep if age_group_id<=5
			collapse (sum) weight
			qui gen age=0
		    tempfile tmp_0
		    save `tmp_0', replace
		    restore
			
		// generate 65+ custom age group
	        preserve
			qui keep if age_group_id>=18
			collapse (sum) weight
			qui gen age=65
		    tempfile tmp_65
		    save `tmp_65', replace
		    restore
			
		// generate custome age groups 5-15, 15-25, ..., 55-65	
		
		use "FILEPATH", clear
		// merge on age_group_names
		merge 1:1 age_group_id using "FILEPATH", keep(3)nogen
		// drop under 5 and 65+ age groups
		drop if age_group_id<=5 | age_group_id>=18
		// create custome age groups 5-15, 15-25, ..., 55-65	
			split age_group_name,p( to )
			gen age=age_group_name1
			destring age, replace
		forvalues i=5(10)55 {
			    preserve
				local l=`i'+5
				qui keep if age>=`i' & age<=`l'
				collapse (sum) weight
				qui gen age=`i'
			    tempfile tmp_`i'
			    save `tmp_`i'', replace 
				restore
		}
		//append the files
		use "`tmp_0'", clear
		append using "`tmp_65'"
		forvalues i=5(10)55 {
			append using "`tmp_`i''"
		}
		qui sort age
		qui order age weig
		tempfile tmp_age_weight
		save `tmp_age_weight', replace
		save "FILEPATH", replace


	** Prep age-specific case notifications  
	
		insheet using "`notifications'", comma names clear 
			keep iso3 year new_sp_* new_sn_* new_ep*
			// drop unnecessary variables
			foreach var in sn ep {
				drop new_`var'_s* new_`var'_mu new_`var'_fu
			}
			drop new_ep new_sp_fu new_sp_mu
			// reshape by variable
			foreach var in sp sn ep {
				preserve
					qui keep iso year new_`var'_m* new_`var'_f*
					qui reshape long new_`var'_m new_`var'_f, i(iso year) j(age) string
					qui reshape long new_`var'_, i(iso year age) j(sex) string
					tempfile tmp_`var'
					save `tmp_`var'', replace 
				restore
			}
			** merge datasets together
			use "`tmp_sp'", clear 
			merge 1:1 iso year age sex using "`tmp_sn'", nogen 
			merge 1:1 iso year age sex using "`tmp_ep'", nogen 
				qui replace age="0-14" if age=="014"
				qui replace age="0-4" if age=="04"
				qui replace age="15-24" if age=="1524"
				qui replace age="15-100" if age=="15plus"
				qui replace age="25-34" if age=="2534" 
				qui replace age="35-44" if age=="3544"
				qui replace age="45-54" if age=="4554"
				qui replace age="5-14" if age=="514"
				qui replace age="55-64" if age=="5564"
				qui replace age="65-100" if age=="65"
				qui drop if age=="0-14" | age=="15-100"
				qui replace sex="1" if sex=="m"
				qui replace sex="2" if sex=="f"
				qui destring sex, replace 
			
	     save "FILEPATH", replace


** ******************************************
// Generate ep and sn inflation factors 
** ******************************************

	// Mark SU & Relapsed outliers 

		insheet using "`notifications'", comma names clear
		qui keep iso3 year new_sp new_sn new_su new_ep ret_rel
			/* drop *lab* tot* *bact* *oth */
			gen outlier_su=0
			gen outlier_rel=0
			**
			qui replace outlier_su=1 if iso=="AGO" & year==1995
			qui replace outlier_rel=1 if iso=="AGO" & year==2006
			qui replace outlier_su=1 if iso=="ARE" & year==1999
			qui replace outlier_su=1 if iso=="ARG" & year==1999
			qui replace outlier_su=1 if iso=="ARG" & year==2000
			qui replace outlier_su=1 if iso=="AUS" & year==2004
			qui replace outlier_su=1 if iso=="AZE" & year==2000
			qui replace outlier_su=1 if iso=="BEL" & year==1995
			qui replace outlier_su=1 if iso=="BIH" & year==1997
			qui replace outlier_su=1 if iso=="BRN" & year==1997
			qui replace outlier_su=1 if iso=="CHE" & year==1998
			qui replace outlier_su=1 if iso=="DJI" & year==1999
			qui replace outlier_su=1 if iso=="ERI" & year==1995
			qui replace outlier_su=1 if iso=="ETH" & year==1996
			qui replace outlier_su=1 if iso=="GBR" & year==1995
			qui replace outlier_su=1 if iso=="IND" & year==1997
			qui replace outlier_su=1 if iso=="ISR" & year==1998
			qui replace outlier_su=1 if iso=="ITA" & year==1995
			qui replace outlier_su=1 if iso=="LBY" & year==1995
			qui replace outlier_su=1 if iso=="MDA" & year==2000
			qui replace outlier_su=1 if iso=="NLD" & year==1995
			qui replace outlier_su=1 if iso=="NZL" & year==1997
			qui replace outlier_su=1 if iso=="PAK" & year==1998
			qui replace outlier_su=1 if iso=="PAN" & year==1999
			qui replace outlier_su=1 if iso=="PRT" & year==1995
			qui replace outlier_su=1 if iso=="SLV" & year==1995
			qui replace outlier_su=1 if iso=="TTO" & year==1997
			qui replace outlier_su=1 if iso=="TUR" & year==2001
			qui replace outlier_su=1 if iso=="TUR" & year==2004
			qui replace outlier_su=1 if iso=="UGA" & year==2000
			qui replace outlier_su=1 if iso=="ZAF" & year==1995
			qui replace outlier_su=1 if iso=="ZMB" & year==2002
			qui replace outlier_su=1 if iso=="ZWE" & year==1995
		replace new_su=. if outlier_su==1
		replace ret=. if outlier_rel==1
		drop out*
		save "FILEPATH", replace
		
	
		
		** Generate Relapsed cases inflation factors...
		
			use "FILEPATH", clear
			// create a variable to indicate whether relapse information is missing or not
				qui gen ret_miss=1 if ret_rel==.
			// temporarily replace missing relapse cases with zeros (we will later mark the inflation factor based on them as missing)
				/* qui replace new_su=0 if new_su==. */
				qui replace ret_rel=0 if ret_rel==.
			// create the relapse inflation factor
				/* qui gen CN_spsu_rel=new_sp+new_su+ret_rel
				qui gen CN_spsu=new_sp+new_su
				qui keep iso year CN_spsu_rel CN_spsu ret_miss
				qui gen ret_inflat_sp=CN_spsu_rel/CN_spsu /*smear unknown is included in both the numerator and the denominator, so replacing the missing values with zeros have no effect on the relapse inflation factor */   */
				
				qui gen CN_sp_rel=new_sp+ret_rel
				rename new_sp CN_sp
				qui keep iso year CN_sp CN_sp_rel ret_miss
				qui gen ret_inflat_sp=CN_sp_rel/CN_sp
				
			// mark the inflation factor as missing if the relapse information is missing
				qui replace ret_inflat_sp=. if ret_miss==1
				qui drop ret_miss
			** 	replace ret_inflat_sp=1 if ret_inflat_sp==.
				qui keep iso year ret_inflat*
			tempfile tmp_ret_inflation
			save `tmp_ret_inflation', replace 				
	        save "FILEPATH", replace
	
	// Interpolate inflation factore values... 

		// Prep data 
		
			// flag countries where we have a single year of data
				
				**
				preserve
					qui drop if ret_inf==.
					qui gen id=1
					collapse (sum) id, by(iso )
					qui gen ratio_ret=0
					qui replace ratio_ret=1 if id<=1
					qui tempfile tmp_ret_ratio
					save `tmp_ret_ratio', replace 
				restore
				
				merge m:1 iso using "`tmp_ret_ratio'", nogen
				drop id
			
		save "FILEPATH", replace

				
		// Interpolate Relapsed
		
			** for multiple year data 
			use "FILEPATH", clear
				keep if ratio_ret==0
				levelsof iso, local(isos) 
				foreach i of local isos {
					preserve
						qui keep if iso=="`i'"
						qui impute ret_inflat year, gen (ret_inflat_sp_xb) 
						qui tempfile tmp_`i'
						save `tmp_`i'', replace 
					restore
				}
				clear
				foreach i of local isos {
					qui append using "`tmp_`i''"
				}
				qui gen ret_inflat_sp_new=ret_inflat_sp 
				qui replace  ret_inflat_sp_new=ret_inflat_sp_xb if ret_inflat_sp==.
				qui replace ret_inflat_sp_new=1 if ret_inflat_sp_new<0
				qui keep iso year ret_inflat_sp_new
			tempfile tmp_ret_1
			save `tmp_ret_1', replace 
			** for one year data
			use "FILEPATH", clear
				qui keep if ratio_ret==1
				qui keep iso ret_infla
				duplicates drop 
				qui drop  if ret_infla==.
				rename ret_infla ret_inflat_avg
			tempfile tmp_ret_2
			save `tmp_ret_2', replace
			** append together
			use "FILEPATH", clear
				qui keep iso year
				merge 1:1 iso year using `tmp_ret_1', nogen
				merge m:1 iso using `tmp_ret_2', nogen
				qui replace ret_inflat_sp_new=ret_inflat_avg if ret_inflat_sp_new==.
				qui drop *avg
				qui duplicates drop 
				drop if year==.
			tempfile tmp_ret_inflat_adj
			save `tmp_ret_inflat_adj', replace
		save "FILEPATH", replace
		
		// Merge together
		use "FILEPATH", clear
			qui drop ratio*
			qui merge 1:1 iso year using "`tmp_ret_inflat_adj'", nogen 
			/* qui merge 1:1 iso year using "`tmp_su_inflat_adj'", nogen  */
			qui replace ret_inflat_sp_new=2 if ret_inflat_sp_new>2
			/* qui replace su_inflat_sp_new=2 if su_inflat_sp_new>2 */
		tempfile tmp_interp_ratios
		save `tmp_interp_ratios', replace 
		save "FILEPATH", replace
		

** ******************************************
// Correct for missing age-sex notifications... (under 15 age group)
** ******************************************
** Prep raw CN data- country year
		insheet using "`notifications'", comma names clear 
		keep iso3 year new_sp new_sn new_su new_ep new_oth ret_rel new_labconf
		tempfile tmp_CN_vars
		save `tmp_CN_vars', replace

		
		use "`tmp_CN_vars'", clear 
			merge 1:m iso year using "FILEPATH", nogen 
			foreach var in sp sn ep {
				rename new_`var'_ new_`var'_age
			}
			order iso year age sex
			drop if new_sp==. & new_sn==. & new_oth==. & new_ep==. & ret_rel==.
			/* merge m:m iso using "`country_codes'", keepusing(ihme_country) keep(1 3) nogen 
			replace ihme=1 if iso=="SSD"
			keep if ihme==1
			drop ihme
			*/
			sort iso3 year age sex
			gen missing_age=0
			replace missing_age=1 if new_sp!=. & new_sp_age==.
			foreach var in new_sp new_sn new_su new_ep new_oth ret_rel {
				rename `var' `var'_cy
			}
			split age,p("-")
			destring age1, replace
			drop age age2
			rename age age
			order iso year age sex
			sort iso year age sex
			drop if year<1990
		save "FILEPATH", replace
		
	
	// Prep data... 

		use "FILEPATH", clear
			drop *cy missing
			** replace age groups <15 missing - Theo outliers
			preserve
				insheet using "FILEPATH", clear names
				gen outlier=1
				tempfile tmp_u15
				save `tmp_u15', replace
			restore
			merge m:1 iso3 using "`tmp_u15'", nogen 
			foreach s in sp sn ep {
				replace new_`s'_age=. if outlier==1 & age<15
			}
			drop outlier
			** replace age-notifications to missing for years where only report one age group
			foreach s in sn ep {
				replace new_`s'_age=. if iso=="IND" & year==2007
				replace new_`s'_age=. if iso=="AGO" & year==2011
				replace new_`s'_age=. if iso=="MLI"
				replace new_`s'_age=. if iso=="BRB"
				replace new_`s'_age=. if iso=="ETH"
				replace new_`s'_age=. if iso=="GRD"
				replace new_`s'_age=. if iso=="TLS"
				replace new_`s'_age=. if iso=="LCA"
				replace new_`s'_age=. if iso=="SYC"
				replace new_`s'_age=. if iso=="ATG"
			}
			** merge on pop
			// rename sex befor merging on pop
			rename sex sex_id
			merge 1:1 iso year age sex using "FILEPATH", keepusing(pop) keep(3) nogen 
			// calculate log incidence rate
			foreach var in sp sn ep {
				gen ln_`var'_rt=ln(new_`var'_age/pop)
			}	
			** Identify where there is all missing data
			preserve
				collapse (sum) new*, by(iso year)
				foreach var in sp sn ep {
					gen missing_`var'=0
				// after collapsing, a missing country-year will be shown as zero
					replace missing_`var'=1 if new_`var'==0
				}
				tempfile tmp_missing
				save `tmp_missing', replace 
			restore
			merge m:1 iso year using "`tmp_missing'", nogen 
		tempfile tmp_reg_data_1
		save `tmp_reg_data_1', replace 
		
		save "FILEPATH", replace
		
	// Run regression by country & sex & impute
	
		// log file
		log using "ADDRESS", replace 
			levelsof sex_id, local(sexes)
			foreach s of local sexes {
				preserve
					keep if  sex==`s'
					**
					di in red "SP Regression - Sex `s'"
						regress ln_sp_rt i.year i.age 
						predict ln_sp_rt_xb
					di in red "SN Regression - Sex `s'"
						regress ln_sn_rt i.year i.age 
						predict ln_sn_rt_xb
					di in red "EP Regression - Sex `s'"
						regress ln_ep_rt i.year i.age 
						predict ln_ep_rt_xb
					tempfile tmp_`s'
					save `tmp_`s'', replace 
				restore
			}
		log close 
		clear
			use "`tmp_1'", clear
			append using "`tmp_2'"
			** convert from log space rates to numbers
			foreach var in sp sn ep {
				gen `var'_rt_xb=exp(ln_`var'_rt_xb)
				gen `var'_xb=`var'_rt_xb*pop
			}
			drop ln* *rt*
		tempfile tmp_age_xbs
		save `tmp_age_xbs', replace
	
	save "FILEPATH", replace
		
	// Fill in missing age groups
	
		use "`tmp_age_xbs'", clear 
			order iso year age sex pop 
			outsheet using "FILEPATH", delim(",") replace
			** Fill missing
			foreach var in sp sn ep {
				// replace with predicted values only when some (but not all) country-years are missing
				replace new_`var'_age=`var'_xb if new_`var'_age==. & missing_`var'==0
			}
			keep iso year age sex new* pop 
		tempfile tmp_age_CN_adj
		save `tmp_age_CN_adj', replace 
	save "FILEPATH", replace

	
** ********************************************
// Prep inputs for EP regression
** ********************************************

	// Generate age standardzied rates & apply SU/RET inflation factors

		use "FILEPATH", clear
			** adjust for smear-unknown
			merge m:1 iso year using "FILEPATH", keep(3) nogen 
				qui drop ret_inflat_sp
				/* qui gen new_sp_adj=new_sp_age*su_inflat_sp */
				qui gen new_sp_adj2=new_sp_age*ret_inflat_sp
				qui keep iso year age sex_id pop new*
		save "FILEPATH", replace 
			** merge on weights
			merge m:1 age using "FILEPATH", nogen 
			** generate rates... & age standardize
			qui replace new_sp_adj2=(new_sp_adj2/pop)*weight
			collapse (sum) new_sp_adj2* pop, by(iso year) 
			qui rename new_sp_adj2 sp_agestd_rt
		tempfile tmp_age_std
		save `tmp_age_std', replace 
				
	    save "FILEPATH", replace

** ********************************************
// Run EP regressions
** ********************************************

	// Prep data

		// Generate desired fractions... 
		
			use "FILEPATH", clear 
				qui drop new_sp_age
				foreach var in sp sn ep {
					qui drop if new_`var'==.
				}
				** gen total 
				qui gen new_tot=new_sp+new_ep+new_sn
				foreach s in sp sn ep {
					qui gen frac_`s'=new_`s'/new_tot
				}
				qui drop if frac_sp==.
				** merge on criteria... 
				** gen iD for populations <5 million
				preserve
					collapse (sum) pop, by(iso year)
					qui gen outlier=0
					qui replace outlier=1 if pop<5000000
					tempfile tmp_pop_out
					save `tmp_pop_out', replace
				restore
				merge m:1 iso year using "`tmp_pop_out'", nogen 
				
			tempfile tmp_fracs
			save `tmp_fracs', replace 
		    save "FILEPATH", replace 
	
		// Prep data for regression 
	
			use "FILEPATH", clear
				merge 1:1 iso year age sex_id using "FILEPATH", nogen  
				merge 1:1 iso year age sex_id using "`FILEPATH", nogen 
				merge m:1 iso year using "FILEPATH", nogen 
				/* ** keep ihme countries
				merge m:m iso3 using "`country_codes'", keepusing(ihme_country) keep(1 3) nogen 
				qui replace ihme=1 if iso=="SSD"
				qui keep if ihme==1
				qui drop ihme
				*/
				** generate age dummies
				levelsof age, local(ages)
				foreach a of local ages {
					qui gen age`a'=0
					qui replace age`a'=1 if age==`a' 
				}
				qui gen sex_2=0
				qui replace sex_2=1 if sex_id==2
				qui gen y1_snsp=ln(frac_sn/frac_sp)
				qui gen y2_epsp=ln(frac_ep/frac_sp)
				qui drop frac* new_sp_age 
				qui order iso year age sex*
				** generate missing cat
				foreach var in sn ep {
					gen missing_`var'=0
					replace missing_`var'=1 if new_`var'==.
				}
			tempfile tmp_reg_data_2
			save `tmp_reg_data_2', replace
		save "FILEPATH", replace
	
	
	// Test regressions
	   
        cap log close
		log using "ADDRESS", replace 
			use "FILEPATH", clear 
			keep if outlier==0
			
			** Try both sexes
			di in red "Both sexes" 
				sureg (y1_snsp sex_2 age0 age5 age15 age25 age45 age55 age65 sp_agestd_rt) (y2_epsp sex_2 age0 age5 age15 age25 age45 age55 age65 sp_agestd_rt)
			di in red "Males"
			preserve
				keep if sex_id==1
				sureg (y1_snsp age0 age5 age15 age25 age45 age55 age65 sp_agestd_rt) (y2_epsp age0 age5 age15 age25 age45 age55 age65 sp_agestd_rt)
			restore
			di in red "Females"
			preserve
				keep if sex_id==2
				sureg (y1_snsp age0 age5 age15 age25 age45 age55 age65 sp_agestd_rt) (y2_epsp age0 age5 age15 age25 age45 age55 age65 sp_agestd_rt)
			restore
			
		log close
	

	// Genearte predictions - Both ages combined

		 use "FILEPATH", clear	
		// Regression
			sureg (y1_snsp sex_2 age0 age5 age15 age25 age45 age55 age65 sp_agestd_rt) (y2_epsp sex_2 age0 age5 age15 age25 age45 age55 age65 sp_agestd_rt) if outlier==0
			
			** store coefficients
			
				matrix m = e(b)'
				** 
				qui gen y1_b_sex_2=m[1,1]
				qui gen y1_b_age0=m[2,1]
				qui gen y1_b_age5=m[3,1]
				qui gen y1_b_age15=m[4,1]
				qui gen y1_b_age25=m[5,1]
				qui gen y1_b_age45=m[6,1]
				qui gen y1_b_age55=m[7,1]
				qui gen y1_b_age65=m[8,1]
				qui gen y1_b_sp_agestd_rt=m[9,1]
				qui gen y1_b_cons=m[10,1]
				** 
				qui gen y2_b_sex_2=m[11,1]
				qui gen y2_b_age0=m[12,1]
				qui gen y2_b_age5=m[13,1]
				qui gen y2_b_age15=m[14,1]
				qui gen y2_b_age25=m[15,1]
				qui gen y2_b_age45=m[16,1]
				qui gen y2_b_age55=m[17,1]
				qui gen y2_b_age65=m[18,1]
				qui gen y2_b_sp_agestd_rt=m[19,1]
				qui gen y2_b_cons=m[20,1]
			
			
			** genearte predictions
			forvalues i=1/2 {
				qui gen y`i'_xb=sex_2*y`i'_b_sex_2+age0*y`i'_b_age0+age5*y`i'_b_age5+age15*y`i'_b_age15+age25*y`i'_b_age25+age45*y`i'_b_age45+age55*y`i'_b_age55+age65*y`i'_b_age65+sp_agestd_rt*y`i'_b_sp_agestd_rt+y`i'_b_cons
				qui drop y`i'_b*
			}
			qui keep iso year age sex* pop y1* y2* new* missing*
			
			** gen smear positive and smear negative
			qui gen xb_sn=exp(y1_xb)*new_sp_adj
			qui gen xb_ep=exp(y2_xb)*new_sp_adj
			qui drop y1* y2*
			
			** generate predictions
			drop if new_sp_adj2==.
			gen new_sn_age_xb=new_sn_age
			gen new_ep_age_xb=new_ep_age
			replace new_sn_age_xb=xb_sn if missing_sn==1
			replace new_ep_age_xb=xb_ep if missing_ep==1
			qui drop xb*
			qui gen CN_bact_xb=new_sn_age_xb+new_sp_adj2+new_ep_age_xb
			qui gen CN_spsn_xb=new_sn_age_xb+new_sp_adj2
			
		tempfile tmp_bothsexes_xb
		save `tmp_bothsexes_xb', replace 	
		save "FILEPATH", replace 

		
** ********************************************************************************************************************************************************************************************************************************
// cacluate an adjustment factor so that the sum of age-sex specific cases would sum up to total number of reported cases

use "FILEPATH", clear
		
collapse (sum) CN_bact_xb, by(iso3 year) fast		

tempfile total
save `total', replace


// get total number of reported cases

insheet using "FILEPATH", comma names clear

keep iso3 year c_newinc		

merge 1:1 iso3 year using `total', keep(3)nogen
		
gen adj_factor=	c_newinc/CN_bact_xb
		
tempfile adj_factor
save `adj_factor', replace

use "FILEPATH", clear
merge m:1 iso3 year using `adj_factor'

rename CN_bact_xb CN_bact_xb_old

gen CN_bact_xb = CN_bact_xb_old*adj_factor

save "FILEPATH", replace 


// prep notifications (2013-present)

// Definitions
** newrel: New and relapse cases (but only new cases if rel_in_agesex_flg=0)
** ret_rel: Relapse cases
** c_newinc: Total of new and relapse cases and cases with unknown previous TB treatment history

// get 4-5 star countries

insheet using "FILEPATH", comma clear
keep iso3 stars
tempfile star
save `star', replace

// get notifications

insheet using "FILEPATH", comma names clear

keep if year==2015

merge m:1 iso3 using `star', keep(3)nogen

keep iso3 year newrel_* rel_in_agesex_flg ret_rel c_newinc

drop newrel_m014 newrel_m15plus newrel_mu newrel_f014 newrel_f15plus newrel_fu newrel_sex* newrel_hiv* newrel_art

// ATG, BMU, BRB, and TKM have either missing data or 0 cases reported for 2013-2015 (will use predicted incidence for them instead)

drop if c_newinc==0 | c_newinc==.

drop if iso3=="TKM"

// "rel_in_agesex_flg" is coded as 0 for CHE, but CHE has no relapse cases reported (i.e., ret_rel is missing) - assume no relapse cases for CHE

// reshape

foreach var in rel {
				preserve
					qui keep iso year new`var'_m* new`var'_f*
					qui reshape long new`var'_m new`var'_f, i(iso year) j(age) string
					qui reshape long new`var'_, i(iso year age) j(sex) string
					tempfile tmp_`var'
					save `tmp_`var'', replace 
				restore
			}
			** merge datasets together
			use "`tmp_rel'", clear 
				qui replace age="0-4" if age=="04"
				qui replace age="15-24" if age=="1524"
				qui replace age="25-34" if age=="2534" 
				qui replace age="35-44" if age=="3544"
				qui replace age="45-54" if age=="4554"
				qui replace age="5-14" if age=="514"
				qui replace age="55-64" if age=="5564"
				qui replace age="65-100" if age=="65"
				qui replace sex="1" if sex=="m"
				qui replace sex="2" if sex=="f"
				qui destring sex, replace 
			
	     save "FILEPATH", replace
		 
// merge on total number of reported cases	


collapse (sum) newrel_, by(iso3 year) fast		

tempfile total_2
save `total_2', replace


// get total number of reported cases

insheet using "FILEPATH", comma names clear

keep iso3 year c_newinc		

merge 1:1 iso3 year using `total_2', keep(3)nogen
		
gen adj_factor=	c_newinc/newrel_
		
tempfile adj_factor_2
save `adj_factor_2', replace

use "FILEPATH", clear
merge m:1 iso3 year using `adj_factor_2'

gen CN_bact_2015 = newrel_*adj_factor

save "FILEPATH", replace 


	
** ********************************************************************age split 65 plus*******************************************************************************************************************************************

use "FILEPATH", clear 


// get BRA age pattern to age split 65plus

use "FILEPATH", clear
keep if age_start>=65
drop if sex=="Both"
tempfile age_pattern
save `age_pattern', replace

// get population

use "FILEPATH", clear
keep if age_group_id==1 | (age_group_id>=6 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
*******************************
merge m:1 age_group_id using "FILEPATH", keep(3)nogen
			merge m:1 location_id using "FILEPATH", keepusing(ihme_loc_id) keep(3)nogen
			rename ihme_loc_id iso3
			gen sex=""
			replace sex="Male" if sex_id==1
			replace sex="Female" if sex_id==2
			keep iso3 year age_start age_end sex sex_id population
			rename year_id year
			
			// create the 90-100 age group
			
			preserve
			keep if age_start==90 | age_start==95
			collapse(sum)pop, by(iso3 year sex sex_id) fast
			gen age_start=90
			gen age_end=100
			tempfile pop_90to100
			save `pop_90to100', replace
			restore
			
			drop if age_start==90 & age_end==94 | age_start==95 & age_end==100
			append using `pop_90to100'
			
			keep if age_start>=65
            tempfile pop_65_plus
            save `pop_65_plus', replace

*******************************
/*
merge m:1 location_id using "FILEPATH", keepusing(ihme_loc_id) keep(3)nogen
rename ihme_loc_id iso3
rename year_id year
keep if age_group_id>=18
tempfile pop_65_plus
save `pop_65_plus', replace
*/

// get prepped notifications (2015)

use "FILEPATH", clear

split age, p( - )
drop age age2
rename age1 age
destring age, replace

// rename
keep iso3 year age sex CN_bact_2015

rename sex sex_id

rename CN_bact_2015 CN_bact_xb

tempfile noti_2015
save `noti_2015', replace

// get prepped notifications (1995-2012)

// get 4-5 star countries

insheet using "FILEPATH", comma clear
keep iso3 stars
tempfile star
save `star', replace

// get notifications

use "FILEPATH", clear
keep iso3 year age sex_id CN_bact_xb

merge m:1 iso3 using `star', keep(3)nogen

drop stars

append using `noti_2015'

tempfile all_noti
save `all_noti', replace

/*
gen zero=0

replace zero=1 if CN_bact_xb==0

collapse (sum) zero, by(iso3 year) fast

drop if zero==0  /* ATG, BHS, BMU, BRB, DMA, GRD, ISL, KWT, LCA, LUX, MLT, NOR, VCT, and TKM have >=5 zeros, will use MI ratio based incidence for these locations)  */
*/

keep if age==65


//merge on population
merge 1:m iso3 year sex_id using `pop_65_plus', keep(3) nogen

//merge on age pattern
merge m:1 sex age_start age_end using `age_pattern', keep(3)nogen

rename pop sub_pop
gen rate_sub_pop=rate*sub_pop

preserve
collapse (sum) rate_sub_pop, by(iso3 year sex) fast
rename rate_sub_pop sum_rate_sub_pop
tempfile sum
save `sum', replace

restore
merge m:1 iso3 year sex using `sum', keep(3)nogen

gen cases=rate_sub_pop*(CN_bact_xb/sum_rate_sub_pop)

rename sub_pop sample_size 

sort iso3 year sex age_start

tempfile tmp_65_split
save `tmp_65_split', replace


// merge on population for those younger than 65 years
use `all_noti', clear
keep iso3 year age sex_id CN_bact_xb
gen cases=CN_bact_xb

drop if age==65


merge m:1 iso3 year age sex_id using "FILEPATH", keep(3) nogen


//rename
rename pop sample_size 
rename age age_start
gen age_end=age_start+10
replace age_end=4 if age_start==0

append using `tmp_65_split'

sort iso3 year sex age_start

drop rate rate_sub_pop sum_rate_sub_pop age

save "FILEPATH", replace


// double check to ensure no discrepancies between all-age numbers and the sum of age-sex specific numbers 
collapse (sum) cases, by(iso3 year) fast
tempfile tot_cases
save `tot_cases', replace

insheet using "FILEPATH", comma names clear

keep iso3 year c_newinc
tempfile WHO
save `WHO', replace

use `tot_cases', clear

merge 1:1 iso3 year using `WHO', keep(3)nogen

gen ratio_noti_cases=c_newinc/cases

keep iso3 year ratio_noti_cases

save "FILEPATH", replace
  

use "FILEPATH", clear

replace sex="Male" if sex_id==1
replace sex="Female" if sex_id==2

// calculate mean and se
gen mean=cases/sample_size
 
gen standard_error=sqrt(mean*(1-mean)/sample_size)

// add nid

gen nid=126384


** **********************************************************************************************************************************************************************************
// Prep subnationals
** **********************************************************************************************************************************************************************************

mdesc
gen rate=cases/sample_size
keep iso3 year age_start age_end sex sex_id rate cases
rename cases cases_national

preserve
keep if iso=="MEX"
keep if year==2012
// no national age pattern for 1990 so use the 2012 age pattern instead
expand 2, gen(new)
replace year=1990 if new==1
drop new
tempfile MEX_age_pattern
save `MEX_age_pattern', replace
restore



preserve
keep if iso=="JPN"
keep if year==2012
// no national age pattern for 2014 so use the 2012 age pattern instead
expand 2, gen(new)
replace year=2014 if new==1
drop new
tempfile JPN_age_pattern
save `JPN_age_pattern', replace
restore

preserve
keep if iso=="USA"
tempfile USA_age_pattern
save `USA_age_pattern', replace
restore


	// Pull province names 
	use "FILEPATH", clear
	// drop location_name duplicates, otherwise it won't merge
	drop if iso3=="BRA_4756"
	drop if iso3=="GEO"
	drop if iso3=="S4" | iso3=="S5"

	tempfile tmp_iso_map
	save `tmp_iso_map', replace 


			// get population

			use "FILEPATH", clear
            keep if age_group_id==1 | (age_group_id>=6 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235

            merge m:1 location_id using "FILEPATH", keepusing(ihme_loc_id) keep(3)nogen
            rename ihme_loc_id iso3
            rename year_id year
            keep if age_group_id>=18
			
			merge m:1 age_group_id using "FILEPATH", keep(3)nogen
			
			/* replace age_group_name="95 to 100" if age_group_name=="95 plus" */
			
			preserve
			
			keep if age_group_id==32 | age_group_id==235
			
			collapse (sum) population, by(iso3 year sex_id) fast
			
			gen age_group_name="90 to 100"
			
			tempfile tmp_90
			save `tmp_90', replace
			
			restore
			
			drop if age_group_id==32 | age_group_id==235
			
			append using `tmp_90'
			
			split age_group_name, p( to )
			
			rename age_group_name1 age_start
			
			rename age_group_name2 age_end
			
			destring age_start, replace
			
			destring age_end, replace

			keep iso3 year age_start age_end sex_id population
			
			rename population pop
			drop if year<1990
			
			tempfile pop_65_plus
			save `pop_65_plus', replace
			
			use "FILEPATH", clear
			drop if age>=65
			rename age age_start
			gen age_end=age_start+10
			replace age_end=4 if age_start==0
			keep iso3 year age_start age_end sex_id pop
			
			append using `pop_65_plus'
			tempfile pop_custom
			save `pop_custom', replace

	
	
	// Prep Mexico 

		** Prep mex data
	
			insheet using "FILEPATH", clear 
				drop age_end
				rename age age
				keep prov_name tb_cns age year
				rename prov location_name
				replace location="Yucatán" if location=="Yucatan"
				replace location="México" if location=="Mexico" 
				replace location="Chihuahua" if location=="Chilhuahua"
				replace location="Michoacán de Ocampo" if location=="Michoacan"
				replace location="Nuevo León" if location=="Nuevo Leon"
				replace location="Querétaro" if location=="Queretaro" 
				replace location="San Luis Potosí" if location=="SanLuis Potosi"
				replace location="Veracruz de Ignacio de la Llave" if location=="Veracruz" 
				merge m:1 location_name using `tmp_iso_map', keep(3) nogen
				collapse (sum) tb_cns, by(iso3 year)
				preserve
				
				keep if year==1990
			    rename tb_cns inc_num_new
			    tempfile inc_num_1990
			    save `inc_num_1990', replace
			    restore
			
				drop if year==1990
				preserve
				collapse (sum) tb_cns, by(year)
				rename tb_cns inc_MEX
				tempfile tmp_MEX
				save `tmp_MEX', replace
			    restore
				merge m:1 year using "`tmp_MEX'", nogen 
				gen frac_nat=tb_cns/inc_MEX
				keep iso frac year
				tempfile tmp_MEX_fracs
				save `tmp_MEX_fracs', replace 
			    
				
				use `MEX_age_pattern', clear
				collapse (sum) cases_national, by(year) fast
				merge 1:m year using `tmp_MEX_fracs', keep(3)nogen
				** apply fraction
				gen inc_num_new=cases_national*frac
			    append using `inc_num_1990'
				
			    tempfile MEX_sub
				save `MEX_sub', replace
				
						
		** Apply MEX national pattern... 

		
		// prep for age split
		use `MEX_sub', clear
		
           //merge on population
            merge 1:m iso3 year using `pop_custom', keep(3) nogen

			//merge on age pattern
			merge m:1 year age_start age_end sex_id using `MEX_age_pattern', keep(3)nogen

			rename pop sub_pop
			gen rate_sub_pop=rate*sub_pop

            preserve
            collapse (sum) rate_sub_pop, by(iso3 year) fast
            rename rate_sub_pop sum_rate_sub_pop
			tempfile sum
			save `sum', replace

			restore
			merge m:1 iso3 year using `sum', keep(3)nogen

			gen cases=rate_sub_pop*(inc_num_new/sum_rate_sub_pop)

			rename sub_pop sample_size 

			sort iso3 year sex age_start
			
			// keep necessary variables
			
			keep iso3 year age_start age_end sex sex_id cases sample_size
			
			// add nid
			
			gen nid=138133 if strpos(iso3,"MEX_")>0 & year==1990
			replace nid=138281 if strpos(iso3,"MEX_")>0 & year==2011
			replace nid=138283 if strpos(iso3,"MEX_")>0 & year==2012
			
			 			
	save "FILEPATH", replace
	


	
	/*
	// Prep GBR

		** Prep data
	
			insheet using "FILEPATH", clear
				keep iso year tb_cases
				
				preserve
				collapse (sum) tb_cases, by(year)
				rename tb_cases inc_GBR
				tempfile tmp_GBR
				save `tmp_GBR', replace
			    restore
				merge m:1 year using "`tmp_GBR'", nogen 
				gen frac_nat=tb_cases/inc_GBR
				keep iso frac year
				tempfile tmp_GBR_fracs
				save `tmp_GBR_fracs', replace 
			    
				
				use `GBR_age_pattern', clear
				collapse (sum) cases_national, by(year) fast
				merge 1:m year using `tmp_GBR_fracs', keep(3)nogen
				** apply fraction
				gen inc_num_new=cases_national*frac
				
				
				//merge on population
            merge 1:m iso3 year using `pop_custom', keep(3) nogen

			//merge on age pattern
			merge m:1 year age_start age_end sex using `GBR_age_pattern', keep(3)nogen

			rename pop sub_pop
			gen rate_sub_pop=rate*sub_pop

            preserve
            collapse (sum) rate_sub_pop, by(iso3 year) fast
            rename rate_sub_pop sum_rate_sub_pop
			tempfile sum
			save `sum', replace

			restore
			merge m:1 iso3 year using `sum', keep(3)nogen

			gen cases=rate_sub_pop*(inc_num_new/sum_rate_sub_pop)

			rename sub_pop sample_size 

			sort iso3 year sex age_start
			
			// keep necessary variables
			
			keep iso3 year age_start age_end sex cases sample_size
			
			// add nid
			
			gen nid=138151 if strpos(iso3,"GBR_")>0 
			
			 // add iso3 code of parent to merge on cdr
			
			gen parent="GBR"
				
		save "FILEPATH", replace
	*/

	// Prep JPN

		** Prep data
	
			insheet using "FILEPATH", comma names clear
				gen year=2012
				
				tempfile JPN_2012
				save `JPN_2012', replace
			/*	
			insheet using "FILEPATH", comma names clear
				gen year=2013
				
				tempfile JPN_2013
				save `JPN_2013', replace
			*/	
			insheet using "FILEPATH", comma names clear
				gen year=2014
				
				tempfile JPN_2014
				save `JPN_2014', replace
				
		    use `JPN_2012', clear
			/* append using `JPN_2013' */
			append using `JPN_2014'
			
			rename cases inc_num_new
			
		    replace location_name="Hokkaid?" if location_name=="Hokkaido"	
			replace location_name="Hy?go" if location_name=="Hyogo"	
		    replace location_name="K?chi" if location_name=="Kochi"	
			replace location_name="Ky?to" if location_name=="Kyoto"
			replace location_name="Niagata" if location_name=="Niigata"	
			replace location_name="Ôita" if location_name=="Oita"	
			replace location_name="?saka" if location_name=="Osaka"	
			replace location_name="T?ky?" if location_name=="Tokyo"	
			
						
			//merge on location_name
			merge m:1 location_name using `tmp_iso_map', keep(3)nogen
			
			/*
			preserve
				
				keep if year==2014
			    rename cases inc_num_new
			    tempfile inc_num_2014
			    save `inc_num_2014', replace
			    restore
			
				drop if year==2014

			preserve
				collapse (sum) cases, by(year)
				rename cases inc_JPN
				tempfile tmp_JPN
				save `tmp_JPN', replace
			    restore
				merge m:1 year using "`tmp_JPN'", nogen 
				gen frac_nat=cases/inc_JPN
				keep iso frac year
				tempfile tmp_JPN_fracs
				save `tmp_JPN_fracs', replace 
			    
				
				use `JPN_age_pattern', clear
				collapse (sum) cases_national, by(year) fast
				merge 1:m year using `tmp_JPN_fracs', keep(3)nogen
				** apply fraction
				gen inc_num_new=cases_national*frac
				append using `inc_num_2014'
				*/
				//merge on population
            merge 1:m iso3 year using `pop_custom', keep(3) nogen

			//merge on age pattern
			merge m:1 year age_start age_end sex_id using `JPN_age_pattern', keep(3)nogen

			rename pop sub_pop
			gen rate_sub_pop=rate*sub_pop

            preserve
            collapse (sum) rate_sub_pop, by(iso3 year) fast
            rename rate_sub_pop sum_rate_sub_pop
			tempfile sum
			save `sum', replace

			restore
			merge m:1 iso3 year using `sum', keep(3)nogen

			gen new_cases=rate_sub_pop*(inc_num_new/sum_rate_sub_pop)

			rename sub_pop sample_size 

			sort iso3 year sex age_start
			
			// keep necessary variables
			
		    keep iso3 year age_start age_end sex sex_id new_cases sample_size
			rename new_cases cases
			
						
			// add nid
			
			gen nid=205877
			replace nid=206136 if year==2013
			replace nid=206141 if year==2014
			
			
				
		save "FILEPATH", replace
	
	
	
	// append files

use "FILEPATH", clear

// keep DisMod years (keep 2006 instead of 2005 due to less missing data)

keep if inlist(year,1990,1995,2000,2006,2010,2015)


		append using "FILEPATH" 
		    append using "FILEPATH"
		
		// calculate mean and se
			gen mean=cases/sample_size
 
			gen standard_error=sqrt(mean*(1-mean)/sample_size)
			tempfile subnationals
			save `subnationals', replace
	


** add NIDs for subnationals
           
			replace nid=138133 if strpos(iso3,"MEX_")>0 & year==1990
			replace nid=138281 if strpos(iso3,"MEX_")>0 & year==2011
			replace nid=138283 if strpos(iso3,"MEX_")>0 & year==2012
			
save "FILEPATH", replace


// prep Norway

import excel using "FILEPATH", firstrow clear

drop if cases==.

gen nid=.
replace nid=273908 if year==1977
replace nid=273909 if year==1978
replace nid=273910 if year==1979
replace nid=273911 if year==1980
replace nid=273912 if year==1981
replace nid=273913 if year==1982
replace nid=273915 if year==1983
replace nid=273916 if year==1984
replace nid=272555 if year==1985
replace nid=272560 if year==1986
replace nid=272561 if year==1987
replace nid=272562 if year==1988
replace nid=272563 if year==1989
replace nid=272566 if year==1990
replace nid=272567 if year==1991
replace nid=271447 if year==1992
replace nid=271723 if year==1993
replace nid=271724 if year==1994
replace nid=271725 if year==1995
replace nid=271726 if year==1996
replace nid=271727 if year==1997
replace nid=271728 if year==1998
replace nid=271729 if year==1999
replace nid=271730 if year==2000
replace nid=271731 if year==2001
replace nid=271732 if year==2002
replace nid=271733 if year==2003
replace nid=271734 if year==2004
replace nid=271735 if year==2005
replace nid=271736 if year==2006
replace nid=271737 if year==2007
replace nid=271738 if year==2008
replace nid=271739 if year==2009
replace nid=271740 if year==2010
replace nid=271741 if year==2011
replace nid=271742 if year==2012
replace nid=271743 if year==2013
replace nid=271744 if year==2014
replace nid=271745 if year==2015

replace age_start=15 if age_start==14

replace age_start=25 if age_start==24

replace age_start=55 if age_start==54

replace age_end=100 if age_end==99

merge m:1 age_start age_end using "FILEPATH", keep(3)nogen

gen year_id=year

gen sex_id=.

replace sex_id=1 if sex=="Male"

replace sex_id=2 if sex=="Female"

merge 1:1 location_id year_id age_group_id sex_id using "FILEPATH", keepusing(population) keep(3)nogen

rename population sample_size


// calculate mean and se
gen mean=cases/sample_size
 
gen standard_error=sqrt(mean*(1-mean)/sample_size)

drop if mean==0

// keep DisMod years  

keep if inlist(year,1990,1995,2000,2005,2010,2015)


save "FILEPATH", replace

keep age_start age_end year sex cases location_id ihme_loc_id location_name nid sex_id sample_size mean standard_error

tempfile NOR
save `NOR', replace

// prep Sweden

import excel using "FILEPATH", firstrow clear

drop if cases==.

gen nid=229784

replace age_end=100 if age_end==99

merge m:1 age_start age_end using "FILEPATH", keep(3)nogen

gen year_id=year

gen sex_id=.

replace sex_id=1 if sex=="Male"

replace sex_id=2 if sex=="Female"

merge 1:1 location_id year_id age_group_id sex_id using "FILEPATH", keepusing(population) keep(3)nogen

rename population sample_size


// calculate mean and se
gen mean=cases/sample_size
 
gen standard_error=sqrt(mean*(1-mean)/sample_size)

drop if mean==0


// keep DisMod years  

keep if inlist(year,1990,1995,2000,2005,2010,2013)

save "FILEPATH", replace

keep age_start age_end year sex cases location_id ihme_loc_id location_name nid sex_id sample_size mean standard_error

tempfile SWE
save `SWE', replace

** ****************************************************************************************************************************************************************************************
// append files

use "FILEPATH", clear
keep age_start age_end year sex cases iso3 nid sex_id sample_size mean standard_error


rename iso3 ihme_loc_id


merge m:1 ihme_loc_id using "FILEPATH", keepusing(location_id location_name) keep(3)nogen

replace nid=126384 if nid==.

// calculate mean and se
replace mean=cases/sample_size
 
replace standard_error=sqrt(mean*(1-mean)/sample_size)

drop if ihme_loc_id=="Norway"

// Add Norway and Sweden subnationals

append using `NOR'

append using `SWE'

//format for DisMod
gen modelable_entity_id=1175
gen modelable_entity_name="Tuberculosis"

gen iso3=ihme_loc_id

drop if mean==0

// add nids
			replace nid=138133 if strpos(iso3,"MEX_")>0 & year==1990
			replace nid=138281 if strpos(iso3,"MEX_")>0 & year==2011
			replace nid=138283 if strpos(iso3,"MEX_")>0 & year==2012
			


gen row_num=.
gen parent_id=.
gen input_type="adjusted"
/* gen modelable_entity_id=1175
gen modelable_entity_name="Tuberculosis" */
gen underlying_nid=.

gen underlying_field_citation_value=""
gen field_citation_value=""
gen file_path=""
gen page_num=.
gen table_num=.
gen source_type="Case notifications - other/unknown"
gen smaller_site_unit=0 
gen site_memo=""
gen representative_name="Nationally representative only"
replace representative_name="Representative for subnational location only" if strpos(iso3,"CHN_")>0 | strpos(iso3,"MEX_")>0 | strpos(iso3,"GBR_")>0 | strpos(iso3,"USA_")>0 | strpos(iso3,"JPN_")>0 | strpos(iso3,"SWE_")>0

gen urbanicity_type=""
replace urbanicity_type="Unknown"
replace urbanicity_type="Unknown"

gen sex_issue=0

gen year_issue=0

gen age_issue=0
gen age_demographer=0
gen lower=.
gen upper=.

gen measure="incidence"
gen effective_sample_size=.
 															
gen design_effect=.
gen unit_type="Person"
gen unit_value_as_published=1
gen measure_issue=0
gen measure_adjustment=.
gen uncertainty_type="Standard error"
gen uncertainty_type_value=.
gen recall_type="Point"
gen recall_type_value=""
gen sampling_type=""
gen response_rate=.
gen case_name=""
gen case_definition=""
gen case_diagnostics=""
gen group=""
gen specificity=""
gen group_review=.
gen note_modeler=""
gen note_SR=""	
gen extractor="hmwek"
gen is_outlier=0
//create covariates
gen cv_subnational=0
replace cv_subnational=1 if strpos(iso3,"CHN_")>0 | strpos(iso3,"MEX_")>0 | strpos(iso3,"GBR_")>0 | strpos(iso3,"USA_")>0 | strpos(iso3,"JPN_")>0
gen cv_diag_smear=0
gen cv_diag_culture=0
gen cv_screening=0
gen cv_case_detection_rate=0
gen data_sheet_file_path=""


gen year_start=year 
gen year_end=year
sort location_name year_start sex age_start
keep row_num parent_id	input_type	modelable_entity_id	modelable_entity_name underlying_nid nid underlying_field_citation_value field_citation_value file_path page_num	table_num	source_type	location_name	location_id	ihme_loc_id	smaller_site_unit	site_memo	representative_name	urbanicity_type	sex	sex_id sex_issue	year_start	year_end	year_issue	age_start	age_end	age_issue	age_demographer	measure	mean	lower	upper	standard_error	effective_sample_size	cases	sample_size	design_effect	unit_type	unit_value_as_published	measure_issue	measure_adjustment	uncertainty_type uncertainty_type_value	recall_type	recall_type_value	sampling_type	response_rate	case_name	case_definition	case_diagnostics group specificity group_review note_modeler note_SR extractor is_outlier cv_* data_sheet_file_path cv_case_detection_rate

order row_num parent_id	input_type	modelable_entity_id	modelable_entity_name underlying_nid nid underlying_field_citation_value field_citation_value file_path page_num	table_num	source_type	location_name	location_id	ihme_loc_id	smaller_site_unit	site_memo	representative_name	urbanicity_type	sex	sex_id sex_issue	year_start	year_end	year_issue	age_start	age_end	age_issue	age_demographer	measure	mean	lower	upper	standard_error	effective_sample_size	cases	sample_size	design_effect	unit_type	unit_value_as_published	measure_issue	measure_adjustment	uncertainty_type uncertainty_type_value	recall_type	recall_type_value	sampling_type	response_rate	case_name	case_definition	case_diagnostics group specificity group_review note_modeler note_SR extractor is_outlier cv_* data_sheet_file_path cv_case_detection_rate 

tempfile temp_prep
save `temp_prep', replace


// add BRA and USA

append using "FILEPATH"

append using "FILEPATH"



keep row_num parent_id	input_type	modelable_entity_id	modelable_entity_name underlying_nid nid underlying_field_citation_value field_citation_value file_path page_num	table_num	source_type	location_name	location_id	ihme_loc_id	smaller_site_unit	site_memo	representative_name	urbanicity_type	sex	sex_id sex_issue	year_start	year_end	year_issue	age_start	age_end	age_issue	age_demographer	measure	mean	lower	upper	standard_error	effective_sample_size	cases	sample_size	design_effect	unit_type	unit_value_as_published	measure_issue	measure_adjustment	uncertainty_type uncertainty_type_value	recall_type	recall_type_value	sampling_type	response_rate	case_name	case_definition	case_diagnostics group specificity group_review note_modeler note_SR extractor is_outlier cv_* data_sheet_file_path 

order row_num parent_id	input_type	modelable_entity_id	modelable_entity_name underlying_nid nid underlying_field_citation_value field_citation_value file_path page_num	table_num	source_type	location_name	location_id	ihme_loc_id	smaller_site_unit	site_memo	representative_name	urbanicity_type	sex	sex_id sex_issue	year_start	year_end	year_issue	age_start	age_end	age_issue	age_demographer	measure	mean	lower	upper	standard_error	effective_sample_size	cases	sample_size	design_effect	unit_type	unit_value_as_published	measure_issue	measure_adjustment	uncertainty_type uncertainty_type_value	recall_type	recall_type_value	sampling_type	response_rate	case_name	case_definition	case_diagnostics group specificity group_review note_modeler note_SR extractor is_outlier cv_* data_sheet_file_path 


// prep for ME id 9422

replace modelable_entity_id=.
replace modelable_entity_name=""

gen seq=.

gen seq_parent=.

keep seq seq_parent input_type underlying_nid nid underlying_field_citation_value field_citation_value file_path page_num	table_num	source_type	location_name	location_id	ihme_loc_id	smaller_site_unit	site_memo	representative_name	urbanicity_type	sex	sex_id sex_issue	year_start	year_end	year_issue	age_start	age_end	age_issue	age_demographer	measure	mean	lower	upper	standard_error	effective_sample_size	cases	sample_size	design_effect	unit_type	unit_value_as_published	measure_issue	measure_adjustment	uncertainty_type uncertainty_type_value	recall_type	recall_type_value	sampling_type	response_rate	case_name	case_definition	case_diagnostics group specificity group_review note_modeler note_SR extractor is_outlier cv_* 

order seq seq_parent input_type	underlying_nid nid underlying_field_citation_value field_citation_value file_path page_num	table_num	source_type	location_name	location_id	ihme_loc_id	smaller_site_unit	site_memo	representative_name	urbanicity_type	sex	sex_id sex_issue	year_start	year_end	year_issue	age_start	age_end	age_issue	age_demographer	measure	mean	lower	upper	standard_error	effective_sample_size	cases	sample_size	design_effect	unit_type	unit_value_as_published	measure_issue	measure_adjustment	uncertainty_type uncertainty_type_value	recall_type	recall_type_value	sampling_type	response_rate	case_name	case_definition	case_diagnostics group specificity group_review note_modeler note_SR extractor is_outlier cv_* 

replace sex="Male" if sex_id==1 & sex==""
replace sex="Female" if sex_id==2 & sex==""

export excel using "FILEPATH", firstrow(variables) nolabel replace

** *****************************************************************************************************************************************************************
// adjust TB prevalence for ep using ST-GPR ep proportions

use "FILEPATH", clear

// generate ep inflation factor
gen ep_inflat=1+(gpr_mean/(1-gpr_mean))

// generate se for the ep inflation factor
gen se_gpr=(gpr_upper-gpr_lower)/(2*1.96)

gen se_ep_inflat=ep_inflat*sqrt(2*(se_gpr/gpr_mean)^2)

gen mean_inflated=mean*ep_inflat

gen se_intermediate=mean_inflated*sqrt((standard_error/mean)^2+(se_ep_inflat/ep_inflat)^2)

replace group_review=0 if mean==0

rename mean mean_raw

rename standard_error standard_error_raw

gen mean=mean_inflated

gen standard_error=se_intermediate

replace cases=mean*sample_size

replace group_review=0 if mean_raw==0

save "FILEPATH", replace


