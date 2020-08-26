/*====================================================================

----------------------------------------------------------------------
     
Output:           Estimate prevalence of Onchocerciasis and sequelae in the Americas and overwrite prevalence draws
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local ADDRESS "FILEPATH"
		set odbcmgr ADDRESS
	}
	else if c(os) == "Windows" {
		local ADDRESS "FILEPATH"
	}
	

* Directory Paths

	local gbd = "FILEPATH
	*model step
	local step 04
	*cluster root
	local clusterRoot "FILEPATH`gbd'"
	*directory for code
	local code_dir "`'FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH'"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "`clusterRoot'/FILEPATH/`step'"
	*local temporary directory for other things:
	local local_tmp_dir "`localRoot'/FILEPATH`step'"

	local out_dir "`clusterRoot'/FILEPATH"
	*directory for logs
	local log_dir "`clusterRoot'/logs/`step'"
	*directory for progress files
	local progress_dir "`clusterRoot'/FILEPATH/`step'"

* Make and Clear Directories
	
	capture shell mkdir /FILEPATH/
	capture shell mkdir /FILEPATH/`gbd'
	capture shell mkdir /FILEPATH/`gbd'/tmp
	capture shell mkdir /FILEPATH/`gbd'/tmp/`step'
	capture shell mkdir /FILEPATH/`gbd'/logs
	capture shell mkdir /FILEPATH/`gbd'/logs/`step'
	capture shell mkdir /FILEPATH/`gbd'/progress
	capture shell mkdir /FILEPATH/`gbd'/progress/`step'



*Directory for standard code files
	adopath + "FILEPATH"


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/FILEPATH", replace
	***********************	

	*print macros in log
	macro dir

/*====================================================================
                        1: Set Up Variables
====================================================================*/

	
	use "`in_dir'/FILEPATH", clear

*--------------------1.1: Create Needed Variables

	*Combine foci
		replace FociCode=round(FociCode,0.1)
		tostring FociCode , replace force
		destring FociCode , replace
		gen double combofoci=FociCode
		gen combofociname=Foci
		//South Chiapas, Mexico + Cuilco/Huehuetenanco, Guatemala
		replace combofoci=77 if inlist(FociCode,1.2,2.1)
		replace combofociname="South Chiapas|MEX + Cuilco|GTM" if combofoci==77
		//All Ecuador
		replace combofoci=88 if inlist(FociCode, 6)
		replace combofociname="All Ecuador Foci" if combofoci==88
		//South, Venezuela + Brazil
		replace combofoci=99 if inlist(FociCode,3.3,4.1)
		replace combofociname="South|VEN + Main|BRA" if combofoci==99

	*Extra combined foci
		gen double combofoci2=.
		replace combofoci2=combofoci
		replace combofoci2=111 if inlist(combofoci,77,1.1,1.3)
			*All MEX + GTM/Cuilco
		replace combofoci2=222 if inlist(combofoci,2.2,2.3,2.4)
			*All GTM except Cuilco
		
	**MAKE FACTOR VARIABLES
		gen double factorcombofoci=combofoci*10
		gen double factorcombofoci2=combofoci2*10
		
	*SPLINES
		mkspline MDAs=MDAyears, cubic displayknots
			
		mkspline YearS=year, cubic displayknots
		mkspline ageS=age_mid, cubic displayknots
			*knots:  .009589       12.5       42.5       67.5       92.5 
		mkspline ageS2=age_mid, cubic knots(0 5 30 45 60 100)
		mkspline ageS3=age_mid, cubic knots(0 5 15 30 60 100)
		mkspline ageS4=age_mid, cubic knots(0 5 15 30 100)
		mkspline ageS5=age_mid, cubic nknots(3) displayknots
		mkspline ageS6=age_mid, cubic nknots(4) displayknots
		mkspline ageS7=age_mid, cubic knots(0 15 30 100)

*--------------------1.2: Format Variables

	*Code categorical variables
		encode combofociname,gen(cfcode)
		encode Foci,gen(fcode)
		gen int stringcombofcode = int(combofoci*10)
		gen int stringfcode = int(FociCode*10)

/*====================================================================
                        2: Run Regression Model
====================================================================*/


*--------------------2.1: Model

	meglm mean i.stringfcode MDAyears ageS7*, family(binomial) link(logit) noconstant || combofoci:
			

*--------------------2.2: Vetting

	
/*====================================================================
                        3: Process Predictions
====================================================================*/


*--------------------3.1: Get Stata Predictions for Comparison

	
		predict fixed, xb nooffset fixedonly
		predict fixedSe, stdp nooffset fixedonly
		predict random, remeans reses(randomSe) 
		gen predFull = invlogit(fixed + random)

		sum random if e(sample)
		replace random = `r(mean)' if missing(random)
		local seTemp = `r(sd)'
		sum randomSe if e(sample)
		replace randomSe = sqrt(`r(mean)'^2 + `seTemp'^2) if missing(randomSe)

*--------------------3.2: Predict Fixed + Random Effects

	*Pull draws of fixed effects from matrix of Betas 
		*Get matrix
			matrix m = e(b)'
			matrix m = m[1..17,1]
		*Get the variance-covariance matrix
			matrix C = e(V)
			matrix C = C[1..17, 1..17]

	*Get list of variables names in matrix (covariates)
		local covars: rownames m
		local num_covars: word count `covars'

	*Get list of betas associated with the covariates in the matrix
		local betas
		forvalues j = 1/`num_covars' {
			local this_covar: word `j' of `covars'
			local covar_fix=subinstr("`this_covar'","b.","",.)
			local covar_rename=subinstr("`covar_fix'",".","",.)
			
			
			local betas `betas' b_`covar_rename'
		}

		*checkpoint
		macro dir

	
		drawnorm `betas', means(m) cov(C)


		forvalues j = 1/1000 {

			display in red ". `j' " _continue

			quietly {
			generate double draw_`j' = 0

			
		

				**Continuous variables
					foreach var in MDAyears ageS71 ageS72 ageS73 {
						replace draw_`j' = draw_`j' + `var' * b_`var'[`j']
					}

				**Foci
					replace draw_`j' = draw_`j' + b_11stringfcode[`j'] if stringfcode==11
					replace draw_`j' = draw_`j' + b_12stringfcode[`j'] if stringfcode==12
					replace draw_`j' = draw_`j' + b_13ostringfcode[`j'] if stringfcode==13
					replace draw_`j' = draw_`j' + b_21ostringfcode[`j'] if stringfcode==21
					replace draw_`j' = draw_`j' + b_22ostringfcode[`j'] if stringfcode==22
					replace draw_`j' = draw_`j' + b_23stringfcode[`j'] if stringfcode==23
					replace draw_`j' = draw_`j' + b_24stringfcode[`j'] if stringfcode==24
					replace draw_`j' = draw_`j' + b_31stringfcode[`j'] if stringfcode==31
					replace draw_`j' = draw_`j' + b_32stringfcode[`j'] if stringfcode==32
					replace draw_`j' = draw_`j' + b_33stringfcode[`j'] if stringfcode==33
					replace draw_`j' = draw_`j' + b_41stringfcode[`j'] if stringfcode==41
					replace draw_`j' = draw_`j' + b_51stringfcode[`j'] if stringfcode==51
					replace draw_`j' = draw_`j' + b_60stringfcode[`j'] if stringfcode==60


					replace draw_`j' = invlogit(draw_`j' + rnormal(random, randomSe))
		
			}
		}
		
	*Make draws 1-1000 into 0-999
		rename draw_1000 draw_0         

	
		egen drawMean = rowmean(draw_*)  
		
		forvalues i = 0 / 999 {
			quietly {
				replace draw_`i' = draw_`i' * predFull / drawMean
			}
		}

*--------------------3.3: Calculate Prevalence in Total Population By Foci

	tempfile predicted
	save `predicted', replace
	
	
		keep if pop_env==1
		
			
	
		expand 2,gen(sex_id)
		replace sex_id=2 if sex_id==0
	
	*Limit to gbd years instead of all years
		keep if inlist(year, 1990,1995,2000,2005,2010,2017)
		
	*Join with PAR estimates
		rename year year_id
		merge 1:1 location_id FociCode age_group_id year_id sex_id using "`in_dir'/FILEPATH", nogen
			
	
	*Convert to case space using PAR: cases=(prevalence in PAR)*PAR
		forvalues i = 0 / 999 {
			local random = rnormal(0,1)
			replace draw_`i' =  draw_`i'* (PAR_`i')
		}
		
		tempfile predcases
		save `predcases', replace

*--------------------3.4: Hard-Code Zeroes for All Years Post Official Elimination

	*Preserve and open elimination to format
		preserve
			use "`in_dir'FILEPATH", clear
			rename year year_id
			rename mean elimination
			keep FociCode year_id elimination
			
			tempfile elim
			save `elim', replace
		restore
	
	*Merge with elimination data
		merge m:1 FociCode year using `elim', nogen
	
	*Replace all draws with 0 if in eliminated years
		forval d=0/999{
			replace draw_`d'=0 if elimination==0
		}

*--------------------3.5: Calculate prevalence in GBD locations from Foci Prevalence

	*Sum cases accross location_ids (combine the cases across foci within a single gbd location)
		run "FILEPATH"
		fastcollapse draw_*, by(location_id year age_group_id sex_id) type(sum)
	
	*Merge with population
		tempfile cases
		save `cases', replace
		
		merge 1:1 location_id age_group_id sex_id year_id using "`in_dir'FILEPATH", nogen
		drop if location_id==.
	
	*Re-calculate prevalence in the total population
		forvalues i = 0 / 999 {
			replace draw_`i' =  draw_`i' / population
		}
		
	*Save
		save "`tmp_dir'/FILEPATH", replace


/*====================================================================
                        4: Perform Sequelae Split
====================================================================*/

	
*--------------------4.1: Get results of sequeale models from database

	tempfile globalseq
	local a 1
	
	foreach meid in `meids'{
		
		get_model_results, gbd_team("epi") gbd_round_id("3") gbd_id("`meid'") location_id("1") year_id("`gbdyears' 2015") age_group_id("`gbdages'") sex_id("-1") clear
		
		sleep 10
		rename mean meid`meid'
		keep location_id year_id age_group_id sex_id meid*
		
		if `a'>1{
			merge 1:1 location_id year_id age_group_id sex_id using `globalseq', nogen
		}
		save `globalseq', replace
		local ++a
	
	}

*--------------------4.2: Calculate proportions to split sequelae

	foreach meid in `seqmeids'{
		gen splitter`meid'= meid`meid'/meid`parentmeid'
		replace splitter`meid'=0 if meid`parentmeid'==0
	}
	
	replace year_id=2016 if year_id==2015
	
	expand 5 if age_group_id==20
	bysort year_id sex_id age_group_id: gen id=_n
	replace age_group_id=30 if age_group_id==20 & id==2
	replace age_group_id=31 if age_group_id==20 & id==3
	replace age_group_id=32 if age_group_id==20 & id==4
	replace age_group_id=235 if age_group_id==20 & id==5
	drop id
	
	keep year_id age_group_id sex_id splitter*
		
	tempfile splitter
	save `splitter', replace	
	save "`tmp_dir'/FILEPATH", replace	

	
*--------------------4.3: Apply splits to my estimates and save each meid in its own tempfile

	merge 1:m age_group_id year_id sex_id using  "`tmp_dir'/FILEPATH", nogen
		

	foreach meid in `seqmeids'{
			
		di in red "Calculating Sequelae Split for Meid `meid'"
		
		quietly{
		preserve
			keep location_id year_id age_group_id sex_id draw* splitter`meid'
			
			forval x=0/999{
				replace draw_`x'=draw_`x'*splitter`meid'
			}
	
			save "`tmp_dir'/FILEPATH", replace
		restore
		}
	}


/*====================================================================
                        5: EXPORT FILES
====================================================================*/


	*Output Draw Files

	local meids ADDRESS


	foreach meid in `meids'{
		
		di in red "Output Draw Files for Meid `meid'"
		
		quietly{
		foreach location in `ameronchoids'{
			
			use  "`tmp_dir'FILEPATH", clear
			
			keep if location_id==`location'
			gen modelable_entity_id=`meid'
			gen measure_id=5
			
			*Hard code younger ages to zero 
			forval i=0/999{
				replace draw_`i'=0 if age_group_id<4
			}
			
			keep age_group_id location_id year_id sex_id modelable_entity_id draw* measure_id
			order modelable_entity_id measure_id location_id year_id age_group_id sex_id  draw* 
			
			outsheet using "`out_dir'/FILEPATH", comma replace
			
			sleep 15
		}	
		}
		
		sleep 30
	}


/*====================================================================
                        7: SAVE RESULTS
====================================================================*/




log close
exit
/* End of do-file */

