/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER         
Output:           Prepare all forms of data for estimating Oncho prevalence
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "J:"
	}
	

* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2016"
	*model step
	local step 04
	*local root
	local localRoot "FILEPATH"
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*local temporary directory for other things:
	local local_tmp_dir "FILEPATH"
	*directory for output of draws > on ihme/scratch
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

* Make and Clear Directories

	*make all directories
	local make_dirs code in tmp local_tmp out log progress_dir
	foreach dir in `make_dirs' {
		capture mkdir `dir'_dir
	}
	*clear tempfiles, logs and progress files
	foreach dir in log progress tmp {
		capture cd `dir'_dir
		capture shell rm *
	}


*Directory for standard code files
	adopath + `j'/temp/central_comp/libraries/current/stata


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/03_estimate_americas_log_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir


/*====================================================================
                        1: Get GBD Info
====================================================================*/


*--------------------1.1: Demographics

	get_demographics,gbd_team(epi) clear
	local gbdyears `r(year_ids)'
	local gbdages `r(age_group_ids)'
	local gbdsexes `r(sex_ids)'

*--------------------1.2: Location Metadata

	*Get GBD location metadata
		get_location_metadata,location_set_id(35) clear
		save "tmp_dir'/metaraw.dta", replace

	*Define list of country ids for oncho countries in the Americas
		local ameronchoids 4771 4649 4662 4752 128 133 122 125

*--------------------1.3: Age Metadata

	*Connect to database
		noisily di "{hline}" _n _n "Connecting to the database"
		create_connection_string, database(shared)
		local shared = r(conn_string)

	*Create table of all ages in oncho data and/or needed for gbd estimates and their age_start and age_end
		odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (`=subinstr("`gbdages'", " ", ",", .)')") `shared' clear
		replace age_end=100 if age_end==125
		save "`tmp_dir'/ages.dta", replace

*--------------------1.4: Population

	get_population, location_id("`ameronchoids'") sex_id("`gbdsexes'") age_group_id("`gbdages'") year_id("`gbdyears'") clear
	save "`tmp_dir'/getpop.dta", replace

*--------------------1.5: Compile Skeleton

	*Make table of gbd estimate subnationals with map to country ids
		use "tmp_dir'/metaraw.dta", clear
		split path_to_top_parent,p(",")
		rename path_to_top_parent4 country_id
		keep location_id location_name country_id
		destring country_id, replace
			
	*Add information about population and age group start/ends		
		merge 1:m location_id using "`tmp_dir'/getpop.dta", nogen keep(matched using)
		merge m:1 age_group_id using "`tmp_dir'/ages.dta", nogen
	
	*Create variable to identify skeleton rows
		gen pop_env=1

	*Save
		save "`in_dir'/skeleton.dta", replace

/*====================================================================
                        2: Calculate Population Split Parameters
====================================================================*/


*--------------------2.1: Get proportion of the population in each age group to do age-sex-split of PAR later
*Use this to give demographic structure to PAR later

	use "`tmp_dir'/skeleton.dta", clear

	*Calculate proportional split
		bysort location_id year_id: egen poplocyear=total(population)
		gen agesexsplit=.
		
		foreach sex in `gbdsexes'{
			foreach ageG in `gbdages'{
				gen pop_`sex'_`ageG'=population/poplocyear if age_group_id==`ageG' & sex_id==`sex'
				replace agesexsplit=pop_`sex'_`ageG' if agesexsplit==.
				drop pop_`sex'_`ageG'
			}
		}

	*Save
		save "`tmp_dir'/skeletonplus.dta", replace
		

*--------------------2.2: Get proportion of foci 4.1 attribuatable to roraima and amazonas by population
*Use this to split out population in foci that cross multiple gbd locations

	*Calculate proportional split
		keep if inlist(location_id,4752,4771)
		bysort year_id: egen popyr41=total(population)
		bysort year_id location_id: egen poplocyr41=total(population)
		
	*Format
		gen RoraimaProp=poplocyr41/popyr41 if location_id==4771
		gen AmazonasProp=poplocyr41/popyr41 if location_id==4752
		
		keep location_id location_name country_id year_id age_group_id sex_id RoraimaProp AmazonasProp

	*Save
		save "`tmp_dir'/pop_foci41.dta", replace


/*====================================================================
                        3: Create Foci<-->GBD Location Map
====================================================================*/


*--------------------3.1: Setup

	use  "`tmp_dir'/skeletonplus.dta", clear
	bysort location_id: gen id=_n
	keep if id == 1
	keep location_id location_name country_id 
	gen double FociCode=.

*--------------------3.2: Manually define Foci Codes

	*Mexico
		replace FociCode=1.3 if location_id==4662
		expand 2 if location_id==4649,gen(temp1)
		replace FociCode=1.1 if location_id==4649 & temp1==1
		replace FociCode=1.2 if location_id==4649 & temp1==0
	*Guatemala
		expand 4 if location_id==128
		bysort location_id: gen temp2=_n
		replace FociCode=2.1 if location_id==128 & temp2==1
		replace FociCode=2.2 if location_id==128 & temp2==2
		replace FociCode=2.3 if location_id==128 & temp2==3
		replace FociCode=2.4 if location_id==128 & temp2==4
	*Venezuela
		expand 3 if location_id==133
		bysort location_id: gen temp3=_n
		replace FociCod=3.1 if location_id==133 & temp3==1
		replace FociCod=3.2 if location_id==133 & temp3==2
		replace FociCod=3.3 if location_id==133 & temp3==3
	*Brazil
		replace FociCode=4.1 if location_id==4752
		replace FociCode=4.1 if location_id==4771
	*Colombia
		replace FociCode=5.1 if location_id==125
	*Ecuador
		replace FociCode=6 if location_id==122
		*All Ecuador sub-foci combined

*--------------------3.3: Format & Save

	*Label foci for ease of analysis
		gen Foci=""
		replace Foci="MEX: North Chiapas" if FociCode==1.1
		replace Foci="MEX: South Chiapas" if FociCode==1.2
		replace Foci="MEX: Oaxaca" if FociCode==1.3
		replace Foci="GTM: Huehuetenango" if FociCode==2.1
		replace Foci="GTM: Solola, Such & Chim" if FociCode==2.2
		replace Foci="GTM: Guatemala & Escuintla" if FociCode==2.3
		replace Foci="GTM: Santa Rosa" if FociCode==2.4
		replace Foci="VEN: North Central" if FociCode==3.1
		replace Foci="VEN: North East" if FociCode==3.2
		replace Foci="VEN: South" if FociCode==3.3
		replace Foci="BRA: Amazonas" if FociCode==4.1
		replace Foci="COL: Lopez de Micay" if FociCode==5.1
		replace Foci="ECU: Foci + Sub-Foci" if FociCode==6

	*Merge back with metadata to get country names in addition to subnational names
		rename location_id origin_location_id
		rename location_name origin_location_name
		rename country_id location_id
		merge m:1 location_id using "tmp_dir'/metaraw.dta",nogen keep(matched master)
		rename location_name country_name
		rename location_id country_id
		rename origin_location_id location_id
		rename origin_location_name location_name

	*Save
		keep FociCode Foci location_id location_name country_id country_name 
		save "`tmp_dir'/focimap.dta", replace


/*====================================================================
                        4: Prepare Oncho Americas Raw Data
====================================================================*/


*--------------------4.1: Import Oncho Americas GBD2016 Systematic Review

	import excel "FILEPATH/epi_lit_Onchocerciasis_AMERICAS_ONLY_6June2017.xlsm", sheet("extraction") firstrow clear
	
*--------------------4.2: Basic Formatting

	*Drop empty rows, destring variables, drop unneeded variables
		drop in 1/1
		destring row_num parent_id modelable_entity_id underlying_nid nid location_id smaller_site_unit poly_id sex_issue year_start year_end PEC is_outlier, replace
		destring year_accuracy year_issue age_start age_end age_issue age_demographer mean lower upper standard_error effective_sample_size, replace
		destring cases sample_size design_effect unit_value_as_published measure_issue measure_adjustment uncertainty_type_value recall_type_value, replace
		destring response_rate cv_subpopulation cv_DxType cv_NumSnips cv_meanmfintensity cv_SDor95Confidence cv_SDmfintensity group, replace
		destring age_sex_split dx_requires_crosswalk, replace
		format modelable_entity_name uncertainty_type sampling_type specificity SubPopDetails %15s
		format underlying_field_citation_value file_path input_type page_num poly_reference group_review data_sheet_filepath %5s
		format field_citation_value table_num ihme_loc_id sex measure unit_type urbanicity_type recall_type extractor poly_id %10s
		format source_type location_name site_memo site_notes sampling_description PEC_definition case_name case_diagnostic SnipLocation GeographyIssueType %25s
		format representative_name case_definition note_modeler note_SR ndIssue CK %40s
		drop cv_OtherIntervention1 cv_OtherIntervention2 InterventionType cv_Pregnant cv_lowermfconfidence cv_uppermfconfidence
		drop if modelable_entity_id==.
	
	*Convert months to year space
		replace month_start="0" if month_start=="NA"
		replace month_end="0" if month_end=="NA"
		destring month_start month_end, replace
		replace year_start=year_start+(month_start/12)
		replace year_end=year_end+(month_end/12)
		egen year=rowmean(year_start year_end)
	
	*Covert sex to numeric
		replace sex="1" if sex=="Male"
		replace sex="2" if sex=="Female"
		replace sex="3" if sex=="Both"
		destring sex, replace
	
	*Drop non-Americas locations - Ghana (207), Burkina Faso (201), Guinea (208), Nigeria (214)
		drop if inlist(location_id,207,201,208,214)
	
	*Convert age in non-demographer notation
		replace age_end=age_end-1 if age_demographer==0
	
	*Correct for unit_value_as_published
		replace mean=mean/unit_value_as_published if unit_value_as_published!=.
		replace standard_error=standard_error/unit_value_as_published if unit_value_as_published~=.

	*Combine all Ecuador sub-foci into a single Foci for analysis
		replace FociCode=6 if FociCode<7 & FociCode>6	

	*Get simplified country variable
		merge m:1 FociCode location_id using "`local_tmp_dir'/focimap.dta", nogen keep(matched master)

	*Keep only needed variables
		keep nid location_name location_id country_id country_name site_memo Foci FociCode sex year age_start age_end mean lower upper standard_error cases sample_size PEC PEC_definition mean_PEC*  group specificity group_review age_sex_split dx_requires_crosswalk  Dx cv_DxType cv_NumSnips SnipLocation 

*--------------------4.3: Calculate Missing Measures
		
	*Calculate missing measures
		replace mean=cases/sample_size if cases!=. & sample_size~=. & mean==.
			*note that some rows still missing mean because cases=0 and sample_size=0 > keep these for potential age aggregation

	*Keep only the rows that were group_reviewd IN
		keep if group_review=="1"
		
	*Collapse across sexes to make everything all-sex
		replace cases=mean*sample_size if cases==.
		replace sample_size=cases/mean if sample_size==.
		
	*Assign random sample size=100 to rows with missing sample size - wont matter after summing/re-calculating, assumption = same # of men and women sampled
		replace sample_size=100 if sample_size==. & cases==.
		replace cases=mean*sample_size if cases==. 
		
	*Collapse
		collapse (sum) cases sample_size, by(Foci FociCode location_name location_id country_name country_id year age_start age_end group)

	*Recalculate mean
		gen mean=cases/sample_size

	*SAVE
		save "`tmp_dir'/formatted.dta", replace


/*====================================================================
                        5: Prepare Elimination Information
====================================================================*/

*--------------------5.1: Add in elimination years for different foci

	*Create table and manually enter elimination years from sources
		clear
		set obs 13
		gen id=_n
		gen double FociCode=.
		*local FociCodes 1.1 1.2 1.3 2.1 2.2 2.3 2.4 3.1 3.2 3.3 4.1 5.1 6.1 6.2 6.3 6.4
		local FociCodes 1.1 1.2 1.3 2.1 2.2 2.3 2.4 3.1 3.2 3.3 4.1 5.1 6
		local i 1
		
		foreach code in `FociCodes'{
			replace FociCode=`code' if id==`i'
			local ++i
		}
			
		gen mean=0
		gen year_interrupt=.
		gen year_eliminate=.
		gen year_start=.

		**Foci 1.1: Mexico - North Chiapas
			*http://www.oepa.net/Mexico.htm
			replace year_eliminate=2010 if FociCode==1.1

		**Foci 1.2: Mexico - South Chiapas
			*http://www.oepa.net/Mexico.htm
			replace year_eliminate=2014 if FociCode==1.2

		**Foci 1.3: Mexico - Oaxaca
			*http://www.oepa.net/Mexico.htm
			replace year_eliminate=2011 if FociCode==1.3

		**Foci 2.1: Guatemala - Cuilco/Huehuetenango
			*http://www.oepa.net/guatemala.html
			replace year_eliminate=2011 if FociCode==2.1

		**Foci 2.2: Guatemala - Solola, Such & Chim
			*http://www.oepa.net/guatemala.html
			replace year_eliminate=2014 if FociCode==2.2
			
		**Foci 2.3: Guatemala - Guatemala & Escuintla
			replace year_eliminate=2010 if FociCode==2.3

		**Foci 2.4: Guatemala - Santa Rosa
			*http://www.oepa.net/guatemala.html
			replace year_eliminate=2010 if FociCode==2.4

		**Foci 3.1: Venezuela - North Central
			*http://www.oepa.net/venezuela.html
			replace year_eliminate=2014 if FociCode==3.1
			
		**Foci 3.2: Venezuela - North East
			*http://www.oepa.net/venezuela.html
			replace year_interrupt=2012 if FociCode==3.2
			
		**Foci 3.3: Venezuela - South

		**Foci 4.1: Brazil - Amazonas/Roraima

		**Foci 5.1: Colombia - Lopez de Micay
			*http://www.globalnetwork.org/colombia-eliminates-onchocerciasis
			replace year_eliminate=2011 if FociCode=5.1

		**Foci 6: Ecuador - Foci + Sub-Foci
			*http://www.who.int/neglected_diseases/ecuador_free_from_onchocerciasis/en/
			replace year_eliminate=2012 if FociCode==6



*--------------------5.2: Finalize Elimination Years

	*Fill in elimination year variables based on elimination framework/interruption year if missing
		replace year_start=year_eliminate
		replace year_start=year_interrupt+3 if year_start==.
		
	*Add rows to current day for all years that were zero after year of interruption
		gen years=2017-year_start+1
		expand years
		bysort Foci: gen yearid=_n
		gen year=year_start+yearid-1
		
	*Drop foci that haven't had interruption events
		drop if year==.
		keep FociCode mean year


*--------------------5.3: Format and Save

	*Add in relevant variables to be able to append this table to the extraction data table
		merge m:m FociCode using "`tmp_dir'/focimap.dta", nogen
		drop if mean==.
		
		gen double age_start=0
		gen double age_end=99
	
	*Save as rows to be added to the extraction data
		save "`in_dir'/elimination.dta", replace


/*====================================================================
                        6: Model Population at Risk
====================================================================*/


*--------------------6.1: Import and Format

	*Import PAR information
		import excel "FILEPATH/OEPA MDA.xlsx", sheet("FociPAR") clear
	
	*Format
		rename B PAR11
		rename C Source11
		rename D PAR12
		rename E Source12
		rename F PAR13
		rename G Source13
		rename H PAR21
		rename I Source21
		rename J PAR22
		rename K Source22
		rename L PAR23
		rename M Source23
		rename N PAR24
		rename O Source24
		rename P PAR31
		rename Q Source31
		rename R PAR32
		rename S Source32
		rename T PAR33
		rename U Source33
		rename V PAR41
		rename W Source41
		rename X PAR51
		rename Y Source51
		rename Z PAR60
		rename AA Source60
		rename A year
		keep year PAR* Source*
		drop in 1/3
		drop if year==.
		destring PAR*, replace
		reshape long PAR Source,i(year) j(FociCode) string
		destring FociCode,replace
		
	
		gen double FC2=FociCode /10
		drop FociCode
		rename FC2 FociCode
		
	*Drop RP2015 as source - inconsistent with CDC/OEPA
		replace PAR=. if Source=="RP2015"
		rename Source SourcePAR
	
	*Save
		save "`tmp_dir'/parraw.dta", replace
		levelsof FociCode,local(codes) clean

*--------------------6.2: Run Regression

	*Make year splines
		mkspline yearS=year, cubic displayknots
			*1953, 1968, 1983, 1999, 2014

	*Run simple regression by Foci to predict PAR for missing years
		
		tempfile appendparest
		local i 1
		foreach code in `codes'{
			use "`tmp_dir'/parraw.dta", clear
			keep if FociCode==`code'
			
			glm PAR yearS*,family(poisson)
			
			*lowess PAR year
			*nl gom3: PAR year
			
			predict estPAR, xb
			predict estPAR_se, stdp
						
			**sort year
			**twoway (scatter PAR year)(line estPAR year)
			
			if `i'>1 append using `appendparest' 
			save `appendparest', replace
		
			local ++i
		}
			
		use `appendparest', clear

		*twoway(scatter PAR year)(line estPAR year),by(FociCode)

*--------------------6.3: Format predicions and Generate Draws

	*Format results of PAR to use later
		keep FociCode year estPAR* MDA 
		
		rename year year_id
		joinby FociCode using "`tmp_dir'/focimap.dta"
		
	*Get 1000 draws of PAR
		forval i=0/999{
			gen PAR_`i'=exp(rnormal(estPAR,estPAR_se))
		}
			
	*Merge with population envelope to get age structure
		joinby location_id year_id using "`tmp_dir'/skeletonplus.dta"
			*adds in 2 sexes + 23 age groups, limits to just 6 gbd years (3864 observations = 14 Foci-loci_ids*2sex*23age*6year)
		*split PARest among age/sex
		forval i=0/999{
			replace PAR_`i'=PAR_`i'*agesexsplit
		}
		
	*Split PARest for 4.1 between Roraima and Amazonas 
		preserve
		joinby location_id year_id age_group_id sex_id using "`tmp_dir'/pop_foci41.dta"
			*limits dataset to just focus 4.1
		forval i=0/999{
			replace PAR_`i'=PAR_`i'*RoraimaProp if location_id==4771
			replace PAR_`i'=PAR_`i'*AmazonasProp if location_id==4752
		}
		drop Ror* Ama*
		tempfile BRA
		save `BRA', replace
		restore
		drop if FociCode==4.1
		append using `BRA'

	
	*Format for simplicity
		keep location_id location_name FociCode Foci year_id age_group_id sex_id PAR_*
				
		save "`in_dir'/PARestimated.dta", replace


/*====================================================================
                        7: Prepare MDA Information
====================================================================*/

	*Import foci information re: MDA
		import excel "FILEPATH/OEPA MDA.xlsx", sheet("General Focus Information") firstrow clear
	
	*Format
		keep FociCode MDAStartYear MDALastYear
		rename MDALastYear mda_end
		rename MDAStartYear mda_start
		drop if FociCode==. | (FociCode>6 & FociCode<7)
		
		replace mda_end="" if mda_end=="NA"
		destring mda_start mda_end, replace
		
	*Save
		save "`tmp_dir'/mda.dta", replace


/*====================================================================
                        8: Assemble Dataset
====================================================================*/

*--------------------8.1: GBD Skeleton to Predict To
	
	*Merge GBD skeleton and foci map
		use "`tmp_dir'/focimap.dta", clear
		joinby location_id using "`tmp_dir'/skeletonplus.dta"
		drop if sex==2
		rename year_id year
		drop sex_id population process_version_map_id poplocyear agesexsplit
		
	*Fill in missing years so that empty rows are available for all years consecutively
		egen panel=group(FociCode Foci location_id location_name country_id country_name age_group_id pop_env)
		tsset panel year
		tsfill
		
		*Fill in variable values post tsfill
		foreach var in Foci location_name country_name location_id age_group_id country_id FociCode age_start age_end pop_env{
			bysort panel : replace `var' = `var'[1]
		}
		
		drop panel
		
*--------------------8.2: Append GBD2016 Americas Systematic Review

		append using "`tmp_dir'/formatted.dta"
			
*--------------------8.3: Append elimination year
		
		append using "`in_dir'/elimination.dta"
		replace year=round(year)
			
*--------------------8.4: MDA DATA
		
		merge m:1 FociCode using "`tmp_dir'/mda.dta", nogen
		
	*Generate MDAyears variable
		gen MDAyears=year-mda_start+1
		replace MDAyears=0 if year<mda_start
		replace MDAyears=. if year>mda_end
		
		bysort FociCode: egen maxmda=max(MDAyears)
		replace MDAyears=maxmda if MDAyears==.
		drop maxmda
	
*--------------------8.5: ADD ZERO PREVALENCE POINT FOR AGE=0 FOR ALL LOC-YEARS WITH DATA
	
		egen datagroup=group(FociCode location_id year)
		preserve
			keep location_id FociCode year age_start age_end mean MDAyears
			replace age_start=0
			replace age_end=0
			replace mean=0
			duplicates drop
			tempfile age0
			save `age0', replace
		restore
		
		append using `age0'
		
*--------------------8.6: Format & Save
		
		egen age_mid=rowmean(age_start age_end)
		
	
	**SAVE**
		save "`in_dir'/dataset.dta", replace






log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.
2.
3.


Version Control:


