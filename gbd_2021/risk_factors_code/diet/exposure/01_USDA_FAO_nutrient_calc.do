***************************************************************************************************************************
**  Description: This script calculates nutrient availability from the prepped SUA using USDA nutrient composition tables. Should be called parallized on location. 
**
**    The USDA nutrient composition table is used to get nutrient content from specific food items (identified by the ndb_no)
**    The composition table has columns per each nutrient labeled with seemingly random nutrient codes that can be found in sr28_doc.pdf
**    In this script the data from the FAO Sua is mutiplied by the nutrient composition of that food to get total nutrients
****************************************************************************************************************************
** Set preferences for STATA
	** Clear memory and set memory and variable limits
		clear all
		set maxvar 32000
	** Set to run all selected code without pausing
		set more off
	** Remove previous restores
		cap restore, not
		if c(os) == "Unix" {
			global prefix "FILEPATH"
			set odbcmgr unixodbc
		}
		else if c(os) == "Windows" {
			global prefix "FILEPATH"
		}

**set the local to the country it is parallelized on

local country `1'


**set GBD round and version for ease of filepath updating
local version "gbd2021"
local gbd_round "gbd2021"

**get GBD round, version, and country for parallelization

di in green "`country'"

**SR 28: Pull the USDA food composition tables

import delimited "FILEPATH", clear

**renaming variables with the names of variable labels
	foreach v of varlist v3 - v152 {
   		local x : variable label `v'
   		rename `v' _`x'
	}

	tempfile USDA_nutrient_codebook
	save `USDA_nutrient_codebook', replace

use "FILEPATH", clear

**just keep the country that is being passed in

macro dir

count if location_id == "`country'"

keep if location_id == "`country'"


*recoding turkey meat 
replace ndb_no = 5165 if ndb_no == 5163 
*recoding processed cheese 
replace ndb_no = 1044 if ndb_no == 1147 
*recoding margerine + shortening
replace ndb_no = 4655 if ndb_no == 4071 
*reoding liquid margerine
replace ndb_no = 4073 if ndb_no == 4107 
*recoding pure frutose
replace ndb_no = 43216 if ndb_no == 9187 
*mixes + dough
replace ndb_no = 18017 if ndb_no == 18099 
*rice fermented beverages
replace ndb_no = 43479 if ndb_no == 14342 
*macaroni
replace ndb_no = 20105 if ndb_no == 20099 
*prunes, canned
replace ndb_no = 09288 if ndb_no == 9188

* Recoding fish and seafoods   
replace ndb_no = 15024 if products == "Freshwater Fish"
replace ndb_no = 15015 if products == "Demersal Fish"
replace ndb_no = 15121 if products == "Pelagic Fish"
replace ndb_no = 15027 if products == "Marine Fish, Other"
replace ndb_no = 15166 if products == "Cephalopods"
replace ndb_no = 15149 if products == "Crustaceans"
replace ndb_no = 15164 if products == " Molluscs, Other"
replace ndb_no = 15229 if products == "Aquatic Animals, Others"


**create a counter for appending the years of the country together (used in the appending section at the base of the script)
local count = 0

levelsof year, local(years)

foreach year of local years {
	preserve
	keep if year == `year'

	merge m:1 ndb_no using `USDA_nutrient_codebook'
	keep if _merge ==3
	drop _merge
	order countries product_codes products ele_codes year country_codes data

	**creating variables to estimate nutrient content per food item
	local nutrients "calcium omega3 sodium fiber pufa tfa satfat cholesterol energy zinc vit_a_rae vit_a_retinol vit_a_iu protein iron mufa folic_acid magnesium phosphorus potassium selenium total_fats sugars starch total_carbohydrates_diff folates_dfe vit_k alcohol"

	foreach nutrient of local nutrients {
		if "`nutrient'" == "calcium" {
			gen est_`nutrient'_mg = data * _301
			egen `nutrient'_mg_sum = sum(est_`nutrient'_mg)
		}
		else{
			if "`nutrient'" == "omega3" {
				**EPA
				gen est_`nutrient'_g_1 = data * _629
				egen `nutrient'_g_sum_1 = sum(est_`nutrient'_g_1)
				**DHA
				gen est_`nutrient'_g_2 = data * _621
				egen `nutrient'_g_sum_2 = sum(est_`nutrient'_g_2)
				gen `nutrient'_g_sum = `nutrient'_g_sum_1 + `nutrient'_g_sum_2
			}
			else { 
				if "`nutrient'" == "sodium" {
					gen est_`nutrient'_mg = data * _307
					egen `nutrient'_mg_sum = sum(est_`nutrient'_mg)
				}
				else {
					if "`nutrient'" == "fiber" {
						gen est_`nutrient'_g = data * _291
						egen `nutrient'_g_sum = sum(est_`nutrient'_g)
					}
					else{
						if "`nutrient'" == "pufa" {
							**18:2 n-6 c,c
							gen est_`nutrient'_g_1 = data * _675
							egen `nutrient'_g_sum_1 = sum(est_`nutrient'_g_1)
							**18:3 n-6 c,c,c
							gen est_`nutrient'_g_2 = data * _685
							egen `nutrient'_g_sum_2 = sum(est_`nutrient'_g_2)
							**20:2 n-6 c,c
							gen est_`nutrient'_g_3 = data * _672
							egen `nutrient'_g_sum_3 = sum(est_`nutrient'_g_3)
							**20:3 n-6
							gen est_`nutrient'_g_4 = data * _853
							egen `nutrient'_g_sum_4 = sum(est_`nutrient'_g_4)
							**20:4 n-6
							gen est_`nutrient'_g_5 = data * _855
							egen `nutrient'_g_sum_5 = sum(est_`nutrient'_g_5)
							gen `nutrient'_g_sum = `nutrient'_g_sum_1 + `nutrient'_g_sum_2 + `nutrient'_g_sum_3 + `nutrient'_g_sum_4 + `nutrient'_g_sum_5
						}
						else{
							if "`nutrient'" == "tfa" {
								gen est_`nutrient'_g = data * _605
								egen `nutrient'_g_sum = sum(est_`nutrient'_g)
							}
							else {
								if "`nutrient'" == "satfat" {
									gen est_`nutrient'_g = data * _606
									egen `nutrient'_g_sum = sum(est_`nutrient'_g)
								}
								else {
									if "`nutrient'" == "energy" {
										gen est_`nutrient'_kcal = data * _208
										egen `nutrient'_kcal_sum = sum(est_`nutrient'_kcal)
									}
									else {
										if "`nutrient'" == "cholesterol" {
											gen est_`nutrient'_mg = data * _601
											egen `nutrient'_mg_sum = sum(est_`nutrient'_mg)
										}
										else {
											if "`nutrient'" == "zinc"{
												gen est_`nutrient'_mg = data * _309
												egen `nutrient'_mg_sum = sum(est_`nutrient'_mg)
											}
											else{
												if "`nutrient'" == "vit_a_rae" {
													gen est_`nutrient'_ug = data * _320
													egen `nutrient'_ug_sum = sum(est_`nutrient'_ug)
												}
												else{
													if "`nutrient'" == "vit_a_retinol" {
														gen est_`nutrient'_ug = data * _319
														egen `nutrient'_ug_sum = sum(est_`nutrient'_ug)	
													}
													else {
														if "`nutrient'" == "vit_a_iu" {
															gen est_`nutrient' = data * _318
															egen `nutrient'_sum = sum(est_`nutrient')
														}
														else{
															if "`nutrient'" == "protein" {
																gen est_`nutrient' = data * _203
																egen `nutrient'_g_sum = sum(est_`nutrient')	
															}
															else {
																if "`nutrient'" == "iron" {
																gen est_`nutrient' = data * _303
																egen `nutrient'_mg_sum = sum(est_`nutrient')
																}
																else {
																	if "`nutrient'" == "mufa" {
																	gen est_`nutrient' = data * _645
																	egen `nutrient'_g_sum = sum(est_`nutrient')
																	}
																	else{
																		if "`nutrient'" == "folic_acid" {
																		gen est_`nutrient' = data * _431
																		egen `nutrient'_ug_sum = sum(est_`nutrient')
																		}
																		else{
																			if "`nutrient'" == "magnesium" {
																			gen est_`nutrient' = data * _304
																			egen `nutrient'_mg_sum = sum(est_`nutrient')
																			}
																			else{
																				if "`nutrient'" == "phosphorus" {
																				gen est_`nutrient' = data * _305
																				egen `nutrient'_mg_sum = sum(est_`nutrient')
																				}
																				else{
																					if "`nutrient'" == "potassium" {
																					gen est_`nutrient' = data * _306
																					egen `nutrient'_mg_sum = sum(est_`nutrient')
																					}
																					else{
																						if "`nutrient'" == "selenium" {
																						gen est_`nutrient' = data * _317
																						egen `nutrient'_ug_sum = sum(est_`nutrient')
																						}
																						else {
																							if "`nutrient'" == "total_fats" {
																							gen est_`nutrient' = data * _204
																							egen `nutrient'_g_sum = sum(est_`nutrient')
																							}
																							else {
																								if "`nutrient'" == "starch" {
																								gen est_`nutrient' = data * _209
																								egen `nutrient'_g_sum = sum(est_`nutrient')
																								}
																								else {
																									if "`nutrient'" == "sugars" {
																									gen est_`nutrient' = data * _269
																									egen `nutrient'_g_sum = sum(est_`nutrient')
																									}
																									else{
																										if "`nutrient'" == "total_carbohydrates_diff" {
																										gen est_`nutrient' = data * _205
																										egen `nutrient'_g_sum = sum(est_`nutrient')
																										}
																										else{
																											if "`nutrient'" == "folates_dfe" {
																											gen est_`nutrient' = data * _435
																											egen `nutrient'_ug_sum = sum(est_`nutrient')
																											}
																											else{
																												if "`nutrient'" == "vit_k" {
																												gen est_`nutrient' = data * _430
																												egen `nutrient'_ug_sum = sum(est_`nutrient')
																												}
																												else{
																													if "`nutrient'" == "alcohol" {
																													gen est_`nutrient' = data * _221
																													egen `nutrient'_g_sum = sum(est_`nutrient')
																													}
																												}
																											}	
																										}	
																									}
																								}
																							}
																						}
																					}
																				}
																			}
																		}
																	}
																}
															}
														}	
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}	
		}
	}	

	**just create total carbohydrates (minus the the grams of fiber) at the country-year level. Creating this for comparison purposes to the carbohydrates estimate produced by item from USDA
	gen total_carbohydrates_g_sum = total_carbohydrates_diff_g_sum - fiber_g_sum

**saving all of the country-year data to one file
	if (`count' == 0) {
	** test run: make sure to change the file path for the final run
		
	save "FILEPATH/nutrients_est_`country'_by_item_'version'", replace

	local count = `count' + 1
	}
	else {

	tempfile country_year_data
	save `country_year_data', replace

	use "FILEPATH/nutrients_est_`country'_by_item_'version'", clear

	append using `country_year_data'

	save "FILEPATH/nutrients_est_`country'_by_item_'version'", replace

	local count = `count' + 1
	}
	restore
}


keep year energy_kcal_sum alcohol_g_sum sugars_g_sum starch_g_sum total_fats_g_sum protein_g_sum

*duplicates drop 

gen alcohol_kcal_sum = alcohol_g_sum * 6.93
gen sugars_kcal_sum = sugars_g_sum * 4
gen starch_kcal_sum = starch_g_sum * 4


