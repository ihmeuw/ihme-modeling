** Set preferences for STATA
	** Clear memory and set memory and variable limits
		clear all
		set maxvar 32000
	** Set to run all selected code without pausing
		set more off
	** Remove previous restores
		cap restore, not
	** Define FILEPATH (data) for cluster (UNIX) and Windows (Windows)
		if c(os) == "Unix" {
			global prefix "FILEPATH"
			set odbcmgr unixodbc
		}
		else if c(os) == "Windows" {
			global prefix "FILEPATH"
		}


import delimited "FILEPATH", clear
	**renaming variables with the names of variable labels
	foreach v of varlist v3 - v152 {
   		local x : variable label `v'
   		rename `v' _`x'
	}
	tempfile USDA_nutrient_codebook
	save `USDA_nutrient_codebook', replace

** Call in dataset
use "FILEPATH", clear

**just keep the country that is being passed in
keep if countries == "`country'"

**create a counter for appending the years of the country together (used in the appending section at the base of the script)
local count = 0

levelsof year, local(years)

foreach year of local years {
	preserve
	keep if year == `year'


	merge m:1 ndb_no using `USDA_nutrient_codebook'
	**6 instances where FAO FBS data does not match up with USDA_nutrient_codebook; dropping for now
	keep if _merge ==3
	drop _merge
	order countries product_codes products ele_codes year country_codes data

	**creating variables to estimate nutrient content per food item
	local nutrients "calcium omega3 sodium fiber pufa tfa satfat cholesterol energy zinc vit_a_rae vit_a_retinol vit_a_iu protein iron mufa folic_acid magnesium phosphorus potassium selenium total_fats sugars starch total_carbohydrates_diff folates_dfe"

**the seemingly random numbers down below (ie. _301) are the nutrient codes that identify each of the nutrients in the datasheet
**this information can be found in the documentation saved here: "FILEPATH"

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
				**DPA
				gen est_`nutrient'_g_2 = data * _631
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
																										**if "`nutrient'" == "folates_dfe" 
																										gen est_`nutrient' = data * _435
																										egen `nutrient'_ug_sum = sum(est_`nutrient')
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


**saving all of the country-year data to one file
	if (`count' == 0) {

	save "FILEPATH/nutrients_est_`country'_by_item_2016", replace

	local count = `count' + 1
	}
	else {

	tempfile country_year_data
	save `country_year_data', replace

	use "FILEPATH/nutrients_est_`country'_by_item_2016", clear

	append using `country_year_data'

	save "FILEPATH/nutrients_est_`country'_by_item_2016", replace

	local count = `count' + 1
	}
	restore
}
