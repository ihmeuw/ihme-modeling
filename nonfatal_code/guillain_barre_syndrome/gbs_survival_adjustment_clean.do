*Author: 
*Updated  2/27/17
*Date: 3/11/2016
*Purpose: For Guillain Barre, we only model nonfatal outcomes of surviving cases, since mortality only occurs in acute phase. 
	//To adjust, we subtract out the CF% 


*1) Meta-analysis of CFR data (currently calculated as survival rate)
*2) Adjust hospital and marketscan data
	//Confirm incidence 
	//Keep only inpatient hospital and Marketscan data 
	//Ajdust mean and standard error 

clear all
	set more off
	set mem 2g
	set maxvar 32000
	set type double, perm
	if c(os) == "Unix" {
		global prefix "/home/j" 
		set odbcmgr unixodbc
		local cluster 1 
	}
	else if c(os) == "Windows" {
		global prefix "J:"
		local cluster 0                                                              
	}
	
*****************************************************************************************************
*****************************************************************************************************
//SPECIFY INPUTS 
local bundle_id 908
local request_num 63707
local download_sheet "FILEPATH.xlsx"
local cfr_data "FILEPATH.csv"
local graphs_dir FILEPATH
local out_dir FILEPATH
local data_type "hospital"

if "`data_type'" == "hospital" {
	local extractor USERNAME
	local source_type "Facility - inpatient"
	local out_data "hospital_final_sr_version6_nooutliers"
	local note "mean 1 hosptial case adjusted for case fatality via Meta-analysis"
}
else if "`data_type'" == "marketscan" {
	local extractor USERNAME
	local source_type "Facility - other/unknown"
	local out_data "hospital_final_sr"
	local note "case adjusted for case fatality via Meta-analysis"
}
*****************************************************************************************************
*****************************************************************************************************

//ADJUST FOR CASE FATALITY 
		//Meta-analysis of CFR data 

			import delimited "`cfr_data'", stringcols(1 4 6 8 10 18) clear 


	//META-ANALYSIS
				ssc install metaprop
				metaprop deaths cases, random lcols(location_name) title("GBS CFR") saving("`graphs_dir'gbs_CFR_metaanalysis", replace)
				local cfr_pooled = `r(ES)'
				local cfr_pooled_se = `r(seES)'

				gen survived = cases - deaths 
				metaprop survived cases, random lcols(location_name) title("GBS SR") saving("`graphs_dir'gbs_SR_metaanalysis", replace)
				local sr_pooled = `r(ES)'
				local sr_pooled_se = `r(seES)'

//LOAD AND FORMAT DATA
	import excel using `download_sheet', firstrow clear  	

	//using unadjusted figures so arrange and rename variables
	//keep only marketscan
	keep if extractor == "`extractor'"

	//Check to see if prevalence or incidence. Should be coming in as incidence 
	count if measure != "incidence"
	if `r(N)' > 0 di in red "CHANGE TO INCIDENCE" BREAK 

	//Keep only inpatient data
	keep if source_type == "`source_type'"


//ADJUST MEAN AND STANDARD ERROR OF INCIDENCE
			//variance of mean*cfr 
			*var(X)var(Y)+var(X)E(Y)^2+var(Y)E(X)^2     as per http://stats.stackexchange.com/questions/15978/variance-of-product-of-dependent-variables
			gen var_mean = standard_error^2
			gen var_SR = `sr_pooled_se'^2
		gen variance = var_mean * var_SR + var_SR * mean^2 + var_mean * `sr_pooled'^2
		replace standard_error = sqrt(variance)

		replace mean = mean * `sr_pooled'	
		drop var*
	//replace uncertainty type to "Standard error" so dismod uses standard error as uncertainty
	replace uncertainty_type = "Standard error"


//FORMAT DATA
replace note_modeler = note_modeler + " | `note'"

//SAVE DATA
	export excel "FILEPATH.xlsx", firstrow(var) sheet("extraction") replace
