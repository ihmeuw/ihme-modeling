******************************************************************************************************
** NEONATAL HEMOLYTIC MODELING
** PART 3: Preterm
** Part A: Prevalence of Preterm birth complications
** 6.18.14

** We get preterm birth prevalence by summing the birth prevalences we calculated for preterm in our 
** preterm custom models. 

*****************************************************************************************************

// discover root
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
	



// functions
adopath + /*FILEPATH*/


// directories 	
	local in_dir /*FILEPATH*/
	local out_dir = /*FILEPATH*/
	
	local plot_dir /*FILEPATH*/
	
// Create timestamp for logs
    local c_date = c(current_date)
    local c_time = c(current_time)
    local c_time_date = "`c_date'"+"_" +"`c_time'"
    display "`c_time_date'"
    local time_string = subinstr("`c_time_date'", ":", "_", .)
    local timestamp = subinstr("`time_string'", " ", "_", .)
    display "`timestamp'"

	
*********************************************************************************************

tempfile data
save `data', emptyok

// set up me_id list, then draw and append each one
local me_ids = "1557 1558 1559"
local x = 1



foreach me_id of local me_ids {


	if `me_id' == 1557 {
		di "Me_id is `me_id'"
		local copula_me 15798
	}
	if `me_id' == 1558 {
		di "Me_id is `me_id'"
		local copula_me 15799
	}
	if `me_id' == 1559 {
		di "Me_id is `me_id'"
		local copula_me 15800
	}

	
	di "importing data for gestational age `x'"
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(`copula_me') source(epi) measure_ids(5) location_ids() year_ids() age_group_ids(2) sex_ids() status(best) clear
	
	di "generation ga var"
	gen ga = `x'
	
	di "appending and saving"
	append using `data'
	save `data', replace
	local x = `x'+1
}

// sum to one bprev for every location-sex-year
collapse (sum) draw*, by(location_id year_id sex_id)


// save
export delimited /*FILEPATH*/, replace

