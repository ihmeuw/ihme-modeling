// Generate proportions of deaths from a particular cause for each age group and sex pair (184 unique pairs)
// Ex: for males 5 - 9, the proportion for syphilis would be 1 because the other three STDs don't affect children

cap program drop cod_data_prop
program define cod_data_prop
	syntax , cause_ids(string) [group_by(string)] [clear]

	// Set OS flexibility
	sysdir set PLUS "`h'/ado/plus"

	// load in stuff
	run "FILEPATH/get_cod_data.ado"
	run "FILEPATH/get_location_metadata.ado"

	***************************************************************
	** Get COD data
	***************************************************************
	
	clear
	tempfile appended
	save `appended', replace emptyok

	// grab COD data for each child std cause: syphilis, chlamydia, gonococcal, other stds 
	// and appends them all to the same file
	foreach cause of local cause_ids { 
		get_cod_data, cause_id(`cause') clear 
		append using `appended'
		save `appended', replace
	}
	
	keep if data_type == "Vital Registration"
	keep cause_id location_id year age_group_id sex study_deaths sample_size
	rename (sex year) (sex_id year_id) 
	tempfile causes
	save `causes', replace 
	
	// merge on location metadata
	get_location_metadata, location_set_id(35) clear 
	merge 1:m location_id using `causes', keep(3) 

	***************************************************************
	** Generate proportions
	***************************************************************

	// chlamydial deaths set to 0 for males
	replace study_deaths = 0 if cause_id == 395 & sex_id == 1
	
	// summarize data set (using sum)
	// group by cause, sex and age
	collapse (sum) study_deaths sample_size , by(cause_id `group_by')
	gen prop = study_deaths / sample_size 
	
	// prop is proportion of sample who died, generated for each sex and age group for each cause
	// scaled (var created below) is proportion of deaths from a particular cause for an age group - sex
	// Ex: for males 5 - 9, the value for scale for syphlis would be 1 because the other three STDs don't affect children

	// Convert the proportion to a percentage
	// Two cases for if group_by is/isn't passed in (since its optional)
	if "`group_by'" != "" bysort `group_by': egen scaled = pc(prop), prop 
	if "`group_by'" == "" egen scaled = pc(prop), prop
	drop study_deaths sample_size prop
end
