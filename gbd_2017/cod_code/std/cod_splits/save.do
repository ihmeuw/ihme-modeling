	// Set OS flexibility
	clear all
	set more off
	

	// parse incoming syntax elements
	cap program drop parse_syntax
	program define parse_syntax
		syntax, input_dir(string) cause_id(string) description(string) mark_best(string) sex_id(string)
		c_local input_dir = "`input_dir'"
		c_local cause_id = "`cause_id'"
		c_local mark_best = "`mark_best'"
		c_local description = "`description'"
		c_local sex_id = "`sex_id'"
	end
	parse_syntax, `0'

	
	run  "FILEPATH/save_results_cod.ado"
	save_results_cod, cause_id(`cause_id') description(`description') input_dir(`input_dir') sex_id(`sex_id') input_file_pattern("{location_id}_{year_id}_{sex_id}.csv") 


