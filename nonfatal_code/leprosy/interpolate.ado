cap program drop interpolate_dismod
program define interpolate_dismod
	version 12
	syntax , measure_id(string) out_dir(string) [in_dir(string) modelable_entity_id(string) ref_year(string)]

	if ("`c(os)'"=="Windows") global j "FILEPATH"
	else {
		global j "FILEPATH"
		set odbcmgr unixodbc
	}	
	
	if ("`modelable_entity_id'" == "" & "`in_dir'" == "") | ("`modelable_entity_id'" != "" & "`in_dir'" != "") {
		di as error "must specify either modelable_entity_id or in_dir"
		error 1
	}
	
	if "`ref_year'"=="" local ref_year 1995 
	cd `out_dir'
	cap mkdir interpolated
	
	get_demographics, gbd_team("epi")

	foreach location of global location_ids {
		foreach sex of global sex_ids {

			!qsub -P "proj_custom_models" -N "interpolate_`location'_`sex'" -l mem_free=8G -pe multi_slot 4   "FILEPATH/stata_shell.sh" "FILEPATH/interpolate_parallel.do" 'location_id(`location') sex_id(`sex') measure_id(`measure_id') modelable_entity_id(`modelable_entity_id') out_dir(`out_dir') in_dir(`in_dir') ref_year(`ref_year')'
		}
	}

end
