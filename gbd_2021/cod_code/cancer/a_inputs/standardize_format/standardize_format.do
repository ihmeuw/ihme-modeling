
// File: standardize_format.do
** Contributors: USERNAME
** Description:	Standardize format of manually formatted cancer data_sources

** *****************************************************************************
** CONFIGURATION
** *****************************************************************************
// Accept Arguments
args dataset_name
noisily di "......            Beginning Standardize Format             ......"
sleep 3000


// Set common macros
run "FILEPATH/set_common_roots.do" 
run "FILEPATH/set_registry_common.do" 1 "`dataset_name'"

// Set macros specific to this script
#delim ;
local data_folder = "$data_folder";
local standardize_causes_script =
	"FILEPATH/standardize_causes.do";
local uid_vars = "registry_index year_start year_end sex_id
    coding_system cause cause_name";
#delim cr


// Get dataset and type name based on the ids
preserve
run "FILEPATH/load_database_table" "registry"
keep location_id registry_name registry_index
tempfile registry_index
save `registry_index', replace
run "FILEPATH/load_database_table" "dataset"
levelsof dataset_id if dataset_name == "`dataset_name'", clean local(dataset_id)
if "`dataset_name'" == "" {
	noisily di in red "ERROR: could not match dataset_name `dataset_name' to dataset_id"
	break
}
restore

// Re-load data
noisily di "`data_folder'/$step_0_output"
use "`data_folder'/$step_0_output", clear

// get the data types based on the available data
count if !inlist(cases1, 0, .)
if r(N) local inc_dataset = 1
else local inc_dataset = 0
count if !inlist(deaths1, 0, .)
if r(N) local mor_dataset = 1
else local mor_dataset = 0
count if !inlist(pop1, 0, .)
if r(N) local pop_dataset = 1
else local pop_dataset = 0

// set `metric_variables' based on data type
if `mor_dataset' local metric_variables = "deaths*"
if `inc_dataset' local metric_variables = "cases* `metric_variables'"
if `pop_dataset' local metric_variables = "pop* `metric_variables'"

// Remove labels from all variables
capture _strip_labels *

// Add cancer database identifiers (dataset_id and registry_index)
gen dataset_id = `dataset_id'
merge m:1 location_id registry_name using `registry_index', keep(3) assert(2 3) nogen

// Keep only the variables of interest
noisily di "     Keeping Relevant Data..."
keep dataset_id `uid_vars' `metric_variables' frmat* im_frmat*
order dataset_id `uid_vars' `metric_variables'

// Drop data for "unknown" age category where it contains no data
if `mor_dataset' capture drop if deaths26 == . & deaths1 == . 
if `inc_dataset' capture drop if cases26 == . & cases1 == . 

// Standardize cause and cause_name
do `standardize_causes_script'

// // Standardize column widths
// long strings
format %30s cause_name

// short strings
format %10s cause coding_system

** *****************************************************************************
**  Sort before saving by type
** *****************************************************************************
sort `uid_vars'
tempfile all_data
save `all_data', replace


// // Save Incidence
if `inc_dataset' {
	use `all_data', clear
	rename (frmat_inc im_frmat_inc) (frmat_id im_frmat_id)
    drop cases1
    keep `uid_vars' dataset_id cases* frmat_id im_frmat_id
	save_prep_step, process(1) data_type("inc")
}
if `mor_dataset' {
	use `all_data', clear
	rename (frmat_mor im_frmat_mor) (frmat_id im_frmat_id)
	drop deaths1
    keep `uid_vars' dataset_id deaths* frmat_id im_frmat_id
	compress
	save_prep_step, process(1) data_type("mor")
}
if `pop_dataset'{
	use `all_data', clear
	rename (frmat_pop im_frmat_pop) (frmat_id im_frmat_id)
	keep frmat_id im_frmat_id dataset_id registry_index year* sex_id pop*
	// Drop registry-years where the total population is 0 or missing
	drop if inlist(pop1, ., 0)
	drop pop1
    duplicates drop
	compress
	save_prep_step, process(1) data_type("pop")
}

run "FILEPATH/update_processing_status.do" "`dataset_name'"

noisily di "......         Formatting of `dataset_name' Complete!         ......"
sleep 2000

** *****************************************************************************
** End standardize_format.do
** *****************************************************************************
