These scripts take as inputs the outputs from /gbd2015_source_prep

This process takes in individual level hospital records and outputs tabulated
data by age/sex_id/location_id/year_start/year_end/icg and then counts of
claims and individual cases for ICGs from the primary diagnosis position only
and from any diagnosis position. So this currently results in 4 columns of
case counts.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
More technical detail and examples of how this is run:

correction_factor_prep_master.create_non_ms_cf_inputs(run_id, user)
	This is called by the inpatient class (3/25/2019 still in dev)
	It sends out the scripts below

{source}_correction_factor_input_{}.py
	This takes the inputs identified above, does some formatting to change
	column names etc and then calls
	estimate_indv which creates the individual level tabulations. Then it
	writes a csv to FILEPATH

estimate_indv.main(df, cause_type)
	This is a shared script that each formatting file uses. It does 3 main things
	1. clean_bad_ids() - This removes any patient id that has multiple ages or
	sexes assigned to it
	2. maps the data from ICD codes to ICGs. This step calls the clinical_mapping
	module and pulls the current map from our database
	3. loop_over_agg_types() - This is the same process that marketscan uses to go
	from records of admissions or events to estimate individual cases by removing
	re-admissions and multiple visits
