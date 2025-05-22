Overview of how the ***three age-sex-splitting*** scripts interact

<br>

1) compute_weights.py
	* `prep_weights()` - creates weights at bundle or icg level, requires gbd-round and decomp step to get population
			Requires a column called "product" which is a population level rate
			Returns a pd.DataFrame

    * `compute_weights()` - (all pipelines) applies age-sex restrictions
							(all pipelines) Runs a test to make sure weights aren't missing for entire age/sex groups
							(inp_pipeline) Writes two CSV files, one for tableau and one for use in inp pipeline

<br>

2) run_age_sex_splitting.py
	* prep code specific to clinical data in a single long-ish function
		- option to write viz data
		- (inp_pipelines) runs hosp_prep.drop_data
		- (all pipelines) apply a-s restricts
		- (all pipelines) manually sets the id cols, perfect ages, and perfect sexes
		- (all pipelines) loops over source col in df splitting by source (why?)
		expects year_start/end


<br>

3) age_sex_split.py
	* Code inherited. Modified to read in weights from a specific path if they're at the bundle id level


<br>



