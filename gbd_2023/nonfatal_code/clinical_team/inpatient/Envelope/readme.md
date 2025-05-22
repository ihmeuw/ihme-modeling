## Envelope
Contains code which pertains to both extraction and modeling of the hospital inpatient envelope and the application of the envelope in the Inpatient pipeline.

* create_population_estimates - Contains the code to transform our inpatient primary admissions to represent admissions at the population level. Note: for sources that have universal coverage the envelope isn't used and we simply divide admissions by population

* apply_env_only.py - Uses the smaller CSV files to apply just a point estimate from the envelope models to admission fractions

* apply_bundle_cfs.py - Applies the Correction Factor draws by age/sex/bundle_id

* FILEPATH - Parent script for the last transformation of our process which involves mapping from ICG to bundle and applying the correction factors (at the draw level with a few exceptions)
	* create_bundle_draw_estimates.py - maps to the bundle level, squares the inp data, then aggregates to 5 year groups
