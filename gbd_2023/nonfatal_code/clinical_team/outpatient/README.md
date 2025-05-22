# Outpatient Pipeline

## A Note on Outpatient Correction Factors
In Early 2022 the Marketscan pipeline, which is currently the only source of input data for the outpatient
correction factors was updated. One result of this update was the cessation of processing _same day_ duplicate
claims for _any_ estimate ID. Essentially these are claims records that when reviewed using only the columns
the clinical team requires for processing are identical. They contain identical demographic, beneficiary and ICD diagnosis data.
The result of this are much lower denominator values for the outpatient CF ratios. The expected final effect on Outpatient
corrected data would be an increase in overall rates because the ratios, which decrease visit rates, would become larger.
As an example pulled from thin-air- let's say we have an unadjusted mean visit rate of 0.05 and an old CF ratio of 0.01. This
would result in an adjusted rate of (0.05 * 0.1) = 0.05. If the updated CF ratio is increased to 0.2 this would result
in a new adjusted rate of (0.05 * 0.2) = 0.01

These same-day duplicate records still exist in the ICD-Mart and could be retrieved by modifying the Marktescan pipeline's read function in the [io module](URL/io.py). You would need to pull in all parquet files in the "is_duplicate" partition.
If this course of action is taken it's recommended that duplicate outpatient claims only be included in processing of
estimate 18. Inclusion in other estimates would result in pretty dramatic and unncessary increases in input data size.

## Steps to run Outpatient
There are three high level steps

1. Run `compile_outpatient_cf_data.py`
    - This gets you inputs for the creation of correction factors
2. Run `create_outpatient_correction_factors.R`
    - This gets you correction factors
3. Run `outpatient.py`
    - This runs the Outpatient process.


## Detailed Steps
1. Load up `compile_outpatient_cf_data.py` in an ipython session on the cluster.
    - Run the compile_marketscan_pipeline_results function with a run_id which has marketscan pipeline results
2. Open an Rstudio session with the script `create_outpatient_correction_factors.R`
    - Set the run_id at the bottom
    - Run the entire script.
3. Import `outpatient.py` into an ipython session on the cluster
4. run `Outpatient(run_id, gbd_round_id, decomp_step)` passing in the appropriate arguments.


## Outpatient Code
- outpatient.py
	- The main outpatient script.
- clean_icpc_map.py
	- There is a special map that maps ICPC codes to Bundles. ICPC is a coding system for primary care settings.
- create_oupatient_correction_factors.R
	- Makes the correction factors based on outputs of remake_outpatient_cf... Uses a very old correction factor method: LOESS smoothing of ratios
- icpc.py
	- formats and maps Norway ICPC data
- outpatient_funcs.py
	- a collection of functions for processing used by outpatient.py
- plot_outpatient.py
	- collection of functions to plot final outpatient results
- remake_outpatient_cf_numerator_denominator_submit.py
	- qsubs jobs for outpatient CFs
- remake_outpatient_cf_numerator_denominator_worker.py
	- processes the marketscan aggregations needed for creating outpatient CFs
