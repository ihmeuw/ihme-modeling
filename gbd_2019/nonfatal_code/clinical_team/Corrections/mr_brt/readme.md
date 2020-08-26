MR-BRT Correction factor process

Three scripts run the majority of the correction factor process, `prep_all_cfs.R` which preps the inputs for the CFs, `mr_brt_parent.R` which is the master script for submitting each MR-BRT bundle/ICG job, and `trim_worker.R` which is the child script that runs the MR-BRT model. Then there is `submit_master.R` which will be made to run all three in one go, and `mr_brt_plofts.R` which creates plots used for diagnostics.

All scripts are taking a run argument for future use to be submitted in a qsub, where when you are say running `run_5` of inpatient, you can call `submit_master.R` with the argument run_5 which will then pass that run argument down through the process.

`prep_all_cfs.R` reads in our hospital sources that result from `correction_factor_prep_master.py` and uses them for CF1 and CF2. The sources we used for GBD 2019 are NZL, HCUP SIDS, and PHL. The script also reads in our Marketscan and Taiwan data to be used for CF1, CF2, and CF3. The Marketscan inputs are the result of `submit_process_marketscan.py` in the claims process, and Taiwan data is sent directly to us from collaborators. There are commented out sections to add in HAQ and the inpatient envelope as potential covariates ,but they were not used in GBD 2019.

`mr_brt_parent.R` submits each of the individual MR-BRT jobs. `trim_worker` takes in all those arguments, runs a MR-BRT model given bundle-specific inputs, makes predictions on an age-sex square dataset, and saves the draws from the model.

`mr_brt_plots.R` does two things: Vets what models broke by comparing the existence of input data files to training data files to the presence of results files, to also help show at what stage the mdoel broke. Then it loops through bundles to plot 3 graphs on top of one another: 1) the input data with colors for trimming vs. not trimming, 2) the model fit from the betas and 3) the uncertainty interval around the fit.




