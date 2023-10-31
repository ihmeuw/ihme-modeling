## Empirical life tables  

### (0) Run-all script  
Code: `gen_lts_run_all.py`  

### (1) Prep  
Code: `01_elt_setup.R`  
Output: `all_lts_1.csv`  
Description:

* Read in empirical deaths from DDM output, perform exclusions, re-format
* Completeness adjustment
* Calculate mx = deaths/populationSave inputs, calculate mx

### (1) Generate life tables (by location)  
Code: `02_gen_elts.R`  
Input:   
Description:  

* Generate life table parameters (ax, qx, lx, dx)
* Extend life tables to age 105-109
* Perform smoothing (3, 5, 7 years)

### (2) Save plots of life tables  
Code: `mv_input_plots_child.R`  
Input: `all_lts.csv`  
Description: Save .jpg files as input for machine vision prediction  

### (3) Use machine vision to predict outliers  
Code: `predict_machine_vision.py`  
Description: Predicts outlier status of LTs based on previously run machine vision model. If a new model is run, the model file needs to be replaced in this script. It is not necessary to train a new model every time, but a once-per-cycle review of the training set and re-run of the model would be appropriate.  

### (4) Select LTS to use  
Code: `03_select_lts.R`  
Input: all `all_lts_{loc}.csv` files  
Description: Outlier data based on a series of criteria, and select LTs to be location-specific

### (5) Save to database   
Code: `upload_empir_lts.R`  



### Additional code with functions:  
* `empir_gen_funcs` directory  
* `machine_vision_elt.py`  
* `elt_functions.R` -- could be reviewed and migrated to the `empir_gen_funcs` folder


### More notes on machine vision:

Life tables change every time population changes, with notable variability in the oldest ages. In 2017, outliers were almost entirely found with a series of empirical rules. However, we decided that these rules were not perfect, and created many outliers where we actually wanted to be included the life tables. In 2019 we tested the use of machine vision to speed up the process of manually vetting life tables. Machine vision alone did not produce satisfactory accuracy/loss, however, sorting life tables into likely outliers and likely non-outliers made the review by-eye faster.  

General steps for machine vision:  
(1) Review initial set of >5000 life tables, and sort into outliers/non-outliers. 
(2) Generate folder structure of .jpgs from life tables that were reviewed (test/train, and outlier/non-outlier), and life tables that remain (predict, unknown)   
(3) Train model to initial set (or bypass this to use a previously fit model)   
(4) Predict outlier/non-outlier for un-reviewed life tables. Machine vision will give you a decimal probability of being an outlier. You can choose to review all life tables by eye, or just the ones in the middle where there is some uncertainty.   
(5) Plot life tables into pdfs for vetting by-eye. One method for this is plotting life tables 2x2, with all smooth-widths on one page, and colored by outlier status. PDF files must be around 1000 pages, or else they crash when viewing.    
(6) Scroll through PDF and generate list of life tables where you want to modify the machine vision decisions  
(7) Use code to implement these adjustments to life table category and save as a new version of ELT   
