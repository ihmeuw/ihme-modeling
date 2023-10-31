# Age-sex process for under-5 mortality  

## General process notes  
* See Hub page for clearer documentation on methods
___

## Run all  
**code:** `00_run_all.py`

**input:**   
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"

**output:** creates output directory and adds input_data_original.dta to the folder  
**description:** launches workflow for age-sex process in jobmon  

___

## Save inputs   
**code:** `00_save_inputs.R`   
**input:** central comp & mortality shared functions  
**output:**
  * Location data: "FILEPATH"; "FILEPATH"; "FILEPATH"; "FILEPATH"
  * Live births

**description:** saves location and covariate files to model version folder  

___

## Compile HIV free ratios  
**code:** `01_compile_hiv_free_ratios.py`  
**input:**
   * "FILEPATH" (for all group1A locs);  
   * "FILEPATH"

**output:** "FILEPATH"
**description:** calculates hiv free ratio from all cause mort rate and hiv mort rate  
**notes:** all cause mortality input and hiv-free ratio output here is sex specific but broadly captures age 0-5  

___

## Fit models  
**code:** `02_fit_models.R`  
**input:**
  * Input data: "FILEPATH"
  * Location data: "FILEPATH"   
  * Covariates: "FILEPATH"

**output:** model objects and plots in fit_models folder  
**description:**  
  * sex model: calculate 5q0 sex ratio in data, drop if >1.5 or <0.8, put in scaled logit space, fit mixed effects model for logit sex ratio with RE on region and ihme loc id and a spline on both-sex 5q0 
  * age model:    
	nested loops for male/female and enn/lnn/pnn/inf/ch: 
		-- covariates: hiv for pnn or inf; hiv, m_educ, s_comp for ch; none for enn and lnn    
		-- mixed effects models for `log_prob_'age'_'sex'` with spline on sex-specific log_q_u5, and random effects on gbd region, and ihme_loc_id, and covariates specified above

___

## Predict sex model stage 1  
**code:** `03_predict_sex_model_stage_1.R`   
**input:**
  * Live births
  * 5q0 summaries
  * Sex model fits: "FILEPATH"

**output:** "FILEPATH"   
**description:**  
  * calculate sex ratio at birth  
  * bring in sex model object and predict scaled logit sex ratio  
  * inv-logit, multiply by 0.7 and add 0.8 to transform into regular space  
  * generate predicted values for q5_female and q5_male using sex ratio at birth:   
	```
	q5_female = (q5_both*(1+birth_sexratio))/(1+q5_sexratio_pred*birth_sexratio)   
	q5_male = q5_female*q5_sexratio_pred
	```  

___

## Predict sex model stage 2  
**code:** `04_predict_sex_model_stage2.R`  
**input:**
  * location data: "FILEPATH" ; "FILEPATH" ; "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH" 
  * "FILEPATH"

**output:**
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"

**description:** reads in both input data and stage 1 predictions for all countries, formats, saves residuals as  "FILEPATH" 
	runs function from `space_time.r` called `resid_space_time`, with prepped spacetime data and parameters from "FILEPATH"  
	formats for GPR  

___

## Predict sex model GPR  
**code:** `05_predict_sex_model_gpr.py`  
**input:**
  * "FILEPATH"
  * "FILEPATH"

**output:**
  * GPR summaries: "FILEPATH"
  * GPR draws: "FILEPATH"

**description:** uses `gpr.py`  to run Gaussian Process Regression for sex model, returns draws and summaries


___

## Predict age model stages 1 & 2  
**code:** `06a_predict_age_model_stages1.R` 
**code:** `06b_predict_age_model_stages2.R`
**code:** `06c_predict_age_model_format_outputs.R`
**input:**
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"
  * location data: "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"

**output:**
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"
  * "FILEPATH"  
  * "FILEPATH" 
  * "FILEPATH"  
  * "FILEPATH"

**description:**
  * calculate data density
  * read in and compile gpr output from sex model 
  * read in live birth data and calculate birth sex ratio,  
  * read in both sex 5q0, calculate female and then male 5q0 from both-sex 5q0, sex-ratio from sex model, and birth sex ratio by a solution to this equation:  
	```
	5q0 = 1/(1+r_birth) * female_5q0 + (r_birth)(1+r_birth) * male_5q0
	``` 
  * 5q0 converted to 5m0 (with-HIV) using $5m0=\frac{-ln(1-5q0)}{5}$  
  * hiv-free mortality rate calculated as mortality rate with HIV minus HIV crude death rate  
  * convert hiv-free mx back to qx and output "FILEPATH" 
  * read in age model parameters, coefficients, and covariates and predict qx with and without random effects  
  * merge back on sex-specific hiv-free qx  
	```
	d[,enn := no_hiv_qx * enn]    
	d[,lnn := (no_hiv_qx * lnn) / ((1-enn))]    
	d[,pnn := (q_u5 * pnn) / ((1-enn)*(1-lnn))]    
	d[,ch := (q_u5 * ch)  / ((1-enn)*(1-lnn)*(1-pnn))]    
	d[,inf:= (q_u5 * inf)]    
	```  
  * merge empirical data back on and calculate residuals  
  * fake regions merged on  
  * calculate number of data sparse regions (cutoff is <=6 data points)  
  * output "FILEPATH"and run `resid_space_time` function separately for each age group  
  * calculate stage 2 estimates as stage 1 minus smoothed residuals and output as "FILEPATH"

**notes:** run parallel over sex (for males & females) and age. Script 06c compiles the age-specific files.

___

## Predict age model GPR  
**code:** `07_predict_age_model_gpr.py`    
**input:**
  * "FILEPATH"   
  * "FILEPATH"

**output:** "FILEPATH"  & "FILEPATH"   
**description:**  uses `gpr.py` to launch GPR for location argument, by age/sex  

___

## Scale age and sex  
**code:** `08_scale_age_sex.R`  
**input:** birth sex-ratio, 5q0, sex-model, age-models  
**output:**  draws and summaries for final age-sex estimates  
**description:**   
  * format GPR output for scaling  
  * scale age in conditional probability space for sex-specific estimates  
  * calculate both-sex by age and scale age in conditional prob space  
 
___


## Graph age and sex  
**code:** `09a_graph_age_sex_wrapper.R`, `09b_prep_graph_age_sex.R`, `09c_graph_age_sex.R`, `09d_append_age_sex.py`

**input:** age-sex results from current run, previous gbd round, and an optional second comparator  
**output:**  diagnostic graphs for age-sex - creates a plot of each under 5 age group for both sexes  
**description:**   
  * qsub graphing code i.e. separate it from the pipeline, so the pipeline can run in parallel with graphing. *NOTE*: this means you'll need to monitor your error logs for issues (Error logs clear every 2 days, so you will need to  resolve any issues within that time frame)  
  * compile estimates and intermediate estimates for graphing  
  * launch graphing code in parallel by location
  * compile graphs into one pdf
   
___

## Other scripts

* Other scripts including gpr.py and space_time.r are helper-function scripts to run ST_GPR.
* The graphing code is not run as part of the pipeline at present. It is qsubed in the run-all, so the rest of the pipeline can run without waiting for age-sex graphs for hours.
