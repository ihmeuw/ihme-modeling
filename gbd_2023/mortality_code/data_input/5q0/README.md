  
# 5q0 model

**Hub page found here**: WEBSITE

## Model Inputs
- 5q0 data from mortality database   
- population  
- Covariates: LDI, Crude HIV, maternal education    


## Model outputs  
5q0 estimates (not by sex) at national & subnational level      
**Note**:          
- For China national, CHN_44533 is used (not CHN_6)         
- Old AP is generated since GBD2017       

## Model vetting  
**Impact of new data**: Adding new data may cause VR source changes from biased to unbiased (or vice versa), and lead to reference change. Thus we need to check reference change after each model run in `FILEPATH`.          
         
		 
## Model Stages
1. nonlinear mixed effect (mx space)
2. spacetime smoothing
3. GPR (logit qx space)
4. raking
              
## Bias adjustment    
Each model must have at least 1 reference sources.         
25% of sources are selected manually.   
General references priorization: unbiased VR > DHS > average of other sources

    
## Structure of process
There are 16 steps (including graphing) for 5q0 model. Currently it is setup to run via jobmon. 

          

## Storage of inputs and outputs
Data is stored at "FILEPATH". Inside each `run_id` folder, there are data, model, raking, draws, summary, comparisons, graphs folders.    
                 

## Order of scripts
### 00_run_all   
This is the 5q0 model jobmon run_all. It runs all the scripts in order.
       
	   
### 01_input.R      
- save input 5q0 data as "FILEPATH"   
- save locations "FILEPATH" and spacetime locations "FILEPATH"   
- save standard locations for first stage model "FILEPATH" 
   

### 02_assess_vr_bias.py   
**input**: "FILEPATH"  
**output**: "FILEPATH"   
- set "type" on method_id;   
- fix sources;    
- Categorize data sources;    
- Change 0 values of 5q0 to be 0.0001;   
- loop through location_ids: Run a regression of 5q0 on year with VR dummy variable. If the p-value for the VR dummy is significant(<0.05), then it's biased         	        
- exceptions	
- Assign no overlap and vr only countries to be biased or unbiased   
	
	
### 03_format_covariates_for_prediction_models.R
**inputs**: "FILEPATH";        
covariates: LDI, HIV, maternal_education;              
population(get_population);             
**output**:  "FILEPATH"           
- merge covariates data with raw 5q0 ("ldi", "maternal_edu","hiv");  
- Set graphing_source;  
- format and save  
							   
						   
### 03_generate_hyperparameters.py  
**input**: "FILEPATH"
**output**: "FILEPATH"    
- Calculating data density. Each location will have a score  
- Make hyperparameter categories  
- Merge on hyperparameters  


### 04a_fit_submodel.R (first stage regression: nonlinear mixed effect model)
**input**: "FILEPATH"  
**output**: "FILEPATH"
1. set reference. Reference code is in `FILE_PATH/helper_functions/choose_reference_categories.r`. Saved as "FILEPATH"
2. Run stage 1 mixed effects model for standard locations only on mx space  
3. Take predicted fixed effect coefficients from the mixed effect model and apply to all location-year-sex combinations  
4. Subtract FE predictions from all 5q0 data values to generate FE-residuals  
5. Then run a random effect only model on FE-residuals  
6. Add RE predictions to FE predictions to get overall model predictions.  
**Note**:   
- the code will break if non-standard locations have a source.type that standard locations don't have     
- Every location must have at least one reference source. Otherwise the code will break in 05_variance.  


### 04b_fit_submodel.R (bias adjustment)  
**input**: "FILEPATH"
**output**: "FILEPATH" 
- Bias adjustment  


### 04c_fit_submodel.R (second stage: spacetime)
**input**: "FILEPATH"
**output**: "FILEPATH"
1. calculate residuals from final first stage regression  
2. Get spacetime locations (have fake regions to deal with running subnationals)  
3. Merge on spacetime regions  
4. Fit spacetime model  


### 05_calculate_data_variance.do
**input**: "FILEPATH"
**output**: "FILEPATH"
- Prep file for data variance in summary birth histories  
- prep under-5 population from the standard mortality group population file  
- calculate data variance  


### 06_fit_gpr.py
**input**: "FILEPATH"
**output**: "FILEPATH" 
- fit GPR model      
	no data model: gpr.gpmodel_nodata             
	data model: gpr.gpmodel           



### 07_append_gpr.py
**input**: "FILEPATH" 
**output**: "FILEPATH" 
- Read in gpr file and append together;



### 08_rake.R
**input**: "FILEPATH" 
**output**: "FILEPATH"  
- Scale everything except South Africa


### 09_save_draws.py
**output**: "FILEPATH" 
- non-raked locations: copy from gpr folder
- rake locations: copy from raking folder
- save draws and summarys

### 10_prep_upload_file.py
**output**: "FILEPATH"      
- combined stage 1, stage 2 and gpr results


### 11_upload.R
- Upload 5q0 estimate and 5q0 bias adjustment

### 12_mortality_5q0_data_comparison.py
- compare reference with GBD2017

### 13_graph_5q0_compare_with_previous.R    
- line plots
- current comparators are previous GBD rounds, GK (optional) and UNICEF

### 15_diagnostic_graph.R
- scatter plots
- current comparator is previous GBD round

### 16_pdf_compilation.py
- Combine all diagnostics in a single pdf
