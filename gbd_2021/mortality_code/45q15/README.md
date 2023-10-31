
# 45q15 model

**Hub page found here**: WEBSITE
          
## Model Inputs
- DDM corrected 45q15       
- population         
- Covariates: LDI, Crude HIV, Education per capita, 5q0 estimates       


## Model outputs  
45q15 estimates (by sex) at national & subnational level      
**Note**:   
- For China national, CHN_44533 is used (not CHN_6)      
- Old AP is not generated         

## Notes
- for incomplete VR, we add variance from DDM      
- for non-VR, we use MAD for sampling variance within a source_type      
            
## Structure of process
There are 10 steps (including graphing) for 45q15 model. Currently it is setup to run via jobmon. 
          

## Storage of inputs and outputs
Data is stored at "FILEPATH". Inside each `run_id` folder, there are data, stage_1, stage_2, gpr, draws, graphs folders.    
                 

## Order of scripts
### jobmon_45q15_model_run_all.py
This is the 45q15 model jobmon run_all. It runs all the scripts in order.
       
	   
### 00_save_inputs.R      
- save locations, population, and covariates   
- save standard locations for first stage model "FILEPATH"   
   

### 01_format_data.r     
**output**: `input_data.csv`   
- load covariates and combined  
- get 5q0 estimates;      
- get DDM completeness;    
- pull 45q15 data;   
- assign data categories
- format and save
  
	
	
### 02_calculate_data_density_select_parameters.py
**inputs**: "FILEPATH"                 
**output**:  "FILEPATH"           
- Calculating data density  
- Make hyperparameter categories  
- Merge on hyperparameters  
							   
						   
### 03_fit_prediction_model.r (first stage regression: mixed effect model)
**input**: "FILEPATH"
**output**: "FILEPATH"  
1. Run stage 1 mixed effects model for standard locations only 
2. Take predicted fixed effect coefficients from the mixed effect model and apply to all location-year-sex combinations  
3. Subtract FE predictions from all 45q15 data values to generate FE-residuals  
4. Then run a random effect only model on FE-residuals  
5. Add RE predictions to FE predictions to get overall model predictions.  


### 04_fit_second_stage.R (second stage: spacetime)
**input**: "FILEPATH"
**output**: "FILEPATH"
1. calculate residuals from final first stage regression  
2. Get spacetime locations 
3. Fit spacetime model  
4. Format for GPR inputs  

### 05_fit_gpr.py
**input**: "FILEPATH"  
**output**: "FILEPATH"  
- fit GPR model      
	no data model: gpr.gpmodel_nodata             
	data model: gpr.gpmodel           



### 06_rake_gpr_results.R
**input**: "FILEPATH"  
**output**: "FILEPATH"  
- Scale results with GBR 1981 exception
- Aggregates "ZAF", "IRN", "BRA"


### 07a_compile_unraked_gpr_results.R
**output**: "FILEPATH"
- non-raked locations: copy from gpr folder

### 07b_compile_all.R
**output**: "FILEPATH"
- all locations: copy from gpr folder

### 08_graph_all_stages_plus_opposite_sex.r
- graphing
- current comparators are previous GBD rounds, UN WPP


### 10_upload_results.R
- Upload 45q15 estimate