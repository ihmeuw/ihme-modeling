
clear
set more off

* DEFINE LOCALS AND DIRECTORIES *

  if c(os) == "Unix" {
    local j "ADDRESS"
    set odbcmgr unixodbc
    }
  else if c(os) == "Windows" {
    local j "ADDRESS"
    }

  
  local inFile FILEPATH
  local  sensData  FILEPATH
 
  
  
/******************************************************************************\
                   ADJUST FOR POOR DIAGNOSTIC SENSITIVITY
\******************************************************************************/  
  
 	
* OPEN SENSITIVITY DATA *
  use "`sensData'", clear
  gen mean = positive_cases/total_cases
	
* CALCULATE STANDARD ERROR OF BINOMIAL * 
  gen se = sqrt((mean/(1-mean))/total_cases)
	
	
* RUN THE METAANLYSIS (NB THIS REQUIRES THAT YOU INSTALL METAN) *
  metan mean se, random nograph
  local pooled = `r(ES)'
  local upper = `r(ci_upp)'
  local lower = `r(ci_low)'
  local se = `r(seES)'

  local sensAlpha = `pooled' * (`pooled' - `pooled' ^ 2 - `se' ^2) / `se' ^2 
  local sensBeta  = `sensAlpha' * (1 - `pooled') / `pooled'
  
  macro dir

  
  * LOAD INTEST DATASHEET & ESTIMATE ALPHA AND BETA PARAMETERS OF INCIDENCE DATA POINTS *
  import excel using `inFile', sheet("extraction") firstrow clear
  drop if missing(measure)
  
  generate seTemp = standard_error
  replace  seTemp = (upper - lower) / (2 * invnormal(0.975))
  
  generate alpha = cases
  replace  alpha = 1.1e-4 if cases==0
  generate beta  = sample_size - alpha
  
  replace alpha = mean * (mean - mean^2 - seTemp^2) / seTemp^2 if missing(alpha)
  replace beta  = alpha * (1 - mean) / mean if missing(beta)
  
  assert !missing(alpha)
  assert !missing(beta)

  
* ADJUST FOR POOR SENSITIVITY WITH UNCERTAINTY *
	forvalues i = 1/1000 {
		quietly {
			generate gammaA = rgamma(alpha, 1)
			generate gammaB = rgamma(beta, 1)

			generate temp`i' = (gammaA / (gammaA + gammaB)) /  rbeta(`sensAlpha', `sensBeta') 
			
			drop gamma?
			}
		}


* Find the mean, confidence limits and standard error from the 1,000 draws
  egen tempMean = rowmean(temp*)
  egen tempSe = rowsd(temp*)


  replace mean  = tempMean  
  replace lower = . 
  replace upper = . 
  replace standard_error = tempSe
  replace cases = . 
  replace sample_size = . 
  replace effective_sample_size = . 
  replace uncertainty_type_value = .

  capture tostring(note_modeler), replace
  
  replace note_modeler = "Multiplied by adjustment factor to account for poor diagnostic sensitivity" + note_modeler 

  drop temp* seTemp alpha beta
  capture drop BH
  drop bundle*
	
* EXPORT FILE *
  tokenize `inFile', parse(.)
  export excel using FILEPATH, sheet("extraction") sheetreplace firstrow(variables)
	
	
