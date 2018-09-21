CODEm
=====

----------------------

Where Codem Falls in the GBD Framework
======================================

-show the big gbd picture  

----------------------

Why CODEm?
==========

- As part of the GBD we want to estimate the number of deaths around the word due to a specific cause  
- Not only a specific cause but to specific times/demogrpahics ie:(location, age, year, sex)  
- We have data for portions of these time/demogrpahic  
- (show the dat points in codviz here)  
- But for the rest(and to ensure consistent results) we need CODEm  

----------------------

CODEm
=====

* Make cause of death estimates for 169 causes of death for every location, age year, sex
* To make those estimates CODEm
  * 1) Uses all available data
  * 2)  Correct for known biases in the data
  * 3)  Make estimates so that we have
    * Number of Deaths
    * Log(rate)
    * Cause Fraction
* Codem will then pass these estimates to COD Mod

----------------------

Outline
=======

- Overview  
  - Develop Individual Models  
    - Covariate Selection  
    
----------------------
    
Covariates
==========

- In order to estimate COD modelers (thats you!) choose covariates that could be indicatores for that COD  
- Because of multicollinearity, including all possible covariates in a model can lead to overfitting, 
implausible signs and unstable coefficients.  
- Specific choice of covariates can make a large different on prediction, especially when predicting out of sample  
- Extensive literature on choosing covariates  
- Building on these, we developed an algorithm that combines all plausible
 relationships between covariates and COD and allows for a diversity of plausible models  

----------------------

Covariates
==========
- (Show covariate selection screen)  

----------------------

How to pick Covariates
======================

1) Identify all covariates that may be related to a given cause of death based on biological, 
etiological, or socioeconomic links. 
2) Based on the literature, identify the expected direction of the relationship: positive, negative, either.  
3) Classify covariates into levels:  
    1) Strong proximal relationship, well known biological pathway  
    2) Strong evidence of relationship but no direct biological link  
    3) Weak evidence of relationship or distal in the causal chain  
    
----------------------
    
How Codem uses Covariates
=========================

- Whats important (direction and significance)  
- Test (almost) all possible combinations (2n) of covariates and retain combinations of covariates where the 
coefficients are statistically significant and in the expected direction.  
- Test all combinations of level 1 covariates  
- Retain all models where sign on covariates is in the expected direction and the coefficient is 
significant at the p<.05 level  
- For each level 1 model retained, test all possible combinations of level 2 covariates  
- Retain the ones  
    - That do not affect either the significance or the sign on level 1 covariates  
    - Where the sign and significance  is in the expected direction  
- Repeat for level 3 covariates  


What we are left with
=====================

- Produces a list of covariate combinations for which prior covariate relationship beliefs are maintained  
- Allows great flexibility in model choice  

----------------------

Outline
=======

- Overview  
  - Develop Individual Models  
    - Covariate Selection  
    - Model Specification  
    
----------------------
    
Four Families of Models
=======================

- show the table  

----------------------

Mixed Effects Models
====================

- Fixed effects on covariates and age dummies  
- Nested random effects on super-region, region, age, country and location if necessary  

----------------------

Space Time Models (Step One: The Regression)
============================================

- Fixed effects on covariates and age dummies  
- Nested random effects on super-region, region and age  
- (Show first plot)

----------------------

Step Two: Space-Time Smoothing
==============================

- Second stage: smooth residuals over age, time and space using residuals  
- Age: take advantage of the fact that mortality estimates typically change smoothly over age  
    - Ages that are close together will be more heavily weighted  
- Time: tricubic weights on time  
    - With data l=.5 w/o country data l=2  
- Space:  
    - For countries with data: z = .9  
    - For countries without data: z= .7  
    - Eta is 0 if equal to D and 1 otherwise  
- Each weight is multiplied with the residuals and then added back on to the estimate  
- (Show second plot)  

----------------------

Gaussian Process
================

- Done for every location-age  
- Uses Space-Time estimates as mean-prior  
- Matern Euclidean function used as CoVariance function  
    - Degree of Differentiability(static value: 2)  
    - Amplitude(based on data variance)  
    - Scale (user_input)  
- (show another plot)

----------------------

Develop Ensemble Models
=======================

- Ensemble models are the standard method used for prediction in many fields including meteorology, 
soil chemistry, consumer choice, etc.  
- Ensembles are weighted averages of individual models.  
- Choice of weights is an active area of research in many statistical departments.  
- Key lessons from the Netflix Challenge:  
    - the more diverse the model pool the better  
    - simple weighting schemes can sometimes beat more complex schemes  
    
Ensemble Weighting Scheme
=========================

- Use out-of-sample performance of each component model to rank their performance  
- A range of ensemble created using  

----------------------
    
Out of Sample Predictive Validity
=================================

- As models and modeling strategies become more diverse and complex, it is critical to have a simple 
framework for assessing which models produce the best forecasts  
- We use repeated testing of out-of-sample predictive validity  
- We divide the data into three parts: 70% for model building, 15% for test 1 and 15% for test 2  
- The model development never uses the test data  
- Predictions are made for these country-years and the predictions are compared to the data held out of the analysis  

----------------------

Knockouts
=========

- How data are split to create train-test1-test2 combinations affects the results  
- Randomly held-out country-years are the easiest test  
- More difficult to forecast or backcast or predict when no data are available for a country at all  
- We mimic the pattern of missingness in the data to create the test datasets  
- (show the knockouts plot)  

----------------------

Metrics for Predictive Validity
===============================

1) Root mean squared error (RMSE)  
2) The fraction of the time the trend in the prediction matches the trend in the data  
3) The percent of the data included in the uncertainty interval (coverage)  

----------------------

In-Sample Fit and Out-of-Sample Predictive Validity
===================================================
- For more complex and flexible modeling strategies, in-sample fit and out 
of sample predictive validity may be only weakly correlated or in some cases negatively correlated  
- Traditional metrics of in-sample fit, (R-squared, likelihood ratio tests, etc.) 
may be a poor guide to choosing the best model or the best ensemble  

----------------------

What It Looks Like in the End
=============================
- (Show Final Plot)  

----------------------

Outline
=======

- Overview of CODEm (Cause of Death Ensemble Model)  
    - Development of individual models  
        - Covariate selection  
        - Model specification  
    - Development of an ensemble model  
    - Assessing how well the models are doing  
    - Choose the best model  
    
----------------------
    
Compare out-of-sample predictive validity of individual and ensemble models
===========================================================================

- All models, the individual models and the various ensemble models are compared using 
the metrics of predictive validity on the test 2 data  
- “Final score”:  
- Rank each component model and ensemble models on median RMSE and median trend  
    - Sum ranks across two metrics  
    - Assign rank 1 to model with smallest value of ranks  
    
----------------------
    
New CODEm Info
==============

- Single 20 slot job on the cluster  
    - Run time range 20 min-24hours (average of 6)  
- Results can still be seen on CodViz  
- Status of a model can now also be seen on Cod Viz  
- Emails have returned!  

----------------------

Git Storage and Custom Models
=============================

- CODEm is completely stored on stash  
- Multiple branches for multiple needs  
- Possible to pull the code and make your own changes (aka custom models) to use directly with the CODEm interface!  

----------------------

Thank you!
==========
