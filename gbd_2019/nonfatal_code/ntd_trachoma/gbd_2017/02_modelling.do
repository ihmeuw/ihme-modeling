use  FILEPATH, clear

bysort nid: gen index = _n if !missing(nid)
expand 3 if index==1, gen(newObs)
foreach var of varlist ageMid cases*  {
  replace `var' = 0 if newObs==1
  }
  
bysort nid newObs (ageMid): gen index2 = _n
replace ageMid = 10 if index2==2 & newObs==1

drop newObs index* 

replace ageMid = 84.5 if age_group_id==21
replace cv_ref=0 if missing(cv_ref)

recode sex_id (1 = -1) (3 = 0) (2 = 1), gen(sexOrdered)

foreach var of varlist sample* {
  replace `var' = 1 if missing(`var') 
  }
  
generate meanBlindness = casesBlindness / sampleBlindness
generate logitMeanBlindness = logit(meanBlindness + 0.0000001)
  
mkspline ageS = ageMid, cubic knots(0 15 40 60 100)  
 
glm meanBlindness ageS* sexOrdered year_id prAtRiskTrachoma i.super_region_id cv_ref, family(binomial) link(logit)

predict test
predict testXb, xb

scatter test ageMid if iso3=="EGY"
scatter test ageMid if prAtRiskTrachoma>0 & location_type=="admin0" & year_id==2015, by(location_name)

