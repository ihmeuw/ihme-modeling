* BOILERPLATE *
  clear all
  set more off
  set maxvar 5000

  
  if c(os) == "Unix" {
    set 
    }
  else if c(os) == "Windows" {
    local j ""
    }
	

  
* BRING IN DATA FOR MODELLING *	


gen highIncome = super_region_name=="High-income"
gen lnAge = ln(ageMid)
recode sex_id 1=-1 3=0 2=1, gen(sexOrdered)

gen include = is_outlier==0 & (mean>.5 | ageMid<20 | highIncome==1) & (mean>0.7 | ageMid<40 | highIncome==1) & !(super_region_name=="High-income" & mean>0.7 & !missing(mean) & ageMid<50) 

gen lnWlCapped = lnWL

* PULL ALL PREVALENCE DATA SLIGHTLY TOWARD 0.5 TO AVOID HAVING ZEROS AND ONES (TWO-SIDED OFFSET) THAT WILL BE DROPPED IN CLOGLOG TRANSFORMATION *

* RUN MODEL *
capture drop fixed random* prediction

meglm mean lnWlCapped sexOrdered if include==1, offset(lnAge) || super_region_id: || region_id: || location_id: , family(binomial) link(cloglog)
predict fixed, fixedonly
predict fixedSe, fixedonly stdp
predict random*, remeans reses(randomSe*) 
  
  foreach var of varlist randomSe* {
    quietly sum `subinstr("`var'", "Se", "", .)'
	  replace `var' = `r(sd)' if missing(`var')
    replace `=subinstr("`var'", "Se", "", .)' = 0 if missing(`=subinstr("`var'", "Se", "", .)')
	}
"
drop if fixed==.
drop if age_group_id==.

preserve
keep location_id year_id age_group_id ageMid sex_id cfAlpha cfBeta fixed* random* is_estimate
restore

keep location_id year_id age_group_id ageMid sex_id cfAlpha cfBeta fixed* random*

