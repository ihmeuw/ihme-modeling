
// MDR meta-analysis

import excel using "FILEPATH", firstrow clear

keep study year_start RR_mean RR_lower RR_upper

tempfile rr_GBD2017
save `rr_GBD2017', replace

import excel using "FILEPATH", firstrow clear

keep study year_start RR_mean RR_lower RR_upper

append using `rr_GBD2017'

gen year_id=year_start

rename RR_mean rr

rename RR_lower lower

rename RR_upper upper

gen logrr=ln(rr)

gen selogrr=(ln(upper)-ln(lower))/3.92

metan logrr selogrr, eform random label(namevar=study) 

graph export "FILEPATH", as(pdf) replace

// generate 1000 draws of MDR RR

import excel using "FILEPATH", firstrow clear

// Draw 1,000 RRs (from mean, 95% CI)	
		
		** put mean, sd in log space:
		gen rr_sd = ((ln(rr_upper)) - (ln(rr_lower))) / (2*1.96)
		replace rr = ln(rr)
		
		** 1,000 draws:	
		di in red "get 1,000 RR draws"
		quietly {
			forvalues k = 0/999 {	
				** gen draw from log space RR distribution
				gen rr_`k' = rnormal(rr, rr_sd)
				
				** transfer from log space to normal
				replace rr_`k' = exp(rr_`k')
						
			}
		}
			
			drop rr rr_lower rr_upper rr_sd	
			
			gen acause="tb_drug"
			

save "FILEPATH", replace



// XDR meta-analysis

import excel using "FILEPATH", firstrow clear

keep study year_start RR_mean RR_lower RR_upper

tempfile xdr_rr_GBD2017
save `xdr_rr_GBD2017', replace


import excel using "FILEPATH", firstrow clear

keep study year_start RR_mean RR_lower RR_upper

append using `xdr_rr_GBD2017'

gen year_id=year_start

rename RR_mean rr

rename RR_lower lower

rename RR_upper upper

gen logrr=ln(rr)

gen selogrr=(ln(upper)-ln(lower))/3.92

metan logrr selogrr, eform random label(namevar=study) astext(70) textsize(150)

graph export "FILEPATH", as(pdf) replace



// generate 1000 draws of XDR RR

import excel using "FILEPATH", firstrow clear

// Draw 1,000 RRs (from mean, 95% CI)	
		
		** put mean, sd in log space:
		gen rr_sd = ((ln(rr_upper)) - (ln(rr_lower))) / (2*1.96)
		replace rr = ln(rr)
		
		** 1,000 draws:	
		di in red "get 1,000 RR draws"
		quietly {
			forvalues k = 0/999 {	
				** gen draw from log space RR distribution
				gen rr_`k' = rnormal(rr, rr_sd)
				
				** transfer from log space to normal
				replace rr_`k' = exp(rr_`k')
						
			}
		}
			
			drop rr rr_lower rr_upper rr_sd	
			
			gen acause="tb_xdr"
			

save "FILEPATH", replace


