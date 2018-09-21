odbc load, exec("SQL QUERY") dsn("ADDRESS") clear
keep if inrange(age_group_id, 2, 21)

replace age_start = .01 if inrange(age_start, .01, .02)
replace age_end   = .01 if inrange(age_end, .01, .02)
replace age_start = .1  if inrange(age_start, .05, .1)
replace age_end   = .1  if inrange(age_end, .05, .1)

egen age_mid = rowmean(age_start age_end)

set obs `=_N+2'
replace age_mid = 15 in `=_N-1'
replace age_mid = 25 in `=_N'
sort age_mid




/* Data below come from McMahon et al (1985) Acute hepatitis B virus infection: 
   relation of age to the clinical expression of disease and subsequent  
   development of the carrier state */
   
generate prAcute = .
replace  prAcute = 0.01  if age_start == 0
replace  prAcute = 0.095 if age_start == 1
replace  prAcute = 0.098 if age_start == 5
replace  prAcute = 0.103 if age_mid == 15 
replace  prAcute = 0.136 if age_mid == 25
replace  prAcute = 0.333 if age_start == 40
replace  prAcute = 0.333 if age_start == 50

capture drop testnl
nl log4: prAcute age_mid if age_start>0 
predictnl testnl = predict(), se(prAcuteSe)
 	   
replace prAcute = testnl if age_start>0
keep age* prAcute*




/* Equations below come from Edmunds et al (1993) The influence of age on the 
   development of the hepatitis B carrier state */

gen prCarrier = exp(-0.645 * age_mid^0.455) if age_mid >0.5
replace prCarrier = 0.885 if age_mid <= 0.5
replace prCarrier = exp(-0.645 * 25^0.455) if age_mid > 25

gen prCarrierSe =  (prCarrier / 0.885) * (0.93 - 0.84) / (2 * invnormal(.975))  // we're deriving a standard error based on CIs given in Edmunds article.

keep age_group_id pr*
keep if !missing(age)



foreach x in prAcute prCarrier {
  generate `x'Alpha = `x' * (`x' - `x'^2 - `x'Se^2) / `x'Se^2
  generate `x'Beta  = `x'Alpha * (1 - `x') / `x'
  
  if "`x'" == "prCarrier" {
    local abTotal = `=prCarrierAlpha[1]' + `=prCarrierBeta[1]'
    replace prCarrierAlpha = prCarrier * `abTotal'
	replace prCarrierBeta  = `abTotal' - prCarrierAlpha
	}
  
  forvalues i = 0 / 999 {
    quietly generate  `x'_`i' = rbeta(`x'Alpha, `x'Beta)
	}
	
  egen `x'Mean  = rowmean(`x'_*)
  egen `x'Lower = rowpctile(`x'_*), p(2.5)
  egen `x'Upper = rowpctile(`x'_*), p(97.5)
  egen `x'Min   = rowmin(`x'_*)
  egen `x'Max   = rowmax(`x'_*)
  }

 

save



