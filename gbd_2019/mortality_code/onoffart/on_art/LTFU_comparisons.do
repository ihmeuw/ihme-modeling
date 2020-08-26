clear all 
set more off 

*old model (model 1)
insheet using "FILEPATH", clear 
regress logit_prop_traced_dead logit_prop_ltfu
matrix b1 = e(b)

*new data but same model (model 2)
insheet using "FILEPATH", clear
replace logit_prop_traced_dead = logit((traced_dead/traced)) if !missing(traced) & !missing(traced_dead) 
replace logit_prop_ltfu = logit((ltfu/enrolled)) if !missing(ltfu) & !missing(enrolled) 
regress logit_prop_traced_dead logit_prop_ltfu
matrix b2 = e(b)

*new data meqrlogit model but with no year component (model 3)
gen u = _n 
meqrlogit traced_dead logit_prop_ltfu || u :, binomial(traced) 
matrix b3 = e(b) 

*same but with year (model 4) 
meqrlogit traced_dead year logit_prop_ltfu || u :, binomial(traced)
matrix b4 = e(b) 

*same but with no random effect and no year
glm traced_dead logit_prop_ltfu, family(binomial traced) link(logit)
matrix b5 = e(b)

*now have to gen the betas
gen b0_1 = b1[1,2]
gen b1_1 = b1[1,1]
gen b0_2 = b2[1,2]
gen b1_2 = b2[1,1]
gen b0_3 = b3[1,2]
gen b1_3 = b3[1,1]
gen b0_4 = b4[1,3]
gen b1_4 = b4[1,2] 
gen b2_4 = b4[1,1]
gen b0_5 = b5[1,2] 
gen b1_5 = b5[1,1]
*insert here the ones from the paper
gen b0_6 = .1328
gen b1_6 = -3.4983
*now create space for preds 
rename year study_year
expand 7
bysort u: gen year = 2000 + (_n-1)*2.5 

*now will generate preds 

foreach i in 1 2 3 5 { 
	
	gen preds`i' = 1/(1+exp(-(b0_`i'+b1_`i'*logit_prop_ltfu)))
	

}

gen preds4 = 1/(1+exp(-(b0_4+b1_4*logit_prop_ltfu + b2_4*year))) 


replace prop_ltfu = 1/(1+exp(-logit_prop_ltfu))
replace prop_traced_dead = 1/(1+exp(-logit_prop_traced_dead)) 

gen preds6 = 1/(1+exp(-(b0_6+b1_6*prop_ltfu)))

levelsof year, local(years)

foreach y of local years{ 
	
	preserve 
	gen period_data = 0
	replace period_data = 1 if study_year >= `y'-2.5 & study_year <= `y'+2.5
	keep if year == `y' 
	sort prop_ltfu
	twoway line preds1 preds2 preds3 preds4 preds5 preds6 prop_ltfu ||  scatter prop_traced_dead prop_ltfu if period_data == 0 [w=traced], msymbol(circle_hollow) color(red*.2) /// 
	|| scatter prop_traced_dead prop_ltfu if period_data == 1 [w=traced], mlabel(study_year) msymbol(circle_hollow) color(red) title("`y'") /// 
	legend(order(1 2 3 4 5 6 8) label(1 "2016") label(2 "new data") label(3 "glmm no year") label(4 "glmmm year") label(5 "glm") label(6 "egger") label(8 "Period data")) ///
	ytitle("Proportion LTFU dead") xtitle("Proportion LTFU")
	local y2 = subinstr("`y'",".",",",.)
	graph export "FILEPATH", replace
	restore 
	
}


