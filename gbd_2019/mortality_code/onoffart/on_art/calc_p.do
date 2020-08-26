clear all 
set more off

*no hi income
insheet using "FILEPATH", clear
gen type = "mortality" 
rename mort par 
tempfile mort 
save `mort', replace 

insheet using "FILEPATH", clear
gen type = "progression" 
rename  prog par
append using `mort'  

gen merge_inc = "nohi"

tempfile pars_nohi
save `pars_nohi', replace  

*hi income
insheet using "FILEPATH", clear
gen type = "mortality" 
rename mort par 
tempfile mort 
save `mort', replace 

insheet using "FILEPATH", clear
gen type = "progression" 
rename  prog par
append using `mort'  

gen merge_inc = "hi" 
append using `pars_nohi' 

tempfile pars 
save `pars', replace

*create empty file for appending 
clear all
set obs 1 
gen blank = 0 
tempfile master 
save `master', replace

foreach d of numlist 1/1000{
	foreach t in hi nohi{
		foreach a in 15-25 25-35 35-45 45-100{
			***calc code*** 
			use `pars', clear 
			keep if age == "`a'" & merge_inc == "`t'" & draw == `d'
			reshape wide par, i(age cd4) j(type) string 
			gen stay_prob = 1-(parmo+parpr) 
			replace stay_prob = 1- parmo if cd4 == "LT50CD4"
			gen cd4_bin = 1 if cd4 == "GT500CD4"
			replace cd4_bin = 2 if cd4 == "350to500CD4"
			replace cd4_bin = 3 if cd4 == "250to349CD4"
			replace cd4_bin = 4 if cd4 == "200to249CD4"
			replace cd4_bin = 5 if cd4 == "100to199CD4"
			replace cd4_bin = 6 if cd4 == "50to99CD4"
			replace cd4_bin = 7 if cd4 == "LT50CD4" 
			sort cd4_bin 

			foreach i in 1 2 3 4 5 6 7 8{  
			gen col`i' = 0

			} 

			replace col8 = parmort 
			set obs 8 
			replace col8 = 1 if _n == 8

			foreach i in 1 2 3 4 5 6{  
				local j = `i'+1
				replace col`i' = stay_prob if _n == `i' 
				replace col`j' = parprog if _n == `i' 
				replace col`i' = 0 if missing(col`i')
			} 

			replace col7  = stay_prob if _n ==7 
			replace col7 = 0 if missing(col7)
			mkmat col1 col2 col3 col4 col5 col6 col7 col8, matrix(A) 
			*five to get 6 months 
			matrix B = A*A*A*A*A  
			svmat B 
			keep B8 cd4 age merge_inc draw
			drop if _n ==8 
			rename B8 LT6Mo_offartmort 
			replace LT6Mo_offartmort = 1-(1-LT6Mo)^2 
			**********
			local ageb = substr(age,1,2)
			tempfile `t'`ageb'`d'
			save ``t'`ageb'`d'', replace
		}
	}
}

use `master', clear 

foreach d of numlist 1/1000{
	foreach t in hi nohi{
		foreach a in 15 25 35 45{ 
			append using ``t'`a'`d''
		
		} 
	}
}

drop blank 
drop if _n ==1

reshape wide LT6Mo_offartmort, i(age cd4 merge_inc) j(draw)

rename age merge_age 
replace cd4 = "ART"+cd4
rename cd4 merge_cd4
saveold "FILEPATH", replace
