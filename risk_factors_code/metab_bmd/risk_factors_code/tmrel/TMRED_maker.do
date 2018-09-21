
import delimited FILEPATH.csv, clear 
//Standardizing for Hologic mashins
gen s_bmd=(0.019+1.087* dxxnkbmd)

//predicting percentile 99th 
qreg s_bmd age sex, quantile(.99)
predict per_99, xb
predict per_99_se, stdp

gen age_start=age
replace age_start=0 if age<30
replace age_start=30 if age>=30 & age<40
replace age_start=40 if age>=40 & age<50
replace age_start=50 if age>=50 & age<55
replace age_start=55 if age>=55 & age<60
replace age_start=60 if age>=60 & age<65
replace age_start=65 if age>=65 & age<70
replace age_start=70 if age>=70 & age<75
replace age_start=75 if age>=75 & age<80
replace age_start=80 if age>=80

//TMRED as the 99th percentiles
table age_start sex, c(mean per_99)

//SD of the measures above percentile 98 as the SD of TMREDs
qreg s_bmd age sex, quantile(.98)
predict per_98, xb


levelsof age_start2 if age_start>=30,local(ages)
foreach s in 1 2 {
	foreach ag of local ages {
		preserve
		keep if sex==`s' & s_bmd> per_98 & age_start2 == `ag' 
		di in red "`s' `ag' `c(N)'"
		bootstrap r(sd), reps(100) :sum s_bmd 
		restore
	}
}
