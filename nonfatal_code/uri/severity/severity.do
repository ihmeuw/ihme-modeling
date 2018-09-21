** Description: calculating % mild uri, and % moderate/severe uri

insheet using "FILEPATH", comma names clear

keep if parameter_type=="Incidence"

keep if data_status==""

preserve

keep if cv_mild==1

gen cases_mild=mean*sample_size

collapse (sum) cases_mild sample_size, by (age_start) fast

gen mean=cases_mild/sample_size

gen se_mild = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) 			

rename mean mild

tempfile mild
save `mild', replace

restore

keep if cv_moderate_severe==1

gen cases_mod=mean*sample_size

collapse (sum) cases_mod sample_size, by (age_start) fast

gen mean=cases_mod/sample_size

gen se_mod = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2)

rename mean moderate

tempfile moderate
save `moderate', replace

use `mild', clear
merge 1:1 age_start using `moderate', nogen

gen total=mild+moderate

gen mild_pc=mild/total

gen se_1=(se_mild/mild)^2
gen se_2=(se_mod/moderate)^2

gen se_final= sqrt(se_1 + se_2)

outsheet using "FILEPATH", comma names replace
