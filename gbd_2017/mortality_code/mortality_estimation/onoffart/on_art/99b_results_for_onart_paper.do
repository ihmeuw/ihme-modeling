

// RATES

	use "FILEPATH", clear

	egen mean_rate=rowmean(rate*)
	egen lower_rate=rowpctile(rate*), p(2.5)
	egen upper_rate=rowpctile(rate*), p(97.5)
	order super dur cd4_lower cd4_upper mean_rate lower_rate upper_rate

	// For example, for sub-Saharan African patients initiating with a CD4 count less than 50, the conditional mortality rate was XX (95% Confidence Interval: XX-XX) ///
	// per person-year for 0-6 months, compared to XX (CI: XX-XX) for 7-12 months and XX (CI: XX-XX) for 13-24 months. 
	br super dur mean_rate lower_rate upper_rate if cd4_lower==0


	collapse rate*, by(super dur)
	egen mean_rate=rowmean(rate*)
	egen lower_rate=rowpctile(rate*), p(2.5)
	egen upper_rate=rowpctile(rate*), p(97.5)
	order super dur mean_rate lower_rate upper_rate
	keep super dur mean_rate lower_rate upper_rate 

	reshape wide mean_rate lower_rate upper_rate, i(super) j(dur) string

	gen times_higher=mean_rate0_6/mean_rate12_24

	// Averaging across initial CD4 categories in Africa, the conditional mortality rate was XX times higher in the first 6 months than from 12-24 months.
	br super times_higher

// CONDITIONAL PROB
	use "FILEPATH", clear

	/// In sub-Saharan Africa, the probability of death at 6 months was 21.4% (CI: 12.1-34.8%) for those initiating with an initial CD4 less than 50,
	/// compared to 4.3% (CI: ) in those with a CD4 of at least 350. The gradient at 6 months was less pronounced in high income countries, with 3.5% (CI: ) 
	/// mortality for those in the 0-50 CD4 group, and 2.5% for those in the 350+ CD4 group. 
	br super dur cd4_lower cd4_upper mean_cond_prob low_cond high_cond if dur=="0_6" & (cd4_lower==0 | cd4_lower==500)


// CUMULATIVE PROB

	split dur, parse(_)
	destring dur2, replace
	gen timepoint=dur2
	
	gen cumul_prob=mean_cond_prob if timepoint==6
	
	order super cd4_point timepoint cumul_prob mean_cond_prob
	
	sort super cd4_point timepoint
	bysort super cd4_point: replace cumul_prob=cumul_prob[_n-1]+((1-cumul_prob[_n-1])*mean_cond_prob) if timepoint==12
	bysort super cd4_point: replace cumul_prob=cumul_prob[_n-1]+((1-cumul_prob[_n-1])*mean_cond_prob) if timepoint==24


preserve
keep super dur cd4_lower cd4_upper cumul_prob 
reshape wide cumul_prob, i(dur cd4_lower cd4_upper) j(super) string
gen times_higher_ssa=cumul_probssa/cumul_probhigh
gen times_higher_other=cumul_probother/cumul_probhigh
collapse times_higher*, by(dur)

/// Averaged across CD4 groups, 24 month mortality was XX% and XX% higher in sub-Saharan Africa and other LMIC than high income countries, respectively. 
br

// The majority of excess mortality compared to high income countries occurred in the first 6 months, where mortality as XX and XX times higher in sub-Saharan African and other LMIC respectively. 
restore

keep super cd4_lower cd4_upper timepoint cumul_prob 
keep if timepoint==24
keep if cd4_lower==0 | cd4_lower==500
reshape wide cumul_prob, i(cd4_lower cd4_upper) j(super) string

gen times_higher=cumul_probssa/cumul_probhigh

// TOP  SSA

/// The greatest gains in terms of conditional mortality were obtained in the first 6 months for the less than 50 CD4 group, where mortality feel from 21.4% (CI: ) to 10.9% (CI: ). 

	use "FILEPATH", clear
	br mean_cond_prob low_ high_ super dur if (super=="ssa" | super=="ssabest") & cd4_lower==0
	
	
	
	