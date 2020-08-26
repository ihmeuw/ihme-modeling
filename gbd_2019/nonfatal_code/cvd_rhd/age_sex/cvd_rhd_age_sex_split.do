//Age-sex split data when necessary
import excel "strPath/epi_input_datafile.xlsx", clear sheet("extraction") firstrow

replace cases = mean*sample_size if cases==.
replace sample_size = effective_sample_size if sample_size==.

egen both_case_total = sum(cases) if sex=="Both" & group_review==1 & measure=="incidence", by(group)
egen both_sample_total = sum(sample_size) if sex=="Both" & group_review==1 & measure=="incidence", by(group)

egen male_case_total = sum(cases) if sex=="Male" & group_review==1 & measure=="incidence", by(group)
egen male_cases = max(male_case_total), by(group)
egen male_sample_total = sum(sample_size) if sex=="Male" & group_review==1 & measure=="incidence", by(group)
egen male_sample = max(male_sample_total), by(group)

egen female_case_total = sum(cases) if sex=="Female" & group_review==1 & measure=="incidence", by(group)
egen female_cases = max(female_case_total), by(group)
egen female_sample_total = sum(sample_size) if sex=="Female" & group_review==1 & measure=="incidence", by(group)
egen female_sample = max(female_sample_total), by(group)

gen age_case_weight = cases/both_case_total if sex=="Both" & group_review==1
gen age_sample_weight = sample_size/both_sample_total if sex=="Both" & group_review==1

expand 2 if sex=="Both" & group_review==1 & measure=="incidence", generate(males)
replace sex="Male" if males==1

expand 2 if sex=="Both" & group_review==1 & measure=="incidence", generate(females)
replace sex="Female" if females==1

replace mean = male_cases*age_case_weight if sex=="Male" & males==1
replace mean = female_cases*age_case_weight if sex=="Female" & females==1

replace sample_size = male_sample*age_sample_weight if sex=="Male" & males==1
replace sample_size = female_sample*age_sample_weight if sex=="Female" & females==1

drop both_case_total both_sample_total male_case_total male_cases male_sample_total male_sample female_case_total female_cases female_sample_total female_sample age_case_weight age_sample_weight males females
