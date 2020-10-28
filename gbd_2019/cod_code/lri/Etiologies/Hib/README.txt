#####################################################################
## These files create attributable fraction estimates for Haemophilus
## influenzae type B (Hib). These files were re-written in R from Stata
## for GBD 2019 and are pretty straightforward.
#####################################################################

* calculate_hib_paf.R: This file takes vaccine efficacy estimates for the Hib vaccine
	among children younger than 5 that reported both the reduction in all pneumonia
	cases and in invasive Hib disease. We assume that the efficacy against invasive Hib 
	disease is the same as for Hib pneumonia. The ratio of these to efficacies 
	represents the expected reduction in LRI and can be used to determine a counter factual. 
	The values by country-year are determined with the vaccine coverage estimates. 
	The values are saved several times, one is used in the second file in this folder
	and the other is used in the S pneumoniae attributable fraction. 

* export_hib_pafs.R: This file just saves the estimates as individual CSVs by location
	on the ihme shared drive so that they can be uploaded in the save_results_paf step.