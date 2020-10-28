########################################################################
## The files in this folder are used to estimate an attributable
## fraction for Streptococcus pneumoniae (sometimes called S pneumo or 
## pneumococcal pneumonia). Like for Hib, S pneumo attributable
## fraction is based on a counterfactual estimated by determining
## the ratio of vaccine efficacy against all pneumonia and against
## pneumococcal pneumonia. Most studies report vaccine efficacy
## against invasive disease and we make an adjustment for this based
## on data in a single study that reported both efficacy against
## invasive disease and against pneumonia (the ratio is about 0.65).
## We account for the serotypes included in the vaccine (vaccine
## can be PCV7, 9, 10, 13) and for the serotype distribution by 
## geography in this analysis. Lastly, we have an age-curve for
## the attributable fraction based on the vaccine efficacy in different
## age groups. 
#########################################################################

* prep_spneumo_study_pafs.R: This file does a lot of work to prepare the 
	input vaccine efficacy data, adjust them for serotype coverage,
	and format them for use in the regression for the age-curve. The
	age-curve regression is used to determine the attributable fraction
	by age and the data that go into that model are those that report
	the vaccine efficacy against S pneumoniae and against all pneumonia/
	invasive disease. 

* winrun_lri_adjusted.do: This is a Stata file that takes the output from the
	file above (prep_spneumo_study_pafs.R) and preps them to be run through a 
	function called "BradMod". BradMod is a wrapper for the global fit in
	DisMod. The reason that this is used is because it has the functionality
	to age-integrate (we provide age ranges). This is the output that we
	need for the last step in the attributable fraction estimations. This
	file launches "winrun_troeger.do".

* winrun_file.do: This Stata file actually runs BradMod and is launched by 
	"winrun_lri_adjusted.do".

* calculate_spneumo_paf.R: This is the file that calculates the final 
	attributable fractions by draw and by age for S pneumoniae and saves them as CSVs
	by location so that the results can be uploaded in save_results_risks.