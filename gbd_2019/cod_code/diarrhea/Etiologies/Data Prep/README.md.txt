####################################################################
## This folder is intended to contain files that are used in 
## cleaning, processing, and generally preparing data to be used
## elsewhere in the etiology attribution. 
#####################################################################

*combine_compare_maled_gems.R: This file formats and appends the laboratory and PCR diagnostic
	test results from GEMS and MALED and saves that file.

*combine_prep_gems_tac.R: This file joins the GEMS primary results, including laboratory diagnostic,
	with the PCR test resutls and saves that file which is used in the "combine_compare_maled_gems.R" script.

*create_adjustment_matrix.do: This file pulls in results from the sensitivity/specificity analysis,
	makes a few data processing decisions, and creates a draw file for use in the attribution estimation.

*evaluate_ecoli_definition_gems_maled.R: This code file only takes the MALED and GEMS data and finds the 
	ratio of typical to all Enteropathogenic E Coli and the 
	ratio of ST- to all Enterotoxigenic E coli. These values are used
	elsewhere to crosswalk these values. 

*max_accuracy_functions.R: This file contains some functions that are used to determine the cutpoint
	in the PCR test results

*plot_max_accuracy.R: This is essentially a file to make some plots.

*pooled_sen-spe_maled_gems.R: This file creates the sensitivity and specificity estimates of the 
	laboratory diagnostic compared to the qPCR reference, including both GEMS and MALED data,
	and saves that file. That result is used in "create_adjustment_matrix.do"

*prep_maled_qpcr_data.R: This file joins the MALED results with the qPCR test results and saves the file
	that is used in the "combine_compare_maled_gems.R" script.