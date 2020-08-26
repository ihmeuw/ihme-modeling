###########################################################################################
Other intestinal diseases Cause of Death model:

This model was inherited from the previous modeler who has built a pretty comprehensive library
of Stata code to run custom models for causes of death. Most of the code is unchanged
from what he wrote but we have needed to make several changes to make the code so that
it runs correctly on the new cluster and within the decomposition framework. 

In theory, the user simply needs to run "other_intestinal_cod_custom.do" on the cluster
and that file calls all of the other required functions.

##########################################################################################

"other_intestinal_cod_custom.do": This file is the master piece of code that runs the other
	files necessary to produce a custom cause of death model for Other intestinal diseases.

"build_cod_dataset.ado": This is a custom function to pull all COD data and covariates

"resolver.ado": This is a custom function to pull either non-fatal model results or covariates.
	Pay attention to the decomp step!

"process_predictions.ado": This file does something with the draws and launches jobs
	to process those draws before launching save_results_cod

"save_results_cod_decompX.R": These files simply exist to upload the CSV draw files to the
	COD database. This takes upwards of 4-6 hours (saves sexes sequentially) and 
	ran without problems at 100G and 10 threads. It might need fewer computational
	resources but I forgot to check qpid after the last upload.

