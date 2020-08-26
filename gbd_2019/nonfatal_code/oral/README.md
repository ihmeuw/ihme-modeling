
# Dental pipeline

##### gen_save_messages.R

This is a standalone R script, that *should be run before every run of the master.do script*. This script pulls the current best model_versions for each parent_id, as well as the split_ids. This is to generate a save_results message that reflects the current models used in this pipeline. Must be run every time new models are marked best. *NOTE:* there are still some manual steps here. Must update the prog_dir in gen_save_messages, as well as further down in 01_saveresults.R

_To run_: login to the cluster, get a qlogin, start an R session in the cluster by typing in _R_, source the function at it's filepath

#### master.do

This is the main submitter script for the dental pipeline. This runs each of 5 steps, that all have their own qsubbed jobs running further scripts. This script will have to be maintained for changes in the shared functions and cluster.

#### 01-05 job scripts

These are the scripts that do the heavy-lifting of this pipeline. In general, they read in draws of a parent, split those draws based on proportions generated from a distribution of symptomatic vs. asymptomatic

#### 00_launch_savejobs.R

This is the submitter script for a parallelized launch of the 01_saveresults.R script, parallelized by modelable_entity_id. Variable within the script allows specification of which mes to upload. Can be edited in case you only want to upload a subset of mes.

#### 01_saveresults.R

This script is called by 00_launch_savejobs.R. This script will need to be maintained for changes to save_results_epi. 

Users must manually change:
	* input_dir - to personal scratch folder that stores the draws generated
	* upload_map - filepath to updated save_descriptions.csv, housed in personal code repo