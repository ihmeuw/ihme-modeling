# Crosswalking Scripts README

### Generate scoping tables
_Age and sex specific counts._  

#### do_xwalk_table.R
Run this script to generate inital table (age/sex specific counts).
	- Input: bundle_id
	- Output: table for bundle_id, giving unique counts, age/sex specific counts, used covariates and all present in bundles, by measure

**NOTE: must run pull_used_study_labels.do in stata on the cluster, to generate input for do_xwalk_table.R for each new me/bundle added. Also, will have to save it as a csv into /share/mnch/crosswalks/study_labels/, I lazily did not remember how to export a csv in stata... Will also need to update the map in /share/mnch/crosswalks/all_me_bundles.csv**

#### xwalk_table_functions.R
Holds suite of functions used in do_xwalk_table.R. Modular functions, most based around generating a metric per unique combo of covariate values. Additions forthcoming: within/between study matches for -- ref:alt, alt:alt

#### 00_launch_create_table.R & 01_create_table.R
_In Progress_ Eventually will make this a qsub job for multiple bundle_ids, further with arguments that allow different ranges of 'fuzzy' age/location/year matching

