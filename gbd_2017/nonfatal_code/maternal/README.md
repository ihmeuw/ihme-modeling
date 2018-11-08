# Live Birth Adjustement Code

The purpose of this code is to calculate live-birth adjustments for maternal causes using age-specific fertility ratio (ASFR).

Jobs are submitted in parallel (by year) using the submit_jobs.py script. In order to change what modelable_entity_ids(me_ids) you want to perform adjustments for, add conditional statements in the submit_jobs.py script to look for specific me_ids. 
The me_ids are listed in the dependency map csv that is read in at the beginning of the submit_jobs.py script. 

Each main me_id group is organized into a class (such as Eptopic, Hemorrhage, etc.) which inherits methods from the "base" class. The base class defines methods that grab locations, ASFR, draws from the causes, etc. Each class then has the same structure of:
1. Grabbing draws
2. Grabbing ASFR
3. Multiplying draws with ASFR
4. Converting incidence to prevalence by multiplying by a duration

Some me_ids are simply saved to csv and uploaded using save results (such as abortion) while others (such as eclampsia) output a sheet for epi-uploader to run another dismod model. 

Severe pre-eclampsia is subtracted from hypertension.

For GBD 2017 the age restriction on the fistula model was changed from 10-54 years to 10-95+ years. There was also discussion regarding the zero'ing out of specific locations in the 1553 and 1554 me_ids.
To create me_ids 1553 and 1554 with zero'd out locations, change the dependency map so that 16535 is the input_me for output_mes 1553;1554. Note that 16535 is populated with data regardless of whether or not it used further in the process.
To create me_ids 1553 and 1554 without zero'd out locations, change the dependency map so that 1552 is the input_me for output_mes 1553;1554.



