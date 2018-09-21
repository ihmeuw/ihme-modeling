# Live Birth Adjustement Code

The purpose of this code is to calculate live-birth adjustments for maternal causes using age-specific fertility ratio (ASFR).  

Jobs are submitted in parallel (by year) using the submit_jobs.py script. The csv is read in at the beginning of the submit_jobs.py script. 

Each main me_id group is organized into a class (such as Eptopic, Hemorrhage, etc.) which inherits methods from the "base" class. The base class defines methods that grab locations, ASFR, draws from the causes, etc. Each class then has the same structure of:
1. Grabbing draws
2. Grabbing ASFR
3. Multiplying draws with ASFR
4. Converting incidence to prevalence by multiplying by a duration

Some me_ids are simply saved to csv and uploaded using save results (such as abortion) while others (such as eclampsia) output a sheet for epi-uploader to run another dismod model. 

Severe pre-eclampsia is subtracted from hypertension.





