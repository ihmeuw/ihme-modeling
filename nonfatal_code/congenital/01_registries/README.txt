This folder contains code used to prepare congenital birth registry data for upload to the central database.

One piece of code for each of the following registries:
	- China MCHS
	- International Clearinghouse of Birth Defects Surveillance and Research (ICBDSR) 
	- National Birth Defects Prevention Network (NBDPN)
	- EUROCAT
	- the World Atlas Report 

	Each of these pieces of code:
	- pulls in files from {FILEPATH}
	- maps the registry locations to GBD locations
	- assigns the ICD codes or case names to GBD causes/models/bundles
	- summarizes to birth prevalence for each sub-cause modelable entity
	- summarizes to birth prevalence for each "total" modelable entity
	- assigns study-level covariates and other necessary information for upload
	- exports files to folders

The National Birth Defects Prevention Network (NBDPN) had the most complete list of reported case definitions 
– ie, the highest case ascertainment – across all types of congenital heart anomalies. This folder also contains code to 
adjust all other birth defects registries to match the congenital heart anomalies case ascertainment seen in the NBDPN. 
