Ebola 2016 flowchart

1. format_ebola_WHO.do
	- redistribute ages 80+ to 80-84, 85-89, 90-94, and 95+
	- map all WHO age "0" deaths to PNN (GBD age_group_id 4)
	- add in 2016 deaths that NAME appended to a WHO file.
2. 01_ebola_cis.do
	- formatting. Be aware that the age groups in the data this code takes in are mostly WHO, except in the old ages; those ages are GBD age_groups.
3. 02_ebola_draws.do
	- do the draws.
4. /05_upload/upload_ebola_new.do
	- make the data square and ready for upload.
5. /05_upload/upload_ebola_new_save_results2016.do
	- actually upload the data using save_results().
