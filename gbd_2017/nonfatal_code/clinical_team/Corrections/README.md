These Stata scripts in FILEPATH process the raw data and keep the columns we need to pass
to the higher level Python scripts in /Corrections/correction_inputs which create the tabulated values in the FILEPATH folder
This prep data is at the ICD LEVEL so we do not need to run these each time a new map is made.
There is a rough master script at FILEPATH/00_master_correction_claim_prep.do which will
send out all the sub jobs for USA HCUP SID and NZL NMDS
