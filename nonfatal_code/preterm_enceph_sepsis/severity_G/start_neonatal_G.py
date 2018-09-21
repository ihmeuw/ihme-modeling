import subprocess
				
if __name__ == '__main__':
	#Run the neonatal severity split process with the following me_ids
	birth_prev_ids = [2525, 1557, 1558, 1559, 1594]
	cfr_ids = ['cfr', 'cfr1', 'cfr2', 'cfr3', 'cfr']
	mild_prop_ids = ['long_mild', 'long_mild_ga1', 'long_mild_ga2', 'long_mild_ga3', 'long_mild']
	modsev_prop_ids = ['long_modsev', 'long_modsev_ga1', 'long_modsev_ga2', 'long_modsev_ga3', 'long_modsev']
	acauses = ['neonatal_enceph', 'neonatal_preterm', 'neonatal_preterm', 'neonatal_preterm', 'neonatal_sepsis']
	zipped = zip(birth_prev_ids, cfr_ids, mild_prop_ids, modsev_prop_ids, acauses)

	#Submit the neonatal severity split jobs, completed by neonatal_work.py
	for birth_prev, cfr, mild_prop, modsev_prop, acause in zipped:
		submission_params = ["qsub", "-P", "proj_neonatal", "-e", FILEPATH, 
						"-o", FILEPATH, "-pe", "multi_slot", "10", "-l", "mem_free=20g", "-N", 
						"%s%s" % (acause, birth_prev), FILEPATH, 
						str(birth_prev), str(cfr), str(mild_prop), str(modsev_prop), str(acause)]
		print submission_params
		subprocess.check_output(submission_params)