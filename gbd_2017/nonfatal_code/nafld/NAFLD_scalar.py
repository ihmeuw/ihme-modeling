# Author:
# Date: 03/30/2018
# Purpose: Parallelizes NAFLD scalar script 


import os
import db_queries as db 
import shutil
import datetime

#making directory
date = datetime.datetime.now()
date = date.strftime("%Y_%m_%d")
scratch = "FILEPATH"

file = os.path.join(scratch, date)

if not os.path.exists(file):
	os.makedirs(file)

#shutil.rmtree(scratch) #deleting previous files if breaks

#path to shell
shell = "FILEPATH"

locations = db.get_location_metadata(location_set_id=9)
locations = locations["location_id"]

for location in locations:
	slots = 1
	qsub = "qsub -pe multi_slot {slots} -l mem_free={ram}g -P proj_custom_models -N NAFLD_{location} "\
			"-o "FILEPATH" -e "FILEPATH" {shell} "\
			""FILEPATH" {location}"
	qsub = qsub.format(slots=slots, ram=slots*2, shell=shell, location=location)
	print "calculating prevalence for {}".format(location)
	os.popen(qsub)