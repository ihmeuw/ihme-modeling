import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import sys
import os
import platform

## set the root directory
if platform.system() == 'Windows':
	root = "FILEPATH"
else:
	root = "FILEPATH" 
	
sys.path.append(os.path.join(root,"FILEPATH"))

## load custom underlying mapping functions
from map_occupation import get_isco
from map_industry import get_isic

## mapping function takes survey id
def map(sur_id):
	## treat survey id as an int
	sur_id = int(sur_id)
	
	## load in the credentials
	## if script can't find spreadsheet, try sharing the spreadsheet to the email in secret.json
	scope = ['FILEPATH']
	creds = ServiceAccountCredentials.from_json_keyfile_name(os.path.join(root,'FILEPATH'), scope)
	client = gspread.authorize(creds)

	## load in the google sheet as a dataframe
	sheet = client.open_by_url("URL").sheet1
	df = pd.DataFrame(sheet.get_all_values())
	df.columns = df.iloc[0]
	df = df[1:]
	
	## subset gsheet to the row corresponding to the given survey id
	df.survey_id = df.survey_id.astype(int)
	df = df.ix[df.survey_id == sur_id]
	df = df.reset_index()
	
	## assign corresponding values from each column
	iso = df.iso3[0]
	md = df.metadata[0]
	isic_special = df.isic_special_mapping[0]
	isic_ver = df.isic_version[0]
	isco_special = df.isco_special_mapping[0]
	isco_ver = df.isco_version[0]
	notes = df.notes[0]
	
	## when survey is labeled skip (missing both occupation and industry), then say it is skipped and finish
	if notes.lower() == "skip":
		print(str(sur_id) + " skipped.")
	else:
		## as long as industry is present (with at least 2 digits), isic_ver will be filled out. If not, just map occupation
		if isic_ver == "":
			print("no_indus")
			no_indus = True
			get_isco(iso, md, isco_special, no_indus, isco_ver)
		## as long as occupation is present, isco_ver will be filled out. If not, just map industry
		elif isco_ver == "":
			print("no occ")
			no_occ = True
			get_isic(iso, md, isic_special, no_occ, isic_ver)
		## map both industry and occupation if both are present
		else:
			no_indus = False
			no_occ = False
			get_isic(iso, md, isic_special, no_occ, isic_ver)
			get_isco(iso, md, isco_special, no_indus, isco_ver)
		
		## let user know it is finished mapping
		print("survey_id, " + str(sur_id) + ", mapped.")

if __name__ == "__main__":
	survey_ids = list(sys.argv[1])
	for survey_id in survey_ids:
		map(survey_id)
