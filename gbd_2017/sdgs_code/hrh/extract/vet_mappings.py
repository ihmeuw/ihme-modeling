import numpy as np
import pandas as pd
import os
from os import listdir
from os.path import isfile, join, isdir
import platform
import datetime
from datetime import date

## set the root directory
if platform.system() == 'Windows':
	root = "FILEPATH"
else:
	root = "FILEPATH"


## test for both occ and industry codes, and that over 80% of rows have an occ_major_label or minor_label (were successfully mapped)
def spottiness_test(df, no_industry, no_occ):
	## check that occ code and industry codes are both columns that are present in the dataframe
	if no_industry and "occ_code" not in list(df):
		return False
	elif no_occ and "industry_code" not in list(df):
		return False
	elif not no_industry and not no_occ and ("occ_code" not in list(df) or "industry_code" not in list(df)):
		return False

	## subset to rows of df where occ_major_label (or major_label if missing occ) is not missing and see what percentage of all rows that is
	if no_occ:
		sub_df = df[df.major_label.notnull()]
		percentage = (len(sub_df) / len(df)) * 100
	elif no_industry:
		sub_df = df[df.occ_major_label.notnull()]
		percentage = (len(sub_df) / len(df)) * 100
	else:
		sub_df1 = df[df.occ_major_label.notnull()]
		sub_df2 = df[df.major_label.notnull()]
		if len(sub_df1) > len(sub_df2):
			percentage = (len(sub_df2) / len(df)) * 100
		else:
			percentage = (len(sub_df1) / len(df)) * 100
	
	## only return true if more than 80% of rows are not missing 
	if percentage < 80.00:
		return(False)
	else:
		return(True)


## test whether any industry or occ make up over 90% of the workers in the dataset
def over_test(df, no_industry, no_occ):
	## generate proportion of all rows made up by each industry/occupation (includes unknowns in denominator, so don't necessarily sum to 100)
	if no_occ:
		job = df.major_label.value_counts() *100 / len(df)
	elif no_industry:
		job = df.occ_major_label.value_counts() *100 / len(df)
	else:
		occ = df.occ_major_label.value_counts() *100 / len(df)
		indus = df.major_label.value_counts() *100 / len(df)
		if max(indus) > max(occ):
			job = indus
		else:
			job = occ
	dt = pd.DataFrame({'proportion':job})
	dt = dt.reset_index()
	dt= dt.rename(columns = {'index':'occ-industry'}) 
	
	## check whether any proportions are above 90%. If not, return true
	over = dt.query("proportion > 90.00")
	if len(over) == 0:
		return(True)
	else:
		return(False)


## convert strata columns that are strings to numeric
def string_strata_to_integer(df):
	if "strata" in list(df):
		if str(type(df.strata[0])) == "<class 'str'>" and df.strata[0].isnumeric() == False:
			strata_string = df.strata.unique()
			diction = pd.DataFrame({'strata':strata_string})
			diction['id'] = range(1,len(diction) + 1)
			df = pd.merge(df,diction,how="left",on=['strata'])
			df = df.drop('strata', 1)
			df= df.rename(columns = {'id':'strata'})
		
	return(df)


##
def vet_all(iso3 = ""):
	mainpath = os.path.join(root,"FILEPATH")
	isolist = [d for d in os.listdir(mainpath) if os.path.isdir(os.path.join(mainpath, d))]
	if iso3 != "":
		target_index = isolist.index(iso3)
		isolist = isolist[target_index:]
	for iso in isolist:
		print(iso)
		typelist = [d for d in os.listdir(os.path.join(mainpath, iso))]
		for types in typelist:
			print(types)
			if types == "final":
				vet(iso)
			elif types == "final_with_only_occ":
				vet(iso, no_industry = True)
			elif types == "final_with_only_indus":
				vet(iso, no_occ = True)


def merge_all(no_industry = False):
	if no_industry:
		accepted_path = os.path.join(root,"FILEPATH")
	else:	
		accepted_path = os.path.join(root,"FILEPATH")
		
	hrh_cols = pd.read_csv(os.path.join(root,"FILEPATH"))
	merge_path = os.path.join(root,"FILEPATH")
	files = [f for f in listdir(accepted_path) if isfile(join(accepted_path, f))]
	for file in files:
		df = pd.read_csv(os.path.join(accepted_path,file), encoding="cp1252")
		if "occ_sub_major" not in list(df):
			print("occupation_code only 1 digit long. HRH columns cannot be merged")
		else:
			df.file_path = str(os.path.join(accepted_path,file))
			df = df.drop('Unnamed: 0', 1)
			df = df.drop('index', 1)
			df = pd.merge(df,hrh_cols,how="left",on=['occ_code'])
			df = string_strata_to_integer(df)       
			df= df.rename(columns = {'occupational_code_type':'cv_occupational_code_type', 'industrial_code_type':'cv_industrial_code_type'})
			df.to_csv(os.path.join(merge_path,file))
	print("merging done")


## vet files from a specific iso3 (need to specify whether to look in folders with industry or without it)
def vet(iso3, no_industry = False, no_occ = False):
	if isinstance(iso3, str):

		iso3 = iso3.split()
	mainpath = os.path.join(root,"FILEPATH")
	
	## only look in folder for specified iso3('s)
	dirlist = [d for d in os.listdir(mainpath) if os.path.isdir(os.path.join(mainpath, d)) and d in iso3]
	
	## allow you to vet a particular file given in the format "dir/filename" as the iso3 (without .csv at the end)
	if not dirlist:
		dirlist = [iso3[0].split("/")[0]]
		files = [iso3[0].split("/")[1] + ".csv"]

	for d in dirlist:
		if no_industry:
			filepath = root + "FILEPATH"
		elif no_occ:
			filepath = root + "FILEPATH"
		else:
			filepath = root + "FILEPATH"
			## info for testing successful occ + industry mapping
			testpath = root + "FILEPATH"
			testfiles = [f for f in listdir(testpath) if isfile(join(testpath, f))]

		savepath_occ = os.path.join(root,"FILEPATH")
		savepath_ind = os.path.join(root,"FILEPATH")
		
		## check if files already defined (would happen if running a single survey)
		if 'files' not in locals():
			files = [f for f in listdir(filepath) if isfile(join(filepath, f))]	

			## for surveys with both occ and industry, make sure that number of surveys that had industry mapped matches the number that then
			## had occ mapped
			if not no_industry and not no_occ:
				assert len(files) == len(testfiles), "final folder missing files from indus_intermediate"

		## iterate through all files in the folder
		for file in files:
			print(file)
			df = pd.read_csv(os.path.join(filepath,file), encoding="cp1252")
			df = df.drop('Unnamed: 0', 1)

			## remove extra columns (including admin_1 if admin_1_mapped is present)
			if "index" in list(df):
				df = df.drop('index', 1)
			if "admin_1_mapped" in list(df) and "admin_1" in list(df):
				df = df.drop('admin_1', 1)
			if "end_year" in list(df):
				df = df.drop('end_year', 1)
			if "level_0" in list(df):
				df = df.drop('level_0', 1)

			## if survey comes from combination of multiple original files, choose one to be the only listed
			## filepath (otherwise the collapse code will treat them as separate surveys)
			df.file_path = df.file_path[0]

			## subset to non-missing ages and sexes and occupation-relevant relevant ages only
			if ('sex_id' in df) & ('age_year' in df):
				df = df[df.sex_id.notnull()]
				df = df[df.age_year.notnull()]
				df = df[(df.age_year > 14) & (df.age_year.astype(int) < 70)]
			else:
				print(str(file) + " is missing sex_id or age_year")
				continue

			## if occupation codes are way too high, divide by 10 repeatedly until it gets down to the right range
			if "occ_code" in list(df):
				if np.mean(df.occ_code) > 10000:
					while np.mean(df.occ_code) > 10000:
						df['occ_code'] = (df['occ_code'].astype(int) // 10)
			if "industry_code" in list(df):
				if np.mean(df.industry_code) > 10000:
					while np.mean(df.industry_code) > 10000:
						df['industry_code'] = (df['industry_code'].astype(int) // 10)
			
			## check for completeness (presence of occ code, industry code, and >80% correctly mapped occupations) and that no industry (or occupation if
			## no industry is present in the data) includes over 90% of the workers in the sample
			not_spotty = spottiness_test(df,no_industry,no_occ)
			not_over = over_test(df,no_industry,no_occ)

			## if survey passes the above tests
			if not_spotty and not_over:
				if no_occ == True:
					## drop rows where occ_major is missing (occ_sub_major could not be successfully mapped) and save to csv
					df = df.dropna(subset = ['major'])
					df = df[df.major != 0]
					df = df.reset_index()
					df = df.drop('index', 1)
					df.to_csv(os.path.join(savepath_ind,file))
				elif no_industry == True:
					## drop rows where occ_major is missing (occ_sub_major could not be successfully mapped) and save to csv
					df = df.dropna(subset = ['occ_major'])
					df = df.reset_index()
					df = df.drop('index', 1)
					df.to_csv(os.path.join(savepath_occ,file))
				else:
					df_og = df.copy()
					df = df.dropna(subset = ['occ_major'])
					df = df.reset_index()
					df = df.drop('index', 1)
					df.to_csv(os.path.join(savepath_occ,file))

					df = df_og
					df = df.dropna(subset = ['major'])
					df = df[df.major != 0]
					df = df.reset_index()
					df = df.drop('index', 1)
					df.to_csv(os.path.join(savepath_ind,file))

			## provide informative errors
			elif not_spotty:
				print(str(file) + " has over 90 percent of sample in one mapped occupation/industry")
			elif not_over:
				if no_industry:
					print(str(file) + " is either missing occ code or over 20 percent of sample was not mapped to valid occupation")
				elif no_occ:
					print(str(file) + " is either missing industry code or over 20 percent of sample was not mapped to valid industry")
				else:
					print(str(file) + " is either missing occ code, missing industry code, or over 20 percent of sample was not mapped to valid occupation/industry")
			else:
				print(str(file) + " has issues with both missingness/mapping and percentage of sample in same occ/industry")
		
		## verify that there were actually files to vet
		if files == []:
			print("no files in ", filepath)
		else:
			print(str(d) + " vetting done")
	

## merge HRH codes onto the ISCO codes
def merge(iso3, no_industry = False):
	## set in/out paths
	if no_industry:
		filepath = os.path.join(root,"FILEPATH")
	else:	
		filepath = os.path.join(root,"FILEPATH")
	savepath = os.path.join(root,"FILEPATH")
	## read in categorical binary columns for HRH model that corresponds to occ_sub_majors
	hrh_cols = pd.read_csv(os.path.join(root,"FILEPATH"))
	files = [f for f in listdir(filepath) if isfile(join(filepath, f)) and f.startswith(iso3)]
	for file in files:
		df = pd.read_csv(os.path.join(filepath,file), encoding="cp1252")
		assert "occ_sub_major" in list(df), "occupation_code only 1 digit long. HRH columns cannot be merged"
		df.file_path = str(os.path.join(filepath,file))
		df = df.drop('Unnamed: 0', 1)
		df = df.drop('index', 1)
		df = pd.merge(df,hrh_cols,how="left",on=['occ_code'])
		df = string_strata_to_integer(df)       
		df= df.rename(columns = {'occupational_code_type':'cv_occupational_code_type', 'industrial_code_type':'cv_industrial_code_type'})
		df.to_csv(os.path.join(merge_path,file))
	print(str(iso3) + " merging done")
	
if __name__ == "__main__":
	iso3 = str(sys.argv[1])	
	no_industry = sys.argv[2]
	vet(iso3, no_industry)
	met(iso3, no_industry)
