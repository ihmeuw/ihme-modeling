import pandas as pd
import numpy as np
import os 
import string
import platform 
import datetime
from datetime import date
import sys 

## set the root directory
if platform.system() == 'Windows':
	root = "FILEPATH"
else:
	root = "FILEPATH"

## files
from os import listdir
from os.path import isfile, join


## load file
def load_file(filepath,filename,target_length = 2,special_mapping = "",special_mappings = ""):
	## try reading in as csv, otherwise as dta
	try:
		df = pd.read_csv(os.path.join(filepath,filename), encoding="cp1252")
		df = df.drop('Unnamed: 0', 1)
	except:
		df = pd.read_stata(os.path.join(filepath,filename))
	
	## if occ_code doesn't exist, alert user
	if "occ_code" not in list(df):
		return("occ_code does not exist in " + str(filename), "oops") #it should raise an error instead but this is a good placeholder for now. for ex, raise Exception("industry code DNE")
	
	## subset df to just rows with a non-null occ_code 
	df = df[df.occ_code.notnull()]
	df = df.reset_index()
	df = df.drop('index', 1)
	
	if "occ_length" in list(df):
		if df.occ_length[0] == 1:
			return("use load_file_one_digit", df)
	## check whether occupation code is only one digit. 
	else:
		def occ_code_one_digit(df):
			digits = 0

			for i in range(0,50):
				digits += len(str(int(df.occ_code[i])))
			if digits > 65:
				return(False)
			else:
				return(True)
		
		## if occ_code is one digit long, alert user to use other load function
		if occ_code_one_digit(df):
			return("use load_file_one_digit", df) #it should raise an error instead but this is a good placeholder for now. for ex, raise Exception("industry code's digit < 1")
	
	## remove letters and symbols from occ code and make sure values are between 0 and 10000
	df = clean_up(df)
	df.occ_code = df.occ_code.astype(str)

	## if special_mapping provided, then it corresponds to a 1:1 map of occ_code
	if special_mapping != "":
		special_csv = load_special_mapping(special_mapping, special_mappings)
		special_csv.columns.values[0] = "occ_code"
		special_csv.columns.values[1] = "isco_code"
		special_csv.occ_code = special_csv.occ_code.astype(str).str.pad(target_length, fillchar='0')
		special_csv.isco_code = special_csv.isco_code.astype(str).str.pad(target_length, fillchar='0')

		df = pd.merge(df, special_csv, how = "left", on = ['occ_code'])
		df = df.drop('occ_code', 1)
		df = df.rename(columns = {'isco_code':'occ_code'})
		df = df.loc[pd.notnull(df.occ_code)] ## may be dropping failed merges that are worth looking into (errors in mapping document?)
		target_length = 2
		
	## get the first target_length number of digits of occ_code (usually 2 unless a special_mapping is involved) and store in occ_sub_major
	if "occ_length" in list(df):
		length = int(df.occ_length[0])
	else:
		length = len(str(max(df[df.occ_code != max(df.occ_code)].occ_code)))
	exp = length - target_length
	df['occ_sub_major'] = (df['occ_code'].astype(int) // 10**exp).astype(str)
	
	## prepend 0 in the front if occ_code less than the target_length number of digits
	df.occ_sub_major = df.occ_sub_major.astype(str).str.pad(target_length, fillchar='0')
	
	## return df with a new column called occ_sub_major (first target_length number digits of occ_code)
	return(df, "success")


def load_file_one_digit(df,target_length = 2):
	## remove letters and symbols from occ code and make sure values are between 0 and 10000
	df = clean_up(df)

	## create occ_major from occ_code
	df['occ_major'] = (df['occ_code'].astype(int).astype(str))
	
	## prepend 0 in the front of occ_major
	df.occ_major = df.occ_major.astype(str).str.pad(target_length, fillchar='0')
	
	## return df with a new column called occ_major (first two digits of occ_code)
	return(df)


## remove letters and symbols from occ code and make sure values are between 0 and 10000
def clean_up(df):
	## get rid of any rows with occ_codes that are not numeric
	def no_characters(df):
		abc = list(string.ascii_lowercase) + list([" ", "&", "-"])
		ABC = list(string.ascii_uppercase) + list([" ", "&", "-"])
		df.occ_code = df.occ_code.astype(str)
		
		## if any letters or symbols are in occ_codes, return dataframe with those rows removed
		for i in range(0, len(abc)):
			AB = df.loc[df.occ_code.str.contains(abc[i], na = False)]
			ab = df.loc[df.occ_code.str.contains(ABC[i], na = False)]
			if len(ab) + len(AB) != 0:
				df = df.loc[~df.occ_code.str.contains(abc[i], na = False)]
				df = df.loc[~df.occ_code.str.contains(ABC[i], na = False)]
		return(df)
	df = no_characters(df)	
	
	## convert occ code to type string
	if is_float(df.occ_code[0]):
		df.occ_code = df.occ_code.astype(float).astype(int).astype(str)
	df.occ_code = df.occ_code.astype(str)
	
	## extract occ code numbers (have to do it two ways because isnumeric doesn't work for floats)
	if df.occ_code[0].isnumeric():
		numbers = [str(int(float(i))) for i in df.occ_code if (i.isnumeric())]
	elif is_float(df.occ_code[0]):
		numbers = [str(int(float(i))) for i in df.occ_code if (is_float(i))]
	
	## subset to only numbers
	df = df.loc[df.occ_code.isin(numbers)]

	## switch back to integers
	df.occ_code = df.occ_code.astype(int)
	
	## if occ codes seem to be very high, divide by ten until they are in correct range, then subset to those between 0 and 10000
	if np.mean(df.occ_code) > 10000:
		while np.mean(df.occ_code) > 10000:
			df['occ_code'] = (df['occ_code'].astype(int) // 10)     
	df = df.query("0 < occ_code < 10000")
	df = df.reset_index()
	df = df.drop('index', 1)
	
	## return dataframe
	return(df)


## check if it's a float	
def is_float(num):
    try:
        float(num)
        return(True)
    except ValueError:
        return(False)


## load the codes used in standard isco versions
def load_isco_map():
	isco_08 = pd.read_excel(os.path.join(root,"FILEPATH"),sheetname = "ISCO_08")
	isco_88 = pd.read_excel(os.path.join(root,"FILEPATH"),sheetname = "ISCO_88")
	isco_68 = pd.read_excel(os.path.join(root,"FILEPATH"),sheetname = "ISCO_68")
	
	## fill 0s at front to get occ_sub_major to 2 characters
	isco_08.occ_sub_major = isco_08.occ_sub_major.astype(str).str.pad(2, fillchar='0')
	isco_88.occ_sub_major = isco_88.occ_sub_major.astype(str).str.pad(2, fillchar='0')
	isco_68.occ_sub_major = isco_68.occ_sub_major.astype(str).str.pad(2, fillchar='0')
	return(isco_08,isco_88, isco_68)


## load 1-digit versions of standard isco versions
def load_isco_map_one_digit():
	isco_08_1 = pd.read_excel(os.path.join(root,"FILEPATH"),sheetname = "ISCO_08")
	isco_88_1 = pd.read_excel(os.path.join(root,"FILEPATH"),sheetname = "ISCO_88")
	isco_68_1 = pd.read_excel(os.path.join(root,"FILEPATH"),sheetname = "ISCO_68")
	
	## fill 0s at front to get occ_sub_major to 2 no_character
	isco_08_1.occ_major = isco_08_1.occ_major.astype(str).str.pad(2, fillchar='0')
	isco_88_1.occ_major = isco_88_1.occ_major.astype(str).str.pad(2, fillchar='0')
	isco_68_1.occ_major = isco_68_1.occ_major.astype(str).str.pad(2, fillchar='0')
	return(isco_08_1,isco_88_1, isco_68_1)	


## load the code conversion for special mappings
def load_special_mapping(request, isco):
	## compile all special mapping options from master excel file
	sheetnames = [sheetname for sheetname in isco.sheet_names]
	
	## if requested special mapping has a sheet in the master file, output it
	if request.upper() in sheetnames:
		df = isco.parse(request.upper())
		return(df)
	else:
		raise Exception("Requested mapping not available")


## guess the isco version for a given survey
def which_version(df,isco_08,isco_88, isco_68):
    ## find differences in codes between occ_sub_majors in dataset vs. standard isco versions
    a = len(list(
                set(df.occ_sub_major) - set(isco_08.occ_sub_major)
                    ))
    b = len(list(
                set(df.occ_sub_major) - set(isco_88.occ_sub_major)
                    ))
    c = len(list(
                set(df.occ_sub_major) - set(isco_68.occ_sub_major)
                    ))
    
    ## identify the standard system with the fewest differences from the survey in question
    if min(a,b,c) == a:
        return("isco_08")
    elif min(a,b,c) == b:
        return("isco_88")
    elif min(a,b,c) == c:
        return("isco_68")
    else:
        raise Exception("Multiple possible isco versions apply to this survey.")


## guess the isco version for a given survey for which occupations are 1-digit
def which_version_one_digit(df,isco_08_1,isco_88_1, isco_68_1):
    a = len(list(
                set(df.occ_major) - set(isco_08_1.occ_major)
                    ))
    b = len(list(
                set(df.occ_major) - set(isco_88_1.occ_major)
                    ))
    c = len(list(
                set(df.occ_major) - set(isco_68_1.occ_major)
                    ))
    if min(a,b,c) == a:
        return("isco_08")
    elif min(a,b,c) == b:
        return("isco_88")
    elif min(a,b,c) == c:
        return("isco_68")
    else:
        raise Exception("Multiple possible isco versions apply to this survey.")


## merge on the labels matching the isco version for the survey, then save
def map_and_save(df,version, isco_08, isco_88, isco_68, savepath, filename):
	## merge dataset and isco coding dataset by occ_sub_major and save (or produce alert if no version was found)
	if version == "isco_08":
		df = pd.merge(df,isco_08,how="left",on=['occ_sub_major'])
		df['occupational_code_type'] = "ISCO 08"
		df.to_csv(os.path.join(savepath,filename.replace("dta","csv")))
		print(str(savepath) + str(filename) + " saved successfully")
	elif version == "isco_88":
		df = pd.merge(df,isco_88,how="left",on=['occ_sub_major'])
		df['occupational_code_type'] = "ISCO 88"
		df.to_csv(os.path.join(savepath,filename.replace("dta","csv")))
		print(str(savepath) + str(filename) + " saved successfully")
	elif version == "isco_68":
		df = pd.merge(df,isco_68,how="left",on=['occ_sub_major'])
		df['occupational_code_type'] = "ISCO 68"
		df.occ_major = '="' + df.occ_major.astype(str) +'"'                      
		df.to_csv(os.path.join(savepath,filename.replace("dta","csv")))
		print(str(savepath) + str(filename) + " saved successfully")
	else:
		raise Exception("Invalid occupational coding system.")


## merge on the labels matching the isco version for the survey, then save (if only has occ_major/1-digit)
def map_and_save_one_digit(df,version, isco_08_1, isco_88_1, isco_68_1, savepath, filename):
	if version == "isco_08":
		df = pd.merge(df,isco_08_1,how="left",on=['occ_major'])
		df['occupational_code_type'] = "ISCO 08"
		df.to_csv(os.path.join(savepath,filename.replace("dta","csv")))
		print(str(savepath) + str(filename) + " saved successfully")			
	elif version == "isco_88":
		df = pd.merge(df,isco_88_1,how="left",on=['occ_major'])
		df['occupational_code_type'] = "ISCO 88"
		df.to_csv(os.path.join(savepath,filename.replace("dta","csv")))
		print(str(savepath) + str(filename) + " saved successfully")
	elif version == "isco_68":
		df = pd.merge(df,isco_68_1,how="left",on=['occ_major'])
		df['occupational_code_type'] = "ISCO 68"
		df.occ_major = '="' + df.occ_major.astype(str) +'"'                  
		df.to_csv(os.path.join(savepath,filename.replace("dta","csv")))
		print(str(savepath) + str(filename) + " saved successfully") 		
	else:
		raise Exception("Invalid occupational coding system.")		


## map occupations
def get_isco(iso3="",metadata="",special_mapping="",no_industry = False,isco_version="guess"):
	## load isco maps
	isco_08,isco_88, isco_68 = load_isco_map()
	
	## load isco one digit maps
	isco_08_1,isco_88_1, isco_68_1 = load_isco_map_one_digit()
	
	## load the stored special mappings
	special_mappings = pd.ExcelFile(os.path.join(root,"FILEPATH"))

	## set filepaths based on presence or absense of industry codes
	if no_industry:
		filepath = os.path.join(root,"FILEPATH")
		savepath = os.path.join(root,"FILEPATH".format(iso3))
	else:
		filepath = os.path.join(root,"FILEPATH".format(iso3))
		savepath = os.path.join(root,"FILEPATH".format(iso3))
		
	## if save path does not exist, create it
	if not os.path.exists(savepath):
		os.makedirs(savepath)
		
	## compile all files that contain the iso3 code followed by the corresponding metadata
	onlyfiles = [f.replace("DTA","dta") for f in listdir(filepath) 
					if (
						isfile(join(filepath, f)) and (f.startswith(iso3 + metadata))
						)
						]
	## output error if no such file exists
	assert len(onlyfiles) > 0, "no file that starts with " + str(iso3) + str(metadata) + " exists in " + str(filepath)
	
	## for every file with specified iso3 and metadata:
	for filename in onlyfiles:
		
		target_length = 2
		if "3d" in special_mapping.lower():
			target_length = 3
		if "4d" in special_mapping.lower():
			target_length = 4

		## load the file (involves cleaning of occ_code and creation of occ_sub_major)
		## if special_mapping corresponds to a 1:1 mapping of occ_code (rather than just mapping
		## occ_sub_major), then it will replace the original occ_code with the remapped isco code
		## and then treat the survey as a normal isco survey
		if any(x in special_mapping.lower() for x in ["3d","4d"]) & (special_mapping.lower() != "nco_3d"):
			df, string = load_file(filepath, filename, target_length, special_mapping, special_mappings)
			special_mapping = ""
		else:
			df, string = load_file(filepath, filename, target_length)
		
		## get file modification time
		try:
			mtime = os.path.getmtime(os.path.join(filepath,filename))
		except OSError:
			mtime = 0
		last_modified_date = date.fromtimestamp(mtime)
		
		## only merge on the ISCO columns if occ_code exists or is valid (no error output from load_file())
		if isinstance(df, pd.core.frame.DataFrame):
			## if custom special mapping exists (denoted by a filepath to the csv with the mapping relationship)
			if special_mapping.lower().endswith(".csv"):
				## read in special mapping and fill sub_majors
				special_csv = pd.read_csv(special_mapping)
				special_csv.columns.values[0] = "occ_sub_major"
				special_csv.columns.values[1] = "isco_sub_major"
				special_csv['occ_sub_major'] = special_csv['occ_sub_major'].astype(str).str.pad(2, fillchar='0')
				special_csv['isco_sub_major'] = special_csv['isco_sub_major'].astype(str).str.pad(2, fillchar='0')
				
				## merge on original occ_sub_major, then set new occ_sub_major as isco version
				df = pd.merge(df, special_csv, how = "left", on = ['occ_sub_major'])
				df = df.drop('occ_sub_major', 1)
				df = df.rename(columns = {'isco_sub_major':'occ_sub_major'})
				
				## if isco version is not specified, use which_version function to label it
				if "guess" in isco_version.lower():
					version = which_version(df,isco_08,isco_88, isco_68)
				else:
					version = isco_version.lower()
				
				## map the survey and save it
				map_and_save(df,version, isco_08, isco_88, isco_68, savepath, filename)

			## if no special mapping is needed		
			elif special_mapping.lower() == "":
				## if isco version is not specified, use which_version function to label it
				if "guess" in isco_version.lower():
					version = which_version(df,isco_08,isco_88, isco_68)
				else:
					version = isco_version.lower()
				
				## map the survey and save it
				map_and_save(df,version, isco_08, isco_88, isco_68, savepath, filename)
			
			## if using one of the existing special mappings
			else:				
				special_csv = load_special_mapping(special_mapping, special_mappings)
				special_csv.columns.values[0] = "occ_sub_major"
				special_csv.columns.values[1] = "isco_sub_major"
				special_csv.occ_sub_major = special_csv.occ_sub_major.astype(str).str.pad(target_length, fillchar='0')
				special_csv.isco_sub_major = special_csv.isco_sub_major.astype(str).str.pad(2, fillchar='0')
				
				## merge and replace after converting to isco
				df = pd.merge(df, special_csv, how = "left", on = ['occ_sub_major'])
				df = df.drop('occ_sub_major', 1)
				df = df.rename(columns = {'isco_sub_major':'occ_sub_major'})					
				
				## if isco version is not specified, use which_version function to label it
				if "guess" in isco_version.lower():
					version = which_version(df,isco_08,isco_88, isco_68)
				else:
					version = isco_version.lower()

				## map the survey and save it
				map_and_save(df,version, isco_08, isco_88, isco_68, savepath, filename)				
		
		## if occ_code's digit is equal to 1 and there exists a custom special mapping csv
		elif df == "use load_file_one_digit" and special_mapping.lower().endswith(".csv"):
			## use proper load function
			df = load_file_one_digit(string)
			
			## load custom special mapping
			special_csv = pd.read_csv(special_mapping)
			special_csv.columns.values[0] = "occ_major"
			special_csv.columns.values[1] = "isco_occ_major"
			
			## make sure custom mapping is actually 1-digit
			assert len(str(int(special_csv['occ_major'][len(special_csv)-2]))) == 1, "your custom special mapping is what broke this script. shame on you. See 'FILEPATH' for an working example"
			special_csv['occ_major'] = special_csv['occ_major'].astype(str).str.pad(2, fillchar='0')	
			special_csv['isco_occ_major'] = special_csv['isco_occ_major'].astype(str).str.pad(2, fillchar='0')
			
			## merge and replace isco codes
			df = pd.merge(df, special_csv, how = "left", on = ['occ_major'])
			df = df.drop('occ_major', 1)
			df = df.rename(columns = {'isco_occ_major':'occ_major'})
			
			## guess version if applicable
			if "guess" in isco_version.lower():
				version = which_version_one_digit(df,isco_08_1,isco_88_1, isco_68_1)
			else:
				version = isco_version.lower()
				
			map_and_save_one_digit(df,version, isco_08_1, isco_88_1, isco_68_1, savepath, filename)
		
		## if occ_code has one digit and no special mapping required
		elif df == "use load_file_one_digit" and special_mapping.lower() == "":
			## use proper load_file
			df = load_file_one_digit(string)
			
			## determine isco version
			if "isco" not in isco_version:
				version = which_version_one_digit(df,isco_08_1,isco_88_1, isco_68_1)	
			else:
				version = isco_version

			## map and save
			map_and_save_one_digit(df,version, isco_08_1, isco_88_1, isco_68_1, savepath, filename)
		
		## load file did not work properly on survey
		else:
			raise Exception(df)

if __name__ == "__main__":
	iso3 = str(sys.argv[1])
	metadata = str(sys.argv[2])
	special_mapping = str(sys.argv[3])
	no_industry = str(sys.argv[4])
	isco_version = str(sys.argv[5])	
	get_isco(iso3,metadata,special_mapping,no_industry,isco_version)
	            