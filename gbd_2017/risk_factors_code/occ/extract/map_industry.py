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
def load_file(filepath, filename):
	## read stata file
	df = pd.read_stata(os.path.join(filepath,filename))
		
	## if industry_code doesnt exist, alert user 
	if "industry_code" not in list(df):
		return("industry_code does not exist in " + str(filename)) #it should raise an error instead but this is a good placeholder for now. for ex, raise Exception("industry code DNE")
		
	## subset df to just rows with a non-null industry_code
	df = df[df.industry_code.notnull()]
	df = df.reset_index()
	df = df.drop('index', 1)  

	## check if industry code is just 1-digit
	if "industry_length" in list(df):
		if df.industry_length[0] == 1 and "IPUMS" not in filename:
			return("one digit industry_code. cannot be used.")
	else:
		## check if industry code is just 1-digit
		def industry_code_one_digit(df):
			digits = 0
			for i in range(0,50):
				digits += len(str(int(df.industry_code[i])))
			if digits > 65:
				return(False)
			else:
				return(True)
		
		## if occ_code is one digit long AND we're not working with IPUMS, we can't use it
		if industry_code_one_digit(df) and "IPUMS" not in filename:
			return("one digit industry_code. cannot be used.") #it should raise an error instead but this is a good placeholder for now. for ex, raise Exception("industry code's digit < 1")
	
	## remove letters and symbols from industry code and make sure values are between 0 and 10000
	df = clean_up(df)
	
	## get the first two digits of the industry code
	if "industry_length" in list(df):
		length = int(df.industry_length[0])
	else:
		length = len(str(max(df[df.industry_code != max(df.industry_code)].industry_code)))
	exp = length - 2
	df['minor'] = (df['industry_code'].astype(int) // 10**exp).astype(str)
	
	## prepend 0 in the front if industry_code less than 2 digits
	df.minor = df.minor.astype(str).str.pad(2, fillchar='0')
	
	## return df with a new column called minor (first two digits of industry_code)
	return(df)


## remove letters and symbols from industry code and make sure values are between 0 and 10000
def clean_up(df):
	## get rid of any rows with industry_codes that are not numeric
	def no_characters(df):
		abc = list(string.ascii_lowercase) + list([" ", "&", "-"])
		ABC = list(string.ascii_uppercase) + list([" ", "&", "-"])
		df.industry_code = df.industry_code.astype(str)
		
		## if any letters or symbols are in codes, return dataframe with those rows removed
		for i in range(0, len(abc)):
			AB = df.loc[df.industry_code.str.contains(abc[i], na = False)]
			ab = df.loc[df.industry_code.str.contains(ABC[i], na = False)]
			if len(ab) + len(AB) != 0:
				df = df.loc[~df.industry_code.str.contains(abc[i], na = False)]
				df = df.loc[~df.industry_code.str.contains(ABC[i], na = False)]
		return(df)
	df = no_characters(df)

	## convert industry_code to type string
	if is_float(df.industry_code[0]):
		df.industry_code = df.industry_code.astype(float).astype(int).astype(str)
	df.industry_code = df.industry_code.astype(str)
	
	## extract numeric codes (for ints and floats)
	if df.industry_code[0].isnumeric():
		numbers = [str(int(float(i))) for i in df.industry_code if (i.isnumeric())]
	elif is_float(df.industry_code[0]):
		numbers = [str(int(float(i))) for i in df.industry_code if (is_float(i))]
	
	## subset to only numbers
	df = df.loc[df.industry_code.isin(numbers)]
	
	## switch back to integers
	df.industry_code = df.industry_code.astype(int)
	return(df)


## check it it's a float
def is_float(num):
    try:
        float(num)
        return(True)
    except ValueError:
        return(False)


## load the codes used in standard isic versions
def load_isic_map():
	isic_rev_2 = pd.read_excel(os.path.join(root,"FILEPATH"),sheetname = "ISIC_REV_2")
	isic_rev_3 = pd.read_excel(os.path.join(root,"FILEPATH"),sheetname = "ISIC_REV_3")
	isic_rev_3_1 = pd.read_excel(os.path.join(root,"FILEPATH"),sheetname = "ISIC_REV_3_1")
	isic_rev_4 = pd.read_excel(os.path.join(root,"FILEPATH"),sheetname = "ISIC_REV_4")

	## fill 0s at front to get minor to 2 no_characters
	isic_rev_2.minor = isic_rev_2.minor.astype(str).str.pad(2, fillchar='0')
	isic_rev_3.minor = isic_rev_3.minor.astype(str).str.pad(2, fillchar='0')
	isic_rev_3_1.minor = isic_rev_3_1.minor.astype(str).str.pad(2, fillchar='0')
	isic_rev_4.minor = isic_rev_4.minor.astype(str).str.pad(2, fillchar='0')

	return(isic_rev_2,isic_rev_3,isic_rev_3_1,isic_rev_4)


## load the code conversion for special mappings
def load_special_mapping(request, special_mappings):
	## compile all special mapping options from master excel file
	sheetnames = [sheetname for sheetname in special_mappings.sheet_names]

	## if requested special mapping has a sheet in the master file, output it
	if request.upper() in sheetnames:
		df = special_mappings.parse(request.upper())
		df.minor = df.minor.astype(str).str.pad(2, fillchar='0')
		return(df)
	else:
		raise Exception("Requested mapping not available")


## guess isic version for a given survey if unknown
def which_version(df,isic_rev_2,isic_rev_3,isic_rev_3_1,isic_rev_4):
	## find differences between survey industry minors and those of isic
    a = len(list(
                set(df.minor) - set(isic_rev_4.minor)
                    ))
    b = len(list(
                set(df.minor) - set(isic_rev_3_1.minor)
                    ))
    c = len(list(
                set(df.minor) - set(isic_rev_3.minor)
                    ))
    
    ## go with isic version with fewest differences from set of isic codes in survey
    if min(a,b,c) == a:
        return("isic_4")
    elif min(a,b,c) == b:
        return("isic_3_1")
    elif min(a,b,c) == c:
        return("isic_3")
    else:
        raise Exception("Multiple possible isic versions apply to this survey.")


## merge on the labels matching the isic version for the survey, then save 
def map_and_save(df, version, isic_rev_2,isic_rev_3,isic_rev_3_1,isic_rev_4, savepath, filename):
	if version == "isic_4":
		df = pd.merge(df,isic_rev_4,how="left",on=['minor'])
		df['industrial_code_type'] = "ISIC rev 4"
		df.to_csv(os.path.join(savepath,filename.replace("dta","csv")))
		print(str(savepath) + str(filename) + " saved successfully")
					
	elif version == "isic_3_1":
		df = pd.merge(df,isic_rev_3_1,how="left",on=['minor'])
		df['industrial_code_type'] = "ISIC rev 3.1"
		df.to_csv(os.path.join(savepath,filename.replace("dta","csv")))				
		print(str(savepath) + str(filename) + " saved successfully")

	elif version == "isic_3":
		df = pd.merge(df,isic_rev_3,how="left",on=['minor'])
		df['industrial_code_type'] = "ISIC rev 3"
		df.to_csv(os.path.join(savepath,filename.replace("dta","csv")))
		print(str(savepath) + str(filename) + " saved successfully")

	elif version == "isic_2":
		df = pd.merge(df,isic_rev_2,how="left",on=['minor'])
		df['industrial_code_type'] = "ISIC rev 2"
		df.to_csv(os.path.join(savepath,filename.replace("dta","csv")))
		print(str(savepath) + str(filename) + " saved successfully")
	
	else:
		raise Exception("Invalid industry coding system.")


## map industry	
def get_isic(iso3="iso3_default",metadata="",special_mapping="", no_occ = False, isic_version="guess"):	
	## load isic maps
	isic_rev_2, isic_rev_3, isic_rev_3_1, isic_rev_4 = load_isic_map()
	
	## load the stored special mappings
	special_mappings = pd.ExcelFile(os.path.join(root,"FILEPATH"))
	
	## store in/out filepaths based on presence of occ codes
	filepath = os.path.join(root,"FILEPATH")
	if no_occ:
		savepath = os.path.join(root,"FILEPATH".format(iso3))
	else:
		## if file has both occupation and industry, save the intermediate
		savepath = os.path.join(root,"FILEPATH".format(iso3))
	
	## create filepath if it doesn't already exist
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
		## load the file (involves cleaning of industry code and creation of minor)
		df = load_file(filepath, filename)
		
		## get file modification time
		try:
			mtime = os.path.getmtime(os.path.join(filepath,filename))
		except OSError:
			mtime = 0
		last_modified_date = date.fromtimestamp(mtime)
		
		if isinstance(df, str):
			raise Exception(df + str(" Try leaving isic_version column empty."))
		else: 
			## if a custom special mapping exists (with csv of mapping attached)
			if special_mapping.lower().endswith(".csv"):
				## read in special mapping and fill minors
				special_csv = pd.read_csv(special_mapping)
				assert ["minor","isic_minor"] == list(special_csv), "rename your special mapping csv's columns to 'minor' and 'isic_minor' immediately"
				special_csv['minor'] = special_csv['minor'].astype(str).str.pad(2, fillchar='0')	
				special_csv['isic_minor'] = special_csv['isic_minor'].astype(str).str.pad(2, fillchar='0')					
				## merge on minor and replace with isic
				df = pd.merge(df, special_csv, how = "left", on = ['minor'])
				df = df.drop('minor', 1)
				df= df.rename(columns = {'isic_minor':'minor'})

			## if no special mapping required
			elif special_mapping == "":
				pass
		
			## if using one of the existing special mappings		
			else:
				## read in special mapping conversions
				special_csv = load_special_mapping(special_mapping, special_mappings)					
				special_csv.columns.values[0] = "minor"
				special_csv.columns.values[1] = "isic_minor"
				special_csv['minor'] = special_csv['minor'].astype(str).str.pad(2, fillchar='0')	
				special_csv['isic_minor'] = special_csv['isic_minor'].astype(str).str.pad(2, fillchar='0')
				## merge and replace minor
				df = pd.merge(df, special_csv, how = "left", on = ['minor'])
				df = df.drop('minor', 1)
				df= df.rename(columns = {'isic_minor':'minor'})
				
			## determine isic version
			if isic_version == "guess":
				version = which_version(df,isic_rev_2,isic_rev_3,isic_rev_3_1,isic_rev_4)
			else:
				version = isic_version
			## map survey and save
			map_and_save(df, version, isic_rev_2,isic_rev_3,isic_rev_3_1,isic_rev_4, savepath, filename)	
				
if __name__ == "__main__":
	iso3 = str(sys.argv[1])
	metadata = str(sys.argv[2])
	special_mapping = str(sys.argv[3])
	isic_version = str(sys.argv[4])
	
	get_isic(iso3,metadata,special_mapping,isic_version)