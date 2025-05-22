from winnower.custom.base import TransformBase
import pandas
import numpy
import math
from datetime import datetime

class Transform(TransformBase):

	def output_columns(self, input_columns):
		print("\n-----------------------------",flush=True)
		print("\nEXTRACTING cb_basic_id #: ",self.config['cb_basic_id'],flush=True)

		res = input_columns + ['measurement', 'hemoglobin_raw', 'hemoglobin_alt_adj','hemog_alt_smoke_adjust','brinda_adj_hemog','pregnant_orig',
		'cv_pregnant','smoking_status','smoking_number','cluster_altitude_unit',
		'anemia_mild_raw','anemia_moderate_raw','anemia_severe_raw','anemia_anemic_raw',
		'anemia_mild_adj','anemia_moderate_adj','anemia_severe_adj','anemia_anemic_adj',
		'anemia_mild_brinda','anemia_moderate_brinda','anemia_severe_brinda','anemia_anemic_brinda',
		'who_adj_hemog','anemia_mild_who','anemia_moderate_who','anemia_severe_who','anemia_anemic_who',
		'trimester','last_preg_days_ago','last_menstrual_period_days','cv_lactating']

		return res


	def execute(self,df):
		#df = self._update_sex_id(df)
		df = self._update_age_year(df)
		df = self._get_pregnancy_status(df)
		#df = self._calculate_menstrual_period_date(df)
		#df = self._find_date_of_last_preg(df)
		#df = self._get_smoking_status(df)
		df = self._get_smoking_freq(df)
		df = self._keep_hemog_obs(df)
		df = self._sift_hemog(df)
		#df = self._update_survey_adj_hemog(df)
		df = self._calculate_brinda_who(df)
		df = self._assign_anemia_status(df)
		return df

	#################################################
	# SEX STATUS ASSIGNMENT SECTION

	"""
	update the sex id to be 3 in the case the value is blank, and update values accordingly given survey module
	"""
	def _update_sex_id(self,df):
		print("\nUPDATING SEX ID",flush=True) #update user it is in update_sex_id function
		update_needed = False
		if 'sex_id' in df:
			selector = (df.sex_id.isna()) | ((df.sex_id!=1) & (df.sex_id!=2))
			if len(df[selector].index)>0:
				df.loc[selector,'sex_id'] = 3 #update it to be 3 (both M and F)
				print("\tWARNING: Some sex_id vars not set, defaulting to 3. Please check survey and codebook and rerun!",flush=True)
				update_needed = True
		else:
			df['sex_id'] = 3 #if sex_id has not yet been codebooked, default everything to both M and F
			print("\tWARNING: NO sex_id vars set, defaulting to 3. Please check survey and codebook and rerun!",flush=True)
			update_needed = True

		if self.config['survey_module']=="WN":
			df.loc[df.sex_id==3,'sex_id'] = 2
			if update_needed:
				print("\tUPDATE: given that it was a women's survey module, all sex_id that were set to 3 have been set to 2.",flush=True)
		elif self.config['survey_module']=="MN":
			df.loc[df.sex_id==3,'sex_id'] = 1
			if update_needed:
				print("\tUPDATE: given that it was a men's survey module, all sex_id that were set to 3 have been set to 1.",flush=True)
		return df


	#################################################
	# AGE ASSIGNMENT SECTION

	"""
	This section of code deals with redefining the age_year value to be as granular as possible given the presence of age_month and/or age_day variables in the survey.
	This is necessary if a survey contains hemoglobin data for children < 0.5 years old, due to the varying anemia severity thresholds for children < 0.5 years old.
	This function will check to see if it can update age_year to reflect either age_month or age_day and they see what the minimum age is in the survey that was flagged in codebooking.
	If a survey isn't able to meet the granularity levels we are looking for, it will flag this survey to be looked at later.
	"""
	def _update_age_year(self,df):
		print("\nUPDATING AGE YEAR",flush=True)
		if 'age_month' in df: #if age month is in df
			print("\tUPDATE: age_year updated to reflect age_month values.", flush=True)
			selector = (df.age_month<60) & (df.age_year<5) #update age year for all children under 5 years old
			df.loc[selector,'age_year'] = df.age_month/12
		if 'age_day' in df: #if age day is in the df
			print("\tUPDATE: age_year updated to reflect age_day values.", flush=True)
			selector = (df.age_day<=365) & (df.age_year<1) #update age year for all children under 1 years old
			df.loc[selector,'age_year'] = df.age_day/365.25
		if 'age_year' not in df:
			print("\tWARNING: NO age_year vars set. Please check survey and codebook and rerun!",flush=True)
		elif 'age_year' in df and 'age_sampling_lower' in df:
			if 'age_month' not in df and 'age_day' not in df and float(self.config['age_sampling_lower'])>=.5: #if age_year is in the df but not age_month and age_day, but the min age is >0.5 years old
				df.loc[df.age_year<1,'age_year'] = .5 #update everything that is below 1 years old to be 6 months old since the 6-11 month age range all have the same anemia thresholds
			elif 'age_month' in df and 'age_day' not in df and float(self.config['age_sampling_lower'])<.083334:
				print("\tWARNING: minimum age denoted in codebook as <1 month, but no age_day present in survey to confirm granularity of age below 1 month. Check output post extraction and impute age later based on age-split mean hemog levels.",flush=True)
			elif 'age_month' not in df and 'age_day' not in df and float(self.config['age_sampling_lower'])<.5:
				print("\tWARNING: minimum age denoted in codebook as <6 months, but no age_month or age_day to confirm granularity of age below 6 months. Check output post extraction and impute age later based on age-split mean hemog levels.",flush=True)
		return df

	#################################################
	# PREGNANCY & TRIMESTER STATUS ASSIGNMENT SECTION

	"""
	helper function that calculates the trimester of a pregnancy
	"""
	def _calculate_trimester(self,df):
		selector = (df.time_preg.notna()) & (df.sex_id==2) & (df.age_year>=10) & (df.age_year<55) & (df.cv_pregnant==1) & (df.time_preg>=0) & (df.time_preg<=3)
		df.loc[selector,'trimester'] = 1
		selector = (df.time_preg.notna()) & (df.sex_id==2) & (df.age_year>=10) & (df.age_year<55) & (df.cv_pregnant==1) & (df.time_preg>=4) & (df.time_preg<=6)
		df.loc[selector,'trimester'] = 2
		selector = (df.time_preg.notna()) & (df.sex_id==2) & (df.age_year>=10) & (df.age_year<55) & (df.cv_pregnant==1) & (df.time_preg>=7) & (df.time_preg<=10)
		df.loc[selector,'trimester'] = 3
		return df

	"""
	function that gets pregnancy status of each women in the survey
	"""
	def _get_pregnancy_status(self,df):
		print("\nGETTING PREGNANCY STATUS",flush=True)
		df['cv_pregnant'] = 0 #default pregnancy status to 0
		df['trimester'] = 0 #default trimester status to 0
		if 'pregnant' in df and self.config['pregnant'] is not None:
			df['pregnant_orig'] = df['pregnant'] #copy over the pregnancy status column
			preg_tuple = self.config['pregnant_true'] #get the 'true' meta-values from the survey
			for tup in preg_tuple: #parse through the true values
				selector = (df['pregnant']==int(float(tup))) & (df['age_year']>=10) & (df['sex_id']==2) #if pregnancy value is present and true, if female, and >10 yo
				df.loc[selector,'cv_pregnant'] = 1 #then update pregnancy status to true!

			#if time_preg is has been defined, this section will attempt to determine the trimester of the pregnancy in _calculate_trimester()
			if self.config['time_preg'] is not None:
				try:
					df = self._calculate_trimester(df)
					print("\tDetermining trimester",flush=True)
				except:
					print("\tUNABLE to determine trimester. Error occured, check output.",flush=True)
			else:
				print("\tNOT determining trimester (no trimester variable defined from original survey).",flush=True)

		else:
			print("\tWARNING: no pregnancy variable. Check survey and codebook and rerun!",flush=True)

		return df

	#################################################
	# CONVERT AND CALCULATE LAST MENSTRUAL PERIOD

	"""
	function that converts mentrual period date in days
	#   last_menstrual_period is formatted in DHS surveys as 3 digit number, where first digit is denoting time in days, weeks, months, years, shown below:
	#   1 -> days ago
	#   2 -> weeks ago
	#   3 -> months ago
	#   4 -> years ago
	#   Example -> 302 = 2 months ago
	#   Example -> 106 = 6 days ago
	"""
	def _convert_menstrual_period_date(self,val):
		val = str(val)
		count_mult_vec = [1,7,30.45,365.25] #day, week, month, years all in days
		time_type = int(val[0])-1 #get the day, week, month, year flag to use as an index for the count_mult_vec
		date_mult = count_mult_vec[time_type] #get the length in days
		days_ago = date_mult*float(val[1:]) #multiply it by the time span (aka the last 2 digits of the given value)
		return int(days_ago)

	"""
	function that calculates last menstrual period into days
	"""
	def _calculate_menstrual_period_date(self,df):
		df['last_menstrual_period_days'] = -1
		if self.config['last_menstrual_period'] is not None:
			try:
				selector = (df.last_menstrual_period.notna()) & (df.sex_id==2) & (df.age_year>=10) & (df.age_year<55) & (df.last_menstrual_period < 500) & (df.last_menstrual_period > 0)
				df.loc[selector,'last_menstrual_period_days'] = df.loc[selector,'last_menstrual_period'].apply(self._convert_menstrual_period_date)
				print("\tDetermining last menstrual period",flush=True)

				if 'time_preg' not in df or self.config['time_preg'] is None or (len(df['time_preg'].unique())==1 and df['time_preg'].unique()[0]==0):

					'''
					If a survey doesn't provide months pregnant data, we can use menstrual period data to calculate the months a woman has been pregnant.

					Using data where both months pregnant data and menstrual period data were present, we came up with a linear fit to accurately predict the months a respondent has been pregnant:

					y = 29.4172457x + 14.2232113

					The requirements for using this linear fit model is as follows:
						- The respondent needs to be preganat --> cv_pregnant == 1
						- The respondent falls in the category of WRA --> sex_id == 2 & age_year >= 10 & age_year < 55
						- The menstrual period data was reported in months --> the first digit of the three digit number to denote days since last menstrual period is 3
					'''

					df['time_preg'] = float('nan')

					selector = (df.last_menstrual_period.notna()) & (df.sex_id==2) & (df.age_year>=10) & (df.age_year<55) & (df.last_menstrual_period < 400) & (df.last_menstrual_period > 300) & (df.cv_pregnant==1)
					selector = selector & (df.last_menstrual_period_days.notna()) & (df.last_menstrual_period_days > 0) & (df.last_menstrual_period_days < 365)

					m_val = 29.4172457
					b_val = 14.2232113

					df.loc[selector,'time_preg'] = round(df.loc[selector,'last_menstrual_period_days']*m_val + b_val)
					df = self._calculate_trimester(df)

					print("\tUsing last menstrual period to calculate the months a woman has been pregnant (and then assigning trimester).",flush=True)

			except:
				print("\tUNABLE to determine last menstrual period. Error occured, check output.",flush=True)
		else:
			print("\tNOT determining last menstrual period",flush=True)

		return df

	#################################################
	# FINDING THE DATE OF THE LAST PREGNANCY TO DETERMINE LACTATING STATUS

	"""
	step 2.2 helper function - converts YYYY/MM/DD dates to date time
	"""
	def _convert_to_date_time(self,yr,month,day):
		toReturn = datetime(yr,month,15) #dhs defaults all dates without day to the 15th of the month
		return toReturn

	"""
	step 2.3 helper function - converts cmc date to YYYY/MM/DD dates
	# Starting date is January 1900
	# CMC = (YY*12) + MM -> use for years 1900 to 1999
	# CMC = ((YYYY-1900) * 12) + MM -> for years >= 2000
	# YYYY = int((CMC - 1) / 12)+1900 -> ”int” is equivalent to “floor” given numbers are > 0 (i.e. truncate decimals)
	# MM = CMC - ((YYYY-1900) * 12)
	"""
	def _convert_cmc_date(self,val):
		dis_year = math.floor((int(val)-1)/12)+1900
		dis_month = int(val) - ((dis_year-1900)*12)
		dhs_day_of_month_default = 15 #DHS surveys default the day of interview of survey to the 15th if not otherwise specified
		good_date = self._convert_to_date_time(dis_year,dis_month,dhs_day_of_month_default)
		return good_date

	"""
	This funciton determines the date of the last pregnancy
	"""

	def _find_date_of_last_preg(self,df):
		if 'child_dob' in df and 'hh_member_index' in df and 'youngest_child_index' in df and 'int_year' in df and 'int_month' in df:
			print("\tDetermining lactating status",flush=True)

			good_cols = ['strata_id','psu','atomic_hh_id','hh_member_index','youngest_child_index','int_year','int_month']
			if 'int_day' in df:
				good_cols.append('int_day')
			selector = (df.sex_id==2) & (df.age_year >= 10) & (df.age_year < 55) & (df.cv_pregnant==0) & (df.youngest_child_index.notna()) & (df.atomic_hh_id.notna()) & (~numpy.isnan(df.youngest_child_index))
			#subset the main df to a smaller df that only contains information of moms that have a reference to their youngest child's household member index
			mom_df = df.loc[selector,good_cols].copy()
			mom_df['last_preg_days_ago'] = float('nan')
			mom_df['cv_lactating'] = 999 #default all values of cv_lactating to 999

			for r in range(len(mom_df.index)):
				dis_hh = int(mom_df.iloc[[r]]['atomic_hh_id']) #get the household ID
				child_id = int(mom_df.iloc[[r]]['youngest_child_index']) #get the household line index of the youngest child
				dis_strata = mom_df.iloc[[r]]['strata_id'].values[0] #get the strata that the household is in
				dis_psu = int(mom_df.iloc[[r]]['psu']) #get the cluster the household is in

				selector = (df.hh_member_index == child_id) & (df.atomic_hh_id == dis_hh) & (df.strata_id == dis_strata) & (df.psu == dis_psu) #find the row of the youngest child in the survey

				dob = df.loc[selector,'child_dob'].values #subset the main df with the information from the mom dataframe
				if len(dob) == 1: #check to see if there is any information brought back from the subset call
					try:
						dob = int(dob[0]) #try to get the cmc dob
					except Exception as e:
						dob = None #else reset the dob to none
				else:
					dob = None

				if dob is not None: #if a date was retrieved
					child_dob_dt = self._convert_cmc_date(dob)  #convert the cmc date to a datetime object
					int_year = int(mom_df.iloc[[r]]['int_year']) #pull in the interview date
					int_month = int(mom_df.iloc[[r]]['int_month'])
					dhs_day_of_month_default = 15 #DHS surveys default the day of interview of survey to the 15th if not otherwise specified
					int_day = dhs_day_of_month_default
					if 'int_day' in df and ~numpy.isnan(float(mom_df.iloc[[r]]['int_day'])): #try to pull in the interview day if it's present in the survey
						int_day = int(mom_df.iloc[[r]]['int_day'])
					interview_dt = self._convert_to_date_time(int_year,int_month,int_day) #convert the interview date to a datetime object
					day_diff = interview_dt - child_dob_dt #get the difference of the youngest childs dob and the interview date
					day_diff = day_diff.days #convert the difference to days
					mom_df.iat[r,mom_df.columns.get_loc('last_preg_days_ago')]=day_diff #insert it into the df

					#now set the cv_lactating flag
					if day_diff <= 42:
						mom_df.iat[r,mom_df.columns.get_loc('cv_lactating')] = 2
					elif day_diff > 42 and day_diff <= 365:
						mom_df.iat[r,mom_df.columns.get_loc('cv_lactating')] = 1
					else:
						mom_df.iat[r,mom_df.columns.get_loc('cv_lactating')] = 0

			#drop the interview columns from the mom_df before merging back onto the main df
			int_vec = ['int_year','int_month','int_day']
			for col in int_vec:
				if col in mom_df:
					del mom_df[col]

			good_cols = ['strata_id','psu','atomic_hh_id','hh_member_index','youngest_child_index']
			df = pandas.merge(df,mom_df,on = good_cols,how = "left") #merge the mom_df onto df on the columns listed above
			df.loc[df.cv_lactating.isna(),'cv_lactating'] = 999 #set all other rows that didn't go through the mom_df for loop to 999

		else: #if unable to determine cv_lactating
			df['cv_lactating'] = 999 #default all cv_lactating flags to 999
			print("\tNot determining lactating status",flush=True)


		return df

	#################################################
	# SMOKING STATUS ASSIGNMENT SECTION

	"""
	function that gets all smoking yes/no status in survey
	"""
	def _get_smoking_status(self,df):
		print("\nGETTING SMOKING STATUS",flush=True)
		if self.config['smoking_status'] is not None and 'smoking_status' in df:
			df['cv_smoke'] = 0 #default smoking status status to 0
			smoke_tuple = self.config['smoking_status_true'] #get the 'true' meta-values from the survey
			for tup in smoke_tuple:
				selector = (df['smoking_status']==int(float(tup))) #find all smoking 'true' values
				df.loc[selector,'cv_smoke'] = 1 #and update our new column to true
		return df

	"""
	function that gets smoking frequency
	"""
	def _get_smoking_freq(self,df):
		print("\nGETTING SMOKING FREQUENCY",flush=True)
		mf_vec = ['female','male']
		for x in mf_vec:
			mf_smoking_number = x+"_smoking_number"
			if mf_smoking_number in df:
				if 'smoking_number' not in df:
					df['smoking_number'] = float('nan')
				mf_missing_number = x+"_smoking_number_missing"
				vec = self.config[mf_missing_number] #get the smoking values that were denoted as being no-nos
				for i in range(len(vec)): #for each "bad value"
					selector = None #make a place holder for the parameters
					if ">" in vec[i]:
						new_val = int(float(vec[i].split(">")[1])) #get the number after the value
						selector = (df[mf_smoking_number]<=new_val) #reverse the logic to keep the values that are within the reasonable bounds
					elif "<" in vec[i]:
						new_val = int(float(vec[i].split("<")[1])) #get the number after the value
						selector = (df[mf_smoking_number]>=new_val) #reverse the logic to keep the values that are within the reasonable bounds
					else:
						new_val = int(float(vec[i])) #convert the string to a number
						selector = (df[mf_smoking_number]!=new_val) #reverse the logic to keep the values that are within the reasonable bounds
					df.loc[selector,'smoking_number'] = df.loc[selector,mf_smoking_number]
				del df[mf_smoking_number]

		if self.config['smoking_time_interval'] is not None and 'smoking_time_interval' in df:
			smoking_rate = self.config['smoking_time_interval']
			df['smoking_number'] = df['smoking_number']/int(float(smoking_rate))
		else:
			print("\tWARNING: Smoking rate is not defined. Please input into codebook and rerun.",flush=True)

		return df

	#################################################
	# HEMOG KEEP/OMIT SECTION based on cv_* flags

	"""
	Locate the measured cols where hgb_measured==0 or 1 (aka contains hemog data) to help with parsing through all hemoglobin data
	This function will drop any rows that don't contain hemoglobin data pre-hemoglobin sifting
	If there are no yes/no flags present, then it will essentially skip this step and not drop any rows.
	"""
	def _keep_hemog_obs(self,df):
		print("\nKEEP HEMOG OBS",flush=True) #update user it is in keep_hemog_obs function
		print("\tNumber of rows in DF pre pruning: "+str(len(df.index)),flush=True)
		if self.config['hgb_measured'] is not None:
			df['measurement'] = float('nan') #initialize a column that contains only zeros
			cols_2_check = ['hgb_result_female','hgb_result_male','hgb_result_child'] #columns to check
			for col in cols_2_check:
				if col in df:
					print("\t"+col,flush=True)
					df.loc[df[col].notna(),'measurement'] = 0 #if column exists, set all values where the column values exist to 0 in the 'measurement' column
					selector = (df[col] == int(float(self.config['hgb_measured']))) #check to see if there are any columns that have the measured flag
					df.loc[selector,'measurement'] = 1 #set those columns equal to 1 if they do exist

			df = df[df.measurement==1].copy() #only keep the rows that contain measurements, use .copy() to get rid of warning message and allow all contents of DF to be retained

		else:
			print('\tNo hemog indicator flag - no observations dropped.',flush=True)

		print("\tNumber of rows in DF post pruning: "+str(len(df.index)),flush=True)
		return df

	#################################################
	# HEMOGLOBIN SIFTING SECTION

	"""
	function that compiles all of the hemog data
	"""
	def _compile_hemog_data(self,df,codebook_col_name,new_col_name,cols_2_check,temp_col_name):
		print("\tChecking to see if hemog values are present for: "+codebook_col_name,flush=True)
		if int(float(self.config[codebook_col_name])) == 1: #if there are hemog values present based on our codebooking yes/no flags
			print("\t\tGenerating new column: "+new_col_name,flush=True)
			df[new_col_name] = float('nan') #make a placeholder for new hemog vals
			for col in cols_2_check:
				print("\t\tChecking codebook column: "+col,flush=True)
				if self.config[col] is not None and col in df: #if the column was codebooked and is in the df
					num_blanks = df[col].isna().sum() #count the number of empty values in the given column
					num_rows = len(df.index) #total number of rows in the df
					if num_rows==num_blanks: #if the column exists but doesn't contain any values, print a warning so user can check to ensure this is the case
						error_msg = "\t\t\tNo hemog data in column: "+col+". Check codebook and survey to confirm! Dropping column."
						print(error_msg,flush=True)
					else:
						df.loc[df[col].notna(),new_col_name] = df.loc[df[col].notna(),col] #transfer all data from the column being checked into the new hemog column

					if(self.config['hgb_unit'].lower()=="g/dl"):
						df[new_col_name] = df[new_col_name]*10 #convert all hemoglobin data to be in g/L

					del df[col] #after parsing through the original hemoglobin column, drop it from the survey so it doesn't end up in the final extracted survey

		else:
			print("\t\tNo hemog data for: "+codebook_col_name+". Check codebook and survey to confirm! Dropping all columns associated with: "+codebook_col_name,flush=True)
			for col in cols_2_check:
				if col in df:
					del df[col] #get rid of all columns that were orginially in the codebook

		return df

	"""
	function that sifts through all of the hemog-reading cols
	"""
	def _sift_hemog(self,df):
		print("\nSIFTING THROUGH HEMOG VALUES",flush=True)

		if self.config['hgb_unit'] is not None:
			hemog_flag_vec = ['cv_hgb_raw','cv_hgb_altitude','cv_hgb_altitude_smoking'] #all of the codebook columns that have T/F flag if data is present for these categories (i.e. all the meta_nums)
			new_col_vec = ['hemoglobin_raw','hemoglobin_alt_adj','hemog_alt_smoke_adjust'] #new columns name that will be present in codebook post extraction
			codebook_cols = pandas.DataFrame({'raw':['hgb_female_raw','hgb_male_raw','hgb_child_raw'], #all of the codebook columns that have data for hemog readings
				'alt':['hgb_female_altitude','hgb_male_altitude','hgb_child_altitude'],
				'smoke_alt':['hgb_female_altitude_smoking','hgb_male_altitude_smoking', numpy.nan]})
			codebook_col_names = codebook_cols.columns #get the column names for the each category of hemog data
			for i in range(len(hemog_flag_vec)): #iterate through each category to extract hemog data
				dis_col = codebook_col_names[i]
				selector = (~codebook_cols[dis_col].isnull())
				curr_cols_vec = codebook_cols.loc[selector,dis_col].values
				df = self._compile_hemog_data(df,hemog_flag_vec[i],new_col_vec[i],curr_cols_vec,codebook_col_names[i])
		else:
			sys.exit("\nHemog unit not defined. Terminating run_extract process. Update codebook and rerun!",flush=True)

		#now sift through the newly extracted data to check and see if any data that made it into the df should be removed based on the parameters below
		print("\tNumber of rows in DF pre pruning: "+str(len(df.index)),flush=True)
		selector = None
		for col in new_col_vec:
			if col in df:
				if selector is None:
					selector = ((df[col].notna()) & (df[col]>=25) & (df[col]<=210)) #any hemoglobin readings between 25-210 g/L are considered "reasonable" and will be kept. all other hemog readings will be dropped
				else:
					selector = selector | ((df[col].notna()) & (df[col]>=25) & (df[col]<=210))

		df = df[selector].copy() #only keep values that meet the params above
		print("\tNumber of rows in DF post pruning: "+str(len(df.index)),flush=True)

		return df

	def _update_survey_adj_hemog(self,df):
		print("\nMOVING SMOKING ADJ HB VALUES TO ADJ HEMOG VALUES",flush=True)

		if 'hemoglobin_alt_adj' in df and 'hemog_alt_smoke_adjust' in df:
			selector = (df['hemog_alt_smoke_adjust'].notna()) & ~(df['hemoglobin_alt_adj'].notna())
			df.loc[selector, 'hemoglobin_alt_adj'] = df.loc[selector, 'hemog_alt_smoke_adjust']

		return df


	#################################################
	# BRINDA & WHO ALTITDUE ADJUSTED HEMOGLOBIN ASSIGNMENT SECTION

	"""
	Function that assigns brinda and WHO elevation adjusted hemoglobin levels
	This process is an adaptation from this paper: https://nyaspubs.onlinelibrary.wiley.com/doi/full/10.1111/nyas.14167

	Therefore, they determined a new hemoglobin adjusted equation (where elevation is in meters above sea level) for:
		- BRINDA -> hemoglobin_adjustment = (0.0056384 × elevation) + (0.0000003 × elevation^2)
		- WHO -> hemoglobin_adjustment = (0.00000257 × altitude) + (0.00105 × altitude^2)
	where it produces a value to subtract off of the raw hemoglobin value (in g/L).
	"""
	def _calculate_brinda_who(self,df):
		print("\nASSIGNING BRINDA ELEVATION ADJUSTED HEMOGLOBIN LEVLES",flush=True)

		if 'cluster_altitude' in self.config and 'cluster_altitude' in df and self.config['cluster_altitude'] is not None and 'cv_hgb_raw' in df and int(float(self.config['cv_hgb_raw']))==1 and (df.cluster_altitude.notna()).all() and (df.cluster_altitude < 5500).all():
			print("\tCalculating BRINDA elevation adjusted hemoglobin values",flush=True) #print a message that signifies that this survey will be run against BRINDA equations (given it meets the criteria below)
			df['brinda_adj_hemog'] = float('nan') #create a column where adjusted BRINDA hemog values will go
			df['who_adj_hemog'] = float('nan')

			brinda_b_val = 0.0056384 #the b-coefficient in the BRINDA alt adj quadratic equation
			brinda_a_val = 0.0000003 #the a-coefficient in the BRINDA alt adj quadratic equation

			who_b_val = 0.00105 #the b-coefficient in the WHO alt adj quadratic equation
			who_a_val = 0.00000257 #the a-coefficient in the WHO alt adj quadratic equation

			selector = (df.hemoglobin_raw.notna()) & (df.cluster_altitude.notna())
			df.loc[selector,'brinda_adj_hemog'] = df.loc[selector,'hemoglobin_raw'] -  (brinda_b_val*df.loc[selector,'cluster_altitude'] + brinda_a_val*df.loc[selector,'cluster_altitude']**2)
			df.loc[selector,'who_adj_hemog'] = df.loc[selector,'hemoglobin_raw'] -  (who_a_val*df.loc[selector,'cluster_altitude']**2 - who_b_val*df.loc[selector,'cluster_altitude'])
		else:
			print("\tNOT caclulating BRINDA elevation adjusted hemoglobin values",flush=True)

		return df

	#################################################
	# ANEMIA ASSIGNMENT SECTION

	"""
	function assigns anemia severity statuses to raw and adjusted hemoglobin levels
	"""
	def _assign_anemia_status(self,df):
		print("\nGETTING ANEMIA STATUS",flush=True)

		#load in anemia severity levels flat file
		anemia_levels = pandas.read_csv("FILEPATH/new_who_thresholds_w_age_group_ids.csv")
		#make a dataframe that references the different lower/upper bounds of anemia severity in the anemia_levels dataframe
		level_col_df = pandas.DataFrame({'lower':["hgb_lower_mild","hgb_lower_moderate","hgb_lower_severe","hgb_lower_anemic"],
			'upper':["hgb_upper_mild","hgb_upper_moderate","hgb_upper_severe","hgb_upper_anemic"]})

		new_col_vec = ['hemoglobin_raw','hemoglobin_alt_adj','hemog_alt_smoke_adjust','brinda_adj_hemog','who_adj_hemog'] #new columns names that will be potentially be present in codebook post extraction (that were assigned in _compile_hemog_data())

		#make new severity cols
		sev_cols = ['anemia_mild','anemia_moderate','anemia_severe','anemia_anemic']
		col_categories = ["_raw","_adj","_adj","_brinda","_who"] #note the extra '_adj' is to link up with the new_col_vec indicies

		#add the new severity columns to the dataframe as blank columns
		for col in sev_cols:
			for cat in col_categories:
				col_name = col+cat
				if col_name not in df:
					df[col_name] = float('nan')

		#now go through each row of the anemia level dataframe and determine anemia statuses
		for r in range(len(anemia_levels.index)):
			#get the lower/upper bounds for age, the sex_id, and the cv_pregnant flag
			age_lower = float(anemia_levels.iloc[r]['age_group_years_start'])
			age_upper = float(anemia_levels.iloc[r]['age_group_years_end'])
			dis_sex = int(anemia_levels.iloc[r]['sex_id'])
			dis_preg = int(anemia_levels.iloc[r]['pregnant'])

			#now sift through the different anemia severities
			for c in range(len(level_col_df.index)):
				#get the lower/upper bounds for hemoglobin levels for a given severity
				col_name_lower = level_col_df.iloc[[c]]['lower'].values[0]
				col_name_upper = level_col_df.iloc[[c]]['upper'].values[0]
				lower_hgb = int(anemia_levels.iloc[[r]][col_name_lower].values[0])
				upper_hgb = int(anemia_levels.iloc[[r]][col_name_upper].values[0])

				#now assign anemia status based of the hemoglobin readings in 'hemoglobin_raw','hemoglobin_alt_adj','hemog_alt_smoke_adjust','brinda_adj_hemog', if present
				for x in range(len(new_col_vec)):
					if new_col_vec[x] in df: #if the hemoglobin reading column is in the df
						sev_col_name = sev_cols[c]+col_categories[x]
						selector = (df.age_year >= age_lower) & (df.age_year < age_upper) & (df.sex_id == dis_sex) & (df.cv_pregnant == dis_preg) & (df[new_col_vec[x]].notna()) #default all readings to 0 for the given demographic (if hemoglobin readings are present)
						df.loc[selector,sev_col_name] = 0
						selector = (df.age_year >= age_lower) & (df.age_year < age_upper) & (df.sex_id == dis_sex) & (df.cv_pregnant == dis_preg) & (df[new_col_vec[x]] >= lower_hgb) & (df[new_col_vec[x]] < upper_hgb) #set severity status if readings within severity thresholds
						df.loc[selector,sev_col_name] = 1

		return df
