from winnower.custom.base import TransformBase
import pandas 
import numpy
import pdb


class Transform(TransformBase):
	def output_columns(self, input_columns):
		result = list(input_columns)
		result.append("injury_fall")
		result.append("injury_homicide_knife")
		result.append("injury_homicide_gun")
		result.append("injury_fires")
		result.append("injury_drowning")
		result.append("injury_poisoning")
		result.append("injury_animal")
		result.append("injury_trans_road")
		return result
	
	def execute(self, df):
		df['injury_fall'] = float('NaN')
		df['injury_homicide_knife'] = float('NaN')
		df['injury_homicide_gun']= float('NaN')
		df['injury_fires']= float('NaN')
		df['injury_drowning'] = float('NaN')
		df['injury_poisoning']= float('NaN')
		df['injury_animal']= float('NaN')

		covar = {'injury_fall': 'inj_fall', 'injury_homicide_knife':'inj_stab', 'injury_homicide_gun':'inj_gun', 
				'injury_fires':'inj_fire', 'injury_poisoning':'inj_poison', 'injury_drowning':'inj_drown', 'injury_animal':'inj_animal'}

		for c,t in covar.items():
			mask = df['injury'].notna()
			df.loc[mask, c] = 0
			"""
			self.config['inj_fall'] (and other same type of meta data) comes as a tuple of single str. Convert it to float.
			"""
			if 'homicide' in c:
				mask2 = (df['injury_type'] == float(self.config[t][0])) & (df['inj_accident']== 0) & (df['inj_medcare'] == 1)
				df.loc[mask2, c] = 1
			else:
				mask2 = (df['injury_type'] == float(self.config[t][0])) & (df['inj_medcare'] == 1)
				df.loc[mask2, c] = 1
		
		if 'rti' in df.columns:
			df['injury_trans_road']= float('NaN')
			df.loc[df['rti'].notna(), 'injury_trans_road'] = 0
			maskr = (df['rti'] == 1 ) & (df['rti_medcare'] == 1)
			df.loc[maskr, 'injury_trans_road'] = 1
		
		return df