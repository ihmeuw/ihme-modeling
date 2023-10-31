from winnower.custom.base import TransformBase
import pandas 
import numpy 


class Transform(TransformBase):
	def output_columns(self, input_columns):
		result = list(input_columns)
		result.append("car")
		result.append("cyc")
		result.append("moto")
		result.append("ped")
		
		result.append("rti_recv_care")
		result.append("car_recv_care")
		result.append("cyc_recv_care")
		result.append("moto_recv_care")
		result.append("ped_recv_care")
		
		result.append("rti_no_care")
		result.append("car_no_care")
		result.append("cyc_no_care")
		result.append("moto_no_care")
		result.append("ped_no_care")
		
		result.append("rti_recv_care_in")
		result.append("car_recv_care_in")
		result.append("cyc_recv_care_in")
		result.append("moto_recv_care_in")
		result.append("ped_recv_care_in")
		
		result.append("rti_recv_care_out")
		result.append("car_recv_care_out")
		result.append("cyc_recv_care_out")
		result.append("moto_recv_care_out")
		result.append("ped_recv_care_out")
		
		return result
		
	def execute(self, df):
		df['car'] = float('NaN')
		df['cyc'] = float('NaN')
		df['moto'] = float('NaN')
		df['ped'] = float('NaN')
		df['rti_recv_care'] = float('NaN')
		df['car_recv_care'] = float('NaN')
		df['cyc_recv_care'] = float('NaN')
		df['moto_recv_care'] = float('NaN')
		df['ped_recv_care'] = float('NaN')
		df['rti_no_care'] = float('NaN')
		df['car_no_care'] = float('NaN')
		df['cyc_no_care'] = float('NaN')
		df['moto_no_care'] = float('NaN')
		df['ped_no_care'] = float('NaN')
		df['rti_recv_care_in'] = float('NaN')
		df['car_recv_care_in'] = float('NaN')
		df['cyc_recv_care_in'] = float('NaN')
		df['moto_recv_care_in'] = float('NaN')
		df['ped_recv_care_in'] = float('NaN')
		df['rti_recv_care_out'] = float('NaN')
		df['car_recv_care_out'] = float('NaN')
		df['cyc_recv_care_out'] = float('NaN')
		df['moto_recv_care_out'] = float('NaN')
		df['ped_recv_care_out'] = float('NaN')
		
		## in the Eurostat surveys, "no" to received care means it should not count as 
		## and injury.  a blank answer means "no care"
		if self.config['nid'] in {139970, 139996, 139997}:
			df.loc[df['rti'].notna(), 'rti_recv_care'] = 0
			df.loc[df['recv_care']==1, 'rti_recv_care'] = 1
			
			df.loc[df['rti'].notna(), 'rti_no_care'] == 0
			mask1 = (df['rti'] == 1) & (df['recv_care'].isna())
			df.loc[mask1, 'rti_no_care'] = 1
			
			df.loc[df['recv_care'] == 0, 'rti'] = 0
		
		## first the child causes are generated.  then all causes with recv_care only
		## are generated.  then all causes with recv_care and inpatient/outpatient 
		## indicators are generated.

		if 'is_car' in df:
			tempdic = {'car':'is_car', 'cyc':'is_cyc', 'moto':'is_moto', 'ped':'is_ped'}
			if self.config['nid'] == 3100:
				df['car'] = 0
				df['cyc'] = 0
				df['moto'] = 0
				df['ped'] = 0
			else:
				for k, v in tempdic.items():
					mask2 = (df['rti'].notna())|(df[v].notna())
					df.loc[mask2, k] = 0
			
			for k, v in tempdic.items():
				mask3 = (df['rti'] == 1) & (df[v] == 1)
				df.loc[mask3, k] = 1

			## cv_recv_care = 1 and cv_no_care = 1 for child causes
			recvdic={'car_recv_care':'car', 'cyc_recv_care':'cyc', 'moto_recv_care':'moto', 'ped_recv_care': 'ped'}
			for k,v in recvdic.items():
				df.loc[df[v].notna(), k] = 0
				mask4 = (df[v]==1) & (df['recv_care']==1)
				df.loc[mask4, k] = 1

			nodic={'car_no_care':'car', 'cyc_no_care':'cyc', 'moto_no_care':'moto', 'ped_no_care':'ped'}
			for k,v in nodic.items():
				df.loc[df[v].notna(), k] = 0
				mask5 = (df[v]==1) & (df['recv_care']==0)
				df.loc[mask5, k] = 1


		## logic when out/in indicator is missing, just cv_recv_care = 1
		df.loc[df['rti'].notna(), 'rti_recv_care'] =0
		mask6 = (df['rti'] == 1) & (df['recv_care'] == 1)
		df.loc[mask6, 'rti_recv_care'] = 1 
		
		df.loc[df['rti'].notna(), 'rti_no_care'] = 0
		mask7 = (df['rti'] == 1) & (df['recv_care'] == 0)
		df.loc[mask7, 'rti_no_care'] = 1

		## cv_recv_care and outpatient stuff
		if 'outpatient' in df:
			## inpatient (outpatient = 0)
			df.loc[df['rti'].notna(), 'rti_recv_care_in'] = 0
			mask8 = (df['rti']==1) & (df['outpatient']==0)
			df.loc[mask8, 'rti_recv_care_in'] = 1
			## outpatient (outpatient = 1)
			df.loc[df['rti'].notna(), 'rti_recv_care_out'] = 0
			mask9 = (df['rti']==1) & (df['outpatient']==1)
			df.loc[mask9, 'rti_recv_care_out'] = 1
			## child causes
			if 'is_car' in df:
				recv_in_dic={'car_recv_care_in':'car', 'cyc_recv_care_in':'cyc', 'moto_recv_care_in':'moto', 'ped_recv_care_in': 'ped',}
				for k,v in recv_in_dic.items():
					df.loc[df[v].notna(), k] = 0
					mask10 = (df[v]==1) & (df['outpatient']==0)
					df.loc[mask10, k] = 1
				recv_out_dic={'car_recv_care_out':'car', 'cyc_recv_care_out':'cyc', 'moto_recv_care_out':'moto', 'ped_recv_care_out':'ped'}
				for k,v in recv_out_dic.items():
					df.loc[df[v].notna(), k] = 0
					mask11 = (df[v]==1) & (df['outpatient']==1)
					df.loc[mask11, k] = 1
		
		return df