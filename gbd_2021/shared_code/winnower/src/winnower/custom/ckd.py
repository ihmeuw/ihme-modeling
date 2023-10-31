from winnower.custom.base import TransformBase
from winnower import errors
import pandas as pd
import numpy as np

class Transform(TransformBase):

    def output_columns(self, input_columns):
        return input_columns

    def execute(self, df):
        df = self._unit_converter(df)
        
        # this survey contains string info in their value
        """
        if survey_name == "CHL/NATIONAL_HEALTH_SURVEY_ENS"{
					destring r_malbpo, replace ignore(" ug/mg creatinina") dpcomma}
        """ 
        if self.config['survey_name'] == 'CHL/NATIONAL_HEALTH_SURVEY_ENS':
            df['r_malbpo'] = df.r_malbpo.str.rstrip(" ug/mg creatinina")
            df['r_malbpo'] = df.r_malbpo.str.replace(',', '.')
            # use to_numeric to handle empty string. 'coerce' will convert any value error to NaN
            df['r_malbpo'] = pd.to_numeric(df['r_malbpo'], errors = 'coerce')

        df = self._serum_calculation(df)
        df = self._CKDEPI(df)
        df = self._MDRD(df)
        df = self._CG(df)
        df = self._SWRTZ(df)
        df = self._ACR(df)
        df = self._stage_category(df)
        df = self._stage_category(df, equation = "_mdrd")
        df = self._stage_category(df, equation = "_cg")
        df = self._stage_category(df, albuminuria = True)
        df = self._stage_category(df, equation = "_mdrd", albuminuria = True)
        df = self._stage_category(df, equation = "_cg", albuminuria = True)
        # calculating no_ckd based on gfr and acr
        df['no_ckd'] = ((df.gfr>60)|(np.isnan(df.gfr))) & ((df.acr<30)|(np.isnan(df.acr)))
        df.loc[np.isnan(df.acr), 'no_ckd'] = float('nan')

        return df
    
    def _unit_converter(self, df):
        """
        Convert units to its most common used one, as well as its corresponding value.
        """
        # convert serum_creatinine_unit to mg/dl
        if self.config['serum_creatinine_unit'] is not None and \
                self.config['serum_creatinine_unit'] == 'umol/l':
            df.serum_creatinine = df.serum_creatinine/88.42
            df['serum_creatinine_unit'] = 'mg/dl'
        
        # convert urine_creatinine_unit to mg/dl
        if self.config['urine_creatinine_unit'] is not None:
            if self.config['urine_creatinine_unit'] == 'mmol/l':
                # 88.4 is not a typo
                df.urine_creatinine = df.urine_creatinine/88.4
            if self.config['urine_creatinine_unit'] == 'mg/dl':
                df.urine_creatinine = df.urine_creatinine/1000
            df['urine_creatinine_unit'] = 'g/dl'
        
        # convert urine_albumin_unit to mg/dl
        if self.config['urine_albumin_unit'] is not None:
            if self.config['urine_albumin_unit'] == 'mg/l' or \
                    self.config['urine_albumin_unit'] == 'ug/ml':
                df.urine_albumin = df.urine_albumin*0.1
                df['urine_albumin_unit'] = 'mg/dl'
        
        # convert height_unit to cm
        if self.config['height_unit'] is not None:
            if self.config['height_unit'] == 'm':
                df.height = df.height*100
                df['height_unit'] = 'cm'
    
        return df

    # drop those w/o creatinine and fix implausible values
    def _serum_calculation(self, df):
        """
        	cap confirm variable serum_creatinine_unit
	        if !_rc{
		        drop if serum_creatinine == . 
		        gen creat_cap = serum_creatinine
		        replace creat_cap = 0.1 if serum_creatinine<0.1
		        replace creat_cap = 20 if serum_creatinine>20
        """
        if 'serum_creatinine_unit' in df:
            # add copy() to avoid warning
            df = df[df.serum_creatinine.notna()].copy()
            df['creat_cap'] = df['serum_creatinine']
            df.loc[df.serum_creatinine < 0.1, 'creat_cap'] = 0.1
            df.loc[df.serum_creatinine > 20, 'creat_cap'] = 20

        return df

    def _CKDEPI(self, df):
        """
         calculate GFR using CKD-EPI equation
        """
        df['k'] = 0.9
        df.loc[df.sex_id == 2, 'k'] = 0.7
        
        df['a_const'] = -0.411
        df.loc[df.sex_id == 2, 'a_const'] = -0.329

        df['creatinine_k'] = df.creat_cap/df.k

        if 'gfr' in df:
            del df['gfr']
        df['gfr'] = float('nan')

        df.loc[df.creatinine_k >= 1, 'gfr'] = 141*(df.creatinine_k**(-1.209))*(0.993**df.age_year)
        df.loc[df.creatinine_k < 1, 'gfr'] = 141*(df.creatinine_k**df.a_const)*(0.993**df.age_year)
        df.loc[df.sex_id == 2, 'gfr'] = df.gfr*1.018
        
        if self.config['race'] is not None:
            df['black'] = 0
            # if column race contains 'black'
            # case insensitive, set na to False
            mask = df.race.str.contains('black', case = False, na = False)
            df.loc[mask == True, 'black'] = 1
            df.loc[df.black == 1, 'gfr'] = df.gfr*1.159
        return df

    def _MDRD(self, df):
        """
         MDRD equation
        """
        df['gfr_mdrd'] = float('nan')
        df.gfr_mdrd = 175*(df.creat_cap**(-1.154))*(df.age_year**(-0.203))
        df.loc[df.sex_id == 2, 'gfr_mdrd'] = df.gfr_mdrd*0.742
        if self.config['race'] is not None:
            df.loc[df.black == 1, 'gfr_mdrd'] = df.gfr_mdrd*1.212

        return df

    def _CG(self, df):
        """
         Cockcroft-Gault equation
        """
        df['gfr_cg'] = float('nan')
        if self.config['weight'] is not None and self.config['height'] is not None:
            df.loc[:, 'gfr_cg'] = ((140-df.age_year)/df.creat_cap)*(df.weight/72)
            df.loc[df.sex_id == 2, 'gfr_cg'] = df.gfr_cg*0.85
            df['bsa'] = np.sqrt((df.height*df.weight)/3600)
            df.loc[:, 'gfr_cg'] = df.gfr_cg*(1.73/df.bsa)
        return df

    def _SWRTZ(self, df):
        """
         schwartz formula for gfr
        """
        if self.config['height'] is not None:
            df.loc[df.age_year <= 18, 'gfr'] = 0.413*(df.height/df.creat_cap)
            df.loc[df.age_year <= 18, 'gfr_mdrd'] = 0.413*(df.height/df.creat_cap)
            df.loc[df.age_year <= 18, 'gfr_cg'] = 0.413*(df.height/df.creat_cap)
        return df
    
    def _ACR(self, df):
        """
            To calculate ACR. Notes including how to translate subinstr() and inlist2
        """
        
        df['acr'] = float('nan')
        
        if self.config['acr'] is not None:
            # what's the reason for this step?
            df.loc[:, 'acr'] = df[self.config['acr']]

            if self.config['acr_unit'] is not None:
                df.loc[df.acr_unit == "mg/mmol", 'acr'] = df.acr/0.113
                df.loc[df.acr_unit == "mg/mmol", 'acr_unit'] = "mg/g"

            # Adding the condition of len()!=0 because now self.config['acr_missing'] is 
            # a tuple after you declared it in topic_form.py
            if self.config['acr_missing'] is not None or len(self.config['acr_missing']) != 0:
                """ 
                local missing clean = subinstr("$acr", ",", " ", .) 
                ^ This is to replace the comma with space. The 4th arguments 
                with . indicates that you want to remove them all.
                Note (12/10): No longer need to replace the comma with space because acr_missing is now a tuple


                replicate inlist2, obj(vars) vals(value)
                For example, 
                    value = self.config[your tuple of value]
                    mask = round(df[vars]).isin(value)
                The mask will return true or false.
                Note: inlist will automatically round the obj().
                """
                acr_missing_vals = self.config['acr_missing']
                mask = round(df['acr']).isin(acr_missing_vals)
                df.loc[mask, 'acr'] = float('nan')
        elif 'urine_albumin' in df and 'urine_creatinine' in df:
                df['acr'] = df.urine_albumin/df.urine_creatinine

        return df
                
    
    def _stage_category(self, df, equation = "", albuminuria = False):
        """
            determine stage category for ckd method.
            default ckd is using euqation CKD-EPI + Schwartz (<18) and alnuminuria = False.
            If you set albuminuria = true, you will use different masks.
        """
        dependent = "gfr"
        # if equation is mdrd or cg, add the dependent name with it
        # e.g. gfr_mdrd, gfr_cg
        if equation != "":
            dependent = dependent + equation
        if (albuminuria == 0): 
            # CKD
            # create 4 condition for each stage of equation. True/False values
            mask1 = (df[dependent]<15)|(np.isclose(df[dependent], 15))
            mask2 = (df[dependent]>15) & ((df[dependent]<30)|(np.isclose(df[dependent], 30)))
            mask3 = (df[dependent]>30) & ((df[dependent]<60)|(np.isclose(df[dependent], 60)))
            mask4 = (df[dependent]<60)|(np.isclose(df[dependent], 60))
            # the stage dictionary. Each stage is match with one specific condition
            stage_dic = {'5': mask1, '4':mask2, '3':mask3, '3_5':mask4}
            varname = 'ckd_stage'
        else:
            # CKD + Albuminuria
            mask1 = ((df[dependent]>60)|(np.isnan(df[dependent]))) & ((df.acr>30)|(np.isclose(df.acr, 30))) & (np.isnan(df.acr) == 0)
            mask2 = ((df[dependent]>60)|(np.isnan(df[dependent]))) & ((df.acr>25)|(np.isclose(df.acr, 25))) & (np.isnan(df.acr) == 0)
            mask3 = ((df[dependent]>60)|(np.isnan(df[dependent]))) & ((df.acr>20)|(np.isclose(df.acr, 20))) & (np.isnan(df.acr) == 0)
            mask4 = ((df[dependent]>60)|(np.isnan(df[dependent]))) & ((df.acr>17)|(np.isclose(df.acr, 17))) & (np.isnan(df.acr) == 0)
            stage_dic = {"":mask1, "_25": mask2, "_20": mask3, "_17": mask4}
            varname = 'albuminuria'

        for s, m in stage_dic.items():
            # assign var name with the stage and its equation
            # e.g. ckd_stage5_mdrd
            var = varname + s + equation
            # assign mask value to each stage
            # e.g. df['ckd_stage5_mdrd] = mask1
            df[var] = m
            
            if equation == '_cg' and albuminuria == 0:
                df.loc[np.isnan(df[dependent]), var] = float('nan')

            if albuminuria == 1:
                df.loc[np.isnan(df.acr), var] = float('nan')
        
        return df

    
