from __future__ import division
import os
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import argparse
from gbd.gbd_round import gbd_round_from_gbd_round_id
from job_utils import draws, parsers
import copy
import re
from datetime import datetime
import xlsxwriter as xl


class Congenital(draws.SquareImport):

    def __init__(self, me_map, cause_name, **kwargs):
        # super init
        super(Congenital, self).__init__(**kwargs)

        # store data by me in this dict key=me_id
        self.me_map = me_map
        self.cause_name = cause_name
        self.me_dict = {}

        # import every input and create a dictionary of dataframes
        """ The .import_square() method of the SquareImport class 
        (created by central_comp/mlsandar) is what will call the 
        draws shared function. It grabs draws for one me_id at a time, 
        shapes the dataframe for downstream manipulation , and stores 
        each dataframe in a dictionary """
        for grp, grp_dict in self.me_map.items():
            for ns in grp_dict.keys(): # name short
                if grp != 'other':
                    me_id = grp_dict[ns]['srcs']['pre_sqzd_me_id']
                    self.me_dict[me_id] = self.import_square(
                        gopher_what={"modelable_entity_id": me_id},
                        source="epi")
        if self.cause_name == "cong_chromo":
            self.zero_out_sexes()

    def zero_out_sexes(self):
        """ Dismod produces results for both sexes regardless of whether
        or not the cause is sex-specific. Turner and Klinefelter
        are sex-specific so we need to zero-out the erroneous sexes """
        turner_key = self.me_map["sub_group"]["Turner Syndrome"]["srcs"][
            "pre_sqzd_me_id"]
        kline_key = self.me_map["sub_group"]["Klinefelter Syndrome"]["srcs"][
            "pre_sqzd_me_id"]

        # zero out males for turner
        turner = self.me_dict[turner_key].copy()
        turner.loc[turner.query('sex_id == 1').index,:] = 0.
        self.me_dict[turner_key] = turner

        # zero out females for klinefelter
        kline = self.me_dict[kline_key].copy()
        kline.loc[kline.query('sex_id == 2').index,:] = 0.
        self.me_dict[kline_key] = kline
    
    def get_date_stamp(self):
        '''returns a datestamp for output files'''
        date_regex = re.compile('\W')
        date_unformatted = str(datetime.now())[0:10]
        date = date_regex.sub('_', date_unformatted)
        return date

    def zero_negatives(self, df):
        """ Turns negative draws to zero and returns the result. 
        Also counts number of negative draws and exports the count for 
        analysis. """
        
        # Count number of negative draws and export
        negative_bool = (df < 0)
        neg_check = df.copy()
        neg_check = neg_check[negative_bool]
        neg_check.loc[:, 'num_negative_draws'] = neg_check.count(axis=1)
        neg_check = neg_check.loc[neg_check.num_negative_draws > 100.,:]
        neg_check.drop(['draw_{}'.format(i) for i in range(1000)], axis=1, 
            inplace=True)
        if not neg_check.empty:
            # Create csv file
            negative_file = ("/ihme/mnch/congenital/sb_negative/"
                "sb_negative_loc{0}_{1}.csv".format(
                    self.idx_dmnsns['location_id'][0],self.get_date_stamp()))
            workbook = xl.Workbook(negative_file)
            worksheet = workbook.add_worksheet()
            # Calculate dimensions of datataframe
            neg_check = neg_check.reset_index()
            num_row, num_col = neg_check.shape
            # Write the data to a sequence of cells.
            worksheet.write_row(0,0,neg_check.columns)
            rowshift, colshift = 1, 1
            for i in range(num_row):
                worksheet.write_row(i+rowshift,0,neg_check.iloc[i])
            desc_text = ("**Only locations with greater than 100 negative draws"
                " are shown. Note that any locations with negative draws are"
                " reduced to the envelope multiplied by the inverse of the"
                " spina bifida floor in the final, saved model.")
            worksheet.merge_range(num_row+rowshift, 0, num_row+rowshift, 
                num_col-colshift, desc_text)
            workbook.close()
        
        # Turn negative draws to zero and return the result
        positive = df.copy()
        positive[positive<0.] = 0.
        assert (positive < 0.0).any().any() == False
        return positive

    def calc_anencephaly(self):

        #######################################################################
        # denominator
        #######################################################################
        # read in denominator dataframe for given year
        location = self.idx_dmnsns['location_id'][0]
        gbd_year = gbd_round_from_gbd_round_id(self.gbd_round_id)
        denom = pd.read_csv((f"/ihme/mnch/neonatal_denom/current/"
            f"neonate_denom_with_mort_rate_gbd{gbd_year}_{location}.csv"))
        denom_idx_cols = ['year_id','sex_id','location_id']
        denom_data_cols = \
            ['births_{}_enn'.format(draw) for draw in range(1000)] + \
            ['births_{}_lnn'.format(draw) for draw in range(1000)] + \
            ['life_years_{}_enn'.format(draw) for draw in range(1000)] + \
            ['life_years_{}_lnn'.format(draw) for draw in range(1000)]
        
        #######################################################################
        # calc numerator
        #######################################################################
        # columns on which calcuations are performed
        draw_cols = ["draw_{i}".format(i=i) for i in range(0, 1000)]

        """
        Anencephaly life table calculations
        NID 296668
        File with calculations here: "/home/j/WORK/12_bundle/cong_neural/610/
        anencephaly life table - NJK 4.19 AS copy.xlsx"
        """
        ly_per_birth_enn = 0.00508204
        ly_per_birth_lnn = 0.001325096
        qx_enn_anenceph = 0.800653595
        
        """ get anencephaly draws (from dismod) and reset index to work with 
        denom dataframe """
        anenceph_dismod_id = self.me_map["special"]["Anencephaly"]["srcs"][
            "pre_sqzd_me_id"]
        anencephaly = self.me_dict[anenceph_dismod_id].copy()
        anencephaly = anencephaly.reset_index()
        
        # Early neonatal
        enn_anenceph = pd.DataFrame()
        enn_anenceph = anencephaly.loc[anencephaly['age_group_id']==164,:]
        enn_anenceph = pd.merge(enn_anenceph, 
            denom[denom_idx_cols+denom_data_cols], on=denom_idx_cols, 
            how='left', indicator=True)
        assert (enn_anenceph._merge=='both').all()
        enn_anenceph.drop('_merge',axis=1, inplace=True)
        
        for col in range(0, 1000):
            enn_anenceph['draw_{i}'.format(i=col)] = (
                enn_anenceph['draw_{i}'.format(i=col)] * \
                enn_anenceph['births_{i}_enn'.format(i=col)] * \
                ly_per_birth_enn) / enn_anenceph['life_years_{i}_enn'.format(
                i=col)]
        enn_anenceph.loc[:,'age_group_id'] = 2

        # Late neonatal
        lnn_anenceph = pd.DataFrame()
        lnn_anenceph = anencephaly.loc[anencephaly['age_group_id']==164,:]
        lnn_anenceph = pd.merge(lnn_anenceph, 
            denom[denom_idx_cols+denom_data_cols], on=denom_idx_cols, 
            how='left', indicator=True)
        assert (lnn_anenceph._merge=='both').all()
        lnn_anenceph.drop('_merge',axis=1, inplace=True)

        for col in range(0, 1000):
            lnn_anenceph['draw_{i}'.format(i=col)] = (
                lnn_anenceph['draw_{i}'.format(i=col)] * \
                lnn_anenceph['births_{i}_lnn'.format(i=col)] * \
                (1-qx_enn_anenceph) * ly_per_birth_lnn) / \
            lnn_anenceph['life_years_{i}_lnn'.format(i=col)]
        lnn_anenceph.loc[:,'age_group_id'] = 3

        """ Combine birth, enn & lnn, reset multiindex, and format for
        save_results. Fill in all age groups other than 164, 2 and 3 as
        zero's for prevalence. Age group 164 should be from dismod (same as 
        original df) """
        new_anencephaly = pd.concat([enn_anenceph,lnn_anenceph],
            ignore_index=True)
        new_anencephaly = new_anencephaly[list(
            self.idx_dmnsns.keys())+draw_cols]
        new_anencephaly.loc[:,'measure_id'] = 5
        dismod_anencephaly_164 = anencephaly.loc[
            anencephaly['age_group_id']==164,:]
        new_anencephaly = pd.concat([new_anencephaly, dismod_anencephaly_164], 
            ignore_index=True)
        new_anencephaly = new_anencephaly.set_index(list(
            self.idx_dmnsns.keys()))
        new_anencephaly = pd.concat([self.index_df, new_anencephaly], axis=1)
        new_anencephaly.fillna(value=0, inplace=True)

        #Add new prevalence calculation to dataframe dictionary
        anenceph_custom_id = self.me_map["special"]["Anencephaly"]["trgs"][
            "custom"]
        self.me_dict[anenceph_custom_id] = new_anencephaly

    def calc_spina_bifida(self):
        """ Subtracts Encephalocele from Total neural tube congenital anomalies 
        to obtain Spina Bifida. Where subtraction would produce negative draws, 
        Encephalocele is reduced to the complement of an empirically 
        calculated spina bifida floor based on EUROCAT data. Saves 
        Encephalocele and Spina Bifida to target meids """
        print("I'm here")
        sb_floor = 0.421745 #[(SB):(SB+enceph)], calculated in SB_ratio.R
        env = self.me_map["envelope"].values()
        env_id = self.me_map["envelope"]["Total Neural Tube Defects"]["srcs"][
            "pre_sqzd_me_id"]
        enc_srcid = self.me_map["sub_group"]["Encephalocele"]["srcs"][
            "pre_sqzd_me_id"]
        sb_trgid = self.me_map["sub_group"]["Spina Bifida"]["trgs"][
            "post_sqzd_me_id"]
        enc_trgid = self.me_map["sub_group"]["Encephalocele"]["trgs"][
            "post_sqzd_me_id"]

        envelope = self.me_dict[env_id]
        adj_env = envelope * (1-sb_floor)
        enceph = self.me_dict[enc_srcid]
        sqz_bool = (enceph > adj_env)
        scaled_enceph = adj_env[sqz_bool]
        scaled_enceph.fillna(enceph[~sqz_bool], inplace=True)
        sb = envelope - scaled_enceph
        sb = self.zero_negatives(sb)
        assert (sb < 0.0).any().any() == False
        self.me_dict[sb_trgid] = sb
        self.me_dict[enc_trgid] = scaled_enceph
        #diagnostic_sb = envelope - enceph
        #diagnostic_sb = self.zero_negatives(diagnostic_sb)

    def get_other_props(self):
        other_bundle_id =  list(
            self.me_map["other"].values())[0]["srcs"]["bundle_id"]
        fname = ("{}_other_props_from_subcauses_{}.xlsx".format(
                self.cause_name,other_bundle_id))
        directory = ("/home/j/WORK/12_bundle/cong/01_input_data/"
            "06_proportion_other")
        other_props = pd.read_excel(os.path.join(directory,fname))
        other_props.drop(['sex', 'age_start', 'age_end', 'cases', 
            'other_cases', 'cause'], axis=1, inplace=True)
        return other_props

    def squeeze(self):
        """ Add post_sqzd_me_ids to dictionary and copy over the unsqueezed 
        data """
        for grp, grp_dict in self.me_map.items():
            if grp == 'sub_group':
                for ns in grp_dict.keys(): # name short
                    src_meid = grp_dict[ns]['srcs']['pre_sqzd_me_id']
                    trg_meid = grp_dict[ns]['trgs']['post_sqzd_me_id']
                    self.me_dict[trg_meid] = self.me_dict[src_meid].copy(
                        deep=True)
        
        # aggregate sub_groups
        sub_causes = pd.DataFrame()
        for grp, grp_dict in self.me_map.items():
            if grp == 'sub_group':
                for ns in grp_dict.keys(): # name short
                    trg_meid = grp_dict[ns]['trgs']['post_sqzd_me_id']
                    sub_causes = pd.concat(
                                    [sub_causes, self.me_dict[trg_meid]])

        # collapse on keys 
        sigma_sub_causes = sub_causes.groupby(
            level=list(self.idx_dmnsns.keys())).sum()
        
        if self.cause_name in ['cong_heart', 'cong_chromo']:
            key_other = list(
                self.me_map["other"].values())[0]["trgs"][
                "post_sqzd_me_id"]
            props = self.get_other_props()
            props.loc[:,'complement_prop_other'] = (1-props['prop_other'])
            odf = sigma_sub_causes.reset_index()
            odf = pd.merge(odf, props[
                [c for c in props.columns if c not in ['prop_other']]], 
                how='left', on=['age_group_id', 'sex_id'],indicator=True)
            assert (odf._merge=='both').all()
            odf[self.draw_cols] = odf[self.draw_cols].divide(
                odf.complement_prop_other, axis=0) - odf[self.draw_cols]
            odf.drop(['complement_prop_other','_merge'], axis=1, inplace=True)
            # set index back to way it was before
            odf = odf.set_index(list(self.idx_dmnsns.keys()))
            odf = odf[self.draw_cols]
            odf = pd.concat([self.index_df, odf], axis=1)
            self.me_dict[key_other] = odf
        
        else:
            # get total env
            env_id = list(
                self.me_map["envelope"].values())[0]["srcs"][
                "pre_sqzd_me_id"]
            cause_env = self.me_dict[env_id].copy(deep=True)
            adj_env = pd.DataFrame()

            if self.cause_name != 'cong_neural':
                # reserve 90% so some is left over for 'other'
                adj_env = cause_env * .90
            else:
                """ these are aliased now, whatever happens to adj_env, also 
                happens to cause_env """
                adj_env = cause_env

            """Squeeze adapted from infertility code
            The fertility code squeezes any case where the total of cause 
            attribution is bigger than 95% of envelop. For some congenital
            causes, we're going to try squeezing the summed subcauses that 
            are bigger than 90% of the envelope. sqz_bool is a df of boolean 
            values that includes only those data points that are bigger than 
            90% of the envelope (those that return True for logical condition). 
            """
            sqz_bool = (sigma_sub_causes > adj_env)
            for grp, grp_dict in self.me_map.items():
                if grp == 'sub_group':
                    for ns in grp_dict.keys(): # name short
                        trg_meid = grp_dict[ns]['trgs']['post_sqzd_me_id']
                        to_scale = self.me_dict[trg_meid]
                        if self.cause_name in ['cong_digestive', 'cong_msk']:
                            scaled = (
                                to_scale[sqz_bool] * adj_env[sqz_bool] /
                                sigma_sub_causes[sqz_bool])
                            scaled.fillna(to_scale[~sqz_bool], inplace=True)

                        elif self.cause_name == 'cong_neural':
                            scaled = (to_scale * adj_env / sigma_sub_causes)
                            scaled.fillna(0, inplace=True)
                    
                        """ reassign scaled value to med_id in dictionary of 
                        dataframes """
                        assert (scaled[
                            self.draw_cols] > 1.0).any().any() == False
                        assert (scaled[
                            self.draw_cols] < 0.0).any().any() == False
                        assert scaled.isnull().values.any() == False
                        self.me_dict[trg_meid] = scaled
                
                elif grp == 'other':
                    # calculate 'Other ...'
                    key_other = list(
                        self.me_map["other"].values())[0]["trgs"][
                        "post_sqzd_me_id"]
                    sqzd = sigma_sub_causes[~sqz_bool].fillna(adj_env[sqz_bool])
                    other = cause_env - sqzd
                    self.me_dict[key_other] = other
                    assert (self.me_dict[key_other] < 0.0).any().any() == False


##############################################################################
# function to run core code
##############################################################################

def congenital_data(me_map, location_id, out_dir, cause_name, 
    gbd_round_id, decomp_step):
    
    """ retrieve default dimensions needed to initialize draws.SquareImport 
    class, then change as needed"""
    dim = Congenital.default_idx_dmnsns(gbd_round_id)
    
    """
    # code test dimensions, if you switch to these
    # you'll also have to filter the denom df accordingly
    # or the anencephaly code won't work
    dim['age_group_id'] = [164, 2, 3, 4, 6, 13]
    dim['location_id'] = [492, 189]
    dim["location_id"] = location_id
    dim["measure_id"] = [5]
    dim["sex_id"] = [1, 2]
    """

    # production dimensions
    # Add age group "Birth" (164)
    # note that there currently are no age_group_set_ids that contain 164
    dim['age_group_id'] = [164] + dim['age_group_id']
    dim["location_id"] = location_id
    dim["measure_id"] = [5]
    dim["sex_id"] = [1, 2]

    # initialize instance of Congenital class
    congenital_cause = Congenital(me_map=me_map, cause_name=cause_name, 
        idx_dmnsns=dim, gbd_round_id=gbd_round_id, decomp_step=decomp_step)
    '''
    # if we don't squeeze any of the cong_neural causes
    if cause_name == "cong_neural": 
        congenital_cause.calc_anencephaly()
        congenital_cause.calc_spina_bifida()
    else:
        congenital_cause.squeeze()
    '''
    if cause_name == "cong_neural":
        congenital_cause.calc_anencephaly()
    congenital_cause.squeeze()

    # save results to disk
    # retrieve target me_ids from map and handle anencephaly case
    mapdf = json_normalize(me_map)
    trgs = mapdf.filter(regex=("^((?!special).)*trgs|trgs.custom"))
    for me_id in trgs.values.tolist()[0]:
        fname = str(location_id[0]) + ".h5"
        out_df = congenital_cause.me_dict[me_id].reset_index()
        out_df.to_hdf(os.path.join(out_dir, str(me_id), fname), key="draws", 
            format="table", data_columns=list(dim.keys()))
##############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("me_map", help="json style dictionary of me_ids", 
        type=parsers.json_parser)
    parser.add_argument("out_dir", help="root directory to save stuff")
    parser.add_argument("location_id", help="which location to use", type=int)
    parser.add_argument("cause_name", help="which cause to graph")
    parser.add_argument("gbd_round_id", type=int)
    parser.add_argument("decomp_step")
    args = vars(parser.parse_args())
    
    # call function
    congenital_data(me_map=args["me_map"], out_dir=args["out_dir"], 
        location_id=[args["location_id"]], cause_name=args["cause_name"],
        gbd_round_id=args["gbd_round_id"], decomp_step=args["decomp_step"])
