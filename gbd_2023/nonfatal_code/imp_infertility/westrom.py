######################################################################################
## PURPOSE:
## Calculates prevalence of infertility due to PID from STIs.
## Applies a proportion from Westrom et al. (1992, Sex Transm Dis) to incidence for 
## each of the 3 STI etiologies derived in the epi_splits_pid.py step.
######################################################################################

import os
import argparse
import subprocess
import pandas as pd
import numpy as np
from db_tools import ezfuncs
from db_queries import get_best_model_versions
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from job_utils import draws, getset
#from elmo import run
from elmo import get_bundle_data, upload_bundle_data, save_bundle_version, get_bundle_version
import getpass
from datetime import datetime
import re


##############################################################################
# globals
##############################################################################

# NID 157067 - Pelvic Inflammatory Disease and Fertility. A Cohort Study of
# 1,844 Women with Laparoscopically Verified Disease and 657 Control Women
# with Normal Laparoscopic Results
draw_seed = 99502
westrom_mean = .114
westrom_lower = .131
westrom_upper = .096

export_cols = [
    "seq", "seq_parent", "data_sheet_file_path", "input_type",
    "underlying_nid", "nid",
    "underlying_field_citation_value", "field_citation_value", "page_num",
    "table_num", "source_type", "location_name", "location_id", "ihme_loc_id",
    "smaller_site_unit", "site_memo", "sex", "sex_issue", "year_start",
    "year_end", "year_issue", "age_start", "age_end", "age_issue",
    "age_demographer", "measure", "mean", "lower", "upper", "standard_error",
    "effective_sample_size", "cases", "sample_size", "design_effect",
    "unit_type", "unit_value_as_published", "measure_issue",
    "measure_adjustment", "uncertainty_type", "uncertainty_type_value",
    "representative_name", "urbanicity_type", "recall_type",
    "recall_type_value", "sampling_type", "response_rate", "case_name",
    "case_definition", "case_diagnostics", "note_modeler", "note_SR",
    "extractor", "is_outlier", "2013_data_id", "specificity", "group",
    "group_review", "bundle_id", "bundle_name"
]

me_to_bundle = {3022: 412, 3023: 413, 3024: 414}
me_to_name = {
    3022: "Infertility due to chlamydia", 
    3023: "Infertility due to gonorrhea",
    3024: "Infertility due to other sexually transmitted diseases"
    }

##############################################################################
# math
##############################################################################

def draw_beta(mean, lower, upper, seed):
    np.random.seed(seed)
    sd = (upper - lower) / (2 * 1.96)
    sample_size = mean * (1 - mean) / sd**2
    alpha = mean * sample_size
    beta = (1 - mean) * sample_size
    draws = np.random.beta(alpha, beta, size=(1000, 1))
    draws = pd.DataFrame(
        draws.T,
        index=[0],
        columns=['draw_%s' % i for i in range(1000)])
    return draws

##############################################################################
# helper for dates
##############################################################################
def get_date_stamp():
    # get datestamp for output file
    date_regex = re.compile('\W')
    date_unformatted = str(datetime.now())[0:10]
    date = date_regex.sub('_', date_unformatted)
    return date

##############################################################################
# apply westrom proportion
##############################################################################

class Westrom(draws.SquareImport):

    def __init__(self, source_me_id, decomp_step, gbd_round_id, **kwargs):
        # super init
        super(Westrom, self).__init__(**kwargs)

        # create beta dist
        self.beta = self.get_beta()

        # get draws that we need
        self.input_df = self.import_square(
            gopher_what={"modelable_entity_id": source_me_id},
            source="epi",
            decomp_step=decomp_step)

        # save decomp_step for later use
        self.decomp_step = decomp_step
        # save gbd round for later use
        self.gbd_round_id = gbd_round_id
        # save source me ID for later
        self.source_me_id = source_me_id

    def get_beta(self):
        # get distribution
        beta = draw_beta(westrom_mean, westrom_lower, westrom_upper,
                         draw_seed)
        beta["joinkey"] = 1
        beta = beta.set_index(["joinkey"])

        # get desired index shape
        idx_df = self.get_index_df().reset_index()
        idx_df["joinkey"] = 1
        idx_df = idx_df.set_index(["joinkey"])

        # merge together
        beta = pd.concat([idx_df, beta], axis=1).reset_index()
        beta = beta.set_index(list(self.idx_dmnsns.keys()))
        beta.drop("joinkey", axis=1, inplace=True)
        return beta

    def apply_westrom(self):
        return self.beta * self.input_df

    def summarize(self, df):
        df = self.apply_westrom()
        summaries = df.reindex(
            index=df.index,
            columns=['mean', 'lower', 'upper'])
        summaries['mean'] = df[self.draw_cols].mean(axis=1)
        summaries[['lower', 'upper']] = np.percentile(
            df[self.draw_cols],
            [2.5, 97.5],
            axis=1
        ).transpose()
        return summaries.reset_index()

    def export_for_upload(self, df, modelable_entity_id, nid):

        # mapped
        # modelable_entity_name
        q = """
        SELECT
            modelable_entity_name
        FROM
            epi.modelable_entity
        WHERE
            modelable_entity_id = {modelable_entity_id}
        """.format(modelable_entity_id=modelable_entity_id)
        me_name = ezfuncs.query(
            q, conn_def="epi")["modelable_entity_name"].item()
        df["modelable_entity_name"] = me_name

        # location_name
        # TODO: update to use get_location_metadata in place of getset
        loc_df = getset.get_current_location_set()
        loc_df = loc_df.loc[
            loc_df["most_detailed"] == 1,
            ["location_id", "location_name"]
        ]
        df = df.merge(loc_df, on="location_id", how="left")

        # sex
        q = "SELECT sex_id, sex FROM shared.sex"
        sex_df = ezfuncs.query(q, conn_def="shared")
        df = df.merge(sex_df, on="sex_id", how="left")

        # age
        # TODO: update to get_age_metadata and talk to researcher about
        # switching to demographer notation for age_end.
        # age_end has never been in demographer notation here.
        age_df = getset.get_age_group_set(12)
        age_df = age_df.rename(columns={"age_group_years_start": "age_start",
                                        "age_group_years_end": "age_end"})
        df = df.merge(age_df, on="age_group_id", how="left")

        # export
        q = """
        SELECT
            bundle_id, acause, rei
        FROM
            bundle.bundle
        LEFT JOIN
            shared.cause using (cause_id)
        LEFT JOIN
            shared.rei using (rei_id)
        WHERE
            bundle_id = {}
        """.format(me_to_bundle[modelable_entity_id])
        bundle_df = ezfuncs.query(q, conn_def="epi")
        assert not bundle_df.empty, 'invalid bundle_id {}'.format(
            me_to_bundle[modelable_entity_id])

        if bundle_df.rei.notnull().item():
            rei_acause = bundle_df.rei.item()
        else:
            rei_acause = bundle_df.acause.item()


        outdir = os.path.join('FILEPATH', rei_acause,
                             str(me_to_bundle[modelable_entity_id]),
                             'FILEPATH')
        fname = os.path.join(outdir, 'FILEPATH'.format(
            get_date_stamp()))

        # export it
        df = df[export_cols]
        not_too_young = (df['age_start'] > 5)
        not_too_old = (df['age_start'] < 60)
        df = df[not_too_young & not_too_old]
        df.to_excel(fname, index=False, encoding="utf8",
            sheet_name="extraction")
        df.to_csv('FILEPATH', index=False)
        print('Wrote csv')
        self.delete(modelable_entity_id, outdir, fname)
        return fname, outdir

    def delete(self, modelable_entity_id, outdir, fname):
        ''' Deletes all data in bundle.
            Make sure that export=True so that a copy of the data is saved 
            before deletion. '''
        
        delete = get_bundle_data(me_to_bundle[modelable_entity_id],
            decomp_step=self.decomp_step, gbd_round_id=self.gbd_round_id,export=True)
        delete = delete[['seq']]
        
        if delete.shape[0]:
                fname = os.path.join(outdir,'FILEPATH'.format(get_date_stamp()))
                delete.to_excel(fname, index=False, encoding="utf8",sheet_name="extraction")
                #self.upload(modelable_entity_id, fname, self.source_me_id)
        #else:
                #self.upload(modelable_entity_id, fname, self.source_me_id)


    def upload(self, modelable_entity_id, fname, source_me_id):
        status_df = upload_bundle_data(me_to_bundle[modelable_entity_id], self.decomp_step,
             filepath=fname, gbd_round_id=7)
#            self.decomp_step, self.gbd_round_id, destination_file)
        assert (status_df['request_status'].item()=='Successful')

        bundle_result = save_bundle_version(me_to_bundle[modelable_entity_id],
                                                decomp_step=self.decomp_step,
                                                gbd_round_id=self.gbd_round_id)
        assert (bundle_result.request_status[0]=='Successful')

        bundle_version_id = int(bundle_result.bundle_version_id[0])
        model_data = get_best_model_versions('modelable_entity',
                                             ids=source_me_id,
                                             gbd_round_id=self.gbd_round_id,
                                             decomp_step=self.decomp_step)
        mvid = model_data.model_version_id[0]
        xwalk_description = f'pulled from MVID {str(mvid)} and MEID {str(source_me_id)}'

        # add crosswalk_parent_seq column and save crosswalk file
        df = get_bundle_version(bundle_version_id, fetch='all')
        df['crosswalk_parent_seq'] = ''
        xwalk_fname = os.path.join(f'FILEPATH')
        df.to_excel(xwalk_fname, index=False, encoding='utf8',
                    sheet_name='extraction')

        xwalk_result = run.save_crosswalk_version(bundle_version_id=bundle_version_id,
                                                  data_filepath=xwalk_fname,
                                                  description=xwalk_description)
        assert (xwalk_result.request_status[0] == 'Successful')
        return xwalk_result

def calculate_westrom(source_me_id, target_me_id, decomp_step, gbd_round_id):

    # initialize westrom class
    dim = Westrom.default_idx_dmnsns
    dim["measure_id"] = [6]
    dim["sex_id"] = [2]
    west = Westrom(source_me_id=source_me_id, decomp_step=decomp_step,
        idx_dmnsns=dim, gbd_round_id=gbd_round_id)

    # calculate westrom
    west_df = west.apply_westrom()
    west_df = west.summarize(west_df)

    # export and upload
    fname, error_path = west.export_for_upload(west_df, target_me_id, 237616)
#    west.upload(target_me_id, fname, error_path)
#    west.upload(target_me_id, fname, source_me_id)

##############################################################################
# when called as a script
##############################################################################

if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_me_id", required=True,
                        help="me_id, apply Westrom to this incidence",
                        type=int)
    parser.add_argument("--target_me_id", required=True,
                        help="me_id, save Westrom incidence to this",
                        type=int)
    parser.add_argument("--decomp_step", required=True,
                        help="the decompostion step associated with this data",
                        type=str)
    parser.add_argument("--gbd_round_id", required=True,
                        help="gbd round id",
                        type=int)
    args = vars(parser.parse_args())

    # run function
    calculate_westrom(
        source_me_id=args["source_me_id"],
        target_me_id=args["target_me_id"],
        decomp_step=args["decomp_step"],
        gbd_round_id=args["gbd_round_id"])