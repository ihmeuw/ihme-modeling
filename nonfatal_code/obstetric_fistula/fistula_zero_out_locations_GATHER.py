# Please see readme.txt in this repo, for full explanation of the strategy
# for each nonfatal maternal cause
from __future__ import division
import pandas as pd
from transmogrifier import gopher
from db_tools import ezfuncs as ez
import numpy as np
import sys


class Base(object):
    def __init__(self, cluster_dir, year_id, input_me, output_me):
        '''This class incorporates all the functions that all the specific
        causes use, but all in different sequence'''
        self.cluster_dir = cluster_dir
        self.year_id = year_id
        self.input_me = input_me
        self.output_me = output_me
        self.conn_def = "cod"
        self.gbd_round = 4

    def get_locations(self, location_set_id):
        '''Pulls the location hierarchy upon which this code is to be run'''
        query = ('SELECT location_id, most_detailed FROM shared.'
                 'location_hierarchy_history WHERE location_set_version_id=('
                 'SELECT location_set_version_id FROM {DATABASE} WHERE location_set_id = %s AND gbd_round_id '
                 '= %s) AND most_detailed = 1' % (location_set_id, self.gbd_round))
        loc_df = ez.query(query=query, conn_def=self.conn_def)
        return loc_df

    def get_draws(self, measure_id=6):
        '''Uses gopher.draws to pull draws of the ME for this class instance'''
        draws = gopher.draws(gbd_ids={'modelable_entity_ids': [self.input_me]},
                             source='epi', measure_ids=[measure_id],
                             location_ids=[], year_ids=[self.year_id],
                             age_group_ids=[],
                             sex_ids=[1,2])
        loc_df = self.get_locations(35)
        draws = draws.merge(loc_df, on='location_id', how='inner')
        draws.drop('most_detailed', axis=1, inplace=True)
        return draws

    def keep_cols(self):
        '''Returns the important columns, for easy subsetting'''
        draw_cols = ['draw_%s' % i for i in xrange(1000)]
        index_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
        keep_cols = list(draw_cols)
        keep_cols.extend(index_cols)
        return keep_cols, index_cols, draw_cols

    def zero_draws(self, df):
        '''Given a dataframe obtained from get_draws zeros out all the draws for the appropriate locations, and returns the zero'd out dataframe
        '''
        # keep SSA, SA, Yemen and Sudan 
        keep_df = pd.read_stata("{FILEPATH}")
        keep_locs = keep_df.location_id.tolist()
        zero_df = df.copy(deep=True)
        keep_cols, index_cols, draw_cols = self.keep_cols()
        for col in draw_cols:
            zero_df.loc[~zero_df.location_id.isin(keep_locs), col] = 0.
        return zero_df

    def output(self, df, output_me, measure):
        '''Outputs in the format required by save_results'''
        out_dir = '%s/%s' % (self.cluster_dir, output_me)
        locations = df.location_id.unique()
        year = df.year_id.unique().item()
        year = int(year)
        for geo in locations:
            for sex in [1,2]:
                output = df[(df.location_id == geo) & (df.sex_id == sex)]
                output.to_csv('%s/%s_%s_%s_%s.csv' % (out_dir, measure,
                                                 geo, year, sex), index=False)

class Zero_Fistula(Base):
    def __init__(self, cluster_dir, year_id, input_me, output_me):
        Base.__init__(self, cluster_dir, year_id, input_me, output_me)

    def run(self):
        k_cols, i_cols, d_cols = self.keep_cols()
        # get incidence draws
        inc = self.get_draws()
        inc = inc[k_cols]
        # get prevalence draws
        prev = self.get_draws(measure_id=5)
        prev = prev[k_cols]
        #zero draws
        zero_prev = self.zero_draws(prev)
        zero_inc = self.zero_draws(inc)
    
        self.output(zero_prev, output_me, 5)
        self.output(zero_inc, output_me, 6)


if __name__ == "__main__":
    if len(sys.argv) < 6:
        raise Exception('''Need class_name, cluster_dir, year_id, input_ME, and
                            output_MEs as args''')
    class_name = sys.argv[1]
    cluster_dir = sys.argv[2]
    year = int(sys.argv[3])
    input_mes = sys.argv[4].split(';')
    out_mes = sys.argv[5].split(';')

    if class_name == "Zero_Fistula":
        input_me = int(input_mes[0])
        output_me = int(out_mes[0])
        model = Zero_Fistula(cluster_dir, year, input_me, output_me)
    else:
        raise ValueError('''Must be Zero_Fistula''')

    model.run()