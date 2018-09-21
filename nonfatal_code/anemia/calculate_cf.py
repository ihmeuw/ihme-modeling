import pandas as pd
import scipy.stats as sp
import sys

hgb_file = '{FILEPATH}'
idcols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
year_id = {YEAR ID}
sex_id = {SEX ID}
age_group_id = {AGE GROUP ID}
pctles = []

def get_cf(year_id, sex_id, age_group_id):
    hb_est = pd.read_hdf(
                hgb_file,
                where="year_id==%s & sex_id==%s & age_group_id==%s" % (year_id, sex_id, age_group_id))

    for d in range(1000):
        hb_est['hgb_pop_normal_%s' % d] = sp.scoreatpercentile(hb_est['hgb_%s' % d], 95)
        hb_est['normal_hb_%s' %d] = hb_est[['mean_hgb', 'hgb_pop_normal_%s' % d]].max(axis=1)

    names = [c for c in list(hb_est) if (c[:4] != 'hgb_')]
    cf_hb = hb_est[names]
    cf_hb.to_csv('/FILEPATH/%s_%s_%s.csv' % (year_id, sex_id, age_group_id))

if __name__ == "__main__":
    try:
        year_id = int(sys.argv[1])
    except:
        year_id = {YEAR IDS}
    try:
        sex_id = int(sys.argv[2])
    except:
        sex_id = {SEX IDS}
    try:
        age_group_id = int(sys.argv[3])
    except:
        age_group_id = {AGE GROUP IDS}

    get_cf(year_id, sex_id, age_group_id)
