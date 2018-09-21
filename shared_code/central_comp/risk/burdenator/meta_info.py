import json


def write_meta(out_dir, meta_dict, filename='meta.json'):
    """Writes the meta_dict to the file meta.json in out_dir. If the
    specified file already exists, it will be overwritten."""
    meta_fn = "{}/{}".format(out_dir, filename)
    json.dump(meta_dict, file(meta_fn, 'w'))


def load_meta(out_dir, filename='meta.json'):
    """Loads and returns the contents of meta.json in out_dir as a
    dictionary"""
    meta_fn = "{}/{}".format(out_dir, filename)
    return json.load(file(meta_fn, 'r'))

def generate_meta(df):
    meta = {}
    for sex_id in df.sex_id.unique().tolist():
        meta[sex_id] =  {}
        for measure_id in df.measure_id.unique().tolist():
            meta[sex_id][measure_id] = {}
            # Get cause ids for this sex, measure combination
            cause_ids = df.loc[((df['sex_id']==sex_id)&
                               (df['measure_id']==measure_id)),
                              'cause_id'].unique().tolist()
            meta[sex_id][measure_id]['cause_ids'] = cause_ids
            # Get risk ids for this sex, measure combination
            rei_ids = df.loc[((df['sex_id']==sex_id)&
                             (df['measure_id']==measure_id)),
                             'rei_id'].unique().tolist()
            rei_ids = list(set(rei_ids) - set([0]))
            meta[sex_id][measure_id]['rei_ids'] = rei_ids
    return meta
