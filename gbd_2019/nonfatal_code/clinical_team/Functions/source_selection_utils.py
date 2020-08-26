"""
tools to subset/add/remove data based on source and/or nid
"""
import pandas as pd

def retain_active_inp_sources(df, verbose=True):
    """
    This function will drop any inactive NIDs from inpatient hospital data
    """

    if 'nid' not in df.columns:
        print("nid is not present, this function cannot retain active sources")
        return df

    else:
        actives = pd.read_csv("FILEPATH")

        actives = actives.query("active_type != 'outpatient'")

        active_nids = actives.loc[actives['is_active'] == 1, 'nid']


        pre_rows = df.shape[0]
        pre_nids = df.nid.unique().tolist()


        df = df[df['nid'].isin(active_nids)]



        drop_rows = pre_rows - df.shape[0]
        drop_nids = set(pre_nids) - set(df.nid.unique())
        drop_source = actives.loc[actives['nid'].isin(drop_nids), 'source_name'].unique()
        unknown_nids = set(drop_nids) - set(actives.nid.unique())

        if verbose:
            print("The source(s) {} have been dropped".\
                format(drop_source))
            if unknown_nids:
                print("Possibly related to the source(s) above, we also found that NID(s) "\
                    "\n{}\n were dropped but they're not in the source/nid lookup table".\
                    format(list(unknown_nids)))
            print("\nA total of {} rows were dropped".format(drop_rows))
        if df.shape[0] == 0:
            assert False, "All records were lost, perhaps your data contains merged nids?"

        return df
