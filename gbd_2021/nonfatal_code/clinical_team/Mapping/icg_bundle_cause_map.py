"""
The mapping will be both partial and duplicated in places, but we can use
bundle_id to map from cause_id to icg_id
"""

import os
import datetime
from clinical_info.Mapping import clinical_mapping
from db_tools.ezfuncs import query


class CauseICG:
    """
    get the 3 maps you'll need, then merge together and produce 2 outputs
    icg-->cause_id and cause_code-->cause_id

    example use:
    # instantiate with map version 28
    icg_cause = CauseICG(mapv=28)
    # run all the methods and output 2 maps to the self.outdir location
    icg_cause.prep_clinical_to_cause_maps()
    """

    def __init__(self, mapv):
        self.mapv = mapv
        self.outdir = "FILEPATH"

    def get_icd_to_icg(self):
        # get cc to icg
        cm = clinical_mapping.get_clinical_process_data(
            "cause_code_icg", map_version=self.mapv
        )
        # drop the special maps
        self.cm = cm[cm["code_system_id"].isin([1, 2])]
        return

    def get_icg_to_bundle(self):
        # get icg to bundle
        self.bm = clinical_mapping.get_clinical_process_data(
            "icg_bundle", map_version=self.mapv
        )

    def get_bundle_to_cause(self):
        # get bundle to cause_id to acause
        cb = query("QUERY", conn_def="CONN",)
        # this total maternal bundle causes some unneeded duplicates, remove it
        drops = [1010]
        self.cb = cb[~cb.bundle_id.isin(drops)]

    def merge_cause_to_bundle(self):
        # compare!
        self.dat = self.bm.merge(self.cb, how="left", on="bundle_id", validate="m:1")
        assert len(self.dat) == len(self.bm)

    def merge_icg_to_cause(self):
        # icg to cause
        keeps = ["icg_name", "icg_id", "cause_id", "acause", "cause_name"]
        ic = self.dat[keeps].drop_duplicates().copy()
        ic = ic[ic.cause_id.notnull()]
        ic["icg_maps_to_multiple_causes"] = "no"
        ic.loc[
            ic["icg_id"].duplicated(keep=False), "icg_maps_to_multiple_causes"
        ] = "yes"
        self.ic = ic

    def icg_to_cause_with_missing(self):
        keeps = ["icg_name", "icg_id", "map_version"]
        df = self.cm[keeps].drop_duplicates().copy()
        df = df.merge(self.ic, how="outer", on=["icg_name", "icg_id"], validate="1:m")
        assert df["icg_id"].isnull().sum() == 0
        self.df = df

        df_icd = (
            self.cm[["cause_code", "code_system_id"] + keeps].drop_duplicates().copy()
        )
        df_icd = df_icd.merge(self.ic, how="left", on=["icg_name", "icg_id"])
        self.df_icd = df_icd

    def _archive(self, df, fpath):
        n = datetime.datetime.now()
        fn = os.path.basename(fpath)
        p = f"FILEPATH"
        df.to_csv(p, index=False)

    def save(self):
        p1 = f"FILEPATH"
        p2 = f"FILEPATH"
        self.df.to_excel(p1, index=False)
        self._archive(self.df, p1)
        self.df_icd.to_excel(p2, index=False)
        self._archive(self.df_icd, p2)

    def prep_clinical_to_cause_maps(self):
        self.get_icd_to_icg()
        self.get_icg_to_bundle()
        self.get_bundle_to_cause()
        self.merge_cause_to_bundle()
        self.merge_icg_to_cause()
        self.icg_to_cause_with_missing()
        self.save()
