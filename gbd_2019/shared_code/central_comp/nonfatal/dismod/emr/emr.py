import logging
import pandas as pd
import numpy as np
from db_tools import loaders, ezfuncs, config, query_tools
from cascade_ode.demographics import Demographics
from cascade_ode.emr import data, envs
from cascade_ode.settings import load as load_settings

settings = load_settings()
LOGGER = logging.getLogger(__name__)


#############################################################################
# CUSTOM EXCEPTIONS
#############################################################################


class InsufficientInputs(Exception):
    pass


class NoEMRCalculated(Exception):
    pass

#############################################################################
# COMPUTE CLASS
#############################################################################


class ComputeEMR(object):

    def __init__(self, data_prev=None, data_inci=None, data_remis=None,
                 data_csmr=None, data_acmr=None, data_emr_pred=None):
        self.data_prev = data_prev
        self.data_inci = data_inci
        self.data_remis = data_remis
        self.data_csmr = data_csmr
        self.data_acmr = data_acmr
        self.data_emr_pred = data_emr_pred

    def can_compute_from_prev(self):
        return (self.data_prev and self.data_csmr)

    def can_compute_from_inci(self):
        return (
            self.data_inci and self.data_csmr and self.data_remis and
            self.data_acmr and self.data_emr_pred)

    def compute_emr_from_prev(self):
        if self.can_compute_from_prev():
            emr_from_prev = pd.DataFrame(index=self.data_prev.df.index)

            # compute mean
            emr_from_prev["mean"] = (
                self.data_csmr.df[self.data_csmr.mean_col] /
                self.data_prev.df[self.data_prev.mean_col])

            # compute se
            emr_from_prev["se"] = (
                emr_from_prev["mean"] *
                np.sqrt(
                    (
                        self.data_prev.df[self.data_prev.se_col] /
                        self.data_prev.df[self.data_prev.mean_col]
                    )**2 +
                    (
                        self.data_csmr.df[self.data_csmr.se_col] /
                        self.data_csmr.df[self.data_csmr.mean_col]
                    )**2
                )
            )

            return emr_from_prev
        else:
            raise InsufficientInputs(
                "compute_emr_from_prev requires data_prev and data_csmr to"
                "be set")

    def compute_emr_from_inci(self):

        if self.can_compute_from_inci():
            emr_from_inci = pd.DataFrame(index=self.data_inci.df.index)

            # compute mean
            emr_from_inci["mean"] = (
                self.data_csmr.df[self.data_csmr.mean_col] *
                (
                    self.data_remis.mean +
                    (
                        self.data_acmr.df[self.data_acmr.mean_col] -
                        self.data_csmr.df[self.data_csmr.mean_col]
                    ) +
                    self.data_emr_pred.df[self.data_emr_pred.mean_col]
                ) /
                self.data_inci.df[self.data_inci.mean_col]
            )

            # compute se
            emr_from_inci["se"] = (
                emr_from_inci["mean"] *
                np.sqrt(
                    (
                        self.data_inci.df[self.data_inci.se_col] /
                        self.data_inci.df[self.data_inci.mean_col]
                    )**2 +
                    (
                        self.data_csmr.df[self.data_csmr.se_col] /
                        self.data_csmr.df[self.data_csmr.mean_col]
                    )**2 +
                    (
                        self.data_acmr.df[self.data_acmr.se_col] /
                        self.data_acmr.df[self.data_acmr.mean_col]
                    )**2 +
                    (
                        self.data_emr_pred.df[self.data_emr_pred.se_col] /
                        self.data_emr_pred.df[self.data_emr_pred.mean_col]
                    )**2 +
                    (
                        self.data_remis.se /
                        self.data_remis.mean
                    )**2
                )
            )
            return emr_from_inci
        else:
            raise InsufficientInputs(
                "compute_emr_from_inci requires data_inci, data_csmr,"
                " data_acmr, data_remis, and data_acmr be set")


class ComputeDismodEMR(ComputeEMR):

    def __init__(self, model_version_id, decomp_step, envr="prod"):
        # setup global environment
        assert envr in ["prod", "dev"], ("envr must be 'prod' or 'dev'")
        envs.Environment.set_environment(envr)
        config.DBConfig(
            load_base_defs=True, load_odbc_defs=True,
            odbc_filepath=settings['odbc_filepath'])

        self.model_version_id = model_version_id
        self.emr = None
        self.decomp_step = decomp_step
        self.gbd_round_id = Demographics(model_version_id).gbd_round_id

    @property
    def data_csmr(self):
        return self._data_csmr

    @property
    def data_prev(self):
        return self._data_prev

    @property
    def data_inci(self):
        return self._data_inci

    @property
    def data_remis(self):
        return self._data_remis

    @property
    def data_acmr(self):
        return self._data_acmr

    @property
    def data_emr_pred(self):
        return self._data_emr_pred

    def set_data_csmr(self, id_template_df, csmr_type="cod"):
        # data_csmr
        if csmr_type == "cod":
            try:
                self._data_csmr = data.DataCoDCSMR(
                    self.model_version_id, id_template_df)
            except data.NoNonZeroValues:
                self._data_csmr = None
        else:
            try:
                self._data_csmr = data.DataCustomCSMR(
                    self.model_version_id, id_template_df)
            except data.NoNonZeroValues:
                self._data_csmr = None

    def set_data_prev(self, adj_data, adj_meta):
        try:
            self._data_prev = data.DataModelDataAdjPrevalence(
                adj_data, adj_meta)
        except data.NoNonZeroValues:
            self._data_prev = None

    def set_data_inci(self, adj_data, adj_meta):
        try:
            self._data_inci = data.DataModelDataAdjIncidence(
                adj_data, adj_meta)
        except data.NoNonZeroValues:
            self._data_inci = None

    def set_data_remis(self, remission_df):
        self._data_remis = data.DataRemission(remission_df)

    def set_data_acmr(self, id_template_df):
        try:
            self._data_acmr = data.DataACMR(id_template_df, self.decomp_step,
                                            self.gbd_round_id)
        except data.NoNonZeroValues:
            self._data_acmr = None

    def set_data_emr_pred(self, id_template_df):
        try:
            self._data_emr_pred = data.DataModelEstimateFitEMR(
                self.model_version_id, id_template_df)
        except data.NoNonZeroValues:
            self._data_emr_pred = None

    def compute_dismod_emr(self, remission_df=None, csmr_type="cod"):

        # pull in crosswalk data and associated metadata
        adj_data = data.DataModelDataAdj(self.model_version_id)
        self.adj_meta = data.DataMetadata(self.model_version_id)

        # build template
        id_template_df = data.adj_data_template(self.adj_meta.df.reset_index(),
                                                self.model_version_id)
        id_template_df = id_template_df[[
            "input_data_key", "location_id", "year_id", "age_group_id",
            "sex_id"]]

        # get ready to compute emr
        emr_dfs = []
        self.set_data_remis(remission_df)
        self.set_data_csmr(id_template_df, csmr_type)
        self.set_data_prev(adj_data, self.adj_meta)

        try:
            emr_dfs.append(self.compute_emr_from_prev())
        except InsufficientInputs:
            emr_dfs.append(pd.DataFrame())

        # set some data attributes dynamically to avoid reading excess
        # if remission is not < 1 compute for incidence
        if not (self.data_remis.upper < 1):
            self.set_data_inci(adj_data, self.adj_meta)
            self.set_data_acmr(id_template_df)
            self.set_data_emr_pred(id_template_df)

            try:
                emr_dfs.append(self.compute_emr_from_inci())
            except InsufficientInputs:
                emr_dfs.append(pd.DataFrame())

        # concatenate prevalence and incidence
        emr = pd.concat(emr_dfs)

        # get rid of any nulls
        try:
            emr = emr.loc[(emr["mean"].notnull()) & (emr["se"].notnull()), :]
        except KeyError:
            # if a key error is raised that means the dataset is empty
            raise InsufficientInputs(
                "The model requires one of incidence and prevalence")

        # check if we computed anything
        if emr.empty:
            raise NoEMRCalculated(
                "No emr could be calculated from the provided parameters")
        else:
            self.emr = emr

    def format_emr_for_upload(self):
        df = self.emr.merge(self.adj_meta.df, left_index=True,
                            right_index=True)
        df = df.reset_index()
        df = df.rename(columns={"input_data_key": "model_version_dismod_id",
                                "nid": "source_nid",
                                "se": "standard_error",
                                "underlying_nid": "source_underlying_nid"})
        df["model_version_id"] = self.model_version_id
        df["measure_id"] = 9
        df["lower"] = df.apply(
            lambda x: data.lower_from_se(x["mean"], x["standard_error"]),
            axis=1)
        df["upper"] = df.apply(
            lambda x:
                data.upper_from_se(x["mean"], x["standard_error"], "rate"),
            axis=1)
        self.emr = df

    def upload_emr(self):
        session = ezfuncs.get_session(envs.Environment.get_odbc_key())
        table = "t3_model_version_emr"
        schema = "epi"

        should_upload = _verify_no_data_present(
            table, schema, self.model_version_id, session)

        if not should_upload:
            return

        path = ("{cascade_root}/{mv}/full/locations/1/outputs/both/2000"
                ).format(cascade_root=envs.Environment.get_cascade_root(),
                         mv=self.model_version_id)
        infiler = loaders.ToInfile(path)
        infiler.stage_2_stagedir(self.emr)
        infiler.upload_stagedir(
            table=table, schema=schema, session=session,
            commit=True)


def _verify_no_data_present(table, schema, model_version_id, session):
    qry = f"""
    SELECT count(*) as count from {schema}.{table}
    WHERE
    model_version_id = :model_version_id"""
    count = query_tools.query_2_df(
        qry, session=session,
        parameters={'model_version_id': model_version_id})['count'].iat[0]
    return count == 0


#############################################################################
# API FUNCTION
#############################################################################


def dismod_emr(model_version_id, decomp_step, envr="prod", remission_df=None,
               csmr_type="cod", no_upload=False):
    """compute emr for dismod cascade and upload to db

    Args:
        model_version_id (int): model_version_id to compute emr for
        decomp_step (str): decomposition step for the provided model_version
        envr (str): string of environments to use. 'dev' or 'prod'
        remission_df (pandas.DataFrame): dataframe of remission values.
            columns (mean, lower, upper) required.
        csmr_type (str): what type of csmr to use. 'cod' if it should pull
            from t3_model_version_csmr. 'custom' if it should pull from
            t3_model_version_dismod.
        no_upload (bool,optional): Whether to upload the resulting EMR.
    """
    compute_emr = ComputeDismodEMR(model_version_id, decomp_step, envr=envr)
    compute_emr.compute_dismod_emr(
        remission_df=remission_df, csmr_type=csmr_type)
    compute_emr.format_emr_for_upload()
    if not no_upload:
        compute_emr.upload_emr()
        LOGGER.info(f"Uploaded EMR")
    else:
        LOGGER.info(f"Not uploading EMR as requested by no_upload flag.")
