import logging
import math
import os
from functools import lru_cache, reduce
from typing import List

import numpy as np
import pandas as pd

from elmo import get_crosswalk_version
from hierarchies.dbtrees import loctree

from cascade_ode.legacy import csmr, db, demographics, ln_asdr, upload
from cascade_ode.legacy.settings import load as load_settings
from cascade_ode.legacy.shared_functions import (
    get_age_weights,
    get_covariate_estimates,
    get_envelope,
    get_population,
)

# Set default file mask to readable-for all users
os.umask(0o0002)

# Get this filepath
this_path = os.path.dirname(os.path.abspath(__file__))

# Get configuration options
settings = load_settings()

# mapping from parameter_type_id to a short name for the prior_type
# (missing from data-base).

inf = float("inf")
IHME_COD_DB = -999999999
"""
This is a data ID to mark data from ASDR and CSMR databases.
"""

likelihood_map = {
    1: "gaussian",
    2: "laplace",
    3: "log_gaussian",
    4: "log_laplace",
    5: "log_gaussian",
}

integrand_rate_map = {
    "incidence": "iota",
    "remission": "rho",
    "mtexcess": "chi",
    "mtother": "omega",
}

integrand_pred = [
    "incidence",
    "remission",
    "prevalence",
    "mtexcess",
    "mtall",
    "relrisk",
    "mtstandard",
    "mtwith",
    "mtspecific",
    "mtother",
]

# ----------------------------------------------------------------------------


def gen_locmap(location_set_version_id: int, release_id: int):
    """Given a location set version ID and release, return a dataframe
    denomormalized on superregion, region, subregion (ie national), and most
    detailed location (ie atom)
    """
    lt = loctree(location_set_version_id=location_set_version_id, release_id=release_id)
    lmap = lt.flatten()
    lmap.drop("level_0", axis=1, inplace=True)
    lmap.rename(
        columns={
            "level_1": "super",
            "level_2": "region",
            "level_3": "subreg",
            "level_4": "atom",
            "leaf_node": "location_id",
        },
        inplace=True,
    )
    lmap = lmap.append(
        lmap.loc[lmap.atom.notnull(), ["super", "region", "subreg"]].drop_duplicates(),
        sort=True,
    )
    lmap.loc[lmap.location_id.isnull(), "location_id"] = lmap.loc[
        lmap.location_id.isnull(), "subreg"
    ]
    lmap = lmap.append(lmap[["super", "region"]].drop_duplicates(), sort=True)
    lmap.loc[lmap.location_id.isnull(), "location_id"] = lmap.loc[
        lmap.location_id.isnull(), "region"
    ]
    lmap = lmap.append(lmap[["super"]].drop_duplicates(), sort=True)
    lmap.loc[lmap.location_id.isnull(), "location_id"] = lmap.loc[
        lmap.location_id.isnull(), "super"
    ]

    for c in ["atom", "subreg", "region", "super"]:
        lmap[c] = lmap[c].apply(lambda x: "{:.0f}".format(x))
        lmap.loc[lmap[c] == "nan", c] = "none"

    return lmap


@lru_cache()
def get_model_version(mvid: int) -> pd.DataFrame:
    """Given a model version ID, return a dataframe of settings/metadata.

    Uses ENVIRONMENT_NAME environment variable to determine prod/dev db.

    Arguments:
        mvid: model_version_id.

    Returns:
        Dataframe of model version metadata.
    """
    query = f"""
     SELECT
     mv.modelable_entity_id, mv.description, mr.*,
     integrand_only, integration_type, location_set_version_id,
     mv.gbd_round_id, mv.crosswalk_version_id, mv.release_id
     FROM epi.model_version_mr mr
     JOIN epi.model_version mv using (model_version_id)
     JOIN epi.modelable_entity USING(modelable_entity_id)
     JOIN epi.integration_type it ON integrate_method=it.integration_type_id
     LEFT JOIN (SELECT measure_id, measure as integrand_only
         FROM shared.measure) m ON mr.measure_only=m.measure_id
     WHERE model_version_id={mvid}
        """
    df = db.execute_select(query)
    df = df.astype(
        {
            "modelable_entity_id": "int",
            "description": "str",
            "gbd_round_id": "int",
            "release_id": "int",
        }
    )
    return df


class Importer(object):
    def __init__(
        self,
        model_version_id: int,
        root_dir: str = os.path.expanduser(settings["cascade_ode_out_dir"]),
    ) -> None:
        """
        The Importer reads input data from the epi database, dismod settings
        from the epi database, and COD CSMR data from the mortality database.

        It does some transformation and validation on this data and writes
        results to various csvs for the particular model version given. It will
        write directories that don't exist.

        Uses ENVIRONMENT_NAME environment variable to determine prod/dev epi
        database server. Always uses prod cod data.

        Args:
            model_version_id(int): model version ID
            root_dir(str): Folder path up to where model_version_id folder
                exists (will make if doesn't exist)
        """

        log = logging.getLogger(__name__)
        log.info("Starting importer")
        self.root_dir = 
        os.makedirs(self.root_dir, exist_ok=True)

        try:
            os.chmod(self.root_dir, 0o775)
        except Exception:
            log.exception("Could not chmod root dir {}".format(self.root_dir))

        self.demographics = demographics.Demographics(model_version_id=model_version_id)
        self.mvid = model_version_id
        self.gbd_round_id = self.demographics.gbd_round_id
        self.model_params = self.get_model_parameters()
        self.model_version_meta = get_model_version(self.mvid)
        self.release_id = self.model_version_meta.release_id.iat[0]
        self.covariate_data = pd.DataFrame()
        self.ages = self.get_age_ranges()
        if self.model_version_meta.integrand_only.isnull().any():
            self.single_param = None
            self.integrand_pred = [
                "incidence",
                "remission",
                "prevalence",
                "mtexcess",
                "mtall",
                "relrisk",
                "mtstandard",
                "mtwith",
                "mtspecific",
                "mtother",
            ]

        else:
            self.single_param = self.model_version_meta.integrand_only.values[0]
            self.model_params["measure"] = self.model_params.measure.replace(
                {self.single_param: "mtother"}
            )
            self.integrand_pred = ["mtother"]

        self.lmap = gen_locmap(
            self.model_version_meta.location_set_version_id.values[0],
            release_id=self.release_id,
        )
        self.age_mesh = self.get_age_mesh()
        log.info("Reading input data")
        self.data = self.get_t3_input_data()
        log.info("Done reading input data")

    @staticmethod
    def execute_proc(proc, args, conn_def="epi"):
        """Execute a stored prodcedure."""
        engine = db.get_engine(
            conn_def=conn_def, env=settings["env_variables"]["ENVIRONMENT_NAME"]
        )
        conn = engine.raw_connection()
        try:
            cursor = conn.cursor()
            cursor.callproc(proc, args)
            cursor.close()
            conn.commit()
        finally:
            conn.close()

    def attach_study_covariates(self, df):
        """
        Takes a dataframe with a column called "study_covariates" that
        is a space-separated list of covariate IDs. Will read matching
        covariates from covariates database and add a new column per covariate
        (i.e., denormalize). It drops original "study_covariates" column.

        New study covariate columns are prefixed with 'x_s_', then short
        str identifier named study_covariate

        NOTE: with the switch to out of dismod crosswalks, query_t3_dismod will
        always return the empty string so this method is a no-op. We're keeping
        the implementation around for a while in case we need to revert.

        Returns:
            Pandas dataframe
        """
        dmdf = df.copy()
        sclists = df.study_covariates.str.strip()
        sclists = sclists.apply(lambda x: str(x).split(" "))

        scovs = reduce(lambda x, y: x.union(set(y)), sclists, set())
        try:
            scovs.remove("None")
        except Exception as e:  # noqa: F841
            pass
        try:
            scovs.remove("")
        except Exception as e:  # noqa: F841
            pass
        scovs = list(scovs)
        scov_str = ",".join(scovs)

        # Retrieve study covariate names and IDs
        query = """
            SELECT study_covariate_id, study_covariate
            FROM epi.study_covariate
            WHERE study_covariate_id IN (%s) """ % (
            scov_str
        )

        if len(scovs) > 0:
            scov_names = db.execute_select(query)

            for _, row in scov_names.iterrows():
                v_zeros_ones = [
                    1 if str(row["study_covariate_id"]) in r else 0 for r in sclists
                ]
                dmdf["x_s_" + row["study_covariate"]] = v_zeros_ones

        dmdf.drop("study_covariates", axis=1, inplace=True)
        return dmdf

    def get_age_ranges(self):
        """Get a dataframe mapping age_group_id to age_group_years_start and
        years_end"""
        aq = """
                SELECT age_group_id, age_group_years_start, age_group_years_end
                FROM shared.age_group"""
        return db.execute_select(aq, "cod")

    def promote_asdr_t2_to_t3(self):
        """
        Take a snapshot of age specific death rate from cod database,
        write in root of model version directory, and infile to
        epi.t3_model_version_asdr.

        Returns:
            pandas dataframe

        Raises:
            ValueError if asdr has already been promoted
        """
        # Check whether this model has already been loaded, and if so
        # throw an error
        t3_csmr_rows = db.execute_select(
            """
            SELECT * FROM epi.t3_model_version_asdr
            WHERE model_version_id={}
            LIMIT 1""".format(
                self.mvid
            )
        )
        if len(t3_csmr_rows) > 0:
            raise ValueError(
                "asdr for model_version {} has already been promoted "
                "to t3".format(self.mvid)
            )

        # Download from T2 to file (can't cross-insert from different DB
        # servers)
        if self.single_param is None:
            mo_vid = self.model_version_meta.csmr_mortality_output_version_id
            mo_vid = mo_vid.values[0]
            asdr = get_envelope(
                location_id=-1,
                location_set_id=self.demographics.location_set_id,
                age_group_id=self.demographics.mortality_age_grid,
                year_id=self.demographics.mortality_years,
                sex_id=self.demographics.sex_ids,
                with_hiv=1,
                release_id=self.release_id,
            )

            if asdr.empty:
                raise RuntimeError("all cause mortality envelope empty")
            run_id_asdr = asdr.run_id.unique().squeeze()
            logging.info("ASDR run_id is {}".format(run_id_asdr))
            pop = get_population(
                location_id=-1,
                location_set_id=self.demographics.location_set_id,
                age_group_id=self.demographics.mortality_age_grid,
                year_id=self.demographics.mortality_years,
                sex_id=self.demographics.sex_ids,
                release_id=self.release_id,
            )
            run_id_pop = pop.run_id.unique().squeeze()
            logging.info("Pop run_id is {}".format(run_id_pop))

            asdr = asdr.merge(pop, on=["location_id", "year_id", "age_group_id", "sex_id"])
            asdr["meas_value"] = asdr["mean"] / asdr["population"]
            asdr["meas_lower"] = asdr["lower"] / asdr["population"]
            asdr["meas_upper"] = asdr["upper"] / asdr["population"]
            asdr = asdr.merge(self.ages, on="age_group_id")
            asdr.rename(
                columns={
                    "year_id": "time_lower",
                    "sex_id": "x_sex",
                    "age_group_years_start": "age_lower",
                    "age_group_years_end": "age_upper",
                },
                inplace=True,
            )
            asdr["age_upper"] = asdr.age_upper.clip(upper=100)

            df = asdr[
                [
                    "location_id",
                    "time_lower",
                    "age_group_id",
                    "x_sex",
                    "age_lower",
                    "age_upper",
                    "meas_value",
                    "meas_lower",
                    "meas_upper",
                ]
            ]

            # Drop NULLs, typically the result of mismatches between
            # COD and MORT
            df = df[df.meas_value.notnull()]

            # Write to disk and upload to T3 table
            asdr_file = 
            df.to_csv(asdr_file, index=False)

            upload.upload_file(
                self.mvid,
                asdr_file,
                "t3_model_version_asdr",
                [
                    "location_id",
                    "year_id",
                    "age_group_id",
                    "sex_id",
                    "@dummy",
                    "@dummy",
                    "mean",
                    "lower",
                    "upper",
                ],
            )
        else:
            df = pd.DataFrame()
        return df

    def promote_csmr_t2_to_t3(self):
        """
        If this model_version metadata says to add csmr data, take a snapshot
        of cause specific mortality rate used in last rounds best model, write
        in root of model version directory, and infile to
        epi.t3_model_version_csmr.

        Returns:
            pandas dataframe

        Raises:
            ValueError if CSMR has already been promoted
            RuntimeError if CSMR cannot be found
        """
        # exit early if no csmr specified or mtspecific is excluded in
        # parameters
        if self.model_version_meta.add_csmr_cause.isnull().squeeze():
            return pd.DataFrame()

        mpm = self.model_params
        exclude_params = mpm.loc[mpm.parameter_type_id == 17, "measure"]
        if "mtspecific" in list(exclude_params):
            return pd.DataFrame()

        # exit early if csmr already promoted
        t3_csmr_rows = db.execute_select(
            """
            SELECT * FROM epi.t3_model_version_csmr
            WHERE model_version_id={}
            LIMIT 1""".format(
                self.mvid
            )
        )
        if len(t3_csmr_rows) > 0:
            raise ValueError(
                "csmr for model_version {} has already been promoted "
                "to t3".format(self.mvid)
            )

        df = csmr.find_csmr_for_model(self.model_version_meta, self.root_dir)

        # Write to disk and upload to T3 table
        csmr_file = 
        df.to_csv(csmr_file, index=False)

        upload.upload_file(
            self.mvid,
            csmr_file,
            "t3_model_version_csmr",
            [
                "location_id",
                "year_id",
                "age_group_id",
                "sex_id",
                "@dummy",
                "@dummy",
                "mean",
                "lower",
                "upper",
            ],
        )
        return df

    @staticmethod
    def query_t3_dismod_data(crosswalk_version_id):
        """
        Return a pandas dataframe of dismod input data

        This data is coming from the relevant crosswalk_version dataset.
        """
        columns_to_return = [
            "data_id",
            "nid",
            "location_id",
            "integrand",
            "mean",
            "year_start",
            "year_end",
            "age_start",
            "age_end",
            "lower",
            "upper",
            "sample_size",
            "standard_error",
            "sex_id",
            "study_covariates",
            "age_demographer",
        ]
        rename_columns = {"seq": "data_id", "measure": "integrand"}

        df = get_crosswalk_version(crosswalk_version_id)

        # be wary of rename-target columns already in df
        df = rename_potential_overwrite_columns(df, list(rename_columns.values()))

        df = df[df.loc[:, "is_outlier"] == 0]
        df = df.merge(db.execute_select("SELECT sex, sex_id FROM shared.sex"))
        df.loc[:, "study_covariates"] = ""
        if "age_demographer" not in df.columns:
            df.loc[:, "age_demographer"] = np.NaN

        df = df.rename(columns=rename_columns)
        return df.loc[:, columns_to_return]

    def query_t3_emr_data(self):
        """Return a pandas dataframe of tier 3 excess mortality data"""
        query = """
            SELECT
                model_version_dismod_id as data_id,
                location_id,
                year_start as time_lower,
                age_start as age_lower,
                age_end as age_upper,
                sex_id as x_sex,
                mean as meas_value,
                lower as meas_lower,
                upper as meas_upper,
                standard_error
            FROM
                epi.t3_model_version_emr t3_emr
            WHERE model_version_id = {}
            AND t3_emr.outlier_type_id = 0""".format(
            self.mvid
        )
        df = db.execute_select(query)
        if len(df) == 0:
            return pd.DataFrame()
        else:
            # Flag EMR derived data with a negative data_id until a better
            # system is devised. It needs to be exclueded from the
            # adjusted data uploads somehow.
            df["integrand"] = "mtexcess"
            df["data_id"] = -1 * df["data_id"]
            return df

    def query_t3_csmr_data(self):
        """Return a dataframe of tier 3 cause specific mortality rates"""
        query = """
            SELECT
                location_id,
                year_id as time_lower,
                age_group_id,
                sex_id as x_sex,
                mean as meas_value,
                lower as meas_lower,
                upper as meas_upper
            FROM
                epi.t3_model_version_csmr t3_csmr
            WHERE model_version_id = {}""".format(
            self.mvid
        )
        df = db.execute_select(query)
        if len(df) == 0:
            return pd.DataFrame()
        else:
            df["integrand"] = "mtspecific"
            df = df.merge(self.ages)
            df.rename(
                columns={
                    "age_group_years_start": "age_lower",
                    "age_group_years_end": "age_upper",
                },
                inplace=True,
            )
            df["age_upper"] = df.age_upper.clip(upper=100)
            return df

    def query_t3_asdr_data(self):
        """Return a dataframe of tier3 age specific death rates"""
        query = """
            SELECT
                location_id,
                year_id as time_lower,
                age_group_id,
                sex_id as x_sex,
                mean as meas_value,
                lower as meas_lower,
                upper as meas_upper
            FROM
                epi.t3_model_version_asdr t3_asdr
            WHERE model_version_id = {}""".format(
            self.mvid
        )
        df = db.execute_select(query)
        if len(df) == 0:
            return pd.DataFrame()
        else:
            df["integrand"] = "mtall"
            df = df.merge(self.ages)
            df.rename(
                columns={
                    "age_group_years_start": "age_lower",
                    "age_group_years_end": "age_upper",
                },
                inplace=True,
            )
            df["age_upper"] = df.age_upper.clip(upper=100)
            return df

    def convert_location_ids(self, df):
        """Convert location_id to flattened atom/subreg/region/super"""
        dmdf = df.merge(self.lmap, on="location_id")
        return dmdf

    def map_t3mt_to_dismod(self, df, integrand):
        """Transform csmr dataframe to something dismod understands"""

        log = logging.getLogger(__name__)  # noqa: F841

        if len(df) == 0:
            return pd.DataFrame()

        mtdf = df.copy()

        # Calculate stdevs
        mtdf["meas_stdev"] = (mtdf.meas_upper - mtdf.meas_lower) / (2.0 * 1.96)
        mtdf["meas_stdev"] = mtdf.meas_stdev.replace({0: 1e-9})
        mtdf.drop(["meas_lower", "meas_upper"], axis=1, inplace=True)

        # Reformat age and sex values
        mtdf["age_upper"] = mtdf.age_upper.fillna(100)
        mtdf.drop("age_group_id", axis=1, inplace=True, errors="ignore")

        mtdf["x_sex"] = mtdf.x_sex.replace({1: 0.5, 2: -0.5, 3: 0.0})

        # Hard-coded values
        mtdf["time_upper"] = mtdf["time_lower"] + 1
        mtdf["year_id"] = mtdf["time_lower"]
        mtdf["hold_out"] = 0
        mtdf["x_local"] = 1
        mtdf["a_data_id"] = IHME_COD_DB
        mtdf["integrand"] = integrand
        mtdf["data_like"] = likelihood_map[self.model_version_meta.data_likelihood.values[0]]

        # Map to location structure
        mtdf = self.convert_location_ids(mtdf)

        # Assign a dummy nid, since these are "estimates" being used as "data"
        mtdf["nid"] = IHME_COD_DB
        return mtdf

    def map_t3dm_to_dismod(self, df):
        """Transform t3 epi input data to a format for dismod"""
        renames = {
            "data_id": "a_data_id",
            "mean": "meas_value",
            "year_start": "time_lower",
            "year_end": "time_upper",
            "age_start": "age_lower",
            "age_end": "age_upper",
            "standard_error": "meas_stdev",
            "sex_id": "x_sex",
        }
        dmdf = df.rename(columns=renames)
        dmdf["x_sex"] = dmdf.x_sex.replace({1: 0.5, 2: -0.5, 3: 0.0})

        # x_local = 1 means that this "data" is real data for the
        # location to be modeled. 0 means it is a "prior," and is handled
        # differently depending on the prior weighting settings.
        #
        # TODO: Consdier moving this setting into Cascade_loc, as it is
        # more an attribute that changes from location to location
        dmdf["x_local"] = 1

        # TODO: Move this to Cascade. It is really more of a property
        # of a given cascade run, not a matter of the import from the
        # database itself
        dmdf["hold_out"] = 0

        # TODO: Re-consider whether we really want to modify this here...
        # The uploader may already be making the proper SE calculations?
        dmdf["meas_stdev"] = dmdf.apply(
            lambda x: recalc_SE(
                x["meas_value"], x["meas_stdev"], x["sample_size"], x["lower"], x["upper"]
            ),
            axis=1,
        )
        dmdf = dmdf.drop(["lower", "upper", "sample_size"], axis=1)

        # Convert locations to scheme
        dmdf = self.convert_location_ids(dmdf)

        # Use the mid-year as the year_id for merging country covariates
        dmdf["year_id"] = ((dmdf.time_upper + dmdf.time_lower) / 2).astype("int")

        return dmdf

    def set_data_likelihood(self, df):
        """Take a dataframe and use model version metadata's data_likelihood
        field to add a 'data_like' column
        """
        # TODO: Consider moving this to Cascade as well, it is more
        # a property of the cascade. Want to scope Importer to be a
        # fairly "dumb" retriveval and translation layer between
        # the cascade and the database
        dmdf = df.copy()
        dmdf["data_like"] = likelihood_map[self.model_version_meta.data_likelihood.values[0]]
        return dmdf

    def exlude_data(self, df):
        """Drop measures from dataframe, depending on model parameters"""
        # TODO: Consider moving this to Cascade, as exclusions are
        # model-parameter specified. Ideally, I think the Importer should
        # retrieve and reshape those parameters, but not necessarily apply
        # them. The Cascade should decide what to do with them.
        mpm = self.model_params
        exclude_params = mpm.loc[mpm.parameter_type_id == 17, "measure"]
        return df[~df.integrand.isin(exclude_params)]

    def restrict_single_param(self, df):
        """If self.single_param, restrict df to just that integrand. Then
        relabel integrand as mtother because dismod expects that.

        If not single_param, restrict to measures in integrand_pred list"""
        # TODO: This should perhaps be handled in the query? No point
        # in retrieve all that other data if we're only using one parameter's
        # worth.
        #
        # Scope to single parameter if requested
        log = logging.getLogger(__name__)
        if self.single_param is not None:
            log.info(f"Single parameter model subsetting to integrand {self.single_param}")
            dmdf = df[df.integrand == self.single_param]
            dmdf["integrand"] = dmdf.integrand.replace({self.single_param: "mtother"})
        else:
            log.info(
                f"{len(df)} rows of data before subsetting integrands to `integrand_pred`"
            )
            dmdf = df[df.integrand.isin(integrand_pred)]
            log.info(
                f"{len(dmdf)} rows of data after subsetting integrands to `integrand_pred`"
            )
            if dmdf[dmdf["a_data_id"] != IHME_COD_DB].empty:
                log.warning(
                    "After restricting input data to valid measures, "
                    "no bundle data remains!"
                )
        return dmdf

    @staticmethod
    def correct_demographer_notation(df, gbd_round_id):
        """Correct for demographer's notation by adding 1 to age_upper for
        relevant demographer-notated age groups.

        A demographer-notated age group describies ages by taking the upper
        bound of that age group to be the limit as that age approaches the
        subsequent integer. e.g.:
            ages 1-4 are actually 1-4.999....
        We account for this notation by incrasing the given 'age_upper' by
        one. We only preform this transformation on rows:
            * That have a lower age bound greater than one
            * That have an upper age bound less than 100

        Upon investigation, these assumptions about the state of input data
        highlighted some more cases where we ought not preform 'age_upper'
        mutation. For gbd2021 and beyond, we reduce the set of rows we
        transform to rows:
            * That aren't codcorrect CSMR
            * That have an age_demographer value that isn't 0
        """
        dmdf = df.copy()
        mutate_mask = (dmdf.age_lower >= 1) & (dmdf.age_upper < 100)
        if gbd_round_id >= 7:
            mutate_mask = (
                mutate_mask & (dmdf.a_data_id != IHME_COD_DB) & (dmdf.age_demographer != 0)
            )
        dmdf.loc[mutate_mask, "age_upper"] = dmdf.loc[mutate_mask, "age_upper"] + 1
        return dmdf

    def get_t3_input_data(self):
        """
        Main method for getting final formatted input data to dismod.  Reads
        promoted tier 3 input data (or data from crosswalk_version) from
        database, transforms to match dismod expectations, sets data
        likelihood.

        Returns:
            pandas dataframe"""
        log = logging.getLogger(__name__)  # noqa: F841
        data = self.query_t3_dismod_data(self.model_version_meta.crosswalk_version_id.iat[0])
        data = self.map_t3dm_to_dismod(data)
        data = self.set_data_likelihood(data)
        data = self.attach_study_covariates(data)

        # Raise an error if there are conflics with CSMR settings
        # and the dataset
        user_requested_csmr = self.model_version_meta.add_csmr_cause.notnull().squeeze()
        csmr_in_data = "mtspecific" in data.integrand.unique()
        if user_requested_csmr and csmr_in_data:
            raise ValueError("Cannot both provide custom CSMR data and data from CodCorrect")

        try:
            csmr_data = self.promote_csmr_t2_to_t3()
        except ValueError as e:
            if "already been promoted" in str(e):
                csmr_data = self.query_t3_csmr_data()
            else:
                raise
        csmr_data = self.map_t3mt_to_dismod(csmr_data, "mtspecific")
        try:
            asdr_data = self.promote_asdr_t2_to_t3()
        except ValueError as e:
            if "already been promoted" in str(e):
                asdr_data = self.query_t3_asdr_data()
            else:
                raise
        asdr_data = self.map_t3mt_to_dismod(asdr_data, "mtall")
        emr_data = self.query_t3_emr_data()
        emr_data = self.map_t3mt_to_dismod(emr_data, "mtexcess")
        data = pd.concat([data, csmr_data, asdr_data, emr_data])

        # TODO: Consider moving all of the below to Cascade
        data = self.exlude_data(data)
        data = self.restrict_single_param(data)
        data = self.correct_demographer_notation(
            data, self.model_version_meta.gbd_round_id.iat[0]
        )
        return data

    def measured_integrands(self):
        """Returns list of unique integrands in input data"""
        return list(self.data.integrand.drop_duplicates())

    def measured_locations(self):
        """Returns dictionary mapping location type (atom/subreg/region/super)
        to possibly empty list. If list is not empty, that location type is in
        the input data"""
        data = self.data
        if self.single_param is None:
            data = data[data.integrand != "mtall"]
        mls = {}
        for ltype in ["atom", "subreg", "region", "super"]:
            mls[ltype] = list(data.loc[data[ltype] != "none", ltype].drop_duplicates())
        return mls

    def get_model_parameters(self):
        """Return dataframe of model parameters"""
        query = """
            SELECT * FROM epi.model_parameter
            LEFT JOIN shared.measure USING(measure_id)
            LEFT JOIN epi.parameter_type USING(parameter_type_id)
            LEFT JOIN epi.study_covariate USING(study_covariate_id)
            WHERE model_version_id=%s """ % (
            self.mvid
        )
        df = db.execute_select(query)
        df.drop(
            [
                "date_inserted",
                "inserted_by",
                "last_updated",
                "last_updated_by",
                "last_updated_action",
            ],
            axis=1,
            inplace=True,
        )

        df["mean"] = df["mean"].fillna((df.upper + df.lower) / 2.0)
        df["std"] = df["std"].fillna(inf)

        return df

    def get_integrand_data(self, input_data, integrand):
        """subset input_data dataframe to just that integrand"""
        return input_data[input_data.integrand == integrand]

    def assign_eta(self):
        """Assign eta based on some logical rules..."""

        model_param_meta = self.model_params
        data = self.data

        db_etas = model_param_meta[model_param_meta.parameter_type_id == 13]

        eta_priors = []
        for integrand in self.integrand_pred:
            if integrand in db_etas.measure.values:
                # check if eta has a value for this integrand in
                # epi.model_parameters
                eta = db_etas.loc[db_etas.measure == integrand, "mean"].values[0]
            elif integrand in data.integrand.values:
                # otherwise check if there is any data for this integrand
                if len(data[(data.integrand == integrand) & (data.meas_value != 0)]) > 0:
                    eta = (
                        1e-2
                        * data.loc[
                            ((data.integrand == integrand) & (data.meas_value != 0)),
                            "meas_value",
                        ].median()
                    )
                else:
                    eta = 1e-5
            else:
                # otherwise use a default value
                eta = 1e-5

            eta = "{:.5g}".format(eta)
            eta_priors.append(eta)

        eta_df = pd.DataFrame({"integrand": self.integrand_pred, "eta": eta_priors})
        return eta_df

    def get_age_mesh(self, use_default=False):
        """Return a dataframe of age_groups and age_group metadata.

        Args:
            use_default(bool, False): If False, use model version's metdata to
                create age mesh. If True, use default mortality age grid
        default age mesh"""
        # parent age grid
        if use_default:
            age_group_ids = self.demographics.mortality_age_grid
            age_group_ids = [str(a) for a in age_group_ids]
            age_group_id_str = ",".join(age_group_ids)
            query = """
                SELECT * FROM shared.age_group
                WHERE age_group_id IN (%s) """ % (
                age_group_id_str
            )
            age_mesh = db.execute_select(query)
            age_mesh.rename(
                columns={
                    "age_group_years_start": "age_start",
                    "age_group_years_end": "age_end",
                },
                inplace=True,
            )
            age_mesh["age_end"] = age_mesh.age_end.clip(upper=100)
            age_mesh = age_mesh.sort_values("age_start")
            age_mesh = (age_mesh["age_start"] + age_mesh["age_end"]) / 2.0
        else:
            age_mesh = self.model_version_meta.age_mesh
            age_mesh = age_mesh.values[0].split(" ")
        age_mesh = ["{:.5g}".format(float(a)) for a in age_mesh]

        return age_mesh

    def get_integrand_bounds(self):
        """Return a dataframe of integrand bounds, based on model version
        metadata and parameters
        """
        model_version_meta = self.model_version_meta
        mpm = self.model_params

        # ETA STUFF
        integrand_df = self.assign_eta()
        integrand_df["min_cv_world2sup"] = model_version_meta["cv_global"].values[0]
        integrand_df["min_cv_sup2reg"] = model_version_meta["cv_super"].values[0]
        integrand_df["min_cv_reg2sub"] = model_version_meta["cv_region"].values[0]

        if "cv_subreg" in model_version_meta.columns:
            integrand_df["min_cv_sub2atom"] = model_version_meta["cv_subreg"].values[0]
        else:
            integrand_df["min_cv_sub2atom"] = model_version_meta["cv_region"].values[0]

        age_mesh = self.get_age_mesh(use_default=True)
        age_mesh = " ".join(age_mesh)

        integrand_df = propagate_mincv_bounds(mpm, integrand_df)

        integrand_df = integrand_df.replace({0: 0.01})
        integrand_df["parent_age_grid"] = age_mesh

        return integrand_df

    def attach_country_covariate(
        self, covariate_id, integrand, asdr_cause_id=None, model_version_id=None
    ):
        """
        Add study covariate column to self.data. Possibly transform covariate
        depending on model parameters. If asdr_cause_id not none, merge
        on log transformed cause specific age standardized rate as a covariate.

        Will take simple average of covariates to collapse to location/year/sex
        specific values
        """
        log = logging.getLogger(__name__)
        if asdr_cause_id is not None:
            cc_vid = self.model_version_meta.csmr_cod_output_version_id.squeeze()
            covname = "lnasdr_%s" % asdr_cause_id
            colname = "raw_c_{}".format(covname)
            covdata = ln_asdr.get_lnasdr_from_db(
                cause_id=int(asdr_cause_id),
                cod_output_version_id=cc_vid,
                model_version_id=self.mvid,
            ).rename(columns={colname: "mean_value"}, errors="raise")
            log.info("Adding ASDR column {}".format(colname))
        else:
            covnq = """
                SELECT covariate_name_short FROM shared.covariate
                WHERE covariate_id=%s """ % (
                covariate_id
            )
            covname = db.execute_select(covnq).values[0][0]
            log.info("Adding country covariate column {}".format(covname))
            if self.gbd_round_id <= 7:
                get_cov_kwargs = {"status": "best"}
            else:
                if model_version_id is None:
                    raise RuntimeError(
                        "Must provide model_version_id for post-gbd-2021 covariates."
                    )
                get_cov_kwargs = {"model_version_id": int(model_version_id)}
            covdata = get_covariate_estimates(
                covariate_id=int(covariate_id), release_id=self.release_id, **get_cov_kwargs
            )
            covdata = covdata[
                ["location_id", "year_id", "age_group_id", "sex_id", "mean_value"]
            ]

            transform_type_id = self.model_params[
                (self.model_params.country_covariate_id == covariate_id)
                & (self.model_params.measure == integrand)
            ]["transform_type_id"].squeeze()

            if transform_type_id == 1:
                covdata["mean_value"] = np.log(covdata.mean_value)
            elif transform_type_id == 2:
                covdata["mean_value"] = np.log(covdata.mean_value / (1 - covdata.mean_value))
            elif transform_type_id == 3:
                covdata["mean_value"] = covdata.mean_value**2
            elif transform_type_id == 4:
                covdata["mean_value"] = np.sqrt(covdata.mean_value)
            elif transform_type_id == 5:
                covdata["mean_value"] = covdata.mean_value * 1000

            colname = "raw_c_{}_{}".format(int(transform_type_id), covname)

        lsvid = self.model_version_meta.location_set_version_id.values[0]
        lt = loctree(
            location_set_id=self.demographics.location_set_id,
            location_set_version_id=lsvid,
            release_id=self.release_id,
        )
        leaves = [l.id for l in lt.leaves()]
        covdata = covdata[covdata.location_id.isin(leaves)]

        # Per implementation, take simple averages to collapse to
        # country-year level estimates
        if 3 not in covdata.sex_id.unique():
            bothdata = (
                covdata[["location_id", "year_id", "mean_value"]]
                .groupby(["location_id", "year_id"])
                .mean()
                .reset_index()
            )
            bothdata["sex_id"] = 3
            covdata = covdata.append(bothdata)
        for s in [1, 2]:
            if s not in covdata.sex_id.unique():
                sexdata = covdata[covdata.sex_id == 3]
                sexdata["sex_id"] = s
                covdata = covdata.append(sexdata)

        covdata.rename(columns={"mean_value": colname}, inplace=True)
        covdata["x_sex"] = covdata.sex_id.replace({1: 0.5, 2: -0.5, 3: 0})
        covdata = covdata[["location_id", "year_id", "x_sex", colname]]

        # Collapse to location-year-sex level (simple averaging)
        covdata = covdata.groupby(["location_id", "year_id", "x_sex"], as_index=False).mean()
        if colname not in self.data.columns:
            self.data = self.data.merge(
                covdata, on=["location_id", "year_id", "x_sex"], how="left"
            )
        if len(self.covariate_data) == 0:
            self.covariate_data = covdata
        else:
            if colname not in self.covariate_data.columns:
                self.covariate_data = self.covariate_data.merge(
                    covdata, on=["location_id", "year_id", "x_sex"]
                )

        return covname, covdata

    def get_effect_priors(self):
        """Try to apply the same defaults as ...

        ----- UNMEASURED INTEGRAND DEFAULTS ----
        For "unmeasured" integrands (i.e. integrands where there is no
        data), create a random effect at every location level (subreg,
        region, super) with a gamma prior:
            defaults = {
                'integrand' : <<missing_integrand_name>>,
                'effect'    : 'gamma',
                'name'      : <<location_level>>,
                'lower'     : 1.0,
                'upper'     : 1.0,
                'mean'      : 1.0,
                'std'       : inf }
        ----------------------------------------

        ----- MEASURED INTEGRAND DEFAULTS ------
        For gamma (effect) priors on location (name), pull from
        model_parameters table where possible (parameter_type_ids 14,15,16).
        Otherwise, use the following gamma defaults for all measured
        integrands:
            defaults = {
                'integrand' : <<measured_integrand_name>>,
                'effec'    : 'gamma',
                'name'      : <<location_level>>,
                'lower'     : 1.0,
                'upper'     : 1.0,
                'mean'      : 1.0,
                'std'       : inf }

        For location random effect priors, which only apply to the
        locations in the dataset (and their respective parent
        locations), use the following defaults when not specified in the
        database:
            defaults = {
                'integrand' : <<measured_integrand_name not mtall>>,
                'effect'    : <<location level (super, region, or subreg)>>,
                'name'      : <<location_name>>,
                'lower'     : -2,
                'upper'     : 2,
                'mean'      : 0,
                'std'       : 1 }

        For beta sex prior, which applies to all measured integrands
        except for mtspecific, use the following defaults:
            defaults = {
                'integrand' : <<measured_integrand_name not mtspecific>>
                'effect'    : 'beta'
                'name'      : 'x_sex'
                'lower'     : -2.0
                'upper'     : 2.0
                'mean'      : 0.0
                'std'       : inf }

        For zeta prior, if not set in the model_parameters table, use
        the following defaults:
            defaults = {
                'integrand' : <<measured_integrand>>
                'effect'    : 'zeta'
                'name'      : 'a_local'
                'lower'     : 0.0
                'upper'     : 0.5
                'mean'      : 0.0
                'std'       : inf }

        For other beta priors (study covariates, country covariates,
        asdr covariates), there are no defaults. Just use whatever is
        listed in the DB. Note that study covariates can also be
        zeta priors (in which case they are listed as study_zcov, as
        opposed to study_xcov)
        """

        log = logging.getLogger(__name__)
        mpm = self.model_params
        if self.single_param is None:
            mis = [m for m in self.measured_integrands() if m not in ["mtall", "mtother"]]
        else:
            mis = ["mtother"]
        mlsets = self.measured_locations()

        #####################################################
        # DEFAULTS
        #####################################################
        # Set location gamma defaults
        ep = []
        for integrand in self.integrand_pred:
            for loc_lvl in ["super", "region", "subreg"]:
                defaults = {
                    "integrand": integrand,
                    "effect": "gamma",
                    "name": loc_lvl,
                    "lower": 1.0,
                    "upper": 1.0,
                    "mean": 1.0,
                    "std": inf,
                }
                ep.append(defaults)

        # Location random effects
        for integrand in self.integrand_pred:
            for loc_lvl, mls in mlsets.items():
                if loc_lvl == "atom":
                    pass
                else:
                    for n in ["none", "cycle"]:
                        cyc_none = {
                            "integrand": integrand,
                            "effect": loc_lvl,
                            "name": n,
                            "lower": 0,
                            "upper": 0,
                            "mean": 0,
                            "std": inf,
                        }
                        ep.append(cyc_none)
                    if integrand in self.integrand_pred:
                        for ml in mls:
                            add_csmr_cause = self.model_version_meta["add_csmr_cause"]
                            if (
                                (add_csmr_cause.isnull().values[0])
                                & (integrand == "mtspecific")
                                & (loc_lvl != "super")
                            ):
                                defaults = {
                                    "integrand": integrand,
                                    "effect": loc_lvl,
                                    "name": ml,
                                    "lower": 0,
                                    "upper": 0,
                                    "mean": 0,
                                    "std": inf,
                                }
                            else:
                                defaults = {
                                    "integrand": integrand,
                                    "effect": loc_lvl,
                                    "name": ml,
                                    "lower": -2,
                                    "upper": 2,
                                    "mean": 0,
                                    "std": 1,
                                }
                            ep.append(defaults)

        # Beta sex priors
        for integrand in self.integrand_pred:
            defaults = {
                "integrand": integrand,
                "effect": "beta",
                "name": "x_sex",
                "lower": -2.0,
                "upper": 2.0,
                "mean": 0.0,
                "std": inf,
            }
            ep.append(defaults)

        # Zeta priors
        for integrand in mis:
            defaults = {
                "integrand": integrand,
                "effect": "zeta",
                "name": "x_local",
                "lower": 0.0,
                "upper": 0.5,
                "mean": 0.0,
                "std": inf,
            }
            ep.append(defaults)

        #####################################################
        # DB SETTINGS
        #####################################################
        # Apply other priors as specified in the database
        """
        1   Value prior
        2   Slope prior
        5   Study-level covariate fixed effect on integrand value (x-cov)
        6   Study-level covariate fixed effect on integrand variance (z-cov)
        7   Country-level covariate fixed effect
        8   ln-ASDR covariate fixed effect
        9   Location random effect
        10  Smoothness (xi)
        11  Heterogeneity (zeta1)
        12  Offset in the log normal transformation of rate smoothing (kappa)
        13  Offset in the log normal transformation of data fitting (eta)
        14  Multiplier of standard deviation for country random effects (gamma)
        15  Multiplier of standard deviation for region random effects (gamma)
        16  Multiplier of standard deviation for super region random effects
            (gamma)
        17  Exclude data for parameter
        18  Exclude prior data
        19  Min CV by integrand
        """

        # Country covariates
        ccovs = mpm[mpm.parameter_type_id == 7]
        asdr_cause_ids = []
        if 8 in mpm.parameter_type_id.unique():
            for asdr_cause_id in self.model_params[self.model_params.parameter_type_id == 8][
                "asdr_cause"
            ].unique():
                asdr_cause_ids.append(asdr_cause_id)
            ccovs = ccovs.append(mpm[mpm.parameter_type_id == 8])

        for _, row in ccovs[ccovs.country_covariate_id.notnull()].iterrows():
            ccov_id = int(row["country_covariate_id"])
            integrand = row["measure"]
            transform = row["transform_type_id"]
            model_version_id = row["country_covariate_model_version_id"]

            ccov_name, ccov_data = self.attach_country_covariate(
                ccov_id, integrand=integrand, model_version_id=model_version_id
            )
            if integrand in self.integrand_pred:
                ccov = {
                    "integrand": integrand,
                    "effect": "beta",
                    "name": "x_c_{}_{}".format(int(transform), ccov_name),
                    "lower": row["lower"],
                    "upper": row["upper"],
                    "mean": row["mean"],
                    "std": inf,
                }
                ep.append(ccov)

            log.info("Setting effect prior for {}".format(ccov_name))

        for _, row in ccovs[ccovs.asdr_cause.isin(asdr_cause_ids)].iterrows():
            asdr_cause_id = row["asdr_cause"]
            integrand = row["measure"]

            ccov_name, ccov_data = self.attach_country_covariate(-1, integrand, asdr_cause_id)
            if integrand in self.integrand_pred:
                ccov = {
                    "integrand": integrand,
                    "effect": "beta",
                    "name": "x_c_{}".format(ccov_name),
                    "lower": row["lower"],
                    "upper": row["upper"],
                    "mean": row["mean"],
                    "std": inf,
                }
                ep.append(ccov)

            log.info("Setting effect prior for {}".format(ccov_name))

        # Study xcovs
        xcovs = mpm[mpm.parameter_type_id == 5]
        xcovs["mean"] = xcovs["mean"].fillna((xcovs.lower + xcovs.upper) / 2.0)
        if len(xcovs) > 0:
            for i, row in xcovs.iterrows():
                integrand = row["measure"]
                if integrand in self.integrand_pred:
                    name = "x_s_%s" % (row["study_covariate"])
                    xcov = {
                        "integrand": integrand,
                        "effect": "beta",
                        "lower": row["lower"],
                        "upper": row["upper"],
                        "mean": row["mean"],
                        "std": inf,
                    }
                    if name == "x_s_sex":
                        xcov["name"] = "x_sex"
                        ep = [
                            (
                                i
                                if not (
                                    i["name"] == "x_sex"
                                    and i["effect"] == "beta"
                                    and i["integrand"] == integrand
                                )
                                else xcov
                            )
                            for i in ep
                        ]
                    else:
                        xcov["name"] = name
                        ep.append(xcov)

                    if name not in self.data.columns:
                        self.data[name] = 0

        # Study zcovs
        zcovs = mpm[mpm.parameter_type_id == 6]
        zcovs["mean"] = zcovs["mean"].fillna((zcovs.lower + zcovs.upper) / 2.0)
        if len(zcovs) > 0:
            for _, row in zcovs.iterrows():
                integrand = row["measure"]
                if integrand in self.integrand_pred:
                    integrand = row["measure"]
                    name = "x_s_%s" % (row["study_covariate"])
                    zcov = {
                        "integrand": integrand,
                        "effect": "zeta",
                        "name": name,
                        "lower": row["lower"],
                        "upper": row["upper"],
                        "mean": row["mean"],
                        "std": inf,
                    }
                    if name == "x_s_sex":
                        zcov["name"] = "x_sex"
                    else:
                        zcov["name"] = name
                    ep.append(zcov)

                    if name not in self.data.columns:
                        self.data[name] = 0

        ep = pd.DataFrame(ep)
        self.study_covariates = list(self.data.filter(like="x_s_").columns)

        # Override location random effects
        loc_res = mpm[mpm.parameter_type_id == 9]
        loc_res["location_id"] = loc_res.location_id.astype("float")
        for _, row in loc_res.iterrows():
            integrand = row["measure"]
            conditions = (
                (ep.integrand == integrand)
                & (~ep.name.isin(["none", "cycle"]))
                & (ep.effect.isin(["subreg", "region", "super"]))
            )
            if math.isnan(row["location_id"]):
                this_conditions = conditions
            else:
                location_id = "{:.0f}".format(int(row["location_id"]))
                this_conditions = conditions & (ep.name == location_id)
            ep.loc[this_conditions, "mean"] = row["mean"]
            ep.loc[this_conditions, "lower"] = row["lower"]
            ep.loc[this_conditions, "upper"] = row["upper"]
            ep.loc[this_conditions, "std"] = row["std"]

        # Override zeta
        zeta1s = mpm[mpm.parameter_type_id == 11]
        zeta1s["mean"] = zeta1s["mean"].fillna((zeta1s.lower + zeta1s.upper) / 2.0)
        for _, row in zeta1s.iterrows():
            integrand = row["measure"]
            conditions = (ep.integrand == integrand) & (ep.name == "x_local")
            ep.loc[conditions, "mean"] = row["mean"]
            ep.loc[conditions, "lower"] = row["lower"]
            ep.loc[conditions, "upper"] = row["upper"]

        # Override location gamma defaults if specified in DB
        subreg_gamma = mpm[(mpm.parameter_type_id == 14) & (mpm.cascade_level_id == 4)]
        reg_gamma = mpm[(mpm.parameter_type_id == 14) & (mpm.cascade_level_id == 3)]
        super_gamma = mpm[(mpm.parameter_type_id == 14) & (mpm.cascade_level_id == 2)]
        for val in ["mean", "lower", "upper"]:
            for _, row in subreg_gamma.iterrows():
                conditions = (
                    (ep.integrand == row["measure"])
                    & (ep.effect == "gamma")
                    & (ep.name.isin(["subreg"]))
                )
                if row["measure"] in self.integrand_pred:
                    ep.loc[conditions, val] = subreg_gamma[val].values[0]
            for _, row in reg_gamma.iterrows():
                conditions = (
                    (ep.integrand == row["measure"])
                    & (ep.effect == "gamma")
                    & (ep.name.isin(["region"]))
                )
                if row["measure"] in self.integrand_pred:
                    ep.loc[conditions, val] = reg_gamma[val].values[0]
            for _, row in super_gamma.iterrows():
                conditions = (
                    (ep.integrand == row["measure"])
                    & (ep.effect == "gamma")
                    & (ep.name.isin(["super"]))
                )
                if row["measure"] in self.integrand_pred:
                    ep.loc[conditions, val] = super_gamma[val].values[0]

        lmap = gen_locmap(
            self.model_version_meta.location_set_version_id.values[0], self.release_id
        )
        if len(self.covariate_data) > 0:
            ccovs = list(
                set(self.covariate_data.columns) - set(["location_id", "year_id", "x_sex"])
            )
            self.covariate_data = self.covariate_data.merge(
                lmap, on="location_id", how="left"
            )

            subregcovs = (
                self.covariate_data[self.covariate_data.atom != "none"]
                .groupby(["subreg", "year_id", "x_sex"])[ccovs]
                .mean()
                .reset_index()
            )
            subregcovs.rename(columns={"subreg": "location_id"}, inplace=True)
            regcovs = (
                self.covariate_data.groupby(["region", "year_id", "x_sex"])[ccovs]
                .mean()
                .reset_index()
            )
            regcovs.rename(columns={"region": "location_id"}, inplace=True)
            supcovs = (
                self.covariate_data.groupby(["super", "year_id", "x_sex"])[ccovs]
                .mean()
                .reset_index()
            )
            supcovs.rename(columns={"super": "location_id"}, inplace=True)
            gcovs = (
                self.covariate_data.groupby(["year_id", "x_sex"])[ccovs].mean().reset_index()
            )
            gcovs["location_id"] = "1"
            self.covariate_data = pd.concat(
                [self.covariate_data, subregcovs, regcovs, supcovs, gcovs]
            )
            self.covariate_data["location_id"] = self.covariate_data.location_id.astype(int)

        return ep

    def get_rate_prior(self):
        """
        If not provided in the database, use the following defaults:
            d<<param>> = {
                'age'   : <<any age_start except the terminal age_start>>,
                'lower' : -inf,
                'upper' : inf,
                'mean'  : 0.0,
                'std'   : inf }

            chi = {
                'age'   : <<any age_start except the terminal age_start>>,
                'lower' : 0.0,
                'upper' : 5.0, ?? changed to 1.0
                'mean'  : 2.5, ?? changed to 0.5
                'std'   : inf })

            iota, omega, rho = {
                'age'   : <<any age_start except the terminal age_start>>,
                'lower' : 0.0,
                'upper' : 1.0,
                'mean'  : 0.5,
                'std'   : inf }

        Mapping from integrand names to rates
            integrand2rate = {
                'incidence' : 'iota' ,
                'remission' : 'rho'  ,
                'mtexcess'  : 'chi'  ,
                'mtother'   : 'omega'
            }

        This implementation ignores the crazy hacks for adjusting
        values to satisfy slope constraints, as I don't understand
        what these hacks are doing... If it is appropriate to do this,
        this section should at least throw warnings or exceptions.
        """
        # Generate default frame
        # ages = [0, 0.5, 1, 5] + range(10, 101, 10)
        ages_num = [float(a) for a in self.age_mesh]
        ages_str = self.age_mesh

        slope_defaults = pd.DataFrame(
            {"age": ages_str, "lower": -inf, "upper": inf, "mean": 0.0, "std": inf}
        )
        slope_defaults = slope_defaults[:-1]

        val_defaults = {}
        val_defaults["iota"] = pd.DataFrame(
            {"age": ages_str, "lower": 0.0, "upper": 10.0, "mean": 5.0, "std": inf}
        )
        val_defaults["rho"] = pd.DataFrame(
            {"age": ages_str, "lower": 0.0, "upper": 10.0, "mean": 5.0, "std": inf}
        )
        val_defaults["chi"] = pd.DataFrame(
            {"age": ages_str, "lower": 0.0, "upper": 10.0, "mean": 5.0, "std": inf}
        )
        if self.single_param in ["proportion", "prevalence"]:
            val_defaults["omega"] = pd.DataFrame(
                {"age": ages_str, "lower": 0.0, "upper": 1.0, "mean": 0.5, "std": inf}
            )
        elif self.single_param == "continuous":
            val_defaults["omega"] = pd.DataFrame(
                {"age": ages_str, "lower": 0.0, "upper": inf, "mean": 1, "std": inf}
            )
        else:
            val_defaults["omega"] = pd.DataFrame(
                {"age": ages_str, "lower": 0.0, "upper": 10.0, "mean": 5.0, "std": inf}
            )

        rp_df = []
        for t in ["iota", "rho", "chi", "omega"]:
            type_slope = slope_defaults.copy()
            type_slope["type"] = "d" + t
            rp_df.append(type_slope)
            type_val = val_defaults[t].copy()
            type_val["type"] = t
            rp_df.append(type_val)
        rp_df = pd.concat(rp_df)

        # Override defaults if specified in the database
        mpm = self.model_params

        val_pri = mpm[mpm.parameter_type_id == 1]
        if len(val_pri) > 0:
            val_pri["mean"] = val_pri["mean"].fillna((val_pri.lower + val_pri.upper) / 2.0)
            val_pri["lower"] = val_pri.lower.fillna(val_pri["mean"])
            val_pri["upper"] = val_pri.upper.fillna(val_pri["mean"])

        slope_pri = mpm[mpm.parameter_type_id == 2]
        if self.single_param is not None:
            val_pri["measure"] = "mtother"
            slope_pri["measure"] = "mtother"
        if len(slope_pri) > 0:
            slope_pri["mean"] = slope_pri["mean"].fillna(
                (slope_pri.lower + slope_pri.upper) / 2.0
            )
            slope_pri["lower"] = slope_pri.lower.fillna(slope_pri["mean"])
            slope_pri["upper"] = slope_pri.upper.fillna(slope_pri["mean"])

        rp_df["age"] = rp_df.age.astype("float")
        for integrand, rate in integrand_rate_map.items():
            ivp = val_pri[val_pri.measure == integrand]
            isp = slope_pri[slope_pri.measure == integrand]
            for a in ages_num:
                ivp_mlu = ivp.loc[
                    (ivp.age_start <= a) & (ivp.age_end >= a), ["mean", "lower", "upper"]
                ]
                isp_m = isp.loc[(isp.age_start <= a) & (isp.age_end >= a), "mean"]

                if len(ivp_mlu) > 0:
                    rp_df.loc[(rp_df.type == rate) & (rp_df.age == a), "lower"] = ivp_mlu[
                        "lower"
                    ].values[0]
                    rp_df.loc[(rp_df.type == rate) & (rp_df.age == a), "upper"] = ivp_mlu[
                        "upper"
                    ].values[0]
                    rp_df.loc[(rp_df.type == rate) & (rp_df.age == a), "mean"] = ivp_mlu[
                        "mean"
                    ].values[0]
                if len(isp_m) > 0:
                    if isp_m.values[0] == 1:
                        rp_df.loc[(rp_df.type == "d" + rate) & (rp_df.age == a), "lower"] = 0
                        rp_df.loc[(rp_df.type == "d" + rate) & (rp_df.age == a), "upper"] = (
                            inf
                        )
                    elif isp_m.values[0] == -1:
                        rp_df.loc[(rp_df.type == "d" + rate) & (rp_df.age == a), "lower"] = (
                            -inf
                        )
                        rp_df.loc[(rp_df.type == "d" + rate) & (rp_df.age == a), "upper"] = 0
                    else:
                        rp_df.loc[(rp_df.type == "d" + rate) & (rp_df.age == a), "lower"] = (
                            -inf
                        )
                        rp_df.loc[(rp_df.type == "d" + rate) & (rp_df.age == a), "upper"] = (
                            inf
                        )

        # Enforce slope priors
        for _, rate in integrand_rate_map.items():
            for i, a in enumerate(ages_num[:-1]):
                next_age = ages_num[i + 1]
                this_mean = rp_df.loc[(rp_df.type == rate) & (rp_df.age == a), "mean"].values[
                    0
                ]
                next_mean = rp_df.loc[
                    (rp_df.type == rate) & (rp_df.age == next_age), "mean"
                ].values[0]

                if (
                    rp_df.loc[(rp_df.type == "d" + rate) & (rp_df.age == a), "lower"].values[
                        0
                    ]
                    == 0
                ):
                    next_mean = np.clip(next_mean, a_min=this_mean, a_max=inf)
                elif (
                    rp_df.loc[(rp_df.type == "d" + rate) & (rp_df.age == a), "upper"].values[
                        0
                    ]
                    == 0
                ):
                    next_mean = np.clip(next_mean, a_min=-inf, a_max=this_mean)

                rp_df.loc[(rp_df.type == rate) & (rp_df.age == next_age), "mean"] = next_mean

        # If this is a single parameter model, set all rates
        # except omega to zero
        if self.single_param is not None:
            rp_df.loc[~rp_df.type.isin(["omega", "domega"]), "mean"] = 0
            rp_df.loc[~rp_df.type.isin(["omega", "domega"]), "lower"] = 0
            rp_df.loc[~rp_df.type.isin(["omega", "domega"]), "upper"] = 0
            rp_df.loc[~rp_df.type.isin(["omega", "domega"]), "std"] = inf

            if self.single_param == "proportion":
                o_low = rp_df.loc[rp_df.type == "omega", "lower"]
                o_upp = rp_df.loc[rp_df.type == "omega", "upper"]
                rp_df.loc[rp_df.type == "omega", "lower"] = o_low.clip(lower=0)
                rp_df.loc[rp_df.type == "omega", "upper"] = o_upp.clip(upper=1)

        return rp_df

    def get_simple_prior(self):
        """
        Generate the simple smoothness priors. Use the integrand to
        greek map in the rate prior to map the database 'smoothness'
        overrides. Note that there is some lost in
        translationism going on with the 'prevalence' integrand.
        This maps to 'p_zero' in this simple_prior. The rest map to
        xi_<<greek_letter>> as expected.
        """
        def_p_zero = pd.DataFrame(
            [{"name": "p_zero", "lower": 0, "upper": 0, "mean": 0, "std": inf}]
        )

        if self.single_param is not None:
            def_omega = pd.DataFrame(
                [{"name": "xi_omega", "lower": 0.3, "upper": 0.3, "mean": 0.3, "std": inf}]
            )
        else:
            def_omega = pd.DataFrame(
                [{"name": "xi_omega", "lower": 1, "upper": 1, "mean": 1, "std": inf}]
            )

        def_other = pd.DataFrame(
            {
                "name": ["xi_iota", "xi_rho", "xi_chi"],
                "lower": 0.3,
                "upper": 0.3,
                "mean": 0.3,
                "std": inf,
            }
        )

        sp = pd.concat([def_p_zero, def_omega, def_other])

        mpm = self.model_params
        smooth_df = mpm[mpm.parameter_type_id == 10]
        if self.single_param is not None:
            smooth_df["measure"] = "mtother"
        for integrand in list(integrand_rate_map.keys()):
            if integrand in smooth_df.measure.values:
                lower = smooth_df.loc[smooth_df.measure == integrand, "lower"].values[0]
                upper = smooth_df.loc[smooth_df.measure == integrand, "upper"].values[0]
                mean = (lower + upper) / 2.0
                rate = integrand_rate_map[integrand]

                sp.loc[sp.name == "xi_" + rate, "lower"] = lower
                sp.loc[sp.name == "xi_" + rate, "upper"] = upper
                sp.loc[sp.name == "xi_" + rate, "mean"] = mean

        if self.model_version_meta["birth_prev"].values[0] == 1:
            sp.loc[sp.name == "p_zero", "lower"] = 0
            sp.loc[sp.name == "p_zero", "upper"] = 1
            sp.loc[sp.name == "p_zero", "mean"] = 0.01

        return sp[["name", "lower", "upper", "mean", "std"]]

    def get_value_prior(self):
        """Most come from user inputs (I think), and the kappas
        should come from the integrand file"""
        etas = self.assign_eta()
        mvm = self.model_version_meta

        value_params = {}
        for integrand, rate in integrand_rate_map.items():
            if integrand in etas.integrand.values:
                kappa = etas.loc[etas.integrand == integrand, "eta"].values[0]
                value_params["kappa_" + rate] = kappa
            else:
                value_params["kappa_" + rate] = 1e-5
        value_params["kappa_omega"] = 1e-5

        for _, eta_row in etas.iterrows():
            value_params["eta_%s" % eta_row["integrand"]] = eta_row["eta"]

        value_params["sample_interval"] = mvm.sample_interval.values[0]
        value_params["num_sample"] = mvm.num_sample.values[0]
        value_params["integrate_step"] = mvm.integrate_step.values[0]
        value_params["random_seed"] = mvm.random_seed.values[0]
        value_params["integrate_method"] = mvm.integration_type.values[0]
        value_params["integrate_method"] = (
            str(value_params["integrate_method"]).lower().replace(" ", "_")
        )
        # value_params['condition'] = mvm.sequela_name.values[0]
        value_params["watch_interval"] = value_params["num_sample"] / 100
        value_params["data_like"] = "in_data_file"
        value_params["prior_like"] = likelihood_map[
            self.model_version_meta.prior_likelihood.values[0]
        ]

        value_params = pd.DataFrame(
            {"name": list(value_params.keys()), "value": list(value_params.values())}
        )

        return value_params

    def get_study_cov_ids(self):
        """Get dataframe mapping study covariate ID to study_covariate short
        string
        """
        query = """
            SELECT study_covariate_id, study_covariate
            FROM epi.study_covariate"""
        df = db.execute_select(query, "epi")
        return df

    def get_country_cov_ids(self):
        """Get dataframe mapping study covariate ID to covariate_name_short
        string
        """
        query = """
            SELECT covariate_id, covariate_name_short
            FROM shared.covariate"""
        df = db.execute_select(query, "epi")
        return df

    def get_measure_ids(self):
        """Get dataframe mapping measure_id to measure"""
        query = """
            SELECT measure_id, measure
            FROM shared.measure"""
        df = db.execute_select(query, "epi")
        return df

    def get_age_weights(self):
        """Get dataframe mapping age group ID to age standardization weights
        for a given release.
        """
        df = get_age_weights(release_id=self.release_id)
        df = df.rename(columns={"age_group_weight_value": "weight"})

        return df


def propagate_mincv_bounds(mpm, df):
    """
    For each integrand, if there is an integrand specific global->SR
    min cv setting, override the general global->SR setting

    Repeat that for global->SR down to subregion->atom, for each integrand.


    Arguments:
        mpm(pandas.dataframe): model version parameter dataframe,
            from (or Importer.model_params).
            This contains the integrand specific cv settings, if
            any.

        df(pandas.dataframe): dataframe of all integrands with
            general mincv settings. Columns are
            ['integrand', 'min_cv_world2sup', 'min_cv_sup2reg',
            'min_cv_reg2sub', 'min_cv_sub2atom']

    Returns:
        df, with possibly updated cv values
    """
    MIN_CV_ID = 19

    lvl_map = {
        1: "min_cv_world2sup",
        2: "min_cv_sup2reg",
        3: "min_cv_reg2sub",
        4: "min_cv_sub2atom",
    }

    mpm = _standardize_lvl_5_mincv(mpm.copy())
    for integrand in df.integrand:
        integrand_mask = df.integrand == integrand
        for lvl in sorted(lvl_map.keys()):
            min_cv_override = mpm[
                (mpm.parameter_type_id == MIN_CV_ID)
                & (mpm.cascade_level_id == lvl)
                & (mpm.measure == integrand)
            ]
            if min_cv_override.empty:
                # Case 1
                # no integrand specific setting specified
                continue

            if len(min_cv_override) > 1:
                # Case 2
                # More than one setting; this should never happen
                raise RuntimeError(
                    (
                        "More than one integrand specific min cv setting"
                        "found {}".format(min_cv_override.to_dict("list"))
                    )
                )

            # Case 3
            # need to update this loc lvl min cv setting
            min_cv_override = min_cv_override["mean"].iat[0]
            lvl_label = lvl_map[lvl]
            df.loc[integrand_mask, [lvl_label]] = min_cv_override
    return df


def _standardize_lvl_5_mincv(mpm):
    """
    The min cv settings allow for global down to subnational,
    but because each level affects the priors of the level below,
    subnational should be invalid. The requested behavior is for
    a subnational setting to be treated as synonymous
    with country level.

    This function mutates the mpm dataframe of model parameters
    by looking for subnational mincv settings and labeling them
    as national. If national settings already exist and they disagree
    with the subnational, it raises a RuntimeError.

    Inputs:
        mpm(pandas.dataframe): model level parameters, from
        importer.model_params

    Returns:
        mpm, possibly mutated
    """
    subnat_level_id = 5
    nat_level_id = 4
    MIN_CV_ID = 19
    subnat_mincv_mask = (mpm.parameter_type_id == MIN_CV_ID) & (
        mpm.cascade_level_id == subnat_level_id
    )
    subnat_mincv_settings = mpm[subnat_mincv_mask]

    if subnat_mincv_settings.empty:
        return mpm

    # check for conflicting nat/subnat settings
    for integrand in subnat_mincv_settings.measure:
        nat_mincv_settings = mpm[
            (mpm.parameter_type_id == MIN_CV_ID)
            & (mpm.cascade_level_id == nat_level_id)
            & (mpm.measure == integrand)
        ]
        if nat_mincv_settings.empty:
            continue

        subnat_value = subnat_mincv_settings.query("measure == @integrand")["mean"].iat[0]
        nat_value = nat_mincv_settings["mean"].iat[0]
        if subnat_value != nat_value:
            err_msg = (
                "Got conflicting national and subnational mincv "
                "settings for integrand {}. National: {}; Subnational: "
                "{}. The integrand-specific mincv settings affect the "
                "priors of the location level below the location "
                "specified in the dropdown, so subnational is an "
                "invalid option. The cascade treats the subnational "
                "option as synonymous with the national option, but "
                "in this case the two settings disagree.".format(
                    integrand, str(nat_value), str(subnat_value)
                )
            )
            raise RuntimeError(err_msg)

    # assign subnat settings to national, now that we know that either
    # national was not specified, or they are the same value.
    mpm.loc[subnat_mincv_mask, ["cascade_level_id"]] = nat_level_id

    # we could have introduced duplicate rows if nat and subnat
    # settings exist for an integrand, and they are equal to each
    # other
    mpm = mpm.drop_duplicates()
    return mpm


def recalc_SE(meas_value, standard_error, sample_size, lower_ci, upper_ci):
    """
    Applied to every row of input data. Will recalculate standard
    error if SE not provided.

    Returns:
        Float

    Raises:
        ValueError if not enogh information to calculate standard error
    """
    # First prefer standard deviation
    if standard_error is not None and standard_error > 0.0:
        return standard_error

    # next use sample_size
    if sample_size is not None and sample_size > 0.0:
        counts = meas_value * sample_size
        # When the number of counts is greater than or equal 5, use
        # standard deviation corresponding to counts is Possion distributed
        # Note that when counts = 5.0 meas_value is 5.0 / sample_size
        std_5 = math.sqrt((5.0 / sample_size) / sample_size)
        if counts >= 5.0:
            return math.sqrt(meas_value / sample_size)
        # Standard deviation for binomial with measure zero is about
        std_0 = 1.0 / sample_size
        # Linear interpolate between value std_5 at counts equal 5
        # and std_0 at counts equal 0
        return ((5.0 - counts) * std_0 + counts * std_5) / 5.0

    # Last choice, interpret 95% confidence limits using normal
    # distribution
    if upper_ci is not None and lower_ci is not None:
        return (upper_ci - lower_ci) / (2.0 * 1.96)

    # Something went wrong if you get this far...
    raise ValueError("Insufficient information to calculate standard error")


def rename_potential_overwrite_columns(
    df: pd.DataFrame, rename_columns: List[str]
) -> pd.DataFrame:
    """Rename columns that might be target of renames.

    CCHMD-14570 caused failure when crosswalk_version had column 'data_id'.
    """
    avoiding_overwriting_prefix = "foo_avoid_overwriting_"
    for overwrite_col in rename_columns:
        if overwrite_col in df.columns:
            df = df.rename(
                columns={overwrite_col: avoiding_overwriting_prefix + overwrite_col}
            )
    return df
