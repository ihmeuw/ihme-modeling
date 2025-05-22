#!

import itertools
import json
import logging
import os
import subprocess
import sys
import warnings
from copy import copy
from typing import List

import numpy as np
import pandas as pd
import pyarrow as pa
from scipy import interpolate, stats

import gbd.constants
from hierarchies.dbtrees import loctree

from cascade_ode.legacy import crosswalk as cw
from cascade_ode.legacy import importer, release, shared_functions
from cascade_ode.legacy.argument_parser import cascade_parser
from cascade_ode.legacy.constants import Methods
from cascade_ode.legacy.cv import enforce_hybrid_cv, enforce_min_cv
from cascade_ode.legacy.demographics import DemographicContext, Demographics
from cascade_ode.legacy.importer import IHME_COD_DB
from cascade_ode.legacy.io import datastore as datastore_module
from cascade_ode.legacy.settings import load as load_settings
from cascade_ode.lib import dismod_messages

inf = float("inf")

# Set default file mask to readable-for all users
os.umask(0o0002)


# Path to this file
this_path = os.path.dirname(os.path.abspath(__file__))

# Get configuration options
settings = load_settings()

cascade_levels = ["world", "super", "region", "subreg", "atom"]
cascade_level_ids = dict((k, v + 1) for v, k in enumerate(cascade_levels))

SEX_DICT = {
    gbd.constants.sex.MALE: 0.5,
    gbd.constants.sex.FEMALE: -0.5,
    gbd.constants.sex.BOTH: 0.0,
}
X_SEX_TO_ID = {v: k for (k, v) in SEX_DICT.items()}


def higher_levels(level):
    """return subset of cascade_levels list that contains all levels higher
    than provided level"""
    idx = cascade_levels.index(level)
    return cascade_levels[:idx]


def lower_levels(level):
    """return subset of cascade_levels list that contains all levels lower
    than provided level"""
    idx = cascade_levels.index(level)
    return cascade_levels[idx + 1 :]


class Cascade(object):
    def __init__(
        self,
        model_version_id,
        root_dir=os.path.expanduser(settings["cascade_ode_out_dir"]),
        reimport=True,
        version="tflem",
        cv_iter=0,
        feature_flags=None,
    ):
        """The Cascade object is responsible for holding cascade settings,
        data, and other metdaata for a particular model version id.

        Instantiating a cascade object causes it to read many csvs from the
        model's root directory. If any of these files are missing, it will
        trigger reimportation of all input data/settings via the importer
        object.

        Args:
            model_version_id(int): model version id for this dismod run
            root_dir(str): Path to directory that contains model_version_id
            reimport(bool, True): if True, run Importer to import data and
                write csv files. If False, read cached csvs instead.
            version(str, 'tflem'): One of 'tflem' or 'bbell'. Is bbell
                deprecated?
            cv_iter(int, 0): cross validation iteration. If cv_iter != 0
                and reimport = True, will hold out a portion of data
                (see self.holdout_data)
            feature_flags (SimpleNamespace): A way to turn features on and off.
        """
        if cv_iter == 0:
            self.root_dir = 
        else:
            self.root_dir = 

        try:
            os.makedirs(self.root_dir)
        except:
            pass
        try:
            os.chmod(self.root_dir, 0o775)
        except:
            pass

        self.model_version_id = model_version_id
        self.cv_iter = cv_iter

        if feature_flags is not None:
            self._feature_flags = feature_flags
        else:
            # This ensures all flags exist and take on their default values.
            self._feature_flags = cascade_parser().parse_args([])

        self.release_method = release.get_release_method_from_mv_and_flags(
            model_version_id, self._feature_flags
        )
        self.demographics = Demographics(model_version_id=model_version_id)

        if version == "bbell":
            self.value_file = 
            self.simple_file = 
            self.rate_file = 
            self.effect_file = 
            self.integrand_file = 
        elif version == "tflem":
            self.data_file = 
            self.value_file = 
            self.simple_file = 
            self.rate_file = 
            self.effect_file = 
            self.integrand_file = 
            self.model_version_file = 
            self.other_settings_file = 
            self.covariate_file = 
            self.ccov_map_file = 
            self.scov_map_file = 
            self.measure_map_file = 
            self.model_param_file = 
            self.age_weights_file = 

        required_files = [
            self.data_file,
            self.value_file,
            self.simple_file,
            self.rate_file,
            self.effect_file,
            self.integrand_file,
            self.model_version_file,
            self.other_settings_file,
            self.covariate_file,
            self.ccov_map_file,
            self.scov_map_file,
            self.model_param_file,
            self.measure_map_file,
            self.age_weights_file,
        ]

        for rf in required_files:
            if not os.path.isfile(rf):
                reimport = True

        if reimport:
            ii = importer.Importer(model_version_id=model_version_id)
            self.value_prior = ii.get_value_prior()
            self.simple_prior = ii.get_simple_prior()
            self.rate_prior = ii.get_rate_prior()
            self.effects_prior = ii.get_effect_priors()
            self.model_version_meta = ii.model_version_meta
            self.integrand_bounds = ii.get_integrand_bounds()
            self.age_mesh = ii.age_mesh
            self.default_age_mesh = ii.get_age_mesh(use_default=True)
            self.study_covariates = ii.study_covariates
            self.alldata = ii.data
            self.data = self.alldata.query('integrand != "mtall"')
            self.mtall = self.alldata.query('integrand == "mtall"')
            self.single_param = ii.single_param
            self.model_params = ii.model_params
            self.covdata = ii.covariate_data
            self.ccovs = [c.replace("raw_c_", "") for c in self.covdata if "raw_c_" in c]
            self.ccov_map = ii.get_country_cov_ids()
            self.scov_map = ii.get_study_cov_ids()
            self.measure_map = ii.get_measure_ids()
            self.age_weights = ii.get_age_weights()
            if len(self.covdata) > 0:
                self.covdata["location_id"] = self.covdata.location_id.astype(float).astype(
                    int
                )
                self.covdata["location_id"] = self.covdata.location_id.astype(str)

            with open(self.other_settings_file, "w") as outfile:
                json.dump(
                    {
                        "age_mesh": self.age_mesh,
                        "default_age_mesh": self.default_age_mesh,
                        "study_covariates": self.study_covariates,
                        "ccovs": self.ccovs,
                        "single_param": self.single_param,
                    },
                    outfile,
                )

            # Readjust variable names for cascade
            if version == "bbell":
                scovs = list(self.alldata.filter(like="x_s").columns) + ["x_sex", "x_local"]
                ccovs = list(self.alldata.filter(like="raw_c").columns)
                srenames = dict((s, "a" + s[1:]) for s in scovs)
                crenames = dict((c, "r" + c[3:]) for c in ccovs)
                self.alldata.rename(columns=srenames, inplace=True)
                self.alldata.rename(columns=crenames, inplace=True)
                self.effects_prior["name"] = self.effects_prior["name"].replace(srenames)
                self.effects_prior["name"] = self.effects_prior["name"].replace(crenames)
                self.effects_prior = self.effects_prior[
                    ~self.effects_prior["name"].isin(["cycle", "none"])
                ]
                self.value_prior = self.value_prior[
                    ~self.value_prior.name.str.startswith("eta")
                ]
                self.value_prior = self.value_prior[self.value_prior.name != "data_like"]
                self.value_prior = self.value_prior[self.value_prior.name != "prior_like"]
                self.alldata = self.alldata.fillna(0)
                self.integrand_bounds = self.integrand_bounds[
                    self.integrand_bounds.integrand != "mtother"
                ]

            # Holdout data for cross-validation runs
            if cv_iter != 0:
                self.alldata = self.holdout_data(self.alldata)

            self.alldata["meas_stdev"] = self.alldata.meas_stdev.clip(lower=0.00001)
            self.alldata = self.alldata.sort_values(
                [
                    "integrand",
                    "age_lower",
                    "age_upper",
                    "time_lower",
                    "time_upper",
                    "super",
                    "region",
                    "subreg",
                    "atom",
                    "x_sex",
                ]
            )
            self.alldata.to_csv(self.data_file, index=False)
            self.value_prior.to_csv(self.value_file, index=False)
            self.simple_prior.to_csv(self.simple_file, index=False)
            self.rate_prior.replace({-inf: "_inf"}).to_csv(self.rate_file, index=False)
            self.covdata.to_csv(self.covariate_file, index=False)
            self.model_params.to_csv(self.model_param_file, index=False)
            self.age_weights.to_csv(self.age_weights_file, index=False)

            self.effects_prior = self.effects_prior.sort_values(
                ["effect", "integrand", "name"]
            )
            self.effects_prior.to_csv(self.effect_file, index=False)
            self.model_version_meta.to_csv(self.model_version_file, index=False)

            self.ccov_map.to_csv(self.ccov_map_file, index=False)
            self.scov_map.to_csv(self.scov_map_file, index=False)
            self.measure_map.to_csv(self.measure_map_file, index=False)

            # Rename integrand bounds columns for more intuitive access
            self.integrand_bounds.rename(
                columns={
                    "min_cv_world2sup": "super",
                    "min_cv_sup2reg": "region",
                    "min_cv_reg2sub": "subreg",
                    "min_cv_sub2atom": "atom",
                },
                inplace=True,
            )
            self.integrand_bounds.to_csv(self.integrand_file, index=False)

        else:
            self.alldata = pd.read_csv(
                self.data_file,
                dtype={
                    "age_lower": str,
                    "age_upper": str,
                    "meas_stdev": str,
                    "meas_value": str,
                    "super": str,
                    "region": str,
                    "subreg": str,
                    "atom": str,
                },
            )
            self.alldata["age_lower"] = self.alldata.age_lower.fillna("nan")
            self.alldata["age_upper"] = self.alldata.age_upper.fillna("nan")
            self.alldata["meas_stdev"] = self.alldata.meas_stdev.fillna("nan")
            self.alldata["meas_value"] = self.alldata.meas_value.fillna("nan")

            self.value_prior = pd.read_csv(self.value_file)
            self.simple_prior = pd.read_csv(self.simple_file)
            self.rate_prior = pd.read_csv(self.rate_file)
            self.rate_prior.replace({"_inf": -inf}, inplace=True)
            self.effects_prior = pd.read_csv(self.effect_file)
            self.effects_prior = self.effects_prior.sort_values(
                ["effect", "integrand", "name"]
            )
            self.integrand_bounds = pd.read_csv(self.integrand_file)
            self.model_version_meta = pd.read_csv(self.model_version_file)
            self.ccov_map = pd.read_csv(self.ccov_map_file)
            self.scov_map = pd.read_csv(self.scov_map_file)
            self.measure_map = pd.read_csv(self.measure_map_file)
            self.model_params = pd.read_csv(self.model_param_file)
            self.age_weights = pd.read_csv(self.age_weights_file)

            try:
                self.covdata = pd.read_csv(self.covariate_file)
            except:
                self.covdata = pd.DataFrame()

            if len(self.covdata) > 0:
                self.covdata["location_id"] = self.covdata.location_id.astype(float).astype(
                    int
                )
                self.covdata["location_id"] = self.covdata.location_id.astype(str)

            # Figure out where these should get stored
            with open(self.other_settings_file) as f:
                other_settings = json.load(f)
            self.age_mesh = other_settings["age_mesh"]
            self.age_mesh = [str(s) for s in self.age_mesh]
            self.default_age_mesh = other_settings["default_age_mesh"]
            self.default_age_mesh = [str(s) for s in self.default_age_mesh]
            self.study_covariates = other_settings["study_covariates"]
            self.study_covariates = [str(s) for s in self.study_covariates]
            self.single_param = other_settings["single_param"]
            self.ccovs = other_settings["ccovs"]

            if self.single_param is None:
                self.data = self.alldata[self.alldata.integrand != "mtall"]
                self.mtall = self.alldata[self.alldata.integrand == "mtall"]
            else:
                self.data = self.alldata

        # Set likelihoods
        likelihood_map = {
            1: "gaussian",
            2: "laplace",
            3: "log_gaussian",
            4: "log_laplace",
            5: "log_gaussian",
        }
        self.data_likelihood = likelihood_map[
            self.model_version_meta.data_likelihood.values[0]
        ]
        self.prior_likelihood = likelihood_map[
            self.model_version_meta.prior_likelihood.values[0]
        ]

        # Format location ids to handle 'none's ...
        for c in ["atom", "subreg", "region", "super"]:
            if self.alldata.dtypes[c] != object:
                self.alldata[c] = self.alldata[c].apply(lambda x: "{:.0f}".format(float(x)))

        # Get location tree
        self.seed = self.model_version_meta.random_seed.values[0]
        self.loctree = loctree(
            location_set_version_id=self.model_version_meta.location_set_version_id.values[0],
            release_id=self.model_version_meta.release_id.values[0],
        )

        # Describe data coverage
        self.data_coverage = {}
        for integrand in self.data.integrand.unique():
            if integrand != "mtexcess":
                subdata = self.data[self.data.integrand == integrand]
                self.data_coverage[integrand] = {
                    "0.5": {
                        "super": subdata[subdata.x_sex == 0.5].super.unique(),
                        "region": subdata[subdata.x_sex == 0.5].region.unique(),
                        "subreg": subdata[subdata.x_sex == 0.5].subreg.unique(),
                        "atom": subdata[subdata.x_sex == 0.5].atom.unique(),
                    },
                    "-0.5": {
                        "super": subdata[subdata.x_sex == -0.5].super.unique(),
                        "region": subdata[subdata.x_sex == -0.5].region.unique(),
                        "subreg": subdata[subdata.x_sex == -0.5].subreg.unique(),
                        "atom": subdata[subdata.x_sex == -0.5].atom.unique(),
                    },
                    "0": {
                        "super": subdata.super.unique(),
                        "region": subdata.region.unique(),
                        "subreg": subdata.subreg.unique(),
                        "atom": subdata.atom.unique(),
                        "integrand": subdata.integrand.unique(),
                    },
                }
        self.data_coverage_bysex = {
            "0.5": {
                "super": self.data[self.data.x_sex == 0.5].super.unique(),
                "region": self.data[self.data.x_sex == 0.5].region.unique(),
                "subreg": self.data[self.data.x_sex == 0.5].subreg.unique(),
                "atom": self.data[self.data.x_sex == 0.5].atom.unique(),
            },
            "-0.5": {
                "super": self.data[self.data.x_sex == -0.5].super.unique(),
                "region": self.data[self.data.x_sex == -0.5].region.unique(),
                "subreg": self.data[self.data.x_sex == -0.5].subreg.unique(),
                "atom": self.data[self.data.x_sex == -0.5].atom.unique(),
            },
            "0": {
                "super": self.data.super.unique(),
                "region": self.data.region.unique(),
                "subreg": self.data.subreg.unique(),
                "atom": self.data.atom.unique(),
                "integrand": self.data.integrand.unique(),
            },
        }

    def holdout_data(self, data, holdout_prop=0.2):
        """For cross validation (ie cv_iter != 0). Marks some proportion of
        dataframe rows with hold_out=1.

        Args:
            data(pd.DataFrame): Dataframe of input data to add hold_out column.
            holdout_prop(float, 0.2): Fraction (0-1) of datframe to mark
                hold_out = 1

        Returns:
            pd.DataFrame
        """
        import math

        nids = data[data.nid.notnull()].nid.unique()
        n_ho = math.ceil(holdout_prop * len(nids))
        np.random.seed(None)
        nids_to_ho = np.random.choice(nids, size=n_ho, replace=False)
        data.loc[data.nid.isin(nids_to_ho), "hold_out"] = 1
        return data


class Cascade_loc(object):
    def __init__(
        self,
        loc,
        sex,
        year,
        cascade,
        parent_loc=None,
        reimport=True,
        timespan=None,
        feature_flags=None,
    ):
        """Initializes a location in the cascade. Contains data specific
        to loc/sex/year specified

        Args:
            loc(str): location id
            sex(float): One of 0.5 (male), -0.5 (female). If neither, sex
                assumed to be both.
            year(int): year id
            model_version_id: An integer specifying the model version to
                cascade
            cascade: drill.Cascade object
            parent_loc: An instance of cascade_loc for this location's parent
            reimport(bool, True): If True, rerun importer object. If false,
                rely on precached CSV files.
            feature_flags (SimpleNamespace): A way to turn features on and off.
        """
        if isinstance(loc, str):
            self.loc = loc
        elif isinstance(loc, (int, float)):
            self.loc = "{:.0f}".format(loc)
        else:
            raise TypeError(f"Location {loc} is type {type(loc)}")
        self.sex = sex
        self.year = year
        self.cascade = cascade
        self.parent = parent_loc
        self.reimport = reimport
        self._requested_timespan = timespan
        log = logging.getLogger(__name__)
        if feature_flags is not None:
            self._feature_flags = feature_flags
            log.info(f"Using user feature flags {feature_flags}")
        else:
            # This ensures all flags exist and take on their default values.
            self._feature_flags = cascade_parser().parse_args([])
            log.info(f"feature flags all default {self._feature_flags}")

    def initialize(self):
        sex, cascade, reimport, timespan = (
            self.sex,
            self.cascade,
            self.reimport,
            self._requested_timespan,
        )
        log = logging.getLogger(__name__)
        self.loctree = cascade.loctree
        self.locnode = self.loctree.get_node_by_id(int(self.loc))
        self.loclvl_id = self.get_location_level(self.locnode.id)
        self.loclvl = cascade_levels[self.loclvl_id - 1]
        self.model_version_id = cascade.model_version_id
        self.average_mtspec = True

        self.release_method = release.get_release_method_from_mv_and_flags(
            cascade.model_version_id, self._feature_flags
        )
        log.info(f"using release method {self.release_method}")

        self.out_dir = 
        self.draws_dir = 

        try:
            os.makedirs(self.out_dir)
        except:
            pass
        try:
            os.makedirs(self.draws_dir)
        except:
            pass
        try:
            os.chmod(self.out_dir, 0o775)
        except:
            pass
        try:
            os.chmod(self.draws_dir, 0o775)
        except:
            pass

        parent_node = self.locnode.parent
        parent_loc_id = None if not parent_node else parent_node.id
        self.context = DemographicContext(
            parent_location_id=parent_loc_id,
            location_id=int(self.loc),
            year_id=int(self.year),
            sex_id=int(X_SEX_TO_ID[self.sex]),
        )
        self.datastore = datastore_module.Datastore(
            submodel_dir=self.out_dir,
            cascade_root_dir=self.cascade.root_dir,
            context=self.context,
        )

        required_datasets = [
            "data",
            "effect",
            "predin",
            "rate",
            "posterior_summ",
            "data_noarea",
            "data_pred",
            "child_prior",
        ]
        missing_datasets = list()
        for dataset in required_datasets:
            try:
                self.datastore[dataset]
            except (FileNotFoundError, RuntimeError, pa.ArrowInvalid):
                missing_datasets.append(dataset)
        not_missing = list(set(required_datasets) - set(missing_datasets))

        if missing_datasets and not reimport:
            reimport = True
            logging.info(
                f"Reversed reimport in drill because datasets "
                f"missing {', '.join(missing_datasets)}."
            )
        elif reimport and not_missing:
            logging.info(f"Drill reimporting, overwrite {not_missing}.")
        # else all missing and reimport or none missing and not reimporting.

        if timespan is None:
            self.timespan = self.cascade.model_version_meta.timespan.values[0]
            self.timespan_specified = False
        else:
            self.timespan = timespan
            self.timespan_specified = True

        if reimport:
            self.data = self.gen_data(sex)
            self.effects_prior = self.gen_effect()
            self.effects_prior = self.effects_prior.sort_values(
                ["effect", "integrand", "name"]
            )
            self.predin = self.gen_predin()
            self.rate = self.gen_rate()
        else:
            self.data = self.datastore["data"]
            self.effects_prior = self.datastore["effect"]
            self.effects_prior = self.effects_prior.sort_values(
                ["effect", "integrand", "name"]
            )
            self.predin = self.datastore["predin"]
            self.rate = self.datastore["rate"]
            self.post_summary = self.datastore["posterior_summ"]

        # Ensure location columns in data are always strings, types can
        # change when re-importing from CSV
        for c in ["atom", "subreg", "region", "super"]:
            if self.data.dtypes[c] != object:
                self.data[c] = self.data[c].apply(lambda x: "{:.0f}".format(float(x)))

        self.dismod_finished = False

    def fix_sex(self):
        """Reads model version metadata fix_sex setting to know
        if current location level requires recomputation of sex effects

        Returns bool"""
        return self.loclvl_id >= (self.cascade.model_version_meta.fix_sex.values[0])

    def at_fix_sex_lvl(self):
        """Returns True if cascade's location is at fix_sex level"""
        return self.loclvl_id == (self.cascade.model_version_meta.fix_sex.values[0])

    def fix_study_covs(self):
        """Reads model version metdata to know if current location level
        requires recomputation of covariate effects

        Returns bool
        """
        return self.loclvl_id > (self.cascade.model_version_meta.fix_cov.values[0])

    def at_fix_study_cov_lvl(self):
        """Returns true if cascade's location is at fix_cov level"""
        return self.loclvl_id == (self.cascade.model_version_meta.fix_cov.values[0] + 1)

    def get_location_level(self, location_id, label=False):
        """Return either integer or string representation of specific
        location's level.
        """
        if label:
            return cascade_levels[self.loctree.get_nodelvl_by_id(location_id)]
        else:
            return self.loctree.get_nodelvl_by_id(location_id) + 1

    def gen_data(self, sex, drop_emr=False):
        """Return the data subsetted to the relevant level in the
        cascade"""
        data = self.cascade.alldata
        if drop_emr:
            data = data[data.integrand != "mtexcess"]
        data["meas_value"] = data.meas_value.astype("float")
        data["meas_stdev"] = data.meas_stdev.astype("float")
        for lvl in cascade_levels[1:]:
            try:
                data[lvl] = data[lvl].replace("nan", "none")
            except Exception:
                pass

        averaged = self.average_data_by_integrand(data, sex)
        if averaged is not None:
            data = data.append(averaged, sort=True)

        subdata = self.subset_data_by_location_and_sex(
            data, sex, self.loclvl, self.loc, self.release_method, self.fix_sex()
        )
        subdata_mtall = self.mtall_only_data(subdata)

        if self.loclvl == "world":
            if self.cascade.single_param is None:
                subdata = subdata[subdata.integrand != "mtall"]
            else:
                subdata = subdata[subdata.integrand == "mtother"]
        # else not subsetting data for children

        # The all-y data is the used for predicting from the fit in order to
        # get the parent adjusted data for this location's children.
        self.save_ally_data_for_prediction(subdata)

        if self.loclvl != "world":
            # Append parent posterior predictions to dataset.
            # parent's child_prior_file, summarized from model_draw output
            raw_parent_data = self.get_parent_pred_draws()
            parent_data = self.prior_data_for_this_location(
                loclvl=self.loclvl,
                at_fix_sex_lvl=self.at_fix_sex_lvl(),
                year=self.year,
                sex=self.sex,
                fix_sex=self.fix_sex(),
                loc=self.loc,
                parent_data=raw_parent_data,
            )
            subdata = subdata.append(parent_data, sort=True)

            # For subnationals, shift priors be mean(data/priors)
            if self.loclvl == "atom":
                # parent.allyadjout_file from parent data_ally_file.
                adj_data = self.get_parent_adj_data()
                subdata = self.shift_priors(subdata, adj_data, self.loc)
                # shifting priors can decrease min_cv, so reapply it.
                if not self.release_method[Methods.ENABLE_HYBRID_CV]:
                    subdata = enforce_min_cv(
                        subdata, self.cascade.integrand_bounds.copy(), "atom"
                    )
                else:
                    subdata = enforce_hybrid_cv(
                        subdata, self.cascade.integrand_bounds.copy(), "atom"
                    )

            subdata = self.filter_data_by_time(subdata)
            prior_upload = self.save_prior_data_for_upload(subdata)
            self.datastore["prior_upload"] = prior_upload

        # Re-center country covariates
        subdata["location_id"] = subdata.location_id.astype("str")
        if self.cascade.single_param is None:
            subdata = subdata[subdata.integrand != "mtall"]

        if len(self.cascade.covdata) > 0:
            subdata = self.recenter_ccovs(subdata)

        # Append mtall to dataset
        subdata = subdata.append(subdata_mtall, sort=True)
        for lvl in ["atom", "subreg", "region", "super"]:
            subdata[lvl] = subdata[lvl].replace("nan", "none")

        # Fill the study level covariates where missing
        covs = subdata.filter(regex="^x_").columns
        for c in covs:
            subdata[c].fillna(0, inplace=True)

        subdata = subdata.sort_values(
            [
                "integrand",
                "age_lower",
                "age_upper",
                "time_lower",
                "time_upper",
                "super",
                "region",
                "subreg",
                "atom",
                "x_sex",
            ]
        )

        # Drop heldout data
        subdata = subdata[
            (subdata.hold_out == 0)
            | (subdata.a_data_id == IHME_COD_DB)
            | (subdata.a_data_id == 0)
        ]
        self.datastore["data"] = subdata

        cw.verify_no_crosswalking(subdata)

        noarea_data, topred_data = self.select_data_for_prediction(subdata)
        self.datastore["data_noarea"] = noarea_data
        self.datastore["data_pred"] = topred_data

        return subdata

    def average_data_by_integrand(self, data, sex):
        """
        See :ref:`_release-changes` for explanation of how averaging
        csmr differs between step 2 and later steps.

        """
        integrand_to_average = "mtspecific"
        mpm = self.cascade.model_params
        exclude_params = mpm.loc[mpm.parameter_type_id == 17, "measure"]
        add_csmr = self.cascade.model_version_meta.add_csmr_cause.notnull().all()
        full_model = self.cascade.single_param is None
        not_excluded = "mtspecific" not in list(exclude_params)

        if not self.release_method[Methods.ENABLE_CSMR_AVERAGE]:
            most_detailed_location_ids = [n.id for n in self.locnode.leaves()]
            return _average_data_by_integrand(
                data=data,
                location_id=int(self.loc),
                sex=sex,
                most_detailed_locs=most_detailed_location_ids,
                csmr_from_codcorrect=add_csmr,
                is_single_param=not full_model,
                csmr_excluded_from_model=not not_excluded,
                release_id=self.cascade.model_version_meta.release_id.iat[0],
                loctree=self.loctree,
                data_likelihood=self.cascade.data_likelihood,
            )
        else:
            # we're in step 2, use legacy method below
            pass

        if add_csmr and full_model and not_excluded and self.average_mtspec:
            data_to_average = data[data.integrand == integrand_to_average]
            mtspec_means = (
                data_to_average.groupby(["super", "age_lower", "age_upper", "x_sex"])
                .mean()
                .reset_index()
            )
            mtspec_means.drop("meas_stdev", axis=1, inplace=True)
            mtspec_stds = (
                data_to_average.groupby(["super", "age_lower", "age_upper", "x_sex"])[
                    "meas_stdev"
                ]
                .apply(lambda x: np.sqrt((x**2).mean()))
                .reset_index()
            )
            mtspec_means = mtspec_means.merge(mtspec_stds)
            mtspec_means["atom"] = "none"
            mtspec_means["region"] = "none"
            mtspec_means["subreg"] = "none"
            mtspec_means["location_id"] = "none"
            mtspec_means["a_data_id"] = 0
            mtspec_means["integrand"] = integrand_to_average
            mtspec_means["data_like"] = self.cascade.data_likelihood
            mtspec_means["year_id"] = mtspec_means.year_id.round()
            return mtspec_means
        else:
            return None

    @staticmethod
    def subset_data_by_location_and_sex(data, sex, loclvl, loc, release_method, fix_sex):
        """
        Subset the locations ... fix the importer so that the
        mortality that gets used is the actual mortality rates for the
        given location level. This modifies the data DataFrame.
        """
        if loclvl != "world":
            subdata = data[data[loclvl] == loc]
            if not release_method[Methods.FIX_CSMR_RE] or loclvl == "atom":
                for hl in higher_levels(loclvl):
                    if hl != "world":
                        subdata.loc[:, hl] = "none"
                subdata.loc[:, loclvl] = "none"
        else:
            subdata = data

        # Subset sex a re-reference so sex of interest is 0
        subdata = subdata.assign(orig_sex=subdata.x_sex)
        if loclvl != "world" and fix_sex:
            subdata = subdata[subdata.x_sex.isin([sex, 0])]
            subdata = subdata.assign(x_sex=subdata.x_sex - sex)

        if loclvl != "world":
            subdata = cw.standardize_sex(subdata)

        # This is important: this is where we decide whether to keep csmr
        # input data -- for step2 we throw away any csmr not created as part
        # of our averaging process (except pre-existing SR data, but that might
        # not ever happen in production)
        if loclvl == "world":
            subdata = subdata[
                (
                    (subdata.integrand == "mtspecific")
                    & (subdata.atom == "none")
                    & (subdata.subreg == "none")
                    & (subdata.region == "none")
                )
                | (subdata.integrand != "mtspecific")
            ]
        else:
            # if not world we drop any mtspecific below current level
            for ll in lower_levels(loclvl):
                subdata = subdata[
                    ~((subdata.integrand == "mtspecific") & (subdata[ll] != "none"))
                ]

        subdata["x_local"] = 1
        return subdata

    def mtall_only_data(self, subdata):
        """
        Takes only the mtall data and removes area information.
        """
        if self.cascade.single_param is None:
            subdata_mtall = subdata[subdata.integrand == "mtall"]
            group_cols = ["integrand", "age_lower", "age_upper", "data_like"]
            subdata_mtall = subdata_mtall.groupby(group_cols).mean().reset_index()
            subdata_mtall = subdata_mtall.assign(
                meas_stdev=inf,
                atom="none",
                subreg="none",
                super="none",
                region="none",
                x_local=1,
            )

            if self.loclvl == "world":
                subdata_mtall = subdata_mtall.drop(columns=["orig_sex"])

        else:
            subdata_mtall = pd.DataFrame(columns=["x_local"])
        return subdata_mtall

    def save_ally_data_for_prediction(self, ally_data):
        """
        All y-data, meaning all observations in the bundle that weren't
        from the database, are saved for later use by predict.
        """
        ally_data["location_id"] = ally_data.location_id.astype("str")
        if len(self.cascade.covdata) > 0:
            for ccov in self.cascade.ccovs:
                rcov_col = "x_c_" + ccov
                ally_data[rcov_col] = 0
        ally_data = ally_data[ally_data.a_data_id != IHME_COD_DB]
        self.datastore["data_ally"] = ally_data

    @staticmethod
    def prior_data_for_this_location(
        loclvl, at_fix_sex_lvl, year, sex, fix_sex, loc, parent_data
    ):
        """Takes raw prior data and filters on two completely different
        conditions, depending on whether the parent data has data with this
        run's location_id. If it does, subselect by location, sex,
        and year. If it doesn't, don't select by year, and take all data
        defined at locations above this run's level in the hierarchy."""
        if fix_sex and not at_fix_sex_lvl:
            prior_sex = 0
        else:
            prior_sex = sex
        if int(float(loc)) in parent_data.location_id.unique():
            parent_data = parent_data[
                (parent_data.location_id == int(float(loc)))
                & (parent_data.x_sex == prior_sex)
                & (parent_data.x_local == 0)
                & (parent_data.year_id == year)
            ]
            logging.info(
                f"appending {len(parent_data)} location-specific "
                f"parent predictions as data from sex {prior_sex}"
            )
        else:
            # When data has a none in the column for this run's location
            # level, that means it is defined for some level above this one.
            parent_data = parent_data[
                (parent_data[loclvl] == "none")
                & (parent_data.x_sex == prior_sex)
                & (parent_data.x_local == 0)
            ]
            logging.info(
                f"appending {len(parent_data)} parent area "
                f"parent predictions as data from sex {prior_sex}"
            )
        for loclvl in ["super", "region", "subreg", "atom"]:
            parent_data[loclvl] = "none"
        parent_data = parent_data.drop_duplicates()
        parent_data["x_local"] = 0
        parent_data["hold_out"] = 0
        parent_data["x_sex"] = 0
        return parent_data

    def get_parent_adj_data(self):
        """Read parent location's allyadjout_file"""
        return self.parent.datastore["allyadjout"]

    @staticmethod
    def shift_priors(data, adj_data, loc, use_median=False, ignore_small_values=False):
        """For subnationals, shift priors given parent run.

        See CCMHD-CCMHD-14654 for use_median and ignore_small_values options.
        They are not used in standard models

        Args:
              data (pd.DataFrame): All of the input data, including priors.
              adj_data (pd.DataFrame): Parent predicted data values.
              use_median (bool): option for self.adjust_prior_by_mean_of_fit
              ignore_small_values (bool): option for self.adjust_prior_by_mean_of_fit

        Returns:
            Data with adjusted priors.
        """
        integrands = data.integrand.unique()
        priors_from_draws = data[data.x_local == 0]
        unadjusted_data = data[data.x_local == 1]
        parent_predict = adj_data[
            (adj_data.x_local == 1)
            & (adj_data.a_data_id != IHME_COD_DB)
            & (adj_data.location_id == int(float(loc)))
        ]
        shifted_priors = pd.DataFrame()
        for ig in integrands:
            ig_prior = priors_from_draws[priors_from_draws.integrand == ig]
            ig_data = parent_predict[parent_predict.integrand == ig]
            if len(ig_prior) == 0:
                pass  # No priors to adjust
            elif len(ig_data) == 0:
                # No fit with which to adjust, so keep the priors.
                shifted_priors = shifted_priors.append(ig_prior)
            else:
                Cascade_loc.adjust_prior_by_mean_of_fit(
                    ig_data,
                    ig_prior,
                    use_median=use_median,
                    ignore_small_values=ignore_small_values,
                )
                shifted_priors = shifted_priors.append(ig_prior)

        shifted_priors["meas_value"] = shifted_priors.meas_value.clip(lower=0)
        newdata = unadjusted_data.append(shifted_priors)
        return newdata

    @staticmethod
    def adjust_prior_by_mean_of_fit(
        parent_predict, child_prior, use_median, ignore_small_values
    ):
        r"""
        Adjust the prior so that its average is near the average of the parent
        fit. If :math:`p_a` is prior at an age, :math:`n_a` is number of
        ages, and :math:`m_a` is adjusted median from the parent fit,

        .. math::

            p_a = \frac{p_a}{n_a}\sum_a\frac{m_a}{p_a}.

        Args:
            parent_predict (pd.DataFrame): Data predicted on intervals,
                so ``age_upper`` > ``age_lower``. Has an ``adjust_median``
                column from Dismod-ODE predict.
            child_prior (pd.DataFrame): Data at midpoints, a complete set
                with no two points at same age.
            use_median: if True, use median instead of average
            ignore_small_values: if True, treat data <= 1e-12 as if it were 0

        Returns:
            None: The result is written to ``child_prior.meas_value``.
        """
        parent_predict["age_mid"] = (
            parent_predict.age_lower.astype("float")
            + parent_predict.age_upper.astype("float")
        ) / 2.0
        prior_func = interpolate.InterpolatedUnivariateSpline(
            child_prior.age_lower.astype("float"),
            child_prior.meas_value.astype("float"),
            k=1,
            ext=3,
        )
        parent_predict["spline"] = parent_predict.age_mid.apply(lambda x: prior_func(x))
        # exclude from calculation if spline or fit is 0, because
        # that can lead to wonky data/spline ratios
        if ignore_small_values:
            fit_mask = ~np.isclose(child_prior.meas_value, 0, atol=1e-12)
            spline_mask = ~np.isclose(parent_predict.spline, 0, atol=1e-12)
        else:
            fit_mask = child_prior.meas_value != 0
            spline_mask = parent_predict.spline != 0
        # scale priors by average(data/spline)
        data_over_prior = (
            parent_predict.adjust_median[spline_mask] / parent_predict.spline[spline_mask]
        )
        summarization_method = np.median if use_median else np.mean
        scale_factor = summarization_method(data_over_prior)
        # scale_factor can be nan if spline_mask results in empty
        # ig_data.spline CCMHD-10254
        if not np.isnan(scale_factor):
            child_prior.loc[fit_mask, "meas_value"] = (
                child_prior.meas_value[fit_mask] * scale_factor
            )

    def filter_data_by_time(self, subdata):
        """
        Uses the single timespan if it is given to Cascade_loc.
        Otherwise, every integrand must have its own timespan in
        the model parameters.

        All priors are always retained.
        """
        ts_meas = self.cascade.model_params[self.cascade.model_params.parameter_type_id == 20]
        ts_subdata = []
        for integrand in subdata.integrand.unique():
            has_timespan = integrand in ts_meas.measure.values
            if has_timespan and not self.timespan_specified:
                this_ts = ts_meas.loc[ts_meas.measure == integrand, "mean"].values[0]
            else:
                this_ts = self.timespan
            time_min = self.year - this_ts
            time_max = self.year + this_ts
            ts_subdata.append(
                subdata[
                    (subdata.integrand == integrand)
                    & (
                        (float(time_min) <= subdata.time_upper)
                        & (float(time_max) >= subdata.time_lower)
                        | (subdata.x_local == 0)
                    )
                ]
            )
        if len(subdata) > 0:
            subdata = pd.concat(ts_subdata)
        else:
            subdata = pd.DataFrame()
            subdata["hold_out"] = 0
            subdata["input_data_key"] = np.nan
            subdata["location_id"] = int(float(self.loc))
        return subdata

    def save_prior_data_for_upload(self, subdata):
        """
        Only the data added as priors will be extracted here.
        """
        prior_upload = subdata.loc[subdata.x_local == 0, :]
        prior_upload["age_lower"] = prior_upload.age_lower.astype("float")
        prior_upload["age_lower"] = prior_upload.age_lower.round(4).astype("str")
        demo = self.cascade.demographics
        prior_upload["age_group_id"] = prior_upload.age_lower.replace(
            demo.age_midpoints_to_age_group_id
        )

        if prior_upload["age_group_id"].isna().any():
            bad = set(prior_upload[prior_upload["age_group_id"].isna()].age_lower)
            raise RuntimeError(f"midpoint replacement failed for {bad}")

        prior_upload["lower"] = prior_upload.meas_value - 1.96 * prior_upload.meas_stdev
        prior_upload["upper"] = prior_upload.meas_value + 1.96 * prior_upload.meas_stdev
        if self.fix_sex():
            sex_map = {-0.5: 2, 0: 3, 0.5: 1}
            prior_upload["sex_id"] = prior_upload.x_sex.replace({0: sex_map[self.sex]})
        else:
            prior_upload["sex_id"] = prior_upload.x_sex.replace({-0.5: 2, 0: 3, 0.5: 1})
        prior_upload.rename(
            columns={"meas_value": "mean", "age_lower": "age", "integrand": "measure"},
            inplace=True,
        )
        if self.cascade.single_param is not None:
            prior_upload["measure"] = prior_upload.measure.replace(
                {"mtother": self.cascade.single_param}
            )
        prior_upload = prior_upload.merge(self.cascade.measure_map, on="measure", how="left")
        prior_upload["location_id"] = int(float(self.loc))
        prior_upload["year_id"] = self.year
        prior_upload = prior_upload[
            [
                "year_id",
                "location_id",
                "sex_id",
                "age_group_id",
                "age",
                "measure_id",
                "mean",
                "lower",
                "upper",
            ]
        ]
        # Clip prevalence and proportion priors
        for col in ["mean", "lower", "upper"]:
            clipped = prior_upload.loc[prior_upload.measure_id.isin([5, 18]), col].clip(
                upper=1
            )
            prior_upload.loc[prior_upload.measure_id.isin([5, 18]), col] = clipped
        for col in ["mean", "lower", "upper"]:
            clipped = prior_upload[col].clip(lower=0)
            prior_upload[col] = clipped
        return prior_upload

    def recenter_ccovs(self, subdata, zero_priors=True):
        """Recenter country level covariates by subtracting covariate mean.
        Given all covariate data for this location, year, and sex,

        1.  For a covariate that isn't in the bundle as a column,
            set its value to zero.
        2.  Subtract the mean of covariate data from the bundle covariate
            value for each record.
        3.  Set the covariate for any prior data to zero, for all covariates.
        4.  Fill in any missing individual covariate values with zero,
            representing the mean value for this location.
        """
        thisdata = subdata.copy()
        cd = self.cascade.covdata
        cd[["super", "region", "subreg", "atom"]] = (
            cd[["super", "region", "subreg", "atom"]].astype("str").replace({"nan": "none"})
        )
        thislvl = cd[
            (cd.location_id.apply(lambda x: int(float(x))) == int(float(self.loc)))
            & (cd.year_id == self.year)
            & (cd.x_sex == self.sex)
        ]
        for ccov in self.cascade.ccovs:
            cov_col = "raw_c_" + ccov
            rcov_col = "x_c_" + ccov
            ccov_mean = thislvl[cov_col].mean()
            if cov_col not in thisdata.columns:
                thisdata[cov_col] = 0
            thisdata[rcov_col] = thisdata[cov_col] - ccov_mean
            if zero_priors:
                thisdata.loc[thisdata.x_local == 0, rcov_col] = 0
            thisdata[rcov_col] = thisdata[rcov_col].fillna(0)
        return thisdata

    def select_data_for_prediction(self, subdata):
        """
        Given data that will be fed to Dismod-ODE, select subsets
        that will be used to make predictions from the fit, in dismod-predict.
        The two datasets modify the random effects and sex covariate
        values.
        """
        noarea_data = subdata.copy()
        # Using data without regions for prediction means no random
        # effects can be applied.
        noarea_data[["super", "region", "subreg", "atom"]] = "none"
        # Predict using reference value of all country covariates.
        cc_cols = noarea_data.filter(like="x_c_").columns
        noarea_data[cc_cols] = 0
        both_data = noarea_data[noarea_data.x_sex == 0]
        ss_data = noarea_data[noarea_data.x_sex != 0]
        ms_data = both_data.copy()
        ms_data["x_sex"] = 0.5
        fs_data = both_data.copy()
        fs_data["x_sex"] = -0.5
        both_data = pd.concat([ms_data, fs_data])
        ss_data["x_sex"] = 0
        both_data["orig_sex"] = 0
        noarea_data = ss_data.append(both_data, sort=True)
        # Add columns for tracking prediction vs. adjustment sex values
        noarea_data["adj_sex"] = noarea_data.x_sex
        noarea_data["pred_sex"] = noarea_data.adj_sex * -1
        noarea_data.loc[noarea_data.adj_sex == 0, "pred_sex"] = noarea_data.loc[
            noarea_data.adj_sex == 0, "orig_sex"
        ]
        # Also make a version with sex covariate.
        topred_data = noarea_data.copy()
        topred_data["x_sex"] = topred_data.pred_sex
        topred_data = topred_data[topred_data.x_local == 1]
        return noarea_data, topred_data

    def get_parent_pred_draws(self):
        """Read parent locations child_prior_file"""
        return self.parent.datastore["child_prior"]

    def summarize_draws_for_children(self, draws, burn=0.2):
        """summarize draws to be used as inputs into children dismod runs"""
        # Summarize draws
        draws = draws.copy()
        start_idx = int(len(draws) * burn) - 1
        draws = draws[start_idx:]
        draws = draws.transpose()
        median = draws.median(axis=1)
        mean = draws.mean(axis=1)
        std = draws.std(axis=1)
        lower = draws.apply(lambda x: stats.scoreatpercentile(x, 2.5), axis=1)
        upper = draws.apply(lambda x: stats.scoreatpercentile(x, 97.5), axis=1)

        # Merge summaries onto the inputs
        draw_summ = pd.DataFrame(
            {"mean": mean, "median": median, "lower": lower, "upper": upper, "std": std}
        )
        draw_summ.index.name = "row_name"
        draw_summ = draw_summ.reset_index()
        draw_summ["row_name"] = draw_summ.row_name.astype("int")
        predin = self.datastore["predin"]
        predin = predin[predin.for_database == 0]
        draw_summ = predin.merge(draw_summ, on="row_name", how="left")

        draw_summ["meas_value"] = draw_summ["median"]
        draw_summ["meas_stdev"] = (draw_summ.upper - draw_summ.lower) / (2 * 1.96)

        # Calculate stdev based on min/max cv constraints
        ib = self.cascade.integrand_bounds
        child_level = lower_levels(self.loclvl)[0]
        if not self.release_method[Methods.ENABLE_HYBRID_CV]:
            draw_summ = enforce_min_cv(draw_summ, ib, child_level)
        else:
            draw_summ = enforce_hybrid_cv(draw_summ, ib, child_level)

        # Exlude priors where specified in the DB
        exclude_priors = self.cascade.model_params[
            self.cascade.model_params.parameter_type_id == 18
        ]["measure"].values
        draw_summ = draw_summ[~draw_summ.integrand.isin(exclude_priors)]

        # Format for appending onto data
        draw_summ.drop(
            ["mean", "median", "lower", "upper", "row_name", "std", "min_loc_cv"],
            axis=1,
            inplace=True,
        )

        return draw_summ

    def get_mtall(self):
        """Subset mtall data to relevant location level"""
        all_mtall = self.cascade.mtall
        self.mtall = all_mtall[all_mtall[self.loclvl] == self.loc]
        return self.mtall

    def gen_effect(self):
        """Returns dataframe of effects data, informed by parent location"""
        if self.parent is None:
            self.effects_prior = self.cascade.effects_prior

            if not self.release_method[Methods.ENABLE_EMR_CSMR_RE]:
                # Only estimate mtspecific effects at the super-region level
                self.effects_prior.loc[
                    (self.effects_prior.effect.isin(["region", "subreg"]))
                    & (self.effects_prior.integrand == "mtspecific"),
                    "lower",
                ] = 0
                self.effects_prior.loc[
                    (self.effects_prior.effect.isin(["region", "subreg"]))
                    & (self.effects_prior.integrand == "mtspecific"),
                    "mean",
                ] = 0
                self.effects_prior.loc[
                    (self.effects_prior.effect.isin(["region", "subreg"]))
                    & (self.effects_prior.integrand == "mtspecific"),
                    "upper",
                ] = 0
                self.effects_prior.loc[
                    (self.effects_prior.effect.isin(["region", "subreg"]))
                    & (self.effects_prior.integrand == "mtspecific"),
                    "std",
                ] = inf

                # Don't estimate mtexcess
                self.effects_prior.loc[
                    (self.effects_prior.effect.isin(["region", "subreg", "super"]))
                    & (self.effects_prior.integrand == "mtexcess"),
                    "mean",
                ] = 0
                self.effects_prior.loc[
                    (self.effects_prior.effect.isin(["region", "subreg", "super"]))
                    & (self.effects_prior.integrand == "mtexcess"),
                    "lower",
                ] = 0
                self.effects_prior.loc[
                    (self.effects_prior.effect.isin(["region", "subreg", "super"]))
                    & (self.effects_prior.integrand == "mtexcess"),
                    "upper",
                ] = 0
                self.effects_prior.loc[
                    (self.effects_prior.effect.isin(["region", "subreg", "super"]))
                    & (self.effects_prior.integrand == "mtexcess"),
                    "std",
                ] = inf

            self.datastore["effect"] = self.effects_prior
            return self.effects_prior
        else:
            # Get original effects priors and posterior from parent level
            p_pri_eff = self.parent.effects_prior

            if self.fix_study_covs() and not self.at_fix_study_cov_lvl():
                p_pri_eff["mean.fixed"] = p_pri_eff["mean.fixed"].fillna(p_pri_eff["mean"])
                p_pri_eff["lower.fixed"] = p_pri_eff["lower.fixed"].fillna(p_pri_eff["lower"])
                p_pri_eff["upper.fixed"] = p_pri_eff["upper.fixed"].fillna(p_pri_eff["upper"])
                p_pri_eff["std.fixed"] = p_pri_eff["std.fixed"].fillna(p_pri_eff["std"])
                p_pri_eff = p_pri_eff[
                    [
                        "effect",
                        "integrand",
                        "name",
                        "mean.fixed",
                        "lower.fixed",
                        "upper.fixed",
                        "std.fixed",
                    ]
                ]
                p_pri_eff = p_pri_eff.rename(
                    columns={
                        "mean.fixed": "mean",
                        "lower.fixed": "lower",
                        "upper.fixed": "upper",
                        "std.fixed": "std",
                    }
                )
            else:
                p_pri_eff = p_pri_eff[
                    ["effect", "integrand", "name", "mean", "lower", "upper", "std"]
                ]
            p_post_eff = self.parent.post_summary
            p_post_eff = p_post_eff[
                ["effect", "median", "mean", "post_lower", "post_upper", "std"]
            ]

            # Extract the betas from the posterior
            betas = p_post_eff[p_post_eff.effect.str.startswith("beta_")]
            betas.loc[:, "integrand"] = betas.effect.str.split("_").apply(lambda x: x[1])
            betas.loc[:, "name"] = betas.effect.str.split("_").apply(
                lambda x: "_".join(x[2:])
            )
            betas.loc[:, "effect"] = "beta"
            betas.drop("mean", axis=1, inplace=True)
            betas.rename(
                columns={"median": "mean", "post_lower": "lower", "post_upper": "upper"},
                inplace=True,
            )
            betas.loc[:, "std"] = betas["std"].apply(lambda x: inf if x == 0 else x)

            effects = p_pri_eff[(p_pri_eff.effect != "beta")]
            effects = effects.append(betas)

            p_pri_betas = p_pri_eff[p_pri_eff.effect == "beta"]
            effects = effects.merge(
                p_pri_betas,
                on=["effect", "integrand", "name"],
                how="left",
                suffixes=("", ".pri"),
            )

            # Fix sex, study, and country cov effects with no wiggle room
            beta_bool = effects.effect == "beta"
            if self.fix_study_covs() and not self.at_fix_study_cov_lvl():
                effects.loc[beta_bool, "mean.fixed"] = effects.loc[beta_bool, "mean.pri"]
                effects.loc[beta_bool, "lower.fixed"] = effects.loc[beta_bool, "lower.pri"]
                effects.loc[beta_bool, "upper.fixed"] = effects.loc[beta_bool, "upper.pri"]
                effects.loc[beta_bool, "std.fixed"] = effects.loc[beta_bool, "std.pri"]
            elif self.fix_study_covs():
                effects.loc[beta_bool, "mean.fixed"] = effects.loc[beta_bool, "mean"]
                effects.loc[beta_bool, "lower.fixed"] = effects.loc[beta_bool, "lower"]
                effects.loc[beta_bool, "upper.fixed"] = effects.loc[beta_bool, "upper"]
                effects.loc[beta_bool, "std.fixed"] = effects.loc[beta_bool, "std"]

            if self.fix_study_covs():
                x_sex_bin = (effects.name.isin(["x_sex"])) & (effects.effect == "beta")
                x_sex_pri_ms = effects.loc[x_sex_bin, "mean.fixed"]
                x_sex_pri_ls = effects.loc[x_sex_bin, "mean.fixed"]
                x_sex_pri_us = effects.loc[x_sex_bin, "mean.fixed"]
                x_sex_pri_ss = effects.loc[x_sex_bin, "std.fixed"]

                effects.loc[x_sex_bin, "mean"] = x_sex_pri_ms
                effects.loc[x_sex_bin, "lower"] = x_sex_pri_ls
                effects.loc[x_sex_bin, "upper"] = x_sex_pri_us
                effects.loc[x_sex_bin, "std"] = x_sex_pri_ss

                study_cov_bin = (effects.name.str.startswith("x_s_")) & (
                    effects.effect == "beta"
                )
                x_study_pri_ms = effects.loc[study_cov_bin, "mean.fixed"]
                x_study_pri_ls = effects.loc[study_cov_bin, "mean.fixed"]
                x_study_pri_us = effects.loc[study_cov_bin, "mean.fixed"]
                x_study_pri_ss = effects.loc[study_cov_bin, "std.fixed"]

                effects.loc[study_cov_bin, "mean"] = x_study_pri_ms
                effects.loc[study_cov_bin, "lower"] = x_study_pri_ls
                effects.loc[study_cov_bin, "upper"] = x_study_pri_us
                effects.loc[study_cov_bin, "std"] = x_study_pri_ss

                ccov_bin = (effects.name.str.startswith("x_c_")) & (effects.effect == "beta")
                x_c_pri_ms = effects.loc[ccov_bin, "mean.fixed"]
                x_c_pri_ls = effects.loc[ccov_bin, "mean.fixed"]
                x_c_pri_us = effects.loc[ccov_bin, "mean.fixed"]
                x_c_pri_ss = effects.loc[ccov_bin, "std.fixed"]

                effects.loc[ccov_bin, "mean"] = x_c_pri_ms
                effects.loc[ccov_bin, "lower"] = x_c_pri_ls
                effects.loc[ccov_bin, "upper"] = x_c_pri_us
                effects.loc[ccov_bin, "std"] = x_c_pri_ss

            # Drop locations with no data
            locs_wdata = list(
                [v for k, v in self.cascade.data_coverage_bysex[str(self.sex)].items()]
            )
            locs_wdata.extend(
                [v for k, v in self.cascade.data_coverage_bysex[str(0)].items()]
            )
            locs_wdata = list(itertools.chain(*locs_wdata)) + ["none", "cycle"]
            locs_wdata = [str(l) for l in locs_wdata]
            effects = effects[
                ~(
                    (effects.effect.isin(["super", "region", "subreg"]))
                    & (~effects.name.isin(locs_wdata))
                )
            ]
            effects = effects.sort_values(["effect", "integrand", "name"])

            # Clip the means to lower/upper bounds
            effects["mean"] = effects.apply(
                lambda x: np.clip(x["mean"], x["lower"], x["upper"]), axis=1
            )
            self.datastore["effect"] = effects
            self.effects_prior = effects

            return effects

    def gen_predin(self):
        """Returns prediction mesh dataframe"""
        if self.cascade.single_param is None:
            integrands_toest = importer.integrand_pred
        else:
            integrands_toest = ["mtother"]

        predin = []
        if not self.fix_sex():
            sex_mesh = [0.5, -0.5, 0]
        else:
            sex_mesh = [0]
        for integrand in integrands_toest:
            for a in self.cascade.default_age_mesh:
                for sex in sex_mesh:
                    for loceff in [0]:
                        row = {
                            "integrand": integrand,
                            "age_lower": a,
                            "age_upper": a,
                            "x_sex": sex,
                            "x_local": loceff,
                        }
                        predin.append(row)
        predin = pd.DataFrame(predin)
        predin["time_lower"] = self.year - self.timespan
        predin["time_upper"] = self.year + self.timespan
        predin["data_like"] = self.cascade.data_likelihood
        predin["meas_value"] = 0
        predin["meas_stdev"] = inf

        for sc in self.cascade.study_covariates:
            predin[sc] = 0

        for cc in self.cascade.ccovs:
            predin["x_c_" + cc] = 0

        # Generate prediction mesh for all children, assigning locations
        # to appropriate columns
        locnode = self.loctree.get_node_by_id(int(float(self.loc)))
        children_df = []
        demo = self.cascade.demographics
        if self.loclvl == "world":
            years_to_predict = demo.year_ids
        else:
            years_to_predict = [self.year]

        for year in years_to_predict:
            for child in locnode.children:
                child_df = predin.copy()
                child_loc = "{:.0f}".format(child.id)
                child_lvl = self.get_location_level(child.id, label=True)
                locs_with_data = list(
                    self.cascade.data_coverage_bysex[str(self.sex)][child_lvl]
                )
                locs_with_data.extend(self.cascade.data_coverage_bysex[str(0)][child_lvl])
                if child_loc in locs_with_data:
                    child_df[child_lvl] = child_loc
                else:
                    child_df[child_lvl] = "none"
                child_df["location_id"] = child_loc
                child_df["year_id"] = year
                children_df.append(child_df)

        if len(children_df) > 0:
            children_df = pd.concat(children_df)
            if len(self.cascade.covdata) > 0:
                ccovs = ["raw_c_" + c for c in self.cascade.ccovs]
                children_df = children_df.merge(
                    self.cascade.covdata[["location_id", "year_id", "x_sex"] + ccovs],
                    on=["location_id", "year_id", "x_sex"],
                    how="left",
                )
                children_df = self.recenter_ccovs(children_df, zero_priors=False).drop(
                    ccovs, axis=1
                )
        else:
            children_df = predin.copy()

        missing_lvls = 0
        for lvl in ["super", "region", "subreg", "atom"]:
            if lvl not in children_df.columns:
                children_df[lvl] = "none"
                missing_lvls += 1

        # Generate cycle prediction mesh for calculating maximum cv by age
        cyc_predin = predin.copy()
        if len(locnode.children) > 0:
            child_lvl = self.get_location_level(locnode.children[0].id, label=True)
            cyc_predin[child_lvl] = "cycle"
        else:
            child_lvl = ""
        other_lvls = set(["super", "region", "subreg", "atom"]) - set([child_lvl])
        for l in other_lvls:
            cyc_predin[l] = "none"
        for l in ["super", "region", "subreg", "atom"]:
            predin[l] = "none"

        # Gen prediction mesh specific to the GBD database
        template_file = 
        # template file has output age groups across all rounds so we need
        # to subset to this round; also, only keep birth if birth_prev == 1
        if self.cascade.model_version_meta["birth_prev"].values[0] == 1:
            age_groups_to_keep = list(demo.age_group_ids) + [
                demo.terminal_dismod_age,
                gbd.constants.age.BIRTH,
            ]
        else:
            age_groups_to_keep = list(demo.age_group_ids) + [demo.terminal_dismod_age]
        template_file = template_file[template_file.age_group_id.isin(age_groups_to_keep)]
        template_file["age_merge_key"] = 1
        db_predin = predin.copy()
        db_predin.drop(["age_lower", "age_upper"], axis=1, inplace=True)
        db_predin = db_predin.drop_duplicates()
        db_predin["age_merge_key"] = 1
        db_predin = db_predin.merge(template_file, on="age_merge_key")
        db_predin.drop("age_merge_key", axis=1, inplace=True)
        db_predin["for_database"] = 1
        if self.loclvl == "world":
            years = copy(demo.year_ids)
            first_year = years[0]
            rest_of_years = years[1:]
            db_predin["year_id"] = first_year
            ys_to_add = []
            for y in rest_of_years:
                dby = db_predin.copy()
                dby["year_id"] = y
                ys_to_add.append(dby)
            db_predin = db_predin.append(pd.concat(ys_to_add))

            if len(self.cascade.covdata) > 0:
                db_predin["location_id"] = "1"
                ccovs = ["raw_c_" + c for c in self.cascade.ccovs]
                db_predin = db_predin.merge(
                    self.cascade.covdata[["location_id", "year_id", "x_sex"] + ccovs],
                    on=["location_id", "year_id", "x_sex"],
                    how="left",
                )
                db_predin = self.recenter_ccovs(db_predin, zero_priors=False).drop(
                    ccovs, axis=1
                )
        else:
            db_predin["year_id"] = self.year

        # Bring it all together
        if missing_lvls != 4:
            children_df = children_df.append(predin)
        children_df = children_df.append(cyc_predin)
        children_df = children_df.append(db_predin)
        children_df = children_df.reset_index(drop=True)
        # location_id column may be absent, depending on if there are child
        # locations. We add it here to ensure schema of predin never changes
        children_df["location_id"] = children_df.get("location_id", np.nan)
        children_df = children_df.fillna(0)
        children_df.index.name = "row_name"
        children_df = children_df.reset_index()
        self.datastore["predin"] = children_df
        return children_df

    def gen_rate(self):
        """Get cascade rate prior, write to datastore, and return
        dataframe"""
        crs = self.cascade.rate_prior
        if self.cascade.single_param is None:
            crs = crs[~crs.type.isin(["omega", "domega"])]
            mtall = self.data[self.data.integrand == "mtall"]
            if mtall.empty:
                raise RuntimeError("No all cause mortality found")
            mtall["age_lower"] = mtall["age_lower"].astype("float")
            mtall = mtall.sort_values("age_lower")
            mtall_ages = (
                (mtall.age_lower.astype("float") + mtall.age_upper.astype("float")) / 2.0
            ).values

            spline = interpolate.InterpolatedUnivariateSpline(
                mtall_ages, mtall.meas_value, k=1
            )
            mtall_zero = spline(0)
            mtall_end = spline(100)
            # CCRED-1333: handle negative spline estimates in USARE.
            if mtall_end <= 0:
                if self.cascade.demographics.release_id == gbd.constants.release.USRE:
                    warnings.warn(
                        f"We found negative spline estimates when estimating envelope for "
                        f"location_id {mtall.meas_value}! Setting to floor of 1e-5."
                    )
                    mtall_end = 1e-5
                else:
                    raise ValueError(
                        f"Envelope estimates for {self.loc} are producing negative spline "
                        f"values. This will break the underlying dismod ODE. Please make a "
                        f"helpdesk ticket."
                    )

            mtall_ages = [0] + list(mtall_ages) + [100]
            mtall_values = np.hstack((mtall_zero, mtall.meas_value, mtall_end)).astype(str)
            omega_rates = {
                "type": "omega",
                "age": mtall_ages,
                "mean": mtall_values,
                "lower": mtall_values,
                "upper": mtall_values,
                "std": inf,
            }
            domega_rates = {
                "type": "domega",
                "age": mtall_ages[:-1],
                "mean": 0,
                "lower": -inf,
                "upper": inf,
                "std": inf,
            }

            crs = crs.append(pd.DataFrame(omega_rates))
            crs = crs.append(pd.DataFrame(domega_rates))
        self.datastore["rate"] = crs.replace({-inf: "_inf"})
        return crs

    def get_parent_effects(self):
        """Transfer posterior effects from the parent level as prior
        information for this level"""
        parent_post = self.parent.post_summary
        return parent_post

    def get_loc_dir(self, loc):
        """Returns the directory for the given location"""
        return 

    def pass_down_covs(self, post_effects):
        """Optionally modify post effects. Returns a boolean that is true if
        post_effects was modified
        """
        cov_cols = [c for c in post_effects.columns if "_x_c_" in c or "_x_s_" in c]
        mod_post_effects = False
        for cov_col in cov_cols:
            effect = cov_col.split("_")[0]
            integrand = cov_col.split("_")[1]
            name = "_".join(cov_col.split("_")[2:])
            if effect == "beta":
                if not any(self.data.loc[self.data.integrand == integrand, name] != 0) or (
                    self.fix_study_covs()
                ):
                    if self.fix_study_covs():
                        cov_mean = self.effects_prior.loc[
                            (self.effects_prior.effect == effect)
                            & (self.effects_prior.integrand == integrand)
                            & (self.effects_prior.name == name),
                            "mean.fixed",
                        ].values[0]
                        cov_lb = self.effects_prior.loc[
                            (self.effects_prior.effect == effect)
                            & (self.effects_prior.integrand == integrand)
                            & (self.effects_prior.name == name),
                            "lower.fixed",
                        ].values[0]
                        cov_ub = self.effects_prior.loc[
                            (self.effects_prior.effect == effect)
                            & (self.effects_prior.integrand == integrand)
                            & (self.effects_prior.name == name),
                            "upper.fixed",
                        ].values[0]
                        cov_sd = self.effects_prior.loc[
                            (self.effects_prior.effect == effect)
                            & (self.effects_prior.integrand == integrand)
                            & (self.effects_prior.name == name),
                            "std.fixed",
                        ].values[0]
                    else:
                        cov_mean = self.effects_prior.loc[
                            (self.effects_prior.effect == effect)
                            & (self.effects_prior.integrand == integrand)
                            & (self.effects_prior.name == name),
                            "mean",
                        ].values[0]
                        cov_lb = self.effects_prior.loc[
                            (self.effects_prior.effect == effect)
                            & (self.effects_prior.integrand == integrand)
                            & (self.effects_prior.name == name),
                            "lower",
                        ].values[0]
                        cov_ub = self.effects_prior.loc[
                            (self.effects_prior.effect == effect)
                            & (self.effects_prior.integrand == integrand)
                            & (self.effects_prior.name == name),
                            "upper",
                        ].values[0]
                        cov_sd = self.effects_prior.loc[
                            (self.effects_prior.effect == effect)
                            & (self.effects_prior.integrand == integrand)
                            & (self.effects_prior.name == name),
                            "std",
                        ].values[0]
                    if (cov_sd == "inf") or (cov_sd == np.inf) or (cov_sd == -np.inf):
                        cov_sd = (cov_ub - cov_lb) / (2 * 1.96)
                    if cov_sd != 0:
                        np.random.seed(self.cascade.seed)
                        post_effects[cov_col] = np.random.normal(
                            cov_mean, cov_sd, size=len(post_effects)
                        )
                        post_effects[cov_col] = post_effects[cov_col].clip(
                            lower=cov_lb, upper=cov_ub
                        )
                    else:
                        post_effects[cov_col] = cov_mean
                    mod_post_effects = True
        return mod_post_effects

    def summarize_posterior(self, burn_pct=20):
        """Computes summary stats from the mcmc chain for each modeled
        parameter. Write out file"""

        post_effect_chain = self.datastore["posterior"]
        mod_pe = self.pass_down_covs(post_effect_chain)
        if mod_pe:
            self.datastore["posterior"] = post_effect_chain
            # write this to disk for when we run predict method
            self.datastore.export_data_to_csv(["posterior"])
        start_idx = int(round(len(post_effect_chain) * burn_pct / 100.0))
        post_effect_chain = post_effect_chain[start_idx:]
        post_effect_chain = post_effect_chain.reset_index(drop=True)

        n_qs = 4
        q_len = 500
        med_qs = []
        for q in range(n_qs):
            start_idx = -(q + 1) * q_len
            end_idx = -(q) * q_len
            if end_idx == 0:
                med = post_effect_chain[start_idx:].median()
                med.name = "median_neg%s_end" % (-start_idx)
            else:
                med = post_effect_chain[start_idx:end_idx].median()
                med.name = "median_neg%s_neg%s" % (-start_idx, -end_idx)
            med_qs.append(med)
        med_qs.reverse()

        medians = post_effect_chain.median()
        means = post_effect_chain.mean()
        stds = post_effect_chain.std()
        lower = post_effect_chain.apply(lambda x: stats.scoreatpercentile(x, 2.5))
        upper = post_effect_chain.apply(lambda x: stats.scoreatpercentile(x, 97.5))
        medians.name = "median"
        means.name = "mean"
        stds.name = "std"
        lower.name = "post_lower"
        upper.name = "post_upper"

        post_summary = pd.concat([medians, means, stds, lower, upper] + med_qs, axis=1)
        post_summary = post_summary.reset_index()
        post_summary.rename(columns={"index": "effect"}, inplace=True)
        self.post_summary = post_summary
        self.datastore["posterior_summ"] = post_summary

        # Extract betas for upload
        ps = post_summary.copy()
        bts = ps[ps.effect.str.startswith("beta_")]
        bts["measure"] = bts.effect.apply(lambda x: x.split("_")[1])

        # Extract zetas for upload
        zts = ps[ps.effect.str.startswith("zeta_")]
        zts["measure"] = zts.effect.apply(lambda x: x.split("_")[1])
        zts["study_covariate"] = zts.effect.apply(lambda x: "_".join(x.split("_")[2:]))
        zeta1 = zts[zts.study_covariate == "x_local"]
        zeta1["parameter_type_id"] = 11
        zts = zts[zts.study_covariate != "x_local"]
        zts["parameter_type_id"] = 6
        zts["study_covariate"] = zts.study_covariate.str.replace("x_s_", "")

        # Extract xis for upload
        xis = ps[ps.effect.str.startswith("xi_")]
        xis["measure"] = xis.effect.apply(lambda x: x.split("_")[1])
        xis["measure"] = xis.measure.replace(
            {"iota": "incidence", "rho": "remission", "chi": "mtexcess", "omega": "mtother"}
        )
        xis["parameter_type_id"] = 10

        # Note that sex is also treated like a study covariate
        scs = bts[bts.effect.str.contains("_x_s_")]
        scs["study_covariate"] = scs.effect.apply(lambda x: "_".join(x.split("_")[4:]))
        scs["parameter_type_id"] = 5

        sexcov = bts[bts.effect.str.contains("_x_sex")]
        sexcov["study_covariate"] = "sex"
        sexcov["parameter_type_id"] = 5

        ccs = bts[bts.effect.str.contains("_x_c_")]
        ccs["covariate_name_short"] = ccs.effect.apply(lambda x: "_".join(x.split("_")[5:]))
        ccs["parameter_type_id"] = 7
        ccs.loc[ccs.effect.str.contains("_x_c_lnasdr"), "parameter_type_id"] = 8
        ccs["asdr_cause"] = r"\N"
        if ccs.effect.str.contains("_x_c_lnasdr").any():
            ccs.loc[ccs.effect.str.contains("_x_c_lnasdr"), "asdr_cause"] = (
                self.cascade.model_params[self.cascade.model_params.parameter_type_id == 8][
                    "asdr_cause"
                ].values[0]
            )

        effect_upload = pd.concat([ccs, scs, sexcov, zeta1, zts, xis])

        res = ps[ps.effect.str.startswith("u_")]
        res["measure"] = res.effect.apply(lambda x: x.split("_")[1])
        res["location_id"] = res.effect.apply(lambda x: x.split("_")[-1])
        res = res[
            ~((res["mean"] == 0) & (res["median"] == 0) & (res["std"] == 0))
            & ~(res.location_id.isin(["none", "cycle"]))
        ]
        res["parameter_type_id"] = 9
        effect_upload = effect_upload.append(res)

        effect_upload["cascade_level_id"] = self.loclvl_id
        effect_upload = effect_upload.merge(
            self.cascade.ccov_map, on="covariate_name_short", how="left"
        )
        effect_upload = effect_upload.merge(
            self.cascade.scov_map, on="study_covariate", how="left"
        )

        if self.cascade.single_param is not None:
            effect_upload["measure"] = effect_upload.measure.replace(
                {"mtother": self.cascade.single_param}
            )
        effect_upload = effect_upload.merge(
            self.cascade.measure_map, on="measure", how="left"
        )
        effect_upload.rename(
            columns={
                "covariate_id": "country_covariate_id",
                "mean": "mean_effect",
                "post_lower": "lower_effect",
                "post_upper": "upper_effect",
            },
            inplace=True,
        )

        # Drop zeros
        effect_upload = effect_upload[
            ~(
                (effect_upload.mean_effect == 0)
                & (effect_upload.lower_effect == 0)
                & (effect_upload.upper_effect == 0)
            )
        ]

        # Drop mtall
        effect_upload = effect_upload[~effect_upload.measure.isin(["mtall"])]

        # Scope columns
        effect_upload = effect_upload[
            [
                "measure_id",
                "parameter_type_id",
                "cascade_level_id",
                "study_covariate_id",
                "country_covariate_id",
                "asdr_cause",
                "location_id",
                "mean_effect",
                "lower_effect",
                "upper_effect",
            ]
        ]
        effect_upload = effect_upload.fillna(r"\N")
        self.datastore["effect_upload"] = effect_upload

        return post_summary

    def run_dismod(self):
        """Call dismod executable in a subprocess, retrying up to 3 times."""

        log = logging.getLogger(__name__)

        def has_noninf_data(data):
            return (
                data.groupby("integrand")["meas_value"]
                .apply(lambda x: (x != inf).any())
                .all()
            )

        hid = has_noninf_data(self.data)
        assert hid, """
            At least 1 data point / prior point per integrand must be
            non-inf"""

        dismod_path = 

        input_data = ["data", "value", "simple", "rate", "effect"]
        output_data = ["posterior", "info"]
        input_paths = self.datastore.export_data_to_csv(input_data)
        output_paths = [str(self.datastore.path_to_csv(d)) for d in output_data]

        cmd = [dismod_path] + input_paths + output_paths
        log.info("Running: " + " ".join(cmd))
        result = _run_and_log_command(cmd)
        self.dismod_finished = True
        # for each output file, ensure we store in datastore cache
        for output_key, output_path in zip(output_data, output_paths):
            self.datastore[output_key] = pd.read_csv(output_path)
        return result

    def draw(self):
        """call model_draw program in a subprocess and format output draws
        before writing to disk"""
        draw_path = 

        input_data = ["predin", "value", "simple", "rate", "effect", "posterior"]
        output_data = ["drawout"]
        input_paths = self.datastore.export_data_to_csv(input_data)
        output_paths = [str(self.datastore.path_to_csv(d)) for d in output_data]

        cmd = [draw_path] + input_paths + output_paths
        result = _run_and_log_command(cmd)

        # Summarize and write to file
        measure_map = self.cascade.measure_map.copy()
        measure_map.rename(columns={"measure": "integrand"}, inplace=True)

        # We don't want to save raw draw output so we don't add to datatore
        drawout_path = output_paths[0]
        draws = pd.read_csv(drawout_path)

        if len(self.locnode.children) > 0:
            child_priors = self.summarize_draws_for_children(draws)
            self.datastore["child_prior"] = child_priors
        start_idx = int(len(draws) * 0.2) - 1
        draws = draws[start_idx:]
        draws = draws.transpose()
        num_draws = draws.shape[1]
        parent_predin = self.datastore["predin"]

        # Format draws for writing to file
        draws = draws.reset_index()
        draws["index"] = draws["index"].astype("int")
        draws.rename(columns={"index": "row_name"}, inplace=True)
        draws = parent_predin.merge(draws, on="row_name", how="left")
        draw_renames = dict(
            (d, "draw_%s" % (d - start_idx)) for d in range(start_idx, start_idx + num_draws)
        )
        draws.rename(columns=draw_renames, inplace=True)
        if self.cascade.single_param is not None:
            draws["integrand"] = draws.integrand.replace(
                {"mtother": self.cascade.single_param}
            )
        draws = draws.merge(measure_map, on="integrand", how="left")
        draws["location_id"] = int(float(self.loc))
        draw_cols = list(draws.filter(like="draw").columns)

        if self.fix_sex():
            sex_map = {-0.5: 2, 0: 3, 0.5: 1}
            draws["sex_id"] = draws.x_sex.replace({0: sex_map[self.sex]})
        else:
            draws["sex_id"] = draws.x_sex.replace({-0.5: 2, 0: 3, 0.5: 1})
        draws = draws[draws.for_database == 1]
        draws = draws[
            ["location_id", "year_id", "age_group_id", "sex_id", "measure_id"] + draw_cols
        ]
        demo = self.cascade.demographics
        draws95 = draws[draws.age_group_id == demo.terminal_dismod_age]
        draws95["age_group_id"] = demo.terminal_gbd_age
        draws = draws.append(draws95)

        # Calculate age standarized fits
        draws_agestd = draws.merge(self.cascade.age_weights, on="age_group_id")
        draws_agestd[draw_cols] = draws_agestd[draw_cols].multiply(
            draws_agestd.weight, axis="index"
        )
        assert len(draws_agestd.age_group_id.unique()) == len(
            demo.age_group_ids
        ), "Age-weight merge is being improperly applied"
        draws_agestd = (
            draws_agestd.groupby(["location_id", "year_id", "sex_id", "measure_id"])
            .sum()
            .reset_index()
        )
        draws_agestd["age_group_id"] = gbd.constants.age.AGE_STANDARDIZED
        draws_agestd.drop("weight", axis=1, inplace=True)
        draws = draws.append(draws_agestd)

        # Clip draws so that values never go below zero and so
        # prevalence and proportions never exceed 1
        for col in draw_cols:
            clipped = draws.loc[draws.measure_id.isin([5, 18]), col].clip(upper=1)
            draws.loc[draws.measure_id.isin([5, 18]), col] = clipped
        for col in draw_cols:
            clipped = draws[col].clip(lower=0)
            draws[col] = clipped

        # Write formatted draw files for lowest estimation levels
        if self.locnode.id in [l.id for l in self.loctree.leaves()]:
            for y in draws.year_id.unique():
                for s in draws.sex_id.unique():
                    filepath = 
                    np.random.seed(self.cascade.seed)
                    draw_cols = draws.filter(like="draw").columns
                    draw_cols = list(np.random.choice(draw_cols, size=1000, replace=False))
                    draws = draws[(draws.year_id == y) & (draws.sex_id == s)][
                        ["measure_id", "location_id", "year_id", "age_group_id", "sex_id"]
                        + draw_cols
                    ]
                    dren = dict((d, "draw_" + str(i)) for i, d in enumerate(draw_cols))
                    draws.rename(columns=dren, inplace=True)
                    draws.to_hdf(
                        filepath,
                        "draws",
                        mode="w",
                        format="table",
                        complib="blosc:zstd",
                        complevel=1,
                        data_columns=["measure_id", "age_group_id"],
                    )

        draw_cols = ["draw_" + str(i) for i in range(1000)]
        draws["median"] = draws[draw_cols].median(axis=1)
        draws["mean"] = draws[draw_cols].mean(axis=1)
        draws["std"] = draws[draw_cols].std(axis=1)
        draws["lower"] = draws[draw_cols].apply(
            lambda x: stats.scoreatpercentile(x, 2.5), axis=1
        )
        draws["upper"] = draws[draw_cols].apply(
            lambda x: stats.scoreatpercentile(x, 97.5), axis=1
        )

        # Merge summaries onto the inputs
        draws["meas_value"] = draws["median"]
        draws["meas_stdev"] = (draws.upper - draws.lower) / (2 * 1.96)

        # Format for appending onto data
        draws.rename(
            columns={
                "mean": "pred_mean",
                "median": "pred_median",
                "lower": "pred_lower",
                "upper": "pred_upper",
            },
            inplace=True,
        )
        draw_summ = draws[
            [
                "year_id",
                "sex_id",
                "age_group_id",
                "measure_id",
                "pred_mean",
                "pred_lower",
                "pred_upper",
            ]
        ]
        self.datastore["drawout_summ"] = draw_summ

        if self.loclvl != "world":
            if self.sex == 0.5:
                draw_summ["sex_id"] = 1
            elif self.sex == -0.5:
                draw_summ["sex_id"] = 2
            else:
                draw_summ["sex_id"] = 3
        draw_summ["location_id"] = int(float(self.loc))
        draw_summ = draw_summ[
            [
                "location_id",
                "year_id",
                "sex_id",
                "age_group_id",
                "measure_id",
                "pred_mean",
                "pred_lower",
                "pred_upper",
            ]
        ]
        self.datastore["posterior_upload"] = draw_summ

        return result

    def predict(self):
        """
        TO DO: Revisit what data pred is doing... it doesn't seem to
        generate the same results as model_draw and manual summary
        of the resulting draws"""

        pred_datasets = [
            {"in": "data_noarea", "out": "dataadjout"},
            {"in": "data_ally", "out": "allyadjout"},
            {"in": "data_pred", "out": "datapredout"},
        ]

        pred_paths = []
        for in_out_dict in pred_datasets:
            in_out_paths = {
                k: str(self.datastore.path_to_csv(v)) for (k, v) in in_out_dict.items()
            }
            pred_paths.append(in_out_paths)

        for i in range(len(pred_datasets)):
            datasets = pred_datasets[i]
            paths = pred_paths[i]
            self.datastore.export_data_to_csv([datasets["in"]])
            predict_path = 
            cmd = [
                predict_path,
                paths["in"],
                str(self.datastore.path_to_csv("value")),
                str(self.datastore.path_to_csv("simple")),
                str(self.datastore.path_to_csv("rate")),
                str(self.datastore.path_to_csv("effect")),
                str(self.datastore.path_to_csv("posterior")),
                paths["out"],
            ]
            result = _run_and_log_command(cmd)

            self.datastore[datasets["out"]] = pd.read_csv(paths["out"])
            # Format adjusted data for upload
            if datasets["out"] == "dataadjout":
                upload_summary = self.datastore[datasets["out"]]
                if self.cascade.single_param is not None:
                    upload_summary["integrand"] = upload_summary.integrand.replace(
                        {"mtother": self.cascade.single_param}
                    )
                param_unc = (upload_summary.adjust_upper - upload_summary.adjust_lower) / (
                    2 * 1.96
                )
                adj_std = np.sqrt(param_unc**2 + upload_summary["meas_stdev"] ** 2)

                # Use Wilson's interval for 'proportions' and normal interval
                # for 'rates'
                props = upload_summary[
                    upload_summary.integrand.isin(["prevalence", "yld", "cfr", "proportion"])
                ]
                props["adjust_median"] = props.adjust_median.clip(lower=0, upper=1)
                rates = upload_summary[
                    ~upload_summary.integrand.isin(["prevalence", "yld", "cfr", "proportion"])
                ]

                def lower_from_se(p, se, param_type, confidence=0.95):
                    """Calculates lower bound of the UI based on standard
                    error"""
                    quantile = (1 - confidence) / 2
                    lower = p + stats.norm.ppf(quantile) * se
                    if param_type == "proportion":
                        lower = np.max([0, lower])
                    return lower

                def upper_from_se(p, se, param_type, confidence=0.95):
                    """Calculates upper bound of the UI based on standard
                    error"""
                    quantile = 1 - (1 - confidence) / 2
                    upper = p + stats.norm.ppf(quantile) * se
                    if param_type == "proportion":
                        upper = np.min([1, upper])
                    return upper

                def se_from_ess(p, ess, param_type, cases=None, quantile=0.975):
                    """Calculates standard error from effective sample size
                    and mean based on the Wilson Score Interval"""

                    err_msg = "param_type must be either " "'proportion' or 'rate'"
                    assert param_type in ["proportion", "rate"], err_msg

                    if cases is None:
                        cases = p * ess

                    if ess == 0:
                        return 0

                    if param_type == "proportion":
                        z = stats.norm.ppf(quantile)
                        se = np.sqrt(p * (1 - p) / ess + z**2 / (4 * ess**2))
                    elif cases <= 5:
                        # Use linear interpolation for rate parameters
                        # with fewer than 5 cases
                        se = ((5 - p * ess) / ess + p * ess * np.sqrt(5 / ess**2)) / 5
                    elif cases > 5:
                        # Use Poisson SE for rate parameters with more than
                        # 5 cases
                        se = np.sqrt(p / ess)
                    else:
                        raise Exception("Can't calculate SE... check your cases parameter")

                    return se

                def lower_from_ess(p, ess, param_type, quantile=0.975):
                    """Calculates the lower bound of the uncertainty interval
                    based on the Wilson Score Interval"""
                    if ess == 0:
                        return 0
                    cases = p * ess
                    se = se_from_ess(p, ess, param_type=param_type, quantile=quantile)

                    if p == 0 and param_type == "proportion":
                        lower = 0
                    elif cases > 5 and param_type == "proportion":
                        z = stats.norm.ppf(quantile)
                        lower = 1 / (1 + z**2 / ess) * (p + z**2 / (2 * ess) - z * se)
                    elif cases <= 5 or param_type == "rate":
                        lower = lower_from_se(p, se, param_type)

                    if param_type == "proportion":
                        lower = np.max([0, lower])

                    return lower

                def upper_from_ess(p, ess, param_type, quantile=0.975):
                    """Calculates the upper bound of the uncertainty interval
                    based on the Wilson Score Interval"""
                    if ess == 0:
                        return 0
                    cases = p * ess
                    se = se_from_ess(p, ess, param_type=param_type, quantile=quantile)

                    if p == 1 and param_type == "proportion":
                        upper = 1
                    elif cases > 5 and param_type == "proportion":
                        z = stats.norm.ppf(quantile)
                        upper = 1 / (1 + z**2 / ess) * (p + z**2 / (2 * ess) + z * se)
                    elif cases <= 5 or param_type == "rate":
                        upper = upper_from_se(p, se, param_type)

                    if param_type == "proportion":
                        upper = np.min([1, upper])

                    return upper

                if len(props) > 0:
                    props["ess"] = props["adjust_median"] / (adj_std**2)
                    props = props[props.ess.notnull()]
                    props["adjust_lower"] = props.apply(
                        lambda x: lower_from_ess(x["adjust_median"], x["ess"], "proportion"),
                        axis=1,
                    )
                    props["adjust_upper"] = props.apply(
                        lambda x: upper_from_ess(x["adjust_median"], x["ess"], "proportion"),
                        axis=1,
                    )
                if len(rates) > 0:
                    rates["adjust_lower"] = upload_summary.adjust_median - 1.96 * adj_std
                    rates["adjust_upper"] = upload_summary.adjust_median + 1.96 * adj_std
                upload_summary = pd.concat([props, rates])
                # Note the sign flip on 0.5->2 and -0.5->1 ... this has to do
                # with the reference sex when computing adjusted data vs.
                # predictions.  Addresses CENCOM-342
                upload_summary.loc[upload_summary.orig_sex == 0, "orig_sex"] = (
                    upload_summary.loc[upload_summary.orig_sex == 0, "x_sex"] * -1
                )
                upload_summary["orig_sex"] = upload_summary.orig_sex.replace(
                    {-0.5: 2, 0: 3, 0.5: 1}
                )
                upload_summary.rename(
                    columns={
                        "a_data_id": "input_data_key",
                        "orig_sex": "sex_id",
                        "adjust_median": "mean",
                        "adjust_lower": "lower",
                        "adjust_upper": "upper",
                    },
                    inplace=True,
                )
                upload_summary.drop("x_sex", axis=1, inplace=True)

                # Use timespan to determine which years data point should apply
                # to
                ts_meas = self.cascade.model_params[
                    self.cascade.model_params.parameter_type_id == 20
                ]
                adj_data = []
                demo = self.cascade.demographics
                for year in demo.year_ids:
                    for integrand in upload_summary.integrand.unique():
                        if (
                            integrand in ts_meas.measure.values
                        ) and not self.timespan_specified:
                            this_ts = ts_meas.loc[
                                ts_meas.measure == integrand, "mean"
                            ].values[0]
                        else:
                            this_ts = self.timespan
                        year_data = upload_summary[
                            (upload_summary.integrand == integrand)
                            & (float(year - this_ts) <= upload_summary.time_upper)
                            & (float(year + this_ts) >= upload_summary.time_lower)
                        ]
                        year_data["year_id"] = year
                        adj_data.append(year_data)
                upload_summary = pd.concat(adj_data)
                upload_summary = upload_summary[
                    ["input_data_key", "sex_id", "year_id", "mean", "lower", "upper"]
                ]

                upload_summary = upload_summary[upload_summary.input_data_key.notnull()]
                upload_summary = upload_summary[upload_summary.input_data_key != 0]

                # Clip so that values never go below zero
                for col in ["mean", "lower", "upper"]:
                    upload_summary[col] = upload_summary[col].clip(lower=0)

                # Drop non-real data
                upload_summary = upload_summary.query("input_data_key > 0")
                self.datastore["adj_data_upload"] = upload_summary
        return result


def _average_data_by_integrand(
    data,
    location_id,
    sex,
    most_detailed_locs,
    csmr_from_codcorrect,
    is_single_param,
    csmr_excluded_from_model,
    release_id,
    loctree,
    data_likelihood,
):
    """
    This method is only called for step3+. It is for potentially
    computing/averaging and appending new csmr data.

    We will return None if any of the following conditions are met:
        if csmr from codcorrect

        if single-parameter-model or csmr excluded from model

        if csmr exists for location and sex already

        if no csmr exists for most-detailed locations to average from

    otherwise we compute and append pop-weighted average csmr

    Arguments:
        data (pd.DataFrame): dataset (pre subsetting by loc/sex),
        location_id (int): corresponds to cascade_loc's location
        sex (float): one of -0.5 (female), 0.5 (male), 0 (both)
        most_detailed_locs (List[int]): list of most detailed location_ids
            beneath location_id argument
        csmr_from_codcorrect (bool): True if add_csmr_cause specified
        is_single_param (bool): True if single parameter model
        csmr_excluded_from_model (bool): True if settings exclude CSMR
        release_id (str): release_id from the values of gbd.constants.release
        loctree (hierarchies.dbtrees.loctree): location hierarchy of cascade
        data_likelihood (str): ie 'log_gaussian'. To be assigned to new
            csmr rows

    Returns:
        Optional[pd.DataFrame]
    """
    if csmr_from_codcorrect or is_single_param or csmr_excluded_from_model:
        return

    csmr_for_loc_sex = data.query(
        (
            "integrand == 'mtspecific' and location_id == @location_id and "
            f" x_sex == {sex} and a_data_id > 0"
        )
    )
    csmr_exists_for_loc_sex = not csmr_for_loc_sex.empty

    if csmr_exists_for_loc_sex:
        return

    no_csmr_in_children = data.query(
        (
            "integrand == 'mtspecific' and a_data_id > 0 and location_id in"
            " @most_detailed_locs"
        )
    ).empty
    if no_csmr_in_children:
        return

    # TODO: we havn't done time subsetting yet so we're creating averages
    # we might throw away later. That's not super efficient so something to fix
    # later.
    avgs = _create_mtspec_averages(
        df=data,
        location_id=location_id,
        most_detailed_locs=most_detailed_locs,
        release_id=release_id,
        loctree=loctree,
        data_likelihood=data_likelihood,
    )
    return avgs


def _create_mtspec_averages(
    df, location_id, most_detailed_locs, release_id, loctree, data_likelihood
) -> pd.DataFrame:
    """
    Subset df down to csmr rows in the set of most detailed locations. Query
    get_population to get pop envelope. Compute and return pop-weighted average
    CSMR.
    """
    data_to_average = df.query(
        (
            "integrand == 'mtspecific' and location_id in @most_detailed_locs and"
            " a_data_id > 0"
        )
    )

    pop = _create_population_data(
        df=data_to_average, most_detailed_locs=most_detailed_locs, release_id=release_id
    )

    avgs = _pop_weight_average(data_to_average, pop, location_id)

    if "location_id" in avgs:
        # avgs are by super region
        by_super = True
        assert location_id == 1
    else:
        by_super = False
        avgs["location_id"] = str(location_id)

    loclvl_to_id_map = loclvls_of(location_id, loctree)
    for lvl, loc_id in loclvl_to_id_map.items():
        if lvl == "super" and by_super:
            avgs[lvl] = avgs.location_id
        else:
            avgs[lvl] = str(loc_id)

    avgs["a_data_id"] = 0
    avgs["integrand"] = "mtspecific"
    avgs["data_like"] = data_likelihood
    avgs["year_id"] = avgs.year_id.round()
    avgs["time_lower"] = avgs.year_id
    avgs["time_upper"] = avgs.year_id

    return avgs


def _pop_weight_average(df, popdf, location_id):
    """
    if data age range spans multiple age groups, says to assume uniform
    distribution when doing pop weighting.

    so for example if we have population for ages 5-9 and 10-14 and a datum
    spanning ages 8-13, our total age range is 13-8=5. 2/5ths of that is from
    5-9 and 3/5ths of that is from 10-14 so our population weight when
    aggregating would be .4*(5-9pop) + .6*(10-14pop)

    For global location we want to compute average down to super region level
    so we handle location_id == 1 differently
    """
    group_cols = ["year_id", "x_sex", "location_id", "super", "age_lower", "age_upper"]
    if location_id != 1:
        agg_cols = [c for c in group_cols if c not in ["location_id", "super"]]
    else:
        # create super region level estimates for global loc
        agg_cols = [c for c in group_cols if c != "location_id"]

    # need to cast back on the way out
    for col in ["age_upper", "age_lower"]:
        df[col] = df[col].astype(float)

    # unifom populations in buckets, with data potentially overlapping
    # multiple buckets. If datum D overlaps  20% in bucket A and 80% in
    # bucket B, we want to use 80% of pop B and 20% of pop A.
    subpop = popdf.merge(df[group_cols + ["meas_value", "meas_stdev"]], how="right")

    subpop = subpop.assign(
        amount=(
            subpop[["age_upper", "age_group_years_end"]].min(axis=1)
            - subpop[["age_lower", "age_group_years_start"]].max(axis=1)
        ),
        age_range=subpop.age_upper - subpop.age_lower,
    )

    data_spans_pop = (subpop.age_upper >= subpop.age_group_years_start) & (
        subpop.age_group_years_end >= subpop.age_lower
    )

    def compute_weights(df):
        return (df.amount * df.population / df.age_range).sum()

    weights = subpop[data_spans_pop].groupby(group_cols).apply(compute_weights).reset_index()
    weights.columns = group_cols + ["weight"]

    df = df.merge(weights, how="left")
    assert df.weight.notna().all(), "problem merging weights during csmr " "calculation"

    if "super" in df:
        # cast so we don't lose in groupby. need to cast back
        df["super"] = df["super"].astype(int)

    # pandas doesn't support groupby mean with weights, and calling np.average
    # for every group can get slow when number of groups is large. So we
    # vectorize this math where possible
    df = df.assign(
        _val_times_weight=df.meas_value * df.weight,
        _stdev_times_weight=df.meas_stdev * df.weight,
    )

    grps = df.groupby(agg_cols)
    weighted_meas_val = (
        (grps["_val_times_weight"].sum() / grps["weight"].sum())
        .reset_index()
        .rename({0: "meas_value"}, axis=1)
    )
    weighted_stdev = (
        (grps["_stdev_times_weight"].sum() / grps["weight"].sum())
        .reset_index()
        .rename({0: "meas_stdev"}, axis=1)
    )
    avgs = pd.merge(weighted_meas_val, weighted_stdev)
    assert len(avgs) == len(weighted_meas_val) == len(weighted_stdev)

    if "super" in avgs:
        avgs = avgs.assign(location_id=avgs["super"].astype(str)).drop("super", axis=1)

    # we cast to float earlier because math -- need to cast back to str
    # so types are consistent
    for col in ["age_upper", "age_lower"]:
        avgs[col] = avgs[col].astype(str)
    return avgs


def loclvls_of(location_id, loctree):
    """
    return a dictionary of {loclvl: str(location_id) or "none"}

    loclvls_of(6, loctree) = {'super': '4', 'region': '5', 'subreg': '6',
        'atom': 'none'}

    Always exclude 'world' as a key, even if location_id == 1
    """
    results = {}
    this_node = loctree.get_node_by_id(location_id)
    this_lvl = loctree.get_nodelvl_by_id(location_id)
    ancestors = [n for n in this_node.ancestors()][:-1]  # skip world
    higher = cascade_levels[1:this_lvl]  # skip world
    lower = cascade_levels[this_lvl + 1 :]

    if location_id != 1:
        results[cascade_levels[this_lvl]] = str(location_id)

    for lvl in lower:
        results[lvl] = "none"

    for lvl, node in zip(higher, reversed(ancestors)):
        results[lvl] = str(node.id)

    return results


def _create_population_data(df, most_detailed_locs, release_id) -> pd.DataFrame:
    """
    call get_pop with demographics from df; massage get_pop output df
    to be mergeable with df
    """
    sex_ids = list(df.x_sex.map(X_SEX_TO_ID).unique())
    year_ids = list(df.year_id.unique())
    log = logging.getLogger(__name__)
    try:
        age_group_df = _get_age_group_span(release_id=release_id)
    except Exception:
        log.info("Failed in get_age_group_span")
        raise
    age_group_ids = age_group_df.age_group_id.tolist()
    try:
        pop = shared_functions.get_population(
            location_id=most_detailed_locs,
            sex_id=sex_ids,
            year_id=year_ids,
            age_group_id=age_group_ids,
            release_id=release_id,
        ).pipe(lambda df: df.assign(x_sex=df.sex_id.map(SEX_DICT)))
    except Exception:
        log.info("Failed in get_population")
        raise

    log.info(f"mtspec population run_id: {pop.run_id.unique()[0]}")
    pop = pop.drop(["sex_id", "run_id"], axis=1)

    # merge on age upper/lower to pop so that we can merge with data later
    pop = pop.merge(age_group_df, how="left", on=["age_group_id"])
    assert not pop.age_group_years_start.isna().any()

    return pop


def _get_age_group_span(release_id: int) -> pd.DataFrame:
    """
    Since input data can span multiple age groups at once, for this function
    we return all most detailed age groups so we can be sure to handle
    any age range in the data
    """
    age_df = shared_functions.get_age_metadata(release_id=release_id)

    return age_df.drop("age_group_weight_value", axis=1)


def _run_and_log_command(cmd: List[str]) -> bytes:
    """
    Run a dismod command in a subprocess, log any exceptional messages, and
    return raw stdout output as bytes.

    Used for dismod_ode, model_draw, and data_pred binaries.
    """
    log = logging.getLogger(__name__)
    log.info("Running: " + " ".join(cmd))
    try:
        result = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.stderr.write(str(e.output))
        raise e
    dismod_messages.log_exceptional_messages(result)
    return result
