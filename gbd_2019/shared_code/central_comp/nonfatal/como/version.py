import os
import getpass
import json
from copy import deepcopy
from ast import literal_eval
from functools import partial
import pandas as pd
import numpy as np
import subprocess

from aggregator.aggregators import AggSynchronous
from aggregator.operators import Sum
from db_tools import ezfuncs
from db_queries import (get_rei_metadata, get_population, get_ids,
                        get_demographics)
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_sources.io import mem_read_func, mem_write_func
from gbd.constants import (age, gbd_process, gbd_metadata_type, measures,
                           gbd_process_version_status)
from gbd.decomp_step import (decomp_step_from_decomp_step_id,
                             decomp_step_id_from_decomp_step)
from gbd_outputs_versions import CompareVersion, GBDProcessVersion
from gbd_outputs_versions.convenience import bleeding_edge
from hierarchies import dbtrees
from ihme_dimensions.dimensionality import DataFrameDimensions

import como
from como.disability_weights import (combine_epilepsy_any,
                                     combine_epilepsy_subcombos,
                                     urolithiasis)
from como.residuals import compute_global_ratios
from como.common import propagate_hierarchy
from como.summarize import ComoSummaries

THIS_PATH = os.path.dirname(os.path.abspath(__file__))

# Set default file mask to readable-for all users
os.umask(0o0002)


class NoCacheFound(Exception):
    pass


class NoCachedValueFound(Exception):
    pass


class MemoizeMethod:
    """cache the return value of a method

    This class is meant to be used as a decorator of methods. The return value
    from a given method invocation will be cached on the instance whose method
    was invoked. All arguments passed to a method decorated with memoize must
    be hashable.
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, obj, objtype=None):
        return partial(self, obj)

    def __call__(self, *args, **kw):
        obj = args[0]
        try:
            cache = obj._memoized
        except AttributeError:
            cache = obj._memoized = {}

        try:
            method_cache = cache[self.func]
        except KeyError:
            cache[self.func] = {}
            method_cache = cache[self.func]

        arg_key = (args[1:], frozenset(list(kw.items())))
        try:
            res = method_cache[arg_key]
        except KeyError:
            res = method_cache[arg_key] = self.func(*args, **kw)
        return res


class FCache:

    _extension_controller_map = {
        "csv": ("_read_csv_cache", "_write_csv_cache"),
        "h5": ("_read_hdf_cache", "_write_hdf_cache"),
        "json": ("_read_json_cache", "_write_json_cache"),
        "dim": ("_read_dim_cache", "_write_dim_cache")
    }

    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        self._f_controllers = {}
        self._cache = {}

    def load_config(self):
        config_path = os.path.join(self.cache_dir, 'cache_config.json')
        try:
            with open(config_path) as config:
                self._f_controllers = json.load(config)
        except IOError:
            raise NoCacheFound(f"no cache found in {self.cache_dir}")

    def dump_config(self):
        config_path = os.path.join(self.cache_dir, 'cache_config.json')
        with open(config_path, 'w') as config:
            json.dump(self._f_controllers, config)

    @MemoizeMethod
    def _read_csv_cache(self, path):
        return pd.read_csv(path)

    def _write_csv_cache(self, val, path):
        val.to_csv(path, index=False)

    @MemoizeMethod
    def _read_json_cache(self, path):
        with open(path) as j_path:
            return json.load(j_path)

    def _write_json_cache(self, val, path):
        with open(path, 'w') as j_path:
            json.dump(val, j_path)

    @MemoizeMethod
    def _read_hdf_cache(self, path):
        pass

    def _write_hdf_cache(self, val, path):
        pass

    @MemoizeMethod
    def _read_dim_cache(self, path):
        with open(path) as dim_path:
            val = dim_path.readlines()
            return literal_eval(val[0])

    def _write_dim_cache(self, val, path):
        with open(path, 'w') as dim_path:
            dim_path.write(repr(val))

    def _infer_controller_method(self, path, io_type):
        f_extns = path.split(".")[-1]
        method_names = self._extension_controller_map[f_extns]
        if io_type == "read":
            method_name = method_names[0]
        elif io_type == "write":
            method_name = method_names[1]
        else:
            raise ValueError("io_type can only be 'read' or 'write. Recieved "
                             f"'{io_type}'")
        return getattr(self, method_name)

    def _read_fcache(self, name, *args, **kwargs):
        """read cache for given name

        Args:
            name (str): name of cache to read

            (*args, **kwargs): passed through to read_cache method of
                io controller
        """
        f_controller_dict = self._f_controllers[name]
        path = f_controller_dict["path"]
        args = f_controller_dict["args"]
        kwargs = f_controller_dict["kwargs"]
        method = self._infer_controller_method(path, "read")
        return method(path, *args, **kwargs)

    def _write_fcache(self, name, val, path, *args, **kwargs):
        """write cache for given name
        Args:
            name (str): name of cache to read
            val (obj): object to write to file cache

            (*args, **kwargs): passed through to write_cache method of
                io controller

        Returns:
            cache data produced by underlying io controller
        """
        # write new results
        method = self._infer_controller_method(path, "write")
        method(val, path, *args, **kwargs)

        # clear old cache. this method leaves the method buried under a
        # partial so we have to dig in a bit
        method = self._infer_controller_method(path, "read")
        try:
            self._memoized[method.func.func] = {}
        except (KeyError, AttributeError):
            pass

        # store params
        self._f_controllers[name] = {
            "path": path,
            "args": args,
            "kwargs": kwargs
        }

    def get_cached(self, name, *args, **kwargs):
        try:
            val = self._read_fcache(name, *args, **kwargs)
        except KeyError:
            raise NoCachedValueFound(f"no cached value for {name}")
        return val

    def set_cached(self, name, val, path, *args, **kwargs):
        self._write_fcache(name, val, path, *args, **kwargs)


class NonfatalDimensions:

    def __init__(self, simulation_index, draw_index, cause_index,
                 sequela_index, impairment_index, injuries_index):
        self.simulation_index = simulation_index
        self.draw_index = draw_index
        self.cause_index = cause_index
        self.sequela_index = sequela_index
        self.impairment_index = impairment_index
        self.injuries_index = injuries_index

    def get_dimension_by_component(self, component, measure_id):
        if component == "cause":
            at_birth = False
            if measure_id == measures.INCIDENCE:
                at_birth = True
            dimensions = self.get_cause_dimensions(measure_id, at_birth)
        if component == "impairment":
            dimensions = self.get_impairment_dimensions(measure_id)
        if component == "sequela":
            at_birth = False
            if measure_id == measures.INCIDENCE:
                at_birth = True
            dimensions = self.get_sequela_dimensions(measure_id, at_birth)
        if component == "injuries":
            dimensions = self.get_injuries_dimensions(measure_id)
        return dimensions

    def get_simulation_dimensions(self, measure_id, at_birth=False):
        sim_idx = deepcopy(self.simulation_index)
        draw_idx = deepcopy(self.draw_index)
        dim = DataFrameDimensions(sim_idx, draw_idx)
        dim.index_dim.add_level("measure_id",
                                np.atleast_1d(measure_id).tolist())
        if at_birth:
            dim.index_dim.extend_level("age_group_id", [age.BIRTH])
        return dim

    def get_cause_dimensions(self, measure_id, at_birth=False):
        base_dim = self.get_simulation_dimensions(measure_id, at_birth)
        for key, val in list(self.cause_index.items()):
            base_dim.index_dim.add_level(key, val)
        return base_dim

    def get_sequela_dimensions(self, measure_id, at_birth=False):
        base_dim = self.get_simulation_dimensions(measure_id, at_birth)
        for key, val in list(self.sequela_index.items()):
            base_dim.index_dim.add_level(key, val)
        return base_dim

    def get_impairment_dimensions(self, measure_id, at_birth=False):
        base_dim = self.get_simulation_dimensions(measure_id, at_birth)
        for key, val in list(self.impairment_index.items()):
            base_dim.index_dim.add_level(key, val)
        return base_dim

    def get_injuries_dimensions(self, measure_id, at_birth=False):
        base_dim = self.get_simulation_dimensions(measure_id, at_birth)
        for key, val in list(self.injuries_index.items()):
            base_dim.index_dim.add_level(key, val)
        return base_dim


class ComoVersion:

    def __init__(self, como_dir):
        self.generate_version_directories(como_dir)
        self.como_dir = como_dir
        self._cache = FCache(self._cache_dir)

    @classmethod
    def new(cls, root_dir, gbd_round_id, decomp_step_id, location_set_id,
            year_id, measure_id, n_draws, components, change_years,
            agg_loc_sets):
        # determine run of como and make new version in the epi db
        como_version_id = cls.generate_new_version(
            gbd_round_id, decomp_step_id, como.__version__)
        como_dir = os.path.join(root_dir, str(como_version_id))
        inst = cls(como_dir)
        # set up constants
        inst.como_version_id = como_version_id
        inst.components = components
        inst.gbd_round_id = gbd_round_id
        inst.decomp_step_id = decomp_step_id
        inst.change_years = change_years
        inst.measure_id = measure_id
        # create a matching process version in the gbd database
        inst.create_gbd_process_version(como.__version__)
        # pull active set version for given entities
        inst.new_cause_set_version_id()
        inst.new_sequela_set_version_id()
        inst.new_inc_cause_set_version_id()
        inst.new_reporting_cause_set_version_id()
        inst.new_location_set_version_id(location_set_id)
        # get lists of all best epi model versions, active causes, and sequela
        inst.new_mvid_list()
        inst.new_cause_list()
        inst.new_sequela_list()
        # pulling a bunch of lists of expecptions and one off things
        inst.new_agg_cause_exceptions()
        inst.new_agg_cause_map()
        inst.new_cause_restrictions()
        inst.new_birth_prev()
        inst.new_injury_sequela()
        inst.new_sexual_violence_sequela()
        inst.new_injury_dws_by_sequela()
        inst.new_impairment_sequela()
        # impariment and ncode hierarchy dfs
        inst.new_impairment_hierarchy()
        inst.new_ncode_hierarchy()
        # dictionaries of dimensions/indexes for different entities
        inst.new_simulation_index(year_id=year_id)
        inst.new_draw_index(n_draws=n_draws)
        inst.new_cause_index()
        inst.new_sequela_index()
        inst.new_impairment_index()
        inst.new_injuries_index()
        # read some other dfs and cache them accordingly
        inst.new_global_ratios(year_id=year_id, n_draws=n_draws)
        inst._cache.dump_config()
        inst.new_population(location_set_id, agg_loc_sets=agg_loc_sets)
        inst.new_epilepsy_dws()
        inst.new_urolith_dws(location_set_id)
        return inst

    @classmethod
    def generate_new_version(cls, gbd_round_id, decomp_step_id, code_version):
        session = ezfuncs.get_session(conn_def="epi")
        try:
            session.execute(
                """
                INSERT INTO epi.output_version (
                    gbd_round_id,
                    decomp_step_id,
                    username,
                    description,
                    code_version,
                    status,
                    is_best
                )
                VALUES(
                    :gbd_round_id,
                    :decomp_step_id,
                    :user,
                    :description,
                    :code_v,
                    :running,
                    :not_best)
                """,
                params={
                    'gbd_round_id': gbd_round_id,
                    'decomp_step_id': decomp_step_id,
                    'user': getpass.getuser(),
                    'description': 'Central COMO run',
                    'code_v': gbd_round_id-1,
                    'running': 0,
                    'not_best': 0
                    })
            session.flush()
            como_version_id = session.execute("select LAST_INSERT_ID()"
                                              ).scalar()
            session.commit()
        except Exception:
            session.rollback()
            raise
        return como_version_id

    @property
    def _cache_dir(self):
        return os.path.join(self.como_dir, "info")

    @property
    def como_version_id(self):
        return self._cache.get_cached("como_version_id")

    @como_version_id.setter
    def como_version_id(self, val):
        self._cache.set_cached(
            "como_version_id",
            val,
            os.path.join(self._cache_dir, "como_version_id.json"))

    @property
    def gbd_process_version_id(self):
        return self._cache.get_cached("gbd_process_version_id")

    @gbd_process_version_id.setter
    def gbd_process_version_id(self, val):
        self._cache.set_cached(
            "gbd_process_version_id",
            val,
            os.path.join(self._cache_dir, "gbd_process_version_id.json"))

    @property
    def components(self):
        return self._cache.get_cached("components")

    @components.setter
    def components(self, val):
        self._cache.set_cached(
            "components",
            val,
            os.path.join(self._cache_dir, "components.json"))

    @property
    def gbd_round_id(self):
        return self._cache.get_cached("gbd_round_id")

    @gbd_round_id.setter
    def gbd_round_id(self, val):
        self._cache.set_cached(
            "gbd_round_id",
            val,
            os.path.join(self._cache_dir, "gbd_round_id.json"))

    @property
    def decomp_step_id(self):
        return self._cache.get_cached("decomp_step_id")

    @decomp_step_id.setter
    def decomp_step_id(self, val):
        self._cache.set_cached(
            "decomp_step_id",
            val,
            os.path.join(self._cache_dir, "decomp_step_id.json"))

    @property
    def change_years(self):
        return self._cache.get_cached("change_years")

    @change_years.setter
    def change_years(self, val):
        self._cache.set_cached(
            "change_years",
            val,
            os.path.join(self._cache_dir, "change_years.json"))

    @property
    def measure_id(self):
        return self._cache.get_cached("measure_id")

    @measure_id.setter
    def measure_id(self, val):
        self._cache.set_cached(
            "measure_id",
            val,
            os.path.join(self._cache_dir, "measure_id.json"))

    @property
    def cause_list(self):
        return self._cache.get_cached("cause_list")

    @cause_list.setter
    def cause_list(self, val):
        self._cache.set_cached(
            "cause_list",
            val,
            os.path.join(self._cache_dir, "cause_list.csv"))

    @property
    def mvid_list(self):
        return self._cache.get_cached("mvid_list")

    @mvid_list.setter
    def mvid_list(self, val):
        self._cache.set_cached(
            "mvid_list",
            val,
            os.path.join(self._cache_dir, "mvid_list.csv"))

    @property
    def sequela_list(self):
        return self._cache.get_cached("sequela_list")

    @sequela_list.setter
    def sequela_list(self, val):
        self._cache.set_cached(
            "sequela_list",
            val,
            os.path.join(self._cache_dir, "sequela_list.csv"))

    @property
    def sequela_set_version_id(self):
        return self._cache.get_cached("sequela_set_version_id")

    @sequela_set_version_id.setter
    def sequela_set_version_id(self, val):
        self._cache.set_cached(
            "sequela_set_version_id",
            val,
            os.path.join(self._cache_dir, "sequela_set_version_id.json"))

    @property
    def cause_set_version_id(self):
        return self._cache.get_cached("cause_set_version_id")

    @cause_set_version_id.setter
    def cause_set_version_id(self, val):
        self._cache.set_cached(
            "cause_set_version_id",
            val,
            os.path.join(self._cache_dir, "cause_set_version_id.json"))

    @property
    def inc_cause_set_version_id(self):
        return self._cache.get_cached("inc_cause_set_version_id")

    @inc_cause_set_version_id.setter
    def inc_cause_set_version_id(self, val):
        self._cache.set_cached(
            "inc_cause_set_version_id",
            val,
            os.path.join(self._cache_dir, "inc_cause_set_version_id.json"))

    @property
    def reporting_cause_set_version_id(self):
        return self._cache.get_cached("reporting_cause_set_version_id")

    @reporting_cause_set_version_id.setter
    def reporting_cause_set_version_id(self, val):
        self._cache.set_cached(
            "reporting_cause_set_version_id",
            val,
            os.path.join(self._cache_dir, "reporting_cause_set_version_id.json"))

    @property
    def location_set_version_id(self):
        return self._cache.get_cached("location_set_version_id")

    @location_set_version_id.setter
    def location_set_version_id(self, val):
        self._cache.set_cached(
            "location_set_version_id",
            val,
            os.path.join(self._cache_dir, "location_set_version_id.json"))

    @property
    def incidence_agg_exclusions(self):
        return self._cache.get_cached("incidence_agg_exclusions")

    @incidence_agg_exclusions.setter
    def incidence_agg_exclusions(self, val):
        self._cache.set_cached(
            "incidence_agg_exclusions",
            val,
            os.path.join(self._cache_dir, "incidence_agg_exclusions.json"))

    @property
    def agg_cause_exceptions(self):
        return self._cache.get_cached("agg_cause_exceptions")

    @agg_cause_exceptions.setter
    def agg_cause_exceptions(self, val):
        self._cache.set_cached(
            "agg_cause_exceptions",
            val,
            os.path.join(self._cache_dir, "agg_cause_exceptions.csv"))

    @property
    def agg_cause_map(self):
        return self._cache.get_cached("agg_cause_map")

    @agg_cause_map.setter
    def agg_cause_map(self, val):
        self._cache.set_cached(
            "agg_cause_map",
            val,
            os.path.join(self._cache_dir, "agg_cause_map.csv"))

    @property
    def cause_restrictions(self):
        return self._cache.get_cached("cause_restrictions")

    @cause_restrictions.setter
    def cause_restrictions(self, val):
        self._cache.set_cached(
            "cause_restrictions",
            val,
            os.path.join(self._cache_dir, "cause_restrictions.csv"))

    @property
    def birth_prev(self):
        return self._cache.get_cached("birth_prev")

    @birth_prev.setter
    def birth_prev(self, val):
        self._cache.set_cached(
            "birth_prev",
            val,
            os.path.join(self._cache_dir, "birth_prev.csv"))

    @property
    def injury_sequela(self):
        return self._cache.get_cached("injury_sequela")

    @injury_sequela.setter
    def injury_sequela(self, val):
        self._cache.set_cached(
            "injury_sequela",
            val,
            os.path.join(self._cache_dir, "injury_sequela.csv"))

    @property
    def sexual_violence_sequela(self):
        return self._cache.get_cached("sexual_violence_sequela")

    @sexual_violence_sequela.setter
    def sexual_violence_sequela(self, val):
        self._cache.set_cached(
            "sexual_violence_sequela",
            val,
            os.path.join(self._cache_dir, "sexual_violence_sequela.csv"))

    @property
    def injury_dws_by_sequela(self):
        return self._cache.get_cached("injury_dws_by_sequela")

    @injury_dws_by_sequela.setter
    def injury_dws_by_sequela(self, val):
        self._cache.set_cached(
            "injury_dws_by_sequela",
            val,
            os.path.join(self._cache_dir, "injury_dws_by_sequela.csv"))

    @property
    def impairment_sequela(self):
        return self._cache.get_cached("impairment_sequela")

    @impairment_sequela.setter
    def impairment_sequela(self, val):
        self._cache.set_cached(
            "impairment_sequela",
            val,
            os.path.join(self._cache_dir, "impairment_sequela.csv"))

    @property
    def impairment_hierarchy(self):
        return self._cache.get_cached("impairment_hierarchy")

    @impairment_hierarchy.setter
    def impairment_hierarchy(self, val):
        self._cache.set_cached(
            "impairment_hierarchy",
            val,
            os.path.join(self._cache_dir, "impairment_hierarchy.csv"))

    @property
    def ncode_hierarchy(self):
        return self._cache.get_cached("ncode_hierarchy")

    @ncode_hierarchy.setter
    def ncode_hierarchy(self, val):
        self._cache.set_cached(
            "ncode_hierarchy",
            val,
            os.path.join(self._cache_dir, "ncode_hierarchy.csv"))

    @property
    def simulation_index(self):
        return self._cache.get_cached("simulation_index")

    @simulation_index.setter
    def simulation_index(self, val):
        self._cache.set_cached(
            "simulation_index",
            val,
            os.path.join(self._cache_dir, "simulation_index.dim"))

    @property
    def draw_index(self):
        return self._cache.get_cached("draw_index")

    @draw_index.setter
    def draw_index(self, val):
        self._cache.set_cached(
            "draw_index",
            val,
            os.path.join(self._cache_dir, "draw_index.dim"))

    @property
    def cause_index(self):
        return self._cache.get_cached("cause_index")

    @cause_index.setter
    def cause_index(self, val):
        self._cache.set_cached(
            "cause_index",
            val,
            os.path.join(self._cache_dir, "cause_index.dim"))

    @property
    def sequela_index(self):
        return self._cache.get_cached("sequela_index")

    @sequela_index.setter
    def sequela_index(self, val):
        self._cache.set_cached(
            "sequela_index",
            val,
            os.path.join(self._cache_dir, "sequela_index.dim"))

    @property
    def impairment_index(self):
        return self._cache.get_cached("impairment_index")

    @impairment_index.setter
    def impairment_index(self, val):
        self._cache.set_cached(
            "impairment_index",
            val,
            os.path.join(self._cache_dir, "impairment_index.dim"))

    @property
    def injuries_index(self):
        return self._cache.get_cached("injuries_index")

    @injuries_index.setter
    def injuries_index(self, val):
        self._cache.set_cached(
            "injuries_index",
            val,
            os.path.join(self._cache_dir, "injuries_index.dim"))

    @property
    def global_ratios(self):
        return self._cache.get_cached("global_ratios")

    @global_ratios.setter
    def global_ratios(self, val):
        self._cache.set_cached(
            "global_ratios",
            val,
            os.path.join(self._cache_dir, "global_ratios.csv"))

    @property
    def nonfatal_dimensions(self):
        sim_idx = deepcopy(self.simulation_index)
        draw_idx = deepcopy(self.draw_index)
        cause_idx = deepcopy(self.cause_index)
        seq_idx = deepcopy(self.sequela_index)
        imp_idx = deepcopy(self.impairment_index)
        inj_idx = deepcopy(self.injuries_index)
        return NonfatalDimensions(
            sim_idx, draw_idx, cause_idx, seq_idx, imp_idx, inj_idx)

    def generate_version_directories(self, como_dir):
        try:
            os.makedirs(como_dir)
            os.chmod(como_dir, 0o775)
        except Exception:
            pass
        with open(f"{THIS_PATH}/config/dirs.config") as dirs_file:
            for d in dirs_file.readlines():
                if d.strip() != "":
                    try:
                        full_path = os.path.join(como_dir, d.strip("\r\n"))
                        os.makedirs(full_path)
                        os.chmod(full_path, 0o775)
                    except Exception:
                        pass

    def load_cache(self):
        self._cache.load_config()

    def new_cause_set_version_id(self):
        # 9 - Non-fatal Capstone
        nonfatal_cause_set = 9
        self.cause_set_version_id = ezfuncs.query(
            "SELECT shared.active_cause_set_version(:cause_set,:gbd_round_id)",
            conn_def="epi",
            parameters={
                'cause_set': nonfatal_cause_set,
                "gbd_round_id": self.gbd_round_id
                }).values.item()

    def new_sequela_set_version_id(self):
        self.sequela_set_version_id = ezfuncs.query(
            """
            SELECT
                sequela_set_version_id
            FROM
                epic.sequela_set_version_active
            WHERE
                gbd_round_id = :gbd_round_id
            """,
            conn_def="epic",
            parameters={"gbd_round_id": self.gbd_round_id}
            ).values.item()

    def new_inc_cause_set_version_id(self):
        # 12 - GBD nonfatal incidence
        inc_cause_set = 12
        self.inc_cause_set_version_id = ezfuncs.query(
            "SELECT shared.active_cause_set_version(:cause_set,:gbd_round_id)",
            conn_def="epi",
            parameters={
                'cause_set': inc_cause_set,
                "gbd_round_id": self.gbd_round_id
                }).values.item()

    def new_reporting_cause_set_version_id(self):
        # 16 - Reporting only cause aggregates
        reporting_cause_set = 16
        self.reporting_cause_set_version_id = ezfuncs.query(
            "SELECT shared.active_cause_set_version(:cause_set,:gbd_round_id)",
            conn_def="epi",
            parameters={
                'cause_set': reporting_cause_set,
                "gbd_round_id": self.gbd_round_id
                }).values.item()

    def new_location_set_version_id(self, location_set_id):
        self.location_set_version_id = ezfuncs.query(
            """
            SELECT shared.active_location_set_version(
                :location_set_id, :gbd_round_id)
            """,
            conn_def="epi",
            parameters={
                "location_set_id": location_set_id,
                "gbd_round_id": self.gbd_round_id
                }).values.item()

    def new_cause_list(self):
        cause_list = get_ids(table="cause")
        self.cause_list = cause_list[["cause_id", "acause"]]

    def new_mvid_list(self):
        mvid_list = ezfuncs.query(
            """
            SELECT
                cause.cause_id, mv.modelable_entity_id,
                mv.model_version_id, mv.decomp_step_id
            FROM epi.model_version mv
            LEFT JOIN epi.modelable_entity_cause cause
                ON mv.modelable_entity_id = cause.modelable_entity_id
            WHERE
                mv.model_version_status_id = :best
                and mv.gbd_round_id = :gbd_round_id
                and mv.decomp_step_id IN (:decomp_step_id, :iterative)
            """,
            conn_def="epi",
            parameters={
                "best": 1,
                "gbd_round_id": self.gbd_round_id,
                "decomp_step_id": self.decomp_step_id,
                "iterative": decomp_step_id_from_decomp_step(
                    step="iterative", gbd_round_id=self.gbd_round_id)
                })

        # Determine which decomp step to find model version in
        all_ntd_cause = [346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356,
                         357, 358, 359, 360, 361, 362, 363, 364, 365, 405, 843,
                         935, 936]
        ntd_decomp_me = [1500, 1503, 10402, 1513, 1514, 1515, 2999, 3109,
                         20265, 1516, 1517, 1518, 3001, 3110, 20266, 1519,
                         1520, 1521, 3000, 3139, 3111, 20009, 2797, 1474,
                         1469, 2965, 1475, 10524, 10525, 1476, 2966, 1470,
                         1471, 10480, 1477, 10537, 1472, 1468, 1466, 1473,
                         1465, 16393, 1478]
        new_gbd_2019_cause = [1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011,
                              1012, 1013, 1014, 1015, 1016, 1017, 628]
        has_decomp_version = mvid_list.loc[
            mvid_list.decomp_step_id == self.decomp_step_id,
            ].modelable_entity_id.unique().tolist()
        use_iterative = mvid_list.loc[
            ~mvid_list.modelable_entity_id.isin(ntd_decomp_me)
            & mvid_list.cause_id.isin(all_ntd_cause + new_gbd_2019_cause)
            & ~mvid_list.modelable_entity_id.isin(has_decomp_version)
            ].modelable_entity_id.unique().tolist()
        mvid_list = mvid_list.loc[
            (mvid_list.decomp_step_id == self.decomp_step_id) |
            (mvid_list.modelable_entity_id.isin(use_iterative))]
        self.mvid_list = mvid_list[
            ['modelable_entity_id', 'model_version_id', 'decomp_step_id']]

    def new_sequela_list(self):
        # We exclude healthstate_id 639 as this is a residual
        # healthstate_id 639 - Post-COMO calculation for residuals
        #   (YLL/YLD ratio, other methods)
        self.sequela_list = ezfuncs.query(
            """
            SELECT
                sequela_id, cause_id, modelable_entity_id, healthstate_id
            FROM
                epic.sequela_hierarchy_history
            WHERE
                sequela_set_version_id = :sequela_set_version_id
                AND most_detailed = 1
                AND healthstate_id != 639
            """,
            conn_def="epic",
            parameters={"sequela_set_version_id": self.sequela_set_version_id})

    def new_agg_cause_exceptions(self):
        ct = dbtrees.causetree(
            cause_set_version_id=self.cause_set_version_id).db_format()
        ct = ct.rename(columns={"location_id": "cause_id"})
        # For these causes, the sum of underlying cause is equal to the parent
        # cause. This is an exception as prevelence is not usually additive at
        # the cause, just the sequela level.
        # 297 - Tuberculosis
        # 298 - HIV/AIDS
        # 400 - Acute hepatitis
        # 417 - Liver cancer
        # 487 - Leukemia
        # 510 - Pneumoconiosis
        # 521 - Cirrhosis and other chronic liver diseases
        # 587 - Diabetes mellitus
        # 589 - Chronic kidney disease
        ct = ct[ct.parent_id.isin([589, 400, 521, 417, 298, 297, 510, 487,
                                   587])]
        ct = ct[["cause_id", "parent_id"]]
        self.agg_cause_exceptions = ct

    def new_agg_cause_map(self):
        ct = dbtrees.causetree(
            cause_set_version_id=self.cause_set_version_id)
        acm = []
        for n in ct.nodes:
            if (n.all_descendants() and n.id not in (
                    self.agg_cause_exceptions.parent_id.unique().tolist())):
                leaves = [l.id for l in n.leaves()]
                ac_seq = self.sequela_list[
                    self.sequela_list.cause_id.isin(leaves)]
                ac_seq['cause_id'] = n.id
                acm.append(ac_seq)
        acm = pd.concat(acm)
        self.agg_cause_map = acm

    def new_cause_restrictions(self):
        restrictions = ezfuncs.query(
            """
            SELECT
                cause_id, male, female,
                age_start.yld_age_start, age_end.yld_age_end,
                cause_set_version_id
            FROM shared.cause_hierarchy_history chh
            LEFT JOIN (
                SELECT cause_id, cause_metadata_value as yld_age_start
                FROM shared.cause_set_version
                JOIN shared.cause_metadata_history
                    using(cause_metadata_version_id)
                WHERE
                    cause_set_version_id = :cause_set_version_id
                    and cause_metadata_type_id=23) age_start using(cause_id)
            LEFT JOIN (
                SELECT cause_id, cause_metadata_value as yld_age_end
                FROM shared.cause_set_version
                JOIN shared.cause_metadata_history
                    using(cause_metadata_version_id)
                WHERE
                    cause_set_version_id = :cause_set_version_id
                    and cause_metadata_type_id=24) age_end using(cause_id)
            WHERE chh.cause_set_version_id = :cause_set_version_id
            """,
            conn_def="epi",
            parameters={"cause_set_version_id": self.cause_set_version_id})
        # Dummy injury restrictions
        restrictions = restrictions.append(pd.DataFrame([{
            'cause_id': -1, 'male': 1, 'female': 1, 'yld_age_start': 2,
            'yld_age_end': 235}]))
        self.cause_restrictions = restrictions

    def new_birth_prev(self):
        self.birth_prev = pd.read_csv(f"{THIS_PATH}/config/birth_prev.csv")

    def new_injury_sequela(self):
        self.injury_sequela = pd.read_csv(f"{THIS_PATH}/config/inj_meid.csv")

    def new_injury_dws_by_sequela(self):
        ismap = pd.read_csv(f"{THIS_PATH}/config/inj_meid.csv")
        ismap = ismap[['n_code', 'sequela_id', "healthstate_id"]]
        ismap = ismap[ismap.duplicated()]
        self.injury_dws_by_sequela = ismap

    def new_sexual_violence_sequela(self):
        self.sexual_violence_sequela = pd.read_csv(
            f"{THIS_PATH}/config/sexual_violence_me_id.csv")

    def new_impairment_sequela(self):
        self.impairment_sequela = ezfuncs.query(
            """
            SELECT
                sequela_id, rei_id
            FROM
                epic.sequela_rei_history
            WHERE
                sequela_set_version_id = :sequela_set_version_id
            """,
            conn_def="epic",
            parameters={"sequela_set_version_id": self.sequela_set_version_id})

    def new_impairment_hierarchy(self):
        # 4 - GBD Reporting Impairments
        imp_hie = get_rei_metadata(
            rei_set_id=4, gbd_round_id=self.gbd_round_id)
        self.impairment_hierarchy = imp_hie[
            ["rei_id", "parent_id", "rei", "rei_set_id"]]

    def new_ncode_hierarchy(self):
        # 7 - GBD Injuries
        ncode_hie = get_rei_metadata(
            rei_set_id=7, gbd_round_id=self.gbd_round_id)
        self.ncode_hierarchy = ncode_hie[
            ["rei_id", "parent_id", "rei", "rei_set_id"]]

    def new_simulation_index(self, year_id):
        lt = dbtrees.loctree(
            location_set_version_id=self.location_set_version_id)
        location_id = [loc.id for loc in lt.leaves()]
        demo = get_demographics(gbd_team="epi", gbd_round_id=self.gbd_round_id)
        if not year_id:
            year_id = demo['year_id']
        simulation_index = {
            "year_id": year_id,
            "location_id": location_id,
            "sex_id": demo['sex_id'],
            "age_group_id": demo['age_group_id']
        }
        self.simulation_index = simulation_index

    def new_draw_index(self, n_draws):
        draw_index = {
            "draws": [f'draw_{d}' for d in range(n_draws)]
        }
        self.draw_index = draw_index

    def new_cause_index(self):
        ctree = dbtrees.causetree(
            cause_set_version_id=self.cause_set_version_id)
        cause_id = [node.id for node in ctree.nodes]
        self.cause_index = {"cause_id": cause_id}

    def new_sequela_index(self):
        seq_tree = dbtrees.sequelatree(
            sequela_set_version_id=self.sequela_set_version_id)
        seq_ids = [node.id for node in seq_tree.nodes]
        self.sequela_index = {"sequela_id": seq_ids}

    def new_impairment_index(self):
        rei_keys = ["rei_id", "cause_id"]
        sequela_cause = self.sequela_list[["sequela_id", "cause_id"]]
        imp_pairs = sequela_cause.merge(self.impairment_sequela)
        imp_pairs = imp_pairs[rei_keys]
        imp_pairs = imp_pairs[~imp_pairs.duplicated()]
        cause_tree = dbtrees.causetree(
            cause_set_version_id=self.cause_set_version_id)
        imp_pairs = propagate_hierarchy(cause_tree, imp_pairs, "cause_id")
        # 4 - GBD Reporting Impairments
        rei_tree = dbtrees.reitree(
            rei_set_id=4, gbd_round_id=self.gbd_round_id)
        imp_pairs = propagate_hierarchy(rei_tree, imp_pairs, "rei_id")
        # rei_id 191 - Impairments (this is the aggregate)
        imp_pairs = imp_pairs[imp_pairs.rei_id != 191]
        rei_vals = list(set(tuple(x) for x in imp_pairs[rei_keys].values))
        self.impairment_index = {tuple(rei_keys): rei_vals}

    def new_injuries_index(self):
        inj_keys = ["cause_id", "rei_id"]
        inj_pairs = self.injury_sequela[inj_keys]
        inj_pairs = inj_pairs[~inj_pairs.duplicated()]
        cause_tree = dbtrees.causetree(
            cause_set_version_id=self.cause_set_version_id)
        inj_pairs = propagate_hierarchy(cause_tree, inj_pairs, "cause_id")
        agg_pairs = inj_pairs.merge(self.ncode_hierarchy)
        agg_pairs = agg_pairs.drop("rei_id", axis=1)
        agg_pairs = agg_pairs.rename(columns={"parent_id": "rei_id"})
        inj_pairs = inj_pairs.append(agg_pairs[inj_keys])
        inj_pairs = inj_pairs[~inj_pairs.duplicated()]
        inj_vals = list(set(tuple(x) for x in inj_pairs[inj_keys].values))
        self.injuries_index = {tuple(inj_keys): inj_vals}

    def new_global_ratios(self, year_id, n_draws):
        df = compute_global_ratios(
            cause_id=self.cause_list['cause_id'].values,
            year_id=year_id,
            gbd_round_id=self.gbd_round_id,
            decomp_step_id=self.decomp_step_id,
            n_draws=n_draws)
        self.global_ratios = df

    def new_population(self, location_set_id, agg_loc_sets=None):
        dim = self.nonfatal_dimensions.get_simulation_dimensions(
            self.measure_id)
        df = get_population(
            age_group_id=(
                dim.index_dim.get_level("age_group_id") + [age.BIRTH]),
            location_id=dbtrees.loctree(location_set_id=location_set_id,
                                        gbd_round_id=self.gbd_round_id
                                        ).node_ids,
            sex_id=dim.index_dim.get_level("sex_id"),
            year_id=dim.index_dim.get_level("year_id"),
            gbd_round_id=self.gbd_round_id,
            decomp_step=decomp_step_from_decomp_step_id(self.decomp_step_id))
        index_cols = ["location_id", "year_id", "age_group_id", "sex_id"]
        data_cols = ["population"]

        io_mock = {}
        source = DrawSource({"draw_dict": io_mock, "name": "tmp"},
                            mem_read_func)
        sink = DrawSink({"draw_dict": io_mock, "name": "tmp"}, mem_write_func)
        sink.push(df[index_cols + data_cols])

        # location
        if agg_loc_sets:
            for set_id in agg_loc_sets:
                operator = Sum(
                        index_cols=[col for col in index_cols if col != "location_id"],
                        value_cols=data_cols)
                aggregator = AggSynchronous(
                    draw_source=source,
                    draw_sink=sink,
                    index_cols=[col for col in index_cols if col != "location_id"],
                    aggregate_col="location_id",
                    operator=operator)
                loc_trees = dbtrees.loctree(
                    location_set_id=set_id,
                    gbd_round_id=self.gbd_round_id,
                    return_many=True)
                for loc_tree in loc_trees:
                    aggregator.run(loc_tree)

        # age
        for age_group_id in ComoSummaries._gbd_compare_age_group_list:
            age_tree = dbtrees.agetree(
                age_group_id=age_group_id, gbd_round_id=self.gbd_round_id)
            operator = Sum(
                index_cols=[col for col in index_cols if col != "age_group_id"
                            ],
                value_cols=data_cols)
            aggregator = AggSynchronous(
                draw_source=source,
                draw_sink=sink,
                index_cols=[col for col in index_cols if col != "age_group_id"
                            ],
                aggregate_col="age_group_id",
                operator=operator)
            aggregator.run(age_tree)

        # sex
        sex_tree = dbtrees.sextree()
        operator = Sum(
            index_cols=[col for col in index_cols if col != "sex_id"],
            value_cols=data_cols)
        aggregator = AggSynchronous(
            draw_source=source,
            draw_sink=sink,
            index_cols=[col for col in index_cols if col != "sex_id"],
            aggregate_col="sex_id",
            operator=operator)
        aggregator.run(sex_tree)
        df = source.content()
        df.to_hdf(
            f"{self.como_dir}/info/population.h5",
            'draws',
            mode='w',
            format='table',
            data_columns=["location_id", "year_id", "age_group_id", "sex_id"])

    def new_epilepsy_dws(self):
        combine_epilepsy_any.epilepsy_any(self)
        combine_epilepsy_subcombos.epilepsy_combos(self.como_dir)

    def new_urolith_dws(self, location_set_id):
        urolithiasis.to_como(como_dir=self.como_dir,
                             location_set_id=location_set_id,
                             gbd_round_id=self.gbd_round_id)

    def unmark_current_best(self):
        session = ezfuncs.get_session(conn_def="epi")
        session.execute(
            """
            UPDATE epi.output_version
            SET
                best_end=NOW(),
                is_best=:not_best
            WHERE
                is_best=:best AND
                gbd_round_id=:gbd_round_id AND
                decomp_step_id=:decomp_step_id AND
                output_version_id <> :version_id
            """,
            params={
                'not_best': 0,
                'best': 1,
                'gbd_round_id': self.gbd_round_id,
                'decomp_step_id': self.decomp_step_id,
                'version_id': self.como_version_id
                })
        session.commit()

    def mark_best(self):
        self.unmark_current_best()
        session = ezfuncs.get_session(conn_def="epi")
        session.execute(
            """
            UPDATE epi.output_version
            SET
                status=:complete,
                best_start=NOW(),
                best_end=NULL,
                is_best=:best,
                best_description=:description,
                best_user=:user
            WHERE output_version_id=:version
            """,
            params={
                'complete': 1,
                'best': 1,
                'description': 'Central COMO run',
                'user': getpass.getuser(),
                'version': self.como_version_id
            })
        session.commit()

    def create_gbd_process_version(self, code_version):
        gbd_metadata = {gbd_metadata_type.COMO: self.como_version_id}
        gbd_note = f'COMO v{self.como_version_id}'
        pv = GBDProcessVersion.add_new_version(
            gbd_process_id=gbd_process.EPI,
            gbd_process_version_note=gbd_note,
            code_version=code_version,
            gbd_round_id=self.gbd_round_id,
            decomp_step=decomp_step_from_decomp_step_id(self.decomp_step_id),
            metadata=gbd_metadata)
        self.gbd_process_version_id = pv.gbd_process_version_id
        return pv

    def create_compare_version(self):
        pv = GBDProcessVersion(self.gbd_process_version_id)
        pv._update_status(gbd_process_version_status.ACTIVE)
        description = f'COMO v{self.como_version_id}'
        cv = CompareVersion.add_new_version(
            gbd_round_id=self.gbd_round_id,
            decomp_step=decomp_step_from_decomp_step_id(self.decomp_step_id),
            compare_version_description=description)
        df = bleeding_edge(cv.compare_version_id)
        cv.add_process_version(df.gbd_process_version_id.tolist())
        cv._update_status(gbd_process_version_status.ACTIVE)
        return cv
