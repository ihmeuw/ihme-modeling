import os
import getpass
import json
from datetime import datetime
from copy import deepcopy
from ast import literal_eval

from functools32 import partial
import pandas as pd
import numpy as np

from hierarchies import dbtrees
from db_tools import ezfuncs
from db_queries import get_rei_metadata, get_population
from gbd_outputs_versions import CompareVersion, GBDProcessVersion
from ihme_dimensions.dimensionality import DataFrameDimensions
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_sources.io import mem_read_func, mem_write_func
from aggregator.operators import Sum
from aggregator.aggregators import AggSynchronous

from como.dws.combine import combine_epilepsy_any, combine_epilepsy_subcombos
from como.dws.urolithiasis import import_gbd2013
from como.residuals import compute_global_ratios
from como.common import propagate_hierarchy
from como.summarize import ComoSummaries

this_path = os.path.dirname(os.path.abspath(__file__))

# Set default file mask to readable-for all users
os.umask(0o0002)


class NoCacheFound(Exception):
    pass


class NoCachedValueFound(Exception):
    pass


class memoize_method(object):
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

        arg_key = (args[1:], frozenset(kw.items()))
        try:
            res = method_cache[arg_key]
        except KeyError:
            res = method_cache[arg_key] = self.func(*args, **kw)
        return res


class FCache(object):

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
            raise NoCacheFound("no cache found in {}".format(self.cache_dir))

    def dump_config(self):
        config_path = os.path.join(self.cache_dir, 'cache_config.json')
        with open(config_path, 'w') as config:
            json.dump(self._f_controllers, config)

    @memoize_method
    def _read_csv_cache(self, path):
        return pd.read_csv(path)

    def _write_csv_cache(self, val, path):
        val.to_csv(path, index=False)

    @memoize_method
    def _read_json_cache(self, path):
        with open(path) as j_path:
            return json.load(j_path)

    def _write_json_cache(self, val, path):
        with open(path, 'w') as j_path:
            json.dump(val, j_path)

    @memoize_method
    def _read_hdf_cache(path):
        pass

    def _write_hdf_cache(self, val, path):
        pass

    @memoize_method
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
                             .format(io_type))
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
            raise NoCachedValueFound("no cached value for {}".format(name))
        return val

    def set_cached(self, name, val, path, *args, **kwargs):
        self._write_fcache(name, val, path, *args, **kwargs)


class NonfatalDimensions(object):

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
            if measure_id == 6:
                at_birth = True
            dimensions = self.get_cause_dimensions(measure_id, at_birth)
        if component == "impairment":
            dimensions = self.get_impairment_dimensions(measure_id)
        if component == "sequela":
            at_birth = False
            if measure_id == 6:
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
            dim.index_dim.extend_level("age_group_id", [164])
        return dim

    def get_cause_dimensions(self, measure_id, at_birth=False):
        base_dim = self.get_simulation_dimensions(measure_id, at_birth)
        for key, val in self.cause_index.iteritems():
            base_dim.index_dim.add_level(key, val)
        return base_dim

    def get_sequela_dimensions(self, measure_id, at_birth=False):
        base_dim = self.get_simulation_dimensions(measure_id, at_birth)
        for key, val in self.sequela_index.iteritems():
            base_dim.index_dim.add_level(key, val)
        return base_dim

    def get_impairment_dimensions(self, measure_id, at_birth=False):
        base_dim = self.get_simulation_dimensions(measure_id, at_birth)
        for key, val in self.impairment_index.iteritems():
            base_dim.index_dim.add_level(key, val)
        return base_dim

    def get_injuries_dimensions(self, measure_id, at_birth=False):
        base_dim = self.get_simulation_dimensions(measure_id, at_birth)
        for key, val in self.injuries_index.iteritems():
            base_dim.index_dim.add_level(key, val)
        return base_dim


class ComoVersion(object):

    def __init__(self, como_dir):
        self.generate_version_directories(como_dir)
        self.como_dir = como_dir
        self._cache = FCache(self._cache_dir)

    @classmethod
    def new(cls, root_dir, gbd_round_id, location_set_id, year_id,
            measure_id, n_draws, components, change_years, agg_loc_sets=[]):
        como_version_id = cls.generate_new_version()
        como_dir = os.path.join(root_dir, str(como_version_id))
        inst = cls(como_dir)
        inst.como_version_id = como_version_id
        inst.components = components
        inst.gbd_round_id = gbd_round_id
        inst.change_years = change_years
        inst.measure_id = measure_id
        inst.create_gbd_process_version()
        inst.new_mvid_list()
        inst.new_cause_list()
        inst.new_sequela_list()
        inst.new_sequela_set_version_id()
        inst.new_cause_set_version_id()
        inst.new_inc_cause_set_version_id()
        inst.new_location_set_version_id(location_set_id)
        inst.new_agg_cause_exceptions()
        inst.new_agg_cause_map()
        inst.new_cause_restrictions()
        inst.new_birth_prev()
        inst.new_injury_sequela()
        inst.new_sexual_violence_sequela()
        inst.new_injury_dws_by_sequela()
        inst.new_impairment_sequela()
        inst.new_impairment_hierarchy()
        inst.new_ncode_hierarchy()
        inst.new_simulation_index(year_id=year_id)
        inst.new_draw_index(n_draws=n_draws)
        inst.new_cause_index()
        inst.new_sequela_index()
        inst.new_impairment_index()
        inst.new_injuries_index()
        inst.new_global_ratios(year_id=year_id, n_draws=n_draws)
        inst._cache.dump_config()

        inst.new_population(location_set_id, agg_loc_sets=agg_loc_sets)
        inst.new_epilepsy_dws()
        inst.new_urolith_dws()
        return inst

    @classmethod
    def generate_new_version(cls):
        session = ezfuncs.get_session(conn_def="como-epi")
        try:
            q = """
                INSERT INTO epi.output_version (
                    username,
                    description,
                    code_version,
                    status,
                    is_best
                )
                VALUES(
                    '{user}',
                    'Central COMO run',
                    '{code_version}',
                    0,
                    0)""".format(user=getpass.getuser(), code_version=4)
            session.execute(q)
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
        with open("{}/config/dirs.config".format(this_path)) as dirs_file:
            for d in dirs_file.readlines():
                if d.strip() != "":
                    try:
                        dir = os.path.join(como_dir, d.strip("\r\n"))
                        os.makedirs(dir)
                        os.chmod(dir, 0o775)
                    except Exception:
                        pass

    def load_cache(self):
        self._cache.load_config()

    def new_cause_list(self):
        q = """
        SELECT cause_id, acause
        FROM shared.cause
        WHERE end_date is NULL
        """
        self.cause_list = ezfuncs.query(q, conn_def="como-epi")

    def new_mvid_list(self):
        q = """
        SELECT
            modelable_entity_id, model_version_id
        FROM epi.model_version
        WHERE
            model_version_status_id = 1
            and gbd_round_id = {}""".format(self.gbd_round_id)
        self.mvid_list = ezfuncs.query(q, conn_def="como-epi")

    def new_sequela_list(self):
        q = """
        SELECT
            sequela_id, cause_id, modelable_entity_id, healthstate_id
        FROM
            epic.sequela_hierarchy_history
        WHERE
            sequela_set_version_id = (
                SELECT
                    sequela_set_version_id
                FROM
                    sequela_set_version_active
                WHERE gbd_round_id = {gbd_round_id})
            AND most_detailed = 1
            AND healthstate_id != 639
        """.format(gbd_round_id=self.gbd_round_id)
        self.sequela_list = ezfuncs.query(q, conn_def="como-epic")

    def new_sequela_set_version_id(self):
        q = """
            SELECT
                sequela_set_version_id
            FROM
                sequela_set_version_active
            WHERE gbd_round_id = {gbd_round_id}
        """.format(gbd_round_id=self.gbd_round_id)
        df = ezfuncs.query(q, conn_def="como-epic")
        self.sequela_set_version_id = df["sequela_set_version_id"].item()

    def new_cause_set_version_id(self):
        q = "select shared.active_cause_set_version(9,{})".format(
            self.gbd_round_id)
        self.cause_set_version_id = ezfuncs.query(q, conn_def="como-epi"
                                                  ).values.item()

    def new_inc_cause_set_version_id(self):
        q = "select shared.active_cause_set_version(12,{})".format(
            self.gbd_round_id)
        self.inc_cause_set_version_id = ezfuncs.query(q, conn_def="como-epi"
                                                      ).values.item()

    def new_location_set_version_id(self, location_set_id):
        q = "select shared.active_location_set_version({},{})".format(
            location_set_id, self.gbd_round_id)
        self.location_set_version_id = ezfuncs.query(q, conn_def="como-epi"
                                                     ).values.item()

    def new_agg_cause_exceptions(self):
        ct = dbtrees.causetree(
            cause_set_version_id=self.cause_set_version_id).db_format()
        ct = ct.rename(columns={"location_id": "cause_id"})
        ct = ct[ct.parent_id.isin([589, 400, 521, 426, 332, 298, 297, 510, 487,
                                   587])]
        ct = ct[["cause_id", "parent_id"]]
        self.agg_cause_exceptions = ct

    def new_agg_cause_map(self):
        ct = dbtrees.causetree(
            cause_set_version_id=self.cause_set_version_id)
        acm = []
        for n in ct.nodes:
            if (len(n.all_descendants()) > 0 and n.id not in (
                    self.agg_cause_exceptions.parent_id.unique().tolist())):
                leaves = [l.id for l in n.leaves()]
                ac_seq = self.sequela_list[
                    self.sequela_list.cause_id.isin(leaves)]
                ac_seq['cause_id'] = n.id
                acm.append(ac_seq)
        acm = pd.concat(acm)
        self.agg_cause_map = acm

    def new_cause_restrictions(self):
        q = """
        SELECT cause_id, male, female, yld_age_start, yld_age_end,
               cause_set_version_id
        FROM shared.cause_hierarchy_history chh
        JOIN shared.cause_set_version csv USING(cause_set_version_id)
        JOIN shared.cause_set cs ON csv.cause_set_id=cs.cause_set_id
        WHERE cause_set_version_id = {}
        """.format(self.cause_set_version_id)

        restrictions = ezfuncs.query(q, conn_def="como-epi")
        restrictions['yld_age_start'] = restrictions.yld_age_start.fillna(0)
        restrictions['yld_age_end'] = restrictions.yld_age_end.fillna(95)
        restrictions['yld_age_start'] = (
            restrictions.yld_age_start.round(2).astype(str))
        restrictions['yld_age_end'] = (
            restrictions.yld_age_end.round(2).astype(str))
        ridiculous_am = {
            '0.0': 2, '0.01': 3, '0.1': 4, '1.0': 5, '5.0': 6, '10.0': 7,
            '15.0': 8, '20.0': 9, '25.0': 10, '30.0': 11, '35.0': 12,
            '40.0': 13, '45.0': 14, '50.0': 15, '55.0': 16, '60.0': 17,
            '65.0': 18, '70.0': 19, '75.0': 20, '80.0': 30, '85.0': 31,
            '90.0': 32, '95.0': 235}
        restrictions['yld_age_start'] = (
            restrictions.yld_age_start.replace(ridiculous_am).astype(int))
        restrictions['yld_age_end'] = (
            restrictions.yld_age_end.replace(ridiculous_am).astype(int))

        # Dummy injury restrictions
        restrictions = restrictions.append(pd.DataFrame([{
            'cause_id': -1, 'male': 1, 'female': 1, 'yld_age_start': 2,
            'yld_age_end': 235}]))
        self.cause_restrictions = restrictions

    def new_birth_prev(self):
        self.birth_prev = pd.read_csv(
            "{}/config/birth_prev.csv".format(this_path))

    def new_injury_sequela(self):
        self.injury_sequela = pd.read_csv("{}/config/inj_meid.csv".format(
                                          this_path))

    def new_injury_dws_by_sequela(self):
        ismap = pd.read_csv("{}/config/inj_meid.csv".format(this_path))
        ismap = ismap[['n_code', 'sequela_id', "healthstate_id"]]
        ismap = ismap[ismap.duplicated()]
        self.injury_dws_by_sequela = ismap

    def new_sexual_violence_sequela(self):
        self.sexual_violence_sequela = pd.read_csv(
            "{}/config/sexual_violence_me_id.csv".format(this_path))

    def new_impairment_sequela(self):
        q = """
        SELECT
            sequela_id, rei_id
        FROM
            epic.sequela_rei_history
        WHERE
            sequela_set_version_id = (
                SELECT
                    sequela_set_version_id
                FROM
                    sequela_set_version_active
                WHERE gbd_round_id = {gbd_round_id})
        """.format(gbd_round_id=self.gbd_round_id)
        self.impairment_sequela = ezfuncs.query(q, conn_def="como-epic")

    def new_impairment_hierarchy(self):
        imp_hie = get_rei_metadata(
            rei_set_id=4, gbd_round_id=self.gbd_round_id)
        self.impairment_hierarchy = imp_hie[
            ["rei_id", "parent_id", "rei", "rei_set_id"]]

    def new_ncode_hierarchy(self):
        ncode_hie = get_rei_metadata(
            rei_set_id=7, gbd_round_id=self.gbd_round_id)
        self.ncode_hierarchy = ncode_hie[
            ["rei_id", "parent_id", "rei", "rei_set_id"]]

    def new_simulation_index(self, year_id=[]):
        lt = dbtrees.loctree(
            location_set_version_id=self.location_set_version_id)
        location_id = [loc.id for loc in lt.leaves()]
        age_group_id = [
            2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            19, 20, 30, 31, 32, 235]
        if not year_id:
            year_id = [1990, 1995, 2000, 2005, 2010, 2017]
        simulation_index = {
            "year_id": year_id,
            "location_id": location_id,
            "sex_id": [1, 2],
            "age_group_id": age_group_id
        }
        self.simulation_index = simulation_index

    def new_draw_index(self, n_draws=1000):
        draw_index = {
            "draws": ['draw_{}'.format(d) for d in range(n_draws)]
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
        rei_tree = dbtrees.reitree(rei_set_id=4)
        imp_pairs = propagate_hierarchy(rei_tree, imp_pairs, "rei_id")
        imp_pairs = imp_pairs[imp_pairs.rei_id != 191]
        rei_vals = list(set(tuple(x) for x in imp_pairs[rei_keys].values))
        self.impairment_index = {tuple(rei_keys): rei_vals}

    def new_injuries_index(self):
        inj_keys = [u"cause_id", u"rei_id"]
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
            year_id=year_id,
            drawcols=["draw_{}".format(i) for i in range(n_draws)])
        self.global_ratios = df

    def new_population(self, location_set_id, agg_loc_sets=[]):
        dim = self.nonfatal_dimensions.get_simulation_dimensions(
            self.measure_id)
        df = get_population(
            age_group_id=(
                dim.index_dim.get_level("age_group_id") + [164]),
            location_id=dbtrees.loctree(location_set_id=location_set_id,
                                        gbd_round_id=self.gbd_round_id
                                        ).node_ids,
            sex_id=dim.index_dim.get_level("sex_id"),
            year_id=dim.index_dim.get_level("year_id"))
        index_cols = ["location_id", "year_id", "age_group_id", "sex_id"]
        data_cols = ["population"]

        io_mock = {}
        source = DrawSource({"draw_dict": io_mock, "name": "tmp"},
                            mem_read_func)
        sink = DrawSink({"draw_dict": io_mock, "name": "tmp"}, mem_write_func)
        sink.push(df[index_cols + data_cols])

        # location
        for set_id in agg_loc_sets:
            loc_tree = dbtrees.loctree(
                location_set_id=set_id,
                gbd_round_id=self.gbd_round_id)
            operator = Sum(
                index_cols=[col for col in index_cols if col != "location_id"],
                value_cols=data_cols)
            aggregator = AggSynchronous(
                draw_source=source,
                draw_sink=sink,
                index_cols=[col for col in index_cols if col != "location_id"],
                aggregate_col="location_id",
                operator=operator)
            aggregator.run(loc_tree)

        # age
        for age_group_id in ComoSummaries._gbd_compare_age_group_list:
            age_tree = dbtrees.agetree(age_group_id)
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
            "{}/info/population.h5".format(self.como_dir),
            'draws',
            mode='w',
            format='table',
            data_columns=["location_id", "year_id", "age_group_id", "sex_id"])

    def new_epilepsy_dws(self):
        combine_epilepsy_any.epilepsy_any(self)
        combine_epilepsy_subcombos.epilepsy_combos(self.como_dir)

    def new_urolith_dws(self):
        import_gbd2013.to_como(self.como_dir)

    def mark_best(self, description=""):
        self.unmark_current_best()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        q = """
            UPDATE epi.output_version
            SET
                best_start='{best_start}',
                best_end=NULL,
                is_best=1,
                best_description='{best_description}',
                best_user='{bu}'
            WHERE output_version_id={ovid}
        """.format(
            best_start=now,
            best_description=description,
            bu=getpass.getuser(),
            ovid=self.como_version_id)
        eng = ezfuncs.get_engine(conn_def="como-epi")
        res = eng.execute(q)
        return res

    def unmark_current_best(self):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        q = """
            UPDATE epi.output_version
            SET best_end='{be}', is_best=0
            WHERE is_best=1""".format(be=now)
        eng = ezfuncs.get_engine(conn_def="como-epi")
        res = eng.execute(q)
        return res

    def create_gbd_process_version(self):
        q = """
        CALL gbd.new_gbd_process_version (
            {}, 1, 'Como run {}', 'fix epi.ov table to accept hash', NULL,
            NULL)
        """.format(self.gbd_round_id, self.como_version_id)
        eng = ezfuncs.get_engine(conn_def="como-gbd")
        res = eng.execute(q)
        row = res.fetchone()
        pv_meta = row[0]
        self.gbd_process_version_id = int(json.loads(
            pv_meta)[0]["gbd_process_version_id"])
        q = """
            INSERT INTO gbd.gbd_process_version_metadata
                (`gbd_process_version_id`, `metadata_type_id`, `val`)
            VALUES
                ({gpvid}, 4, '{cv}')
        """.format(gpvid=self.gbd_process_version_id, cv=self.como_version_id)
        eng.execute(q)
        return pv_meta

    def create_compare_version(self, current_compare_version_id,
                               description):
        cur_cv = CompareVersion(current_compare_version_id)
        cv = CompareVersion.add_new_version(
            gbd_round_id=self.gbd_round_id,
            compare_version_description=description)
        for pv in cur_cv.gbd_process_version_ids():
            gbd_process_version = GBDProcessVersion(pv)
            if gbd_process_version.gbd_process_id != 1:
                cv.add_process_version(pv)
        cv.add_process_version(self.gbd_process_version_id)
        cv.mark_best()
        cv.unmark_best()
