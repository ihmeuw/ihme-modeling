from loguru import logger
from multiprocessing import Queue, Process

from core_maths.interpolate import pchip_interpolate
from gbd.constants import age, measures
from gbd.decomp_step import decomp_step_from_decomp_step_id
from gbd.estimation_years import estimation_years_from_gbd_round_id
from hierarchies import dbtrees

from como.legacy import residuals
from como.legacy.io import SourceSinkFactory
from como.legacy.common import agg_hierarchy, ExceptionWrapper


SENTINEL = None


class BirthPrevInputCollector:
    def __init__(self, como_version, location_id, year_id, sex_id):

        self.como_version = como_version
        self._estim_years = estimation_years_from_gbd_round_id(self.como_version.gbd_round_id)

        self._ss_factory = SourceSinkFactory(como_version)

        self.dimensions = self.como_version.nonfatal_dimensions
        if location_id:
            self.dimensions.simulation_index["location_id"] = location_id
        if year_id:
            self.dimensions.simulation_index["year_id"] = year_id
        if sex_id:
            self.dimensions.simulation_index["sex_id"] = sex_id
        self.dimensions.simulation_index["age_group_id"] = [age.BIRTH]

    @property
    def _birth_prev_set(self):
        memv_df = self.como_version.mvid_list.merge(
            self.como_version.birth_prev, on="modelable_entity_id"
        )
        arglist = list(zip(list(memv_df.modelable_entity_id), list(memv_df.model_version_id)))
        return list(set(arglist))

    def read_single_birth_prev(self, modelable_entity_id, model_version_id):
        birth_source = self._ss_factory.get_birth_prev_modelable_entity_source(
            modelable_entity_id, model_version_id
        )
        dim = self.dimensions.get_simulation_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False
        )

        filters = dim.index_dim.to_dict()["levels"]
        req_years = filters["year_id"]
        if not set(req_years).issubset(set(self._estim_years)):
            filters["year_id"] = list(set(req_years + self._estim_years))

        df = birth_source.content(filters=filters)
        if df.empty:
            logger.error(
                f"No data returned for ME {modelable_entity_id}, "
                f"model version {model_version_id}."
            )
            raise Exception(
                f"No data returned for ME {modelable_entity_id}, "
                f"model version {model_version_id}."
            )
        draw_cols = [col for col in df.columns if "draw_" in col]

        dim.index_dim.add_level("sequela_id", df.sequela_id.unique().tolist())
        dim.index_dim.add_level("cause_id", df.cause_id.unique().tolist())

        if not set(df.year_id.unique()).issuperset(set(req_years)):
            interp_df = pchip_interpolate(
                df=df,
                id_cols=dim.index_names,
                value_cols=draw_cols,
                time_col="year_id",
                time_vals=req_years,
            )
            df = df[df.year_id.isin(req_years)]
            df = df.append(interp_df)
        else:
            df = df[df.year_id.isin(req_years)]

        df["measure_id"] = measures.INCIDENCE
        return df

    def _q_read_single_birth_prev(self, inq, outq):
        for arglist in iter(inq.get, SENTINEL):
            try:
                result = self.read_single_birth_prev(*arglist)
            except Exception as e:
                logger.error(
                    f"q_read_single_birth_prev({inq}, {outq}); {arglist}. Exception: {e}"
                )
                result = ExceptionWrapper(e)
            outq.put(result)

    def collect_birth_prev_inputs(self, n_processes=20):
        _set = self._birth_prev_set

        inq = Queue()
        outq = Queue()

        read_procs = []
        for i in range(n_processes):
            p = Process(target=self._q_read_single_birth_prev, args=(inq, outq))
            read_procs.append(p)
            p.start()

        for readkey in _set:
            inq.put(readkey)

        for _ in read_procs:
            inq.put(SENTINEL)

        result_list = []
        for _ in _set:
            proc_result = outq.get()
            result_list.append(proc_result)

        for p in read_procs:
            p.join()

        return result_list


class SequelaInputCollector:
    def __init__(self, como_version, location_id, year_id, age_group_id, sex_id):

        self.como_version = como_version
        self._estim_years = estimation_years_from_gbd_round_id(self.como_version.gbd_round_id)

        self._ss_factory = SourceSinkFactory(como_version)

        self.dimensions = self.como_version.nonfatal_dimensions
        if location_id:
            self.dimensions.simulation_index["location_id"] = location_id
        if year_id:
            self.dimensions.simulation_index["year_id"] = year_id
        if sex_id:
            self.dimensions.simulation_index["sex_id"] = sex_id
        if age_group_id:
            self.dimensions.simulation_index["age_group_id"] = age_group_id

    @property
    def _sequela_set(self):
        memv_df = self.como_version.mvid_list.merge(
            self.como_version.sequela_list, on="modelable_entity_id"
        )
        memv_df = memv_df[memv_df.sequela_id > 0]
        arglist = list(zip(list(memv_df.modelable_entity_id), list(memv_df.model_version_id)))
        return list(set(arglist))

    def read_single_sequela(
        self,
        modelable_entity_id,
        model_version_id,
        measure_id=[measures.PREVALENCE, measures.INCIDENCE],
    ):

        sequela_source = self._ss_factory.get_sequela_modelable_entity_source(
            modelable_entity_id, model_version_id
        )
        dim = self.dimensions.get_simulation_dimensions(measure_id=measure_id, at_birth=False)

        filters = dim.index_dim.to_dict()["levels"]
        req_years = filters["year_id"]
        if not set(req_years).issubset(set(self._estim_years)):
            filters["year_id"] = list(set(req_years + self._estim_years))

        df = sequela_source.content(filters=filters)
        if df.empty:
            logger.error(
                f"No data returned for ME {modelable_entity_id}, "
                f"model version {model_version_id}."
            )
            raise Exception(
                f"No data returned for ME {modelable_entity_id}, "
                f"model version {model_version_id}."
            )
        draw_cols = [col for col in df.columns if "draw_" in col]

        dim.index_dim.add_level("sequela_id", df.sequela_id.unique().tolist())
        dim.index_dim.add_level("cause_id", df.cause_id.unique().tolist())
        dim.index_dim.add_level("healthstate_id", df.healthstate_id.unique().tolist())

        if not set(df.year_id.unique()).issuperset(set(req_years)):
            interp_df = pchip_interpolate(
                df=df,
                id_cols=dim.index_names,
                value_cols=draw_cols,
                time_col="year_id",
                time_vals=req_years,
            )
            df = df[df.year_id.isin(req_years)]
            df = df.append(interp_df)
        else:
            df = df[df.year_id.isin(req_years)]

        return df

    def _q_read_single_sequela(self, inq, outq):
        for arglist in iter(inq.get, SENTINEL):
            try:
                result = self.read_single_sequela(*arglist)
            except Exception as e:
                logger.error(
                    f"q_read_single_sequela({inq}, {outq}): {arglist}. exception: {e}"
                )
                result = ExceptionWrapper(e)
            outq.put(result)

    def collect_sequela_inputs(
        self, n_processes=20, measure_id=[measures.PREVALENCE, measures.INCIDENCE]
    ):
        _set = self._sequela_set

        inq = Queue()
        outq = Queue()

        read_procs = []
        for i in range(n_processes):
            p = Process(target=self._q_read_single_sequela, args=(inq, outq))
            read_procs.append(p)
            p.start()

        for readkey in _set:
            args = readkey + (measure_id,)
            inq.put(args)

        for _ in read_procs:
            inq.put(SENTINEL)

        result_list = []
        for _ in _set:
            proc_result = outq.get()
            result_list.append(proc_result)

        for p in read_procs:
            p.join()

        return result_list


class SequelaResultComputer:
    def __init__(self, como_version):
        self.como_version = como_version

        self._ss_factory = SourceSinkFactory(como_version)

        self.dimensions = self.como_version.nonfatal_dimensions

    @property
    def index_cols(self):
        return self.dimensions.get_sequela_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False
        ).index_names

    @property
    def draw_cols(self):
        return self.dimensions.get_sequela_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False
        ).data_list()

    def aggregate_sequela(self, df):
        seq_tree = dbtrees.sequelatree(
            sequela_set_id=2,
            sequela_set_version_id=self.como_version.sequela_set_version_id,
            decomp_step=decomp_step_from_decomp_step_id(self.como_version.decomp_step_id),
        )
        df = df[self.index_cols + self.draw_cols]
        df = df.groupby(self.index_cols).sum().reset_index()
        df = agg_hierarchy(
            tree=seq_tree,
            df=df,
            index_cols=self.index_cols,
            data_cols=self.draw_cols,
            dimension="sequela_id",
        )
        df = df[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            df[col] = df[col].astype(int)
        return df

    def residuals(self, df):
        cause_ylds = df.merge(self.como_version.sequela_list, on="sequela_id")
        cause_idx = [col for col in self.index_cols if col != "sequela_id"] + ["cause_id"]
        cause_ylds = cause_ylds.groupby(cause_idx).sum()
        cause_ylds = cause_ylds[self.draw_cols].reset_index()
        dims = self.dimensions.get_simulation_dimensions(
            measure_id=measures.YLD, at_birth=False
        )
        location_id = cause_ylds["location_id"].unique().item()
        ratio_df = residuals.calc(
            location_id=location_id,
            ratio_df=self.como_version.global_ratios,
            output_type="sequela_id",
            drawcols=dims.data_list(),
            seq_ylds=df,
            cause_ylds=cause_ylds,
        )
        ratio_df = ratio_df[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            ratio_df[col] = ratio_df[col].astype(int)
        return ratio_df
