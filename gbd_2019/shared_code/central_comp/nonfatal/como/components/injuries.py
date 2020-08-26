from multiprocessing import Queue, Process

from core_maths.interpolate import pchip_interpolate
from gbd.constants import measures
from gbd.estimation_years import estimation_years_from_gbd_round_id
from hierarchies import dbtrees
from ihme_dimensions import gbdize

from como.io import SourceSinkFactory
from como.common import agg_hierarchy, ExceptionWrapper


SENTINEL = None


class SexualViolenceInputCollector:

    def __init__(
            self, como_version, location_id, year_id, age_group_id, sex_id):

        self.como_version = como_version
        self._estim_years = estimation_years_from_gbd_round_id(
            self.como_version.gbd_round_id)

        # set up draw source factory
        self._ss_factory = SourceSinkFactory(como_version)

        # set up the dimensions we are using
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
    def _sexual_violence_set(self):
        memv_df = self.como_version.mvid_list.merge(
            self.como_version.sexual_violence_sequela,
            on='modelable_entity_id')
        arglist = list(zip(
            list(memv_df.modelable_entity_id),
            list(memv_df.model_version_id)))
        return list(set(arglist))

    def read_single_sexual_violence_injury(
            self, modelable_entity_id, model_version_id,
            measure_id=[measures.YLD, measures.PREVALENCE]):
        injury_source = (
            self._ss_factory.get_sexual_violence_modelable_entity_source(
                modelable_entity_id, model_version_id))
        dim = self.dimensions.get_simulation_dimensions(
            measure_id=measure_id, at_birth=False)

        # get filters w/ added years if interpolation is needed
        filters = dim.index_dim.to_dict()["levels"]
        req_years = filters["year_id"]
        if not set(req_years).issubset(set(self._estim_years)):
            filters["year_id"] = list(set(req_years + self._estim_years))

        # read data
        df = injury_source.content(filters=filters)
        if df.empty:
            raise Exception(f"No data returned for ME {modelable_entity_id}, "
                            f"model version {model_version_id}.")
        draw_cols = [col for col in df.columns if "draw_" in col]

        # add indices to dimensions object from draw source transforms
        dim.index_dim.add_level("cause_id", df.cause_id.unique().tolist())

        # interpolate missing years
        if not set(df.year_id.unique()).issuperset(set(req_years)):
            interp_df = pchip_interpolate(
                df=df,
                id_cols=dim.index_names,
                value_cols=draw_cols,
                time_col="year_id",
                time_vals=req_years)
            df = df[df.year_id.isin(req_years)]
            df = df.append(interp_df)
        else:
            df = df[df.year_id.isin(req_years)]

        # resample
        if len(dim.data_list()) != len(draw_cols):
            gbdizer = gbdize.GBDizeDataFrame(dim)
            df = gbdizer.correlated_percentile_resample(df)

        return df

    def _q_read_single_sexual_violence_injury(self, inq, outq):
        for arglist in iter(inq.get, SENTINEL):
            try:
                result = self.read_single_sexual_violence_injury(*arglist)
            except Exception as e:
                print(arglist)
                result = ExceptionWrapper(e)
            outq.put(result)

    def collect_sexual_violence_inputs(self, n_processes=20,
                                       measure_id=[measures.YLD,
                                                   measures.PREVALENCE]):
        # spin up xcom queues
        inq = Queue()
        outq = Queue()

        # Create and feed reader procs
        read_procs = []
        for i in range(n_processes):
            p = Process(target=self._q_read_single_sexual_violence_injury,
                        args=(inq, outq))
            read_procs.append(p)
            p.start()

        for readkey in self._sexual_violence_set:
            args = readkey + (measure_id,)
            inq.put(args)

        # make the workers die after
        for _ in read_procs:
            inq.put(SENTINEL)

        # get results
        result_list = []
        for _ in self._sexual_violence_set:
            proc_result = outq.get()
            result_list.append(proc_result)

        # close up the queue
        for p in read_procs:
            p.join()

        return result_list


class ENInjuryInputCollector:

    def __init__(
            self, como_version, location_id, year_id, age_group_id, sex_id):

        self.como_version = como_version
        self._estim_years = estimation_years_from_gbd_round_id(
            self.como_version.gbd_round_id)

        # set up draw source factory
        self._ss_factory = SourceSinkFactory(como_version)

        # set up the dimensions we are using
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
    def _injury_set(self):
        memv_df = self.como_version.mvid_list.merge(
            self.como_version.injury_sequela, on='modelable_entity_id')
        arglist = list(zip(
            list(memv_df.modelable_entity_id),
            list(memv_df.model_version_id)))
        return list(set(arglist))

    def read_single_en_injury(self, modelable_entity_id, model_version_id,
                              measure_id=[measures.YLD, measures.INCIDENCE,
                                          measures.ST_PREVALENCE,
                                          measures.LT_PREVALENCE]):
        injury_source = (
            self._ss_factory.get_en_injuries_modelable_entity_source(
                modelable_entity_id, model_version_id))
        dim = self.dimensions.get_simulation_dimensions(
            measure_id=measure_id, at_birth=False)

        # get filters w/ added years if interpolation is needed
        filters = dim.index_dim.to_dict()["levels"]
        req_years = filters["year_id"]
        if not set(req_years).issubset(set(self._estim_years)):
            filters["year_id"] = list(set(req_years + self._estim_years))

        # read data
        df = injury_source.content(filters=filters)
        if df.empty:
            raise Exception(f"No data returned for ME {modelable_entity_id}, "
                            f"model version {model_version_id}.")
        draw_cols = [col for col in df.columns if "draw_" in col]

        # add indices to dimensions object from draw source transforms
        dim.index_dim.add_level("sequela_id", df.sequela_id.unique().tolist())
        dim.index_dim.add_level("cause_id", df.cause_id.unique().tolist())
        dim.index_dim.add_level("healthstate_id",
                                df.healthstate_id.unique().tolist())
        dim.index_dim.add_level("rei_id", df.rei_id.unique().tolist())

        # interpolate missing years
        if not set(df.year_id.unique()).issuperset(set(req_years)):
            interp_df = pchip_interpolate(
                df=df,
                id_cols=dim.index_names,
                value_cols=draw_cols,
                time_col="year_id",
                time_vals=req_years)
            df = df[df.year_id.isin(req_years)]
            df = df.append(interp_df)
        else:
            df = df[df.year_id.isin(req_years)]

        # resample
        if len(dim.data_list()) != len(draw_cols):
            gbdizer = gbdize.GBDizeDataFrame(dim)
            df = gbdizer.correlated_percentile_resample(df)

        return df

    def _q_read_single_en_injury(self, inq, outq):
        for arglist in iter(inq.get, SENTINEL):
            try:
                result = self.read_single_en_injury(*arglist)
            except Exception as e:
                print(arglist)
                print(e)
                result = ExceptionWrapper(e)
            outq.put(result)

    def collect_injuries_inputs(self, n_processes=20,
                                measure_id=[measures.YLD, measures.INCIDENCE,
                                            measures.ST_PREVALENCE,
                                            measures.LT_PREVALENCE]):
        _set = self._injury_set
        # spin up xcom queues
        inq = Queue()
        outq = Queue()

        # Create and feed reader procs
        read_procs = []
        for i in range(n_processes):
            p = Process(target=self._q_read_single_en_injury,
                        args=(inq, outq))
            read_procs.append(p)
            p.start()

        for readkey in _set:
            args = readkey + (measure_id,)
            inq.put(args)

        # make the workers die after
        for _ in read_procs:
            inq.put(SENTINEL)

        # get results
        result_list = []
        for _ in _set:
            proc_result = outq.get()
            result_list.append(proc_result)

        # close up the queue
        for p in read_procs:
            p.join()

        return result_list


class InjuryResultComputer:

    def __init__(self, como_version):
        # inputs
        self.como_version = como_version

        # set up draw source factory
        self._ss_factory = SourceSinkFactory(como_version)

        # set up the dimensions we are using
        self.dimensions = self.como_version.nonfatal_dimensions

    def get_injuries_dimensions(self, measure_id):
        return self.dimensions.get_injuries_dimensions(
            measure_id=measure_id, at_birth=False)

    @property
    def index_cols(self):
        # measure/birth dimension doesn't matter for columns
        return self.dimensions.get_injuries_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False).index_names

    @property
    def draw_cols(self):
        # measure/birth dimension doesn't matter for columns
        return self.dimensions.get_injuries_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False).data_list()

    def aggregate_injuries(self, df):
        ct = dbtrees.causetree(
            cause_set_version_id=self.como_version.cause_set_version_id)
        df = agg_hierarchy(
            tree=ct,
            df=df,
            index_cols=self.index_cols,
            data_cols=self.draw_cols,
            dimension="cause_id")
        df = df[self.index_cols + self.draw_cols]

        # aggregate ncodes
        df = df.merge(self.como_version.ncode_hierarchy)
        df_agg = df.copy()
        df_agg = df_agg.groupby(
            ["age_group_id", "location_id", "year_id", "sex_id", "measure_id",
             "cause_id", "parent_id"])[self.draw_cols].sum().reset_index()
        df_agg = df_agg.rename(columns={"parent_id": "rei_id"})

        # set attribute
        df = df.append(df_agg)
        df = df[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            df[col] = df[col].astype(int)
        return df
