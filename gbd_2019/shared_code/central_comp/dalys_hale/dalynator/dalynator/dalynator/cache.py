import logging
import pickle
import os
from typing import List, Dict

import pandas as pd

import gbd.constants as gbd
import gbd.gbd_round as gbr

import dalynator.tool_objects as to
from dalynator.data_source import GetPopulationDataSource
from dalynator.compute_summaries import MetricConverter

from db_tools.ezfuncs import query
from db_queries.get_age_metadata import get_age_weights, get_age_spans
from hierarchies import dbtrees

logger = logging.getLogger(__name__)


class Cache:
    """
    Load all of the data needed for DJS so it is ready to be accessed
    """
    def __init__(self, tool_name, input_data_root, codcorrect_version,
                 fauxcorrect_version, epi_version, paf_version,
                 cause_set_ids, gbd_round_id, decomp_step, cache_dir,
                 location_set_ids, all_year_ids, full_location_ids,
                 measure_ids):
        self.tool_name = tool_name
        self.input_data_root = input_data_root
        self.codcorrect_version = codcorrect_version
        self.fauxcorrect_version = fauxcorrect_version
        self.epi_version = epi_version
        self.paf_version = paf_version
        self.cause_set_ids = cause_set_ids
        self.gbd_round_id = gbd_round_id
        self.decomp_step = decomp_step
        self.cache_dir = cache_dir
        self.location_set_ids = location_set_ids
        self.all_year_ids = all_year_ids
        self.full_location_ids = full_location_ids
        self.measure_ids = measure_ids
        # we now select either codcorrect or fauxcorrect, but not both
        self.cod_object = to.cod_or_faux_correct(
            self.input_data_root,
            codcorrect_version=self.codcorrect_version,
            fauxcorrect_version=self.fauxcorrect_version)

    def load_caches(self) -> None:
        logger.debug("Caching to dir: {}".format(self.cache_dir))
        self._cache_pop()
        self._cache_regional_scalars()
        self._cache_age_weights()
        self._cache_age_spans()
        if self.tool_name == "burdenator":
            self._cache_cause_hierarchy()
            for loc_set in self.location_set_ids:
                self._cache_location_hierarchy(
                    loc_set)
            self._cache_cause_risk_metadata()
            self._cache_all_reis()

    def _cache_pop(self) -> None:
        """Caches the call to the database for population AND
        also adds in a fake population for location 44620 (Global) so that SDI
        aggregation does not fail due to a missing population value
        """
        logger.debug(
            "Starting to load population cache")
        core_index = [
            'location_id', 'year_id', 'age_group_id', 'sex_id']
        pop_ds = GetPopulationDataSource(
            "population", year_id=self.all_year_ids,
            location_id=self.full_location_ids,
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step,
            desired_index=core_index)

        pop_df = pop_ds.get_data_frame()
        try:
            pop_df = MetricConverter.aggregate_population(
                pop_df)
        except ValueError as e:
            if str(e) != "No objects to concatenate":
                raise

        if len(pop_df[pop_df.location_id == 44620]) == 0:
            pop_df.append(pop_df[pop_df.location_id == 1].replace(
                {'location_id': {1: 44620}}))
        cache_file = "{}/pop.h5".format(
            self.cache_dir)
        pop_df.to_hdf(cache_file, "pop", data_columns=core_index,
                      format="table")
        logger.debug(
            "Cached population in {}".format(cache_file))

    def _cache_regional_scalars(self) -> None:
        if self.gbd_round_id < 5:  # no scalars before round 5 are in the db
            # so need to do this for the test suite
            gbd_round_id = 5
        else:
            gbd_round_id = self.gbd_round_id
        sql_q = ("SELECT location_id, year_id, mean as scaling_factor "
                 "FROM mortality.upload_population_scalar_estimate WHERE "
                 "run_id = (SELECT run_id FROM mortality.process_version "
                 "WHERE process_id = 23 AND gbd_round_id = {} AND status_id = "
                 "5)".format(gbd_round_id))
        scalars = query(sql_q, conn_def='CONN_DEF')
        cache_file = "FILEPATH".format(
            self.cache_dir)
        scalars.to_hdf(cache_file, "scalars",
                       data_columns=[
                           'location_id', 'year_id'],
                       format="table")
        logger.debug(
            "Cached regional scalars in {}".format(cache_file))

    def _cache_age_weights(self) -> None:
        logger.debug(
            "Starting to load age_weights cache")
        age_weights_df = get_age_weights(gbd_round_id=int(self.gbd_round_id))
        # returns age_group_id, age_group_weight_value as a pandas df
        cache_file = "FILEPATH".format(
            self.cache_dir)
        age_weights_df.to_hdf(cache_file, "age_weights",
                              data_columns=['age_group_id'], format="table")
        logger.debug(
            "Cached age_weights in {}".format(cache_file))

    def _cache_age_spans(self) -> None:
        logger.debug(
            "Starting to load age_spans cache")

        age_spans_df = get_age_spans(12)
        # returns age_group_id, age_group_years_start, age_group_years_end as a
        # pandas df
        cache_file = "FILEPATH".format(
            self.cache_dir)
        age_spans_df.to_hdf(cache_file, "age_spans",
                            data_columns=['age_group_id'], format="table")
        logger.debug(
            "Cached age_spans in {}".format(cache_file))

    def _cache_cause_hierarchy(self) -> None:
        logger.debug("Starting to load cause_hierarchy cache")
        tree_list = []
        for cause_set_id in self.cause_set_ids:
            sub_tree_list = dbtrees.causetree(
                cause_set_id=cause_set_id,
                gbd_round_id=self.gbd_round_id,
                return_many=True)
            for tree in sub_tree_list:
                tree_list.append(tree)
        cache_file = "FILEPATH".format(
            self.cache_dir)
        pickle.dump(tree_list, open(cache_file, "wb"))
        logger.debug("Cached cause_hierarchies in {}".format(
            cache_file))

    def _cache_location_hierarchy(self, location_set_id: int) -> None:
        logger.debug("Starting to load location_hierarchy cache")
        location_hierarchy = dbtrees.loctree(location_set_id=location_set_id,
                                             gbd_round_id=self.gbd_round_id,
                                             return_many=True)
        cache_file = "FILEPATH".format(self.cache_dir,
                                       location_set_id)
        pickle.dump(location_hierarchy, open(cache_file, "wb"))
        logger.debug("Cached location_hierarchy {} in {}".format(
            cache_file, location_set_id))

    def _cache_cause_risk_metadata(self) -> None:
        logger.debug("Starting to load cause_risk_metadata cache")
        metadata_df = self.load_cause_risk_metadata()
        cache_file = "FILEPATH".format(self.cache_dir)
        metadata_df.to_csv(cache_file, columns=['cause_id', 'rei_id'],
                           index=False)
        logger.debug("Cached cause_risk_metadata in {}".format(cache_file))

    def _cache_all_reis(self) -> None:
        logger.debug("Starting to load all_reis cache")
        meta_info: Dict = self.load_rei_restrictions(self.measure_ids)
        reis: List = []
        for sex in [gbd.sex.MALE, gbd.sex.FEMALE]:
            for measure in self.measure_ids:
                reis.extend(meta_info[sex][measure]['rei_ids'])
        all_reis = pd.DataFrame(reis, columns=['rei_id'])
        cache_file = "FILEPATH".format(self.cache_dir)
        all_reis.to_csv(cache_file, index=False)
        logger.debug("Cached all_reis in {}".format(cache_file))

    def load_rei_restrictions(self, measure_ids: List[int]) -> Dict:
        """Get measure/sex restrictions for reis"""
        # get cause/reis that exist in paf output
        existing_reis = self._get_rei_from_paf_output()

        # get causes that exist in codcorrect output
        cc_output = self._get_causes_from_cc_output()

        # get causes that exist in como output
        como_output = self._get_causes_from_como_output()

        central_machinery_output = pd.concat([cc_output, como_output]
                                             ).reset_index(drop=True)
        restricted_reis = self._log_mismatch_with_pafs(existing_reis,
                                                       central_machinery_output
                                                       )

        measure_dict = {gbd.measures.YLL: [gbd.measures.YLL],
                        gbd.measures.YLD: [gbd.measures.YLD],
                        gbd.measures.DALY: [gbd.measures.YLL, gbd.measures.YLD]
                        }
        if gbd.measures.YLL in self.measure_ids:
            measure_dict[gbd.measures.DEATH] = [gbd.measures.YLL]
        else:
            measure_dict[gbd.measures.DEATH] = [gbd.measures.DEATH]
        sex_dict = {gbd.sex.MALE: [gbd.sex.MALE],
                    gbd.sex.FEMALE: [gbd.sex.FEMALE],
                    gbd.sex.BOTH: [gbd.sex.MALE, gbd.sex.FEMALE]}
        meta_info: Dict = {}
        for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE, gbd.sex.BOTH]:
            meta_info[sex_id] = {}
            for measure_id in measure_ids:
                meta_info[sex_id][measure_id] = {
                    'rei_ids': list(restricted_reis.query(
                        "sex_id==[{}] & measure_id==[{}]".format(
                            ','.join(str(s) for s in sex_dict[sex_id]),
                            ','.join(str(m) for m in measure_dict[measure_id])
                                     )).rei_id.unique())}
        return meta_info

    def load_cause_risk_metadata(self) -> pd.DataFrame:
        """Get 100 percent pafs metadata for cause-risk pairs"""
        # get reis that exist in paf output
        existing_reis = self._get_rei_from_paf_output()
        existing_reis = set(
            existing_reis.rei_id.unique())

        # get cause-risk metadata from the database
        metadata = self._get_cause_risk_metadata_from_database()
        metadata_reis = set(
            metadata.rei_id.unique())

        # filter down to the risks that we have paf output for AND metadata for
        usable_reis = list(
            existing_reis & metadata_reis)
        metadata = metadata.loc[
            metadata.rei_id.isin(usable_reis)]

        return metadata[['cause_id', 'rei_id']]

    def _get_rei_from_paf_output(self) -> pd.DataFrame:
        paf_dir = "FILEPATH".format(
            self.input_data_root, self.paf_version)
        existing_reis = pd.read_csv(os.path.join(paf_dir,
                                                 'FILEPATH'))
        cond = (gbd.measures.YLL not in self.measure_ids and
                gbd.measures.DEATH not in existing_reis['measure_id'].unique())
        if cond:
            existing_reis = existing_reis.replace(
                to_replace={'measure_id': gbd.measures.YLL},
                value={'measure_id': gbd.measures.DEATH})
        return existing_reis

    def _get_cause_risk_metadata_from_database(self) -> pd.DataFrame:

        q = """
            SELECT
                cm.cause_id,
                cm.rei_id,
                cm.cause_risk_metadata_type_id,
                cm.cause_risk_metadata_value as metadata_val
            FROM
                shared.cause_risk_metadata_history cm
            JOIN
                (SELECT
                    max(mh.cause_risk_metadata_version_id)
                    as cause_risk_metadata_version_id
                FROM
                    shared.cause_risk_metadata_version cmv
                JOIN
                    shared.gbd_round gr ON gr.gbd_round = cmv.gbd_round
                JOIN
                    shared.cause_risk_metadata_history mh
                    ON cmv.cause_risk_metadata_version_id =
                    mh.cause_risk_metadata_version_id
                WHERE
                    gbd_round_id = {gbd_round}) cmv ON
                cmv.cause_risk_metadata_version_id=cm.cause_risk_metadata_version_id
            WHERE
                cm.cause_risk_metadata_type_id =1
                and cm.cause_risk_metadata_value = 1
                """.format(gbd_round=self.gbd_round_id)
        metadata = query(
            q, conn_def='CONN_DEF')
        return metadata

    def _get_causes_from_cc_output(self) -> pd.DataFrame:
        if gbd.measures.YLL in self.measure_ids or gbd.measures.DEATH in self.measure_ids:
            # this function assumes the summary files are square on demographics
            summary_dir = path_to_summaries(
                self.cod_object.abs_path_to_summaries)
            summary_file = os.path.join(summary_dir, 'FILEPATH')
            global_df = pd.read_csv(summary_file)
            if gbd.measures.YLL in self.measure_ids:
                global_df['measure_id'] = gbd.measures.YLL
            else:
                global_df['measure_id'] = gbd.measures.DEATH
            global_df = global_df.query("metric_id==1 & age_group_id==22"
                                        )[['measure_id', 'sex_id', 'cause_id']]
            return global_df
        return pd.DataFrame()

    def _get_causes_from_como_output(self) -> pd.DataFrame:
        if gbd.measures.YLD in self.measure_ids:
            epi_dir = ("FILEPATH"
                       .format(self.input_data_root, str(self.epi_version)))
            sample_summary_file = os.path.join(
                epi_dir, '{}.csv'.format(str(self.all_year_ids[0])))
            global_df = pd.read_csv(sample_summary_file)
            global_df = global_df.query("metric_id==1 & age_group_id==22"
                                        )[['measure_id', 'sex_id', 'cause_id']]
            return global_df
        return pd.DataFrame()

    def _log_mismatch_with_pafs(self, rei_df: pd.DataFrame,
                                measure_df: pd.DataFrame) -> pd.DataFrame:
        """do outer join for each measure group (como/codcorrect),
        log mismatch, return only matched results
        """
        paf_match = pd.merge(rei_df, measure_df, how='outer', indicator=True,
                             on=['measure_id', 'sex_id', 'cause_id'])
        paf_only = paf_match.query(
            "_merge == 'left_only'")
        for cause in paf_only.cause_id.unique():
            logger.info("Cause {} exists in existing_reis but is missing "
                        "in central machinery output for measures {}"
                        .format(cause,
                                paf_only.query("cause_id=={}".format(cause)
                                               ).measure_id.unique()))
        paf_match = paf_match.query(
            "_merge == 'both'")
        return paf_match[['measure_id', 'sex_id', 'cause_id', 'rei_id']]


def path_to_summaries(path):
    this_path = path
    for root, directory, file in os.walk(path):
        if file:
            return this_path
        elif directory:
            this_path = os.path.join(this_path, directory[0])
        else:
            raise ValueError("No files found in root: {}".format(path))

