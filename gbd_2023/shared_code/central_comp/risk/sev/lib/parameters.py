"""Logic related to SEV calculator parameters, which include inputs."""

from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd

import db_queries
import ihme_cc_risk_utils
from gbd import release
from gbd.constants import gbd_teams
from ihme_cc_cache import FileBackedCacheReader, FileBackedCacheWriter

from ihme_cc_sev_calculator.lib import constants, input_utils, io, model_versions


@dataclass
class Parameters:
    """Parameters (validated) for a SEV Calculator run.

    Offers a thin abstraction layer over ihme_cc_cache.
    """

    # Input parameters - given by users
    compare_version_id: Optional[int]
    paf_version_id: Optional[int]
    como_version_id: Optional[int]
    codcorrect_version_id: Optional[int]
    release_id: int
    n_draws: int
    year_ids: List[int]
    location_set_ids: List[int]
    measures: List[str]
    percent_change: bool
    by_cause: bool
    resume: bool
    test: bool
    overwrite_compare_version: bool

    # Runtime parameters - pulled by machinery
    version_id: int
    measure_ids: List[int]
    location_set_version_ids: List[int]
    output_dir: str
    age_group_ids: List[int]  # most detailed age groups used in run (no aggregates)
    aggregate_age_group_ids: List[int]
    sex_ids: List[int]
    draw_cols: List[str]
    population_version_id: int
    rei_set_ids: List[int]
    rei_ids: List[int]
    aggregate_rei_ids: List[int]
    temperature_rei_ids: List[int]
    all_rei_ids: List[int]  # rei_ids + aggregate_rei_ids + temperature_rei_ids
    most_detailed_location_ids: List[int]
    all_location_ids: List[int]  # includes aggregates
    cause_set_ids: List[int]
    cause_set_version_ids: List[int]
    percent_change_years: Optional[List[List[int]]]

    # File parameters - we don't always want to read them in for performance reasons
    population_dict: Optional[Dict[int, pd.DataFrame]]  # location set -> population
    rei_metadata_dict: Optional[Dict[int, pd.DataFrame]]  # location set -> rei metadata
    cause_metadata_dict: Optional[Dict[int, pd.DataFrame]]  # cause set -> cause metadata
    all_me_ids: Optional[pd.DataFrame]
    all_rr_metadata: Optional[pd.DataFrame]
    demographics: Optional[dict]
    pafs_of_one: Optional[pd.DataFrame]
    age_weights: Optional[pd.DataFrame]
    age_spans: Optional[pd.DataFrame]
    ensemble_weights: Optional[pd.DataFrame]

    # Cannot set a mutable default for a dataclass directly
    FILE_PARAMETERS: List[str] = 

    def cache(self) -> None:
        """Cache all parameters.

        Assumes manifest path determined from version_id is valid and
        its parent directory exists.

        File parameters are cached separately from other parameters so
        we don't read them in unnecessarily 100s of times. Also writes out
        storage metadata for draws.
        """
        writer = FileBackedCacheWriter(io.get_manifest_path(self.version_id, self.output_dir))

        # Write all parameters to a single spot minus file parameters
        param_dict = {k: v for k, v in asdict(self).items() if k not in self.FILE_PARAMETERS}
        for parameter in self.FILE_PARAMETERS:
            param_dict[parameter] = None

        writer.put(obj=param_dict, obj_name="parameters", relative_path=)

        # For file params like population, cause metadata, etc, write individually as well
        for location_set_id, population in self.population_dict.items():
            writer.put(
                obj=population,
                obj_name=f"population_{location_set_id}",
                relative_path=,
                storage_format=,
            )

        for rei_set_id, rei_metadata in self.rei_metadata_dict.items():
            writer.put(
                obj=rei_metadata,
                obj_name=f"rei_metadata_{rei_set_id}",
                relative_path=,
                storage_format=,
            )

        for cause_set_id, cause_metadata in self.cause_metadata_dict.items():
            writer.put(
                obj=cause_metadata,
                obj_name=f"cause_metadata_{cause_set_id}",
                relative_path=,
                storage_format=,
            )

        # Write more basic files
        writer.put(obj=self.all_me_ids, obj_name="all_me_ids", relative_path=)
        writer.put(
            obj=self.all_rr_metadata,
            obj_name="all_rr_metadata",
            relative_path=,
        )
        writer.put(
            obj=self.demographics, obj_name="demographics", relative_path=
        )
        writer.put(
            obj=self.pafs_of_one, obj_name="pafs_of_one", relative_path=
        )
        writer.put(
            obj=self.age_weights, obj_name="age_weights", relative_path=
        )
        writer.put(obj=self.age_spans, obj_name="age_spans", relative_path=)
        writer.put(
            obj=self.ensemble_weights,
            obj_name="ensemble_weights",
            relative_path=,
        )

        io.cache_storage_metadata(self.output_dir, self.measure_ids)

    @staticmethod
    def read_from_cache(version_id: int) -> "Parameters":
        """Read all parameters from cache."""
        cache_reader = FileBackedCacheReader(io.get_manifest_path(version_id))
        return Parameters(**cache_reader.get("parameters"))

    @property
    def cache_reader(self) -> FileBackedCacheReader:
        """Under-the-hood FileBackedCacheReader logic."""
        return FileBackedCacheReader(io.get_manifest_path(self.version_id, self.output_dir))

    def read_population(self, location_set_id: Optional[int] = None) -> pd.DataFrame:
        """Read population.

        If location set is given, reads population for that location set. Otherwise,
        read population for all location sets in run.
        """
        if location_set_id:
            return self.cache_reader.get(f"population_{location_set_id}")
        else:
            population = []
            for location_set_id in self.location_set_ids:
                population.append(self.cache_reader.get(f"population_{location_set_id}"))

            return pd.concat(population).drop_duplicates()

    def read_rei_metadata(
        self, rei_set_id: int = constants.COMPUTATION_REI_SET_ID
    ) -> pd.DataFrame:
        """Read REI metadata for given REI set id."""
        return self.cache_reader.get(f"rei_metadata_{rei_set_id}")

    def read_cause_metadata(
        self, cause_set_id: int = constants.COMPUTATION_CAUSE_SET_ID
    ) -> pd.DataFrame:
        """Read cause metadata for given cause set ID."""
        return self.cache_reader.get(f"cause_metadata_{cause_set_id}")

    def read_rei_me_ids(self, rei_id: int) -> pd.DataFrame:
        """Read REI ME IDs for the given rei_id.

        all_me_ids varies slightly from ihme_cc_risk_utils.get_rei_me_ids's
        output in that it has a med_id column that represents the mediator risk,
        like rr_metadata and that the rei_id column is a single value per risk.

        This function reverts that change to match ihme_cc_risk_utils.get_rei_me_ids's
        behavior.
        """
        return (
            self.cache_reader.get("all_me_ids")
            .query(f"rei_id == {rei_id}")
            .drop(columns="rei_id")
            .rename(columns={"med_id": "rei_id"})
        )

    def read_rr_metadata(self, rei_id: int) -> pd.DataFrame:
        """Read RR metadata for the given rei_id."""
        return self.cache_reader.get("all_rr_metadata").query(f"rei_id == {rei_id}")

    def read_file(self, file_name: str) -> pd.DataFrame:
        """Read cached file.

        For population, REI metadata, cause metadata, etc, use their respective functions.
        """
        if file_name not in self.FILE_PARAMETERS:
            raise ValueError(f"Attempted to read unrecognized file: {file_name}")

        return self.cache_reader.get(file_name)


def process_parameters(
    compare_version_id: Optional[int],
    paf_version_id: Optional[int],
    como_version_id: Optional[int],
    codcorrect_version_id: Optional[int],
    release_id: Optional[int],
    n_draws: Optional[int],
    year_ids: Optional[List[int]],
    location_set_ids: Optional[List[int]],
    measures: Optional[List[str]],
    percent_change: bool,
    by_cause: bool,
    resume: bool,
    version_id: int,
    test: bool,
    overwrite_compare_version: bool,
) -> Parameters:
    """Process parameters, including validating inputs and pulling runtime params."""
    # Validate all input parameters, updating any missingness with defaults
    measures, measure_ids = input_utils.validate_measures(measures)
    release.validate_release_id(release_id)
    year_ids = input_utils.validate_year_ids(year_ids, release_id)
    (paf_version_id, como_version_id, codcorrect_version_id) = (
        input_utils.validate_compare_version(
            compare_version_id,
            paf_version_id,
            como_version_id,
            codcorrect_version_id,
            measures,
        )
    )
    input_utils.validate_machinery_versions(
        paf_version_id, como_version_id, codcorrect_version_id, measures, year_ids, n_draws
    )
    input_utils.validate_n_draws(n_draws)
    (location_set_ids, location_set_version_ids) = input_utils.validate_location_set_ids(
        location_set_ids, release_id
    )
    percent_change = input_utils.validate_percent_change(percent_change, year_ids)
    percent_change_years = _set_percent_change_years(percent_change)

    # Pull runtime parameters
    draw_cols = [f"draw_{i}" for i in range(n_draws)]
    demographics = db_queries.get_demographics(release_id=release_id, gbd_team=gbd_teams.EPI)
    age_group_ids = demographics["age_group_id"]
    aggregate_age_group_ids = constants.SEV_SUMMARY_AGE_GROUP_IDS
    sex_ids = demographics["sex_id"]

    population_version_id, population_dict = _pull_population(
        release_id, location_set_ids, location_set_version_ids, year_ids
    )

    rei_metadata_dict = _pull_rei_metadata(release_id)
    rei_ids, aggregate_rei_ids, temperature_rei_ids = _determine_reis_for_run(
        rei_metadata_dict, measures, paf_version_id
    )
    all_rei_ids = rei_ids + aggregate_rei_ids + temperature_rei_ids

    all_me_ids, all_rr_metadata = model_versions.get_rei_mes_and_rr_metadata(
        rei_ids, release_id
    )

    # Check for custom RRmax files once we know what the included risks are
    input_utils.validate_custom_rrmax_files(
        all_rei_ids, rei_metadata_dict[constants.COMPUTATION_REI_SET_ID], release_id
    )

    most_detailed_location_ids, all_location_ids = _pull_location_ids(
        release_id, location_set_ids, location_set_version_ids
    )

    cause_metadata_dict, cause_set_version_ids = _pull_cause_metadata(release_id)

    pafs_of_one = ihme_cc_risk_utils.get_pafs_of_one(release_id=release_id)

    age_weights = db_queries.get_age_weights(release_id=release_id)
    age_spans = db_queries.get_age_spans()

    ensemble_weights = _read_ensemble_weights(
        rei_metadata_dict[constants.COMPUTATION_REI_SET_ID], release_id
    )

    # Note: ihme_cc_cache cannot JSON serialize numpy int64s, which is the type
    # the set version ids come out as, so we convert to a normal int
    return Parameters(
        # Input parameters...
        compare_version_id=compare_version_id,
        paf_version_id=paf_version_id,
        como_version_id=como_version_id,
        codcorrect_version_id=codcorrect_version_id,
        release_id=release_id,
        n_draws=n_draws,
        year_ids=year_ids,
        location_set_ids=location_set_ids,
        measures=measures,
        percent_change=percent_change,
        by_cause=by_cause,
        resume=resume,
        test=test,
        overwrite_compare_version=overwrite_compare_version,
        # Runtime parameters...
        location_set_version_ids=[int(i) for i in location_set_version_ids],
        measure_ids=measure_ids,
        version_id=version_id,
        output_dir=str(io.get_output_dir(version_id)),
        age_group_ids=age_group_ids,
        aggregate_age_group_ids=aggregate_age_group_ids,
        sex_ids=sex_ids,
        draw_cols=draw_cols,
        population_version_id=int(population_version_id),
        rei_set_ids=constants.REI_SET_IDS,
        rei_ids=rei_ids,
        aggregate_rei_ids=aggregate_rei_ids,
        temperature_rei_ids=temperature_rei_ids,
        all_rei_ids=all_rei_ids,
        most_detailed_location_ids=most_detailed_location_ids,
        all_location_ids=all_location_ids,
        cause_set_ids=constants.CAUSE_SET_IDS,
        cause_set_version_ids=cause_set_version_ids,
        percent_change_years=percent_change_years,
        # File parameters...
        population_dict=population_dict,
        rei_metadata_dict=rei_metadata_dict,
        cause_metadata_dict=cause_metadata_dict,
        all_me_ids=all_me_ids,
        all_rr_metadata=all_rr_metadata,
        demographics=demographics,
        pafs_of_one=pafs_of_one,
        age_weights=age_weights,
        age_spans=age_spans,
        ensemble_weights=ensemble_weights,
    )


def _pull_population(
    release_id: int,
    location_set_ids: List[int],
    location_set_version_ids: List[int],
    year_ids: List[int],
) -> Tuple[int, Dict[int, pd.DataFrame]]:
    """Pull population for all location sets.

    Pulls for all requested years + the arbitrary year used in PAF aggregation.

    Returns:
        Population version, dictionary of location set id -> population data frame.
    """
    population_dict = {}
    population_version_id = None
    for set_id, set_version_id in zip(location_set_ids, location_set_version_ids):
        population = db_queries.get_population(
            release_id=release_id,
            location_set_id=set_id,
            location_set_version_id=set_version_id,
            # Duplicate years are allowed by get_population, so its ok if year_ids
            # already has the arbitrary year
            year_id=year_ids + [constants.ARBITRARY_MACHINERY_YEAR_ID],
            location_id="all",
            sex_id="all",
            age_group_id="all",
            run_id=population_version_id,
            use_rotation=False,
        )

        # Update version after first get_populations call to ensure consistency
        if population_version_id is None:
            population_version_id = population["run_id"].iat[0]

        population_dict[set_id] = population

    return (population_version_id, population_dict)


def _pull_rei_metadata(release_id: int) -> Dict[int, pd.DataFrame]:
    """Pull REI metadata for relevant REI sets with minor SEV-specific edits."""
    rei_metadata_dict = {}
    for rei_set_id in constants.REI_SET_IDS:
        rei_metadata = db_queries.get_rei_metadata(
            release_id=release_id, rei_set_id=rei_set_id, include_all_metadata=True
        )

        # Edit hierarchy to set certain risks that are estimates but not most detailed
        # as most detailed for the purpose of directly calculating RRmax rather than
        # aggregating from child RRmaxes.
        # Currently: particulate matter pollution and Low birth weight and short gestation
        rei_metadata.loc[
            (rei_metadata["is_estimate"] == 1) & (rei_metadata["most_detailed"] == 0),
            "most_detailed",
        ] = 1

        # Explicitly convert inv_exp values to ints, so we don't have to rely on implicit
        # conversion when saving/loading metadata. First, we set NaNs to 0.
        rei_metadata.loc[rei_metadata["inv_exp"].isnull(), "inv_exp"] = 0
        rei_metadata["inv_exp"] = rei_metadata["inv_exp"].astype(int)

        rei_metadata_dict[rei_set_id] = rei_metadata

    return rei_metadata_dict


def _determine_reis_for_run(
    rei_metadata_dict: Dict[int, pd.DataFrame], measures: List[str], paf_version_id: int
) -> Tuple[List[int], List[int], List[int]]:
    """Determine REIs to calculate RRmax/SEVs for.

    Some REIs do not have relative risks, so those are also excluded.
    Temperature risks are a special case as the modeling team calculates their own SEVs.

    If only calculating RRmax, we're not limited by REIs that went into the PAF aggregator as
    we don't need PAFs. If we're also calculating SEVs, we drop REIs that we don't have
    PAFs for.

    Temperature REIs are a special case: their modeling team calculates their own SEVs, so
    we don't have to, which is why they're separate from other REIs we do produce SEVs for.
    However, we copy them over within this pipeline if the were included in the PAF version.
    """
    rei_ids = (
        rei_metadata_dict[constants.COMPUTATION_REI_SET_ID]
        .query("most_detailed == 1")
        .query(f"~rei_id.isin({constants.NO_RR_REI_IDS})")
    )["rei_id"].tolist()

    aggregate_rei_ids = (
        rei_metadata_dict[constants.REPORTING_REI_SET_ID]
        .query(f"~rei_id.isin({constants.NO_RR_REI_IDS})")
        .query(f"~rei_id.isin({rei_ids})")
    )["rei_id"].tolist()
    temperature_rei_ids = []

    if constants.SEV in measures:
        paf_rei_ids = (
            io.read_reis_in_paf_version(paf_version_id)["rei_id"].drop_duplicates().tolist()
        )
        rei_ids = list(set(paf_rei_ids).intersection(rei_ids))
        aggregate_rei_ids = list(set(paf_rei_ids).intersection(aggregate_rei_ids))
        temperature_rei_ids = (
            rei_metadata_dict[constants.REPORTING_REI_SET_ID]
            .query(f"rei_id.isin({constants.TEMPERATURE_REI_IDS})")
            .query(f"rei_id.isin({paf_rei_ids})")
        )["rei_id"].tolist()

    if len(rei_ids) == 0:
        raise RuntimeError("Could not determine any REI IDs to use for the run.")

    return (rei_ids, aggregate_rei_ids, temperature_rei_ids)


def _pull_location_ids(
    release_id: int, location_set_ids: List[int], location_set_version_ids: List[int]
) -> Tuple[List[int], List[int]]:
    """Pull all location ids for all location sets."""
    all_location_ids = set()
    most_detailed_location_ids = set()
    for set_id, set_version_id in zip(location_set_ids, location_set_version_ids):
        location_metadata = db_queries.get_location_metadata(
            release_id=release_id,
            location_set_id=set_id,
            location_set_version_id=set_version_id,
        )
        all_location_ids.update(location_metadata["location_id"])
        most_detailed_location_ids.update(
            location_metadata.query("most_detailed == 1")["location_id"]
        )

    return (list(most_detailed_location_ids), list(all_location_ids))


def _pull_cause_metadata(release_id: int) -> Tuple[Dict[int, pd.DataFrame], List[int]]:
    """Pull cause metadata for relevant cause sets."""
    cause_metadata_dict = {}
    cause_set_version_ids = []
    for cause_set_id in constants.CAUSE_SET_IDS:
        cause_metadata = db_queries.get_cause_metadata(
            cause_set_id=cause_set_id, release_id=release_id
        )
        cause_set_version_ids.append(int(cause_metadata["cause_set_version_id"].iat[0]))
        cause_metadata_dict[cause_set_id] = cause_metadata

    return (cause_metadata_dict, cause_set_version_ids)


def _read_ensemble_weights(rei_metadata: pd.DataFrame, release_id: int) -> pd.DataFrame:
    """Read ensemble weights for risks that we calculate risk prevalence via ensemble density.

    Assumes none of said risks have location- or year-specific weights. Combines all weights
    into a single file.

    If the edensity code were in Python, we could call ihme_cc_risk_utils.get_ensemble_weights
    right before, but it's in R. We read all ensemble weights beforehand and cache them so
    that the R code doens't need to reimplement the function.
    """
    arbitrary_location_id = 523
    all_weights = []
    for rei_id in constants.EDENSITY_REI_IDS:
        # Pull ensemble weights for a specific location to ensure we get back global
        weights = ihme_cc_risk_utils.get_ensemble_weights(
            rei=rei_metadata.query(f"rei_id == {rei_id}")["rei"].iat[0],
            release_id=release_id,
            location_id=arbitrary_location_id,
        )

        # We expect these risks to have global weights (otherwise we'd need all most detailed
        # locations)
        if weights["location_id"].iat[0] != 1:
            raise RuntimeError(
                "ihme_cc_risk_utils.get_ensemble_weights returned location-specific ensemble "
                f"weights for rei_id {rei_id} unexpectedly."
            )

        # We also expect the weights not to vary by year (otherwise we'd keep all years)
        n_unique_years = len(weights["year_id"].unique())
        weights_no_year_col = weights.drop(columns="year_id").drop_duplicates()
        if len(weights_no_year_col) != len(weights) / n_unique_years:
            raise RuntimeError(
                f"Ensemble weights for rei_id {rei_id} appear to vary by year. The SEV "
                f"Calculator assumes they do not. Weights:\n{weights}"
            )

        all_weights.append(weights_no_year_col.assign(rei_id=rei_id))

    return pd.concat(all_weights).reset_index(drop=True)


def _set_percent_change_years(percent_change: bool) -> Optional[List[List[int]]]:
    """Set percent change years if percent_change is True.

    If percent_change is False, return None.

    TODO: these percent change years are hardcoded. This should be more flexible logic
    but need to consult with team.
    """
    return constants.PERCENT_CHANGE_YEARS if percent_change else None
