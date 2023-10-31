from functools import reduce
from pathlib import Path
from typing import Dict, List, Iterable, Optional, Tuple, Union

from loguru import logger
import pandas as pd

from covid_model_seiir_pipeline.lib import (
    io,
    utilities,
    static_vars,
)
from covid_model_seiir_pipeline.pipeline.regression.specification import (
    RegressionSpecification,
    CovariateSpecification,
)
from covid_model_seiir_pipeline.pipeline.regression.model import (
    RatioData,
    HospitalCensusData,
)


class RegressionDataInterface:

    def __init__(self,
                 infection_root: io.InfectionRoot,
                 covariate_root: io.CovariateRoot,
                 priors_root: Optional[io.CovariatePriorsRoot],
                 coefficient_root: Optional[io.RegressionRoot],
                 regression_root: io.RegressionRoot):
        self.infection_root = infection_root
        self.covariate_root = covariate_root
        self.priors_root = priors_root
        self.coefficient_root = coefficient_root
        self.regression_root = regression_root

    @classmethod
    def from_specification(cls, specification: RegressionSpecification) -> 'RegressionDataInterface':
        infection_root = io.InfectionRoot(specification.data.infection_version)
        covariate_root = io.CovariateRoot(specification.data.covariate_version)
        if specification.data.priors_version:
            priors_root = io.CovariatePriorsRoot(specification.data.priors_version)
        else:
            priors_root = None

        if specification.data.coefficient_version:
            coefficient_root = Path(specification.data.coefficient_version)
            coefficient_spec_path = coefficient_root / static_vars.REGRESSION_SPECIFICATION_FILE
            coefficient_spec = RegressionSpecification.from_path(coefficient_spec_path)
            coefficient_root = io.RegressionRoot(coefficient_spec.data.output_root,
                                                 data_format=coefficient_spec.data.output_format)
        else:
            coefficient_root = None

        regression_root = io.RegressionRoot(specification.data.output_root,
                                            data_format=specification.data.output_format)

        return cls(
            infection_root=infection_root,
            covariate_root=covariate_root,
            priors_root=priors_root,
            coefficient_root=coefficient_root,
            regression_root=regression_root,
        )

    def make_dirs(self, **prefix_args) -> None:
        io.touch(self.regression_root, **prefix_args)

    ####################
    # Metadata loaders #
    ####################

    def get_infections_metadata(self):
        return io.load(self.infection_root.metadata())

    def get_model_inputs_metadata(self):
        infection_metadata = self.get_infections_metadata()
        return infection_metadata['rates_metadata']['model_inputs_metadata']

    def get_n_draws(self) -> int:
        regression_spec = self.load_specification()
        return regression_spec.data.n_draws

    def is_counties_run(self) -> bool:
        regression_spec = self.load_specification()
        return regression_spec.data.run_counties

    #########################
    # Raw location handling #
    #########################

    @staticmethod
    def load_hierarchy_from_primary_source(location_set_version_id: Optional[int],
                                           location_file: Optional[Union[str, Path]]) -> pd.DataFrame:
        """Retrieve a location hierarchy from a file or from GBD if specified."""
        location_metadata = utilities.load_location_hierarchy(
            location_set_version_id=location_set_version_id,
            location_file=location_file,
        )
        return location_metadata

    def filter_location_ids(self, desired_location_hierarchy: pd.DataFrame = None) -> List[int]:
        """Get the list of location ids to model.
        This list is the intersection of a location metadata's
        locations, if provided, and the available locations in the infections
        directory.
        """
        regression_spec = self.load_specification()
        death_threshold = 0 #10 if regression_spec.data.run_counties else 5

        draw_0_data = self.load_full_past_infection_data(draw_id=0)
        total_deaths = draw_0_data.groupby('location_id').deaths.sum()

        ies_locations = set(total_deaths.index.tolist())
        threshold_locations = set(total_deaths[total_deaths > death_threshold].index.tolist())
        drop_locations = set(regression_spec.data.drop_locations)

        if desired_location_hierarchy is None:
            desired_locations = threshold_locations
        else:
            most_detailed = desired_location_hierarchy.most_detailed == 1
            desired_locations = set(desired_location_hierarchy.loc[most_detailed, 'location_id'].tolist())

        # Ies locations may be larger than desired locations, but we
        # do not care about the extras, only what's missing.
        ies_missing_locations = list(desired_locations - ies_locations)
        # Threshold locations is a subset of ies_locations
        not_enough_deaths_locations = list(ies_locations - threshold_locations)
        if drop_locations > threshold_locations:
            raise ValueError(f'Attempting to drop locations {list(drop_locations - threshold_locations)} '
                             f'which are not present in modeled locations that meet the epidemiological '
                             f'threshold of {death_threshold} deaths.')

        if ies_missing_locations:
            logger.warning("Some locations present in location metadata are missing from the "
                           f"infection models. Missing locations are {sorted(list(ies_missing_locations))}.")
        if not_enough_deaths_locations:
            logger.warning("Some locations present in location metadata do not meet the epidemiological "
                           f"threshold of {death_threshold} total deaths required for modeling. "
                           f"Locations below the threshold are {sorted(list(not_enough_deaths_locations))}.")
        if drop_locations:
            logger.warning("Some locations present in the location metadata are being dropped. "
                           f"Locations being dropped are {sorted(list(drop_locations))}.")

        return list((desired_locations & threshold_locations) - drop_locations)

    ######################
    # Population loaders #
    ######################

    def load_population(self) -> pd.DataFrame:
        metadata = self.get_model_inputs_metadata()
        model_inputs_version = metadata['output_path'].replace('covid-19-2', 'covid-19')
        population_path = Path(model_inputs_version) / 'output_measures' / 'population' / 'all_populations.csv'
        population_data = pd.read_csv(population_path)
        return population_data

    def load_five_year_population(self) -> pd.DataFrame:
        population = self.load_population()
        location_ids = self.load_location_ids()
        in_locations = population['location_id'].isin(location_ids)
        is_2019 = population['year_id'] == 2019
        is_both_sexes = population['sex_id'] == 3
        five_year_bins = [1, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235]
        is_five_year_bins = population['age_group_id'].isin(five_year_bins)
        population = population.loc[in_locations & is_2019 & is_both_sexes & is_five_year_bins, :]
        return population

    def load_risk_group_populations(self) -> pd.DataFrame:
        population = self.load_five_year_population()
        pop_lr = population[population['age_group_years_start'] < 65].groupby('location_id')['population'].sum()
        pop_hr = population[population['age_group_years_start'] >= 65].groupby('location_id')['population'].sum()
        return pd.concat([pop_lr.rename('population_lr'), pop_hr.rename('population_hr')], axis=1)

    def load_total_population(self) -> pd.Series:
        population = self.load_five_year_population()
        population = population.groupby('location_id')['population'].sum()
        return population

    ########################
    # Observed epi loaders #
    ########################

    def load_full_data_unscaled(self) -> pd.DataFrame:
        regression_spec = self.load_specification()
        metadata = self.get_model_inputs_metadata()
        model_inputs_version = metadata['output_path'].replace('covid-19-2', 'covid-19')
        if regression_spec.data.run_counties:
            full_data_path = Path(model_inputs_version) / 'full_data_fh_subnationals_unscaled.csv'
        else:
            full_data_path = Path(model_inputs_version) / 'full_data_unscaled.csv'
        full_data = pd.read_csv(full_data_path).rename(columns={
            'Deaths': 'cumulative_deaths',
            'Confirmed': 'cumulative_cases',
            'Hospitalizations': 'cumulative_hospitalizations',
        })

        full_data['date'] = pd.to_datetime(full_data['Date'])
        full_data['location_id'] = full_data['location_id'].astype(int)

        locs = full_data.location_id.unique()
        measures = ['cumulative_cases', 'cumulative_deaths', 'cumulative_hospitalizations']
        full_data = full_data.set_index(['location_id', 'date'])
        dfs = []
        for loc_id in locs:
            measure_series = []
            for measure in measures:
                s = (full_data
                     .loc[loc_id, measure]
                     .dropna()
                     .asfreq('D')
                     .interpolate())
                measure_series.append(s)
            df = pd.concat(measure_series, axis=1).reset_index()
            df['location_id'] = loc_id
            dfs.append(df.set_index(['location_id', 'date']).sort_index())

        return pd.concat(dfs)

    def load_total_deaths(self):
        """Load cumulative deaths by location."""
        location_ids = self.load_location_ids()
        full_data = self.load_full_data()
        total_deaths = full_data.groupby('location_id')['Deaths'].max().rename('deaths')
        return total_deaths.loc[location_ids]

    def load_hospital_census_data(self) -> 'HospitalCensusData':
        metadata = self.get_model_inputs_metadata()

        model_inputs_path = Path(metadata['output_path'].replace('covid-19-2', 'covid-19'))
        corrections_data = {}
        file_map = (
            ('hospitalizations', 'hospital_census'),
            ('icu', 'icu_census'),
        )
        for dir_name, measure in file_map:
            path = model_inputs_path / 'output_measures' / dir_name / 'population.csv'
            df = pd.read_csv(path)
            df['date'] = pd.to_datetime(df['date'])
            df = df.loc[(df.age_group_id == 22) & (df.sex_id == 3)]
            df = df[["location_id", "date", "value"]]
            if df.groupby(["location_id", "date"]).count().value.max() > 1:
                raise ValueError(f"Duplicate usages for location_id and date in {path}")
            # Location IDs to exclude from the census input data. Requested by Steve on 9/30
            census_exclude_locs = [200, 69, 179, 172, 170, 144, 26, 74, 67, 58]
            df = df.loc[~df.location_id.isin(census_exclude_locs)]
            corrections_data[measure] = df.set_index(['location_id', 'date']).value
        return HospitalCensusData(**corrections_data)

    def load_hospital_bed_capacity(self) -> pd.DataFrame:
        metadata = self.get_model_inputs_metadata()
        model_inputs_path = Path(metadata['output_path'].replace('covid-19-2', 'covid-19'))
        path = model_inputs_path / 'hospital_capacity.csv'
        return pd.read_csv(path)

    ##########################
    # Infection data loaders #
    ##########################

    def load_full_past_infection_data(self, draw_id: int) -> pd.DataFrame:
        infection_data = io.load(self.infection_root.infections(draw_id=draw_id))
        infection_data = (infection_data
                          .loc[:, ['infections_draw', 'deaths']]
                          .rename(columns={'infections_draw': 'infections'}))
        return infection_data

    def load_past_infection_data(self, draw_id: int):
        location_ids = self.load_location_ids()
        infection_data = self.load_full_past_infection_data(draw_id=draw_id)
        return infection_data.loc[location_ids]

    def load_em_scalars_draws(self) -> pd.DataFrame:
        location_ids = self.load_location_ids()
        em_scalars = io.load(self.infection_root.em_scalars())
        em_scalars = em_scalars.set_index('draw', append=True).unstack()
        em_scalars.columns = em_scalars.columns.droplevel().rename(None)
        try:
            india = em_scalars.loc[161].reset_index()
        except KeyError:
            india = em_scalars.loc[4849].reset_index()  # Use delhi
        dfs = []
        for location_id in [44539, 44540]:
            if location_id in em_scalars.reset_index().location_id.unique():
                continue
            loc_df = india.copy()
            loc_df['location_id'] = location_id
            loc_df = loc_df.set_index(['location_id', 'date'])
            dfs.append(loc_df)
        em_scalars = pd.concat([em_scalars, *dfs]).sort_index()
        assert em_scalars.index.duplicated().sum() == 0
        return em_scalars.loc[location_ids]

    def load_em_scalars(self, draw_id: int) -> pd.Series:
        return self.load_em_scalars_draws().loc[:, draw_id].rename('em_scalar')

    def load_ifr(self, draw_id: int) -> pd.DataFrame:
        ifr = io.load(self.infection_root.ifr(draw_id=draw_id))
        ifr = self.format_ratio_data(ifr)
        vaccinations = self.load_vaccinations().groupby('location_id').cumsum()
        population = self.load_risk_group_populations().reindex(vaccinations.index, level='location_id')
        vax_groups = ['non_escape', 'escape']

        for risk_group in ['lr', 'hr']:
            total_pop = population[f'population_{risk_group}']
            immune = vaccinations[[
                f'vaccinations_{vax_group}_immune_{risk_group}' for vax_group in vax_groups
            ]].sum(axis=1)
            protected = vaccinations[[
                f'vaccinations_{vax_group}_protected_{risk_group}' for vax_group in vax_groups
            ]].sum(axis=1)
            denom = total_pop - immune
            target_denom = total_pop - immune - protected
            ifr[f'ifr_{risk_group}'] *= (denom / target_denom).reindex(ifr.index).fillna(1.0)
        if 'variant_risk_ratio' not in ifr.columns:
            ifr['variant_risk_ratio'] = 1.29

        return ifr

    def load_ihr(self, draw_id: int) -> pd.DataFrame:
        ihr = io.load(self.infection_root.ihr(draw_id=draw_id))
        ihr = self.format_ratio_data(ihr)
        if 'variant_risk_ratio' not in ihr.columns:
            ihr['variant_risk_ratio'] = 1.29

        return ihr

    def load_idr(self, draw_id: int) -> pd.DataFrame:
        idr = io.load(self.infection_root.idr(draw_id=draw_id))
        idr = self.format_ratio_data(idr)
        return idr

    def load_ratio_data(self, draw_id: int) -> RatioData:
        ifr = self.load_ifr(draw_id)
        ihr = self.load_ihr(draw_id)
        idr = self.load_idr(draw_id)
        return RatioData(
            infection_to_death=int(ifr.duration.max()),
            infection_to_admission=int(ihr.duration.max()),
            infection_to_case=int(idr.duration.max()),
            ifr_scalar=ifr.variant_risk_ratio.max(),
            ihr_scalar=ihr.variant_risk_ratio.max(),
            ifr=ifr.ifr,
            ifr_hr=ifr.ifr_hr,
            ifr_lr=ifr.ifr_lr,
            ihr=ihr.ihr,
            idr=idr.idr,
        )

    def format_ratio_data(self, ratio_data: pd.DataFrame) -> pd.DataFrame:
        location_ids = self.load_location_ids()
        ratio_data = ratio_data.loc[location_ids]
        additional_cols = [c for c in ['duration', 'variant_risk_ratio'] if c in ratio_data.columns]        
        col_map = {c: c.split('_draw')[0] for c in ratio_data.columns if '_draw' in c}
        
        ratio_data = ratio_data.loc[:, additional_cols + list(col_map)].rename(columns=col_map)
        return ratio_data

    ##########################
    # Covariate data loaders #
    ##########################

    def check_covariates(self, covariates: Iterable[str]) -> None:
        """Ensure a reference scenario exists for all covariates.
        The reference scenario file is used to find the covariate values
        in the past (which we'll use to perform the regression).
        """
        missing = []

        for covariate in covariates:
            if covariate != 'intercept':
                if not io.exists(self.covariate_root[covariate](covariate_scenario='reference')):
                    missing.append(covariate)

        if missing:
            raise ValueError('All covariates supplied in the regression specification'
                             'must have a reference scenario in the covariate pool. Covariates'
                             f'missing a reference scenario: {missing}.')

    def load_covariate(self, covariate: str,
                       covariate_version: str = 'reference',
                       with_observed: bool = False,
                       covariate_root: io.CovariateRoot = None) -> pd.DataFrame:
        covariate_root = covariate_root if covariate_root is not None else self.covariate_root
        location_ids = self.load_location_ids()
        covariate_df = io.load(covariate_root[covariate](covariate_scenario=covariate_version))
        covariate_df = self._format_covariate_data(covariate_df, location_ids, with_observed)
        covariate_df = (covariate_df
                        .rename(columns={f'{covariate}_{covariate_version}': covariate})
                        .loc[:, [covariate]])
        return covariate_df

    def load_covariates(self, covariates: Iterable[str],
                        covariate_root: io.CovariateRoot = None) -> pd.DataFrame:
        if not isinstance(covariates, dict):
            covariates = {c: 'reference' for c in covariates}
        covariate_data = []
        for covariate, covariate_version in covariates.items():
            if covariate != 'intercept':
                covariate_data.append(
                    self.load_covariate(covariate, covariate_version,
                                        with_observed=False, covariate_root=covariate_root)
                )
        covariate_data = reduce(lambda x, y: x.merge(y, left_index=True, right_index=True), covariate_data)
        return covariate_data

    def load_priors(self, covariates: Iterable[CovariateSpecification]) -> Dict[str, pd.DataFrame]:
        cov_names = [cov.name for cov in covariates if cov.gprior == 'data']
        if not cov_names:
            return {}

        if not self.priors_root:
            raise ValueError(f'Covariates {cov_names} specified data-based priors but no priors version '
                             f'was specified.')
        priors = io.load(self.priors_root.priors())
        missing_priors = set(cov_names).difference(priors.covariate.unique())
        if missing_priors:
            raise ValueError(f'Covariates {missing_priors} specified data-based priors but no priors were '
                             f'found in the specified covariate priors version.')
        out = {}
        for covariate in covariates:
            if covariate.gprior == 'data':
                covariate_prior = (
                    priors
                    .set_index(['covariate', covariate.group_level])
                    .loc[covariate.name, ['mean', 'sd']]
                    .drop_duplicates()
                )
                assert set(covariate_prior.index) == set(covariate_prior.index.drop_duplicates())
                out[covariate.name] = covariate_prior

        return out

    def load_vaccinations(self, vaccine_scenario: str = 'reference',
                          covariate_root: io.CovariateRoot = None):
        covariate_root = covariate_root if covariate_root is not None else self.covariate_root
        location_ids = self.load_location_ids()
        if vaccine_scenario == 'none':
            # Grab the reference so we get the right index/schema.
            info_df = io.load(covariate_root.vaccine_info(info_type='vaccinations_reference'))
            info_df.loc[:, :] = 0.0
        else:
            info_df = io.load(covariate_root.vaccine_info(info_type=f'vaccinations_{vaccine_scenario}'))
        info_df = info_df[[c for c in info_df if 'omega' not in c]]
        return self._format_covariate_data(info_df, location_ids)

    def load_vaccination_summaries(self,
                                   measure: str,
                                   vaccine_scenario: str = 'reference',
                                   covariate_root: io.CovariateRoot = None):
        covariate_root = covariate_root if covariate_root is not None else self.covariate_root
        location_ids = self.load_location_ids()
        measures = [
            'cumulative_all_effective',
            'cumulative_all_vaccinated',
            'cumulative_all_fully_vaccinated',
            'hr_vaccinated',
            'lr_vaccinated',
            'vaccine_acceptance',
            'vaccine_acceptance_point',
        ]
        assert measure in measures
        if vaccine_scenario == 'none':
            # Grab the reference so we get the right index/schema.
            info_df = io.load(covariate_root.vaccine_info(info_type='vaccinations_reference_summary'))
            info_df = info_df.loc[:, [measure]]
            info_df.loc[:, :] = 0.0
        else:
            info_df = io.load(covariate_root.vaccine_info(info_type=f'vaccinations_{vaccine_scenario}_summary'))
            info_df = info_df.loc[:, [measure]]
        if measure == 'vaccine_acceptance_point':
            info_df = info_df.groupby('location_id').max()
        return info_df.dropna()

    def load_vaccine_efficacy(self, covariate_root: io.CovariateRoot = None):
        covariate_root = covariate_root if covariate_root is not None else self.covariate_root
        df = io.load(covariate_root.vaccine_info(info_type='vaccine_efficacy'))
        return df

    def load_mobility_info(self, info_type: str,
                           covariate_root: io.CovariateRoot = None) -> pd.DataFrame:
        covariate_root = covariate_root if covariate_root is not None else self.covariate_root
        location_ids = self.load_location_ids()
        info_df = io.load(covariate_root.mobility_info(info_type=info_type))
        return self._format_covariate_data(info_df, location_ids)

    def load_mandate_data(self, mobility_scenario: str,
                          covariate_root: io.CovariateRoot = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        percent_mandates = self.load_mobility_info(f'{mobility_scenario}_mandate_lift', covariate_root)
        mandate_effects = self.load_mobility_info(f'effect', covariate_root)
        return percent_mandates, mandate_effects

    def load_raw_variant_prevalence(self, variant_scenario: str = 'reference',
                                    covariate_root: io.CovariateRoot = None) -> pd.DataFrame:
        covariate_root = covariate_root if covariate_root is not None else self.covariate_root
        data = io.load(covariate_root.variant_info(info_type=variant_scenario))
        return data

    def load_variant_prevalence(self, variant_scenario: str = 'reference',
                                covariate_root: io.CovariateRoot = None) -> pd.DataFrame:
        data = self.load_raw_variant_prevalence(variant_scenario, covariate_root)
        rho = ((data['alpha'] / (data['alpha'] + data['ancestral']))
               .rename('rho')
               .groupby('location_id')
               .ffill()
               .fillna(0.))
        rho_variant = (data[['beta', 'gamma', 'delta', 'other']]
                       .sum(axis=1)
                       .rename('rho_variant'))
        rho_total = ((data['alpha'] + rho_variant)
                     .rename('rho_total'))
        rho_b1617 = ((data['delta'] / rho_variant)
                     .rename('rho_b1617')
                     .groupby('location_id')
                     .ffill()
                     .fillna(0.))
        return pd.concat([rho, rho_variant, rho_b1617, rho_total], axis=1)

    #######################
    # Regression data I/O #
    #######################

    def save_specification(self, specification: RegressionSpecification) -> None:
        io.dump(specification.to_dict(), self.regression_root.specification())

    def load_specification(self) -> RegressionSpecification:
        spec_dict = io.load(self.regression_root.specification())
        return RegressionSpecification.from_dict(spec_dict)

    def save_location_ids(self, location_ids: List[int]) -> None:
        io.dump(location_ids, self.regression_root.locations())

    def load_location_ids(self) -> List[int]:
        return io.load(self.regression_root.locations())

    def save_hierarchy(self, hierarchy: pd.DataFrame) -> None:
        io.dump(hierarchy, self.regression_root.hierarchy())

    def load_hierarchy(self) -> pd.DataFrame:
        return io.load(self.regression_root.hierarchy())

    def save_betas(self, betas: pd.DataFrame, draw_id: int) -> None:
        io.dump(betas, self.regression_root.beta(draw_id=draw_id))

    def load_betas(self, draw_id: int) -> pd.DataFrame:
        return io.load(self.regression_root.beta(draw_id=draw_id))

    def save_coefficients(self, coefficients: pd.DataFrame, draw_id: int) -> None:
        io.dump(coefficients, self.regression_root.coefficients(draw_id=draw_id))

    def load_coefficients(self, draw_id: int) -> pd.DataFrame:
        return io.load(self.regression_root.coefficients(draw_id=draw_id))

    def load_prior_run_coefficients(self, draw_id: int) -> Union[None, pd.DataFrame]:
        if self.coefficient_root:
            return io.load(self.coefficient_root.coefficients(draw_id=draw_id))
        return None

    def save_compartments(self, compartments: pd.DataFrame, draw_id: int) -> None:
        io.dump(compartments, self.regression_root.compartments(draw_id=draw_id))

    def load_compartments(self, draw_id: int) -> pd.DataFrame:
        return io.load(self.regression_root.compartments(draw_id=draw_id))

    def save_ode_parameters(self, df: pd.DataFrame, draw_id: int) -> None:
        io.dump(df, self.regression_root.ode_parameters(draw_id=draw_id))

    def load_ode_parameters(self, draw_id: int) -> pd.DataFrame:
        return io.load(self.regression_root.ode_parameters(draw_id=draw_id))

    def save_infections(self, infections: pd.Series, draw_id: int) -> None:
        io.dump(infections.to_frame(), self.regression_root.infections(draw_id=draw_id))

    def load_infections(self, draw_id: int) -> pd.Series:
        return io.load(self.regression_root.infections(draw_id=draw_id)).infections

    def save_deaths(self, deaths: pd.Series, draw_id: int) -> None:
        io.dump(deaths.to_frame(), self.regression_root.deaths(draw_id=draw_id))

    def load_deaths(self, draw_id: int) -> pd.Series:
        return io.load(self.regression_root.deaths(draw_id=draw_id)).deaths

    def save_hospitalizations(self, df: pd.DataFrame, measure: str) -> None:
        io.dump(df, self.regression_root.hospitalizations(measure=measure))

    def load_hospitalizations(self, measure: str) -> pd.DataFrame:
        return io.load(self.regression_root.hospitalizations(measure=measure))

    #########################
    # Non-interface helpers #
    #########################

    @staticmethod
    def _format_covariate_data(dataset: pd.DataFrame, location_ids: List[int], with_observed: bool = False):
        shared_locs = list(set(dataset.index.get_level_values('location_id')).intersection(location_ids))
        dataset = dataset.loc[shared_locs]
        if with_observed:
            dataset = dataset.set_index('observed', append=True)
        return dataset
