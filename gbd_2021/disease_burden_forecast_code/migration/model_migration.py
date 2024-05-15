"""
Clean and model UN migration estimates for forecasting.

example call:
python FILEPATH/model_migration.py \
--shocks-past-version 20201005_shocks_get_draws_1980_to_2019 \
--shocks-forecast-version 20210419_shocks_only_decay_weight_15 \
--gbd-round-id 6 \
--output-version click_20210510_limetr_fixedint \
--years 1950:2020:2050 \
--subnats

NOTE: If running with --subnats, make sure that each covariate (shocks,
 natural pop increase) has past and future years, and all locations.
 This avoids NaNs, which can prevent LimeTr from achieving convergence.
"""
import click
import pandas as pd

from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.file_system_manager import FileSystemManager
from fhs_lib_file_interface.lib.os_file_system import OSFileSystem
FileSystemManager.set_file_system(OSFileSystem())
from os.path import isfile
from fhs_lib_year_range_manager.lib.year_range import YearRange
from db_queries import get_location_metadata
from tiny_structured_logger.lib import fhs_logging

logger = fhs_logging.get_logger()


def _wrangle_2022_wpp(
        wpp_data: pd.DataFrame, 
        fhs_col_name: str, 
        loc_mapping: pd.DataFrame
        ) -> pd.DataFrame:

    logger.info("Reformatting and cleaning WPP input data")
    wpp_data = wpp_data.rename(columns={"Location code": "iso_num", "Year": "year_id"})
    wpp_data.fillna(0, inplace=True)
    wpp_data["iso_num"] = wpp_data["iso_num"].apply(int)
    wpp_data["year_id"] = wpp_data["year_id"].apply(int)
    
    # convert covariate to float
    wpp_data[fhs_col_name] = wpp_data[fhs_col_name].apply(float)
    
    bool_series = wpp_data.duplicated(keep='first')
    wpp_data = wpp_data[~bool_series]
    
    wpp_data = wpp_data.merge(loc_mapping, how="inner", on="iso_num")
    wpp_data = wpp_data.loc[:, ["location_id", "location_name", "year_id", "super_region_id", fhs_col_name]]

    return wpp_data


def _read_2022_wpp(
    data_path: FBDPath, 
    wpp_col_name: str,
    fhs_col_name: str,
    loc_mapping: pd.DataFrame,
    ) -> pd.DataFrame:
    """
    load Past and Future data in expected WPP format, data in same xlsx file
    as seperate sheets. Past as "ESTIMATES" and forecasts as "MEDIUM VARIANT"

    NOTE: read_excel requires openpyxl to be installed to function in later
    versions of pandas see https://github.com/pandas-dev/pandas/issues/38424

    NOTE: this loads data as formatted in WPP 2022, future WPP data may require
    refactoring

    Args:
        data_path (FBDPath): file path for WPP 2022 covariates
        wpp_col_name (str): WPP column name of covariate to load
        fhs_col_name (str): FHS column name to replace WPP name
        loc_mapping (pd.DataFrame): dataframe mapping IHME country codes to WPP country


    """
    logger.info("Reading WPP xlsx file")
    past_data = pd.read_excel(data_path, sheet_name="Estimates", skiprows=16,
                              na_values="...", engine="openpyxl")
    
    past_data = past_data.loc[:, ["Location code", "Year", wpp_col_name]]
    
    
    forecast_data = pd.read_excel(data_path, sheet_name="Medium variant", skiprows=16,
                                na_values="...", engine="openpyxl")
    
    forecast_data = forecast_data.loc[:, ["Location code", "Year", wpp_col_name]]
        
    complete_timeseries = past_data.append(forecast_data)
    complete_timeseries = complete_timeseries.rename(columns={wpp_col_name: fhs_col_name})
    
    #return complete_timeseries
    return _wrangle_2022_wpp(complete_timeseries, fhs_col_name, loc_mapping)


def read_shock(past_path, forecast_path, data_column_name, years):
    # Combine past/forecast shocks
    logger.info("Reading in Shocks")
    past_shock = pd.read_csv(past_path)
    past_shock = past_shock[["location_id", "year_id", "mortality"]]
    forecast_shock = pd.read_csv(forecast_path)
    forecast_shock = forecast_shock[["location_id", "year_id", "mortality"]]
    forecast_shock = forecast_shock.loc[
        forecast_shock["year_id"] >= years.forecast_start]
    past_shock = past_shock.loc[past_shock["year_id"] < years.forecast_start]
    shock = past_shock.append(forecast_shock)
    shock = shock.rename({"mortality": data_column_name}, axis=1)
    return shock


@click.command()
@click.option("--shocks-past-version", required=True, type=str)
@click.option("--shocks-forecast-version", required=True, type=str)
@click.option("--gbd-round-id", required=True, type=int)
@click.option("--output-version", required=True, type=str)
@click.option("--years", required=True, type=str, help="Years to forecast")
@click.option("--subnats/--no-subnats", required=False, default=False,
              help="Whether or not to include subnats")
def main(
    shocks_past_version: str, 
    shocks_forecast_version: str,
    gbd_round_id: int, 
    output_version: str, 
    years: str, 
    subnats: bool,
):
    """
    setup directories, creating output if needed. direct calls to other
    functions e.g. wrangle_wpp_data, load_combine_data, etc
    """

    years = YearRange.parse_year_range(years)

    disaster_past_path = FBDPath("FILEPATH/mean_inj_disaster.csv")
    disaster_forecast_path = FBDPath("FILEPATH/mean_inj_disaster.csv")
    execution_past_path = FBDPath("FILEPATH/mean_inj_war_execution.csv")
    execution_forecast_path = FBDPath("FILEPATH/mean_inj_war_execution.csv")
    terror_past_path = FBDPath("FILEPATH/mean_inj_war_warterror.csv")
    terror_forecast_path = FBDPath("FILEPATH/mean_inj_war_warterror.csv")

    logger.info("loading WPP data")
    loc_mapping_dir = ("FILEPATH.CSV")
    
    covariates_2022_path = FBDPath("FILEPATH.xlsx")


    data_dir = FBDPath("FILEPATH")
    data_dir.mkdir(parents=True, exist_ok=True)

    # Test whether migration forecasts already exist. If they do, skip recreating them.
    if isfile(f"{data_dir}/covariates_single_years.csv"):
        mig_covs_pred = pd.read_csv(f"{data_dir}/covariates_single_years.csv")
        logger.info(f"Result files already exist at: {data_dir}")
    # If not, align UN and IHME location ids
    else:
        logger.info("Result files did not exist")
        loc_mapping = pd.read_csv(loc_mapping_dir, encoding="ISO-8859-1")
        location_hierarchy = get_location_metadata(location_set_id=21,
                                                   gbd_round_id=gbd_round_id)
        loc_mapping = loc_mapping.merge(location_hierarchy, how="inner",
                                        on="location_id")
        loc_namelist = ["location_id", "iso_num", "ihme_loc_id",
                        "super_region_name", "region_name", "location_name_x", "super_region_id"]
        loc_mapping = loc_mapping[loc_namelist]
        loc_mapping = loc_mapping.rename(
            columns={"location_name_x":"location_name"})

        natural_pop_increase = _read_2022_wpp(
            covariates_2022_path,
            "Rate of Natural Change (per 1,000 population)",
            "natural_pop_increase",
            loc_mapping,
        )
        migration_rate = _read_2022_wpp(
            covariates_2022_path,
            "Net Migration Rate (per 1,000 population)",
            "migration_rate",
            loc_mapping,
        )
        median_age = _read_2022_wpp(
            covariates_2022_path,
            "Median Age, as of 1 July (years)",
            "median_age",
            loc_mapping,
        )

        # Read shocks
        disaster_dt = read_shock(disaster_past_path,
                                 disaster_forecast_path, "disaster", years)
        execution_dt = read_shock(execution_past_path,
                                  execution_forecast_path, "execution", years)
        terror_dt = read_shock(terror_past_path,
                               terror_forecast_path, "terror", years)


        merge_cols = ["location_id", "year_id"]
        # [ disaster, execution_dt, terror_dt, med_age, nat_pop, mig_rate]
        mig_covs_pred = pd.merge(execution_dt, disaster_dt, on=merge_cols)
        mig_covs_pred = mig_covs_pred.merge(terror_dt, on=merge_cols)
        mig_covs_pred = mig_covs_pred.merge(median_age, on=merge_cols)
        mig_covs_pred = mig_covs_pred.merge(natural_pop_increase,
                                            on=merge_cols + ["location_name"])
        mig_covs_pred = mig_covs_pred.merge(migration_rate,
                                            on=merge_cols + ["location_name"])

        mig_covs_pred.loc[:, "shocks"] = (mig_covs_pred["disaster"] +
                                          mig_covs_pred["execution"] +
                                          mig_covs_pred["terror"])

        logger.info("Remove duplicates by year & location if any,"
                    "create df list")

        # Removing duplicates, if any, from various dataframes.
        covar_list = []
        for df in [disaster_dt, execution_dt, terror_dt, natural_pop_increase, median_age]:
            if True in df.duplicated(
                                subset=['year_id', 'location_id']).to_list():
                df = df.drop_duplicates(subset=['year_id', 'location_id'])
                covar_list.append(df)
            else:
                covar_list.append(df)

        # Reassigning dataframes sans duplicates
        disaster_dt = covar_list[0]
        execution_dt = covar_list[1]
        terror_dt = covar_list[2]
        natural_pop_increase = covar_list[3]
        median_age = covar_list[5]

        # Creating subnats, appending to nationals, if --subnats is True
        if subnats is True:
            logger.info("Creating subnationals")
            subnat_locations = location_hierarchy[
                (location_hierarchy["parent_id"].isin(
                                                     [44533,
                                                      6,
                                                      102,
                                                      163,
                                                      135])) &
                (location_hierarchy["location_id"] != 44533)].location_id

            mig_covs_subnat_pred = subnat_locations.to_frame()
            mig_covs_subnat_pred["year_id"] = ([years.years] *
                                               len(mig_covs_subnat_pred))
            mig_covs_subnat_pred = mig_covs_subnat_pred.explode("year_id")
            subnat_location_hierarchy_dt = location_hierarchy.query(
                "level > 3")[["location_id", "location_name", "path_to_top_parent"]]

            # create table of subnational locations with national id as a new col
            subnat_location_hierarchy_dt.loc[:, "nat_location_id"] = pd.to_numeric(
                subnat_location_hierarchy_dt.path_to_top_parent.str.split(
                    ",", expand=True)[3], downcast="integer")
            subnat_location_hierarchy_dt.loc[:, "sr_location_id"] = pd.to_numeric(
                subnat_location_hierarchy_dt.path_to_top_parent.str.split(
                    ",", expand=True)[1], downcast="integer")

            mig_covs_subnat_pred = pd.merge(
                mig_covs_subnat_pred,
                subnat_location_hierarchy_dt[["location_id", "location_name",
                                              "nat_location_id", "sr_location_id"]],
                on="location_id")

            subset_cols = ["location_id", "year_id", "disaster", "execution",
                           "terror", "median_age", "natural_pop_increase",
                           "migration_rate", "shocks", "super_region_id"]

            mig_covs_subnat_pred = pd.merge(
                mig_covs_subnat_pred,
                mig_covs_pred[subset_cols].rename(
                    columns={"location_id": "nat_location_id", "super_region_id": "sr_location_id"}),
                on=["nat_location_id", "sr_location_id", "year_id"])
            mig_covs_subnat_pred = mig_covs_subnat_pred.drop("nat_location_id",
                                                             axis=1)
            mig_covs_subnat_pred = pd.merge(mig_covs_subnat_pred,
                                            on=merge_cols)
            # drop past rows for Hong Kong and Macao
            hong_kong_macao_forecast = mig_covs_subnat_pred[
                (mig_covs_subnat_pred["location_id"].isin([354, 361])) &
                (mig_covs_subnat_pred["year_id"] >= years.forecast_start)]
            mig_covs_subnat_pred = mig_covs_subnat_pred[
                ~mig_covs_subnat_pred.location_id.isin([354, 361])].append(
                    hong_kong_macao_forecast)
            
            mig_covs_subnat_pred = mig_covs_subnat_pred.rename(columns={"sr_location_id": "super_region_id"})
            mig_covs_pred = mig_covs_pred.append(mig_covs_subnat_pred)

        if True in mig_covs_pred.duplicated(
                                subset=['year_id', 'location_id']).to_list():
            mig_covs_pred = mig_covs_pred.drop_duplicates(
                                            subset=['year_id', 'location_id'])

        mig_covs_pred.loc[:, "scenario"] = 0
        mig_covs_pred = mig_covs_pred.drop("super_region_id_y", axis=1) # remove duplicate col

        # Writing covars to csv
        mig_covs_pred.to_csv(data_dir / "covariates_single_years.csv",
                             index=False)
        mig_covs_pred[mig_covs_pred["year_id"].isin(years.past_years)].to_csv(
            data_dir / "past_mig_rate_single_years.csv", index=False)

        p_mig_path = FBDPath("FILEPATH")
        p_mig_path.mkdir(parents=True, exist_ok=True)

        merge_cols = ["location_id", "year_id", "scenario"]

        mig_covs_pred[
                ["migration_rate"] + merge_cols].set_index(
                merge_cols).to_xarray().to_netcdf(
                data_dir / "migration_single_years.nc")
           
        # Saving past migration rate to past folder for LimeTr
        mig_covs_pred[
            ["migration_rate"] + merge_cols].set_index(
                    merge_cols).to_xarray().sel(
                    year_id=years.past_years).to_netcdf(
                    p_mig_path / "past_mig_rate_single_years.nc")

        def _save_covariate_as_nc(
            covariate_col: str,
            file_name: str,
            stage: str,
        ) -> None:
            
            logger.info(f"saving {covariate_col} to both migration directory and `FILEPATH` directory")
            covs_da = mig_covs_pred[[covariate_col] + merge_cols].set_index(merge_cols).to_xarray()
            covs_da.to_netcdf(data_dir / f"{file_name}.nc")

            covariate_dir = FBDPath("FILEPATH")
            past_covariate_dir = FBDPath("FILEPATH")

            covariate_dir.mkdir(parents=True, exist_ok=True)
            past_covariate_dir.mkdir(parents=True, exist_ok=True)

            mig_covs_pred[[covariate_col] + merge_cols].set_index(
                merge_cols).to_xarray().sel(
                year_id=years.past_years).to_netcdf(
                past_covariate_dir / f"past_{file_name}.nc")
            
            mig_covs_pred[[covariate_col] + merge_cols].set_index(
                merge_cols).to_xarray().sel(
                year_id=years.forecast_years).to_netcdf(
                covariate_dir / f"forecast_{file_name}.nc")
            
        _save_covariate_as_nc("shocks", "shocks_single_years", "death")
        _save_covariate_as_nc("natural_pop_increase", "natural_pop_increase_single_years", "population")
        _save_covariate_as_nc("median_age", "median_age_single_years", "median_age")

if __name__ == "__main__":
    main()
