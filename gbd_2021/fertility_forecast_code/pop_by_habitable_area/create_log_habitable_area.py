import pandas as pd
import xarray as xr
from db_queries import get_location_metadata
from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr, save_xr
from fhs_lib_database_interface.lib.query.location import get_location_set
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions

gbd_round_pop = 6
stage = "population"
locations_df = get_location_set(gbd_round_id=6)
loc_ids = locations_df[(locations_df["level"] >= 3)].location_id.values
ihme_locs = get_location_metadata(location_set_id = 35, gbd_round_id = 6)
ihme_locs = ihme_locs[['location_id', 'location_name', 'local_id', 'super_region_name']]

####################################
##### load population #########
####################################

# load past population
past_pop_version = "VERSION"
past_pop_dir = FBDPath(f"/FILEPATH")
past_pop_da = open_xr(f"/FILEPATH.nc").data.sel(age_group_id = 22, sex_id = 3, location_id = list(loc_ids))
# load future population
future_pop_version = "VERSION"
future_pop_dir = FBDPath(f"/FILEPATH")
future_pop_da = open_xr(f"/FILEPATH.nc").data.sum(dim = "age_group_id").sum(dim = "sex_id").sel(scenario=0)

####################################
##### load habitable area #########
####################################
area_hab = pd.read_csv("/FILEPATH.csv")
area_hab = area_hab.rename(columns={"loc_id": "location_id"})[["location_id", "area_hab"]]

########################################################
##### Create Pop Density = Pop/Habitable Area #########
########################################################

# Create past version
past_pop_df_merge = past_pop_da.to_dataframe('population').reset_index().merge(area_hab).merge(ihme_locs)
past_pop_df_merge['pop_density'] = past_pop_df_merge['population'] / past_pop_df_merge['area_hab']
past_pop_density = xr.DataArray.from_series(past_pop_df_merge.set_index(["location_id", "year_id", "age_group_id", "sex_id"])["pop_density"])

# Create future version
future_pop_df_merge = future_pop_da.to_dataframe('population').reset_index().merge(area_hab).merge(ihme_locs)
future_pop_df_merge['pop_density'] = future_pop_df_merge['population'] / future_pop_df_merge['area_hab']
future_pop_density = xr.DataArray.from_series(future_pop_df_merge.set_index(["location_id", "year_id", "draw"])["pop_density"])

# Save future version with the required years
new_future_da = expand_dimensions(future_pop_density, year_id=range(2101, 2151), fill_value=future_pop_density.sel(year_id=2100, drop=True))
da_new = xr.concat([past_pop_density, new_future_da.sel(year_id=range(2020, 2151))], "year_id").sel(year_id=range(2020, 2151))
future_pop_density_file_path_past = "/FILEPATH/"
save_xr(da_new, f"{future_pop_density_file_path_past}/urbanicity.nc", metric="number", space="identity")

# Save past version with the required years
da = xr.concat([past_pop_density, future_pop_density.sel(year_id=range(2020, 2023))], "year_id")
past_pop_density_file_path_past = "/FILEPATH/"
save_xr(da, f"{past_pop_density_file_path_past}/urbanicity.nc", metric="number", space="identity")

log_past_da = np.log(da)
save_xr(log_past_da, f"{file_path_past}/urbanicity.nc", metric="number", space="identity")
log_future_da = np.log(da_new)
save_xr(log_future_da, f"{file_path_future}/urbanicity.nc", metric="number", space="identity")
